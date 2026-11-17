import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { appConfig } from '../../config';
import type {
  ConfirmStatus,
  OrderConfirmedMessage,
  PurchaseRequestMessage,
  ReserveResult,
} from '../../domain/types';
import { keys as k } from '../../infra/redis/keys';
import { RedisService } from '../../infra/redis/redis.service';

const LAZY_RECLAIM_BATCH = 50;

export interface InventorySnapshot {
  capacity: number;
  available: number;
  held: number;
  sold: number;
  queued: number;
  processed: number;
}

/** All stock mutations go through Lua scripts. No read-modify-write from Node. */
@Injectable()
export class InventoryService {
  constructor(private readonly redis: RedisService) {}

  private get r() {
    return this.redis.client;
  }

  /**
   * Reserves seats for a chunk of queued requests in ONE pipeline.
   * Redis executes pipelined commands in order, so FIFO is preserved while we
   * pay a single network round trip for the whole chunk.
   */
  async reserveMany(reqs: PurchaseRequestMessage[]): Promise<ReserveResult[]> {
    if (reqs.length === 0) return [];
    const p = this.r.pipeline();
    for (const q of reqs) {
      p.fsReserve(
        k.available(q.eventId),
        k.holds(q.eventId),
        k.holdMeta(q.eventId),
        k.userHold(q.eventId, q.userId),
        k.request(q.eventId, q.requestId),
        k.queueDone(q.eventId),
        q.seatId ?? '',
        q.userId,
        randomUUID(),
        appConfig.holdTtlMs,
        appConfig.requestResultTtlSec,
        q.position,
        LAZY_RECLAIM_BATCH,
      );
    }
    const out = (await p.exec()) ?? [];
    return out.map(([err, raw], i) => {
      const base = { requestId: reqs[i].requestId, eventId: reqs[i].eventId };
      if (err) return { ...base, status: 'ERROR', message: err.message };
      return { ...base, ...(JSON.parse(raw as string) as Omit<ReserveResult, 'requestId' | 'eventId'>) };
    });
  }

  /** Marks requests that waited too long as expired (no inventory touched). */
  async expireRequests(reqs: PurchaseRequestMessage[]): Promise<ReserveResult[]> {
    if (reqs.length === 0) return [];
    const p = this.r.pipeline();
    const results = reqs.map<ReserveResult>((q) => ({
      requestId: q.requestId,
      eventId: q.eventId,
      status: 'EXPIRED_IN_QUEUE',
    }));
    results.forEach((res, i) => {
      const { status } = res;
      p.set(k.request(reqs[i].eventId, reqs[i].requestId), JSON.stringify({ status }), 'EX', appConfig.requestResultTtlSec, 'NX');
    });
    await p.exec();
    return results;
  }

  async getResult(eventId: number, requestId: string): Promise<ReserveResult | null> {
    const raw = await this.r.get(k.request(eventId, requestId));
    return raw ? { ...JSON.parse(raw), requestId, eventId } : null;
  }

  async getHold(eventId: number, seatId: string) {
    const [meta, score] = await Promise.all([
      this.r.hget(k.holdMeta(eventId), seatId),
      this.r.zscore(k.holds(eventId), seatId),
    ]);
    if (!meta || !score) return null;
    const [holdId, userId] = meta.split('|');
    return { holdId, userId, seatId, expiresAt: Number(score) };
  }

  async isConfirmed(eventId: number, holdId: string): Promise<boolean> {
    return (await this.r.hexists(k.confirmed(eventId), holdId)) === 1;
  }

  async priceOf(eventId: number, seatId: string): Promise<number | null> {
    const v = await this.r.hget(k.prices(eventId), seatId);
    return v === null ? null : Number(v);
  }

  /** Held -> Sold + outbox entry, atomically. */
  confirm(order: OrderConfirmedMessage): Promise<ConfirmStatus> {
    const e = order.eventId;
    return this.r.fsConfirm(
      k.holds(e),
      k.holdMeta(e),
      k.sold(e),
      k.userHold(e, order.userId),
      k.outbox(e),
      k.confirmed(e),
      order.seatId,
      order.orderId,
      order.userId,
      JSON.stringify(order),
    ) as Promise<ConfirmStatus>;
  }

  releaseExpired(eventId: number, batch = 500): Promise<number> {
    return this.r.fsReleaseExpired(k.available(eventId), k.holds(eventId), k.holdMeta(eventId), batch);
  }

  async snapshot(eventId: number): Promise<InventorySnapshot> {
    const [capacity, available, held, sold, queued, processed] = await Promise.all([
      this.r.hget(k.meta(eventId), 'capacity'),
      this.r.scard(k.available(eventId)),
      this.r.zcard(k.holds(eventId)),
      this.r.scard(k.sold(eventId)),
      this.r.get(k.queueSeq(eventId)),
      this.r.get(k.queueDone(eventId)),
    ]);
    return {
      capacity: Number(capacity ?? 0),
      available,
      held,
      sold,
      queued: Number(queued ?? 0),
      processed: Number(processed ?? 0),
    };
  }

  async availableSeats(eventId: number): Promise<string[]> {
    return this.r.smembers(k.available(eventId));
  }

  async isOnSale(eventId: number): Promise<boolean> {
    return (await this.r.sismember(k.activeSales, String(eventId))) === 1;
  }

  /**
   * Loads inventory from PostgreSQL (source of truth) into Redis.
   * Also the recovery path if Redis loses data: available = seats - sold tickets.
   */
  async loadSale(
    eventId: number,
    seats: { code: string; priceCents: number }[],
    soldCodes: string[],
  ): Promise<void> {
    await this.wipe(eventId);
    const sold = new Set(soldCodes);
    const tx = this.r.multi();
    const free = seats.filter((s) => !sold.has(s.code)).map((s) => s.code);
    for (let i = 0; i < free.length; i += 1000) tx.sadd(k.available(eventId), ...free.slice(i, i + 1000));
    for (let i = 0; i < soldCodes.length; i += 1000) tx.sadd(k.sold(eventId), ...soldCodes.slice(i, i + 1000));
    for (let i = 0; i < seats.length; i += 1000) {
      tx.hset(k.prices(eventId), Object.fromEntries(seats.slice(i, i + 1000).map((s) => [s.code, s.priceCents])));
    }
    tx.hset(k.meta(eventId), { capacity: seats.length, openedAt: Date.now() });
    tx.sadd(k.activeSales, String(eventId));
    await tx.exec();
  }

  async closeSale(eventId: number): Promise<void> {
    await this.r.srem(k.activeSales, String(eventId));
  }

  async activeSales(): Promise<number[]> {
    return (await this.r.smembers(k.activeSales)).map(Number);
  }

  async outboxEntries(eventId: number): Promise<OrderConfirmedMessage[]> {
    const all = await this.r.hvals(k.outbox(eventId));
    return all.map((v) => JSON.parse(v) as OrderConfirmedMessage);
  }

  async outboxSize(eventId: number): Promise<number> {
    return this.r.hlen(k.outbox(eventId));
  }

  async ackOutbox(eventId: number, orderId: string): Promise<void> {
    await this.r.hdel(k.outbox(eventId), orderId);
  }

  /** Deletes every key of an event (SCAN, never KEYS, to avoid blocking Redis). */
  private async wipe(eventId: number): Promise<void> {
    const stream = this.r.scanStream({ match: k.eventPattern(eventId), count: 1000 });
    for await (const batch of stream) {
      if ((batch as string[]).length) await this.r.unlink(...(batch as string[]));
    }
  }
}
