import { ConflictException, Injectable, NotFoundException, ServiceUnavailableException } from '@nestjs/common';
import { appConfig } from '../../config';
import type { PurchaseRequestMessage, ReserveResult } from '../../domain/types';
import { TOPICS } from '../../infra/kafka/topics';
import { KafkaService } from '../../infra/kafka/kafka.service';
import { keys as k } from '../../infra/redis/keys';
import { RedisService } from '../../infra/redis/redis.service';
import { InventoryService } from '../inventory/inventory.service';
import type { CreatePurchaseRequestDto } from './purchase.dto';

export type RequestStatus =
  | { state: 'QUEUED'; requestId: string; position: number; ahead: number }
  | { state: 'DONE'; requestId: string; result: ReserveResult };

@Injectable()
export class PurchaseService {
  constructor(
    private readonly redis: RedisService,
    private readonly kafka: KafkaService,
    private readonly inventory: InventoryService,
  ) {}

  /**
   * Admission gate. Hot path: 2-3 Redis ops + 1 Kafka append. No DB access.
   * Returns 202 immediately; the outcome arrives via SSE.
   */
  async enqueue(eventId: number, userId: string, dto: CreatePurchaseRequestDto) {
    const r = this.redis.client;
    const posKey = k.requestPos(eventId, dto.requestId);

    const [onSale, existing, available, held] = await Promise.all([
      this.inventory.isOnSale(eventId),
      r.get(posKey),
      r.scard(k.available(eventId)),
      r.zcard(k.holds(eventId)),
    ]);
    if (!onSale) throw new ConflictException({ code: 'SALE_NOT_OPEN' });
    if (existing) return this.accepted(eventId, dto.requestId, Number(existing)); // idempotent retry

    // Shed load at the edge: no point queueing when nothing can be sold.
    // `held` tells the client whether seats may come back when holds expire.
    if (available === 0) throw new ConflictException({ code: 'SOLD_OUT', held });

    const position = await r.incr(k.queueSeq(eventId));
    const claimed = await r.set(posKey, position, 'EX', appConfig.requestResultTtlSec, 'NX');
    if (!claimed) return this.accepted(eventId, dto.requestId, Number(await r.get(posKey)));

    const msg: PurchaseRequestMessage = {
      v: 1,
      requestId: dto.requestId,
      eventId,
      userId,
      seatId: dto.seatId ?? null,
      position,
      enqueuedAt: Date.now(),
    };
    try {
      await this.kafka.producer.send(TOPICS.purchaseRequests, { key: String(eventId), value: JSON.stringify(msg) });
    } catch {
      await r.del(posKey); // let the client retry with the same requestId
      throw new ServiceUnavailableException({ code: 'QUEUE_UNAVAILABLE' });
    }
    return this.accepted(eventId, dto.requestId, position);
  }

  async status(eventId: number, requestId: string): Promise<RequestStatus> {
    const [result, pos, done] = await Promise.all([
      this.inventory.getResult(eventId, requestId),
      this.redis.client.get(k.requestPos(eventId, requestId)),
      this.redis.client.get(k.queueDone(eventId)),
    ]);
    if (result) return { state: 'DONE', requestId, result };
    if (!pos) throw new NotFoundException({ code: 'REQUEST_NOT_FOUND' });
    const position = Number(pos);
    return { state: 'QUEUED', requestId, position, ahead: Math.max(0, position - Number(done ?? 0) - 1) };
  }

  private async accepted(eventId: number, requestId: string, position: number) {
    const done = Number((await this.redis.client.get(k.queueDone(eventId))) ?? 0);
    const base = `/events/${eventId}/purchase-requests/${requestId}`;
    return {
      requestId,
      eventId,
      position,
      ahead: Math.max(0, position - done - 1),
      statusUrl: base,
      streamUrl: `${base}/stream`,
    };
  }
}
