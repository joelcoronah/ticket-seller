import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import type { KafkaMessage } from 'kafkajs';
import { appConfig } from '../config';
import type { PurchaseRequestMessage } from '../domain/types';
import { KafkaService } from '../infra/kafka/kafka.service';
import { CONSUMER_GROUPS, TOPICS } from '../infra/kafka/topics';
import { InventoryService } from '../modules/inventory/inventory.service';
import { RealtimePublisher } from '../modules/realtime/realtime.publisher';

/**
 * Reservation worker pool (Kafka consumer group).
 *
 * - FIFO: one event == one partition, and a partition is consumed by exactly one
 *   group member, in offset order.
 * - Throughput: messages are processed in chunks; each chunk is ONE Redis
 *   pipeline of `reserve.lua` calls, executed in order.
 * - At-least-once: offsets are committed after Redis wrote the results; a
 *   redelivered message hits the idempotency cache inside the Lua script.
 */
@Injectable()
export class PurchaseConsumer implements OnApplicationBootstrap {
  private readonly log = new Logger(PurchaseConsumer.name);

  constructor(
    private readonly kafka: KafkaService,
    private readonly inventory: InventoryService,
    private readonly publisher: RealtimePublisher,
  ) {}

  async onApplicationBootstrap() {
    const consumer = this.kafka.consumer(CONSUMER_GROUPS.reservations);
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.purchaseRequests, fromBeginning: true });
    await consumer.run({
      partitionsConsumedConcurrently: appConfig.workerPartitionConcurrency,
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
        const size = appConfig.workerChunkSize;
        for (let i = 0; i < batch.messages.length; i += size) {
          if (!isRunning() || isStale()) return;
          const chunk = batch.messages.slice(i, i + size);
          await this.processChunk(chunk);
          resolveOffset(chunk[chunk.length - 1].offset);
          await commitOffsetsIfNecessary();
          await heartbeat();
        }
      },
    });
    this.log.log(`Consuming ${TOPICS.purchaseRequests} (partition concurrency ${appConfig.workerPartitionConcurrency})`);
  }

  private async processChunk(messages: KafkaMessage[]) {
    const fresh: PurchaseRequestMessage[] = [];
    const stale: PurchaseRequestMessage[] = [];
    const lastPosition = new Map<number, number>();
    const now = Date.now();

    for (const m of messages) {
      const req = parse(m.value);
      if (!req) {
        this.log.warn(`Skipping malformed message at offset ${m.offset}`); // poison pill
        continue;
      }
      (now - req.enqueuedAt > appConfig.requestMaxAgeMs ? stale : fresh).push(req);
      lastPosition.set(req.eventId, Math.max(lastPosition.get(req.eventId) ?? 0, req.position));
    }

    const results = [
      ...(await this.inventory.reserveMany(fresh)),
      ...(await this.inventory.expireRequests(stale)),
    ];
    await this.publisher.publishResults(results, lastPosition);
  }
}

function parse(value: Buffer | null): PurchaseRequestMessage | null {
  if (!value) return null;
  try {
    const m = JSON.parse(value.toString()) as PurchaseRequestMessage;
    return m.v === 1 && m.requestId && m.userId && Number.isInteger(m.eventId) ? m : null;
  } catch {
    return null;
  }
}
