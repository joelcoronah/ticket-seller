import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { appConfig } from '../config';
import { KafkaService } from '../infra/kafka/kafka.service';
import { TOPICS } from '../infra/kafka/topics';
import { InventoryService } from '../modules/inventory/inventory.service';
import { RealtimePublisher } from '../modules/realtime/realtime.publisher';

const OUTBOX_INTERVAL_MS = 15_000;
const OUTBOX_GRACE_MS = 30_000;

/**
 * Periodic jobs. Safe to run on every worker replica: the reaper is an atomic
 * Lua script, and republishing the outbox is idempotent downstream.
 */
@Injectable()
export class MaintenanceService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly log = new Logger(MaintenanceService.name);
  private readonly timers: NodeJS.Timeout[] = [];
  private reaping = false;
  private reconciling = false;

  constructor(
    private readonly inventory: InventoryService,
    private readonly publisher: RealtimePublisher,
    private readonly kafka: KafkaService,
  ) {}

  onApplicationBootstrap() {
    this.timers.push(setInterval(() => void this.reapHolds(), appConfig.reaperIntervalMs));
    this.timers.push(setInterval(() => void this.reconcileOutbox(), OUTBOX_INTERVAL_MS));
  }

  /** Hold TTL: expired holds go back to `available` within ~1 s. */
  async reapHolds() {
    if (this.reaping) return;
    this.reaping = true;
    try {
      for (const eventId of await this.inventory.activeSales()) {
        const released = await this.inventory.releaseExpired(eventId);
        if (released > 0) {
          this.log.log(`event ${eventId}: ${released} expired hold(s) returned to inventory`);
          await this.publisher.publishInventory(eventId, released);
        }
      }
    } catch (err) {
      this.log.error(`reaper failed: ${(err as Error).message}`);
    } finally {
      this.reaping = false;
    }
  }

  /** Republishes confirmed orders that were not persisted (API crashed, Kafka hiccup...). */
  async reconcileOutbox() {
    if (this.reconciling) return;
    this.reconciling = true;
    try {
      for (const eventId of await this.inventory.activeSales()) {
        const stale = (await this.inventory.outboxEntries(eventId)).filter(
          (o) => Date.now() - o.confirmedAt > OUTBOX_GRACE_MS,
        );
        for (const o of stale) {
          await this.kafka.producer.send(TOPICS.ordersConfirmed, { key: o.orderId, value: JSON.stringify(o) });
        }
        if (stale.length) this.log.warn(`event ${eventId}: republished ${stale.length} outbox order(s)`);
      }
    } catch (err) {
      this.log.error(`outbox reconcile failed: ${(err as Error).message}`);
    } finally {
      this.reconciling = false;
    }
  }

  onModuleDestroy() {
    this.timers.forEach(clearInterval);
  }
}
