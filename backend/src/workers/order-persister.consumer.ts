import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import type { OrderConfirmedMessage } from '../domain/types';
import { DatabaseService } from '../infra/database/database.service';
import { KafkaService } from '../infra/kafka/kafka.service';
import { CONSUMER_GROUPS, TOPICS } from '../infra/kafka/topics';
import { InventoryService } from '../modules/inventory/inventory.service';

/**
 * Final persistence. Idempotent (orders.id = holdId, ON CONFLICT DO NOTHING),
 * so outbox republishes and Kafka redeliveries are harmless.
 * UNIQUE (event_id, seat_id) on tickets is the last line of defense: if Redis
 * were ever wrong, PostgreSQL refuses the second ticket and we flag a CONFLICT.
 */
@Injectable()
export class OrderPersisterConsumer implements OnApplicationBootstrap {
  private readonly log = new Logger(OrderPersisterConsumer.name);

  constructor(
    private readonly kafka: KafkaService,
    private readonly db: DatabaseService,
    private readonly inventory: InventoryService,
  ) {}

  async onApplicationBootstrap() {
    const consumer = this.kafka.consumer(CONSUMER_GROUPS.persisters);
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.ordersConfirmed, fromBeginning: true });
    await consumer.run({
      partitionsConsumedConcurrently: 3,
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const order = JSON.parse(message.value.toString()) as OrderConfirmedMessage;
        await this.persist(order); // throws -> kafkajs retries with backoff
        await this.inventory.ackOutbox(order.eventId, order.orderId);
      },
    });
  }

  async persist(o: OrderConfirmedMessage): Promise<void> {
    await this.db.tx(async (c) => {
      const inserted = await c.query(
        `INSERT INTO orders (id, event_id, user_id, status, total_cents, payment_ref, confirmed_at)
         VALUES ($1, $2, $3, 'PAID', $4, $5, to_timestamp($6 / 1000.0))
         ON CONFLICT (id) DO NOTHING`,
        [o.orderId, o.eventId, o.userId, o.priceCents, o.paymentRef, o.confirmedAt],
      );
      if (inserted.rowCount === 0) return; // already persisted (redelivery)

      const ticket = await c.query(
        `INSERT INTO tickets (order_id, event_id, seat_id)
         SELECT $1, $2, s.id FROM seats s WHERE s.event_id = $2 AND s.code = $3
         ON CONFLICT ON CONSTRAINT tickets_one_per_seat DO NOTHING
         RETURNING id`,
        [o.orderId, o.eventId, o.seatId],
      );
      if (ticket.rowCount === 0) {
        // Seat already ticketed to another order (or unknown seat): never silently drop it.
        await c.query(`UPDATE orders SET status = 'CONFLICT' WHERE id = $1`, [o.orderId]);
        this.log.error(`OVERSELL GUARD: seat ${o.seatId} of event ${o.eventId} rejected for order ${o.orderId} — refund required`);
      }
    });
  }
}
