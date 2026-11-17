import { GoneException, HttpException, HttpStatus, Injectable, Logger, NotFoundException } from '@nestjs/common';
import type { OrderConfirmedMessage } from '../../domain/types';
import { KafkaService } from '../../infra/kafka/kafka.service';
import { TOPICS } from '../../infra/kafka/topics';
import { InventoryService } from '../inventory/inventory.service';
import { PaymentDeclinedError, PaymentsService } from './payments.service';

@Injectable()
export class CheckoutService {
  private readonly log = new Logger(CheckoutService.name);

  constructor(
    private readonly inventory: InventoryService,
    private readonly payments: PaymentsService,
    private readonly kafka: KafkaService,
  ) {}

  /**
   * Hold -> payment -> confirm. Traffic here is bounded by capacity (only hold
   * owners reach it), never by the size of the crowd.
   */
  async checkout(eventId: number, holdId: string, userId: string, seatId: string) {
    if (await this.inventory.isConfirmed(eventId, holdId)) {
      return { orderId: holdId, status: 'CONFIRMED' as const };
    }

    const hold = await this.inventory.getHold(eventId, seatId);
    if (!hold || hold.holdId !== holdId || hold.userId !== userId) {
      throw new NotFoundException({ code: 'HOLD_NOT_FOUND' });
    }
    if (hold.expiresAt <= Date.now()) throw new GoneException({ code: 'HOLD_EXPIRED' });

    const priceCents = (await this.inventory.priceOf(eventId, seatId)) ?? 0;

    let paymentRef: string;
    try {
      paymentRef = await this.payments.charge({ idempotencyKey: holdId, amountCents: priceCents, userId });
    } catch (err) {
      if (err instanceof PaymentDeclinedError) {
        // Hold stays until TTL: the user can retry with another card.
        throw new HttpException({ code: 'PAYMENT_DECLINED' }, HttpStatus.PAYMENT_REQUIRED);
      }
      throw err;
    }

    const order: OrderConfirmedMessage = {
      v: 1,
      orderId: holdId,
      eventId,
      userId,
      seatId,
      priceCents,
      paymentRef,
      confirmedAt: Date.now(),
    };

    const outcome = await this.inventory.confirm(order);
    if (outcome === 'HOLD_EXPIRED' || outcome === 'HOLD_NOT_FOUND') {
      // Hold expired while the provider was charging: give the money back.
      await this.payments.refund(paymentRef);
      throw new GoneException({ code: outcome, refunded: true });
    }

    // Best effort: if this fails, the outbox reconciler republishes it.
    try {
      await this.kafka.producer.send(TOPICS.ordersConfirmed, { key: order.orderId, value: JSON.stringify(order) });
    } catch (err) {
      this.log.warn(`orders-confirmed publish failed for ${order.orderId}; outbox will retry`);
    }
    return { orderId: order.orderId, status: 'CONFIRMED' as const, seatId, priceCents };
  }
}
