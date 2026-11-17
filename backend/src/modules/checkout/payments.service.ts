import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { appConfig } from '../../config';

export class PaymentDeclinedError extends Error {}

/**
 * Payment gateway mock. A real integration (Stripe, Mercado Pago, ...) must use
 * `idempotencyKey` so a retried checkout never charges twice.
 */
@Injectable()
export class PaymentsService {
  private readonly charges = new Map<string, string>();

  async charge(input: { idempotencyKey: string; amountCents: number; userId: string }): Promise<string> {
    const prev = this.charges.get(input.idempotencyKey);
    if (prev) return prev;
    await sleep(50 + Math.random() * 150); // simulated provider latency
    if (Math.random() < appConfig.paymentFailureRate) throw new PaymentDeclinedError('card_declined');
    const ref = `pay_${randomUUID()}`;
    this.charges.set(input.idempotencyKey, ref);
    return ref;
  }

  async refund(paymentRef: string): Promise<void> {
    await sleep(20);
    for (const [key, ref] of this.charges) if (ref === paymentRef) this.charges.delete(key);
  }
}
