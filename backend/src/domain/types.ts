export type ReserveStatus =
  | 'HELD'
  | 'USER_HAS_HOLD'
  | 'SOLD_OUT'
  | 'SEAT_UNAVAILABLE'
  | 'EXPIRED_IN_QUEUE'
  | 'ERROR';

export interface ReserveResult {
  status: ReserveStatus;
  requestId: string;
  eventId: number;
  holdId?: string;
  seatId?: string;
  expiresAt?: number;
  message?: string;
}

export type ConfirmStatus = 'CONFIRMED' | 'ALREADY_CONFIRMED' | 'HOLD_NOT_FOUND' | 'HOLD_EXPIRED';

/** Message on topic `purchase-requests` (key = eventId -> one partition per event = FIFO). */
export interface PurchaseRequestMessage {
  v: 1;
  requestId: string;
  eventId: number;
  userId: string;
  seatId: string | null;
  position: number;
  enqueuedAt: number;
}

/** Message on topic `orders-confirmed` (key = orderId). orderId === holdId. */
export interface OrderConfirmedMessage {
  v: 1;
  orderId: string;
  eventId: number;
  userId: string;
  seatId: string;
  priceCents: number;
  paymentRef: string;
  confirmedAt: number;
}

/** Pub/sub payloads from workers to API instances. */
export type RealtimeUpdate =
  | { type: 'result'; result: ReserveResult }
  | { type: 'progress'; done: number }
  | { type: 'inventory'; released: number };
