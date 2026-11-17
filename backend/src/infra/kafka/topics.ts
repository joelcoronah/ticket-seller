import type { ITopicConfig } from 'kafkajs';
import { appConfig } from '../../config';

export const TOPICS = {
  purchaseRequests: 'purchase-requests',
  ordersConfirmed: 'orders-confirmed',
} as const;

export const CONSUMER_GROUPS = {
  reservations: 'reservation-workers',
  persisters: 'order-persisters',
} as const;

export const TOPIC_CONFIGS: ITopicConfig[] = [
  {
    topic: TOPICS.purchaseRequests,
    // key = eventId: every request of one event lands in ONE partition -> strict FIFO
    // per event. Partitions give parallelism ACROSS events.
    numPartitions: appConfig.kafkaPurchasePartitions,
    configEntries: [{ name: 'retention.ms', value: String(24 * 3600 * 1000) }],
  },
  {
    topic: TOPICS.ordersConfirmed,
    numPartitions: 3,
    configEntries: [{ name: 'retention.ms', value: String(7 * 24 * 3600 * 1000) }],
  },
];
