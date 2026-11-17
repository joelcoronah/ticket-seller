const int = (v: string | undefined, d: number) => (v ? parseInt(v, 10) : d);

export const appConfig = {
  port: int(process.env.PORT, 3000),
  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(','),
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://flashsale:flashsale@localhost:5432/flashsale',
  corsOrigins: (process.env.CORS_ORIGINS ?? 'http://localhost:5173').split(','),
  adminToken: process.env.ADMIN_TOKEN ?? 'dev-admin-token',
  /** Allows `open-sale?reset=true` to wipe orders. Never enable in production. */
  allowReset: process.env.ALLOW_RESET === 'true',

  holdTtlMs: int(process.env.HOLD_TTL_MS, 5 * 60 * 1000),
  requestResultTtlSec: int(process.env.REQUEST_RESULT_TTL_SEC, 3600),
  /** Requests that waited longer than this in the queue are discarded (user is gone). */
  requestMaxAgeMs: int(process.env.REQUEST_MAX_AGE_MS, 10 * 60 * 1000),

  kafkaPurchasePartitions: int(process.env.KAFKA_PURCHASE_PARTITIONS, 12),
  workerPartitionConcurrency: int(process.env.WORKER_PARTITION_CONCURRENCY, 4),
  workerChunkSize: int(process.env.WORKER_CHUNK_SIZE, 250),
  reaperIntervalMs: int(process.env.REAPER_INTERVAL_MS, 1000),

  paymentFailureRate: parseFloat(process.env.PAYMENT_FAILURE_RATE ?? '0'),
};
