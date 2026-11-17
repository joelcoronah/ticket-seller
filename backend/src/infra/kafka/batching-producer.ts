import type { Message, Producer } from 'kafkajs';

interface Pending {
  message: Message;
  resolve: () => void;
  reject: (err: unknown) => void;
}

/**
 * Micro-batching on top of the kafkajs producer.
 *
 * An idempotent producer allows only 1 in-flight request, so 5,000 individual
 * `send()` calls would be serialized (~ms each). Instead we buffer messages for
 * `lingerMs` (or until `maxBatch`) and ship them in one `sendBatch`. Batches are
 * chained, so arrival order at the API == order in the partition log (FIFO).
 * Each caller's promise resolves only after the broker acked (acks=all).
 */
export class BatchingProducer {
  private buffer = new Map<string, Pending[]>();
  private size = 0;
  private timer: NodeJS.Timeout | null = null;
  private chain: Promise<void> = Promise.resolve();

  constructor(
    private readonly producer: Producer,
    private readonly opts: { lingerMs: number; maxBatch: number },
  ) {}

  send(topic: string, message: Message): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const list = this.buffer.get(topic) ?? [];
      list.push({ message, resolve, reject });
      this.buffer.set(topic, list);
      this.size++;
      if (this.size >= this.opts.maxBatch) void this.flush();
      else if (!this.timer) this.timer = setTimeout(() => void this.flush(), this.opts.lingerMs);
    });
  }

  flush(): Promise<void> {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (this.size === 0) return this.chain;

    const entries = [...this.buffer.entries()];
    this.buffer = new Map();
    this.size = 0;

    this.chain = this.chain.then(async () => {
      try {
        await this.producer.sendBatch({
          acks: -1,
          topicMessages: entries.map(([topic, list]) => ({ topic, messages: list.map((p) => p.message) })),
        });
        for (const [, list] of entries) list.forEach((p) => p.resolve());
      } catch (err) {
        for (const [, list] of entries) list.forEach((p) => p.reject(err));
      }
    });
    return this.chain;
  }
}
