import { Injectable } from '@nestjs/common';
import type { RealtimeUpdate, ReserveResult } from '../../domain/types';
import { channels } from '../../infra/redis/keys';
import { RedisService } from '../../infra/redis/redis.service';

/** Worker side: fans results out to every API instance via Redis pub/sub. */
@Injectable()
export class RealtimePublisher {
  constructor(private readonly redis: RedisService) {}

  async publishResults(results: ReserveResult[], positions: Map<number, number>): Promise<void> {
    if (results.length === 0 && positions.size === 0) return;
    const p = this.redis.client.pipeline();
    for (const result of results) {
      p.publish(channels.updates(result.eventId), JSON.stringify({ type: 'result', result } satisfies RealtimeUpdate));
    }
    // One progress message per event per chunk (not per request): keeps fan-out bounded.
    for (const [eventId, done] of positions) {
      p.publish(channels.updates(eventId), JSON.stringify({ type: 'progress', done } satisfies RealtimeUpdate));
    }
    await p.exec();
  }

  async publishInventory(eventId: number, released: number): Promise<void> {
    await this.redis.client.publish(
      channels.updates(eventId),
      JSON.stringify({ type: 'inventory', released } satisfies RealtimeUpdate),
    );
  }
}
