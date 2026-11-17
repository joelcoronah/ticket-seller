import { Injectable, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';
import { appConfig } from '../../config';
import { defineScripts } from './scripts';

@Injectable()
export class RedisService implements OnModuleDestroy {
  /**
   * Command client. Auto-pipelining batches concurrent commands issued in the
   * same event-loop tick into one round trip — big win under 5k concurrent requests.
   */
  readonly client: Redis;
  private readonly extra: Redis[] = [];

  constructor() {
    this.client = new Redis(appConfig.redisUrl, {
      enableAutoPipelining: true,
      maxRetriesPerRequest: 3,
    });
    defineScripts(this.client);
  }

  /** Dedicated connection (required for SUBSCRIBE, which blocks the connection). */
  duplicate(): Redis {
    const c = this.client.duplicate();
    this.extra.push(c);
    return c;
  }

  async onModuleDestroy() {
    await Promise.allSettled([this.client, ...this.extra].map((c) => c.quit()));
  }
}
