import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { asyncScheduler, finalize, Observable, share, Subject, throttleTime } from 'rxjs';
import type { RealtimeUpdate, ReserveResult } from '../../domain/types';
import { channels } from '../../infra/redis/keys';
import { RedisService } from '../../infra/redis/redis.service';

/**
 * API side: ONE Redis subscription per API instance, demultiplexed in memory to
 * thousands of SSE connections. Results are routed by requestId in O(1).
 */
@Injectable()
export class RealtimeService implements OnModuleInit {
  private readonly log = new Logger(RealtimeService.name);
  private readonly waiters = new Map<string, Subject<ReserveResult>>();
  private readonly progressIn = new Map<number, Subject<number>>();
  private readonly progressOut = new Map<number, Observable<number>>();

  constructor(private readonly redis: RedisService) {}

  async onModuleInit() {
    const sub = this.redis.duplicate();
    sub.on('pmessage', (_pattern, channel, message) => this.dispatch(channel, message));
    await sub.psubscribe(channels.updatesPattern);
  }

  /** Emits once when the worker publishes this request's result. */
  resultOf(requestId: string): Observable<ReserveResult> {
    let subject = this.waiters.get(requestId);
    if (!subject) {
      subject = new Subject<ReserveResult>();
      this.waiters.set(requestId, subject);
    }
    return subject.pipe(finalize(() => this.waiters.delete(requestId)));
  }

  /** Last processed queue position, throttled to 4 updates/s per event. */
  progressOf(eventId: number): Observable<number> {
    let out = this.progressOut.get(eventId);
    if (!out) {
      const input = new Subject<number>();
      this.progressIn.set(eventId, input);
      out = input.pipe(throttleTime(250, asyncScheduler, { leading: true, trailing: true }), share());
      this.progressOut.set(eventId, out);
    }
    return out;
  }

  private dispatch(channel: string, message: string) {
    const eventId = Number(channel.slice(channel.lastIndexOf(':') + 1));
    let update: RealtimeUpdate;
    try {
      update = JSON.parse(message) as RealtimeUpdate;
    } catch {
      this.log.warn(`Bad pub/sub payload on ${channel}`);
      return;
    }
    if (update.type === 'result') {
      const waiter = this.waiters.get(update.result.requestId);
      if (waiter) {
        waiter.next(update.result);
        waiter.complete();
      }
    } else if (update.type === 'progress') {
      this.progressIn.get(eventId)?.next(update.done);
    }
  }
}
