import { Module } from '@nestjs/common';
import { RealtimePublisher } from './realtime.publisher';
import { RealtimeService } from './realtime.service';

/** API imports RealtimeService (subscriber); workers only use RealtimePublisher. */
@Module({ providers: [RealtimeService], exports: [RealtimeService] })
export class RealtimeModule {}

@Module({ providers: [RealtimePublisher], exports: [RealtimePublisher] })
export class RealtimePublisherModule {}
