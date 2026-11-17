import { Module } from '@nestjs/common';
import { DatabaseModule } from './infra/database/database.module';
import { KafkaModule } from './infra/kafka/kafka.module';
import { RedisModule } from './infra/redis/redis.module';
import { InventoryModule } from './modules/inventory/inventory.module';
import { RealtimePublisherModule } from './modules/realtime/realtime.module';
import { MaintenanceService } from './workers/maintenance.service';
import { OrderPersisterConsumer } from './workers/order-persister.consumer';
import { PurchaseConsumer } from './workers/purchase.consumer';

@Module({
  imports: [RedisModule, KafkaModule, DatabaseModule, InventoryModule, RealtimePublisherModule],
  providers: [PurchaseConsumer, OrderPersisterConsumer, MaintenanceService],
})
export class WorkerModule {}
