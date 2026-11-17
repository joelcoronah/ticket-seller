import { Controller, Get, Module } from '@nestjs/common';
import { DatabaseModule } from './infra/database/database.module';
import { KafkaModule } from './infra/kafka/kafka.module';
import { RedisModule } from './infra/redis/redis.module';
import { AdminModule } from './modules/admin/admin.module';
import { CatalogModule } from './modules/catalog/catalog.module';
import { CheckoutModule } from './modules/checkout/checkout.module';
import { PurchaseModule } from './modules/purchase/purchase.module';

@Controller('health')
class HealthController {
  @Get()
  ok() {
    return { status: 'ok' };
  }
}

@Module({
  imports: [RedisModule, KafkaModule, DatabaseModule, CatalogModule, PurchaseModule, CheckoutModule, AdminModule],
  controllers: [HealthController],
})
export class ApiModule {}
