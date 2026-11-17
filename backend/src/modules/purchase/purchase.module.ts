import { Module } from '@nestjs/common';
import { InventoryModule } from '../inventory/inventory.module';
import { RealtimeModule } from '../realtime/realtime.module';
import { PurchaseController } from './purchase.controller';
import { PurchaseService } from './purchase.service';

@Module({
  imports: [InventoryModule, RealtimeModule],
  controllers: [PurchaseController],
  providers: [PurchaseService],
})
export class PurchaseModule {}
