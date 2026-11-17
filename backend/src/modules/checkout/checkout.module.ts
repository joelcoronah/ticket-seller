import { Module } from '@nestjs/common';
import { InventoryModule } from '../inventory/inventory.module';
import { CheckoutController } from './checkout.controller';
import { CheckoutService } from './checkout.service';
import { PaymentsService } from './payments.service';

@Module({
  imports: [InventoryModule],
  controllers: [CheckoutController],
  providers: [CheckoutService, PaymentsService],
})
export class CheckoutModule {}
