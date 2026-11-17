import { Module } from '@nestjs/common';
import { CatalogModule } from '../catalog/catalog.module';
import { InventoryModule } from '../inventory/inventory.module';
import { AdminController } from './admin.controller';

@Module({ imports: [CatalogModule, InventoryModule], controllers: [AdminController] })
export class AdminModule {}
