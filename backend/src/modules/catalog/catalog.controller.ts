import { Controller, Get, Param, ParseIntPipe } from '@nestjs/common';
import { CatalogService } from './catalog.service';

@Controller('events')
export class CatalogController {
  constructor(private readonly catalog: CatalogService) {}

  @Get(':eventId')
  event(@Param('eventId', ParseIntPipe) eventId: number) {
    return this.catalog.getEvent(eventId);
  }

  /** Live counters + free seat codes for the seat map (cached 500 ms per instance). */
  @Get(':eventId/availability')
  availability(@Param('eventId', ParseIntPipe) eventId: number) {
    return this.catalog.availability(eventId);
  }
}
