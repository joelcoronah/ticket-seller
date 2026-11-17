import { Controller, ForbiddenException, Get, HttpCode, Param, ParseBoolPipe, ParseIntPipe, Post, Query, UseGuards } from '@nestjs/common';
import { AdminGuard } from '../../common/admin.guard';
import { appConfig } from '../../config';
import { DatabaseService } from '../../infra/database/database.service';
import { CatalogService } from '../catalog/catalog.service';
import { InventoryService } from '../inventory/inventory.service';

@Controller('admin/events/:eventId')
@UseGuards(AdminGuard)
export class AdminController {
  constructor(
    private readonly catalog: CatalogService,
    private readonly inventory: InventoryService,
    private readonly db: DatabaseService,
  ) {}

  /**
   * Loads the inventory from PostgreSQL into Redis and opens the sale.
   * `reset=true` (only with ALLOW_RESET) also deletes the event's orders —
   * used by the load test to run repeatable experiments.
   */
  @Post('open-sale')
  @HttpCode(200)
  async openSale(
    @Param('eventId', ParseIntPipe) eventId: number,
    @Query('reset', new ParseBoolPipe({ optional: true })) reset = false,
  ) {
    if (reset) {
      if (!appConfig.allowReset) throw new ForbiddenException('ALLOW_RESET is disabled');
      await this.db.tx(async (c) => {
        await c.query('DELETE FROM tickets WHERE event_id = $1', [eventId]);
        await c.query('DELETE FROM orders WHERE event_id = $1', [eventId]);
      });
    }
    const { event, soldCodes } = await this.catalog.seatsForSale(eventId);
    await this.inventory.loadSale(eventId, event.seats, soldCodes);
    await this.db.query(`UPDATE events SET status = 'ON_SALE' WHERE id = $1`, [eventId]);
    return { eventId, capacity: event.seats.length, alreadySold: soldCodes.length };
  }

  @Post('close-sale')
  @HttpCode(200)
  async closeSale(@Param('eventId', ParseIntPipe) eventId: number) {
    await this.inventory.closeSale(eventId);
    await this.db.query(`UPDATE events SET status = 'CLOSED' WHERE id = $1`, [eventId]);
    return { eventId, status: 'CLOSED' };
  }

  /** Proof of correctness: Redis conservation law + PostgreSQL uniqueness. */
  @Get('audit')
  async audit(@Param('eventId', ParseIntPipe) eventId: number) {
    const [redis, outboxPending, pgRes] = await Promise.all([
      this.inventory.snapshot(eventId),
      this.inventory.outboxSize(eventId),
      this.db.query<{ tickets: string; distinct_seats: string; conflicts: string; revenue: string }>(
        `SELECT
           (SELECT count(*) FROM tickets WHERE event_id = $1) AS tickets,
           (SELECT count(DISTINCT seat_id) FROM tickets WHERE event_id = $1) AS distinct_seats,
           (SELECT count(*) FROM orders WHERE event_id = $1 AND status = 'CONFLICT') AS conflicts,
           (SELECT coalesce(sum(total_cents), 0) FROM orders WHERE event_id = $1 AND status = 'PAID') AS revenue`,
        [eventId],
      ),
    ]);
    const pg = {
      tickets: Number(pgRes.rows[0].tickets),
      distinctSeats: Number(pgRes.rows[0].distinct_seats),
      conflicts: Number(pgRes.rows[0].conflicts),
      revenueCents: Number(pgRes.rows[0].revenue),
    };
    return {
      eventId,
      redis,
      pg,
      outboxPending,
      invariants: {
        // available + held + sold must always equal capacity
        conservation: redis.available + redis.held + redis.sold === redis.capacity,
        noOversell:
          redis.sold <= redis.capacity &&
          pg.tickets <= redis.capacity &&
          pg.tickets === pg.distinctSeats &&
          pg.conflicts === 0,
        // once the outbox is drained, PG must match Redis exactly
        persisted: outboxPending === 0 ? pg.tickets === redis.sold : null,
      },
    };
  }
}
