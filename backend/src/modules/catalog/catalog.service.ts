import { Injectable, NotFoundException } from '@nestjs/common';
import { DatabaseService } from '../../infra/database/database.service';
import { InventoryService, InventorySnapshot } from '../inventory/inventory.service';

interface SeatRow {
  code: string;
  section: string;
  row_label: string;
  seat_number: number;
  price_cents: number;
}

export interface EventCatalog {
  id: number;
  slug: string;
  name: string;
  venue: string;
  startsAt: string;
  saleOpensAt: string;
  seats: { code: string; section: string; row: string; number: number; priceCents: number }[];
}

/** Tiny per-instance TTL cache so a 5k-user waiting room doesn't hammer PG / Redis. */
class TtlCache<T> {
  private readonly m = new Map<number, { at: number; v: Promise<T> }>();
  constructor(private readonly ttlMs: number) {}
  get(key: number, load: () => Promise<T>): Promise<T> {
    const hit = this.m.get(key);
    if (hit && Date.now() - hit.at < this.ttlMs) return hit.v;
    const v = load().catch((e) => {
      this.m.delete(key);
      throw e;
    });
    this.m.set(key, { at: Date.now(), v });
    return v;
  }
}

@Injectable()
export class CatalogService {
  private readonly catalogCache = new TtlCache<EventCatalog>(30_000);
  private readonly availabilityCache = new TtlCache<InventorySnapshot & { availableSeats: string[] }>(500);

  constructor(
    private readonly db: DatabaseService,
    private readonly inventory: InventoryService,
  ) {}

  getEvent(eventId: number): Promise<EventCatalog> {
    return this.catalogCache.get(eventId, () => this.loadEvent(eventId));
  }

  availability(eventId: number) {
    return this.availabilityCache.get(eventId, async () => {
      const [snap, availableSeats] = await Promise.all([
        this.inventory.snapshot(eventId),
        this.inventory.availableSeats(eventId),
      ]);
      return { ...snap, availableSeats };
    });
  }

  async seatsForSale(eventId: number) {
    const event = await this.loadEvent(eventId);
    const sold = await this.db.query<{ code: string }>(
      `SELECT s.code FROM tickets t JOIN seats s ON s.id = t.seat_id WHERE t.event_id = $1`,
      [eventId],
    );
    return { event, soldCodes: sold.rows.map((r) => r.code) };
  }

  private async loadEvent(eventId: number): Promise<EventCatalog> {
    const ev = await this.db.query(
      `SELECT id, slug, name, venue, starts_at, sale_opens_at FROM events WHERE id = $1`,
      [eventId],
    );
    if (ev.rowCount === 0) throw new NotFoundException({ code: 'EVENT_NOT_FOUND' });
    const seats = await this.db.query<SeatRow>(
      `SELECT code, section, row_label, seat_number, price_cents FROM seats WHERE event_id = $1 ORDER BY code`,
      [eventId],
    );
    const e = ev.rows[0];
    return {
      id: Number(e.id),
      slug: e.slug,
      name: e.name,
      venue: e.venue,
      startsAt: e.starts_at,
      saleOpensAt: e.sale_opens_at,
      seats: seats.rows.map((s) => ({
        code: s.code,
        section: s.section,
        row: s.row_label,
        number: s.seat_number,
        priceCents: s.price_cents,
      })),
    };
  }
}
