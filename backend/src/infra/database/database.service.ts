import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, PoolClient, QueryResultRow } from 'pg';
import { appConfig } from '../../config';

/**
 * Thin wrapper over `pg`. Raw SQL on purpose: the interesting guarantees here
 * (UNIQUE constraints, ON CONFLICT, transactions) are clearer without an ORM.
 * PostgreSQL is NOT on the hot path: it only sees confirmed orders (<= capacity).
 */
@Injectable()
export class DatabaseService implements OnModuleDestroy {
  readonly pool = new Pool({ connectionString: appConfig.databaseUrl, max: 10 });

  query<T extends QueryResultRow = QueryResultRow>(sql: string, params: unknown[] = []) {
    return this.pool.query<T>(sql, params);
  }

  async tx<T>(fn: (c: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const out = await fn(client);
      await client.query('COMMIT');
      return out;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async onModuleDestroy() {
    await this.pool.end();
  }
}
