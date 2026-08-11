import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

// Minimal surface of the `pg` package this store needs.
interface PgQueryResult<T> {
  rows: T[];
  rowCount: number | null;
}

interface PgClient {
  query<T = unknown>(text: string, values?: unknown[]): Promise<PgQueryResult<T>>;
  release(): void;
}

interface PgPool {
  connect(): Promise<PgClient>;
}

interface PgModule {
  Pool: new (config: { connectionString?: string }) => PgPool;
}

interface EntryRow {
  id: number;
  value: string;
  expiration: number | null;
}

/**
 * Stores documents in a `entries` table, auto-created on first connect.
 * `expiration` is a unix-seconds column; NULL means "never expires" — the
 * table is created lazily so a fresh database needs no manual migration.
 *
 * `pg` isn't a dependency of this package, so it's loaded through an
 * indirect dynamic import (see redis.ts for why the specifier goes through
 * a variable first).
 */
export class PostgresDocumentStore implements DocumentStore {
  private readonly expire?: number;
  private readonly options: StorageConfig;
  private poolPromise?: Promise<PgPool>;

  constructor(options: StorageConfig) {
    this.expire = options.expire;
    this.options = options;
  }

  private connect(): Promise<PgPool> {
    if (!this.poolPromise) {
      this.poolPromise = this.doConnect().catch((error) => {
        // Don't cache a failed connection — let the next request retry.
        this.poolPromise = undefined;
        throw error;
      });
    }
    return this.poolPromise;
  }

  private async doConnect(): Promise<PgPool> {
    const driverName = "pg";
    let mod: PgModule;
    try {
      mod = (await import(/* webpackIgnore: true */ driverName)) as unknown as PgModule;
    } catch {
      throw new Error(
        `postgres document store requires the "pg" package — run: pnpm add pg`
      );
    }

    const connectionString =
      process.env.DATABASE_URL || this.options.connectionUrl;
    const pool = new mod.Pool({ connectionString });

    const client = await pool.connect();
    try {
      await client.query(
        `CREATE TABLE IF NOT EXISTS entries (
           id serial primary key,
           key varchar(255) not null,
           value text not null,
           expiration int,
           unique(key)
         )`
      );
    } finally {
      client.release();
    }

    return pool;
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    const now = Math.floor(Date.now() / 1000);
    try {
      const pool = await this.connect();
      const client = await pool.connect();
      try {
        await client.query(
          "INSERT INTO entries (key, value, expiration) VALUES ($1, $2, $3)",
          [key, data, this.expire && !skipExpire ? this.expire + now : null]
        );
        return true;
      } finally {
        client.release();
      }
    } catch (error) {
      logger.error("error persisting value to postgres", { error });
      return false;
    }
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    const now = Math.floor(Date.now() / 1000);
    try {
      const pool = await this.connect();
      const client = await pool.connect();
      try {
        const result = await client.query<EntryRow>(
          "SELECT id,value,expiration from entries where KEY = $1 and (expiration IS NULL or expiration > $2)",
          [key, now]
        );
        if (!result.rows.length) return null;

        const row = result.rows[0];
        if (this.expire && !skipExpire) {
          await client.query("UPDATE entries SET expiration = $1 WHERE ID = $2", [
            this.expire + now,
            row.id,
          ]);
        }
        return row.value;
      } finally {
        client.release();
      }
    } catch (error) {
      logger.error("error retrieving value from postgres", { error });
      return null;
    }
  }

  async del(key: string): Promise<boolean> {
    try {
      const pool = await this.connect();
      const client = await pool.connect();
      try {
        const result = await client.query("DELETE FROM entries WHERE key = $1", [
          key,
        ]);
        return (result.rowCount ?? 0) > 0;
      } finally {
        client.release();
      }
    } catch (error) {
      logger.error("error deleting value from postgres", { error });
      return false;
    }
  }
}

export default PostgresDocumentStore;
