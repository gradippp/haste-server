import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

// Minimal surface of the `mysql2` package this store needs.
interface MySqlConnection {
  query(sql: string, values: unknown[]): Promise<[unknown, unknown]>;
  end(): Promise<void>;
}

interface MySqlModule {
  createConnection(config: {
    host?: string;
    user?: string;
    port: number;
    password?: string;
    database?: string;
  }): MySqlConnection;
}

interface EntryRow {
  value: string;
  expiration: number;
}

/**
 * Stores documents in a `entries` table, auto-created on first connect.
 * `expiration = -1` is a sentinel meaning "never expires" (rather than NULL,
 * because the old store used it that way and it keeps the column usable in
 * an index/comparison without NULL-handling).
 *
 * `mysql2` isn't a dependency of this package, so it's loaded through an
 * indirect dynamic import (see redis.ts for why the specifier goes through
 * a variable first). A fresh connection is opened per operation, matching
 * the old store's behavior of connecting, querying, and closing each time.
 */
export class MariaDbDocumentStore implements DocumentStore {
  private readonly expire?: number;
  private readonly connectionConfig: {
    host?: string;
    user?: string;
    port: number;
    password?: string;
    database?: string;
  };
  private modPromise?: Promise<MySqlModule>;

  constructor(options: StorageConfig) {
    this.expire = options.expire;
    this.connectionConfig = {
      host: process.env.DB_HOST || options.host,
      user: process.env.DB_USER || options.user,
      port: Number(process.env.DB_PORT) || options.port || 3306,
      password: process.env.DB_PASSWORD || options.password,
      database: process.env.DB_NAME || options.database,
    };
  }

  private loadDriver(): Promise<MySqlModule> {
    if (!this.modPromise) {
      this.modPromise = this.doLoadDriver().catch((error) => {
        // Don't cache a failed connection — let the next request retry.
        this.modPromise = undefined;
        throw error;
      });
    }
    return this.modPromise;
  }

  private async doLoadDriver(): Promise<MySqlModule> {
    const driverName = "mysql2/promise";
    try {
      return (await import(
        /* webpackIgnore: true */ driverName
      )) as unknown as MySqlModule;
    } catch {
      throw new Error(
        `mariadb document store requires the "mysql2" package — run: pnpm add mysql2`
      );
    }
  }

  private async safeConnect(): Promise<MySqlConnection> {
    const mod = await this.loadDriver();
    const connection = mod.createConnection(this.connectionConfig);

    await connection.query(
      `CREATE TABLE IF NOT EXISTS entries (
         entry_id   VARCHAR(255) PRIMARY KEY,
         value      LONGTEXT NOT NULL,
         expiration INT NOT NULL,
         INDEX idx_expiration (expiration)
       )`,
      []
    );

    return connection;
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    const now = Math.floor(Date.now() / 1000);
    const expiration = this.expire && !skipExpire ? this.expire + now : -1;

    let connection: MySqlConnection | undefined;
    try {
      connection = await this.safeConnect();
      await connection.query(
        `INSERT INTO entries (entry_id, value, expiration)
         VALUES (?, ?, ?)
         ON DUPLICATE KEY UPDATE
             value = VALUES(value),
             expiration =
                 CASE
                     WHEN expiration = -1 OR expiration > ?
                     THEN VALUES(expiration)
                     ELSE expiration
                 END`,
        [key, JSON.stringify(data), expiration, now]
      );
      return true;
    } catch (error) {
      logger.error("error persisting value to mysql", { error });
      return false;
    } finally {
      await connection?.end();
    }
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    const now = Math.floor(Date.now() / 1000);

    let connection: MySqlConnection | undefined;
    try {
      connection = await this.safeConnect();
      const [rows] = await connection.query(
        `SELECT value, expiration
         FROM entries
         WHERE entry_id = ?
           AND (expiration = -1 OR expiration > ?)
         LIMIT 1`,
        [key, now]
      );

      const result = rows as EntryRow[];
      if (!result.length) return null;

      const entry = result[0];
      const value = JSON.parse(entry.value) as string;

      if (entry.expiration !== -1 && this.expire && !skipExpire) {
        await connection.query("UPDATE entries SET expiration = ? WHERE entry_id = ?", [
          this.expire + now,
          key,
        ]);
      }

      return value;
    } catch (error) {
      logger.error("error fetching value from mysql", { error });
      return null;
    } finally {
      await connection?.end();
    }
  }

  async del(key: string): Promise<boolean> {
    let connection: MySqlConnection | undefined;
    try {
      connection = await this.safeConnect();
      const [result] = await connection.query("DELETE FROM entries WHERE entry_id = ?", [
        key,
      ]);
      return (result as { affectedRows: number }).affectedRows > 0;
    } catch (error) {
      logger.error("error deleting value from mysql", { error });
      return false;
    } finally {
      await connection?.end();
    }
  }
}

export default MariaDbDocumentStore;
