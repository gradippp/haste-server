import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

// Minimal surface of the `mongodb` package this store needs.
interface MongoUpdateResult {
  acknowledged: boolean;
}

interface MongoDeleteResult {
  deletedCount: number;
}

interface MongoEntry {
  entry_id: string;
  value: string;
  expiration: number;
}

interface MongoCollection {
  updateOne(
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options: { upsert: boolean }
  ): Promise<MongoUpdateResult>;
  findOne(filter: Record<string, unknown>): Promise<MongoEntry | null>;
  deleteOne(filter: Record<string, unknown>): Promise<MongoDeleteResult>;
}

interface MongoDb {
  collection(name: string): MongoCollection;
}

interface MongoClientInstance {
  connect(): Promise<MongoClientInstance>;
  db(): MongoDb;
}

interface MongoModule {
  MongoClient: new (url: string) => MongoClientInstance;
}

/**
 * Stores documents in an `entries` collection. `expiration = -1` is a
 * sentinel meaning "never expires", matching the postgres/mariadb stores.
 *
 * `mongodb` isn't a dependency of this package, so it's loaded through an
 * indirect dynamic import (see redis.ts for why the specifier goes through
 * a variable first). The connection is opened lazily and memoized so
 * concurrent requests share it.
 */
export class MongoDocumentStore implements DocumentStore {
  private readonly expire?: number;
  private readonly connectionUrl?: string;
  private dbPromise?: Promise<MongoDb>;

  constructor(options: StorageConfig) {
    this.expire = options.expire;
    this.connectionUrl = process.env.DATABASE_URL || options.connectionUrl;
  }

  private connect(): Promise<MongoDb> {
    if (!this.dbPromise) {
      this.dbPromise = this.doConnect().catch((error) => {
        // Don't cache a failed connection — let the next request retry.
        this.dbPromise = undefined;
        throw error;
      });
    }
    return this.dbPromise;
  }

  private async doConnect(): Promise<MongoDb> {
    const driverName = "mongodb";
    let mod: MongoModule;
    try {
      mod = (await import(/* webpackIgnore: true */ driverName)) as unknown as MongoModule;
    } catch {
      throw new Error(
        `mongo document store requires the "mongodb" package — run: pnpm add mongodb`
      );
    }

    if (!this.connectionUrl) {
      throw new Error("mongo document store requires a connectionUrl");
    }

    const client = new mod.MongoClient(this.connectionUrl);
    await client.connect();
    return client.db();
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    const now = Math.floor(Date.now() / 1000);
    try {
      const db = await this.connect();
      await db.collection("entries").updateOne(
        {
          entry_id: key,
          $or: [{ expiration: -1 }, { expiration: { $gt: now } }],
        },
        {
          $set: {
            entry_id: key,
            value: data,
            expiration: this.expire && !skipExpire ? this.expire + now : -1,
          },
        },
        { upsert: true }
      );
      return true;
    } catch (error) {
      logger.error("error persisting value to mongodb", { error });
      return false;
    }
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    const now = Math.floor(Date.now() / 1000);
    try {
      const db = await this.connect();
      const entry = await db.collection("entries").findOne({
        entry_id: key,
        $or: [{ expiration: -1 }, { expiration: { $gt: now } }],
      });

      if (entry === null) return null;

      if (entry.expiration !== -1 && this.expire && !skipExpire) {
        await db
          .collection("entries")
          .updateOne({ entry_id: key }, { $set: { expiration: this.expire + now } }, {
            upsert: false,
          });
      }

      return entry.value;
    } catch (error) {
      logger.error("error retrieving value from mongodb", { error });
      return null;
    }
  }

  async del(key: string): Promise<boolean> {
    try {
      const db = await this.connect();
      const result = await db.collection("entries").deleteOne({ entry_id: key });
      return result.deletedCount > 0;
    } catch (error) {
      logger.error("error deleting value from mongodb", { error });
      return false;
    }
  }
}

export default MongoDocumentStore;
