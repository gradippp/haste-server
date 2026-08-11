import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

// Minimal surface of the `redis` v4 package that this store needs. Declared
// locally instead of importing the package's types because the package
// itself isn't a dependency (see the lazy import below).
interface RedisClient {
  connect(): Promise<unknown>;
  on(event: "error", listener: (err: unknown) => void): unknown;
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<unknown>;
  expire(key: string, seconds: number): Promise<unknown>;
  del(key: string): Promise<number>;
}

interface RedisModule {
  createClient(options: {
    socket: { host: string; port: number };
    database: number;
    password?: string;
  }): RedisClient;
}

/**
 * Stores documents as plain redis keys, optionally with a TTL.
 *
 * `redis` is not a dependency of this package (only installed if you use
 * this backend), so the driver is loaded through an indirect dynamic import
 * — assigning the specifier to a variable first stops TypeScript from trying
 * to resolve it at compile time.
 */
export class RedisDocumentStore implements DocumentStore {
  private readonly expire?: number;
  private readonly options: StorageConfig;
  private clientPromise?: Promise<RedisClient>;

  constructor(options: StorageConfig) {
    this.expire = options.expire;
    this.options = options;
  }

  // Connecting is deferred until the first request and memoized so
  // concurrent requests share a single client rather than racing to create
  // several.
  private connect(): Promise<RedisClient> {
    if (!this.clientPromise) {
      this.clientPromise = this.doConnect().catch((error) => {
        // Don't cache a failed connection — let the next request retry.
        this.clientPromise = undefined;
        throw error;
      });
    }
    return this.clientPromise;
  }

  private async doConnect(): Promise<RedisClient> {
    const host = this.options.host || "127.0.0.1";
    const port = this.options.port || 6379;
    // STORAGE_DB arrives from the environment as a string; for redis it's a
    // numeric database index.
    const db = Number(this.options.db) || 0;

    const driverName = "redis";
    let mod: RedisModule;
    try {
      mod = (await import(/* webpackIgnore: true */ driverName)) as unknown as RedisModule;
    } catch {
      throw new Error(
        `redis document store requires the "redis" package — run: pnpm add redis`
      );
    }

    const client = mod.createClient({
      socket: { host, port },
      database: db,
      password: this.options.password,
    });

    client.on("error", (err) => {
      logger.error("redis disconnected", { error: err });
    });

    await client.connect();
    logger.info(`connected to redis on ${host}:${port}/${db}`);
    return client;
  }

  private async setExpiration(client: RedisClient, key: string): Promise<void> {
    if (this.expire) {
      try {
        await client.expire(key, this.expire);
      } catch {
        logger.error(`failed to set expiry on key: ${key}`);
      }
    }
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    try {
      const client = await this.connect();
      await client.set(key, data);
      if (!skipExpire) {
        await this.setExpiration(client, key);
      }
      return true;
    } catch (error) {
      logger.error("error persisting value to redis", { error });
      return false;
    }
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    try {
      const client = await this.connect();
      const reply = await client.get(key);
      if (reply !== null && !skipExpire) {
        await this.setExpiration(client, key);
      }
      return reply;
    } catch (error) {
      logger.error("error retrieving value from redis", { error });
      return null;
    }
  }

  async del(key: string): Promise<boolean> {
    try {
      const client = await this.connect();
      const removed = await client.del(key);
      return removed > 0;
    } catch (error) {
      logger.error("error deleting value from redis", { error });
      return false;
    }
  }
}

export default RedisDocumentStore;
