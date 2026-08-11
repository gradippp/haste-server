import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

// Minimal surface of the `memcached` package this store needs.
interface MemcachedClient {
  on(event: "failure", listener: (error: unknown) => void): unknown;
  get(key: string, callback: (error: unknown, data: string | undefined) => void): void;
  set(
    key: string,
    value: string,
    lifetime: number,
    callback: (error: unknown) => void
  ): void;
  del(key: string, callback: (error: unknown, ok: boolean) => void): void;
}

interface MemcachedModule {
  new (url: string): MemcachedClient;
}

/**
 * Stores documents as plain memcached keys. Like the old store, a
 * successful GET bumps the expiry by re-issuing the SET — memcached has no
 * "touch and keep value" primitive available here, so this rewrites the
 * value it just read.
 *
 * `memcached` isn't a dependency of this package, so it's loaded through an
 * indirect dynamic import (see redis.ts for why the specifier goes through
 * a variable first).
 */
export class MemcachedDocumentStore implements DocumentStore {
  private readonly expire?: number;
  private readonly options: StorageConfig;
  private clientPromise?: Promise<MemcachedClient>;

  constructor(options: StorageConfig) {
    this.expire = options.expire;
    this.options = options;
  }

  private connect(): Promise<MemcachedClient> {
    if (!this.clientPromise) {
      this.clientPromise = this.doConnect().catch((error) => {
        // Don't cache a failed connection — let the next request retry.
        this.clientPromise = undefined;
        throw error;
      });
    }
    return this.clientPromise;
  }

  private async doConnect(): Promise<MemcachedClient> {
    const driverName = "memcached";
    let Memcached: MemcachedModule;
    try {
      Memcached = (
        await import(/* webpackIgnore: true */ driverName)
      ).default as unknown as MemcachedModule;
    } catch {
      throw new Error(
        `memcached document store requires the "memcached" package — run: pnpm add memcached`
      );
    }

    const host = this.options.host || "127.0.0.1";
    const port = this.options.port || 11211;
    const url = `${host}:${port}`;

    logger.info(`connecting to memcached on ${url}`);
    const client = new Memcached(url);
    client.on("failure", (error) => {
      logger.error("error connecting to memcached", { error });
    });
    return client;
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    try {
      const client = await this.connect();
      return await new Promise<boolean>((resolve) => {
        client.set(key, data, skipExpire ? 0 : this.expire || 0, (error) => {
          resolve(!error);
        });
      });
    } catch (error) {
      logger.error("error persisting value to memcached", { error });
      return false;
    }
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    try {
      const client = await this.connect();
      const value = await new Promise<string | null>((resolve) => {
        client.get(key, (error, data) => {
          resolve(error || data === undefined ? null : data);
        });
      });

      // Update the key so that the expiration is pushed forward.
      if (value !== null && !skipExpire) {
        const updated = await this.set(key, value, skipExpire);
        if (!updated) {
          logger.error("failed to update expiration on GET", { key });
        }
      }

      return value;
    } catch (error) {
      logger.error("error retrieving value from memcached", { error });
      return null;
    }
  }

  async del(key: string): Promise<boolean> {
    try {
      const client = await this.connect();
      return await new Promise<boolean>((resolve) => {
        client.del(key, (error, ok) => {
          resolve(!error && !!ok);
        });
      });
    } catch (error) {
      logger.error("error deleting value from memcached", { error });
      return false;
    }
  }
}

export default MemcachedDocumentStore;
