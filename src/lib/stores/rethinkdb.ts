import crypto from "crypto";
import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

// Minimal surface of the `rethinkdbdash` package this store needs.
interface RethinkRunnable<T> {
  run(): Promise<T>;
}

interface RethinkTable {
  insert(doc: Record<string, unknown>): RethinkRunnable<unknown>;
  get(id: string): {
    run(): Promise<{ id: string; data: string } | null>;
    delete(): RethinkRunnable<{ deleted: number }>;
  };
}

interface RethinkClient {
  table(name: string): RethinkTable;
}

interface RethinkModule {
  (options: {
    silent: boolean;
    host: string;
    port: number;
    db: string;
    user: string;
    password: string;
  }): RethinkClient;
}

function md5(str: string): string {
  return crypto.createHash("md5").update(str).digest("hex");
}

/**
 * Stores each document as a row in the `uploads` table, keyed by the md5 of
 * the document key (matching the old store).
 *
 * `rethinkdbdash` isn't a dependency of this package, so it's loaded through
 * an indirect dynamic import (see redis.ts for why the specifier goes
 * through a variable first). This backend has no expiry support, matching
 * the old store — `expire` is simply unused.
 */
export class RethinkDbDocumentStore implements DocumentStore {
  private readonly options: StorageConfig;
  private clientPromise?: Promise<RethinkClient>;

  constructor(options: StorageConfig) {
    this.options = options;
  }

  private connect(): Promise<RethinkClient> {
    if (!this.clientPromise) {
      this.clientPromise = this.doConnect().catch((error) => {
        // Don't cache a failed connection — let the next request retry.
        this.clientPromise = undefined;
        throw error;
      });
    }
    return this.clientPromise;
  }

  private async doConnect(): Promise<RethinkClient> {
    const driverName = "rethinkdbdash";
    let rethink: RethinkModule;
    try {
      rethink = (
        await import(/* webpackIgnore: true */ driverName)
      ) as unknown as RethinkModule;
    } catch {
      throw new Error(
        `rethinkdb document store requires the "rethinkdbdash" package — run: pnpm add rethinkdbdash`
      );
    }

    return rethink({
      silent: true,
      host: this.options.host || "127.0.0.1",
      port: this.options.port || 28015,
      db: (this.options.db as string) || "haste",
      user: this.options.user || "admin",
      password: this.options.password || "",
    });
  }

  async set(key: string, data: string): Promise<boolean> {
    try {
      const client = await this.connect();
      await client.table("uploads").insert({ id: md5(key), data }).run();
      return true;
    } catch (error) {
      logger.error("failed to insert to table", { error });
      return false;
    }
  }

  async get(key: string): Promise<string | null> {
    try {
      const client = await this.connect();
      const result = await client.table("uploads").get(md5(key)).run();
      return result ? result.data : null;
    } catch (error) {
      logger.error("failed to read from table", { error });
      return null;
    }
  }

  async del(key: string): Promise<boolean> {
    try {
      const client = await this.connect();
      const result = await client.table("uploads").get(md5(key)).delete().run();
      return result.deleted > 0;
    } catch (error) {
      logger.error("failed to delete from table", { error });
      return false;
    }
  }
}

export default RethinkDbDocumentStore;
