import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

// Minimal surface of the `@google-cloud/datastore` package this store needs.
interface DatastoreKey {
  __datastoreKey: true;
}

interface DatastoreEntity {
  value: string;
  expiration: Date | null;
}

interface DatastoreClient {
  key(path: [string, string]): DatastoreKey;
  insert(entity: {
    key: DatastoreKey;
    data: Array<{ name: string; value: unknown; excludeFromIndexes?: boolean }>;
  }): Promise<unknown>;
  update(entity: {
    key: DatastoreKey;
    data: Array<{ name: string; value: unknown; excludeFromIndexes?: boolean }>;
  }): Promise<unknown>;
  get(key: DatastoreKey): Promise<[DatastoreEntity | undefined]>;
  delete(key: DatastoreKey): Promise<unknown>;
}

interface DatastoreModule {
  Datastore: new () => DatastoreClient;
}

/**
 * Stores each document as a "Haste" entity keyed by the document key.
 * `expiration` is a nullable Date property — null means "never expires"
 * (matching the old store, which only set it when `expire` was configured).
 *
 * `@google-cloud/datastore` isn't a dependency of this package, so it's
 * loaded through an indirect dynamic import (see redis.ts for why the
 * specifier goes through a variable first). Credentials/project come from
 * the standard `GOOGLE_APPLICATION_CREDENTIALS` environment, same as the
 * old store.
 */
export class GoogleDatastoreDocumentStore implements DocumentStore {
  private readonly kind = "Haste";
  private readonly expire?: number;
  private clientPromise?: Promise<DatastoreClient>;

  constructor(options: StorageConfig) {
    this.expire = options.expire;
  }

  private connect(): Promise<DatastoreClient> {
    if (!this.clientPromise) {
      this.clientPromise = this.doConnect().catch((error) => {
        // Don't cache a failed connection — let the next request retry.
        this.clientPromise = undefined;
        throw error;
      });
    }
    return this.clientPromise;
  }

  private async doConnect(): Promise<DatastoreClient> {
    const driverName = "@google-cloud/datastore";
    let mod: DatastoreModule;
    try {
      mod = (await import(
        /* webpackIgnore: true */ driverName
      )) as unknown as DatastoreModule;
    } catch {
      throw new Error(
        `google-datastore document store requires the "@google-cloud/datastore" package — run: pnpm add @google-cloud/datastore`
      );
    }

    return new mod.Datastore();
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    try {
      const datastore = await this.connect();
      const taskKey = datastore.key([this.kind, key]);
      const expireTime =
        skipExpire || this.expire === undefined
          ? null
          : new Date(Date.now() + this.expire * 1000);

      await datastore.insert({
        key: taskKey,
        data: [
          { name: "value", value: data, excludeFromIndexes: true },
          { name: "expiration", value: expireTime },
        ],
      });
      return true;
    } catch (error) {
      logger.error("error persisting value to google datastore", { error });
      return false;
    }
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    try {
      const datastore = await this.connect();
      const taskKey = datastore.key([this.kind, key]);
      const [entity] = await datastore.get(taskKey);

      if (!entity) return null;

      if (skipExpire || entity.expiration == null) {
        return entity.value;
      }

      if (entity.expiration < new Date()) {
        logger.info("document expired", { key, expiration: entity.expiration });
        return null;
      }

      if (this.expire !== undefined) {
        datastore
          .update({
            key: taskKey,
            data: [
              { name: "value", value: entity.value, excludeFromIndexes: true },
              { name: "expiration", value: new Date(Date.now() + this.expire * 1000) },
            ],
          })
          .catch((error) => {
            logger.error("failed to update expiration", { error });
          });
      }

      return entity.value;
    } catch (error) {
      logger.error("error retrieving value from google datastore", { error });
      return null;
    }
  }

  async del(key: string): Promise<boolean> {
    try {
      const datastore = await this.connect();
      const taskKey = datastore.key([this.kind, key]);
      await datastore.delete(taskKey);
      return true;
    } catch (error) {
      logger.error("error deleting value from google datastore", { error });
      return false;
    }
  }
}

export default GoogleDatastoreDocumentStore;
