import { getConfig, type StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import { FileDocumentStore } from "./file";
import { MemoryDocumentStore } from "./memory";
import type { DocumentStore } from "./types";

export type { DocumentStore } from "./types";

/**
 * Builds the store named by `options.type`.
 *
 * Backends other than `file`/`memory` need a third-party driver that isn't a
 * dependency of this package — install only the one you use (see README).
 * Their modules are imported lazily so an uninstalled driver never breaks
 * startup for someone using a different backend.
 */
export async function createStore(
  options: StorageConfig
): Promise<DocumentStore> {
  switch (options.type) {
    case "memory":
      return new MemoryDocumentStore(options);
    case "file":
      return new FileDocumentStore(options);
    case "redis":
      return new (await import("./redis")).RedisDocumentStore(options);
    case "postgres":
      return new (await import("./postgres")).PostgresDocumentStore(options);
    case "mariadb":
      return new (await import("./mariadb")).MariaDbDocumentStore(options);
    case "mongo":
      return new (await import("./mongo")).MongoDocumentStore(options);
    case "memcached":
      return new (await import("./memcached")).MemcachedDocumentStore(options);
    case "rethinkdb":
      return new (await import("./rethinkdb")).RethinkDbDocumentStore(options);
    case "amazon-s3":
      return new (await import("./amazon-s3")).AmazonS3DocumentStore(options);
    case "google-datastore":
      return new (await import("./google-datastore")).GoogleDatastoreDocumentStore(
        options
      );
    default:
      throw new Error(`Unknown storage type: ${options.type}`);
  }
}

// Memoized as a promise so concurrent requests during cold start share one
// store (and one connection pool) rather than racing to build several.
let storePromise: Promise<DocumentStore> | null = null;

export function getStore(): Promise<DocumentStore> {
  if (!storePromise) {
    const { storage } = getConfig();
    logger.info(`using ${storage.type} document store`);
    storePromise = createStore(storage).catch((error) => {
      // Don't cache a failed construction — let the next request retry.
      storePromise = null;
      throw error;
    });
  }
  return storePromise;
}

/** Test-only: replace the process-wide store. */
export function setStore(store: DocumentStore | null) {
  storePromise = store ? Promise.resolve(store) : null;
}
