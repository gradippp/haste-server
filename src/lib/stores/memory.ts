import type { StorageConfig } from "@/lib/config";
import type { DocumentStore } from "./types";

/**
 * In-process store. Used by the test suite, and available as
 * `STORAGE_TYPE=memory` for throwaway instances — everything is lost when the
 * process exits.
 */
export class MemoryDocumentStore implements DocumentStore {
  private readonly data = new Map<string, { value: string; expiresAt?: number }>();
  private readonly expire?: number;

  constructor(options: StorageConfig = { type: "file" }) {
    this.expire = options.expire;
  }

  private expiresAt(skipExpire?: boolean): number | undefined {
    if (!this.expire || skipExpire) return undefined;
    return Date.now() + this.expire * 1000;
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    this.data.set(key, { value: data, expiresAt: this.expiresAt(skipExpire) });
    return true;
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    const entry = this.data.get(key);
    if (!entry) return null;

    if (entry.expiresAt !== undefined && entry.expiresAt <= Date.now()) {
      this.data.delete(key);
      return null;
    }

    if (entry.expiresAt !== undefined && !skipExpire) {
      entry.expiresAt = this.expiresAt(false);
    }

    return entry.value;
  }

  async del(key: string): Promise<boolean> {
    return this.data.delete(key);
  }

  /** Test helper. */
  clear() {
    this.data.clear();
  }
}

export default MemoryDocumentStore;
