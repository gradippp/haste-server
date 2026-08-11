import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

/**
 * Stores each document in a file named after the md5 of its key.
 *
 * Hashing the key (rather than using it as the filename directly) is what the
 * original store did and it doubles as defence in depth against a key that
 * contains path separators — though `assertValidKey` already rejects those
 * before a key reaches here.
 *
 * This backend cannot expire documents; `expire` is warned about and ignored,
 * matching the old behavior.
 */
export class FileDocumentStore implements DocumentStore {
  private readonly basePath: string;
  private readonly expire?: number;
  private warnedAboutExpiry = false;

  constructor(options: StorageConfig) {
    this.basePath = options.path || "./data";
    this.expire = options.expire;
  }

  private fileFor(key: string): string {
    return path.join(this.basePath, FileDocumentStore.md5(key));
  }

  private warnExpiry(skipExpire?: boolean) {
    if (this.expire && !skipExpire && !this.warnedAboutExpiry) {
      this.warnedAboutExpiry = true;
      logger.warn("file store cannot set expirations on keys");
    }
  }

  static md5(str: string): string {
    return crypto.createHash("md5").update(str).digest("hex");
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    try {
      await fs.mkdir(this.basePath, { recursive: true, mode: 0o700 });
      await fs.writeFile(this.fileFor(key), data, "utf8");
      this.warnExpiry(skipExpire);
      return true;
    } catch (error) {
      logger.error("error persisting document to file", { error });
      return false;
    }
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    try {
      const data = await fs.readFile(this.fileFor(key), "utf8");
      this.warnExpiry(skipExpire);
      return data;
    } catch {
      return null;
    }
  }

  async del(key: string): Promise<boolean> {
    try {
      await fs.unlink(this.fileFor(key));
      return true;
    } catch {
      return false;
    }
  }
}

export default FileDocumentStore;
