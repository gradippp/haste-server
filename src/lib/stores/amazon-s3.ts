import type { StorageConfig } from "@/lib/config";
import { logger } from "@/lib/logger";
import type { DocumentStore } from "./types";

// Minimal surface of the `@aws-sdk/client-s3` v3 package this store needs.
// The old store used aws-sdk v2's callback-based `getObject`/`putObject`;
// v3 is command-based and promise-native, which this ports to.
interface S3Body {
  transformToString(encoding?: string): Promise<string>;
}

interface GetObjectCommandOutput {
  Body?: S3Body;
}

interface S3Client {
  send(command: unknown): Promise<GetObjectCommandOutput | unknown>;
}

interface S3Module {
  S3Client: new (config: { region?: string }) => S3Client;
  GetObjectCommand: new (input: { Bucket: string; Key: string }) => unknown;
  PutObjectCommand: new (input: {
    Bucket: string;
    Key: string;
    Body: string;
    ContentType: string;
  }) => unknown;
  DeleteObjectCommand: new (input: { Bucket: string; Key: string }) => unknown;
}

/**
 * Stores each document as an object in an S3 bucket, keyed directly by the
 * document key. This backend has no expiry support (S3 object lifecycle
 * rules aren't per-key), matching the old store — a warning is logged once
 * if `expire` is configured but ignored.
 *
 * `@aws-sdk/client-s3` isn't a dependency of this package, so it's loaded
 * through an indirect dynamic import (see redis.ts for why the specifier
 * goes through a variable first).
 */
export class AmazonS3DocumentStore implements DocumentStore {
  private readonly expire?: number;
  private readonly bucket?: string;
  private readonly region?: string;
  private clientPromise?: Promise<{ client: S3Client; mod: S3Module }>;
  private warnedAboutExpiry = false;

  constructor(options: StorageConfig) {
    this.expire = options.expire;
    this.bucket = options.bucket;
    this.region = options.region;
  }

  private connect(): Promise<{ client: S3Client; mod: S3Module }> {
    if (!this.clientPromise) {
      this.clientPromise = this.doConnect().catch((error) => {
        // Don't cache a failed connection — let the next request retry.
        this.clientPromise = undefined;
        throw error;
      });
    }
    return this.clientPromise;
  }

  private async doConnect(): Promise<{ client: S3Client; mod: S3Module }> {
    const driverName = "@aws-sdk/client-s3";
    let mod: S3Module;
    try {
      mod = (await import(/* webpackIgnore: true */ driverName)) as unknown as S3Module;
    } catch {
      throw new Error(
        `amazon-s3 document store requires the "@aws-sdk/client-s3" package — run: pnpm add @aws-sdk/client-s3`
      );
    }

    return { client: new mod.S3Client({ region: this.region }), mod };
  }

  private warnExpiry(skipExpire?: boolean) {
    if (this.expire && !skipExpire && !this.warnedAboutExpiry) {
      this.warnedAboutExpiry = true;
      logger.warn("amazon s3 store cannot set expirations on keys");
    }
  }

  async get(key: string, skipExpire?: boolean): Promise<string | null> {
    try {
      const { client, mod } = await this.connect();
      const result = (await client.send(
        new mod.GetObjectCommand({ Bucket: this.bucket!, Key: key })
      )) as GetObjectCommandOutput;

      if (!result.Body) return null;

      const body = await result.Body.transformToString("utf-8");
      this.warnExpiry(skipExpire);
      return body;
    } catch (error) {
      logger.error("error retrieving value from s3", { error });
      return null;
    }
  }

  async set(key: string, data: string, skipExpire?: boolean): Promise<boolean> {
    try {
      const { client, mod } = await this.connect();
      await client.send(
        new mod.PutObjectCommand({
          Bucket: this.bucket!,
          Key: key,
          Body: data,
          ContentType: "text/plain",
        })
      );
      this.warnExpiry(skipExpire);
      return true;
    } catch (error) {
      logger.error("error persisting value to s3", { error });
      return false;
    }
  }

  async del(key: string): Promise<boolean> {
    try {
      const { client, mod } = await this.connect();
      await client.send(
        new mod.DeleteObjectCommand({ Bucket: this.bucket!, Key: key })
      );
      return true;
    } catch (error) {
      logger.error("error deleting value from s3", { error });
      return false;
    }
  }
}

export default AmazonS3DocumentStore;
