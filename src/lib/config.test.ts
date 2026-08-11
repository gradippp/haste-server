import { describe, expect, it } from "vitest";
import { buildConfig } from "@/lib/config";

// buildConfig takes both the env and the parsed config.json so it can be
// exercised without touching either the real environment or the filesystem.
const noEnv: Record<string, string | undefined> = {};

describe("buildConfig", () => {
  it("falls back to defaults with no env and no file", () => {
    const config = buildConfig(noEnv, {});
    expect(config.keyLength).toBe(10);
    expect(config.maxLength).toBe(400000);
    expect(config.storage.type).toBe("file");
    expect(config.keyGenerator.type).toBe("random");
    expect(config.rateLimits).toBeNull();
    expect(config.documents).toEqual({});
  });

  it("reads values from config.json", () => {
    const config = buildConfig(noEnv, {
      keyLength: 4,
      maxLength: 100,
      storage: { type: "redis", host: "db", port: 6379, expire: 60 },
      keyGenerator: { type: "phonetic" },
      documents: { about: "./about.md" },
    });

    expect(config.keyLength).toBe(4);
    expect(config.maxLength).toBe(100);
    expect(config.storage).toMatchObject({
      type: "redis",
      host: "db",
      port: 6379,
      expire: 60,
    });
    expect(config.keyGenerator.type).toBe("phonetic");
    expect(config.documents).toEqual({ about: "./about.md" });
  });

  it("lets environment variables override the file", () => {
    const config = buildConfig(
      {
        KEY_LENGTH: "16",
        MAX_LENGTH: "999",
        STORAGE_TYPE: "postgres",
        STORAGE_HOST: "pg",
        STORAGE_PORT: "5432",
        STORAGE_EXPIRE_SECONDS: "30",
        KEYGENERATOR_TYPE: "phonetic",
        DOCUMENTS: "about=./about.md, help=./help.md",
      },
      {
        keyLength: 4,
        storage: { type: "file" },
        keyGenerator: { type: "random" },
        documents: { ignored: "./ignored.md" },
      }
    );

    expect(config.keyLength).toBe(16);
    expect(config.maxLength).toBe(999);
    expect(config.storage.type).toBe("postgres");
    expect(config.storage.host).toBe("pg");
    expect(config.storage.port).toBe(5432);
    expect(config.storage.expire).toBe(30);
    expect(config.keyGenerator.type).toBe("phonetic");
    expect(config.documents).toEqual({
      about: "./about.md",
      help: "./help.md",
    });
  });

  it("ignores empty and non-numeric env values instead of producing NaN", () => {
    const config = buildConfig(
      { KEY_LENGTH: "", MAX_LENGTH: "abc" },
      { keyLength: 7 }
    );
    expect(config.keyLength).toBe(7);
    expect(config.maxLength).toBe(400000);
  });

  it("builds rate limits from the file", () => {
    const config = buildConfig(noEnv, {
      rateLimits: {
        whitelist: [],
        blacklist: [],
        categories: { normal: { totalRequests: 500, every: 60000 } },
      },
    });

    expect(config.rateLimits?.categories.normal).toEqual({
      totalRequests: 500,
      every: 60000,
    });
  });

  it("builds rate limits from the environment, including IP lists", () => {
    const config = buildConfig(
      {
        RATELIMITS_NORMAL_TOTAL_REQUESTS: "10",
        RATELIMITS_NORMAL_EVERY_MILLISECONDS: "1000",
        RATELIMITS_WHITELIST: "1.2.3.4, 5.6.7.8",
      },
      {}
    );

    expect(config.rateLimits?.categories.normal).toEqual({
      totalRequests: 10,
      every: 1000,
    });
    expect(config.rateLimits?.whitelist).toEqual(["1.2.3.4", "5.6.7.8"]);
  });

  it("leaves rate limiting off when only half a category is configured", () => {
    const config = buildConfig(
      { RATELIMITS_NORMAL_TOTAL_REQUESTS: "10" },
      {}
    );
    expect(config.rateLimits).toBeNull();
  });
});
