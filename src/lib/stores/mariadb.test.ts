import { beforeEach, describe, expect, it } from "vitest";
import { MariaDbDocumentStore } from "@/lib/stores/mariadb";

/**
 * A stand-in for `mysql2/promise` backed by a Map.
 *
 * The important detail is that `createConnection` returns a *promise* of a
 * connection, exactly as the real promise API does. An earlier version of
 * this store called it without awaiting and then invoked `.query()` on the
 * pending promise, which fails with "query is not a function" against a real
 * database — so modelling that asynchrony here is the whole point of this
 * fake, not an incidental detail.
 */
interface Row {
  value: string;
  expiration: number;
}

class FakeDriver {
  readonly rows = new Map<string, Row>();
  createdTable = false;
  openConnections = 0;
  queries: string[] = [];

  createConnection = async () => {
    this.openConnections += 1;
    return {
      query: async (sql: string, values: unknown[]) => {
        this.queries.push(sql.trim().split(/\s+/).slice(0, 2).join(" "));

        if (sql.includes("CREATE TABLE")) {
          this.createdTable = true;
          return [[], undefined] as [unknown, unknown];
        }

        if (sql.includes("INSERT INTO entries")) {
          const [key, value, expiration] = values as [string, string, number];
          const existing = this.rows.get(key);
          // Mirrors the ON DUPLICATE KEY UPDATE clause: an existing non-
          // sentinel expiration is only moved forward, never shortened.
          const now = values[3] as number;
          const nextExpiration =
            !existing || existing.expiration === -1 || existing.expiration > now
              ? expiration
              : existing.expiration;
          this.rows.set(key, { value, expiration: nextExpiration });
          return [{ affectedRows: 1 }, undefined] as [unknown, unknown];
        }

        if (sql.includes("SELECT value")) {
          const [key, now] = values as [string, number];
          const row = this.rows.get(key);
          if (!row || (row.expiration !== -1 && row.expiration <= now)) {
            return [[], undefined] as [unknown, unknown];
          }
          return [[row], undefined] as [unknown, unknown];
        }

        if (sql.includes("UPDATE entries")) {
          const [expiration, key] = values as [number, string];
          const row = this.rows.get(key);
          if (row) row.expiration = expiration;
          return [{ affectedRows: row ? 1 : 0 }, undefined] as [unknown, unknown];
        }

        if (sql.includes("DELETE FROM entries")) {
          const [key] = values as [string];
          const existed = this.rows.delete(key);
          return [{ affectedRows: existed ? 1 : 0 }, undefined] as [
            unknown,
            unknown,
          ];
        }

        throw new Error(`unexpected SQL: ${sql}`);
      },
      end: async () => {
        this.openConnections -= 1;
      },
    };
  };
}

class TestableStore extends MariaDbDocumentStore {
  constructor(
    public readonly driver: FakeDriver,
    expire?: number
  ) {
    super({ type: "mariadb", host: "db.test", expire });
  }

  protected async doLoadDriver() {
    return this.driver as unknown as Awaited<
      ReturnType<MariaDbDocumentStore["doLoadDriver"]>
    >;
  }
}

describe("MariaDbDocumentStore", () => {
  let driver: FakeDriver;
  let store: TestableStore;

  beforeEach(() => {
    // DB_* env vars take precedence over the passed options, so clear any
    // the surrounding environment happens to set.
    for (const key of [
      "DB_HOST",
      "DB_PORT",
      "DB_USER",
      "DB_PASSWORD",
      "DB_NAME",
    ]) {
      delete process.env[key];
    }
    driver = new FakeDriver();
    store = new TestableStore(driver);
  });

  it("round-trips a document", async () => {
    expect(await store.set("abc", "hello")).toBe(true);
    expect(await store.get("abc")).toBe("hello");
  });

  it("awaits the connection before querying it", async () => {
    // Regression guard: mysql2/promise's createConnection resolves a
    // connection rather than returning one. Without the await, `.query` is
    // read off a pending promise and set/get silently fail.
    await store.set("abc", "hello");

    expect(driver.createdTable).toBe(true);
    expect(await store.get("abc")).toBe("hello");
  });

  it("returns null for a missing key", async () => {
    expect(await store.get("nope")).toBeNull();
  });

  it("overwrites an existing key", async () => {
    await store.set("abc", "one");
    await store.set("abc", "two");
    expect(await store.get("abc")).toBe("two");
  });

  it("deletes a document", async () => {
    await store.set("abc", "hello");
    expect(await store.del("abc")).toBe(true);
    expect(await store.get("abc")).toBeNull();
  });

  it("reports false when deleting something that isn't there", async () => {
    expect(await store.del("nope")).toBe(false);
  });

  it("round-trips content containing JSON and multi-byte characters", async () => {
    // Values are JSON-encoded on the way in, so content that is itself JSON
    // is the case most likely to break the encoding.
    const content = '{"not":"really json"} — 世界 🎉';
    await store.set("uni", content);
    expect(await store.get("uni")).toBe(content);
  });

  it("closes every connection it opens", async () => {
    await store.set("abc", "hello");
    await store.get("abc");
    await store.del("abc");
    expect(driver.openConnections).toBe(0);
  });

  it("closes the connection even when a query fails", async () => {
    driver.createConnection = async () => {
      driver.openConnections += 1;
      return {
        query: async () => {
          throw new Error("connection reset");
        },
        end: async () => {
          driver.openConnections -= 1;
        },
      };
    };

    expect(await store.set("abc", "hello")).toBe(false);
    expect(driver.openConnections).toBe(0);
  });

  it("stores no expiry when none is configured", async () => {
    await store.set("abc", "hello");
    expect(driver.rows.get("abc")?.expiration).toBe(-1);
  });

  it("stores an expiry when one is configured", async () => {
    const expiring = new TestableStore(driver, 60);
    await expiring.set("abc", "hello");

    const expiration = driver.rows.get("abc")?.expiration ?? 0;
    const expected = Math.floor(Date.now() / 1000) + 60;
    expect(Math.abs(expiration - expected)).toBeLessThanOrEqual(2);
  });

  it("skips the expiry when asked to, so static documents never age out", async () => {
    const expiring = new TestableStore(driver, 60);
    await expiring.set("about", "hello", true);
    expect(driver.rows.get("about")?.expiration).toBe(-1);
  });

  it("treats an elapsed expiration as a miss", async () => {
    const expiring = new TestableStore(driver, 60);
    await expiring.set("abc", "hello");
    driver.rows.get("abc")!.expiration = Math.floor(Date.now() / 1000) - 1;

    expect(await expiring.get("abc")).toBeNull();
  });

  it("bumps the expiry on read unless skipExpire is passed", async () => {
    const expiring = new TestableStore(driver, 60);
    await expiring.set("abc", "hello");
    driver.rows.get("abc")!.expiration = Math.floor(Date.now() / 1000) + 5;

    await expiring.get("abc");
    const bumped = driver.rows.get("abc")!.expiration;
    expect(bumped).toBeGreaterThan(Math.floor(Date.now() / 1000) + 50);

    driver.rows.get("abc")!.expiration = Math.floor(Date.now() / 1000) + 5;
    await expiring.get("abc", true);
    expect(driver.rows.get("abc")!.expiration).toBeLessThan(
      Math.floor(Date.now() / 1000) + 10
    );
  });

  it("reads back a bare string written by the pre-rewrite server", async () => {
    // The old store JSON-encoded values the same way, so existing rows stay
    // readable — this is what protects live data across the upgrade.
    driver.rows.set("legacy", {
      value: JSON.stringify("plain old content"),
      expiration: -1,
    });

    expect(await store.get("legacy")).toBe("plain old content");
  });
});
