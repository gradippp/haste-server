import fs from "fs";
import os from "os";
import path from "path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { FileDocumentStore } from "@/lib/stores/file";
import { MemoryDocumentStore } from "@/lib/stores/memory";

describe("FileDocumentStore", () => {
  let basePath: string;
  let store: FileDocumentStore;

  beforeEach(() => {
    basePath = fs.mkdtempSync(path.join(os.tmpdir(), "haste-store-"));
    store = new FileDocumentStore({ type: "file", path: basePath });
  });

  afterEach(() => {
    fs.rmSync(basePath, { recursive: true, force: true });
  });

  it("round-trips a document", async () => {
    expect(await store.set("abc", "hello")).toBe(true);
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

  it("names files after the md5 of the key, not the key itself", async () => {
    await store.set("abc", "hello");
    const files = fs.readdirSync(basePath);
    expect(files).toEqual([FileDocumentStore.md5("abc")]);
  });

  it("creates its base directory on demand", async () => {
    const nested = path.join(basePath, "deep", "nested");
    const nestedStore = new FileDocumentStore({ type: "file", path: nested });
    expect(await nestedStore.set("abc", "hi")).toBe(true);
    expect(await nestedStore.get("abc")).toBe("hi");
  });

  it("round-trips multi-byte content", async () => {
    const content = "héllo — 世界 🎉";
    await store.set("uni", content);
    expect(await store.get("uni")).toBe(content);
  });
});

describe("MemoryDocumentStore", () => {
  it("round-trips and deletes", async () => {
    const store = new MemoryDocumentStore();
    await store.set("abc", "hello");
    expect(await store.get("abc")).toBe("hello");
    expect(await store.del("abc")).toBe(true);
    expect(await store.get("abc")).toBeNull();
  });

  it("expires documents once past their TTL", async () => {
    const store = new MemoryDocumentStore({ type: "memory", expire: 1 });
    await store.set("abc", "hello");
    expect(await store.get("abc")).toBe("hello");

    // Rewind the clock rather than waiting a real second.
    const realNow = Date.now;
    Date.now = () => realNow() + 2000;
    try {
      expect(await store.get("abc")).toBeNull();
    } finally {
      Date.now = realNow;
    }
  });

  it("does not set an expiry when skipExpire is passed", async () => {
    const store = new MemoryDocumentStore({ type: "memory", expire: 1 });
    await store.set("static", "hello", true);

    const realNow = Date.now;
    Date.now = () => realNow() + 10_000;
    try {
      expect(await store.get("static")).toBe("hello");
    } finally {
      Date.now = realNow;
    }
  });
});
