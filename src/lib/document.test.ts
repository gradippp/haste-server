import { describe, expect, it } from "vitest";
import {
  generateRevocationKey,
  hashRevocationKey,
  isValidKey,
  normalizeKey,
  parseStored,
  revocationKeyMatches,
  serializeStored,
} from "@/lib/document";

describe("isValidKey", () => {
  it("accepts generated key shapes", () => {
    expect(isValidKey("abcdefghij")).toBe(true);
    expect(isValidKey("aB3_-xyz")).toBe(true);
    expect(isValidKey("about")).toBe(true);
  });

  it("rejects anything that could escape a storage path", () => {
    expect(isValidKey("..")).toBe(false);
    expect(isValidKey("../etc/passwd")).toBe(false);
    expect(isValidKey("a/b")).toBe(false);
    expect(isValidKey("a\\b")).toBe(false);
    expect(isValidKey("")).toBe(false);
    expect(isValidKey("a".repeat(65))).toBe(false);
  });
});

describe("normalizeKey", () => {
  it("strips a file extension the way the old routes did", () => {
    expect(normalizeKey("abc123.js")).toBe("abc123");
    expect(normalizeKey("abc123")).toBe("abc123");
  });

  it("returns null for keys that fail validation", () => {
    expect(normalizeKey("../../etc/passwd")).toBeNull();
    expect(normalizeKey("")).toBeNull();
  });

  it("rejects percent-encoded traversal attempts", () => {
    // %2e%2e%2f decodes to "../"
    expect(normalizeKey("%2e%2e%2fsecret")).toBeNull();
  });
});

describe("parseStored", () => {
  it("round-trips a v1 envelope", () => {
    const doc = {
      content: "print('hi')",
      language: "python",
      title: "My paste",
      created_at: 1700000000000,
      revocation_hash: "deadbeef",
    };
    expect(parseStored(serializeStored(doc))).toEqual(doc);
  });

  it("treats a legacy bare string as the body", () => {
    expect(parseStored("just some text")).toEqual({
      content: "just some text",
      created_at: 0,
    });
  });

  it("treats JSON that isn't a v1 envelope as a bare string", () => {
    const raw = '{"some":"other json"}';
    expect(parseStored(raw)).toEqual({ content: raw, created_at: 0 });
  });

  it("treats a v1 envelope with a non-string content as a bare string", () => {
    const raw = '{"v":1,"content":42}';
    expect(parseStored(raw)).toEqual({ content: raw, created_at: 0 });
  });

  it("drops empty optional fields rather than returning empty strings", () => {
    const raw = JSON.stringify({
      v: 1,
      content: "x",
      language: "",
      title: "",
      created_at: 5,
    });
    expect(parseStored(raw)).toEqual({
      content: "x",
      language: undefined,
      title: undefined,
      created_at: 5,
      revocation_hash: undefined,
    });
  });
});

describe("revocation keys", () => {
  it("generates distinct 64-char hex keys", () => {
    const a = generateRevocationKey();
    const b = generateRevocationKey();
    expect(a).toMatch(/^[0-9a-f]{64}$/);
    expect(a).not.toBe(b);
  });

  it("matches a key against its hash", () => {
    const key = generateRevocationKey();
    expect(revocationKeyMatches(key, hashRevocationKey(key))).toBe(true);
  });

  it("rejects a wrong key", () => {
    const key = generateRevocationKey();
    expect(
      revocationKeyMatches(generateRevocationKey(), hashRevocationKey(key))
    ).toBe(false);
  });

  it("rejects when either side is missing", () => {
    expect(revocationKeyMatches(null, "abc")).toBe(false);
    expect(revocationKeyMatches("abc", undefined)).toBe(false);
    expect(revocationKeyMatches("", "")).toBe(false);
  });

  it("rejects a malformed hash without throwing", () => {
    expect(revocationKeyMatches("abc", "not-hex-and-wrong-length")).toBe(false);
  });
});
