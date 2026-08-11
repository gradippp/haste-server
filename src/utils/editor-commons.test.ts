import { describe, expect, it } from "vitest";
import { SHORTCUTS, LANGUAGE_OPTIONS } from "@/utils/editor-commons";

// Note: the chord-matching logic itself lives inline inside Footer.tsx's
// keydown/keyup useEffect and is not extracted into a pure, exported
// function (that extraction is Phase 4 scope — src/hooks/useShortcuts.ts).
// There is nothing importable to unit-test the matching algorithm against
// yet, so this file only covers the shape of the constants it consumes.

describe("SHORTCUTS", () => {
  it("is a non-empty object", () => {
    expect(Object.keys(SHORTCUTS).length).toBeGreaterThan(0);
  });

  it("maps every shortcut to a non-empty array of non-empty key strings", () => {
    for (const [name, keys] of Object.entries(SHORTCUTS)) {
      expect(Array.isArray(keys), `${name} should be an array`).toBe(true);
      expect(keys.length, `${name} should not be empty`).toBeGreaterThan(0);
      for (const key of keys) {
        expect(typeof key).toBe("string");
        expect(key.length).toBeGreaterThan(0);
      }
    }
  });

  it("has no duplicate keys within a single shortcut's chord", () => {
    for (const [name, keys] of Object.entries(SHORTCUTS)) {
      const unique = new Set(keys);
      expect(unique.size, `${name} has duplicate keys in its chord`).toBe(keys.length);
    }
  });
});

describe("LANGUAGE_OPTIONS", () => {
  it("includes an auto-detect option with an empty value", () => {
    expect(LANGUAGE_OPTIONS.some((opt) => opt.value === "")).toBe(true);
  });

  it("has a non-empty label for every option", () => {
    for (const opt of LANGUAGE_OPTIONS) {
      expect(opt.label.length).toBeGreaterThan(0);
    }
  });

  it("has no duplicate non-empty values", () => {
    const values = LANGUAGE_OPTIONS.map((opt) => opt.value).filter((v) => v !== "");
    expect(new Set(values).size).toBe(values.length);
  });
});
