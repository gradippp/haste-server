import { describe, expect, it } from "vitest";
import { highlight, detectLanguage } from "@/lib/highlight";
import { LANGUAGE_OPTIONS } from "@/utils/editor-commons";

describe("highlight language registration", () => {
  for (const { label, value } of LANGUAGE_OPTIONS) {
    if (value === "") continue; // auto-detect option, no fixed language

    it(`registers "${value}" (${label}) so it highlights without throwing`, () => {
      expect(() => highlight("const x = 1;\nfunction f() { return x; }", value)).not.toThrow();
      const result = highlight("const x = 1;\nfunction f() { return x; }", value);
      expect(typeof result).toBe("string");
      expect(result.length).toBeGreaterThan(0);
    });
  }

  it("falls back to auto-detection for an unregistered language", () => {
    expect(() => highlight("const x = 1;", "not-a-real-language")).not.toThrow();
  });
});

describe("detectLanguage", () => {
  it("returns a string (possibly empty) without throwing", () => {
    expect(() => detectLanguage("function f() { return 1; }")).not.toThrow();
    expect(typeof detectLanguage("function f() { return 1; }")).toBe("string");
  });
});
