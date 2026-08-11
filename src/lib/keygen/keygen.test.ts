import fs from "fs";
import os from "os";
import path from "path";
import { afterAll, describe, expect, it } from "vitest";
import { createKeyGenerator } from "@/lib/keygen";
import { DictionaryKeyGenerator } from "@/lib/keygen/dictionary";
import { CONSONANTS, PhoneticKeyGenerator, VOWELS } from "@/lib/keygen/phonetic";
import { DEFAULT_KEYSPACE, RandomKeyGenerator } from "@/lib/keygen/random";

describe("RandomKeyGenerator", () => {
  it("generates a key of the requested length", () => {
    expect(new RandomKeyGenerator().createKey(6)).toHaveLength(6);
  });

  it("only uses characters from the default keyspace", () => {
    const key = new RandomKeyGenerator().createKey(200);
    for (const char of key) {
      expect(DEFAULT_KEYSPACE).toContain(char);
    }
  });

  it("honours a custom keyspace", () => {
    const gen = new RandomKeyGenerator("abc");
    expect(gen.createKey(50)).toMatch(/^[abc]{50}$/);
  });
});

describe("PhoneticKeyGenerator", () => {
  it("generates a key of the requested length", () => {
    expect(new PhoneticKeyGenerator().createKey(10)).toHaveLength(10);
  });

  it("alternates consonants and vowels", () => {
    const gen = new PhoneticKeyGenerator();
    for (let run = 0; run < 20; run++) {
      const key = gen.createKey(10);
      const kinds = [...key].map((char) =>
        CONSONANTS.includes(char) ? "c" : VOWELS.includes(char) ? "v" : "?"
      );
      expect(kinds).not.toContain("?");
      for (let i = 1; i < kinds.length; i++) {
        expect(kinds[i]).not.toBe(kinds[i - 1]);
      }
    }
  });
});

describe("DictionaryKeyGenerator", () => {
  const dictPath = path.join(
    fs.mkdtempSync(path.join(os.tmpdir(), "haste-dict-")),
    "words.txt"
  );
  fs.writeFileSync(dictPath, "alpha\nbeta\ngamma\n", "utf8");

  afterAll(() => {
    fs.rmSync(path.dirname(dictPath), { recursive: true, force: true });
  });

  it("concatenates `keyLength` words from the dictionary", () => {
    const gen = new DictionaryKeyGenerator(dictPath);
    expect(gen.dictionary).toEqual(["alpha", "beta", "gamma"]);
    expect(gen.createKey(3)).toMatch(/^(alpha|beta|gamma){3}$/);
  });

  it("throws when the dictionary path is missing", () => {
    expect(() => new DictionaryKeyGenerator("")).toThrow(
      /No dictionary path/
    );
  });
});

describe("createKeyGenerator", () => {
  it("builds each supported type", () => {
    expect(createKeyGenerator({ type: "random" })).toBeInstanceOf(
      RandomKeyGenerator
    );
    expect(createKeyGenerator({ type: "phonetic" })).toBeInstanceOf(
      PhoneticKeyGenerator
    );
  });

  it("requires a path for the dictionary generator", () => {
    expect(() => createKeyGenerator({ type: "dictionary" })).toThrow(
      /dictionary path/
    );
  });

  it("rejects an unknown type", () => {
    expect(() =>
      createKeyGenerator({
        type: "nope" as unknown as "random",
      })
    ).toThrow(/Unknown key generator/);
  });
});
