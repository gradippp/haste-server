import crypto from "crypto";
import fs from "fs";
import type { KeyGenerator } from "./types";

export class DictionaryKeyGenerator implements KeyGenerator {
  readonly dictionary: string[];

  constructor(dictionaryPath: string) {
    if (!dictionaryPath) {
      throw new Error("No dictionary path specified in options");
    }

    // The old generator read the file asynchronously and would throw from a
    // callback if a key was requested before the read finished. Reading it
    // once, synchronously, at construction removes that race — the generator
    // is only built once per process.
    this.dictionary = fs
      .readFileSync(dictionaryPath, "utf8")
      .split(/[\n\r]+/)
      .filter(Boolean);

    if (this.dictionary.length === 0) {
      throw new Error(`Dictionary at ${dictionaryPath} is empty`);
    }
  }

  /** Generates a dictionary-based key of `keyLength` words. */
  createKey(keyLength: number): string {
    let text = "";
    for (let i = 0; i < keyLength; i++) {
      text += this.dictionary[crypto.randomInt(this.dictionary.length)];
    }
    return text;
  }
}
