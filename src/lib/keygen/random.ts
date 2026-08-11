import crypto from "crypto";
import type { KeyGenerator } from "./types";

export const DEFAULT_KEYSPACE =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

export class RandomKeyGenerator implements KeyGenerator {
  readonly keyspace: string;

  constructor(keyspace?: string) {
    this.keyspace = keyspace || DEFAULT_KEYSPACE;
  }

  createKey(keyLength: number): string {
    let text = "";
    for (let i = 0; i < keyLength; i++) {
      text += this.keyspace.charAt(crypto.randomInt(this.keyspace.length));
    }
    return text;
  }
}
