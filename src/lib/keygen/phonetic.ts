import crypto from "crypto";
import type { KeyGenerator } from "./types";

// Draws inspiration from pwgen and http://tools.arantius.com/password

export const VOWELS = "aeiou";
export const CONSONANTS = "bcdfghjklmnpqrstvwxyz";

const randOf = (collection: string) => () =>
  collection.charAt(crypto.randomInt(collection.length));

const randVowel = randOf(VOWELS);
const randConsonant = randOf(CONSONANTS);

export class PhoneticKeyGenerator implements KeyGenerator {
  /** Generate a phonetic key of alternating consonant & vowel. */
  createKey(keyLength: number): string {
    let text = "";
    const start = crypto.randomInt(2);

    for (let i = 0; i < keyLength; i++) {
      text += i % 2 === start ? randConsonant() : randVowel();
    }

    return text;
  }
}
