import type { Config } from "@/lib/config";
import { DictionaryKeyGenerator } from "./dictionary";
import { PhoneticKeyGenerator } from "./phonetic";
import { RandomKeyGenerator } from "./random";
import type { KeyGenerator } from "./types";

export type { KeyGenerator } from "./types";
export { DictionaryKeyGenerator } from "./dictionary";
export { PhoneticKeyGenerator } from "./phonetic";
export { RandomKeyGenerator } from "./random";

export function createKeyGenerator(
  options: Config["keyGenerator"]
): KeyGenerator {
  switch (options.type) {
    case "phonetic":
      return new PhoneticKeyGenerator();
    case "dictionary":
      if (!options.path) {
        throw new Error("No dictionary path specified in keyGenerator options");
      }
      return new DictionaryKeyGenerator(options.path);
    case "random":
      return new RandomKeyGenerator(options.keyspace);
    default:
      throw new Error(`Unknown key generator type: ${options.type}`);
  }
}
