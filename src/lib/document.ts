import crypto from "crypto";

/**
 * On-disk shape of a document.
 *
 * haste historically stored the paste body as a bare string. The UI needs a
 * title and language alongside it, so new documents are stored as this JSON
 * envelope instead. `parseStored` falls back to treating anything it can't
 * recognise as a bare body, which keeps documents written by the old server
 * — and the static documents seeded from `config.documents`, which really are
 * plain files — readable.
 */
export interface StoredDocument {
  v: 1;
  content: string;
  language?: string;
  title?: string;
  created_at: number;
  /** SHA-256 of the revocation key. Never the key itself. */
  revocation_hash?: string;
}

export type ParsedDocument = Omit<StoredDocument, "v">;

/** Keys we're willing to hand to a storage backend. */
const KEY_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

export function isValidKey(key: string): boolean {
  return KEY_PATTERN.test(key);
}

/**
 * Strips a trailing extension (`abc.js` -> `abc`) the way the old routes did,
 * then validates. Returns null when the result isn't a usable key, so callers
 * can answer 404 without ever touching the store — this is what keeps `../`
 * and friends out of the file store's path building.
 */
export function normalizeKey(raw: string): string | null {
  const key = decodeURIComponent(raw).split(".")[0];
  return isValidKey(key) ? key : null;
}

export function parseStored(raw: string): ParsedDocument {
  try {
    const parsed = JSON.parse(raw);
    if (
      parsed &&
      typeof parsed === "object" &&
      parsed.v === 1 &&
      typeof parsed.content === "string"
    ) {
      return {
        content: parsed.content,
        language:
          typeof parsed.language === "string" && parsed.language
            ? parsed.language
            : undefined,
        title:
          typeof parsed.title === "string" && parsed.title
            ? parsed.title
            : undefined,
        created_at:
          typeof parsed.created_at === "number" ? parsed.created_at : 0,
        revocation_hash:
          typeof parsed.revocation_hash === "string"
            ? parsed.revocation_hash
            : undefined,
      };
    }
  } catch {
    // Not JSON — a legacy bare-string document.
  }

  return { content: raw, created_at: 0 };
}

export function serializeStored(doc: ParsedDocument): string {
  const envelope: StoredDocument = { v: 1, ...doc };
  return JSON.stringify(envelope);
}

export function generateRevocationKey(): string {
  return crypto.randomBytes(32).toString("hex");
}

export function hashRevocationKey(key: string): string {
  return crypto.createHash("sha256").update(key).digest("hex");
}

/** Constant-time comparison of a supplied revocation key against its hash. */
export function revocationKeyMatches(
  supplied: string | null,
  hash: string | undefined
): boolean {
  if (!supplied || !hash) return false;

  const a = Buffer.from(hashRevocationKey(supplied), "hex");
  const b = Buffer.from(hash, "hex");
  if (a.length !== b.length) return false;

  return crypto.timingSafeEqual(a, b);
}
