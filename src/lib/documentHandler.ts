import fs from "fs/promises";
import { getConfig } from "@/lib/config";
import {
  generateRevocationKey,
  hashRevocationKey,
  parseStored,
  serializeStored,
  type ParsedDocument,
} from "@/lib/document";
import { createKeyGenerator, type KeyGenerator } from "@/lib/keygen";
import { logger } from "@/lib/logger";
import { getStore } from "@/lib/stores";

/** How many times to re-roll a key before giving up on finding a free one. */
const MAX_KEY_ATTEMPTS = 10;

let keyGenerator: KeyGenerator | null = null;

function getKeyGenerator(): KeyGenerator {
  if (!keyGenerator) keyGenerator = createKeyGenerator(getConfig().keyGenerator);
  return keyGenerator;
}

/**
 * Seeds `config.documents` (e.g. `about=./about.md`) into the store.
 *
 * The old server did this once at boot. There's no boot hook here, so it runs
 * on the first request that touches a document and is memoized afterwards.
 * These documents are written with `skipExpire` so they never age out.
 */
let staticDocsPromise: Promise<void> | null = null;

export function ensureStaticDocuments(): Promise<void> {
  if (!staticDocsPromise) {
    staticDocsPromise = (async () => {
      const { documents } = getConfig();
      const store = await getStore();

      for (const [name, docPath] of Object.entries(documents)) {
        try {
          const data = await fs.readFile(docPath, "utf8");
          await store.set(name, data, true);
          logger.debug("loaded static document", { name, path: docPath });
        } catch (error) {
          logger.warn("failed to load static document", {
            name,
            path: docPath,
            error,
          });
        }
      }
    })().catch((error) => {
      // Never let a static-document failure break document serving.
      logger.error("error seeding static documents", { error });
    });
  }
  return staticDocsPromise;
}

/** Test-only: forget that static documents were seeded. */
export function resetDocumentHandler() {
  staticDocsPromise = null;
  keyGenerator = null;
}

export async function getDocument(
  key: string
): Promise<ParsedDocument | null> {
  await ensureStaticDocuments();

  const { documents } = getConfig();
  const skipExpire = Boolean(documents[key]);

  const store = await getStore();
  const raw = await store.get(key, skipExpire);
  if (raw === null) return null;

  return parseStored(raw);
}

export interface CreateDocumentInput {
  content: string;
  language?: string;
  title?: string;
}

export interface CreateDocumentResult {
  key: string;
  revocationKey: string;
}

export class DocumentTooLargeError extends Error {
  constructor(public readonly maxLength: number) {
    super("Document exceeds maximum length.");
    this.name = "DocumentTooLargeError";
  }
}

export async function createDocument(
  input: CreateDocumentInput
): Promise<CreateDocumentResult> {
  const { maxLength, keyLength } = getConfig();

  if (maxLength && input.content.length > maxLength) {
    throw new DocumentTooLargeError(maxLength);
  }

  const store = await getStore();
  const key = await chooseKey(keyLength);

  const revocationKey = generateRevocationKey();
  const payload = serializeStored({
    content: input.content,
    language: input.language || undefined,
    title: input.title || undefined,
    created_at: Date.now(),
    revocation_hash: hashRevocationKey(revocationKey),
  });

  const ok = await store.set(key, payload);
  if (!ok) throw new Error("Error adding document.");

  logger.verbose("added document", { key });
  return { key, revocationKey };
}

export async function deleteDocument(key: string): Promise<boolean | null> {
  const store = await getStore();
  // null signals "this backend can't delete" so the route can answer 501.
  if (!store.del) return null;
  return store.del(key);
}

/** Keep choosing keys until one isn't taken. */
async function chooseKey(keyLength: number): Promise<string> {
  const store = await getStore();
  const generator = getKeyGenerator();

  for (let attempt = 0; attempt < MAX_KEY_ATTEMPTS; attempt++) {
    const key = generator.createKey(keyLength);
    // skipExpire: probing for a free key must not bump anything's expiry.
    const existing = await store.get(key, true);
    if (existing === null) return key;
  }

  throw new Error(
    `Unable to find a free key after ${MAX_KEY_ATTEMPTS} attempts — consider increasing keyLength.`
  );
}
