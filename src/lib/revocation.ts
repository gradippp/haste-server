/**
 * Client-side bookkeeping for revocation keys.
 *
 * Pastes are anonymous, so the revocation key handed back at creation is the
 * only thing that lets the creator delete their paste later. It lives in
 * `localStorage` under `paste_<id>` — losing it (different browser, cleared
 * storage) means losing the ability to delete, which is inherent to having no
 * accounts.
 */

const PREFIX = "paste_";

interface StoredEntry {
  revocation_key?: string;
  date?: number;
}

function read(id: string): StoredEntry | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = localStorage.getItem(`${PREFIX}${id}`);
    if (!raw) return null;
    return JSON.parse(raw) as StoredEntry;
  } catch {
    return null;
  }
}

export function rememberRevocationKey(id: string, revocationKey: string) {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(
      `${PREFIX}${id}`,
      JSON.stringify({ revocation_key: revocationKey, date: Date.now() })
    );
  } catch {
    // Storage can be full or blocked (private mode); deletion just won't be
    // available for this paste.
  }
}

export function getRevocationKey(id: string): string | null {
  return read(id)?.revocation_key ?? null;
}

export function forgetRevocationKey(id: string) {
  if (typeof window === "undefined") return;
  try {
    localStorage.removeItem(`${PREFIX}${id}`);
  } catch {
    // Nothing useful to do.
  }
}
