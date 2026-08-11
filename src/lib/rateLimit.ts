import { getConfig, type RateLimitCategory } from "@/lib/config";

/**
 * In-process fixed-window rate limiter, replacing `connect-ratelimit`.
 *
 * It reads the same `rateLimits` config shape as before (categories with
 * `totalRequests` per `every` milliseconds, plus whitelist/blacklist IP
 * lists). Counters live in this process's memory, so behind multiple
 * instances the effective limit is per-instance — put a shared limiter in
 * your reverse proxy if you need a global one.
 */

interface Window {
  count: number;
  resetAt: number;
}

const windows = new Map<string, Window>();

export interface RateLimitResult {
  allowed: boolean;
  /** Seconds until the current window resets. Only set when blocked. */
  retryAfter?: number;
}

function categoryFor(ip: string): RateLimitCategory | null | undefined {
  const { rateLimits } = getConfig();
  if (!rateLimits) return undefined;

  if (rateLimits.blacklist.includes(ip)) return rateLimits.categories.blacklist;
  if (rateLimits.whitelist.includes(ip)) return rateLimits.categories.whitelist;
  return rateLimits.categories.normal;
}

export function checkRateLimit(ip: string, now = Date.now()): RateLimitResult {
  const category = categoryFor(ip);

  // undefined: rate limiting is off entirely.
  // null: this IP's category has no limit configured (e.g. a whitelist with
  // no numbers set), which means unlimited — the old module behaved the same.
  if (!category) return { allowed: true };

  const existing = windows.get(ip);

  if (!existing || existing.resetAt <= now) {
    windows.set(ip, { count: 1, resetAt: now + category.every });
    return { allowed: true };
  }

  if (existing.count >= category.totalRequests) {
    return {
      allowed: false,
      retryAfter: Math.max(1, Math.ceil((existing.resetAt - now) / 1000)),
    };
  }

  existing.count += 1;
  return { allowed: true };
}

/**
 * Best-effort client IP. Behind a proxy this needs `X-Forwarded-For` to be
 * set by something you trust; a client can forge the header otherwise.
 */
export function clientIp(request: Request): string {
  const forwarded = request.headers.get("x-forwarded-for");
  if (forwarded) return forwarded.split(",")[0].trim();
  return request.headers.get("x-real-ip")?.trim() || "unknown";
}

/** Test-only: clear all counters. */
export function resetRateLimits() {
  windows.clear();
}
