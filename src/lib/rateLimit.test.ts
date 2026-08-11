import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { resetConfig } from "@/lib/config";
import { checkRateLimit, clientIp, resetRateLimits } from "@/lib/rateLimit";

// checkRateLimit reads getConfig(), which is memoized and built from
// process.env — so each test sets the env it needs and resets both caches.
function configure(env: Record<string, string>) {
  for (const [key, value] of Object.entries(env)) process.env[key] = value;
  resetConfig();
  resetRateLimits();
}

const RATE_ENV_KEYS = [
  "RATELIMITS_NORMAL_TOTAL_REQUESTS",
  "RATELIMITS_NORMAL_EVERY_MILLISECONDS",
  "RATELIMITS_WHITELIST",
  "RATELIMITS_WHITELIST_TOTAL_REQUESTS",
  "RATELIMITS_WHITELIST_EVERY_MILLISECONDS",
  "RATELIMITS_BLACKLIST",
  "RATELIMITS_BLACKLIST_TOTAL_REQUESTS",
  "RATELIMITS_BLACKLIST_EVERY_MILLISECONDS",
];

beforeEach(() => {
  for (const key of RATE_ENV_KEYS) delete process.env[key];
});

afterEach(() => {
  for (const key of RATE_ENV_KEYS) delete process.env[key];
  resetConfig();
  resetRateLimits();
});

describe("checkRateLimit", () => {
  it("allows everything when rate limiting isn't configured", () => {
    configure({});
    for (let i = 0; i < 100; i++) {
      expect(checkRateLimit("1.1.1.1").allowed).toBe(true);
    }
  });

  it("allows up to the limit and blocks after it", () => {
    configure({
      RATELIMITS_NORMAL_TOTAL_REQUESTS: "3",
      RATELIMITS_NORMAL_EVERY_MILLISECONDS: "60000",
    });

    const now = 1_000_000;
    expect(checkRateLimit("1.1.1.1", now).allowed).toBe(true);
    expect(checkRateLimit("1.1.1.1", now).allowed).toBe(true);
    expect(checkRateLimit("1.1.1.1", now).allowed).toBe(true);

    const blocked = checkRateLimit("1.1.1.1", now);
    expect(blocked.allowed).toBe(false);
    expect(blocked.retryAfter).toBe(60);
  });

  it("counts each IP separately", () => {
    configure({
      RATELIMITS_NORMAL_TOTAL_REQUESTS: "1",
      RATELIMITS_NORMAL_EVERY_MILLISECONDS: "60000",
    });

    const now = 1_000_000;
    expect(checkRateLimit("1.1.1.1", now).allowed).toBe(true);
    expect(checkRateLimit("1.1.1.1", now).allowed).toBe(false);
    expect(checkRateLimit("2.2.2.2", now).allowed).toBe(true);
  });

  it("starts a fresh window once the old one has elapsed", () => {
    configure({
      RATELIMITS_NORMAL_TOTAL_REQUESTS: "1",
      RATELIMITS_NORMAL_EVERY_MILLISECONDS: "1000",
    });

    const now = 1_000_000;
    expect(checkRateLimit("1.1.1.1", now).allowed).toBe(true);
    expect(checkRateLimit("1.1.1.1", now).allowed).toBe(false);
    expect(checkRateLimit("1.1.1.1", now + 1001).allowed).toBe(true);
  });

  it("exempts a whitelisted IP that has no category limit", () => {
    configure({
      RATELIMITS_NORMAL_TOTAL_REQUESTS: "1",
      RATELIMITS_NORMAL_EVERY_MILLISECONDS: "60000",
      RATELIMITS_WHITELIST: "9.9.9.9",
    });

    const now = 1_000_000;
    for (let i = 0; i < 10; i++) {
      expect(checkRateLimit("9.9.9.9", now).allowed).toBe(true);
    }
  });

  it("applies a stricter limit to a blacklisted IP", () => {
    configure({
      RATELIMITS_NORMAL_TOTAL_REQUESTS: "100",
      RATELIMITS_NORMAL_EVERY_MILLISECONDS: "60000",
      RATELIMITS_BLACKLIST: "6.6.6.6",
      RATELIMITS_BLACKLIST_TOTAL_REQUESTS: "1",
      RATELIMITS_BLACKLIST_EVERY_MILLISECONDS: "60000",
    });

    const now = 1_000_000;
    expect(checkRateLimit("6.6.6.6", now).allowed).toBe(true);
    expect(checkRateLimit("6.6.6.6", now).allowed).toBe(false);
  });
});

describe("clientIp", () => {
  it("takes the first entry of X-Forwarded-For", () => {
    const request = new Request("http://localhost/", {
      headers: { "x-forwarded-for": "1.1.1.1, 2.2.2.2" },
    });
    expect(clientIp(request)).toBe("1.1.1.1");
  });

  it("falls back to X-Real-IP", () => {
    const request = new Request("http://localhost/", {
      headers: { "x-real-ip": "3.3.3.3" },
    });
    expect(clientIp(request)).toBe("3.3.3.3");
  });

  it("returns 'unknown' when neither header is present", () => {
    expect(clientIp(new Request("http://localhost/"))).toBe("unknown");
  });
});
