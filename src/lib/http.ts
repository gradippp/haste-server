import { NextResponse } from "next/server";
import { checkRateLimit, clientIp } from "@/lib/rateLimit";

/**
 * The error shape `src/lib/api.ts` on the client expects: `{ error: string }`.
 * Keep messages user-safe — they're rendered verbatim in the UI.
 */
export function apiError(message: string, status: number): NextResponse {
  return NextResponse.json({ error: message }, { status });
}

/** The legacy `/documents` and `/raw` routes use `{ message }` instead. */
export function legacyError(message: string, status: number): NextResponse {
  return NextResponse.json({ message }, { status });
}

/**
 * Applies the rate limit to a mutating request. Returns a 429 response to
 * return immediately, or null when the request may proceed.
 */
export function enforceRateLimit(
  request: Request,
  errorShape: "api" | "legacy" = "api"
): NextResponse | null {
  const result = checkRateLimit(clientIp(request));
  if (result.allowed) return null;

  const message = "Rate limit exceeded. Try again shortly.";
  const response =
    errorShape === "api"
      ? apiError(message, 429)
      : legacyError(message, 429);

  if (result.retryAfter) {
    response.headers.set("Retry-After", String(result.retryAfter));
  }
  return response;
}

/**
 * Reads a request body as text, refusing anything over `maxBytes`.
 *
 * The stream is consumed incrementally and abandoned as soon as the cap is
 * passed, so an oversized upload never gets fully buffered in memory.
 */
export async function readBodyText(
  request: Request,
  maxBytes: number
): Promise<string | null> {
  const declared = request.headers.get("content-length");
  if (declared && Number(declared) > maxBytes) return null;

  const body = request.body;
  if (!body) return "";

  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        return null;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  return Buffer.concat(chunks).toString("utf8");
}
