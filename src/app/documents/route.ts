import { NextResponse } from "next/server";
import { getConfig } from "@/lib/config";
import { createDocument, DocumentTooLargeError } from "@/lib/documentHandler";
import { enforceRateLimit, legacyError, readBodyText } from "@/lib/http";
import { logger } from "@/lib/logger";

export const dynamic = "force-dynamic";

/**
 * POST /documents — the original haste API, kept so existing CLI clients
 * (`haste`, `hastebin` shell functions, editor plugins) keep working.
 *
 * The body is either the raw document, or `multipart/form-data` with a
 * `data` field. Responds `{ key }`.
 */
export async function POST(request: Request) {
  const limited = enforceRateLimit(request, "legacy");
  if (limited) return limited;

  const { maxLength } = getConfig();
  const contentType = request.headers.get("content-type") || "";

  let content: string | null;

  try {
    if (contentType.split(";")[0].trim() === "multipart/form-data") {
      const form = await request.formData();
      const value = form.get("data");
      // See the note in /api/pastes: multipart encoding canonicalizes line
      // breaks to CRLF.
      content = typeof value === "string" ? value.replace(/\r\n/g, "\n") : "";
    } else {
      // Read with a hard cap so an oversized upload is rejected without
      // being buffered in full. The cap is generous relative to maxLength
      // because maxLength counts characters while this counts bytes.
      content = await readBodyText(request, Math.max(maxLength * 4, 1024));
      if (content === null) {
        logger.warn("document >maxLength", { maxLength });
        return legacyError("Document exceeds maximum length.", 400);
      }
    }
  } catch (error) {
    logger.error("connection error", { error });
    return legacyError("Connection error.", 500);
  }

  if (!content) {
    return legacyError("Document is empty.", 400);
  }

  try {
    const { key } = await createDocument({ content });
    return NextResponse.json({ key });
  } catch (error) {
    if (error instanceof DocumentTooLargeError) {
      logger.warn("document >maxLength", { maxLength });
      return legacyError("Document exceeds maximum length.", 400);
    }
    logger.error("error adding document", { error });
    return legacyError("Error adding document.", 500);
  }
}
