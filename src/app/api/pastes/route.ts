import { NextResponse } from "next/server";
import { getConfig } from "@/lib/config";
import {
  createDocument,
  DocumentTooLargeError,
} from "@/lib/documentHandler";
import { apiError, enforceRateLimit } from "@/lib/http";
import { logger } from "@/lib/logger";

export const dynamic = "force-dynamic";

/**
 * POST /api/pastes
 *
 * Accepts `multipart/form-data` with `content`, `language` and `title`
 * fields (what the editor sends), or a JSON body with the same keys.
 * Returns `{ id, revocation_key }` — the revocation key is shown only here
 * and is the only way to delete the paste later.
 */
export async function POST(request: Request) {
  const limited = enforceRateLimit(request);
  if (limited) return limited;

  const { maxLength } = getConfig();

  let content: string;
  let language: string | undefined;
  let title: string | undefined;

  const contentType = request.headers.get("content-type") || "";

  try {
    if (contentType.includes("application/json")) {
      const body = await request.json();
      content = typeof body?.content === "string" ? body.content : "";
      language = typeof body?.language === "string" ? body.language : undefined;
      title = typeof body?.title === "string" ? body.title : undefined;
    } else {
      const form = await request.formData();
      const raw = form.get("content");
      // multipart/form-data encoding canonicalizes line breaks to CRLF, so a
      // paste typed with LF endings would come back with stray carriage
      // returns. Undo that — the editor's buffer is the source of truth.
      content = typeof raw === "string" ? raw.replace(/\r\n/g, "\n") : "";
      const rawLanguage = form.get("language");
      const rawTitle = form.get("title");
      language = typeof rawLanguage === "string" ? rawLanguage : undefined;
      title = typeof rawTitle === "string" ? rawTitle : undefined;
    }
  } catch {
    return apiError("Could not parse request body.", 400);
  }

  if (!content) {
    return apiError("Paste content is required.", 400);
  }

  try {
    const { key, revocationKey } = await createDocument({
      content,
      language,
      title,
    });
    return NextResponse.json({ id: key, revocation_key: revocationKey });
  } catch (error) {
    if (error instanceof DocumentTooLargeError) {
      logger.warn("document >maxLength", { maxLength });
      return apiError(
        `Document exceeds the maximum length of ${maxLength} characters.`,
        400
      );
    }
    logger.error("error adding document", { error });
    return apiError("Error adding document.", 500);
  }
}
