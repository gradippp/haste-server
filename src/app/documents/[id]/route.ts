import { NextResponse } from "next/server";
import { normalizeKey } from "@/lib/document";
import { getDocument } from "@/lib/documentHandler";
import { legacyError } from "@/lib/http";
import { logger } from "@/lib/logger";

export const dynamic = "force-dynamic";

type Params = { params: Promise<{ id: string }> };

/**
 * GET /documents/:id — the original haste API. Responds `{ data, key }`,
 * where `data` is the paste body. `:id` may carry a file extension
 * (`abc.js`), which is stripped.
 */
export async function GET(_request: Request, { params }: Params) {
  const { id } = await params;
  const key = normalizeKey(id);
  if (!key) return legacyError("Document not found.", 404);

  const doc = await getDocument(key);
  if (!doc) {
    logger.warn("document not found", { key });
    return legacyError("Document not found.", 404);
  }

  logger.verbose("retrieved document", { key });
  return NextResponse.json({ data: doc.content, key });
}

/** HEAD /documents/:id — same status and headers, no body. */
export async function HEAD(request: Request, context: Params) {
  const response = await GET(request, context);
  return new NextResponse(null, {
    status: response.status,
    headers: response.headers,
  });
}
