import { NextResponse } from "next/server";
import { normalizeKey } from "@/lib/document";
import { getDocument } from "@/lib/documentHandler";
import { legacyError } from "@/lib/http";
import { logger } from "@/lib/logger";

export const dynamic = "force-dynamic";

type Params = { params: Promise<{ id: string }> };

/** GET /raw/:id — the original haste raw endpoint, plain text. */
export async function GET(_request: Request, { params }: Params) {
  const { id } = await params;
  const key = normalizeKey(id);
  if (!key) return legacyError("Document not found.", 404);

  const doc = await getDocument(key);
  if (!doc) {
    logger.warn("raw document not found", { key });
    return legacyError("Document not found.", 404);
  }

  logger.verbose("retrieved raw document", { key });
  return new NextResponse(doc.content, {
    status: 200,
    headers: { "content-type": "text/plain; charset=UTF-8" },
  });
}

/** HEAD /raw/:id — same status and headers, no body. */
export async function HEAD(request: Request, context: Params) {
  const response = await GET(request, context);
  return new NextResponse(null, {
    status: response.status,
    headers: response.headers,
  });
}
