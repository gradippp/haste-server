import { NextResponse } from "next/server";
import { normalizeKey, revocationKeyMatches } from "@/lib/document";
import { deleteDocument, getDocument } from "@/lib/documentHandler";
import { apiError } from "@/lib/http";
import { logger } from "@/lib/logger";

export const dynamic = "force-dynamic";

type Params = { params: Promise<{ id: string }> };

/** GET /api/pastes/:id — the full paste body. */
export async function GET(_request: Request, { params }: Params) {
  const { id } = await params;
  const key = normalizeKey(id);
  if (!key) return apiError("Document not found.", 404);

  const doc = await getDocument(key);
  if (!doc) {
    logger.warn("document not found", { key });
    return apiError("Document not found.", 404);
  }

  logger.verbose("retrieved document", { key });
  return NextResponse.json({
    content: doc.content,
    language: doc.language,
    title: doc.title,
  });
}

/**
 * DELETE /api/pastes/:id?revocation_key=...
 *
 * Pastes are anonymous, so the revocation key handed out at creation is the
 * only proof of ownership.
 */
export async function DELETE(request: Request, { params }: Params) {
  const { id } = await params;
  const key = normalizeKey(id);
  if (!key) return apiError("Document not found.", 404);

  const doc = await getDocument(key);
  if (!doc) return apiError("Document not found.", 404);

  const supplied = new URL(request.url).searchParams.get("revocation_key");
  if (!revocationKeyMatches(supplied, doc.revocation_hash)) {
    return apiError("Not allowed to delete this document.", 403);
  }

  const result = await deleteDocument(key);
  if (result === null) {
    return apiError(
      "This server's storage backend does not support deletion.",
      501
    );
  }
  if (!result) return apiError("Error deleting document.", 500);

  logger.verbose("deleted document", { key });
  return new NextResponse(null, { status: 204 });
}
