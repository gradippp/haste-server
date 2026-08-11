import { NextResponse } from "next/server";
import { normalizeKey } from "@/lib/document";
import { getDocument } from "@/lib/documentHandler";
import { apiError } from "@/lib/http";

export const dynamic = "force-dynamic";

/**
 * GET /api/pastes/:id/meta
 *
 * Everything the paste page's `generateMetadata` needs, without shipping the
 * body.
 */
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const key = normalizeKey(id);
  if (!key) return apiError("Document not found.", 404);

  const doc = await getDocument(key);
  if (!doc) return apiError("Document not found.", 404);

  return NextResponse.json({
    id: key,
    title: doc.title || key,
    language: doc.language || "plaintext",
    created_at: doc.created_at || null,
  });
}
