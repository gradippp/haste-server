import { NextResponse } from "next/server";
import { normalizeKey } from "@/lib/document";
import { getDocument } from "@/lib/documentHandler";

export const dynamic = "force-dynamic";

/**
 * GET /:id/raw — the plain-text view the viewer footer links to.
 * `/raw/:id` (the original haste path) is served separately and behaves the
 * same.
 */
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const key = normalizeKey(id);
  if (!key) return new NextResponse("Not found", { status: 404 });

  const doc = await getDocument(key);
  if (!doc) return new NextResponse("Not found", { status: 404 });

  return new NextResponse(doc.content, {
    status: 200,
    headers: {
      "Content-Type": "text/plain; charset=UTF-8",
      "X-Paste-Language": doc.language || "plaintext",
      "X-Paste-ID": key,
    },
  });
}
