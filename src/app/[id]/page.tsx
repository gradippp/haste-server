import { notFound } from "next/navigation";
import { normalizeKey } from "@/lib/document";
import { getDocument } from "@/lib/documentHandler";
import { highlight } from "@/lib/highlight";
import PasteViewer from "./PasteViewer";

export const dynamic = "force-dynamic";

interface PageProps {
  params: Promise<{ id: string }>;
}

// This page renders on the server and the document store is in-process, so
// it reads the store directly instead of making an HTTP round-trip back to
// its own /api/pastes route.
async function load(id: string) {
  const key = normalizeKey(id);
  if (!key) return null;
  return getDocument(key);
}

export async function generateMetadata({ params }: PageProps) {
  const { id } = await params;
  const doc = await load(id);

  if (!doc) {
    return {
      title: "Paste not found",
      description: "The paste you're looking for may not exist.",
    };
  }

  const title = doc.title || id;
  return {
    title: `haste - ${title}`,
    description: `Code snippet '${title}' written in '${doc.language || "plaintext"}'`,
  };
}

export default async function PastePage({ params }: PageProps) {
  const { id } = await params;
  const doc = await load(id);

  if (!doc) notFound();

  const content = doc.content || "";
  const highlighted = highlight(content, doc.language);

  return (
    <PasteViewer
      id={id}
      title={doc.title || ""}
      language={doc.language || ""}
      lineCount={content.split("\n").length}
      highlightedHtml={highlighted}
    />
  );
}
