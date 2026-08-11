// Shared paste-related types.
//
// These represent two distinct shapes returned by different endpoints and
// are intentionally kept separate rather than force-unified:
//  - PasteContent: the full paste body, as returned by GET /api/pastes/:id
//  - PasteMeta: the lightweight metadata used in history listings

export type PasteContent = {
  content: string;
  language?: string;
  title?: string;
};

export type PasteMeta = {
  id: string;
  title: string;
  filename?: string;
  language?: string;
  user?: string;
  created_at: string | number | Date;
};
