import type { PasteContent } from "@/types/paste";

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number
  ) {
    super(message);
    this.name = "ApiError";
  }
}

/**
 * Single entry point for talking to the API.
 *
 * Pastes are anonymous on this server — there's no session token and no
 * Authorization header. Non-2xx responses are expected to carry a JSON
 * `{ error }` body, whose message is surfaced straight to the user.
 */
export async function apiFetch<T>(
  path: string,
  init?: RequestInit
): Promise<T> {
  const res = await fetch(
    `${process.env.NEXT_PUBLIC_BASE_URL || ""}${path}`,
    init
  );

  if (!res.ok) {
    let message = res.statusText || "Request failed";
    try {
      const body = await res.json();
      if (body?.error) message = body.error;
    } catch {
      // response had no JSON body
    }
    throw new ApiError(message, res.status);
  }

  // Some routes (e.g. DELETE) return no content.
  const text = await res.text();
  if (!text) return undefined as T;
  return JSON.parse(text) as T;
}

export async function getPaste(id: string): Promise<PasteContent> {
  const data = await apiFetch<{
    content?: string;
    language?: string;
    title?: string;
  }>(`/api/pastes/${encodeURIComponent(id)}`);

  return {
    content: data.content || "",
    language: data.language,
    title: data.title,
  };
}
