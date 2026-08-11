import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, apiFetch, getPaste } from "@/lib/api";

function jsonResponse(body: unknown, init?: { status?: number; ok?: boolean }) {
  const status = init?.status ?? 200;
  return {
    ok: init?.ok ?? (status >= 200 && status < 300),
    status,
    statusText: "Status",
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

describe("apiFetch", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("sends no Authorization header — pastes are anonymous", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);

    await apiFetch("/api/thing");

    const [, init] = fetchMock.mock.calls[0];
    const headers = new Headers(init?.headers);
    expect(headers.has("Authorization")).toBe(false);
  });

  it("prefixes the request with NEXT_PUBLIC_BASE_URL", async () => {
    const original = process.env.NEXT_PUBLIC_BASE_URL;
    process.env.NEXT_PUBLIC_BASE_URL = "https://example.test";
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);

    await apiFetch("/api/thing");

    expect(fetchMock.mock.calls[0][0]).toBe("https://example.test/api/thing");

    // Assigning `undefined` to a process.env key stores the string
    // "undefined", which would then prefix every later request.
    if (original === undefined) delete process.env.NEXT_PUBLIC_BASE_URL;
    else process.env.NEXT_PUBLIC_BASE_URL = original;
  });

  it("returns undefined for an empty body", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 204,
      statusText: "No Content",
      json: async () => ({}),
      text: async () => "",
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(apiFetch("/api/thing")).resolves.toBeUndefined();
  });

  it("throws ApiError with status and message from a JSON error body", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ error: "Not found" }, { status: 404, ok: false })
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(apiFetch("/api/thing")).rejects.toMatchObject({
      name: "ApiError",
      message: "Not found",
      status: 404,
    });
  });

  it("throws ApiError with statusText fallback when body has no error field", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      json: async () => ({}),
      text: async () => "{}",
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(apiFetch("/api/thing")).rejects.toMatchObject({
      message: "Internal Server Error",
      status: 500,
    });
  });

  it("throws ApiError with statusText fallback when body is not JSON", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 502,
      statusText: "Bad Gateway",
      json: async () => {
        throw new Error("not json");
      },
      text: async () => "not json",
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(apiFetch("/api/thing")).rejects.toBeInstanceOf(ApiError);
    await expect(apiFetch("/api/thing")).rejects.toMatchObject({
      message: "Bad Gateway",
      status: 502,
    });
  });
});

describe("getPaste", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns the {content, language, title} shape", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({
          content: "print('hi')",
          language: "python",
          title: "My paste",
        })
      );
    vi.stubGlobal("fetch", fetchMock);

    const paste = await getPaste("abc123");

    expect(paste).toEqual({
      content: "print('hi')",
      language: "python",
      title: "My paste",
    });
  });

  it("defaults content to an empty string when missing", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({}));
    vi.stubGlobal("fetch", fetchMock);

    const paste = await getPaste("abc123");

    expect(paste.content).toBe("");
  });

  it("url-encodes the id", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ content: "" }));
    vi.stubGlobal("fetch", fetchMock);

    await getPaste("a b/c");

    expect(fetchMock.mock.calls[0][0]).toBe("/api/pastes/a%20b%2Fc");
  });
});
