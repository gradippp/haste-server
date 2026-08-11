import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { resetConfig } from "@/lib/config";
import { resetDocumentHandler } from "@/lib/documentHandler";
import { resetRateLimits } from "@/lib/rateLimit";
import { MemoryDocumentStore } from "@/lib/stores/memory";
import { setStore } from "@/lib/stores";

import { POST as createPaste } from "@/app/api/pastes/route";
import {
  GET as getPaste,
  DELETE as deletePaste,
} from "@/app/api/pastes/[id]/route";
import { GET as getPasteMeta } from "@/app/api/pastes/[id]/meta/route";
import { POST as legacyPost } from "@/app/documents/route";
import {
  GET as legacyGet,
  HEAD as legacyHead,
} from "@/app/documents/[id]/route";
import { GET as legacyRaw, HEAD as legacyRawHead } from "@/app/raw/[id]/route";
import { GET as pasteRaw } from "@/app/[id]/raw/route";

/**
 * These exercise the route handlers directly — constructing a Request and
 * calling the exported method — against an in-memory store, so they cover
 * real request parsing and status codes without needing a server.
 */

const params = (id: string) => ({ params: Promise.resolve({ id }) });

let store: MemoryDocumentStore;

beforeEach(() => {
  // A short key length makes collision handling observable; keep it valid.
  process.env.KEY_LENGTH = "8";
  process.env.MAX_LENGTH = "1000";
  process.env.STORAGE_TYPE = "memory";
  delete process.env.RATELIMITS_NORMAL_TOTAL_REQUESTS;
  delete process.env.RATELIMITS_NORMAL_EVERY_MILLISECONDS;

  resetConfig();
  resetRateLimits();
  resetDocumentHandler();

  store = new MemoryDocumentStore();
  setStore(store);
});

afterEach(() => {
  delete process.env.KEY_LENGTH;
  delete process.env.MAX_LENGTH;
  delete process.env.STORAGE_TYPE;
  setStore(null);
  resetConfig();
  resetRateLimits();
  resetDocumentHandler();
});

function formRequest(fields: Record<string, string>, url = "http://x/api/pastes") {
  const body = new FormData();
  for (const [key, value] of Object.entries(fields)) body.append(key, value);
  return new Request(url, { method: "POST", body });
}

async function create(fields: Record<string, string>) {
  const response = await createPaste(formRequest(fields));
  expect(response.status).toBe(200);
  return (await response.json()) as { id: string; revocation_key: string };
}

describe("POST /api/pastes", () => {
  it("creates a paste and returns an id and revocation key", async () => {
    const body = await create({
      content: "print('hi')",
      language: "python",
      title: "My paste",
    });

    expect(body.id).toMatch(/^[A-Za-z0-9_-]{8}$/);
    expect(body.revocation_key).toMatch(/^[0-9a-f]{64}$/);
  });

  it("accepts a JSON body too", async () => {
    const response = await createPaste(
      new Request("http://x/api/pastes", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ content: "hi", language: "bash" }),
      })
    );
    expect(response.status).toBe(200);
  });

  it("rejects an empty paste", async () => {
    const response = await createPaste(formRequest({ content: "" }));
    expect(response.status).toBe(400);
    expect((await response.json()).error).toMatch(/required/i);
  });

  it("rejects content over maxLength", async () => {
    const response = await createPaste(
      formRequest({ content: "x".repeat(1001) })
    );
    expect(response.status).toBe(400);
    expect((await response.json()).error).toMatch(/maximum length/i);
  });

  it("returns 429 once the rate limit is exhausted", async () => {
    process.env.RATELIMITS_NORMAL_TOTAL_REQUESTS = "2";
    process.env.RATELIMITS_NORMAL_EVERY_MILLISECONDS = "60000";
    resetConfig();
    resetRateLimits();

    const request = () =>
      createPaste(
        new Request("http://x/api/pastes", {
          method: "POST",
          headers: { "content-type": "application/json", "x-real-ip": "5.5.5.5" },
          body: JSON.stringify({ content: "hi" }),
        })
      );

    expect((await request()).status).toBe(200);
    expect((await request()).status).toBe(200);

    const blocked = await request();
    expect(blocked.status).toBe(429);
    expect(blocked.headers.get("Retry-After")).toBe("60");
  });
});

describe("GET /api/pastes/:id", () => {
  it("returns the stored content, language and title", async () => {
    const { id } = await create({
      content: "print('hi')",
      language: "python",
      title: "My paste",
    });

    const response = await getPaste(new Request("http://x"), params(id));
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      content: "print('hi')",
      language: "python",
      title: "My paste",
    });
  });

  it("never leaks the revocation hash", async () => {
    const { id } = await create({ content: "secret" });
    const body = await (await getPaste(new Request("http://x"), params(id))).json();
    expect(JSON.stringify(body)).not.toContain("revocation");
  });

  it("404s for an unknown id", async () => {
    const response = await getPaste(new Request("http://x"), params("nothere"));
    expect(response.status).toBe(404);
    expect((await response.json()).error).toBe("Document not found.");
  });

  it("404s for an id that would escape the store's key space", async () => {
    const response = await getPaste(
      new Request("http://x"),
      params("../../etc/passwd")
    );
    expect(response.status).toBe(404);
  });
});

describe("GET /api/pastes/:id/meta", () => {
  it("returns metadata without the body", async () => {
    const { id } = await create({
      content: "print('hi')",
      language: "python",
      title: "My paste",
    });

    const response = await getPasteMeta(new Request("http://x"), params(id));
    const body = await response.json();

    expect(response.status).toBe(200);
    expect(body).toMatchObject({ id, title: "My paste", language: "python" });
    expect(body.content).toBeUndefined();
    expect(typeof body.created_at).toBe("number");
  });

  it("falls back to the id and plaintext when title/language are absent", async () => {
    const { id } = await create({ content: "hi" });
    const body = await (
      await getPasteMeta(new Request("http://x"), params(id))
    ).json();
    expect(body).toMatchObject({ title: id, language: "plaintext" });
  });

  it("404s for an unknown id", async () => {
    const response = await getPasteMeta(new Request("http://x"), params("nope"));
    expect(response.status).toBe(404);
  });
});

describe("DELETE /api/pastes/:id", () => {
  it("deletes with a matching revocation key", async () => {
    const { id, revocation_key } = await create({ content: "hi" });

    const response = await deletePaste(
      new Request(`http://x/api/pastes/${id}?revocation_key=${revocation_key}`, {
        method: "DELETE",
      }),
      params(id)
    );

    expect(response.status).toBe(204);
    expect(
      (await getPaste(new Request("http://x"), params(id))).status
    ).toBe(404);
  });

  it("403s with a wrong revocation key", async () => {
    const { id } = await create({ content: "hi" });

    const response = await deletePaste(
      new Request(`http://x/api/pastes/${id}?revocation_key=deadbeef`, {
        method: "DELETE",
      }),
      params(id)
    );

    expect(response.status).toBe(403);
    // Still there.
    expect((await getPaste(new Request("http://x"), params(id))).status).toBe(200);
  });

  it("403s with no revocation key at all", async () => {
    const { id } = await create({ content: "hi" });
    const response = await deletePaste(
      new Request(`http://x/api/pastes/${id}`, { method: "DELETE" }),
      params(id)
    );
    expect(response.status).toBe(403);
  });

  it("404s for an unknown id", async () => {
    const response = await deletePaste(
      new Request("http://x/api/pastes/nope?revocation_key=x", {
        method: "DELETE",
      }),
      params("nope")
    );
    expect(response.status).toBe(404);
  });

  it("501s when the storage backend cannot delete", async () => {
    const { id, revocation_key } = await create({ content: "hi" });

    // A store with no `del` — several backends legitimately lack one.
    setStore({
      get: (key: string, skipExpire?: boolean) => store.get(key, skipExpire),
      set: (key: string, data: string, skipExpire?: boolean) =>
        store.set(key, data, skipExpire),
    });

    const response = await deletePaste(
      new Request(`http://x/api/pastes/${id}?revocation_key=${revocation_key}`, {
        method: "DELETE",
      }),
      params(id)
    );

    expect(response.status).toBe(501);
  });
});

describe("GET /:id/raw", () => {
  it("returns the body as plain text", async () => {
    const { id } = await create({ content: "line1\nline2", language: "bash" });

    const response = await pasteRaw(new Request("http://x"), params(id));

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe(
      "text/plain; charset=UTF-8"
    );
    expect(response.headers.get("X-Paste-Language")).toBe("bash");
    expect(await response.text()).toBe("line1\nline2");
  });

  it("404s for an unknown id", async () => {
    const response = await pasteRaw(new Request("http://x"), params("nope"));
    expect(response.status).toBe(404);
  });
});

describe("legacy haste API", () => {
  it("POST /documents accepts a raw body and returns { key }", async () => {
    const response = await legacyPost(
      new Request("http://x/documents", { method: "POST", body: "hello world" })
    );

    expect(response.status).toBe(200);
    const { key } = await response.json();
    expect(key).toMatch(/^[A-Za-z0-9_-]{8}$/);

    const get = await legacyGet(new Request("http://x"), params(key));
    expect(await get.json()).toEqual({ data: "hello world", key });
  });

  it("POST /documents accepts a multipart `data` field", async () => {
    const body = new FormData();
    body.append("data", "from a form");

    const response = await legacyPost(
      new Request("http://x/documents", { method: "POST", body })
    );

    const { key } = await response.json();
    const get = await legacyGet(new Request("http://x"), params(key));
    expect((await get.json()).data).toBe("from a form");
  });

  it("POST /documents rejects an over-long body", async () => {
    const response = await legacyPost(
      new Request("http://x/documents", {
        method: "POST",
        body: "x".repeat(1001),
      })
    );
    expect(response.status).toBe(400);
    expect((await response.json()).message).toMatch(/maximum length/i);
  });

  it("GET /documents/:id strips a file extension", async () => {
    const response = await legacyPost(
      new Request("http://x/documents", { method: "POST", body: "console.log()" })
    );
    const { key } = await response.json();

    const get = await legacyGet(new Request("http://x"), params(`${key}.js`));
    expect(await get.json()).toEqual({ data: "console.log()", key });
  });

  it("GET /documents/:id 404s with the legacy message shape", async () => {
    const response = await legacyGet(new Request("http://x"), params("nope"));
    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({ message: "Document not found." });
  });

  it("GET /raw/:id returns plain text", async () => {
    const response = await legacyPost(
      new Request("http://x/documents", { method: "POST", body: "raw body" })
    );
    const { key } = await response.json();

    const raw = await legacyRaw(new Request("http://x"), params(key));
    expect(raw.status).toBe(200);
    expect(raw.headers.get("content-type")).toBe("text/plain; charset=UTF-8");
    expect(await raw.text()).toBe("raw body");
  });

  it("HEAD variants return the same status with an empty body", async () => {
    const response = await legacyPost(
      new Request("http://x/documents", { method: "POST", body: "head me" })
    );
    const { key } = await response.json();

    const head = await legacyHead(new Request("http://x"), params(key));
    expect(head.status).toBe(200);
    expect(await head.text()).toBe("");

    const rawHead = await legacyRawHead(new Request("http://x"), params(key));
    expect(rawHead.status).toBe(200);
    expect(await rawHead.text()).toBe("");

    const missing = await legacyHead(new Request("http://x"), params("nope"));
    expect(missing.status).toBe(404);
    expect(await missing.text()).toBe("");
  });

  it("a document written by the old server still reads back", async () => {
    // The old server stored a bare string, not a JSON envelope.
    await store.set("legacy1", "plain old content");

    const get = await legacyGet(new Request("http://x"), params("legacy1"));
    expect(await get.json()).toEqual({
      data: "plain old content",
      key: "legacy1",
    });

    const api = await getPaste(new Request("http://x"), params("legacy1"));
    expect((await api.json()).content).toBe("plain old content");
  });
});
