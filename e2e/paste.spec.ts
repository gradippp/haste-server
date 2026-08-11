import { expect, test } from "@playwright/test";

/**
 * End-to-end coverage of the editor -> save -> view -> raw flow against a
 * real `next dev` server backed by the file store (see playwright.config.ts).
 */

const EDITOR = 'textarea[placeholder="Write your code here..."]';

async function createPaste(
  page: import("@playwright/test").Page,
  content: string,
  title?: string
) {
  await page.goto("/");
  await page.locator(EDITOR).fill(content);
  if (title) await page.locator("#paste-title").fill(title);

  await page.locator("#action-save").click();
  await page.waitForURL(/\/[A-Za-z0-9_-]+$/);

  const id = new URL(page.url()).pathname.slice(1);
  expect(id).toMatch(/^[A-Za-z0-9_-]+$/);
  return id;
}

test("creates a paste and renders it on the viewer page", async ({ page }) => {
  const id = await createPaste(page, "const answer = 42;", "My snippet");

  await expect(page.locator("pre code")).toContainText("const answer = 42;");
  await expect(page.locator("#paste-title")).toHaveValue("My snippet");
  await expect(page.locator("#action-raw")).toBeVisible();
  await expect(page).toHaveTitle(/My snippet/);

  // Line-number gutter is rendered alongside the code.
  await expect(page.getByText("1", { exact: true }).first()).toBeVisible();

  expect(id).toBeTruthy();
});

test("serves the paste as plain text at /:id/raw", async ({ page, request }) => {
  const id = await createPaste(page, "line one\nline two");

  const response = await request.get(`/${id}/raw`);
  expect(response.status()).toBe(200);
  expect(response.headers()["content-type"]).toContain("text/plain");
  expect(await response.text()).toBe("line one\nline two");
});

test("serves the paste at the legacy /raw/:id and /documents/:id paths", async ({
  page,
  request,
}) => {
  const id = await createPaste(page, "legacy access");

  const raw = await request.get(`/raw/${id}`);
  expect(raw.status()).toBe(200);
  expect(await raw.text()).toBe("legacy access");

  const doc = await request.get(`/documents/${id}`);
  expect(doc.status()).toBe(200);
  expect(await doc.json()).toEqual({ data: "legacy access", key: id });
});

test("accepts a paste posted to the legacy POST /documents endpoint", async ({
  request,
}) => {
  const response = await request.post("/documents", {
    data: "posted by a CLI client",
    headers: { "content-type": "text/plain" },
  });

  expect(response.status()).toBe(200);
  const { key } = await response.json();

  const raw = await request.get(`/raw/${key}`);
  expect(await raw.text()).toBe("posted by a CLI client");
});

test("shows the not-found page for an unknown id", async ({ page }) => {
  await page.goto("/definitelynotarealpaste");
  await expect(
    page.getByRole("heading", { name: "Paste not found" })
  ).toBeVisible();
});

test("404s the raw endpoint for an unknown id", async ({ request }) => {
  expect((await request.get("/definitelynotreal/raw")).status()).toBe(404);
  expect((await request.get("/raw/definitelynotreal")).status()).toBe(404);
});

test("Ctrl+S saves and Alt+R opens the raw view", async ({ page }) => {
  await page.goto("/");
  await page.locator(EDITOR).fill("saved with a shortcut");

  await page.keyboard.press("Control+s");
  await page.waitForURL(/\/[A-Za-z0-9_-]+$/);
  const id = new URL(page.url()).pathname.slice(1);

  await expect(page.locator("pre code")).toContainText("saved with a shortcut");

  await page.keyboard.press("Alt+r");
  await page.waitForURL(`/${id}/raw`);
  await expect(page.locator("body")).toContainText("saved with a shortcut");
});

test("editing a paste prefills the editor and saves a new one", async ({
  page,
}) => {
  const id = await createPaste(page, "original content", "Original");

  await page.locator("#action-edit").click();
  await page.waitForURL(`/?edit=${id}`);

  const editor = page.locator(EDITOR);
  await expect(editor).toHaveValue("original content");
  await expect(page.locator("#paste-title")).toHaveValue("Original");

  await editor.fill("edited content");
  await page.locator("#action-save").click();
  await page.waitForURL(/\/[A-Za-z0-9_-]+$/);

  const newId = new URL(page.url()).pathname.slice(1);
  expect(newId).not.toBe(id);
  await expect(page.locator("pre code")).toContainText("edited content");
});

test("the creator can delete their own paste", async ({ page }) => {
  const id = await createPaste(page, "delete me");

  // The delete action only appears for the creator, who holds the
  // revocation key in localStorage.
  await page.locator("#action-delete").click();
  await page.getByRole("button", { name: "Yes, delete it" }).click();

  await page.waitForURL("/");
  await expect(page.locator(EDITOR)).toBeVisible();

  await page.goto(`/${id}`);
  await expect(
    page.getByRole("heading", { name: "Paste not found" })
  ).toBeVisible();
});

test("a visitor who did not create the paste gets no delete action", async ({
  browser,
  page,
}) => {
  const id = await createPaste(page, "not yours to delete");

  // A fresh context has no revocation key in localStorage.
  const visitor = await browser.newContext();
  const visitorPage = await visitor.newPage();
  await visitorPage.goto(`/${id}`);

  await expect(visitorPage.locator("#action-raw")).toBeVisible();
  await expect(visitorPage.locator("#action-delete")).toHaveCount(0);

  await visitor.close();
});
