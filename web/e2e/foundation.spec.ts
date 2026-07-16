import { expect, test, type Page } from "@playwright/test";

/**
 * Coverage for the frontend-only UI foundation work: i18n, the responsive
 * shell, and first-run onboarding. Unlike smoke.spec.ts these scenarios never
 * touch the backend (no /health, /v1/jobs, etc.), so they can run against a
 * plain `npm run dev` server as well as the full Compose stack — set
 * SMOKE_BASE_URL to point at either.
 */

async function freshVisit(page: Page, path: string) {
  // Reset local-only state once, then land on the real route. Using
  // evaluate (rather than addInitScript) means a later page.reload() in the
  // same test does not wipe state set during the test itself.
  await page.goto(path);
  await page.evaluate(() => window.localStorage.clear());
  await page.goto(path);
}

const sidebar = (page: Page) => page.locator("aside").filter({ hasText: "Keel" });

test.describe("i18n locale switching", () => {
  test("switching to Simplified Chinese updates shell copy, <html lang>, and persists across reload", async ({
    page,
  }) => {
    await freshVisit(page, "/chat");

    await expect(page.locator("html")).toHaveAttribute("lang", "en");
    await expect(sidebar(page).getByText("Workspace", { exact: true })).toBeVisible();

    await sidebar(page).getByLabel("Language").selectOption("zh-CN");

    await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
    await expect(sidebar(page).getByText("工作区", { exact: true })).toBeVisible();

    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
    await expect(sidebar(page).getByText("工作区", { exact: true })).toBeVisible();
  });
});

test.describe("responsive shell", () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test("the mobile menu opens the sidebar drawer and a skip link targets main content", async ({
    page,
  }) => {
    await freshVisit(page, "/chat");

    // Sidebar is off-canvas AND non-visible (visibility:hidden) on narrow
    // viewports until the hamburger button opens it — visibility:hidden also
    // removes its links from the tab order, unlike transform alone.
    await expect(sidebar(page)).not.toBeInViewport();
    await expect(sidebar(page)).not.toBeVisible();
    await expect(sidebar(page).getByRole("link", { name: "Chat" })).not.toBeVisible();

    await page.getByRole("button", { name: "Open navigation menu" }).click();
    await expect(sidebar(page)).toBeInViewport();
    await expect(sidebar(page)).toBeVisible();
    await expect(sidebar(page).getByRole("link", { name: "Chat" })).toBeVisible();

    await page.getByRole("button", { name: "Close navigation menu" }).click();
    await expect(sidebar(page)).not.toBeInViewport();
    await expect(sidebar(page)).not.toBeVisible();

    const skipLink = page.getByText("Skip to main content");
    await expect(skipLink).toHaveAttribute("href", "#main-content");
  });

  test("the open drawer has dialog semantics and inerts the background main content until closed", async ({
    page,
  }) => {
    await freshVisit(page, "/chat");

    const main = page.locator("#main-content");
    await expect(main).not.toHaveAttribute("aria-hidden", "true");
    await expect(page.getByRole("heading", { name: "Chat" })).toBeVisible();

    await page.getByRole("button", { name: "Open navigation menu" }).click();

    const dialog = page.getByRole("dialog", { name: "Navigation menu" });
    await expect(dialog).toHaveAttribute("aria-modal", "true");
    await expect(page.getByRole("navigation", { name: "Primary navigation" })).toBeVisible();

    // The background content is inert while the modal drawer is open: it must
    // drop out of the accessibility tree and stop accepting focus/clicks.
    await expect(main).toHaveAttribute("aria-hidden", "true");
    await expect(page.getByRole("heading", { name: "Chat" })).toBeHidden();
    await expect(main).toHaveJSProperty("inert", true);

    await page.getByRole("button", { name: "Close navigation menu" }).click();

    await expect(main).not.toHaveAttribute("aria-hidden", "true");
    await expect(main).toHaveJSProperty("inert", false);
    await expect(page.getByRole("heading", { name: "Chat" })).toBeVisible();
  });
});

test.describe("desktop shell", () => {
  test("the persistent desktop sidebar stays visible regardless of the mobile drawer's closed state", async ({
    page,
  }) => {
    await freshVisit(page, "/chat");

    // No mobile hamburger button is reachable at desktop widths — the
    // sidebar is always visible via the lg:visible override.
    await expect(page.getByRole("button", { name: "Open navigation menu" })).toBeHidden();
    await expect(sidebar(page)).toBeVisible();
    await expect(sidebar(page).getByRole("link", { name: "Chat" })).toBeVisible();
  });
});

test.describe("first-run onboarding", () => {
  test("is discoverable from the sidebar, walks every step, and disappears once completed", async ({
    page,
  }) => {
    await freshVisit(page, "/chat");

    const prompt = page.getByRole("link", { name: /Welcome to Keel/ });
    await expect(prompt).toBeVisible();
    await prompt.click();

    await expect(page).toHaveURL(/\/onboarding$/);
    await expect(page.getByRole("heading", { level: 2, name: "Welcome", exact: true })).toBeVisible();

    await page.getByRole("button", { name: "Next" }).click(); // -> locale
    await page.getByRole("button", { name: "Next" }).click(); // -> workspace
    await page.getByLabel("Workspace name").fill("Ops team");
    await page.getByRole("button", { name: "Next" }).click(); // -> connectors

    await expect(page.getByText("Requires backend · not performed here")).toBeVisible();

    await page.getByRole("button", { name: "Next" }).click(); // -> finish
    await page.getByRole("button", { name: "Finish" }).click();

    await expect(page).toHaveURL(/\/chat$/);
    await expect(page.getByRole("link", { name: /Welcome to Keel/ })).toHaveCount(0);
  });
});
