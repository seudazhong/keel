import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

/**
 * Safe, read-only smoke coverage for the documented 10-15 minute demo
 * (docs/DEMO.md). This suite never sends Gmail, revokes connectors, cancels
 * jobs, resolves approvals, runs schedules, or creates/deletes user data or
 * settings — it only verifies that the current-main React app and API are
 * being served and render valid states.
 *
 * Run against an already-started stack (this file does not start or stop
 * Docker Compose, and never deletes volumes):
 *
 *   docker compose --profile dev up -d
 *   cd web
 *   npx playwright install --with-deps chromium   # first run only
 *   npm run test:e2e
 *
 * Target a different host/port with SMOKE_BASE_URL, e.g. a Vite dev server:
 *
 *   $env:SMOKE_BASE_URL = "http://127.0.0.1:5173"
 *   npm run test:e2e
 */

const REBUILD_HINT =
  "Rebuild the current-main images so the target serves the current API and React " +
  "bundle:\n  docker compose --profile dev up -d --build\n" +
  "This does not require deleting volumes (no '-v'/'--volumes' flag).";

const NAV_ITEMS: Array<{ label: string; path: string }> = [
  { label: "Sessions", path: "/sessions" },
  { label: "Memory", path: "/memory" },
  { label: "Knowledge", path: "/knowledge" },
  { label: "Approvals", path: "/approvals" },
  { label: "Schedules", path: "/schedules" },
  { label: "Observability", path: "/observability" },
  { label: "Jobs", path: "/jobs" },
];

async function getOrThrow(
  request: APIRequestContext,
  url: string,
  hint: string,
): Promise<import("@playwright/test").APIResponse> {
  try {
    return await request.get(url, { timeout: 10_000 });
  } catch (error) {
    throw new Error(
      `Could not reach ${url}.\n${hint}\nOriginal error: ${(error as Error).message}`,
    );
  }
}

/**
 * Fails fast with an actionable error when the target is not reachable, or is
 * reachable but running a stale/pre-M3.1 build: an older stack served either
 * a static web stub (no `/v1/jobs` or `/v1/knowledge-bases`, per docs/DEMO.md)
 * or no API at all. This must throw rather than let later tests silently skip
 * current-main-only routes.
 */
async function assertCurrentStack(request: APIRequestContext, baseURL: string): Promise<void> {
  const startHint =
    `Is the Compose dev stack running for ${baseURL}? Start it with:\n` +
    "  docker compose --profile dev up -d\n" +
    "Then wait for `docker compose ps` to report keel-server/keel-web healthy.";

  const health = await getOrThrow(request, `${baseURL}/health`, startHint);
  if (!health.ok()) {
    throw new Error(`${baseURL}/health returned HTTP ${health.status()}.\n${startHint}`);
  }

  const readiness = await getOrThrow(request, `${baseURL}/readiness`, startHint);
  if (readiness.status() !== 200) {
    const body = await readiness.text().catch(() => "<no body>");
    throw new Error(
      `${baseURL}/readiness reported not ready (HTTP ${readiness.status()}): ${body}\n${startHint}`,
    );
  }

  const [jobs, knowledgeBases] = await Promise.all([
    getOrThrow(request, `${baseURL}/v1/jobs?limit=1`, startHint),
    getOrThrow(request, `${baseURL}/v1/knowledge-bases`, startHint),
  ]);
  if (jobs.status() === 404 || knowledgeBases.status() === 404) {
    throw new Error(
      `Detected a stale pre-M3.1 stack at ${baseURL}: ` +
        `/v1/jobs=${jobs.status()} /v1/knowledge-bases=${knowledgeBases.status()}. ` +
        "These current-main routes must exist (an empty `[]` is fine; 404 is not).\n" +
        REBUILD_HINT,
    );
  }
}

/** The sidebar shell only renders from the built React bundle, not the legacy static stub. */
async function assertReactShellRendered(page: Page): Promise<void> {
  const stubMarker = page.getByText("web stub", { exact: false });
  if (await stubMarker.isVisible().catch(() => false)) {
    throw new Error(
      `${page.url()} served the legacy static web stub instead of the React app.\n${REBUILD_HINT}`,
    );
  }
  await expect(
    page.locator("aside").getByText("Keel", { exact: true }),
    "expected the React AppShell sidebar brand to render",
  ).toBeVisible();
}

test.beforeAll(async ({ request, baseURL }) => {
  await assertCurrentStack(request, baseURL ?? "http://127.0.0.1:3000");
});

test.describe("Keel demo smoke (read-only)", () => {
  test("React app boots and redirects to Chat", async ({ page }) => {
    await page.goto("/");
    await assertReactShellRendered(page);
    await expect(page).toHaveURL(/\/chat$/);
    await expect(page.getByRole("heading", { name: "Chat" })).toBeVisible();
  });

  test("SPA history fallback serves the app for a direct deep link", async ({ page }) => {
    // A hard navigation (not a client-side route push) to a nested path only
    // works if nginx's `try_files $uri $uri/ /index.html;` fallback is
    // configured (deploy/docker/web.nginx.conf); otherwise this 404s.
    const response = await page.goto("/knowledge");
    expect(response?.status(), "deep link should not 404").toBeLessThan(400);
    await assertReactShellRendered(page);
    await expect(page.getByRole("heading", { name: "Knowledge" })).toBeVisible();
  });

  test("primary navigation exposes the documented sections", async ({ page }) => {
    await page.goto("/chat");
    await assertReactShellRendered(page);

    const nav = page.locator("aside nav");
    for (const item of NAV_ITEMS) {
      await nav.getByRole("link", { name: item.label }).click();
      await expect(page).toHaveURL(new RegExp(`${item.path}$`));
      await expect(page.getByRole("heading", { name: item.label })).toBeVisible();
    }

    // Settings is reached from the Chat model badge rather than the sidebar.
    await nav.getByRole("link", { name: "Chat" }).click();
    await page.getByTitle("切换模型").click();
    await expect(page).toHaveURL(/\/settings$/);
    await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();
  });

  test("/health proxy reports a healthy keel-server", async ({ request, baseURL }) => {
    const res = await request.get(`${baseURL}/health`);
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.service).toBe("keel-server");
    expect(body.status).toBe("ok");
  });

  test("/readiness proxy reports dependency checks", async ({ request, baseURL }) => {
    const res = await request.get(`${baseURL}/readiness`);
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.ready).toBe(true);
    expect(body.checks?.postgres).toBe("ok");
    expect(body.checks?.redis).toBe("ok");
  });

  test("Jobs page renders a valid empty or populated state", async ({ page }) => {
    await page.goto("/jobs");
    await assertReactShellRendered(page);
    const list = page.locator('section[aria-label="Jobs list"]');
    await expect(list.locator(".animate-pulse")).toHaveCount(0, { timeout: 15_000 });
    await expect(page.getByText("Could not load jobs.")).toHaveCount(0);

    const empty = list.getByText("No durable jobs have been created for this scope.");
    const rows = list.locator("button[aria-pressed]");
    await expect(async () => {
      expect((await empty.isVisible()) || (await rows.count()) > 0).toBe(true);
    }).toPass({ timeout: 15_000 });
  });

  test("Knowledge page renders a valid empty or populated state", async ({ page }) => {
    await page.goto("/knowledge");
    await assertReactShellRendered(page);
    const bases = page.locator('section[aria-label="Knowledge bases"]');
    await expect(bases.locator(".animate-pulse")).toHaveCount(0, { timeout: 15_000 });
    await expect(page.getByText("Request failed. Check your input or try again.")).toHaveCount(0);

    const empty = bases.getByText("No knowledge bases yet. Create one to add documents.");
    const rows = bases.locator(".line-clamp-2");
    await expect(async () => {
      expect((await empty.isVisible()) || (await rows.count()) > 0).toBe(true);
    }).toPass({ timeout: 15_000 });
  });

  test("Memory page renders a valid empty or populated proposals state", async ({ page }) => {
    await page.goto("/memory");
    await assertReactShellRendered(page);
    await expect(page.locator(".animate-pulse")).toHaveCount(0, { timeout: 15_000 });
    await expect(page.getByText("Could not load memory proposals.")).toHaveCount(0);

    const empty = page.getByText("There are no consolidation proposals to review.");
    const rows = page.getByText(/% confidence/);
    await expect(async () => {
      expect((await empty.isVisible()) || (await rows.count()) > 0).toBe(true);
    }).toPass({ timeout: 15_000 });
  });

  test("API docs are reachable when exposed", async ({ request }) => {
    // The React origin's nginx does not proxy /docs (deploy/docker/web.nginx.conf),
    // so this checks the API origin directly. Skips (rather than fails) when the
    // API is not reachable on that origin, since /docs exposure is optional.
    const docsURL = process.env.SMOKE_API_DOCS_URL ?? "http://127.0.0.1:8000/docs";
    let res;
    try {
      res = await request.get(docsURL, { timeout: 10_000 });
    } catch {
      test.skip(true, `${docsURL} is not reachable from this runner; API docs are not exposed here.`);
      return;
    }
    test.skip(res.status() === 404, `${docsURL} returned 404; API docs are disabled on this build.`);
    expect(res.status()).toBe(200);
    const body = await res.text();
    expect(body.toLowerCase()).toContain("swagger");
  });
});
