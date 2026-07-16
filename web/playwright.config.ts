import { defineConfig, devices } from "@playwright/test";

// Default targets the real Compose React surface (deploy/docker/web.nginx.conf
// on docker-compose.yml's keel-web service, published at :3000). Override for
// a different host/port, e.g. a Vite dev server, with SMOKE_BASE_URL.
const baseURL = process.env.SMOKE_BASE_URL ?? "http://127.0.0.1:3000";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 30_000,
  expect: { timeout: 10_000 },
  reporter: [["list"]],
  use: {
    baseURL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
