# Product Shell v1 — Approvals Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A real React Approvals-queue page (from `slice-preview.html`) that stands up the ADR-0004 toolchain, the ported light design system, and a reusable app shell.

**Architecture:** A Vite React-TS SPA in `web/`, Tailwind + hand-authored shadcn-style primitives, TanStack Query over the existing durable-approvals REST API, React Router. Dev-served by Vite with a proxy to `keel-server`.

**Tech Stack:** Vite, React 18, TypeScript (strict), Tailwind CSS v3, TanStack Query v5, React Router v6, Vitest + React Testing Library + MSW.

## Global Constraints

- **Work inside `web/`.** All `npm` commands run from `C:\src\keel\web`.
- **Node 24 / npm 11** on this machine; a global npm `min-release-age` config delays very new releases — pin mature versions (below), don't chase latest.
- **TypeScript strict**; `npm run build` runs `tsc -b && vite build` and must be type-clean.
- **No backend changes** — consume `/v1/approvals` as-is.
- **Design parity:** match `docs/mockups/slice-preview.html` (light admin console; tokens in `docs/mockups/assets/app.css`).
- **Commits:** stage with `git -c core.safecrlf=false add web/...`; end each message with `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`. Commit `web/package-lock.json`; add `web/node_modules` + `web/dist` to `.gitignore`.

## File Structure

```
web/  package.json vite.config.ts tsconfig.json tsconfig.node.json
      tailwind.config.ts postcss.config.js index.html vitest.config.ts .gitignore
  src/ main.tsx App.tsx router.tsx vite-env.d.ts
       lib/{api.ts,queryClient.ts}
       styles/index.css
       components/ui/{button,card,badge,banner,chip,skeleton}.tsx
       components/{AppShell,Sidebar,Topbar}.tsx
       features/approvals/{ApprovalsPage,ApprovalCard,useApprovals,types}.ts(x)
       test/{setup.ts,handlers.ts,utils.tsx}
```

---

## Task 1: Scaffold Vite React-TS + Tailwind (walking skeleton)

De-risk the toolchain first: a blank app that builds, type-checks, and runs.

**Files:** Create `web/` via Vite; add Tailwind config + `src/styles/index.css`.

- [ ] **Step 1: Scaffold**

Run (from `C:\src\keel`):
```powershell
npm create vite@latest web -- --template react-ts
cd web
npm install
npm install -D tailwindcss@^3 postcss autoprefixer
npx tailwindcss init -p
```

- [ ] **Step 2: Configure Tailwind** — `web/tailwind.config.ts`:
```ts
import type { Config } from "tailwindcss";
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: { extend: {
    colors: {
      bg: "var(--bg)", surface: "var(--surface)", "surface-2": "var(--surface-2)",
      border: "var(--border)", text: "var(--text)", "text-soft": "var(--text-soft)",
      "text-muted": "var(--text-muted)", accent: "var(--accent)",
      green: "var(--green)", amber: "var(--amber)", red: "var(--red)", sky: "var(--sky)",
    },
    borderRadius: { DEFAULT: "10px", sm: "7px" },
  } },
  plugins: [],
} satisfies Config;
```

- [ ] **Step 3: Design tokens** — replace `web/src/index.css` → `web/src/styles/index.css`:
```css
@tailwind base; @tailwind components; @tailwind utilities;
:root {
  --bg:#f6f7f9; --surface:#fff; --surface-2:#fbfcfd; --border:#e6e8ec;
  --text:#10151f; --text-soft:#4a5568; --text-muted:#8a94a6;
  --accent:#4f46e5; --green:#16a34a; --amber:#b45309; --red:#dc2626; --sky:#0369a1;
}
body { margin:0; background:var(--bg); color:var(--text);
  font-family:-apple-system,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; }
```
Update `src/main.tsx` to `import "./styles/index.css";` and render a placeholder `<div className="p-6 text-accent">Keel</div>`.

- [ ] **Step 4: Verify build + typecheck + dev**

Run: `npm run build` → expect `dist/` produced, no TS errors. Then `npm run dev` → open `http://localhost:5173`, expect the placeholder renders (kill after check).

- [ ] **Step 5: gitignore + commit**

Add to `web/.gitignore`: `node_modules` and `dist`. Then:
```bash
git -c core.safecrlf=false add web/package.json web/package-lock.json web/*.ts web/*.js web/*.json web/index.html web/src web/.gitignore
git commit -m "feat(web): scaffold Vite React-TS + Tailwind (walking skeleton)

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 2: UI primitives (design system)

**Files:** Create `src/lib/cn.ts` + `src/components/ui/{button,card,badge,banner,chip,skeleton}.tsx`. Test: `src/components/ui/ui.test.tsx`.

**Interfaces (Produces):**
- `cn(...classes)` — clsx-style joiner.
- `Button({variant?: "primary"|"danger"|"default", ...})`, `Card`, `Badge({tone?: "green"|"amber"|"red"|"sky"|"violet"})`, `Banner({tone?: "warn"|"danger"|"info"})`, `Chip`, `Skeleton`.

- [ ] **Step 1: Install test deps**
```powershell
npm install -D vitest @testing-library/react @testing-library/jest-dom jsdom
```
Add to `web/package.json` scripts: `"test": "vitest run"`. Create `web/vitest.config.ts`:
```ts
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  test: { environment: "jsdom", globals: true, setupFiles: ["./src/test/setup.ts"] },
});
```
Create `src/test/setup.ts`: `import "@testing-library/jest-dom/vitest";`

- [ ] **Step 2: Write the failing test** — `src/components/ui/ui.test.tsx`:
```tsx
import { render, screen } from "@testing-library/react";
import { Badge } from "./badge";
import { Button } from "./button";

test("badge renders tone class + text", () => {
  render(<Badge tone="red">高风险</Badge>);
  const el = screen.getByText("高风险");
  expect(el.className).toContain("text-red");
});
test("primary button carries accent bg", () => {
  render(<Button variant="primary">批准</Button>);
  expect(screen.getByRole("button", { name: "批准" }).className).toContain("bg-accent");
});
```

- [ ] **Step 2b: Run → fail** — `npm run test` (modules missing).

- [ ] **Step 3: Implement primitives** — e.g. `src/lib/cn.ts`:
```ts
export const cn = (...xs: (string | false | undefined)[]) => xs.filter(Boolean).join(" ");
```
`src/components/ui/badge.tsx`:
```tsx
import { cn } from "../../lib/cn";
const tones = { green:"text-green bg-green/10", amber:"text-amber bg-amber/10",
  red:"text-red bg-red/10", sky:"text-sky bg-sky/10", violet:"text-accent bg-accent/10" };
export function Badge({ tone, className, children }:
  { tone?: keyof typeof tones; className?: string; children: React.ReactNode }) {
  return <span className={cn("inline-flex items-center gap-1 rounded-full border border-border px-2 py-0.5 text-xs font-semibold text-text-soft",
    tone && tones[tone], className)}>{children}</span>;
}
```
`src/components/ui/button.tsx`:
```tsx
import { cn } from "../../lib/cn";
const variants = { primary:"bg-accent text-white border-accent", danger:"bg-red text-white border-red",
  default:"bg-surface text-text border-border" };
export function Button({ variant="default", className, ...p }:
  React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: keyof typeof variants }) {
  return <button className={cn("inline-flex items-center gap-1.5 rounded-sm border px-3.5 py-2 text-sm font-semibold disabled:opacity-50",
    variants[variant], className)} {...p} />;
}
```
Author `card.tsx`, `banner.tsx`, `chip.tsx`, `skeleton.tsx` similarly (matching `app.css`).

- [ ] **Step 4: Run → pass** — `npm run test`.
- [ ] **Step 5: Commit** (`feat(web): design-system UI primitives`).

---

## Task 3: App shell + router

**Files:** Create `src/components/{AppShell,Sidebar,Topbar}.tsx`, `src/router.tsx`, update `src/App.tsx`, `src/main.tsx`. Install `react-router-dom`. Test: `src/components/AppShell.test.tsx`.

**Interfaces (Produces):** `AppShell` (renders `Sidebar`+`Topbar`+`<Outlet/>`); router with routes `/` (redirect `/approvals`), `/approvals`. `Sidebar` nav data: active `/approvals`, other items `disabled`.

- [ ] **Step 1:** `npm install react-router-dom`.
- [ ] **Step 2: Failing test** — `AppShell.test.tsx`: render the router at `/approvals`, assert the sidebar shows "Approvals" and a disabled "Chat" item.
- [ ] **Step 3: Implement** `Sidebar` (brand + scope switcher `个人助理 · web:local` + nav groups 工作区/配置/自动化/运维 from the mockup; active link highlighted; non-approvals items `aria-disabled`), `Topbar` (title + right slot), `AppShell` (grid `240px 1fr`), `router.tsx` (`createBrowserRouter` with `AppShell` layout route → `/approvals` child), wire `main.tsx` with `RouterProvider` + `QueryClientProvider` (Task 4 provides the client; for now a local `new QueryClient()`).
- [ ] **Step 4: Run → pass**; also `npm run build` clean.
- [ ] **Step 5: Commit** (`feat(web): app shell + router`).

---

## Task 4: API client + Query infrastructure + MSW

**Files:** Create `src/lib/api.ts`, `src/lib/queryClient.ts`, `src/test/handlers.ts`, `src/test/utils.tsx`. Install `@tanstack/react-query`, `msw`.

**Interfaces (Produces):**
- `api.get<T>(path)`, `api.post<T>(path)` — fetch wrapper, throws on non-2xx.
- `queryClient` — configured `QueryClient` (no retry in tests).
- `renderWithClient(ui)` test util (wraps in a fresh `QueryClientProvider`).
- MSW `handlers` for `GET /v1/approvals`, `POST /v1/approvals/:id/approve|reject`.

- [ ] **Step 1:** `npm install @tanstack/react-query` and `npm install -D msw`.
- [ ] **Step 2: Implement** `api.ts`:
```ts
async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, init);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.status === 204 ? (undefined as T) : ((await r.json()) as T);
}
export const api = {
  get: <T,>(p: string) => req<T>(p),
  post: <T,>(p: string) => req<T>(p, { method: "POST" }),
};
```
`queryClient.ts`: `export const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });`
`test/handlers.ts`: MSW handlers returning a sample pending approval; `test/setup.ts`: start/stop an MSW `server`. `test/utils.tsx`: `renderWithClient`.

- [ ] **Step 3: Test** — `src/lib/api.test.ts`: with MSW, `api.get('/v1/approvals?status=pending')` returns the sample row; a 500 handler makes it throw.
- [ ] **Step 4: Run → pass.**
- [ ] **Step 5: Commit** (`feat(web): api client + query infra + MSW`).

---

## Task 5: Approvals data hooks

**Files:** Create `src/features/approvals/{types.ts,useApprovals.ts}`. Test: `src/features/approvals/useApprovals.test.tsx`.

**Interfaces (Produces):**
- `type Approval = { id, run_id, session_id, tool, args: Record<string,unknown>, call_id, reason, status, created_at, expires_at }`.
- `useApprovals()` → `UseQueryResult<Approval[]>` (key `['approvals','pending']`).
- `useResolveApproval()` → mutation `({id, decision:"approve"|"reject"}) => POST`; `onSuccess` optimistically removes `id` from the `['approvals','pending']` cache + invalidates.

- [ ] **Step 1: Failing test** — with MSW: `useApprovals` returns the sample row (`waitFor` the tool name); calling `useResolveApproval().mutate({id, decision:"approve"})` POSTs and removes the row from the query cache.
- [ ] **Step 2: Implement** the hooks over `api` + `queryClient`.
- [ ] **Step 3: Run → pass.**
- [ ] **Step 4: Commit** (`feat(web): approvals data hooks`).

---

## Task 6: Approvals page + card UI

**Files:** Create `src/features/approvals/{ApprovalsPage,ApprovalCard}.tsx`; wire into `router.tsx`. Test: `src/features/approvals/ApprovalsPage.test.tsx`.

**Interfaces (Consumes):** `useApprovals`, `useResolveApproval`, UI primitives.

- [ ] **Step 1: Failing tests** (MSW-backed, via `renderWithClient`):
  - a pending row renders `email_send` + `args.to` + the taint banner (reason `tainted`);
  - clicking **批准** removes the card (optimistic);
  - empty API → "没有待处理的审批";
  - error API → an error banner.
- [ ] **Step 2: Implement** `ApprovalCard` (matches `slice-preview.html`: red-tinted header, confused-deputy `Banner` when `reason==='tainted'`, tool + `args.to`, chips `⏸ 已挂起 · run …`/`超时 fail-closed`, `Button variant="danger"` 拒绝 / `Button` 批准 — disabled while the mutation is pending) and `ApprovalsPage` (title, list, loading `Skeleton`s, empty + error states). Register the route.
- [ ] **Step 3: Run → pass**; `npm run build` clean.
- [ ] **Step 4: Commit** (`feat(web): Approvals queue page + card`).

---

## Task 7: Dev proxy + live verification + final green

**Files:** `web/vite.config.ts` (proxy). 

- [ ] **Step 1: Proxy** — `vite.config.ts`:
```ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/v1": "http://localhost:8000", "/health": "http://localhost:8000" } },
});
```

- [ ] **Step 2: Live check** — ensure a keel-server is running (Docker or host) with a seeded/suspended approval (`scripts/seed_digest_schedule.py` + a worker run, or POST a run that suspends). Run `npm run dev`; open `http://localhost:5173/approvals`; confirm the live pending approval renders and **批准** removes it (verify the row's status flips via `GET /v1/approvals`). Screenshot for parity vs `slice-preview.html`.

- [ ] **Step 3: Final gates** — from `web/`: `npm run build` (tsc clean + dist) and `npm run test` (green); `npm run lint` if present.

- [ ] **Step 4: Commit** (`chore(web): dev proxy + verified Approvals slice`).

---

## Self-Review

**Spec coverage:** toolchain scaffold (T1), design system (T2), app shell (T3), API client + query infra (T4), approvals hooks (T5), approvals page/card matching the mockup (T6), dev proxy + live verify (T7), tests throughout, DoD in T7. Both spec open-questions realized (manual-refresh + invalidate in T5; hand-authored primitives in T2).

**Placeholder scan:** no bare TODOs; every code step shows real config/code.

**Type consistency:** `Approval` (T5) fields match the backend contract (spec §3) and are consumed unchanged in T6; `useApprovals`/`useResolveApproval` names stable across T5→T6; UI primitive names (`Button`/`Badge`/`Banner`/`Chip`/`Card`/`Skeleton`) defined in T2 are consumed in T3/T6.
