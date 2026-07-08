# Keel — Product Shell v1: Approvals Queue (React frontend walking skeleton)

- **Status:** Draft for review
- **Date:** 2026-07-08
- **Related:** `docs/adr/0004-frontend.md`, `docs/mockups/slice-preview.html`,
  `docs/mockups/approvals.html`, `docs/mockups/assets/app.css`, `docs/PRD.md` (FR-X2/N4),
  `docs/superpowers/specs/2026-07-07-keel-ux-design.md`

## 1. Purpose

Turn the Approvals mockup into a real **React** page, and in doing so stand up the
ADR-0004 frontend toolchain (Vite + React + TS + Tailwind + shadcn/ui + TanStack Query),
port the mockups' light "admin-console" design system, and establish a reusable **app
shell** — the foundation every later page (Chat next) builds on. Approvals is the first
feature because it is self-contained (simple REST, no streaming), so it de-risks the
toolchain before the harder Chat/SSE slice.

## 2. Scope

**In scope**
- Vite React-TS app under `web/`, npm.
- Design system ported from `docs/mockups/assets/app.css` to a Tailwind theme +
  shadcn/ui-style primitives (button, card, badge, banner, table, toggle).
- **App shell**: sidebar (brand, scope switcher, grouped nav) + topbar + content area,
  matching the mockups; React Router with `/approvals` as the first live route and the
  other nav items as disabled placeholders.
- **Approvals feature**: TanStack Query hooks over the existing durable-approvals API;
  pending queue UI (confused-deputy taint banner, tool/target, suspended/timeout chips,
  approve/reject) with empty/loading/error states, matching `slice-preview.html`.
- Vite dev proxy to `keel-server`; unit tests (Vitest + React Testing Library + MSW).

**Out of scope** (YAGNI — later slices)
- Chat + SSE streaming (the next slice).
- All other pages (Chat/Sessions/Agents/Connectors/… nav items are placeholders).
- Login/auth (single-user; scope `web:local`).
- nginx/Docker static-serving wiring (dev-mode Vite proxy only for now).
- Dark theme, i18n toggle, desktop (Tauri) shell.

## 3. Backend contract (already implemented — consumed as-is)

- `GET /v1/approvals?status=pending` → `[{id, run_id, session_id, tool, args, call_id,
  reason, status, created_at, expires_at}]` (scope-bound to `web:local`).
- `POST /v1/approvals/{id}/approve` and `POST /v1/approvals/{id}/reject` → `{ok: bool}`
  (single-shot; on success the server enqueues a `resume_run`).
- `GET /health` → `{status, service, version}` (used for a dev connectivity check).

No backend changes are required for this slice.

## 4. Toolchain & scaffolding

- **Stack:** Vite (`react-ts` template), React 18, TypeScript (strict), Tailwind CSS v3,
  shadcn/ui-style primitives (hand-authored to avoid the generator's churn), TanStack
  Query v5, React Router v6.
- **Location:** `web/` (a self-contained Vite app). The existing `web/stub/` stays as the
  compose placeholder; wiring nginx to serve `web/dist` is a later slice.
- **Package manager:** npm. (Note: this machine has a global npm `min-release-age`
  config that delays very new releases; all chosen deps are mature, so unaffected.)
- **Dev proxy** (`vite.config.ts`): proxy `/v1` and `/health` → `http://localhost:8000`.

## 5. Design system

Port the tokens from `docs/mockups/assets/app.css` (light admin console) into a Tailwind
theme (CSS variables for `--bg/--surface/--border/--text/--accent/--green/--amber/--red/
--radius/--shadow`, fonts). Re-author the recurring primitives as small React components
(`Button`, `Card`, `Badge`, `Banner`, `Chip`) so pages compose them. The look must match
the mockups pixel-closely enough that `/approvals` reads as the real `slice-preview.html`.

## 6. App shell

`AppShell` = `Sidebar` + `Topbar` + `<Outlet/>`. `Sidebar` mirrors the mockup: brand,
scope switcher (static `个人助理 · web:local` for now), nav groups (工作区/配置/自动化/
运维) with icons; the active route highlights; non-`/approvals` items render disabled.
`Topbar` shows the page title + a right-slot (e.g., a pending count badge). This shell is
the reusable frame for Chat and every later page.

## 7. Approvals feature

- **Data (`features/approvals/useApprovals.ts`):**
  - `useApprovals()` → `useQuery(['approvals','pending'], …)` hitting `GET /v1/approvals`.
  - `useResolveApproval()` → `useMutation` posting approve/reject; `onSuccess` invalidates
    `['approvals']` and optimistically removes the row from the cached list.
- **UI (`ApprovalsPage.tsx`, `ApprovalCard.tsx`):** the pending queue. Each `ApprovalCard`
  renders the confused-deputy **taint banner** (when `reason === 'tainted'`), the tool +
  target (`args.to`), chips (`⏸ 已挂起 · run …`, `超时 fail-closed`), and Approve/Reject
  buttons wired to the mutation (disabled while pending). Empty state: "没有待处理的审批".
  Loading: skeleton cards. Error: an inline error banner with a retry.

## 8. Testing

Vitest + React Testing Library + MSW (mock `/v1/approvals`):
1. `useApprovals` renders a pending row's tool + target.
2. Clicking **Approve** POSTs `/v1/approvals/{id}/approve` and optimistically removes the
   row.
3. Empty response → the empty-state copy renders.
4. A failing GET → the error state renders.
5. `ApprovalCard` shows the taint banner only when `reason === 'tainted'`.

## 9. File structure

```
web/
  package.json  vite.config.ts  tsconfig.json  tailwind.config.ts
  postcss.config.js  index.html
  src/
    main.tsx  App.tsx  router.tsx
    lib/api.ts            # typed fetch wrapper (base '', paths '/v1/...')
    lib/queryClient.ts    # TanStack QueryClient
    styles/index.css      # tailwind layers + design tokens
    components/ui/{button,card,badge,banner,chip}.tsx
    components/{AppShell,Sidebar,Topbar}.tsx
    features/approvals/{ApprovalsPage,ApprovalCard,useApprovals,types}.ts(x)
    test/{setup.ts,msw.ts}
```

## 10. Definition of Done

- `npm run dev` serves the app; `/approvals` shows live pending approvals from a running
  keel-server (manually verified against the durable-approvals API).
- `npm run build` produces `web/dist` with no type errors (`tsc --noEmit` clean).
- `npm run test` (Vitest) green; ESLint (Vite's default) clean.
- The page visually matches `docs/mockups/slice-preview.html` (light admin console).

## 11. Open questions

1. **Polling vs manual refresh** for the pending queue: spec picks **manual refresh +
   invalidate-on-mutate** for the slice (a live push comes with the Chat/SSE slice).
   Confirm.
2. **shadcn generator vs hand-authored primitives:** spec hand-authors the few needed
   primitives to avoid the generator's dependency/config churn on this npm setup. Confirm.
