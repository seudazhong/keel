# Web Sessions Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A Sessions list + detail (M1.7 slice 3): browse past sessions (Web/IM) with a client-side filter, and open a read-only replay of a session's event/message history.

**Architecture:** Add two additive `/v1` endpoints — `GET /v1/sessions` (a new scope-bound `list_sessions` over the `sessions` table + derived message count / first-user-message preview) and `GET /v1/sessions/{id}/history` (the durable event log as JSON via `PostgresEventStore.read`). The React list mirrors the Approvals/Connectors features; the **detail view reuses the chat reducer** (`applyEvent`) already built — folding the history events into a thread and rendering `ChatThread` read-only. Hybrid `session_search` wiring is deferred (client-side title filter for now).

**Tech Stack:** FastAPI + SQLAlchemy async; Vite + React 19 + TS + TanStack Query + React Router; Vitest/MSW. No new deps.

## Global Constraints

- Backend: ruff + `mypy packages` clean; endpoints additive under `/v1` (G14); scope-bound (RLS GUC).
- Frontend: oxlint + tsc + `vitest run` green; reuse UI primitives + `Topbar` + `chatReducer`/`ChatThread`.
- Scope: list + detail + client-side filter. Defer server-side hybrid `session_search` (note it in the UI).

---

## File Structure

- Modify `packages/keel-core/src/keel_core/state.py` — `SessionSummary` + `list_sessions`.
- Modify `packages/keel-server/src/keel_server/api/v1.py` — `GET /v1/sessions`, `GET /v1/sessions/{id}/history`.
- Create `tests/integration/test_sessions_api.py`.
- Create `web/src/features/sessions/{types.ts,useSessions.ts,useSessions.test.tsx,SessionsPage.tsx,SessionsPage.test.tsx,SessionDetailPage.tsx,SessionDetailPage.test.tsx}`.
- Modify `web/src/test/handlers.ts` — sessions + history handlers.
- Modify `web/src/router.tsx`, `web/src/components/Sidebar.tsx`, `web/src/components/AppShell.test.tsx`.

---

### Task 1: Backend — `list_sessions`

**Files:** Modify `state.py`; Create `tests/integration/test_sessions_api.py`.

**Interfaces — Produces:**
```python
@dataclass(frozen=True)
class SessionSummary:
    id: str
    title: str | None
    messages: int
    created_at: datetime
    updated_at: datetime

async def list_sessions(engine: AsyncEngine, scope_id: ScopeId, *, limit: int = 100) -> list[SessionSummary]: ...
```

- [ ] Write failing integration test: with a `PostgresEventStore(migrated_db, scope)`, `admit(...)` a user message into a session, then `list_sessions(migrated_db, scope)` returns one summary with `messages >= 1`, a non-null `updated_at`, and `title`/preview containing the message text; a different scope returns `[]`.
- [ ] Run `pytest -m integration tests/integration/test_sessions_api.py -v` → FAIL.
- [ ] Implement `SessionSummary` + `list_sessions` (SET scope GUC; select `s.id, COALESCE(s.title, first-user-message) AS title, count(message.token) AS messages, s.created_at, s.updated_at FROM sessions s WHERE scope_id=:scope ORDER BY updated_at DESC LIMIT :limit`, using correlated subqueries for the count + preview).
- [ ] Run → PASS. ruff + `mypy packages` clean.
- [ ] Commit.

### Task 2: Backend — endpoints

**Files:** Modify `v1.py`; add to `tests/integration/test_sessions_api.py`.

**Interfaces — Produces:**
- `GET /v1/sessions -> [{id, title, messages, created_at, updated_at}]`
- `GET /v1/sessions/{id}/history -> SseEvent[]` (durable event log, oldest first).

- [ ] Write failing test (full app via httpx ASGITransport with a real `migrated_db` engine on `app.state.engine` + `app.state.durable_scope`): after admitting a message, `GET /v1/sessions` lists the session and `GET /v1/sessions/{id}/history` returns the events including a `message.token`.
- [ ] Run → FAIL.
- [ ] Implement: `list_sessions_endpoint` (calls `list_sessions(engine, scope)` → dicts with ISO dates; `[]` when no engine); `session_history` (builds `PostgresEventStore(engine, scope)`, collects `read(session_id)` into `[e.model_dump(mode="json") for e in events]`).
- [ ] Run → PASS. ruff + `mypy packages` clean.
- [ ] Commit.

### Task 3: Frontend — hooks + MSW

**Files:** Create `web/src/features/sessions/{types.ts,useSessions.ts,useSessions.test.tsx}`; Modify `web/src/test/handlers.ts`.

**Interfaces — Produces:**
```ts
export interface SessionSummary { id: string; title: string | null; messages: number; created_at: string; updated_at: string; }
export function useSessions(): UseQueryResult<SessionSummary[]>;
export function useSessionHistory(id: string): UseQueryResult<SseEvent[]>; // SseEvent from features/chat/types
```

- [ ] `handlers.ts`: add `sampleSessions` + `GET /v1/sessions` and a `GET /v1/sessions/:id/history` returning a small event list (user message.token + assistant message.token + a tool.call/result).
- [ ] Write failing test: `useSessions` resolves to the sample list; `useSessionHistory("s1")` resolves to the event array.
- [ ] Run → FAIL.
- [ ] Implement `types.ts` + hooks (`useQuery` keys `["sessions"]` / `["sessions", id, "history"]`; `api.get`).
- [ ] Run → PASS. lint + tsc clean.
- [ ] Commit.

### Task 4: Frontend — SessionsPage (list + filter)

**Files:** Create `SessionsPage.tsx`, `SessionsPage.test.tsx`.

- [ ] Write failing test: given the MSW sample, the page renders each session title, a message count, and filtering the search input narrows the visible rows.
- [ ] Run → FAIL.
- [ ] Implement `SessionsPage`: `Topbar` ("Sessions", sub "· 跨 Web / IM 的历史会话"); a search `input` (client-side filter over title/id) + an info `Banner` noting hybrid search is coming; a `Card` table (会话 title, 消息 count, 最近 `updated_at`) where each row links to `/sessions/{id}`; loading `Skeleton` + error `Banner` + empty state.
- [ ] Run → PASS. lint + tsc clean.
- [ ] Commit.

### Task 5: Frontend — SessionDetailPage (replay)

**Files:** Create `SessionDetailPage.tsx`, `SessionDetailPage.test.tsx`.

- [ ] Write failing test: given the MSW history, folding events renders a user bubble, an assistant bubble, and a tool step (via `ChatThread`).
- [ ] Run → FAIL.
- [ ] Implement `SessionDetailPage`: read `:id` from the router; `useSessionHistory(id)`; fold `events.reduce(applyEvent, initialChatState)` → render `ChatThread items={state.items} onResolve={noop}` inside a scrollable area; `Topbar` with the session id + a back link to `/sessions`; loading/error states.
- [ ] Run → PASS. lint + tsc + `npm run build`.
- [ ] Commit.

### Task 6: Route + nav

**Files:** Modify `router.tsx`, `Sidebar.tsx`, `AppShell.test.tsx`.

- [ ] `router.tsx`: add `{ path: "sessions", element: <SessionsPage /> }` and `{ path: "sessions/:id", element: <SessionDetailPage /> }`.
- [ ] `Sidebar.tsx`: give Sessions `to: "/sessions"`.
- [ ] `AppShell.test.tsx`: include Sessions among the real links; pick a still-disabled item (e.g. Memory) for the placeholder test.
- [ ] `npm run test` green; lint clean.
- [ ] Commit.

### Task 7: Live verification (Docker server)

- [ ] Rebuild `keel-server` (new endpoints) + restart; `npm run dev`.
- [ ] Open `/sessions`: the list shows real sessions (the `digest:web:local` run + the chat sessions from earlier). Filter narrows rows.
- [ ] Open a session → the detail replays its real history (user/assistant messages + tool steps + any resolved approval) read-only. No console errors; screenshots.
