# Web Connectors Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A Connectors page (M1.7 slice 2) giving read-only visibility into the scope's OAuth connectors — connection status, granted scopes, last activity — plus the confused-deputy taint rule and a catalog of addable integrations. Surfaces the real Gmail connector built earlier.

**Architecture:** No connector REST API exists yet, so add a minimal `GET /v1/connectors`: a static server-side catalog (id/name/icon/scopes, sourced from `keel_core.gmail`) joined with connection status from `connector_tokens` (a new `list_connected(engine, scope)` helper — no decryption needed). The React page mirrors the Approvals feature (TanStack Query + `api.get` + MSW). Actions (revoke / re-authorize / in-browser connect) are **deferred** and shown as disabled hints.

**Tech Stack:** FastAPI + SQLAlchemy async (backend); Vite + React 19 + TS + TanStack Query + Vitest/MSW (frontend). No new deps.

## Global Constraints

- Backend: ruff + `mypy packages` clean; endpoint additive-only under `/v1` (G14).
- `list_connected` reads only `(connector_id, updated_at)` — never decrypts tokens (no cipher dependency).
- Frontend: oxlint + tsc + `vitest run` green; reuse existing UI primitives + `Topbar`.
- Scope: read-only visibility. Defer revoke/re-auth/connect mutations (note them in the UI).

---

## File Structure

- Modify `packages/keel-core/src/keel_core/tokens.py` — `ConnectorTokenInfo` + `list_connected`.
- Modify `packages/keel-server/src/keel_server/api/v1.py` — `CONNECTOR_CATALOG` + `GET /v1/connectors`.
- Create `tests/integration/test_connectors_api.py` — store helper (Postgres) + endpoint (no-engine).
- Create `web/src/features/connectors/{types.ts,useConnectors.ts,ConnectorsPage.tsx,ConnectorsPage.test.tsx,useConnectors.test.tsx}`.
- Modify `web/src/test/handlers.ts` — `GET /v1/connectors` handler.
- Modify `web/src/router.tsx`, `web/src/components/Sidebar.tsx` — `/connectors` route + nav.

---

### Task 1: Backend — `list_connected`

**Files:** Modify `packages/keel-core/src/keel_core/tokens.py`; Create `tests/integration/test_connectors_api.py`.

**Interfaces — Produces:**
```python
@dataclass(frozen=True)
class ConnectorTokenInfo:
    connector_id: str
    updated_at: datetime | None

async def list_connected(engine: AsyncEngine, scope_id: ScopeId) -> list[ConnectorTokenInfo]: ...
```

- [ ] Write failing integration test: seed two tokens for a unique scope via `PostgresTokenStore(...).put`, then `list_connected(migrated_db, scope)` returns both `connector_id`s sorted with non-null `updated_at`; a different scope returns `[]`.
```python
async def test_list_connected(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    cipher = EnvelopeCipher("k")
    await PostgresTokenStore(migrated_db, scope, cipher).put("gmail", "t1")
    await PostgresTokenStore(migrated_db, scope, cipher).put("calendar", "t2")
    infos = await list_connected(migrated_db, scope)
    assert [i.connector_id for i in infos] == ["calendar", "gmail"]
    assert all(i.updated_at is not None for i in infos)
    assert await list_connected(migrated_db, f"u:{uuid.uuid4().hex}") == []
```
- [ ] Run `pytest -m integration tests/integration/test_connectors_api.py -v` → FAIL.
- [ ] Implement `ConnectorTokenInfo` + `list_connected` (SET scope GUC, `SELECT connector_id, updated_at FROM connector_tokens WHERE scope_id = :scope ORDER BY connector_id`).
- [ ] Run → PASS. ruff + `mypy packages` clean.
- [ ] Commit.

### Task 2: Backend — `GET /v1/connectors`

**Files:** Modify `packages/keel-server/src/keel_server/api/v1.py`; add to `tests/integration/test_connectors_api.py`.

**Interfaces — Produces:** `GET /v1/connectors -> [{id, name, icon, kind, scopes: string[], connected: bool, updated_at: string|null}]`.

- [ ] Write failing test (minimal FastAPI + `v1.router`, `app.state.engine = None`, `app.state.durable_scope = "web:local"`, httpx ASGITransport): `GET /v1/connectors` returns a list containing a gmail entry with `connected == False` and `scopes == ["gmail.readonly", "gmail.send"]`.
- [ ] Run → FAIL.
- [ ] Implement `CONNECTOR_CATALOG` (gmail: name "Gmail", icon "✉️", kind "oauth", scopes = `[s.rsplit("/",1)[-1] for s in GMAIL_SCOPES]`) + the endpoint (join catalog with `list_connected` when an engine is present).
- [ ] Run → PASS. ruff + `mypy packages` clean.
- [ ] Commit.

### Task 3: Frontend — data hook + MSW

**Files:** Create `web/src/features/connectors/types.ts`, `useConnectors.ts`, `useConnectors.test.tsx`; Modify `web/src/test/handlers.ts`.

**Interfaces — Produces:**
```ts
export interface Connector { id: string; name: string; icon: string; kind: string; scopes: string[]; connected: boolean; updated_at: string | null; }
export function useConnectors(): UseQueryResult<Connector[]>;
```

- [ ] `handlers.ts`: add `sampleConnectors` (gmail connected with both scopes + a not-connected catalog entry) and `http.get("/v1/connectors", () => HttpResponse.json(sampleConnectors))`.
- [ ] Write failing test: `useConnectors` (rendered via `renderWithClient`/`renderHook` with the query client) resolves to the sample list including a connected gmail.
- [ ] Run → FAIL.
- [ ] Implement `types.ts` + `useConnectors` (`useQuery({ queryKey: ["connectors"], queryFn: () => api.get<Connector[]>("/v1/connectors") })`).
- [ ] Run → PASS. lint + tsc clean.
- [ ] Commit.

### Task 4: Frontend — ConnectorsPage

**Files:** Create `web/src/features/connectors/ConnectorsPage.tsx`, `ConnectorsPage.test.tsx`.

- [ ] Write failing render test: given the MSW sample, the page shows "Gmail", both scope chips, a 正常 status, and the taint-rule banner text; a not-connected catalog entry shows a disabled 连接 hint.
- [ ] Run → FAIL.
- [ ] Implement `ConnectorsPage`: `Topbar` (title "Connectors", sub about (scope,connector) token storage, right scope badge); a "已连接 · N" `Card` with a table of connected connectors (icon+name, scope `Chip`s, green `Badge` 正常, `updated_at`); a `Banner tone="warn"` taint rule; a "可添加" grid of the not-connected catalog with disabled 连接 buttons titled "通过 CLI 授权（scripts/gmail_authorize.py）"; loading `Skeleton` + error `Banner`.
- [ ] Run → PASS. lint + tsc + `npm run build`.
- [ ] Commit.

### Task 5: Route + nav

**Files:** Modify `web/src/router.tsx`, `web/src/components/Sidebar.tsx`; update `web/src/components/AppShell.test.tsx` (now 3 real links).

- [ ] `router.tsx`: add `{ path: "connectors", element: <ConnectorsPage /> }`.
- [ ] `Sidebar.tsx`: give the Connectors item `to: "/connectors"`.
- [ ] `AppShell.test.tsx`: assert Connectors is among the real links (and pick a still-disabled item, e.g. Sessions, for the placeholder test).
- [ ] `npm run test` green; lint clean.
- [ ] Commit.

### Task 6: Live verification (Docker server)

- [ ] Docker stack up (`keel-server` healthy, `connector_tokens` has the gmail row from earlier) + `npm run dev`.
- [ ] Open `/connectors` in the controlled browser: the 已连接 table shows **Gmail** with `gmail.readonly` + `gmail.send` chips and 正常; the taint banner renders; the addable catalog shows disabled connect hints. No console errors; screenshot.
