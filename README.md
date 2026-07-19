<!-- Keel — cloud-native personal and team agent platform -->
<h1 align="center">⛵ Keel</h1>
<p align="center"><em>A durable agent runtime for private personal agents and explicitly shared team agents, reached through web, IM, and operator tools.</em></p>

---

Keel's target is a **multi-user, cloud-native agent platform**:

- every user has a private, persisted personal agent;
- team agents share only resources explicitly granted to them;
- web and IM are surfaces of the same durable runtime, memory, permissions, approvals, and
  audit trail;
- native connectors provide depth for core services, while MCP and automation platforms
  cover the integration long tail.

That is the product direction, not the current feature claim. Today Keel has a strong,
tested, deployable **single-operator** engine and a broad React surface on a **completed
safety foundation**: the data plane runs as a non-owner least-privilege runtime DB login with
enforced RLS (M3A), and shell/file execution runs in a deployed authenticated isolated sandbox
(M3B). It is not yet a multi-user product: there is no browser login flow, and the sandbox is a
single-operator rootless-OCI boundary rather than a hardened multi-tenant microVM.

## Maturity scale

Capabilities are rated on four independent levels. A higher level never implies a lower one,
and code or passing tests (C/T) are **never** reported as a usable product scenario (P).

- **C — Code** exists on `main`.
- **T — Tested** by automated tests that pass in CI.
- **D — Deployable** in the standard Compose local-preview stack.
- **P — Product** end-to-end scenario works through a shipped surface (not just an API/preview).

| Track | Maturity |
|---|---|
| Agent/data engine | C/T/D solid: durable sessions/runs/jobs/approvals/schedules, memory/search/consolidation/evals, Knowledge RAG. P is single-operator preview. |
| Product surface | C/T/D present: React app with Chat, Sessions, Jobs, Memory, Knowledge, Connectors, onboarding, i18n, Agents, Projects. P is preview; many journeys need identity/login and backend work. |
| Production readiness | Pre-production: safety foundation complete (M3A non-owner least-privilege runtime DB login + RLS; M3B deployed isolated sandbox), but sandbox is a single-operator rootless-OCI boundary (not a multi-tenant microVM), no browser OIDC, no production scheduler/OTel/DR. |

Read [`docs/STATUS.md`](./docs/STATUS.md) for the C/T/D/P capability table, verified evidence,
and blockers, and [`docs/ROADMAP.md`](./docs/ROADMAP.md) for the active M0–M9 sequence.

## What works now

Verified on `main` at `b885f0d` (non-integration 1924 passed / 2 skipped; integration 397
passed; frontend 116; Playwright 18/18; migration head `0020` applied by the standard Compose
`migrate → runtime-secret-init → provision → sandbox → server/worker/web` startup):

- FastAPI chat with SSE, tool timeline, and approvals
- durable sessions/runs/jobs/schedules with cancellation/retry/recovery, and session search
- core/archival memory, consolidation proposals, and deterministic memory evals
- Knowledge Base RAG: lifecycle, durable ingest/delete, hybrid retrieval, citations, and taint
- identity/org/agents/grants APIs and read-only code-review API + worker (both headless)
- projects/GitHub App/storage backend
- Gmail OAuth/status/read/approval-gated send, plus OneBot/Telegram IM routing
- Compose serves the built **React application** (Chat, Sessions, Jobs, Memory, Knowledge,
  Connectors, Observability), with onboarding, i18n, and Agents/Projects surfaces
- non-owner least-privilege runtime DB login (`keel_runtime_login`) with enforced RLS, and a
  deployed HMAC-authenticated isolated sandbox for file execution (readiness reports
  `runtime_db_principal = 'least-privilege (keel_runtime_login)'` and `sandbox = ok`)
- CLI local runtime with file, shell, and provider tools

The controlled Patch/Draft-PR foundation is **merged on `main`** (migration `0019`, plus
models/store/bundle/generation/approval with recovery+lease guard/trusted writeback/coordinator),
but it is a **C/T foundation only** — it has no API/SDK, worker jobs, dispatch outbox,
reconciler, or UI (those are milestones M4/M5), so it is **not yet product usable**. See
[`docs/STATUS.md`](./docs/STATUS.md).

Important limits (trusted single-operator local deployment — **not production-safe**): the
Compose `dev`/`full` stack runs a real authenticated `keel-sandbox` execution boundary (server/
worker use the fail-closed `sandbox` backend over an internal-only RPC network to a hardened,
credential-less executor) and the data plane runs as the non-owner least-privilege
`keel_runtime_login` (RLS/DDL/`SET ROLE` denied, passwordless URL + `0600` `PGPASSFILE`). Honest
residuals remain: the sandbox is the single-operator rootless-OCI floor — **not** a hardened
multi-tenant microVM — its internal network is bidirectional, and shell execution stays disabled
(file tools work, isolated per scope); the Kubernetes path is example manifests where an operator
runs `migrate`/`provision` themselves; there is no browser OIDC login flow; and the review
API+worker and IM routing ship without a UI. Do not expose this stack to untrusted networks.

## Run the development stack

Prerequisites: Docker Desktop and a configured model/provider in `.env`.

```powershell
Copy-Item .env.example .env
# Edit .env: set KEEL_DEFAULT_MODEL and its provider credentials.
docker compose -f docker-compose.yml --profile dev up -d --build
Invoke-RestMethod http://localhost:8000/readiness
```

Open `http://localhost:3000/` for the Compose-served React application,
`http://localhost:8000/` for the minimal server chat, or `http://localhost:8000/docs` for the
current API. For a live React dev server against the running `:8000` API:

```powershell
Set-Location web
npm ci
npm run dev
```

Use [`docs/DEMO.md`](./docs/DEMO.md) for a safe 10–15 minute walkthrough and
[`docs/USAGE.md`](./docs/USAGE.md) for CLI/API details.

## Documentation

Start with the canonical [`docs/README.md`](./docs/README.md) index.

- [Demo](./docs/DEMO.md)
- [Status](./docs/STATUS.md)
- [Roadmap](./docs/ROADMAP.md)
- [Product requirements](./docs/PRD.md)
- [Architecture and implementation fidelity](./docs/ARCHITECTURE.md)
- [Operations](./docs/OPERATIONS.md)
- [Development](./docs/DEVELOPMENT.md)

## Stack

Implemented foundations use Python 3.12/asyncio, FastAPI, LiteLLM, PostgreSQL + pgvector,
Redis/arq, React + Vite, and Docker Compose, including a separate authenticated `keel-sandbox`
execution service. The broader target architecture adds a hardened multi-tenant (microVM)
sandbox, a separate scheduler service, full observability, generated SDKs, and production
delivery profiles; those remain roadmap work.

## License

No repository license file has been added yet. Do not assume a license from historical
planning text.
