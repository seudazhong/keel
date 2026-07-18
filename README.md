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
tested, deployable **single-operator** engine and a broad React surface. It is not yet a
multi-user product: there is no browser login flow, no real execution sandbox, and the
runtime database role still owns the schema.

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
| Production readiness | Pre-production: no real sandbox, runtime DB role still owns the schema, no browser OIDC, no production scheduler/OTel/DR. |

Read [`docs/STATUS.md`](./docs/STATUS.md) for the C/T/D/P capability table, verified evidence,
and blockers, and [`docs/ROADMAP.md`](./docs/ROADMAP.md) for the active M0–M9 sequence.

## What works now

Verified on `main` at `810a64c`:

- FastAPI chat with SSE, tool timeline, and approvals
- durable sessions/runs/jobs/schedules with cancellation/retry/recovery, and session search
- core/archival memory, consolidation proposals, and deterministic memory evals
- Knowledge Base RAG: lifecycle, durable ingest/delete, hybrid retrieval, citations, and taint
- identity/org/agents/grants APIs and read-only code-review API + worker (both headless)
- projects/GitHub App/storage backend
- Gmail OAuth/status/read/approval-gated send, plus OneBot/Telegram IM routing
- Compose serves the built **React application** (Chat, Sessions, Jobs, Memory, Knowledge,
  Connectors, Observability), with onboarding, i18n, and Agents/Projects surfaces
- CLI local runtime with file, shell, and provider tools

A controlled Patch/Draft-PR foundation is stable on branch `feat/future-patch-pr`
(commit `c97fc46`) but is **not** merged into `main` and has no API/worker/UI — it is not a
current product feature. See [`docs/STATUS.md`](./docs/STATUS.md).

Important limits (trusted single-operator local preview — **not production-safe**): the Compose
`dev`/`full` stack runs the opt-in `unsafe-local-dev` execution backend (a dedicated exec
volume, **not** a real sandbox) with shell execution disabled; the runtime DB role owns the
schema and can bypass RLS; there is no browser OIDC login flow; and the review API+worker and
IM routing ship without a UI. Do not expose this stack to untrusted networks.

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
Redis/arq, React + Vite, and Docker Compose. The broader target architecture includes
separate sandbox/scheduler services, full observability, generated SDKs, and production
delivery profiles; those remain roadmap work.

## License

No repository license file has been added yet. Do not assume a license from historical
planning text.
