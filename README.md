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

That is the product direction, not the current feature claim. Today Keel has a strong
single-scope engine: a bounded tool-using runtime, durable sessions and jobs, approvals,
schedules, Gmail, memory/search/consolidation/evals, and a RAG/Knowledge Base vertical
slice. The product still lacks real user identity, persisted Agents CRUD, hard multi-user
isolation, Calendar, complete Web/IM parity, onboarding, and production delivery controls.

## Current maturity

| Track | Maturity |
|---|---|
| Agent/data engine | Late M3: Durable Jobs and Memory/Knowledge/Quality slices are complete and tested. |
| Product surface | Early M1: useful single-scope chat and management surfaces, but no real users or Agents model. |
| Production readiness | Pre-production: critical isolation, sandbox, durability, auth, webhook, delivery, and operations gaps remain. |

Read [`docs/STATUS.md`](./docs/STATUS.md) for verified evidence and blockers, and
[`docs/ROADMAP.md`](./docs/ROADMAP.md) for the active sequence.

## What works now

- FastAPI minimal chat with SSE, tool timeline, and approvals
- sessions/history and hybrid session search
- schedules, durable background jobs, cancellation/retry/recovery
- Gmail OAuth/status/read flow and approval-gated send path
- core/archival memory, consolidation proposals, deterministic memory evals
- Knowledge Base lifecycle, durable ingest/delete, hybrid retrieval, citations, and taint
- React application runnable with Vite
- OneBot and Telegram gateway slices
- CLI local runtime with file, shell, and provider tools

Important limits: the server scope is hard-coded to `web:local`; open mode is implicit
admin; the runtime DB owner can bypass RLS; the Compose stack runs the opt-in
`unsafe-local-dev` execution backend (a trusted local preview, not a real sandbox) with
shell execution disabled; interactive run state is process-local; Compose `:3000` serves a
static stub rather than the React bundle.

## Run the development stack

Prerequisites: Docker Desktop and a configured model/provider in `.env`.

```powershell
Copy-Item .env.example .env
# Edit .env: set KEEL_DEFAULT_MODEL and its provider credentials.
docker compose -f docker-compose.yml --profile dev up -d --build
Invoke-RestMethod http://localhost:8000/readiness
```

Open `http://localhost:8000/` for minimal chat or
`http://localhost:8000/docs` for the current API. For the actual React UI:

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
