<!-- Keel — governed cloud agents -->
<h1 align="center">Keel</h1>
<p align="center"><em>A durable, governed runtime for private personal agents and explicitly shared team agents.</em></p>

Keel is a server-hosted agent platform built around durable conversations, memory, Knowledge,
connectors, routines, approvals, and auditable effects. Its first product boundary is a
single-organization deployment in which every user can have a private personal Agent and teams can
share Agents through explicit access and resources through explicit grants.

Managed code projects, read-only review, and controlled patch proposals are supported capability
areas. They are not the identity of the product and do not bypass the same Agent, grant, approval,
and execution boundaries used elsewhere.

## Current state

`main` is a substantial **trusted local preview**, not a production or hostile multi-tenant
release.

Implemented foundations include:

- worker-owned durable runs, jobs, schedules, approvals, outboxes, recovery, and replayable events;
- a React application for Chat, Sessions, Memory, Knowledge, Connectors, Projects,
  Approvals, Schedules, Jobs, and Observability;
- identity, organization, Agent, grant, IM-routing, retention/erasure, and event-evolution backends;
- a manifest-driven connector runtime with Gmail, Google Calendar, Google Drive/Docs,
  Microsoft 365, Notion, Feishu, GitHub, RSS/Atom, and webhook providers at differing maturity;
- managed projects, GitHub App synchronization, and a read-only code-review API/worker;
- controlled patch generation/writeback workers, durable proposal/outbox state, and reconciliation;
- a non-owner runtime database login with enforced RLS;
- an authenticated out-of-process sandbox used by server and worker.

Important limits:

- the browser has no built-in OIDC authorization-code flow; local preview or directly supplied
  credentials are used instead;
- the Compose sandbox is one hardened rootless-OCI service with per-scope file namespaces, not a
  per-run microVM; shell/build/test execution remains disabled;
- read-only review has no React surface;
- controlled patch proposals have no public API, SDK, or UI yet;
- the separate production scheduler, full OTel/metrics/SLO coverage, tested DR, and production
  deployment profile are not complete.

See [`docs/STATUS.md`](./docs/STATUS.md) for the current capability matrix and
[`docs/ROADMAP.md`](./docs/ROADMAP.md) for the active plan.

## Run the trusted preview

Prerequisites: Docker Desktop and a configured chat-capable model/provider.

```powershell
Copy-Item .env.example .env
# Configure KEEL_DEFAULT_MODEL and the matching provider credentials.
docker compose -f docker-compose.yml --profile dev up -d --build
Invoke-RestMethod http://localhost:8000/readiness
```

Open:

- React application: `http://localhost:3000/`
- API documentation: `http://localhost:8000/docs`
- minimal server chat: `http://localhost:8000/`

Read [`docs/DEMO.md`](./docs/DEMO.md) for a safe walkthrough and
[`docs/USAGE.md`](./docs/USAGE.md) for current usage.

## Documentation

Start at [`docs/README.md`](./docs/README.md).

- [Product requirements](./docs/PRD.md)
- [Current status](./docs/STATUS.md)
- [Architecture](./docs/ARCHITECTURE.md)
- [Roadmap](./docs/ROADMAP.md)
- [Invariant acceptance specifications](./docs/INVARIANTS.md)
- [Operations](./docs/OPERATIONS.md)
- [Development](./docs/DEVELOPMENT.md)

## Stack

Python 3.12/asyncio, FastAPI, PostgreSQL + pgvector, Redis/arq, LiteLLM, React/Vite,
Docker Compose, and an authenticated `keel-sandbox` execution service.

## License

No repository license file has been added. Do not infer a license from historical planning text.
