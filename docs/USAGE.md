# Using Keel

This guide covers the current trusted preview. The running OpenAPI document at `/docs` is
authoritative for request/response schemas.

## Start

```powershell
Copy-Item .env.example .env
# Configure KEEL_DEFAULT_MODEL and provider credentials.
docker compose -f docker-compose.yml --profile dev up -d --build
Invoke-RestMethod http://localhost:8000/readiness
```

Open:

- React: `http://localhost:3000/`
- API: `http://localhost:8000/docs`
- minimal server chat: `http://localhost:8000/`

## Authentication and workspace

The React preview supports:

- local preview;
- API key;
- directly supplied OIDC bearer token;
- optional organization and Agent selection.

It does not perform an OIDC provider redirect/callback login. Do not treat the credential-entry
screen as a production browser-auth design.

In authenticated use, run data is routed through the selected organization and Agent. `web:local`
is the local-preview compatibility scope.

## React surfaces

| Surface | Current use |
|---|---|
| Chat | Durable message admission, streaming events, tool timeline, approvals, and session resume. |
| Sessions | List, search, inspect, and resume durable conversations. |
| Memory | Current blocks, versioned/proposed changes, and consolidation controls. |
| Knowledge | Knowledge Base/document lifecycle and cited search. |
| Agents | Create/select persisted personal/team Agent records; full policy configuration is not yet present. |
| Connectors | Generic setup, authorization, resources, targets, health, sync, and disconnect. |
| Projects | Create/import/list/detail/delete preview. |
| Approvals | Pending/resolved durable approvals. |
| Schedules | List/toggle/run current schedule rows. |
| Jobs | Durable job status and cancellation. |
| Observability | Current local overview, not a complete production telemetry product. |

Projects and Agents are labeled Preview. Read-only review and controlled patches do not yet have
React pages.

## Chat and sessions

Start a new chat from `/chat`. The browser keeps the durable session id in the route and can reopen
an existing session from Sessions.

Submitting through the API:

```powershell
$session = [guid]::NewGuid().ToString()
$body = @{ content = "Reply briefly and do not call tools." } | ConvertTo-Json
Invoke-RestMethod "http://localhost:8000/v1/sessions/$session/messages" `
  -Method Post -ContentType "application/json" -Body $body
```

Follow:

```text
GET /v1/sessions/{session_id}/events
```

as replayable SSE.

## Memory and Knowledge

Memory is small Agent/session context. Knowledge is cited external/user document content. They are
not interchangeable.

Useful reads:

```powershell
Invoke-RestMethod http://localhost:8000/v1/memory/blocks
Invoke-RestMethod http://localhost:8000/v1/memory/proposals
Invoke-RestMethod http://localhost:8000/v1/knowledge-bases
```

Knowledge ingestion runs as durable jobs and requires Postgres, Redis, worker, and embeddings.

## Connections

The generic Connectors page discovers installed provider manifests. Provider-specific setup is in
[`docs/connectors`](./connectors/README.md).

Current implementations include Gmail, Google Calendar, Google Drive/Docs, Microsoft 365, Notion,
Feishu, GitHub collaboration, RSS/Atom, and webhook.

Provider presence does not imply equal product maturity. Gmail and Calendar are the initial
connected-Agent product candidates; other providers remain subject to common qualification.

External content is tainted. Outbound effects require approval when influenced by tainted content
and use durable idempotency/reconciliation where implemented.

## Projects and review

Projects can be blank or imported through a linked GitHub App installation.

```powershell
Invoke-RestMethod http://localhost:8000/v1/projects
```

Read-only review endpoints live under:

```text
/v1/projects/{project_id}/reviews
```

The API/worker/report flow exists, but the React UI does not expose it.

Controlled patch workers are not reachable through a public API yet.

## Schedules, jobs, and approvals

```powershell
Invoke-RestMethod http://localhost:8000/v1/schedules
Invoke-RestMethod http://localhost:8000/v1/jobs
Invoke-RestMethod http://localhost:8000/v1/approvals
```

Run/toggle/cancel/approve/reject operations mutate durable state. Use disposable preview data.

Current schedule rows are a precursor to first-class Routines and may execute only the built-in
supported agent kinds.

## CLI

```powershell
uv sync --frozen
uv run keel version
uv run keel ping
uv run keel chat --workspace .
uv run keel run "Summarize README.md without modifying files" --workspace .
```

The CLI is an operator/developer surface. Mutating tools should ask. Avoid broad allow flags outside
disposable workspaces.

## Demo data

For an unmistakably local development stack:

```powershell
$env:KEEL_APP_ENV = "dev"
uv run python scripts/seed_demo_data.py --dry-run
uv run python scripts/seed_demo_data.py --yes
```

The command is guarded and idempotent. See [Demo](./DEMO.md).
