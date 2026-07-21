# Using Keel

This guide covers product workflows in the trusted preview. Start and operate the stack through
[Operations](./OPERATIONS.md). The live OpenAPI document at `http://localhost:8000/docs` is
authoritative for schemas.

## Authentication and workspace

The React preview supports:

- local preview;
- API key;
- directly supplied OIDC bearer token;
- optional organization and Agent selection.

It does not implement the OIDC redirect/callback login. `web:local` is the local-preview
compatibility scope, not the target product model.

## React surfaces

| Surface | Current use |
|---|---|
| Chat | Durable admission, streaming events, tools, approvals, and resume |
| Sessions | List, search, inspect, and resume conversations |
| Memory | Blocks, proposals, and consolidation controls |
| Knowledge | Knowledge Base/document lifecycle and cited search |
| Agents | Persisted Agent create/select preview |
| Connectors | Setup, resources, targets, health, sync, and disconnect |
| Projects | Create/import/list/detail/delete preview |
| Approvals | Durable pending/resolved decisions |
| Schedules | Current schedule management |
| Jobs | Durable job status/cancellation |
| Observability | Local operational overview |

Review and controlled Patch do not yet have React routes.

## Chat and sessions

Start from `/chat`; reopen prior conversations from Sessions.

API admission:

```powershell
$session = [guid]::NewGuid().ToString()
$body = @{ content = "Reply briefly and do not call tools." } | ConvertTo-Json
Invoke-RestMethod "http://localhost:8000/v1/sessions/$session/messages" `
  -Method Post -ContentType "application/json" -Body $body
```

Follow `GET /v1/sessions/{session_id}/events` as replayable SSE.

## Memory and Knowledge

Memory is small Agent/session context; Knowledge is cited document content. See
[Memory](./MEMORY.md) and [Knowledge](./KNOWLEDGE.md).

Useful reads:

```powershell
Invoke-RestMethod http://localhost:8000/v1/memory/blocks
Invoke-RestMethod http://localhost:8000/v1/memory/proposals
Invoke-RestMethod http://localhost:8000/v1/knowledge-bases
```

## Connections

Provider setup is documented in [Connector providers](./connectors/README.md). Current
implementations include Gmail, Calendar, Drive/Docs, Microsoft 365, Notion, Feishu, GitHub
collaboration, RSS/Atom, and webhook at differing maturity.

External content is tainted. Outbound effects remain approval/idempotency governed.

## Projects and review

Projects may be blank or imported through a linked GitHub App installation.

```powershell
Invoke-RestMethod http://localhost:8000/v1/projects
```

Read-only review lives under `/v1/projects/{project_id}/reviews`. Patch workers are not reachable
through a public API yet.

## Schedules, jobs, and approvals

```powershell
Invoke-RestMethod http://localhost:8000/v1/schedules
Invoke-RestMethod http://localhost:8000/v1/jobs
Invoke-RestMethod http://localhost:8000/v1/approvals
```

Mutations affect durable preview data. Current schedules are a precursor to first-class Routines.

## CLI

```powershell
uv sync --frozen
uv run keel version
uv run keel ping
uv run keel chat --workspace .
uv run keel run "Summarize README.md without modifying files" --workspace .
```

The CLI is an operator/developer surface. Avoid broad allow flags outside disposable workspaces.

For a guided walkthrough, use [Demo](./DEMO.md).
