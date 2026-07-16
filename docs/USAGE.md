# Using Keel

Keel currently provides a minimal server-rendered chat, a separate React development UI,
a local CLI runtime, and REST/SSE APIs. The implementation is still single-scope
(`web:local`) and is not a multi-user product.

## Start the current Compose stack

```powershell
Copy-Item .env.example .env
# Edit .env and configure KEEL_DEFAULT_MODEL plus the matching provider credentials.
docker compose --profile dev up -d --build
Invoke-RestMethod http://localhost:8000/readiness
```

Open:

- `http://localhost:8000/` — minimal chat
- `http://localhost:8000/docs` — OpenAPI
- `http://localhost:8000/approvals` — server-rendered durable approvals page
- `http://localhost:3000/` — Compose static stub only

Sessions, connector status, schedules, and admin overview are available through `/v1` APIs
and the separately run React UI; they are not standalone server-rendered pages.

See [Demo](./DEMO.md) for a safe walkthrough.

## CLI

Install the locked workspace and inspect commands:

```powershell
uv sync --frozen
uv run keel version
uv run keel ping
uv run keel chat --workspace .
uv run keel run "Summarize README.md without modifying files" --workspace .
```

CLI sessions are in memory unless `--durable` is supplied. On Windows, durable Postgres
mode uses a selector event loop and cannot be combined reliably with shell subprocesses.
Mutating tools ask by default. Avoid `--allow-all` and `--yes` outside disposable workspaces.

## REST and streaming

The API is under `/v1`; the running `/docs` is authoritative. Common reads:

```powershell
Invoke-RestMethod http://localhost:8000/v1/sessions
Invoke-RestMethod http://localhost:8000/v1/jobs
Invoke-RestMethod http://localhost:8000/v1/knowledge-bases
Invoke-RestMethod http://localhost:8000/v1/connectors
```

Submitting a message is a mutation:

```powershell
$session = [guid]::NewGuid().ToString()
$body = @{ content = "Reply briefly; do not call tools." } | ConvertTo-Json
Invoke-RestMethod "http://localhost:8000/v1/sessions/$session/messages" `
  -Method Post -ContentType "application/json" -Body $body
```

Then follow `GET /v1/sessions/{id}/events` as SSE. If `KEEL_API_KEYS` is configured, pass
`X-API-Key` or a Bearer token with the required role.

## React UI

The React app is developed independently from Compose:

```powershell
Set-Location web
npm ci
npm run dev
```

Vite normally serves `http://localhost:5173` and proxies API calls to `:8000`. Current
routes include chat, sessions, connectors, Knowledge, schedules, approvals, overview, and
settings. Responsive layout, i18n, accessibility completion, onboarding, identity, and a
real agent switcher remain roadmap work.

## Gmail

Gmail is the only native connector today. Configure a Desktop OAuth client and a shared
`KEEL_SECRET_KEY`, then:

```powershell
uv run python scripts/gmail_authorize.py --scope web:local
```

Enable Gmail in the worker environment. Read access and connection status can be used
without enabling real sends. `KEEL_GMAIL_SEND_ENABLED=1` enables production mutations;
leave it off for development and demos.

## Knowledge and jobs

Current-main exposes Knowledge Base CRUD/search and durable job list/detail/cancel APIs.
Ingestion needs Postgres, Redis/arq, a worker, and an embedding provider. New stacks contain
no Knowledge data. Use the OpenAPI schemas rather than copying old dated plans.
