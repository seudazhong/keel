# Keel demo guide

This guide favors a predictable, read-only demonstration over feature breadth. The stack
already running on `http://localhost:8000` may be healthy but built from an older worktree.
Do not infer current-main API availability from health alone.

## Demo matrix

| Surface | Existing stale `:8000` stack | Rebuilt current `main` | Requirements / caveats |
|---|---|---|---|
| Minimal chat `/` | Available | Available | A working configured LLM model/key. Tool calls may request approval. |
| Approvals `/approvals` | Available | Available | Read the queue only unless the approval is disposable. |
| Sessions/history | Available | Available | Existing session data improves the demo. |
| Gmail status | Available | Available | Status is safe to read. Connecting needs OAuth credentials; revoking is destructive. |
| Schedules | Available | Available | List only. "Run now" and pause/resume mutate state. |
| Admin overview | Available | Available | Open mode grants implicit admin; this is not production auth. |
| API docs `/docs` | Available | Available | Best source for the exact API exposed by the running image. |
| `/v1/jobs` | Usually absent | Available | Requires current-main rebuild and Postgres/Redis. |
| `/v1/knowledge-bases` | Usually absent | Available | Requires rebuild; useful results require seeded KB data and embeddings. |
| React app | Run Vite separately | Run Vite separately | Compose `keel-web` on `:3000` is a static stub, not the React bundle. |

## Prerequisites

- Docker Desktop with Compose, PowerShell 7+, and ports `8000`, `3000`, `5432`, and `6379`
  available when rebuilding.
- For chat: set a valid provider configuration in `.env` or the service environment.
- For Gmail: a Desktop OAuth client, `KEEL_SECRET_KEY`, and an authorized token.
- For Knowledge search: current-main containers plus seeded/ingested data. A clean stack has
  no demo KB content.

## Safe 10–15 minute Windows PowerShell script

Run from the repository root.

### 1. Establish what is running (1 minute)

```powershell
Set-Location C:\src\keel
$base = "http://localhost:8000"
Invoke-RestMethod "$base/health" | ConvertTo-Json
Invoke-RestMethod "$base/readiness" | ConvertTo-Json -Depth 4
```

Expected: health names `keel-server`; readiness is HTTP 200 with Postgres and Redis `ok`.
If either fails, use the rebuild path below.

### 2. Detect stale versus current-main API (1 minute)

```powershell
$jobs = Invoke-WebRequest "$base/v1/jobs" -SkipHttpErrorCheck
$kb = Invoke-WebRequest "$base/v1/knowledge-bases" -SkipHttpErrorCheck
"jobs=$($jobs.StatusCode) knowledge=$($kb.StatusCode)"
```

Expected on the older running image: one or both are `404`. Expected after a current-main
rebuild: both are `200` (often returning `[]`).

### 3. Demonstrate safe read surfaces (4–6 minutes)

Open the two server-rendered surfaces and API docs:

```powershell
Start-Process "$base/"
Start-Process "$base/approvals"
Start-Process "$base/docs"
```

Show sessions/history, Gmail connection status, schedules, and admin overview through their
read-only APIs (or through the React/Vite UI in step 5):

```powershell
Invoke-RestMethod "$base/v1/sessions" | ConvertTo-Json -Depth 6
Invoke-RestMethod "$base/v1/approvals?status=pending" | ConvertTo-Json -Depth 6
Invoke-RestMethod "$base/v1/connectors" | ConvertTo-Json -Depth 6
Invoke-RestMethod "$base/v1/schedules" | ConvertTo-Json -Depth 6
Invoke-RestMethod "$base/v1/admin/overview" | ConvertTo-Json -Depth 8
```

If sessions exist, inspect one without mutation:

```powershell
$sessions = @(Invoke-RestMethod "$base/v1/sessions")
if ($sessions.Count -gt 0) {
  Invoke-RestMethod "$base/v1/sessions/$($sessions[0].id)/history" |
    ConvertTo-Json -Depth 10
}
```

For chat, use a harmless prompt such as: **"Reply with a two-sentence description of
Keel. Do not call tools."** Expected: streamed text and a named run completion. If provider
credentials are unavailable, skip chat and continue with read-only management surfaces.

### 4. Optional current-main rebuild (3–8 minutes)

This replaces the currently running images and may take longer on a cold machine:

```powershell
docker compose --profile dev up -d --build
docker compose --profile dev ps
Invoke-RestMethod "$base/readiness" | ConvertTo-Json -Depth 4
Invoke-RestMethod "$base/v1/jobs" | ConvertTo-Json -Depth 6
Invoke-RestMethod "$base/v1/knowledge-bases" | ConvertTo-Json -Depth 6
```

Expected: services become healthy; jobs and Knowledge endpoints return JSON. Empty arrays
are correct on an unseeded stack.

### 5. Optional React UI via Vite (2 minutes)

Compose `:3000` is a static readiness stub. To demonstrate the actual React application:

```powershell
Set-Location C:\src\keel\web
npm ci
npm run dev
```

Open the URL Vite prints (normally `http://localhost:5173`). Vite proxies `/v1` and
`/health` to `:8000`. Stop it with `Ctrl+C` after the demo.

## Safety and fallbacks

- Do **not** send Gmail, revoke connectors, approve unknown actions, run schedules, cancel
  jobs, delete Knowledge data, or change production-like settings during a demo.
- Open mode has no user authentication and treats callers as admin. Bind only to trusted
  local networks.
- Session semantic search may be slow or degrade to lexical mode while embeddings catch up.
- If chat fails, show `/docs`, health/readiness, sessions/history, approvals, schedules, and
  admin overview.
- If rebuild fails, restore the prior stack with the exact image/worktree procedure used by
  its operator; do not delete volumes. Capture `docker compose logs --tail 100 keel-server
  keel-worker` for diagnosis.
