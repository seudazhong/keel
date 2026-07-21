# Keel trusted-preview demo

This walkthrough demonstrates current product value without implying browser multi-user,
production, shell, review-UI, or patch-UI readiness.

## Prerequisites

- Docker Desktop;
- a configured chat-capable model/provider;
- optional connector credentials;
- ports `3000`, `8000`, `5432`, and `6379`.

## 1. Start and verify

```powershell
Set-Location C:\src\keel
docker compose -f docker-compose.yml --profile dev up -d --build
docker compose -f docker-compose.yml --profile dev ps
Invoke-RestMethod http://localhost:8000/readiness | ConvertTo-Json -Depth 6
```

Expected: `ready=true`, least-privilege runtime DB principal, shared run substrate/queue, Knowledge
dispatch, and sandbox all healthy.

## 2. Enter the React preview

```powershell
Start-Process http://localhost:3000/
```

Choose local preview only on a trusted machine. The sidebar should expose Chat, Sessions, Memory,
Knowledge, Projects, Agents, Connectors, Approvals, Schedules, Observability, and Jobs.

Projects and Agents are explicitly labeled Preview.

## 3. Demonstrate durable chat and resume

1. Start a chat with:
   **"Describe Keel in two sentences. Do not call tools."**
2. Navigate to Sessions.
3. Open the session and resume it.
4. Return to Chat and confirm the route/session identity remains stable.

The purpose is to show durable admission, replayable history, and worker-owned execution.

## 4. Demonstrate Memory and Knowledge

### Memory

- Open Memory.
- Inspect current blocks and any consolidation proposals.
- Explain that model-learned changes should become proposal-first as the product model is tightened.

### Knowledge

On an empty local stack, seed deterministic demo content:

```powershell
$env:KEEL_APP_ENV = "dev"
uv run python scripts/seed_demo_data.py --dry-run
uv run python scripts/seed_demo_data.py --yes
```

Then:

- open Knowledge;
- inspect the demo Knowledge Base/document;
- run a search and show citation/source metadata.

## 5. Demonstrate Connections

Open Connectors and show:

- manifest-driven provider catalog;
- connected/setup states;
- selected resources and targets;
- health/sync/disconnect controls;
- the warning that external content is tainted and outbound actions require approval.

If Gmail or Calendar is already connected, ask a read-only question such as:

```text
List the next few calendar events and cite the connected source. Do not create or update anything.
```

Do not configure new provider secrets during a short demo.

## 6. Demonstrate Agents and Projects

### Agents

- Show persisted Agent records and selection.
- State honestly that full model/tool/resource/memory/budget configuration is not yet represented
  in the Agent record.

### Projects

- Show the Project list/import flow.
- If a GitHub App installation is already configured, import a disposable repository.
- Explain that read-only review exists through the API but has no React page.
- Explain that controlled patch workers exist but no public Patch API/UI ships.

## 7. Demonstrate operational truth

Open:

- Jobs;
- Approvals;
- Schedules;
- Observability.

Show status and feedback only. Do not approve unknown actions, run unfamiliar schedules, cancel
valuable jobs, revoke Connections, or delete data.

## 8. Optional API evidence

```powershell
Invoke-RestMethod http://localhost:8000/v1/sessions
Invoke-RestMethod http://localhost:8000/v1/jobs
Invoke-RestMethod http://localhost:8000/v1/knowledge-bases
Invoke-RestMethod http://localhost:8000/v1/connectors
Invoke-RestMethod http://localhost:8000/v1/projects
```

The live `/docs` exposes the complete current API, including identity, IM routing, lifecycle, and
read-only review.

## 9. Automated browser smoke

Against an already-running stack:

```powershell
Set-Location C:\src\keel\web
npm ci
npm run test:e2e:install
npm run test:e2e
```

The smoke must not send mail, create calendar effects, resolve unknown approvals, run destructive
schedules, or delete data.

## Demo boundaries

Do not claim:

- browser OIDC login;
- completed multi-user/team administration;
- production or hostile multi-tenant readiness;
- shell/build/test isolation;
- review or patch React workflows;
- full OTel/SLO/DR.

The demo is successful when it shows one coherent trusted-preview Agent experience and makes every
remaining boundary explicit.
