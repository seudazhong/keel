# Keel trusted-preview demo

This walkthrough assumes the Compose preview is already healthy. Use
[Operations](./OPERATIONS.md) to start and diagnose the stack.

Open `http://localhost:3000/` and choose local preview only on a trusted machine.

## 1. Durable chat and resume

1. Start a chat with: **"Describe Keel in two sentences. Do not call tools."**
2. Navigate to Sessions.
3. Reopen the session and continue it.
4. Confirm the route/session identity remains stable.

This demonstrates durable admission, replayable history, and worker-owned execution.

## 2. Memory and Knowledge

### Memory

- inspect current blocks and consolidation proposals;
- distinguish user-managed memory from model-proposed learning;
- state that direct model mutation remains a known preview limitation.

### Knowledge

If the stack has no demo content:

```powershell
$env:KEEL_APP_ENV = "dev"
uv run python scripts/seed_demo_data.py --dry-run
uv run python scripts/seed_demo_data.py --yes
```

Open Knowledge, inspect a document, run search, and show its citation/source metadata.

## 3. Connections

Show:

- provider catalog and connected/setup state;
- selected resources and targets;
- health/sync/disconnect controls;
- taint and approval messaging.

If Gmail or Calendar is already connected, use a read-only prompt such as:

```text
List the next few calendar events and cite the connected source. Do not create or update anything.
```

Do not configure secrets or perform outbound effects during a short demo.

## 4. Agents and Projects

### Agents

- show persisted Agent records and selection;
- explain that complete model/tool/resource/memory/budget configuration is still roadmap work.

### Projects

- show Project list/import;
- use only a disposable repository when GitHub is already configured;
- explain that review is API-only and Patch has no public API/UI.

## 5. Operational truth

Open Jobs, Approvals, Schedules, and Observability. Show status and feedback without resolving
unknown approvals, running unfamiliar schedules, cancelling valuable jobs, revoking Connections, or
deleting data.

The live API contract is at `http://localhost:8000/docs`.

## Automated smoke

The read-only browser smoke is documented in [Development](./DEVELOPMENT.md). It must not send mail,
create calendar effects, run destructive schedules, or delete data.

## Boundaries

Do not claim:

- browser OIDC login or finished multi-user/team administration;
- production or hostile multi-tenant readiness;
- shell/build/test isolation;
- review or patch React workflows;
- full OTel/SLO/DR.

A successful demo shows one coherent trusted-preview Agent journey and names every remaining
boundary.
