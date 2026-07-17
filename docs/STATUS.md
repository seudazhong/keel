# Keel implementation status

> **Snapshot:** 2026-07-16 · **Branch:** `main`
> **Target:** [PRD](./PRD.md) · **Architecture fidelity:** [ARCHITECTURE](./ARCHITECTURE.md#0-implementation-status-and-fidelity) · **Active execution:** [ROADMAP](./ROADMAP.md)

## Summary

Keel has **late-M3 engine/data maturity**, an **M1 product surface**, and
**pre-production operational readiness**. Durable Jobs and the Memory/Knowledge/Quality
track are complete. The current product remains a hard-coded single-scope/single-agent
system and should not be presented as the target multi-user platform.

## Maturity by track

| Track | Maturity | Evidence | Main gap |
|---|---|---|---|
| Agent runtime and data engine | Late M3 | Bounded loop, durable event/session state, tools, approvals, schedules, Durable Jobs, memory/search/consolidation, deterministic evals, Knowledge lifecycle/search/citations/taint. | Event upcasters, retention/erasure, isolated execution, durable interactive topology. |
| Product surface | M1, M3.1 in progress | Compose-delivered React app, chat, sessions, approvals, Gmail status, schedules, admin overview, Knowledge, Jobs, Memory proposals, demo bootstrap, Playwright smoke, OneBot/Telegram slices. | Local first-run wizard, identity, Agents CRUD/switcher, Calendar, complete Memory/Admin governance, Web/IM parity, responsive/i18n/a11y. |
| Production readiness | Pre-production | Compose dev stack, migrations, health/readiness, RBAC tiers, core CI, durable jobs recovery tests. | Enforced RLS role, real sandbox, durable auth/OAuth/webhook/idempotency, accurate delivery profiles, scale/SLO/DR/security gates. |

## Verified completed capabilities

### Runtime and autonomy

- Persisted agent loop, streaming/tool events, interrupt, permission/approval paths.
- LiteLLM gateway with chat-completions and Responses API paths.
- Cron/interval/one-shot schedules run by worker cron.
- Durable Jobs:
  - Postgres lifecycle source of truth with scoped rows/RLS policies;
  - at-least-once arq delivery with DB lease/reclaim;
  - progress, cooperative cancellation, bounded retries;
  - duplicate/crash recovery and attempt exhaustion;
  - exactly-once terminal result injection;
  - list/detail/cancel API and RBAC.
- Durable interactive runs (M3.6, WS-M):
  - Postgres `runs` state machine (admitted/queued/running/waiting_approval/completed/
    failed/cancelled/interrupted/expired) with scope RLS + FORCE RLS, fenced `lease_token`,
    optimistic `version`, attempt counter, budget/cost summary, and single-active-owner
    constraint; reversible migration `0014_durable_runs`.
  - Atomic claim/heartbeat/renew/release, lease-expiry reclaim with fencing, idempotent
    admission (`(scope, idempotency_key)` unique) and idempotent terminalization
    (`keel_core/runs.py`).
  - Worker-owned execution reusing the single agent loop via
    `keel_core.run_service.execute_run` + `keel_worker.runs.run_interactive`; the server
    admits + streams and no longer owns the run task for the durable path. A per-run lease
    keeper renews the fenced lease well before expiry and, on a lost renewal, fences the run
    so no further model/tool/event/terminal write proceeds under a stale lease; the loop
    budget is the authoritative persisted `max_iterations`/`token_budget`.
  - Admission is crash-safe and idempotently repairable: the user turn is always persisted
    before the queued/dispatch transition, a retried request completes exactly the missing
    steps, and reconciliation never dispatches a prompt-less run.
  - Durable interrupt/cancel/steering (`run_control`) use claim/ack semantics — steering is
    acked only after its durable turn is appended; interrupt/cancel are re-honored by a
    reclaiming worker after a crash. Durable approvals resolve through `DurableRunService`
    bound to org/actor/action-hash/attempt/run-state/expiry (never optional), persist an
    explicit resume marker (`resume_requested`) captured atomically at claim time, and are
    routed separately from legacy scheduled `resume_run`; approval expiry resumes the run to
    record the denial. Run status/steer/interrupt/approval APIs enforce the run's `org_id`
    (cross-org user access answers 404) while preserving local-preview/API-key compatibility;
    queue/lease reconciliation cron.
  - Evidence: `tests/unit/test_runs_state_machine.py`, `tests/unit/test_run_service.py`,
    `tests/unit/test_runs_api.py` (production `/v1` wrappers: org authz + approval routing),
    `tests/integration/test_runs_postgres.py` (two-worker claim race, lease
    expiry/reclaim/fencing, resume-marker capture, redispatch of queued, peek/ack controls,
    duplicate admission, durable interrupt across restart, RLS cross-scope denial, stale
    approval-hash/expiry deny). See _Remaining limitations_ below.

### Memory, search, and quality

- Versioned core memory and self-editing tools.
- Archival pgvector plus lexical/semantic hybrid retrieval.
- Hybrid session recall with explicit `hybrid`, `lexical`, or degraded mode.
- Proposal-first memory consolidation with evidence constraints, cursor/lease, CAS, and
  retry deduplication.
- Deterministic Memory eval datasets, replay cassettes, reports, gates, optional judge, and
  optional Langfuse reporting.

### Knowledge Base

- Scope-bound KB/document/version/chunk lifecycle.
- Text/Markdown create, update, reindex, immediate-hide delete, and durable purge.
- Deterministic chunking and pinned embedding model/dimension.
- Hybrid retrieval, stable structured citations, and always-tainted `kb_search`.
- REST/RBAC and React management/search UI.
- Durable ingest/delete jobs with ownership fencing, active/desired rollback safety, and
  zombie-write prevention.

### Current surfaces/integrations

- Server-rendered minimal chat and management pages.
- Compose `:3000` serves the built React app through nginx, including SPA fallback and
  `/v1`, `/health`, `/readiness`, and SSE proxying.
- React Jobs and Memory proposal pages expose current backend contracts.
- Guarded, idempotent demo bootstrap seeds searchable Knowledge content and a welcome session.
- Gmail is the only native connector; OAuth/read/status/revoke paths exist, with optional
  approval-gated real send.
- OneBot and Telegram gateway code exists.

## Verified baselines

The final M3.1 increment validation on 2026-07-16 reported:

- Python non-integration: **828 passed / 1 skipped**; isolated Postgres/Redis integration:
  **239 passed**.
- React/Vitest: **53 passed**; Playwright Compose smoke: **9 passed** against the isolated
  stack before and after demo seeding.
- Ruff lint/format, strict mypy, web lint/build, Compose config, app/web image builds, and
  live nginx syntax: passed.
- Demo bootstrap: dry-run credential redaction, production guard refusal, isolated seed,
  searchable 3-document corpus, and idempotent replay passed.
- The populated Playwright smoke left sessions, events, Jobs, Knowledge, Memory proposals,
  approvals, schedules, and connector-token row counts unchanged.
- Memory replay: **12/12 cases**, **7/7 gates**, weighted overall **0.982**, no live fallback.
- Knowledge replay: **8/8 cases**, **7/7 gates**, all named retrieval/citation/taint/deletion
  measures **1.000**, no live embedding fallback.
- Durable Jobs acceptance: real Postgres + Redis/arq covering lost/duplicate delivery,
  retry, crash/reclaim, exhaustion, cancel, and exactly-once injection.
- Knowledge live smoke: create → ingest → cited/tainted search → safe update/activation →
  immediate-hide delete → durable purge.

The Memory/Knowledge eval figures are retained feature-audit baselines; the other figures above
come from the final isolated M3.1 increment run.

## M3.1 increment status

**In progress.** This increment completes real Compose React delivery, truthful Jobs/Memory
surfaces, safe demo bootstrap data, stale-stack detection, and a non-destructive browser smoke.
M3.1 remains open because the planned local first-run wizard for provider/secret/default-Agent
and optional connector setup is not implemented. Product-state copy still needs a dedicated
exit-gate audit before declaring the milestone complete.

## Critical and high blockers

1. **RLS bypass:** the runtime DB role owns the schema/database and can bypass RLS.
2. **No real sandbox:** shell executes inside server/CLI processes.
3. **Process-local interaction:** interactive runs and some approvals are not restart-safe
   or worker-owned.
4. **No identity/Agents:** fixed `web:local`; no users, organizations, persisted Agents,
   memberships, or grants.
5. **Weak API credential model:** configured keys are plaintext/unscoped; empty means
   implicit admin.
6. **OAuth/gateway safety:** OAuth state is process-local; gateway webhooks are
   unauthenticated.
7. **Outbound retry safety:** idempotency is process-local.
8. **Permission construction:** a default can become allow-all when omitted in some paths;
   explicit fail-closed defaults are not universal.
9. **Event/data lifecycle:** event versions exist, but no upcasters, retention, or complete
   erasure.
10. **Scheduler/topology:** scheduler package is a stub; worker cron schedules jobs;
    interactive runtime is not the target server/worker topology.
11. **Delivery profile gap:** Compose `:3000` now delivers the React app, but `full` still does
    not deliver the documented observability/object-store/sandbox stack.
12. **SDK/operations:** no generated-client/versioning pipeline; observability, CI security/
    performance coverage, backup/restore, and DR are below target.

## Current product gaps

- User sign-in/session identity and single-organization-v1 membership/RBAC.
- Agents CRUD, persisted personal/team Agents, explicit resource grants, real switcher.
- Calendar and a reusable connector/trigger framework.
- Web/IM runtime and approval parity.
- Complete Memory block/history editing, Admin/RBAC UI, and local first-run onboarding.
- Correct current copy, responsive behavior, internationalization, and accessibility.

### Durable runs — routing status (M3.6, WS-M)

The durable substrate (schema, run state machine/repository, worker executor, durable
approval binding, reconciliation, `/v1/runs` status/interrupt/steer APIs, lifecycle
erasure) is implemented and tested. Routing status:

- **Web admission is durable by default.** `POST /v1/sessions/{id}/messages` admits through
  the identity-bound `DurableRunService.admit` and dispatches the worker-owned
  `run_interactive` job; there is no server-local asyncio run task and no in-process fallback
  (a live run queue is required — explicit 503 otherwise). The tenant/actor/Agent identity is
  derived from the request actor + selected org (`X-Keel-Org`) + selected Agent
  (`X-Keel-Agent`, re-authorized via `select_agent`); an idempotency key (`Idempotency-Key`
  header/body) makes a retried message admit exactly once. The in-process `AgentRuntime`
  remains only as the explicitly-labelled **local-preview** path (`admit_and_run`), never the
  production default. Evidence: `tests/unit/test_message_routing.py`, `tests/unit/test_server.py`.
- **Local-preview compatibility profile** (`local` org + `web` Agent + `im:`/`local:` actor)
  is used only in non-cloud mode; a cloud request with no authenticated user + selected
  org/Agent fails closed (403). Authorization is org-bound (`select_org`/`select_agent`/
  `_authorize_run`) — the `web:local` data-plane scope is never an authorization basis.
- **Worker execution parity.** `run_interactive` rebuilds the interactive Agent from the
  *persisted* selected-Agent profile (id/name/persona) via the shared builders
  (`keel_core.interactive`), with file/shell + memory + Knowledge tool + permission parity to
  the server web runtime, and re-checks Agent visibility / org membership / archived status at
  claim time (a revoke between admit and claim fails the run closed). Evidence:
  `tests/unit/test_worker_run_routing.py`.

Still to do (not yet done):

- **IM durable routing.** OneBot/Telegram gateways (`ImRunner`) still run the in-process
  untrusted safe-agent loop and reply inline. Re-pointing them at `DurableRunService`
  requires an untrusted **safe-agent** branch in the worker (IM must keep the read-only safe
  toolset — it must never reach write/shell), a durable outbox-idempotent reply delivery that
  survives a server/worker restart, a reconciler sweep for the crash-after-terminalize
  delivery window, and a cloud channel→org/Agent mapping (fail closed when absent). Designed
  but not implemented in this increment.
- **Connector tool parity** in the durable worker path (beyond file/shell + memory +
  Knowledge) is a follow-up.
- **SSE token-streaming liveness** from the worker: durable events (including completed
  assistant turns) appear via the server's durable-polling SSE tail, but sub-100ms
  token-by-token deltas are not fanned out from the worker to Redis yet.

## Next work

Follow [Roadmap](./ROADMAP.md), in order:

1. M3.1 Demo-ready Product Surface
2. M3.2 Personal Agent Experience Preview
3. M3.3 Cloud Safety Foundation
4. M3.4 Event Evolution
5. M3.5 Retention/Erasure
6. M3.6 Multi-user Identity, Access, and durable run topology
7. M3.7 Connector and Team Experience
8. M3.8 Production Delivery and Scale

Plugin SDK and Desktop are deferred until those gates.
