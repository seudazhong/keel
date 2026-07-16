# Keel implementation status

> **Snapshot:** 2026-07-16 · **Branch:** `main`
> **Target:** [PRD](./PRD.md) · **Architecture fidelity:** [ARCHITECTURE](./ARCHITECTURE.md#0-implementation-status-and-fidelity) · **Active execution:** [ROADMAP](./ROADMAP.md)

## Summary

Keel has **late-M3 engine/data maturity**, an **early-M1 product surface**, and
**pre-production operational readiness**. Durable Jobs and the Memory/Knowledge/Quality
track are complete. The current product remains a hard-coded single-scope/single-agent
system and should not be presented as the target multi-user platform.

## Maturity by track

| Track | Maturity | Evidence | Main gap |
|---|---|---|---|
| Agent runtime and data engine | Late M3 | Bounded loop, durable event/session state, tools, approvals, schedules, Durable Jobs, memory/search/consolidation, deterministic evals, Knowledge lifecycle/search/citations/taint. | Event upcasters, retention/erasure, isolated execution, durable interactive topology. |
| Product surface | Early M1 | Minimal chat, React development UI, sessions, approvals, Gmail status, schedules, admin overview, Knowledge UI, OneBot/Telegram slices. | Identity, Agents CRUD/switcher, Calendar, onboarding, Memory/Admin governance UI, Web/IM parity, responsive/i18n/a11y. |
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
- React app runnable through Vite; Compose web service is not the React app.
- Gmail is the only native connector; OAuth/read/status/revoke paths exist, with optional
  approval-gated real send.
- OneBot and Telegram gateway code exists.

## Verified baselines

The latest completed feature audits reported:

- Python: **1036 passed / 1 skipped** (1037 collected).
- React/Vitest: **49 passed**.
- Ruff lint/format and strict mypy: passed.
- Memory replay: **12/12 cases**, **7/7 gates**, weighted overall **0.982**, no live fallback.
- Knowledge replay: **8/8 cases**, **7/7 gates**, all named retrieval/citation/taint/deletion
  measures **1.000**, no live embedding fallback.
- Durable Jobs acceptance: real Postgres + Redis/arq covering lost/duplicate delivery,
  retry, crash/reclaim, exhaustion, cancel, and exactly-once injection.
- Knowledge live smoke: create → ingest → cited/tainted search → safe update/activation →
  immediate-hide delete → durable purge.

These are retained baselines, not a claim that every current-main dependency/environment
was re-run for this documentation change.

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
11. **Delivery mismatch:** Compose `:3000` is a static stub; `full` does not deliver the
    documented observability/object-store/sandbox stack.
12. **SDK/operations:** no generated-client/versioning pipeline; observability, CI security/
    performance coverage, backup/restore, and DR are below target.

## Current product gaps

- User sign-in/session identity and single-organization-v1 membership/RBAC.
- Agents CRUD, persisted personal/team Agents, explicit resource grants, real switcher.
- Calendar and a reusable connector/trigger framework.
- Web/IM runtime and approval parity.
- Memory UI, complete Admin/RBAC UI, onboarding.
- Correct current copy, responsive behavior, internationalization, and accessibility.

## Next work

Follow [Roadmap](./ROADMAP.md), in order:

1. M3.1 Cloud Safety Foundation
2. M3.2 Event Evolution
3. M3.3 Retention/Erasure
4. M3.4 Multi-user Identity + Agents + durable run topology
5. M3.5 Connector/Product Experience
6. M3.6 Production Delivery/Scale

Plugin SDK and Desktop are deferred until those gates.
