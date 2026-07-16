# Keel roadmap

> **Updated:** 2026-07-16 · **Authority:** active execution sequence

Keel has a mature agent/data engine relative to its product and production surfaces.
Durable Jobs, memory consolidation/evals, and the RAG/Knowledge vertical slice are complete.
The next milestones close safety and lifecycle gaps before adding multi-user product breadth.

## Completed foundation

### Durable Jobs + Memory/Knowledge/Quality

**Delivered:** DB-backed job lifecycle with leases/reclaim, retries, cancellation, progress,
and exactly-once result injection; core/archival/recall memory; deterministic memory evals;
Knowledge Base lifecycle, durable ingest/delete, hybrid retrieval, citations, and taint.

**Evidence:** verified baselines and exact capability boundaries are in
[Status](./STATUS.md). Completion does not imply multi-user or production readiness.

## M3.1 — Cloud Safety Foundation

**Goal:** make the current single-scope cloud runtime safe enough to extend.

**Scope:** non-owner runtime DB role with enforced RLS; explicit fail-closed permission
defaults; real isolated execution backend; durable interactive run/approval coordination;
hashed/scoped API credentials; durable OAuth state; authenticated gateway webhooks; durable
outbound idempotency; secrets/key-rotation design; safety regression suite.

**Dependencies:** current jobs/event/RBAC foundations.

**Exit gates:**

- Cross-scope reads fail even for the runtime application role and are audited.
- Shell/file execution cannot run in the API process and passes escape/egress tests.
- Server restart does not lose an admitted run or pending approval.
- OAuth callback and gateway replay/forgery tests fail closed.
- Every permission engine has an explicit non-allow default.

## M3.2 — Event Evolution

**Goal:** preserve replay and projection rebuilds across schema changes.

**Scope:** upcaster registry, event compatibility policy, fixtures for every historical
version, projection rebuild tooling, and additive API/schema checks.

**Dependencies:** M3.1 durable runtime boundaries.

**Exit gates:** event streams from v0 through current rebuild identical projections; CI
requires old→new contract fixtures; incompatible event changes cannot merge.

## M3.3 — Retention and Erasure

**Goal:** give operators complete, testable control over persisted user data.

**Scope:** data map; retention policies; scope/session erasure; event tombstones where
required; projection, memory, vector, Knowledge, token, artifact, and telemetry purge;
audited deletion jobs and operator runbook.

**Dependencies:** M3.2 replay/upcasters and M3.1 secrets/identity-safe boundaries.

**Exit gates:** seeded data is removed from every documented store; rebuild cannot resurrect
erased content; connector tokens are revoked/purged; retention jobs are idempotent and
observable.

## M3.4 — Multi-user Identity, Agents, and Durable Run Topology

**Goal:** turn the single hard-coded scope into the target persisted personal/team-agent
model.

**Scope:** users and sessions; single-organization-v1 membership/RBAC; Agents CRUD; real
agent/scope switcher; personal versus explicitly shared team agents; connector/resource
grants; worker-owned interactive runs; cross-surface approvals; audit UX.

**Dependencies:** M3.1–M3.3.

**Exit gates:** two users and one shared team agent pass isolation/grant tests; no route
depends on `web:local`; restart/scale-out preserves run and approval ownership; Web and IM
operate the same durable runtime.

Multi-organization SaaS, billing, and hard organizational tenancy remain later work.

## M3.5 — Connector and Product Experience

**Goal:** make the durable runtime usable as a personal and team assistant.

**Scope:** Calendar plus a connector framework; native depth for core connectors and
MCP/n8n for the long tail; connector triggers; IM/Web parity; Memory and Admin/RBAC UI;
onboarding; current product copy; responsive design, accessibility, and i18n foundations.

**Dependencies:** M3.4 identity/Agents and grants.

**Exit gates:** a new user completes onboarding, creates/selects an agent, grants Gmail and
Calendar, receives a trigger-driven draft, and approves it from Web or IM; no personal
resource is visible to an ungranted team agent; key flows meet documented a11y/responsive
checks.

## M3.6 — Production Delivery and Scale

**Goal:** provide a supportable cloud-native deployment.

**Scope:** real React delivery image; accurate Compose/deployment profiles; scheduler
service/leadership; N-worker and multi-server topology; generated/versioned SDK; OTel,
metrics, alerts, and SLOs; CI security/performance gates; backup/restore and DR drills;
upgrade/rollback runbooks.

**Dependencies:** M3.1–M3.5.

**Exit gates:** repeatable clean install and upgrade; N-worker load/chaos demonstration;
100% required run traces and reconciled usage; restore drill meets RPO/RTO; production
deployment has no static-stub or in-process safety substitutions.

## After the gates

### Plugin SDK and hooks

Begin only after event/API compatibility and production delivery gates. Exit requires
manifest validation, capability permissions, lifecycle compatibility, rollback, examples,
and a generated client/version policy.

### Desktop / LocalDaemon

Begin only after identity, grants, cross-surface approvals, and isolated execution are
proven. A desktop shell must not create a second runtime; local execution needs its own
fail-closed trust boundary.

## Roadmap rules

- [Status](./STATUS.md) supplies completion evidence; dated plans do not.
- New connector breadth cannot bypass M3.1 safety or M3.4 grants.
- Plugin SDK/Desktop do not displace safety, lifecycle, identity, or delivery gates.
- Milestone completion requires measurable exit evidence, not only merged code.
