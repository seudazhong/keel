# Keel roadmap

> **Updated:** 2026-07-16 · **Authority:** active execution sequence

Keel has a mature agent/data engine relative to its product and production surfaces.
Durable Jobs, memory consolidation/evals, and the RAG/Knowledge vertical slice are complete.
Execution now prioritizes **visible, usable product value** in a clearly labeled local/single-
organization preview, then closes production-safety and multi-user gates before external rollout.

## Execution policy

- Ship visible vertical slices early: real React delivery, truthful product states, Agent/Memory
  management, onboarding, Calendar, and useful routines.
- M3.1–M3.2 remain **trusted-environment previews**. They do not imply production or multi-user
  readiness and must not be exposed to untrusted networks.
- User login, OIDC, and platform OAuth authentication are deferred to M3.6. Connector-specific
  OAuth remains in the milestone that introduces that connector because Gmail/Calendar require it.
- Cloud safety, event evolution, and erasure remain mandatory gates before multi-user rollout.

## Completed foundation

### Durable Jobs + Memory/Knowledge/Quality

**Delivered:** DB-backed job lifecycle with leases/reclaim, retries, cancellation, progress,
and exactly-once result injection; core/archival/recall memory; deterministic memory evals;
Knowledge Base lifecycle, durable ingest/delete, hybrid retrieval, citations, and taint.

**Evidence:** verified baselines and exact capability boundaries are in
[Status](./STATUS.md). Completion does not imply multi-user or production readiness.

## M3.1 — Demo-ready Product Surface

**Status:** **In progress.**

**Goal:** make the capabilities already implemented visible, coherent, and easy to demonstrate.

**Scope:** serve the real React application from the dev/demo Compose path; remove stale product
copy; add demo seed/bootstrap data; improve empty/loading/error states; expose current Memory
proposals and Knowledge/jobs clearly; add a lightweight local first-run wizard for provider,
secret, default Agent profile, and optional connector setup; add Playwright smoke coverage.

**Dependencies:** completed Durable Jobs, Memory, Knowledge, and React source.

**Completed increment:** the dev/demo Compose path serves the built React application through
nginx with API/health/readiness/SSE proxying and SPA fallback; Jobs and Memory proposals have
truthful React pages; an opt-in guarded/idempotent bootstrap seeds searchable demo content; and
a read-only Playwright smoke detects stale stacks and covers empty or populated states.

**Remaining before milestone completion:** implement the lightweight local first-run wizard for
provider, secret, default Agent profile, and optional connector setup; complete the product-state
copy/badge exit-gate audit. Until then, M3.1 is not complete.

**Exit gates:**

- One documented command launches the current React application rather than `web/stub`.
- A clean demo profile can show Chat, Sessions, Memory/Knowledge, Jobs, Schedules, Approvals,
  Connectors, and Observability without manually editing database rows.
- Product copy and capability badges come from current API state, not milestone-era constants.
- The safe 10–15 minute demo passes as an automated browser smoke.

## M3.2 — Personal Agent Experience Preview

**Goal:** deliver a useful personal-assistant loop before implementing full user authentication.

**Scope:** persisted Agent profiles owned by the implicit local operator; Agents CRUD and a real
switcher; Memory blocks/history/proposal UI; Calendar as the second native connector; natural-
language routines/triggers; improved approval explanations; local onboarding; responsive and
accessibility fixes for the primary journey.

**Dependencies:** M3.1 product surface. This milestone may use current open/API-key local mode;
it does not add public user accounts or claim tenant isolation.

**Exit gates:**

- A local operator creates/selects an Agent, reviews/edits its memory, grants Gmail/Calendar,
  and runs a useful inbox/meeting routine from the React UI.
- Agent selection changes persona, memory, connector grants, and tool policy without code edits.
- Calendar read/draft/create behavior has least-scope consent and approval tests.
- Core flows work at narrow desktop/mobile widths and pass keyboard/critical a11y checks.

## M3.3 — Cloud Safety Foundation

**Goal:** make the product preview safe enough to become a durable cloud runtime.

**Scope:** non-owner runtime DB role with enforced RLS; explicit fail-closed permission defaults;
real isolated execution backend; durable interactive run/approval coordination; hashed/scoped API
credentials; durable connector OAuth state; authenticated gateway webhooks; durable outbound
idempotency; secrets/key-rotation design; safety regression suite.

**Dependencies:** M3.1–M3.2. Safety work may begin earlier in parallel, but the milestone closes
before public exposure or multi-user development.

**Exit gates:**

- Cross-scope reads fail even for the runtime application role and are audited.
- Shell/file execution cannot run in the API process and passes escape/egress tests.
- Server restart does not lose an admitted run or pending approval.
- Connector OAuth callback and gateway replay/forgery tests fail closed.
- Every permission engine has an explicit non-allow default.

## M3.4 — Event Evolution

**Goal:** preserve replay and projection rebuilds across schema changes.

**Scope:** upcaster registry, event compatibility policy, fixtures for every historical version,
projection rebuild tooling, and additive API/schema checks.

**Dependencies:** M3.3 durable runtime boundaries.

**Exit gates:** event streams from v0 through current rebuild identical projections; CI requires
old→new contract fixtures; incompatible event changes cannot merge.

## M3.5 — Retention and Erasure

**Goal:** give operators complete, testable control over persisted user data.

**Scope:** data map; retention policies; scope/session erasure; event tombstones where required;
projection, memory, vector, Knowledge, token, artifact, and telemetry purge; audited deletion jobs
and operator runbook.

**Dependencies:** M3.3–M3.4.

**Exit gates:** seeded data is removed from every documented store; rebuild cannot resurrect
erased content; connector tokens are revoked/purged; retention jobs are idempotent and observable.

## M3.6 — Multi-user Identity, Access, and Durable Run Topology

**Goal:** evolve the visible single-operator Agent experience into the target multi-user model.

**Scope:** users and login sessions; local accounts plus OIDC/OAuth where required; single-
organization-v1 membership/RBAC; bind existing Agent profiles to users; personal versus explicitly
shared team Agents; connector/resource grants; worker-owned interactive runs; cross-surface
approvals; audit UX.

**Dependencies:** M3.3–M3.5.

**Exit gates:** two users and one shared team Agent pass isolation/grant tests; no route depends on
`web:local`; restart/scale-out preserves run and approval ownership; Web and IM operate the same
durable runtime; authentication cannot expand connector/resource authority.

Multi-organization SaaS, billing, and hard organizational tenancy remain later work.

## M3.7 — Connector and Team Experience

**Goal:** expand from the personal preview to governed personal and team workflows.

**Scope:** connector framework hardening; native depth for core connectors and MCP/n8n for the
long tail; event triggers; Web/IM parity; team Agent grants; Admin/RBAC UI; connector health and
reauth; internationalization foundations.

**Dependencies:** M3.6 identity, Agents, and grants.

**Exit gates:** an authenticated user grants Gmail/Calendar to a personal Agent, receives a
trigger-driven draft, and approves it from Web or IM; no personal resource is visible to an
ungranted team Agent; admins can govern connector availability and audit actions.

## M3.8 — Production Delivery and Scale

**Goal:** provide a supportable cloud-native deployment.

**Scope:** production React image; accurate deployment profiles; scheduler service/leadership;
N-worker and multi-server topology; generated/versioned SDK; OTel, metrics, alerts, and SLOs; CI
security/performance gates; backup/restore and DR drills; upgrade/rollback runbooks.

**Dependencies:** M3.3–M3.7.

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
- M3.1–M3.2 are preview milestones, not authorization to expose open mode publicly.
- Connector OAuth may ship with its connector; user login/OIDC remains deferred to M3.6.
- New connector breadth cannot bypass M3.3 safety or M3.6 grants.
- Plugin SDK/Desktop do not displace safety, lifecycle, identity, or delivery gates.
- Milestone completion requires measurable exit evidence, not only merged code.
