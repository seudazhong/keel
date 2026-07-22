# Keel architecture

> **Status:** Living architecture
> **Product boundary:** [PRD](./PRD.md)
> **Current capability:** [Status](./STATUS.md)
> **Accepted domain model:** [ADR-0011](./adr/0011-product-boundary-and-domain-model.md),
> [ADR-0012](./adr/0012-user-mailboxes-todos-notifications.md),
> [ADR-0013](./adr/0013-mailbox-portfolio-and-todo-experience.md)

This document describes both the system that exists and the contracts it is moving toward. Every
section labels a statement as **current** or **target** when the distinction matters.

## 1. Architectural position

Keel is a server-hosted, durable Agent platform with a narrow runtime core and governed resources.
The primary product is connected personal/team assistance. Managed projects, review, and patch
generation reuse the same Agent, authorization, approval, and execution boundaries.

Five commitments drive the architecture:

1. **One runtime, many surfaces.** Web, email, IM, schedules, and operator tools do not own separate
   Agent logic.
2. **Durable orchestration.** Accepted work, approvals, and effects survive process and delivery
   failure.
3. **Explicit authority.** Actor membership and Agent/resource grants are intersected; a storage
   scope never grants authority by itself.
4. **Separated trust zones.** Control/orchestration services do not execute arbitrary code or hand
   broad credentials to the sandbox.
5. **Evidence-based product claims.** A backend component is not a product scenario until its
   shipped surface and end-to-end acceptance pass.

## 2. Current implementation summary

| Area | Current implementation | Target delta |
|---|---|---|
| Product profile | Trusted local/single-operator Compose preview. | Single-organization multi-user product, then production profile. |
| Identity | Users, organizations, memberships, Agents, grants, first-class Agent Access edges (discover/use/manage; R1B), session ownership/visibility (private/agent_members/explicit; R1B), OIDC JWT verification, API keys, local actor. | Browser authorization-code/PKCE login, secure session, complete admin UI for Agent Access/session-visibility/shares. |
| Agent model | Persisted kind, owner, name, persona, status, version. Runs capture an immutable, schema-versioned `AgentConfigSnapshot` at admission. Agent management is API-only; the incomplete React tab is not shipped. | Agent-owned versioned tool/resource/memory/autonomy config when product requirements justify a management surface. |
| Scope | `agent:<org>/<agent>` is derived for authenticated runs; `web:local` remains preview compatibility. | Scope remains internal and disappears from normal product UX. |
| Runs | Postgres-owned worker execution with leases, controls, approvals, dispatch outbox, reconciliation, and an immutable per-run Agent config snapshot bound into the admission fingerprint. | Richer admitted authority fields when a concrete product requirement needs them. |
| Schedules | Persistent rows and compare-and-set cursor advance in worker cron. | Reliability hardening and any Routine redesign are deferred until automation requirements justify them. |
| Jobs | Postgres lifecycle, at-least-once Redis/arq delivery, leases, retries, cancellation, result injection, dispatch recovery. | Capability-specific worker pools and production SLOs. |
| Effects | Generic durable Effect ledger (R1B): `reserved -> executing -> {confirmed, unknown, failed}`, with `unknown` leaving only through provider reconciliation (`reconciled_confirmed`/`reconciled_absent`, the latter permitting one controlled retry). Fenced single-winner execution lease; a cross-scope worker cron reaps expired leases and drives reconciliation. Gmail send and Google Calendar create/update have reconciliation capability today; every other connector's `unknown` Effects surface via `/v1/effects` for an operator/user decision. | Reconciliation capability for the remaining connectors (comment/PR effects, etc.), a React history/status surface, and richer per-effect approval-context columns beyond action hash. |
| Memory | Core blocks/version history, archival memory, search, consolidation proposals, evals. Interactive model tools can mutate memory directly under the current permission/approval policy. | Any change to learning authority requires comparative product evidence; proposal-first is a deferred option, not an active gate. |
| Knowledge | Versioned documents, chunking, embeddings, hybrid retrieval, citations, taint, delete lifecycle. | Product qualification and source lifecycle across supported Connections. |
| Connections | One provider binding per connector per scope plus selected resources/targets. | Multi-account user/organization-owned Connections move to R2. |
| Keel Mailbox | Not implemented. | One Primary plus optional private Purpose Mailboxes per User, private routing, signed inbound delivery, and approval-gated outbound mail. |
| ToDos and notifications | Not implemented. | User-owned ToDos with durable reminders and reusable Web/email/IM notification delivery. |
| Projects | Organization-owned project records, shared Git storage, worktrees, GitHub App sync, grants. | Simpler GitHub setup and complete user journeys. |
| Review | Durable API/worker and immutable evidence-checked report artifacts. | React review request/status/report surface. |
| Patch | Durable proposal/generation/outbox/approval/writeback backend. | API, SDK, UI, evidence, capability pool, and Draft PR e2e. |
| Execution | Authenticated sandbox RPC; per-scope file namespaces; shell disabled. | Ephemeral per-run command sandbox with quotas and egress controls. |
| Event/lifecycle | Event upcasters, checkpointed rebuilds, tombstones, retention, scope/session/project erasure, identity purge CLI. | Operator/UI integration and durable user/org erasure API. |
| Operations | Compose readiness, non-owner DB principal, K8s scaffold. | Separate scheduler, telemetry, scale proof, secret manager, backup/restore/DR. |

## 3. Canonical domain model

ADR-0011 defines the core platform vocabulary. ADR-0012 adds user-scoped Keel Mailboxes, ToDos, and
Notifications; ADR-0013 defines mailbox cardinality, product surfaces, and Agent ToDo tools.

```text
Organization
  ├─ Memberships -> Users
  ├─ Agent Access -> Users / Channels
  ├─ Agents (personal | team)
  ├─ Connections -> selected external resources
  ├─ Knowledge Bases
  ├─ Projects
  ├─ User-owned ToDos
  └─ Routines

User
  ├─ Keel Mailboxes (one primary, optional purpose)
  │    └─ Mail Threads / Messages / Drafts
  ├─ Verified Delivery Endpoint
  └─ Notifications -> Channel Deliveries

Actor + Agent Access + Agent + Routine + Resource Grants
  -> Session
  -> Run
  -> Jobs / Approvals / Effects / Artifacts
```

### 3.1 Actor

**Current:** requests resolve to a user, machine API-key actor, or local preview actor. Every
admitted run records the stable actor plus an immutable `AgentConfigSnapshot` (INVARIANTS.md
C8) captured at admission.

**Target:** the snapshot's authority basis grows to include a full effect policy alongside the
Agent config, and non-run effects (not just runs) record an equivalent immutable snapshot.

### 3.2 Organization

The organization is the tenant and administrative boundary. The schema can represent several
organizations, but the initial production profile supports one active organization per deployment
until multi-organization isolation and operations are separately proven.

**Current gap:** the API can create several active organizations. The single-organization release
must enforce a deployment tenant policy rather than rely on operator convention.

### 3.3 Agent

An Agent is a persisted execution identity. Personal Agents are private to their owner. Team Agents
require an explicit Agent Access edge (R1B) — organization membership alone is no longer sufficient.

**Current:** persisted Agent records are intentionally small (kind, owner, name, persona, status,
version). A run's admission separately assembles the rest of the executed configuration
(model, tool set, permission profile, memory policy, budget, active grants) per surface and
freezes it into the run's `AgentConfigSnapshot` — the worker executes from that frozen snapshot,
never from the Agent's later-mutated `name`/`persona` (INVARIANTS.md C8). Revoking Agent
visibility (archive, membership loss, or a revoked Agent Access edge) still denies execution at
claim time.

**Target:** an Agent version itself *owns* persona, model selection, tool policy, memory policy,
default resources, budgets, and allowed Routine classes, so the run snapshot is stamped from one
first-class versioned Agent record rather than assembled per-surface at admission time.

### 3.4 Agent access and session visibility

**Current (R1B):** a first-class `agent_access` edge — `(org, agent, user|channel principal) ->
discover|use|manage` — gates team-Agent discovery/use; bare organization membership no longer
implies either. Access levels are ordered (`manage` implies `use` implies `discover`); an org
admin/owner keeps an explicit administrative path equivalent to an implicit `manage` edge, and a
delegated `manage`-level holder may administer the Agent's own access list. Personal Agent access
remains owner-private (no edges). Every session additively records `owner_user_id` or a channel
identity (`channel_provider`/`channel_external_id`) plus a `visibility` policy — `private`
(default), `agent_members` (any principal with an active Agent Access edge), or `explicit` (owner +
`session_access` share rows) — checked independently of the selected Agent's scope on every session
read endpoint (list/history/event-stream/run-detail). Using a team Agent does not grant read access
to another user's private session created under that Agent. See `keel_core.identity.authz`,
`keel_core.session_visibility`, and INVARIANTS.md C2/C6.

**Target:** an admin-facing UI for granting/revoking Agent Access and managing session
visibility/shares (API-only today); org-admin "support access" to session content remains an
explicit, audited, out-of-scope decision for a later PR rather than an implicit override.

### 3.5 Connection, resource, and grant

A Connection is a credential-bearing provider account owned by a user or organization. It has child
resources such as mailboxes, calendars, folders, repositories, or chats. Credentials belong to the
Connection; Agents and Routines receive only selected resource capabilities.

Resources include external Connection resources, Knowledge bases, Projects, and future governed
objects. Effective capability is the intersection of:

```text
actor membership
  x Agent access
  x Agent grant
  x Routine policy
  x resource state
  x deployment capability
```

Routine policy only attenuates the Agent's grants. RLS is defense in depth after this authorization
decision, not a replacement for it.

### 3.6 Routine (deferred)

If future automation requirements justify it, a first-class Routine would bind:

- trigger;
- Agent version or selection rule;
- input template;
- allowed resources and actions;
- budget and timeout;
- approval policy;
- delivery target;
- owner and lifecycle.

R1 retains the current `schedules` rows and hard-coded digest/consolidation dispatch. Neither a
Routine model nor a Schedule migration is on the active R1 plan.

### 3.7 Keel Mailbox, ToDo, and Notification

A Keel Mailbox is a platform-managed email identity bound to a User, not to a persisted Agent and
not to a user-configured Connection. When Mail is enabled and any active mailbox exists, the User has
exactly one active Primary Mailbox and may have additional private Purpose Mailboxes within
deployment quota. They survive personal-Agent changes. A mail thread may be pinned to one private
email Session and personal Agent when first admitted.

A ToDo is user-owned within an organization. Agent, Session, Run, Routine, and mail-message
references record who created or changed it but do not become owners. Team Agents receive no
implicit access to a member's personal ToDos or mailbox.

A Notification is a durable user-facing notice. Each channel delivery is tracked separately and an
email delivery is an external Effect. Only a structurally constrained, versioned template addressed
to the user's verified delivery endpoint may bypass per-message approval.

### 3.8 Scope

`scope_id` partitions runtime data. Authenticated Agent work derives a canonical scope from
organization and Agent. `web:local` is a preview compatibility scope.

Scope equality can prevent accidental cross-partition reads, but it cannot answer whether an actor
or Agent is allowed to use a Project, Connection, or other resource. Product authorization always
uses explicit identity and grants. A Session's own ownership/visibility (R1B —
`keel_core.session_visibility`) is a further, independent example: two sessions can share the same
Agent scope while one is fully private to its owner and unreadable by any other Agent-authorized
user.

User-owned Mailbox/mail/Notification data uses a separate `owner_user_id` RLS axis. ToDos and
reminders require both `org_id` and `owner_user_id`. These rows are not forced into an arbitrary
Agent scope; only a mail-triggered Session/Run receives the derived Agent `scope_id`.

## 4. Trust zones

```text
Web / Email / IM / CLI / operator
          |
          v
Control plane
  identity, Agents, Routines, Connections, mailboxes, ToDos, grants, approvals, audit, API
          |
          v
Orchestration plane
  durable admission, event log, model calls, context, jobs, scheduling, recovery
          |
          +------------------------------+------------------------------+
          v                              v
Trusted effect plane              Untrusted execution plane
connector / mail / Git brokers    sandbox
narrow/JIT credentials            no control-plane credentials
```

### 4.1 Control plane

Trusted services define and authorize work. They may hold encrypted connector configuration and
mint short-lived credentials. They never execute repository/build code.

### 4.2 Orchestration plane

The orchestration plane owns run/job state and calls models. It may request an effect or sandbox
operation, but it does not receive broader authority than the admitted Actor/Agent/Routine
snapshot.

### 4.3 Trusted effect plane

- Connector actions receive only the provider credential and resource capability needed for that
  effect.
- Git writeback mints a just-in-time installation token in the trusted control plane; the sandbox
  never receives it.

**Current:** these brokers run inside the general worker process. The target uses separate queues
and capability-specific workers so orchestration workers do not hold every effect credential.

### 4.4 Untrusted execution plane

File/command execution occurs behind the `ExecutionEnvironment` seam and receives no control-plane
credentials.

The current Compose sandbox is one long-lived rootless-OCI service with directory namespaces.
That is sufficient for trusted-preview file isolation, not hostile-tenant command execution.

## 5. Current container topology

```text
Browser
  -> keel-web (nginx + React)
  -> keel-server (FastAPI)

keel-server
  -> PostgreSQL (events, runs, jobs, identity, resources)
  -> Redis (arq delivery, live fan-out, coordination)
  -> model/providers
  -> connector APIs / GitHub
  -> keel-sandbox RPC

keel-worker
  -> PostgreSQL
  -> Redis
  -> model/providers
  -> connector APIs / GitHub
  -> shared project storage
  -> keel-sandbox RPC

one-shot services
  -> keel-migrate
  -> keel-runtime-secret-init
  -> keel-provision
  -> keel-secret-init

optional
  -> Ollama for local embeddings/models
```

The standard Compose startup uses:

```text
migrate -> runtime secret -> runtime-role provision -> sandbox -> server/worker/web
```

`keel-scheduler` exists as a package but is not a deployed long-lived service. Worker cron owns
schedule, dispatch, reconciliation, connector, review, and patch ticks today.

## 6. Durable execution model

### 6.1 Interactive runs

1. The API resolves actor, organization, Agent, and canonical scope.
2. Input and run admission are committed in Postgres before model execution.
3. A run-dispatch outbox records the worker intent.
4. Redis/arq delivers `run_interactive` at least once.
5. A worker claims the run under a fenced lease and reconstructs its admitted model/identity.
6. Events are appended and streamed through replayable SSE.
7. Interrupt, cancel, steer, and approvals are durable records.
8. Reconcilers recover missed dispatch and expired owners.

Postgres is the lifecycle source of truth. Redis is delivery and fan-out, never the only durable
record.

### 6.2 Background jobs

ADR-0010 defines the job contract:

- Postgres state machine;
- at-least-once delivery;
- lease token and heartbeat;
- bounded attempts/backoff;
- cooperative or immediate cancellation policy;
- idempotent handler effects;
- terminal result injection in the same durable transaction;
- dispatch reconciliation.

### 6.3 Schedule delivery

**Current:** a schedule occurrence advances `next_run_at` before enqueue. This prevents duplicate
delivery but can lose an occurrence if the process crashes after the cursor update.

**Deferred hardening:** atomically create a unique accepted occurrence and dispatch outbox row.
Current R1 behavior and its narrow crash-loss window remain documented rather than redesigned.

### 6.4 External effects

**Current:** provider-specific idempotency is strong in several connectors and patch writeback, but
the generic connector wrapper releases an idempotency claim after any exception.

**Target:** every effect, including an email send or notification delivery, has:

```text
reserved -> executing -> confirmed
                       -> unknown -> reconciled
                       -> failed
```

A timeout after a possible provider success becomes `unknown`; it is reconciled before any retry.

## 7. Agent runtime

The core loop provides:

- persist-before-first-provider-call;
- bounded iterations and token budgets;
- named stop reasons;
- stop-reason-gated tool execution;
- deterministic ordering for parallel-safe tools;
- replayable typed events;
- interrupt/steer boundaries;
- durable suspend/resume for approvals.

### 7.1 Permission construction

Every low-level `run()` call requires a permission engine. Interactive server/worker paths build
explicit read/ask policies; specialized Routines and patch generation use narrow allow-lists. An
omitted policy fails construction rather than substituting allow-all. Trusted local CLI execution
may still choose allow-all explicitly.

### 7.2 Content influence

Connector and Knowledge content is tainted and carries provenance. Tainted influence escalates
outbound connector actions to approval.

The target generalizes binary taint into an influence envelope containing origin, resource,
sensitivity, trust, and provenance. Conservative rule: if untrusted content influences an external
effect, the effect requires the configured human decision.

### 7.3 Rate limiting

**Current:** IM gateways have per-key sliding-window limits and provider clients handle upstream
rate-limit responses, but there is no unified provider-credential/session/chat limiter.

**Target:** Redis-backed token buckets bound provider credentials, sessions, and chat/channel keys,
with upstream `Retry-After` reconciliation and observable denial/backoff.

## 8. Memory and Knowledge

Detailed current contracts are in [Memory](./MEMORY.md) and [Knowledge](./KNOWLEDGE.md).

These are separate data classes:

1. **Session history:** append-only conversational events and search projections.
2. **Core/profile memory:** small, user-visible, versioned Agent context.
3. **Learned memory proposals:** model-suggested changes with source provenance and human review.
4. **Knowledge:** external/user documents with versions, chunks, citations, source lifecycle, and
   persistent taint.
5. **Run-local scratch:** temporary state that must not become durable memory implicitly.

Current interactive registration places memory mutation tools in the explicitly governed extra-tool
set. Consolidation can create proposals and can auto-commit high-confidence archival facts. R1 keeps
this behavior unchanged; [Memory](./MEMORY.md) records proposal-first as a future option that requires
comparative evidence before adoption.

Knowledge collections pin embedding model and dimension. A model change is an explicit re-embed
operation, never silent cross-model vector search.

## 9. Connections

Provider modules publish manifests and factories. The shared runtime owns:

- encrypted credential envelopes and rotation;
- setup/callback/health/revoke lifecycle;
- selected resources and targets;
- recurring sync/renewal;
- routed authenticated webhooks;
- provenance and taint;
- durable jobs and effect idempotency.

Current providers include Gmail, Google Calendar, Google Drive/Docs, Microsoft 365, Notion, Feishu,
GitHub collaboration, RSS, Atom, and generic webhook.

**Current limitation:** the database allows one binding per `(scope, connector_id)`. The target
Connection model supports several accounts per provider, organization/user ownership, and explicit
resource grants to Agents and Routines.

Provider presence is not product support. A provider graduates only after common setup, refresh,
revoke, scope, webhook, idempotency, reconciliation, provenance, and health suites plus a usable UI
journey.

## 10. Keel Mailboxes, ToDos, and notifications

AgentMail is the first Keel Mailbox provider. Its deployment credential remains in the trusted
control/effect planes. Keel persists an opaque local mailbox ID first and derives the deterministic
provider `client_id` from that mailbox ID, so retries reuse one inbox while Purpose Mailboxes cannot
collide. Runtime mail operations use the narrowest practical inbox-scoped credential and never
expose it to the model, browser, or sandbox. The data model and UI support one Primary plus optional
Purpose Mailboxes; a partial uniqueness constraint permits only one active primary per Mail-enabled
User.

Inbound processing is:

```text
verified webhook or reconciler
  -> durable delivery deduplication
  -> normalized untrusted message
  -> private thread/session routing
  -> triage, proposed ToDo, or local draft
```

Webhook authenticity establishes provider delivery, not human authorization. HTML, links, and
attachments remain tainted. Missing large bodies are fetched through the trusted provider client;
attachments are quarantined before use.

Mail storage is user-owned and independent of Agent scope. A content-free global route index resolves
the provider inbox to a User, after which normal access requires `app.user_id`; ToDos additionally
require `app.org_id`. If no unique active organization/personal-Agent route exists, mail is retained
with `routing_required` and no model Run starts.

Inbound persistence is cheap and does not imply model execution. User opt-in, per-mailbox/sender rate
limits, queue bounds, budgets, and a circuit breaker govern automatic triage.

Outbound processing is:

```text
Notification or approved Mail Draft
  -> immutable mail Effect
  -> trusted mail broker
  -> AgentMail idempotent send
  -> delivery/bounce/rejection reconciliation
```

The only automatic-send exception is a versioned template sent to the user's active verified
delivery endpoint under preferences, quiet hours, quotas, and deduplication. Freeform content, other
recipients, replies/forwards, and attachments require exact-draft approval.

ToDos are normal user data, not Jobs or Routines. Their state uses optimistic versions and audit
history. Due reminders create accepted Notification occurrences and are atomically cancelled or
replaced when the ToDo is changed, completed, cancelled, or archived.

The user-facing ToDo tool contract is deliberately small:

```text
todo_create
todo_list
todo_get
todo_update
todo_transition
todo_set_reminders
```

Untrusted email/autonomous contexts receive `todo_propose` instead of active mutation authority.
Tools derive User/Organization ownership from admitted context and never accept it from model input.

Mail and ToDos are full-width Workspace surfaces. Wide screens use list/detail workspaces; medium
screens collapse secondary rails; mobile uses separate list/detail routes. Exact-draft Send in the
authenticated Mail UI is itself the bound human approval rather than a redundant second dialog.

## 11. Managed projects, review, and patches

### 11.1 Projects

Projects are organization-owned resources. Users and Agents receive capabilities; they are not
storage owners.

Current storage separates:

- Postgres metadata and grants;
- active Git/project state on shared POSIX storage;
- disposable worktrees;
- retained report/patch artifacts.

The sandbox never receives writable authoritative Git storage.

### 11.2 Read-only review

Review resolves exact refs, computes a bounded diff, calls the shared provider path with no tools,
verifies every finding against the reviewed hunk, and stores immutable JSON/Markdown reports.

It is backend-complete but headless in the React product.

### 11.3 Controlled patch proposals

The patch backend performs:

```text
durable request
  -> isolated file-only generation
  -> immutable candidate bundle
  -> exact candidate approval
  -> trusted GitHub writeback
  -> Draft PR
```

It has no public API/UI. Shell/build/test is unavailable, so candidates must be labeled
unvalidated unless another trusted validator actually ran.

General or opaque coding agents are later work and require per-run execution isolation, capability
workers, egress policy, quotas, and explicit credential/legal review.

## 12. Persistence, versioning, and lifecycle

PostgreSQL stores relational state, event logs, vector/search data, lifecycle ledgers, and outboxes.
Redis provides queueing, fan-out, locks, and ephemeral coordination.

Event envelopes carry a per-type version. Read paths upcast immutable stored events through explicit
one-step transitions. Projection rebuild supports dry-run, checkpoints, resume, and tombstone
filtering.

Retention and erasure cover scope, session, and project data through durable jobs. User and
organization erasure uses a separate least-privilege maintenance path today. Target mailbox, mail,
ToDo, reminder, Notification, and delivery stores are user content or user-linked operational data
and must join the data map before implementation closes. Remote mailbox deletion reports partial
rather than false success when the provider cannot be verified. Every migration-created table,
including global dispatch indices and patch stores, is currently classified; a unit guard compares
the migration history to the data map so a new unclassified table fails CI.

The HTTP contract is additive under `/v1`; the committed OpenAPI baseline detects breaking changes.

## 13. Security model

### 13.1 Database

Migrations/provisioning use an owner principal. Server/worker use a separate non-owner,
non-`BYPASSRLS` runtime login. Standard Compose generates its password into a `0600` pgpass file and
verifies the connected principal before serving.

### 13.2 Browser and machine authentication

Current server auth supports:

- verified OIDC bearer JWT;
- hashed/scoped API keys;
- explicit non-cloud local preview.

The React app currently accepts a supplied credential or local preview. Production browser login
must use authorization code + PKCE and an HTTP-only server session.

### 13.3 Sandbox and secrets

The current sandbox:

- authenticates RPC with an HMAC secret;
- runs non-root with read-only rootfs, dropped capabilities, and no public network;
- has no DB, Redis, provider, or GitHub credentials;
- provides per-scope file namespaces;
- denies shell.

Enabling shell requires a per-run process/mount boundary, resource limits, default-deny egress, and
cleanup/recovery evidence. Directory naming alone is not sufficient.

### 13.4 Remote writes

Connector effects, mail sends, and Git writeback remain in trusted brokers. Approval must bind the
exact effect or immutable candidate. The template notification exemption is enforced structurally
after model output and cannot accept arbitrary recipients or bodies. Default-branch push,
force-push, merge, and host Docker socket access are forbidden.

## 14. Deployment trust profiles

| Profile | Contract |
|---|---|
| **preview** | Current Compose: trusted operator, local ports, optional open mode, non-owner DB, file-only sandbox. No production or hostile-input claim. |
| **single-org** | Target production profile: browser OIDC, explicit admin, separate scheduler/capability workers, hardened per-run execution where needed, object storage, telemetry, DR. |
| **multi-org** | Future: stronger tenant isolation, KMS, regional controls, billing, adversarial tenant tests. |
| **lite** | Future/limited CLI developer mode; no team, autonomy, or production parity claim until implemented and tested. |

The current `dev` and `full` Compose profiles select effectively the same implemented service set.
They are not separate trust contracts.

## 15. Repository layout

```text
packages/
  keel-core/       runtime and domain libraries
  keel-server/     FastAPI control/API surface
  keel-worker/     arq execution, jobs, effects, reconciliation
  keel-scheduler/  schedule primitives; no deployed long-lived service
  keel-sandbox/    authenticated execution service
  keel-cli/        operator/local CLI
  keel-sdk/        typed Python client

web/               React application
migrations/        Alembic schema history
deploy/            Docker and Kubernetes delivery assets
tests/             unit, integration, invariant, e2e, and eval coverage
docs/              living canon, subsystem references, ADRs, and history
```

## 16. Open architectural gates

The active ordering is in [Roadmap](./ROADMAP.md). The remaining load-bearing gaps are:

1. Enforced single-organization policy; an admin-facing UI for Agent Access/session-visibility/
   shares (API-only foundation landed, R1B);
2. a complete versioned personal-assistant configuration and first-class Connection model;
3. deferred Schedule reliability hardening and any future Routine model;
4. reconciliation capability for providers beyond Gmail/Calendar;
5. per-user Primary/Purpose Keel Mailboxes, verified delivery endpoint, ToDo, and Notification
   models and product surfaces;
6. browser OIDC session and team/admin product surfaces;
7. review/patch productization;
8. separate trusted effect brokers and per-run command isolation;
9. scheduler/capability workers, telemetry, scale proof, and DR.
