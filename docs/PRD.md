# Keel product requirements

> **Status:** Living target specification · **Updated:** 2026-07-16
> **Current implementation:** [STATUS.md](./STATUS.md) · **Execution:** [ROADMAP.md](./ROADMAP.md)
> **Architecture:** [ARCHITECTURE.md](./ARCHITECTURE.md) · **Product form:** [ADR-0009](./adr/0009-product-form-and-primary-use-cases.md)

## 1. Product thesis

Keel is a cloud-native agent platform where:

1. each user has a **private, persisted personal agent**;
2. users can create or join **team agents** that share only explicitly granted resources;
3. Web and IM are surfaces of the **same durable runtime**, not separate bots;
4. memory, connectors, permissions, approvals, schedules, and audit are attached to an
   Agent and enforced by identity and grants;
5. native connectors provide reliable depth for core services, while MCP and workflow
   systems such as n8n provide the long tail.

The stable "keel" is the bounded, durable runtime. Product value comes from safely
connecting that runtime to people, memory, knowledge, and actions.

### 1.1 Current implementation versus target

The current code is a **single deployment with one hard-coded `web:local` scope/agent**.
It has a comparatively mature runtime/data engine, Durable Jobs, Gmail, memory/evals, and
RAG/Knowledge, but no real users, organizations, Agents CRUD, Calendar, or hard
production-grade tenant boundary. See [Status](./STATUS.md) for evidence and blockers.

This PRD describes the target unless a requirement is explicitly marked current.

### 1.2 Tenancy boundary

- **Initial product boundary:** multi-user, **single organization per deployment**. Users
  have private agents and can explicitly share team agents/resources inside that
  organization.
- **Future boundary:** multi-organization SaaS with hard organizational tenancy, billing,
  fleet administration, and regional/data-residency controls.
- Self-hosted deployments remain supported; "single organization" must not mean
  "unauthenticated single user."

## 2. Goals and non-goals

### Goals

- **Identity and Agents:** durable users/sessions, Agents CRUD, private personal agents,
  explicit team-agent membership and resource grants.
- **One durable runtime:** Web, IM, schedules, triggers, and operator clients share session,
  event, approval, and run ownership.
- **Connected assistance:** Gmail and Calendar first; docs/knowledge next; connector
  triggers can initiate safe runs.
- **Memory and knowledge:** editable memory, session recall, consolidation, and cited,
  tainted Knowledge retrieval.
- **Safe action:** fail-closed permissions, cross-scope isolation, durable approvals,
  sandboxed execution, auditable/idempotent outbound actions.
- **Governance:** admin/RBAC, data retention/erasure, observability, cost controls, and
  operable cloud delivery.
- **Extensibility:** connector/tool seams, MCP, skills, and eventually a versioned SDK.

### Non-goals for the initial product

- Hosted multi-organization SaaS, billing, and marketplace.
- Visual no-code workflow authoring; integrate with n8n rather than rebuilding it.
- Model training/fine-tuning.
- Native mobile applications.
- Local desktop/file execution before the remote trust and approval model is proven.
- Broad connector quantity at the expense of safe identity, grants, and lifecycle.

## 3. Personas

| Persona | Need |
|---|---|
| **Individual user** | A private agent with personal memory, Gmail/Calendar/knowledge, proactive triggers, and approval before external action. |
| **Team member** | A shared agent in Web/IM that can use team-granted resources but cannot access private agents or connectors. |
| **Organization admin** | User/role/Agent/connector governance, audit, retention, costs, health, and incident controls. |
| **Operator/SRE** | Repeatable deployment, upgrades, metrics, backup/restore, security boundaries, and run/job diagnosis. |
| **Builder** | Safe extension points, MCP, connector framework, compatibility policy, examples, and test harnesses. |

Developer CLI automation remains useful, but it is secondary to the connected personal/team
assistant product.

## 4. Primary journeys

### 4.1 Personal onboarding

1. A user signs in and receives a private personal Agent.
2. They choose a model and grant Gmail and Calendar with least OAuth scopes.
3. They review what data is stored, retention settings, and approval policy.
4. A first-run guide demonstrates read-only retrieval before enabling outbound actions.

### 4.2 Personal proactive assistant

1. A connector trigger or schedule starts a durable run.
2. The Agent reads only resources granted to it, with external content taint preserved.
3. It updates private memory or drafts an outbound action.
4. The user approves/rejects from Web or IM; retry cannot duplicate the action.

### 4.3 Team agent

1. An admin/member creates a team Agent and grants selected team resources.
2. Members use it from Web and an IM channel mapped to the same Agent/session policy.
3. Private personal memory/connectors are never visible.
4. Actions and grant changes are audited.

### 4.4 Admin governance

An admin can manage users, roles, Agents, memberships, connector grants, API credentials,
retention/erasure, pending approvals, jobs, schedules, costs, and health without direct DB
access.

### 4.5 Builder extension

A builder adds a connector/tool through the narrow tool contract, declares permissions and
taint/provenance behavior, tests it against compatibility/security suites, and deploys it
without modifying the runtime loop.

## 5. Product principles

- **One core, many surfaces:** no UI- or gateway-specific agent logic.
- **Identity before sharing:** every read/action has an actor, Agent, scope, and grant.
- **Private by default:** personal resources are never ambient to team Agents.
- **Durable before autonomous:** admitted work, approvals, and idempotency survive restart.
- **Content trust differs from scope trust:** trusted users still ingest untrusted email,
  web, docs, and messages.
- **Fail closed on trust, degrade on operations.**
- **Human control is cross-surface:** approval policy and pending state are consistent in
  Web and IM.
- **Native depth, extensible breadth:** native Gmail/Calendar; MCP/n8n for the long tail.
- **Evidence over aspiration:** Status and milestone exit gates define completion.

## 6. Functional requirements

Priority: **P0** initial product, **P1** fast follow, **P2** later.

### 6.1 Identity, organization, Agents, and governance

| ID | Requirement | Priority |
|---|---|---|
| ID-1 | User identity and durable login/session management. | P0 |
| ID-2 | Single-organization-v1 membership and owner/admin/member/viewer roles. | P0 |
| ID-3 | Agents CRUD with persona/model/tools/memory/connectors/permissions. | P0 |
| ID-4 | Every user receives a private persisted personal Agent. | P0 |
| ID-5 | Team Agents expose only explicitly granted resources to explicit members/channels. | P0 |
| ID-6 | Real Agent/scope switcher in Web; active Agent visible on every surface. | P0 |
| ID-7 | Admin UI/API for users, roles, Agents, grants, credentials, audit, and safety state. | P0 |
| ID-8 | Future multi-organization tenancy and billing. | P2 |

### 6.2 Surfaces and durable interaction

| ID | Requirement | Priority |
|---|---|---|
| SUR-1 | Web chat, sessions, approvals, Agent switcher, memory, connectors, schedules, Knowledge, and admin. | P0 |
| SUR-2 | IM adapters map channels/DMs to the same Agent/runtime and approval state as Web. | P0 |
| SUR-3 | Streaming typed events with replay/resume after reconnect. | P0 |
| SUR-4 | Interactive runs are worker-owned/durable; API restart does not lose them. | P0 |
| SUR-5 | Responsive layout, keyboard access, WCAG-oriented semantics, and EN/简体中文 foundation. | P0 |
| SUR-6 | CLI remains an operator/builder and local-runtime surface. | P1 |
| SUR-7 | Desktop shell/local executor only after trust gates. | P2 |

### 6.3 Connectors and triggers

| ID | Requirement | Priority |
|---|---|---|
| CON-1 | Native Gmail and Calendar connectors with least-scope OAuth. | P0 |
| CON-2 | Connector framework owns auth, lifecycle, provenance, grants, triggers, and health; connectors still enter the loop as tools. | P0 |
| CON-3 | Tokens are encrypted, revocable, rotated, and keyed to user/Agent/grant. | P0 |
| CON-4 | Connector content carries provenance and untrusted-content taint. | P0 |
| CON-5 | Webhook/poll/schedule triggers can start idempotent durable runs. | P0 |
| CON-6 | Outbound send/post/invite actions are approval-gated, audited, and durably idempotent. | P0 |
| CON-7 | Docs/storage/contacts via native depth where justified; curated MCP/n8n elsewhere. | P1 |

### 6.4 Runtime, tools, and multi-agent

| ID | Requirement | Priority |
|---|---|---|
| RUN-1 | Bounded two-loop runtime, durable admission, stop-reason gate, named termination. | P0 |
| RUN-2 | Deterministic parallel tool execution and bounded outputs. | P0 |
| RUN-3 | File/shell/web tools execute through an isolated environment, never the API process. | P0 |
| RUN-4 | Explicit fail-closed permission defaults and durable cross-surface approvals. | P0 |
| RUN-5 | Background jobs provide leases/reclaim, retries, progress, cancellation, and exactly-once result injection. | P0/current |
| RUN-6 | Sub-agents share budgets and inherit explicit scope/grants without expanding authority. | P1 |

### 6.5 Memory, Knowledge, and data lifecycle

| ID | Requirement | Priority |
|---|---|---|
| DATA-1 | Durable session history and lexical/semantic recall. | P0/current |
| DATA-2 | Versioned core memory and archival memory; proposal-first consolidation. | P0/current |
| DATA-3 | Knowledge ingest/version/delete/search with citations, taint, and embedding pinning. | P0/current |
| DATA-4 | Event upcasters and projection rebuild compatibility. | P0 |
| DATA-5 | Per-Agent/session retention and complete audited erasure across events, projections, vectors, Knowledge, tokens, artifacts, and telemetry. | P0 |
| DATA-6 | User-facing Memory UI with edit/history/proposal controls. | P0 |

### 6.6 Operations and extensibility

| ID | Requirement | Priority |
|---|---|---|
| OPS-1 | Non-owner runtime DB role and RLS enforced as defense in depth. | P0 |
| OPS-2 | Hashed/scoped machine credentials, OIDC for humans, authenticated webhooks. | P0 |
| OPS-3 | Complete traces, reconciled usage/cost, health/SLO metrics, and alerts. | P0 |
| OPS-4 | Repeatable install/upgrade/rollback plus tested backup/restore. | P0 |
| OPS-5 | Accurate production images/profiles, including React delivery and isolated services. | P0 |
| EXT-1 | Skills and MCP remain supported with import/trust controls. | P1/current foundation |
| EXT-2 | Generated/versioned SDK and plugin lifecycle only after event/API gates. | P2 |

## 7. Non-functional requirements

| Area | Requirement |
|---|---|
| Isolation | Automated two-user/private-team tests prove no unauthorized cross-Agent resource access; runtime DB ownership cannot bypass the tested boundary. |
| Reliability | No admitted turn or pending approval is lost on process restart; duplicate delivery does not duplicate outbound effects. |
| Performance | Warm first token target <2 seconds p50 excluding provider latency; management pages remain usable with production-sized histories. |
| Availability | Stateless API and horizontally scalable workers after durable topology gates; graceful drain and recovery. |
| Security | Isolated execution, least privilege, SSRF/egress/path controls, encrypted secrets, authenticated webhooks, and audit. |
| Privacy | Documented data map, configurable retention, PII/secret redaction, and verified erasure. |
| Accessibility/i18n | Core journeys keyboard-operable and screen-reader-labeled; architecture supports English and 简体中文 without duplicated product logic. |
| Operability | Health/readiness, structured logs, traces/metrics, queue/run diagnosis, tested backup/restore, upgrade/rollback. |
| Compatibility | Event upcasters and additive API policy protect stored histories and clients. |

## 8. KPIs and exit evidence

KPIs apply only after their prerequisite milestone:

- **Activation:** ≥80% of invited users complete sign-in, select/create an Agent, and finish
  one successful read-only connected task.
- **Time to value:** median <10 minutes from deployment-ready credentials to first
  successful run; median <15 minutes for user onboarding after admin setup.
- **Cross-surface continuity:** ≥95% of sampled Web/IM journeys preserve the same session,
  Agent, approval, and result state.
- **Safety:** zero unauthorized cross-Agent reads in acceptance suites; 100% of outbound
  mutations pass policy, audit, and idempotency checks.
- **Durability:** zero lost admitted turns/approvals in restart tests; job recovery suites
  remain green.
- **Connector quality:** Gmail and Calendar happy-path success ≥95% excluding upstream
  outages; refresh/revoke failures fail closed.
- **Retrieval quality:** published deterministic Memory/Knowledge gates remain at or above
  their accepted thresholds.
- **Operations:** required runs traced with usage reconciliation; restore drill meets the
  declared RPO/RTO before production release.
- **Accessibility:** no critical automated violations in core journeys plus documented
  keyboard/manual review.

## 9. Release sequence

The active release plan is [Roadmap](./ROADMAP.md):

1. Cloud Safety Foundation
2. Event Evolution
3. Retention/Erasure
4. Multi-user Identity + Agents + durable run topology
5. Connector/Product Experience
6. Production Delivery/Scale
7. Plugin SDK and Desktop only after their gates

## 10. Risks

| Risk | Response |
|---|---|
| Cross-scope data leak/confused deputy | Identity/grants, enforced RLS, taint, approval, audit, adversarial acceptance tests. |
| Prompt injection through email/web/docs/IM | Treat content as untrusted independent of user/scope trust; constrain outbound/cross-connector actions. |
| Sandbox escape or server-process shell | Isolated execution service with least privilege, egress/path policy, and security tests. |
| Duplicate email/invite/post | Durable idempotency keys and provider-ID reconciliation. |
| Connector breadth overwhelms product | Gmail/Calendar native first; framework + MCP/n8n long tail. |
| UI outruns runtime truth | One API/event model; Status and measurable gates govern claims. |
| Privacy deletion is incomplete | Data map, upcasters, idempotent erasure jobs, rebuild/no-resurrection tests. |
| Premature SDK/Desktop | Explicitly gated after event, identity, approval, and production delivery milestones. |
