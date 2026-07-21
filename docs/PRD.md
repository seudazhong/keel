# Keel product requirements

> **Status:** Living target specification
> **Current implementation:** [Status](./STATUS.md)
> **Architecture:** [Architecture](./ARCHITECTURE.md)
> **Active delivery plan:** [Roadmap](./ROADMAP.md)

## 1. Product thesis

Keel is a cloud-native platform for durable, governed Agents:

1. each user can have a private, persisted personal Agent;
2. organizations can create team Agents shared only with explicit members and resources;
3. Web, email, IM, schedules, and operator tools are surfaces of the same runtime;
4. memory, Knowledge, connectors, projects, permissions, approvals, budgets, and audit attach to
   an Agent or Routine and are enforced consistently;
5. each user can have one primary Keel Mailbox, optional private purpose mailboxes, user-owned ToDos,
   and durable notifications;
6. native integrations provide dependable depth for core services, while MCP and automation
   systems cover the long tail.

The initial production boundary is **multi-user, one active organization per deployment**. The
schema may remain multi-organization-ready, but hosted multi-organization SaaS is a later product.

Managed projects, code review, and controlled patch proposals are a capability pack for the same
platform. Keel is not redefined as a coding-only product.

## 2. Product principles

- **One Agent runtime, many surfaces.**
- **Identity and grants before sharing.**
- **Private by default; explicit sharing only.**
- **Durable admission before autonomous execution.**
- **Scope is a partition key, not authority.**
- **Untrusted content stays untrusted even when its account or user is trusted.**
- **Human decisions bind exact effects or immutable revisions.**
- **Native depth before connector count.**
- **Evidence over aspiration.**

## 3. Users

| User | Primary need |
|---|---|
| Individual user | A private Agent with memory, connected accounts, cited Knowledge, ToDos, a Keel Mailbox, routines, and approval before sensitive effects. |
| Team member | A shared Agent that can use only team-granted resources and never private resources. |
| Organization admin | Membership, Agent, Connection, grant, routine, audit, retention, cost, and safety management. |
| Operator/SRE | Repeatable deployment, upgrades, diagnostics, observability, backup/restore, and incident controls. |
| Builder | Stable extension seams, capability declarations, compatibility tests, and safe examples. |

## 4. Primary journeys

### 4.1 Personal connected Agent

1. A user signs in, receives or selects a private personal Agent, and enables a Primary Keel
   Mailbox.
2. They verify a human delivery address, connect Gmail and Calendar, select resources, and review
   stored-data and approval policy.
3. The Agent answers using cited Knowledge and user-managed memory.
4. The user asks Keel to create and manage ToDos through chat or the ToDo surface.
5. A Routine prepares a digest, reminder, notification, or mail draft.
6. Sensitive outbound effects require an exact, cross-surface approval and remain idempotent after
   restart or retry.

### 4.2 Keel Mailbox and ToDos

1. Each user has one Primary Keel Mailbox and may add private Purpose Mailboxes within deployment
   quota; all remain independent of personal-Agent changes.
2. Incoming mail is durably received as untrusted content and can be triaged into a private email
   Session, draft, or proposed ToDo.
3. Keel may send a versioned template notification to the user's verified email without per-message
   approval.
4. Freeform mail, other recipients, replies, forwards, and attachments require approval bound to
   the exact draft.
5. ToDo reminders survive restart and cannot be duplicated by delivery retry.

### 4.3 Team Agent

1. An admin creates a team Agent and grants selected team resources.
2. Explicit Agent-access members or mapped channels use it from Web and IM.
3. Private personal memory, Connections, and projects are never visible.
4. Private Web sessions remain private unless their visibility is explicitly changed.
5. Membership, Agent access, grants, actions, and failures are auditable.

### 4.4 Managed project

1. An authorized user connects GitHub and imports a repository as an organization-owned Project.
2. A read-only review produces an immutable evidence-checked report.
3. A controlled patch request produces an immutable candidate revision.
4. A human approves that exact revision before the trusted control plane may create a branch and
   Draft PR.
5. Build/test execution is offered only when a qualified per-run sandbox can enforce it.

### 4.5 Administration and operations

Admins and operators can manage identity, Agents, Connections, grants, Routines, mailboxes,
notifications, approvals, jobs, retention/erasure, health, costs, and incidents without direct
database edits.

## 5. Canonical product concepts

The product language is Actor, Organization, Agent, Agent Access, Connection, Resource, Grant,
Routine, Session, Run, Job, Approval, Effect, Artifact, Keel Mailbox, ToDo, and Notification. See
[ADR-0011](./adr/0011-product-boundary-and-domain-model.md) and
[ADR-0012](./adr/0012-user-mailboxes-todos-notifications.md), refined by
[ADR-0013](./adr/0013-mailbox-portfolio-and-todo-experience.md).

`scope_id` is an internal isolation key. It must not be exposed as the user's mental model for
selecting an Agent, sharing a resource, or authorizing an action.

## 6. Functional requirements

Priority: **P0** initial single-organization product, **P1** fast follow, **P2** later.

### 6.1 Identity, Agents, and governance

| ID | Requirement | Priority |
|---|---|---|
| ID-1 | Browser OIDC authorization-code login with secure server-managed session. | P0 |
| ID-2 | One active organization per initial production deployment with owner/admin/member/viewer roles. | P0 |
| ID-3 | Versioned personal/team Agent definitions: persona, model, tools, resources, memory policy, budgets, and autonomy defaults. | P0 |
| ID-4 | Private personal Agent provisioning and explicit team Agent access for users/channels. | P0 |
| ID-5 | Actor authority, Agent access, and Agent resource grants are intersected and fail closed. | P0 |
| ID-6 | Admin UI/API for users, Agents, Connections, grants, Routines, audit, retention, and safety state. | P0 |
| ID-7 | Initial production enforces one active organization per deployment. | P0 |
| ID-8 | Hosted multi-organization tenancy, billing, and regional policy. | P2 |

### 6.2 Durable interaction and routines

| ID | Requirement | Priority |
|---|---|---|
| RUN-1 | Bounded runs with named termination, persist-before-call, and replayable typed events. | P0 |
| RUN-2 | Worker-owned interactive runs survive API restart and support cancel, interrupt, steer, and approval. | P0 |
| RUN-3 | A Routine binds trigger, Agent, input, allowed resources/actions, budget, approval policy, owner, and delivery target. | P0 |
| RUN-4 | Accepted Routine occurrences are never silently lost and duplicate delivery cannot duplicate work. | P0 |
| RUN-5 | Web, email, and IM share the same Agent, session, run, approval, and result state. | P0 |
| RUN-6 | Sessions record owner/channel and visibility; team Agent access does not imply access to every private session. | P0 |
| RUN-7 | Child/sub-agent runs inherit immutable authority and one shared budget without expansion. | P2 |

### 6.3 Connections and effects

| ID | Requirement | Priority |
|---|---|---|
| CON-1 | First-class multi-account Connections owned by a user or organization, initially Gmail and Google Calendar. | P0 |
| CON-2 | Users select provider resources and grant explicit capabilities to Agents/Routines. | P0 |
| CON-3 | Connector manifests declare action semantics, OAuth scopes, risk, approval, idempotency, reconciliation, provenance, health, and worker capability. | P0 |
| CON-4 | External content retains provenance, sensitivity, and untrusted influence. | P0 |
| CON-5 | Effects use durable states including ambiguous/unknown provider outcomes and reconcile before retry. | P0 |
| CON-6 | Additional providers graduate from experimental only after common lifecycle/security suites. | P1 |

A Routine may narrow an Agent's granted resources/actions but may never expand them.

### 6.4 Memory, Knowledge, and lifecycle

| ID | Requirement | Priority |
|---|---|---|
| DATA-1 | Durable session history and lexical/semantic recall. | P0 |
| DATA-2 | User-managed core/profile memory with version history; model learning is proposal-first with provenance. | P0 |
| DATA-3 | Knowledge ingest/version/delete/search with citations, taint, and embedding pinning. | P0 |
| DATA-4 | Event upcasters and tombstone-aware projection rebuilds. | P0 |
| DATA-5 | Audited retention and erasure across owned stores; external gaps are reported honestly. | P0 |
| DATA-6 | Run-local scratch state is separate from durable memory and Knowledge. | P0 |

### 6.5 Keel Mailbox, ToDos, and notifications

| ID | Requirement | Priority |
|---|---|---|
| MAIL-1 | When Keel Mail is enabled, each user has exactly one active Primary Keel Mailbox and may have additional private Purpose Mailboxes within deployment quota; none is owned by a persisted Agent or modeled as a user Connection. | P0 |
| MAIL-2 | The user's human delivery address has explicit verification, opt-in, timezone, quiet-hours, and channel preferences. | P0 |
| MAIL-3 | Signed inbound events are durably deduplicated, stored with provenance/taint, and cannot confer user authority from a sender address alone. | P0 |
| MAIL-4 | Only versioned template notifications to the user's verified address bypass per-message approval; all other mail binds approval to the exact draft. | P0 |
| MAIL-5 | Mail sends use durable Effect states, provider idempotency, delivery/bounce evidence, and reconciliation before retry after an ambiguous outcome. | P0 |
| TODO-1 | ToDos are user-owned within an organization and survive Agent replacement or deletion. | P0 |
| TODO-2 | Users can create, read, update, complete, reopen, cancel, archive, filter, inspect provenance, and accept/dismiss proposals through Web and chat. | P0 |
| TODO-3 | ToDo mutations use optimistic versioning, explicit policy, audit history, and user-derived ownership rather than caller-supplied scope identifiers. | P0 |
| TODO-4 | Due reminders create durable Notifications whose pending occurrences are atomically replaced or cancelled when the ToDo changes. | P0 |

### 6.6 Managed projects and controlled code changes

| ID | Requirement | Priority |
|---|---|---|
| CODE-1 | Organization-owned Projects with GitHub App import/sync and explicit Agent grants. | P1 |
| CODE-2 | Read-only review produces immutable, bounded, evidence-verified reports. | P1 |
| CODE-3 | Patch approval binds an immutable candidate revision and trusted writeback creates only a branch and Draft PR. | P1 |
| CODE-4 | Patch generation labels validation truthfully; no build/test claim without actual execution. | P1 |
| CODE-5 | Shell/build/test runs only in an ephemeral, resource-bounded, default-deny execution environment. | P2 |
| CODE-6 | Automatic merge, default-branch push, force-push, and host Docker socket access are forbidden. | P0 |

### 6.7 Operations and extensibility

| ID | Requirement | Priority |
|---|---|---|
| OPS-1 | Non-owner runtime database principal and enforced RLS. | P0 |
| OPS-2 | Separate scheduler and capability-specific worker pools. | P0 |
| OPS-3 | End-to-end traces, metrics, usage/cost reconciliation, alerts, and SLOs. | P0 |
| OPS-4 | Tested install, upgrade, rollback, backup, restore, and DR. | P0 |
| OPS-5 | Production trust profiles never substitute in-process execution or implicit admin. | P0 |
| EXT-1 | Skills and MCP remain governed tool sources with import-not-trust controls. | P1 |
| EXT-2 | Public plugin SDK and local desktop executor wait for stable contracts and security gates. | P2 |

## 7. Non-functional requirements

| Area | Requirement |
|---|---|
| Isolation | Automated two-user/private-team tests prove no unauthorized Agent discovery, session read, or cross-resource access. |
| Reliability | No admitted run, accepted Routine occurrence, pending approval, ToDo reminder, Notification delivery, or confirmed effect is lost after restart. |
| Security | Explicit policy, isolated execution, least privilege, SSRF/egress controls, secret isolation, authenticated webhooks, and audit. |
| Privacy | Documented data map for mail and ToDos, configurable retention, redaction, verified erasure, and no false claim when an external deletion cannot be proven. |
| Performance | Management surfaces remain usable with production-sized histories; provider latency is measured separately. |
| Accessibility/i18n | Core journeys are keyboard-operable and screen-reader-labeled; English and Simplified Chinese share one product implementation. |
| Operability | Health/readiness, structured logs, traces/metrics, queue/run/effect diagnosis, and tested recovery. |
| Compatibility | Event upcasters and additive API policy protect stored histories and clients. |

## 8. Success measures

- A new user completes sign-in, personal Agent setup, and one connected read-only task without
  operator intervention.
- Personal and team journeys show zero unauthorized Agent discovery, private-session access, or
  resource access in adversarial acceptance tests.
- Accepted Routine occurrences and approvals survive induced crashes with no duplicate effect.
- A user creates a ToDo, receives one due notification from their own Keel Mailbox, and can complete
  or reschedule it without a stale reminder.
- Two users cannot discover or use each other's Keel Mailboxes, mail, notifications, or ToDos.
- Gmail and Calendar happy paths meet declared success and revoke/refresh error budgets.
- Memory and Knowledge quality gates remain deterministic and published.
- Controlled patch approval creates the exact reviewed Draft PR revision.
- Production release requires measured SLOs and a backup/restore drill meeting declared RPO/RTO.

## 9. Non-goals

- Billing, marketplace, and broad multi-organization SaaS in the initial product.
- A visual no-code workflow builder; integrate with automation platforms instead.
- Bulk marketing, unsolicited outreach, or a general email-campaign product.
- A full project-management/issue-tracking suite in the initial ToDo release.
- Shared/team mailboxes in the initial personal-Mail release.
- Model training/fine-tuning.
- Native mobile applications.
- A browser IDE or unrestricted remote-code-execution service.
- Automatic merge or autonomous default-branch writes.
- Connector quantity at the expense of lifecycle, security, and usable journeys.
