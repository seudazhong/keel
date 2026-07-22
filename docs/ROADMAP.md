# Keel roadmap

> **Updated:** 2026-07-22
> **Authority:** Active outcome sequence
> **Baseline:** [Status](./STATUS.md)

The previous M0-M9 numbering is retained only in historical plans, old commits, and dated design
documents. This roadmap uses release outcomes so implementation work cannot be mistaken for a
usable product scenario.

## Delivery policy

- The connected personal/team Agent product is the mainline.
- Controlled code review/patching is an optional capability track, not a prerequisite for the
  personal assistant.
- Visible vertical slices are delivered early. Authority and external-effect correctness are
  load-bearing; Schedule occurrence hardening is explicitly deferred and documented.
- Platform browser OIDC is deferred until the multi-user release; connector-specific OAuth ships
  with the connector that needs it.
- Do not add net-new provider breadth before Gmail and Google Calendar pass the common Connection
  lifecycle, security, effect, and product-journey gates.
- AgentMail is a Keel-owned communication provider, not another user Connection; it still must pass
  the same webhook, secret, effect, reconciliation, lifecycle, and product-journey gates.
- No release closes on code or tests alone; its end-to-end exit scenarios must pass.

## R0 — Truth and design baseline

**Status:** complete with this documentation baseline.

**Outcome:** one product boundary, one domain vocabulary, truthful current status, and one active
roadmap.

**Exit evidence:**

- README, PRD, Architecture, Status, Roadmap, and Invariants have non-overlapping authority.
- ADR-0011 defines Agent Access, Connection/Resource/Grant, Routine/Effect, session visibility, and
  the four trust zones.
- ADR-0012 defines per-user Keel Mailboxes, user-owned ToDos, and durable Notification policy.
- historical plans are clearly non-authoritative;
- `uv run python scripts/check_markdown_links.py`, `git diff --check`, and the CI documentation
  gate pass;
- the canonical set has been cross-reviewed against the current OpenAPI, migration head, and
  implementation modules.

## R1 — Preview convergence

**Status:** complete for the narrowed trusted-preview scope.

R1 deliberately closed on the existing product rather than adding new autonomy or account models.
It removed an incomplete product surface and retained the proven backend safety work already merged.

### R1A — Visible assistant coherence

**Outcome:** one honest local/single-operator preview with no shipped Agent-management tab.

**Exit evidence:**

- durable Chat/Session resume already works and was manually verified by the owner;
- the incomplete Agents navigation item and product page are removed; legacy `/agents` links redirect
  to Chat;
- the current Chat, Sessions, Memory, Knowledge, Connectors, Projects, Approvals, Schedules, Jobs,
  Observability, and Settings routes pass a real Compose browser smoke;
- current Memory, connector, and Schedule behavior is retained rather than changed without product
  evidence.

### R1B — Correctness and domain foundation

**Outcome:** the merged safety foundation includes explicit fail-closed permissions, immutable Agent
configuration snapshots, Agent Access and Session Visibility, durable Effects with `unknown`
reconciliation, and complete lifecycle classification.

### Explicitly deferred from R1

- first-class Routine, accepted-occurrence/outbox, and Scheduler/reconciler hardening;
- user/organization-owned multi-account Connections (moves to R2);
- proposal-first Memory, citation UI, and detailed run/effect status UI pending product evidence;
- Gmail/Calendar product qualification beyond their existing preview behavior;
- browser Agent administration; backend Agent identity and authorization remain internal/API-only.

## R2 — Connected personal Agent

**Dependencies:** R1A and R1B.

**Goal:** deliver the primary product for a trusted single operator before platform login work.

**Deliverables:**

- a complete personal assistant definition: model, persona, tools, Connections/resources, memory
  policy, and budgets;
- one private Primary Keel Mailbox per user plus optional Purpose Mailboxes within quota, a verified
  human delivery endpoint, and safe inbound mail triage that fails closed when
  organization/personal-Agent routing is ambiguous;
- user-owned ToDos managed through full Web/chat surfaces with proposals, provenance, and durable
  reminders;
- durable Notifications, including template-only email delivery to the verified user address;
- Gmail + Google Calendar as product-supported Connections with common lifecycle/security tests;
- user-managed memory with a learning policy selected from measured product evidence;
- consistent Web approvals and effect history;
- onboarding that explains stored data, grants, and approval policy.

**Exit scenario:** from an empty preview, one operator configures a personal Agent and Primary Keel
Mailbox, adds one Purpose Mailbox without cross-routing threads, verifies a delivery address,
connects Gmail and Calendar, completes a cited read task, creates a ToDo, receives exactly one email
reminder from the Primary Mailbox, approves one exact outbound effect, and sees the result survive a
service restart.

**Non-goals:** browser OIDC, team Agents, broad connector catalog.

## R3 — Controlled code workflows

**Track:** may proceed after R1B without blocking R2.

**Goal:** productize the backend already built for managed code while preserving the platform trust
model.

**Deliverables:**

- self-service GitHub connection and Project import without normal users handling PEM paths or
  installation identifiers;
- React read-only review request/status/report surface;
- public Patch API and typed SDK;
- proposal list/detail/diff/approval UI;
- exact immutable candidate approval and Draft PR writeback;
- durable generation transcript/tool evidence;
- truthful validation state: file-only/unvalidated unless tests actually ran;
- separate patch-capable worker pool and default-off capability flag outside the preview profile.

**Exit scenario:** import a repository, request review, request a controlled patch, inspect the
candidate, approve the exact revision, and receive a GitHub Draft PR. Default-branch push, merge,
and force-push remain impossible.

**Non-goal:** arbitrary shell/build/test or opaque vendor coding agents.

## R4 — Single-organization multi-user and team beta

**Dependencies:** R1B and R2; R3 is optional.

**Goal:** turn backend identity and grants into a real multi-user product.

**Deliverables:**

- browser OIDC authorization code + PKCE with secure HTTP-only session;
- enforcement of one active organization for the initial production profile;
- automatic private personal Agent provisioning;
- automatic private Primary Keel Mailbox provisioning for every user, Purpose Mailbox quota
  enforcement, and adversarial cross-user mail/ToDo isolation;
- membership, Agent Access, Connection, grant, Routine, and audit administration;
- team Agents with explicit user/channel access and resources;
- explicit session owner/channel and visibility policy;
- Web/email/IM continuity for sessions, approvals, and results;
- no user-facing dependence on `scope_id`;
- local open mode isolated to an explicit preview profile.

**Exit gates:**

- two users, two private Agents, and one team Agent pass adversarial Agent-discovery and isolation
  tests;
- a team member cannot read another member's private Web session without an explicit visibility
  grant;
- a private Connection/Memory/Project cannot be discovered or used by the other user/team Agent;
- membership/grant revocation takes effect on worker claim and before effects;
- Web and IM show the same durable approval and result.

## R5 — Production single-organization profile

**Dependencies:** R4 and any capability intended for production.

**Goal:** a supportable cloud deployment with declared reliability and security boundaries.

**Deliverables:**

- separate long-lived scheduler, orchestration workers, trusted effect brokers, and
  capability-specific worker pools;
- N-server/N-worker scale and rolling-upgrade proof;
- OTel export, metrics, usage/cost reconciliation, SLOs, dashboards, and alerts;
- object storage/retention for artifacts where required;
- secret-manager/KMS integration and rotation procedures;
- tested install, upgrade, rollback, backup, restore, and DR with declared RPO/RTO;
- production Kubernetes profile with no implicit admin or unsafe execution fallback.

**Exit gates:** load/chaos, isolation, restore, and incident-runbook drills pass against the
production profile.

## R6 — Executable coding and ecosystem

**Dependencies:** R5.

**Goal:** extend the stable platform without weakening its authority or execution model.

**Candidates:**

- ephemeral per-run shell/build/test sandbox with quotas, default-deny egress, and no host Docker
  socket;
- controlled-tool and explicitly labeled opaque-CLI adapter modes;
- qualified OSS coding-agent adapter before subscription-bound vendor adapters;
- public plugin SDK and compatibility suite;
- curated additional connectors;
- LocalDaemon/desktop access for explicitly approved local resources;
- multi-organization tenancy only after stronger isolation and adversarial evidence.

## Dependency summary

```text
R0 -> R1A --\
 \-> R1B ---> R2 -> R4 -> R5 -> R6
      \------> R3 -----------/
```

R2 remains the mainline product. R3 is an optional capability track. R4 and R5 cannot claim
multi-user or production readiness by inheriting RLS or sandbox infrastructure alone.
