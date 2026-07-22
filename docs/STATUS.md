# Keel implementation status

> **Snapshot:** 2026-07-22
> **Branch:** `main`
> **Implementation baseline reviewed:** `10f6806`
> **Migration head:** `0026_effect_ledger`
> **Target:** [Product requirements](./PRD.md)
> **Next work:** [Roadmap](./ROADMAP.md)

This document is the authority for what is true now. It intentionally avoids historical milestone
claims and volatile test-count copies.

Evidence is reproducible through the committed CI workflow and the commands in
[Development](./DEVELOPMENT.md). Capability-specific evidence is linked from the subsystem
references in [Documentation](./README.md).

## Maturity scale

| Level | Meaning |
|---|---|
| **C — Code** | Implementation exists on `main`. |
| **T — Tested** | Automated tests cover the behavior. |
| **D — Deployable** | The standard Compose preview wires and starts it. |
| **P — Product** | A coherent user journey works through a shipped surface. |

`yes` = met, `partial` = useful but incomplete/preview, `no` = not met.

## Executive summary

Keel has a strong durable runtime and data foundation. The current Compose stack is healthy with:

- a non-owner `keel_runtime_login` data plane and enforced RLS;
- shared Postgres run/event/job state and Redis delivery;
- worker-owned interactive run admission;
- an authenticated out-of-process sandbox;
- the built React application.

The product remains a **trusted local/single-operator preview**. Backend capability has outpaced
product cohesion: identity, lifecycle, connectors, projects, review, and patch infrastructure are
substantial, while browser login, admin/team journeys, review/patch UI, safe shell execution, and
production operations remain incomplete.

## Capability matrix

| Capability | C | T | D | P | Current truth |
|---|:--:|:--:|:--:|:--:|---|
| Durable chat/session/run pipeline | yes | yes | yes | partial | Worker-owned admission, replayable events, interrupt/steer/approval, recovery, explicit fail-closed permission construction, an immutable per-run `AgentConfigSnapshot` bound into the admission fingerprint, and (R1B) session ownership/visibility enforced independent of the selected Agent's scope on every session read endpoint exist. Product use is still preview-oriented. |
| Jobs, schedules, approvals, outboxes | yes | yes | yes | partial | Durable jobs and approvals are strong. Schedules still use legacy hard-coded agent behavior and zero-or-one trigger delivery. |
| Memory and session recall | yes | yes | yes | partial | Core/archival memory, search, consolidation, proposals, and UI exist. Interactive tools mutate directly; consolidation reads scope-wide sessions and auto-commits high-confidence archival facts. |
| Knowledge Base | yes | yes | yes | partial | Lifecycle, chunking, hybrid retrieval, citations, taint, connector ingest, and React management/search are present. |
| Identity, organizations, Agents, grants | yes | yes | yes | partial | Backend and preview UI exist. R1B added first-class Agent Access edges (discover/use/manage; bare org membership no longer implies team-Agent access) and session ownership/visibility (private/agent_members/explicit), both API-only. No browser OIDC flow, enforced one-org policy, or complete admin journey (Agent Access/session-visibility UI). Persisted Agent definition is still thin. |
| Per-user Keel Mailboxes | no | no | no | no | AgentMail and the Primary/Purpose mailbox UX are designed in ADR-0012/0013, but provisioning, inbound mail, drafts, notifications, and UI are not implemented. |
| User ToDos and reminders | no | no | no | no | ADR-0012/0013 define ownership, UI, proposals, and Agent tools; no API, tools, persistence, reminders, or React surface exist. |
| Event evolution and data lifecycle | yes | yes | yes | partial | Upcasters, rebuild checkpoints/tombstones, retention classes, durable erasure, identity purge, and complete migration-table classification exist. Product administration and external-provider erasure remain partial. |
| Connector framework | yes | yes | yes | partial | Manifest discovery, encrypted credentials, routed webhooks, recurring sync, provenance, taint, and durable actions exist. Provider maturity and setup UX differ. |
| Durable Effect ledger (R1B, C4/C5) | yes | yes | yes | partial | Generic `reserved -> executing -> {confirmed, unknown, failed}` state machine with fenced single-winner execution, worker-cron reconciliation, and an API/SDK surface exist (migration `0026_effect_ledger`). Reconciliation capability is implemented for Gmail send and Google Calendar create/update only; every other connector's `unknown` Effects surface via the API for an operator/user decision rather than being reconciled automatically. No React history/status surface yet. |
| Gmail | yes | yes | yes | partial | Mail read and approval-gated send exist. The connected personal-Agent journey still needs common release qualification. |
| Google Calendar | yes | yes | yes | partial | Read/sync/create/update exist with incremental OAuth and approval. It has less product history than Gmail and needs its own end-to-end qualification. |
| Other connector providers | yes | yes | partial | no | Drive/Docs, Microsoft 365, Notion, Feishu, GitHub collaboration, RSS/Atom, and webhook providers exist but are not all product-qualified. |
| Managed projects and GitHub sync | yes | yes | yes | partial | API and React project/import surfaces exist; GitHub App setup is still operator-heavy. |
| Read-only code review | yes | yes | yes | no | Durable API/worker/report path exists. No React review surface ships. |
| Controlled patch proposals | yes | yes | partial | no | Proposal store, generation, sandbox transfer, jobs, outbox, approval, writeback, and reconciler exist. No public API/SDK/UI. |
| IM routing | yes | yes | partial | no | OneBot/Telegram ingress, durable mappings/replies, webhook authentication, and worker routing exist; admission now wires channel session identity/visibility (R1B: private 1:1 chat owned by the run-as user, group chat bound to the channel with agent_members visibility). Admin and cross-surface product journeys do not exist. |
| Runtime DB isolation | yes | yes | yes | n/a | Standard Compose uses the least-privilege runtime login; owner/RLS bypass is denied. |
| Sandbox execution | yes | yes | yes | n/a | File operations run out of process. Compose uses one hardened container with per-scope namespaces; shell/build/test stays disabled. |
| React application | yes | yes | yes | partial | Broad navigation and pages ship. Several pages are explicitly Preview or lack complete workflows. |
| Production operations | partial | partial | no | no | K8s scaffold, readiness, and hardened defaults exist; separate scheduler, complete telemetry, scale proof, and DR remain open. |

## Product scenarios that work today

### Trusted local connected-agent preview

- Start the Compose stack and enter local preview.
- Chat through durable sessions and resume prior sessions.
- Inspect and manage Memory, Knowledge, Jobs, Schedules, and Approvals.
- Configure supported connectors and expose their actions to the interactive runtime.

This is useful but not a production identity/team journey.

### Managed project preview

- Create or import an organization-owned Project through the React/API surface.
- Synchronize through a configured GitHub App.
- Request and retrieve a read-only review through the API.

Review results do not yet have a React surface.

## Important gaps

1. **Browser identity:** no OIDC authorization-code/PKCE login or secure browser session.
2. **Domain completeness:** Agent definitions are not yet the versioned home for model, tools,
   resources, memory policy, budgets, and autonomy; Routine and reusable multi-account Connection
   concepts are missing. Agent Access and Session Visibility now exist as a durable model + API
   (R1B) but have no admin UI, and session content has no audited org-admin "support access"
   override (private is private, even from admins, in this PR).
3. **Memory authority:** normal interactive Agents can directly call memory mutation tools;
   consolidation is not surface/trust-aware and auto-commits high-confidence archival facts. The
   proposal-first model is not yet enforced.
4. **Autonomous-effect correctness:** accepted schedule occurrences may be lost between claim and
   enqueue, and generic connector effects need an explicit ambiguous/unknown reconciliation state.
5. **Product cohesion:** review is headless; patch has no API/UI; team/IM administration is absent.
6. **Personal task and communication:** there is no per-user Keel Mailbox, verified notification
   endpoint, durable Notification model, or user ToDo surface.
7. **Execution:** standard sandbox supports isolated file operations only, not safe build/test or
   general coding-agent command execution.
8. **Production:** no separate scheduler, trusted effect/capability worker pools, complete
   OTel/metrics/SLOs,
   proven horizontal topology, or tested backup/restore/DR.

## Safety contract

The Compose `dev`/`full` stack is a trusted preview:

- do not expose open local-preview mode to untrusted networks;
- the sandbox is a rootless-OCI floor, not a hostile-tenant microVM boundary;
- shell is disabled because directory namespaces are not sufficient command isolation;
- GitHub and connector credentials stay in the control plane;
- API keys are machine credentials, not a browser login design;
- production claims require the gates in [Roadmap](./ROADMAP.md).
