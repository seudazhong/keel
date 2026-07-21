# ADR-0011: Product boundary and domain model

- **Status:** Accepted
- **Date:** 2026-07-21
- **Refines:** ADR-0008 and ADR-0009
- **Related:** ADR-0005, ADR-0010

## Context

ADR-0009 correctly made Keel a server-primary connected assistant centered on personal/team
Agents, connectors, memory, and messaging. Since then the implementation added durable identity,
projects, read-only review, and controlled patch generation. Documentation began to conflate:

- Agent identity with a storage `scope_id`;
- organization tenancy with a personal workspace;
- schedules with autonomous behavior;
- connector bindings with reusable external accounts;
- managed code projects with the whole product.

The result was a strong implementation foundation but an unclear product and authorization model.

## Decision

### 1. Product boundary

Keel is a **single-organization-first cloud agent platform**:

- every user may have a private personal Agent;
- team Agents are shared only through explicit Agent access and resource grants;
- Web, IM, schedules, and operator tools use one durable runtime;
- connectors, memory, Knowledge, and projects are resources governed through the same authority
  model;
- managed code review and controlled patch proposals are an optional capability pack, not the
  platform identity.

### 2. Domain model

The canonical concepts are:

| Concept | Meaning |
|---|---|
| Actor | A user, machine, or system principal initiating work. |
| Organization | The tenant and administrative boundary. Initial production supports one active organization per deployment. |
| Agent | A persisted execution identity and versioned configuration. Personal and team are Agent kinds. |
| Agent access | An explicit user/channel-to-Agent access edge controlling who may discover, use, or manage a team Agent. |
| Connection | A credential-bearing external account owned by a user or organization, with child provider resources. |
| Resource | A connector account/resource, Knowledge base, project, or other governed object. |
| Grant | An explicit Agent capability on a resource, intersected with the actor's authority. |
| Routine | A trigger plus Agent, input, allowed resources/actions, budget, approval policy, and delivery target. |
| Session | A durable conversation/context stream under an Agent with explicit owner/channel and visibility policy. |
| Run | One durable, bounded execution attempt using immutable admitted configuration. |
| Job | Background work with lease, retry, cancellation, and reconciliation semantics. |
| Approval | A human decision bound to an exact action or immutable candidate revision. |
| Effect | An external mutation with durable idempotency and reconciliation state. |
| Artifact | Immutable or retained output such as a report, patch bundle, or log. |

`scope_id` remains an internal data-partition key derived from organization and Agent. It is not a
user-facing product identity and is never sufficient authorization by itself.

Effective authority is the intersection of:

```text
actor membership
  x Agent access
  x Agent authority
  x Routine policy
  x resource grant
  x deployment capability
```

### 3. Trust zones

Keel uses four explicit zones:

1. **Control plane:** identity, Agent/Routine definitions, grants, connector configuration,
   approvals, audit, and API.
2. **Orchestration plane:** durable admission, event log, model calls, context assembly, jobs,
   scheduling, and recovery. It holds no arbitrary-code execution authority.
3. **Trusted effect plane:** narrow connector effect brokers and a separate Git writeback broker
   using just-in-time credentials.
4. **Untrusted execution plane:** sandboxed file/command execution with no control-plane
   credentials.

Routine policy can only attenuate the selected Agent's authority. It can never add a resource,
credential, or capability the Agent was not granted.

### 4. Browser authentication

Production browser login uses OIDC authorization code with PKCE and a server-managed secure,
HTTP-only session. Raw long-lived bearer tokens and API keys are not normal browser storage.
API keys remain machine credentials. The current credential-paste/local-preview UI is explicitly a
preview compatibility path.

### 5. Release strategy

Delivery is scenario-based:

- trusted preview first, with explicit limitations;
- connected personal-agent experience as the mainline product;
- controlled code review/patching as an optional parallel capability;
- single-organization multi-user/team beta only after browser identity and adversarial isolation
  evidence;
- production and hostile multi-tenant claims only after stronger execution isolation,
  observability, and DR gates.

## Consequences

- Agent definitions must grow beyond name/persona and be versioned at run admission.
- Team Agent access and session visibility must be explicit; organization membership alone is not
  permission to discover every team Agent or read every session.
- Autonomous work moves from hard-coded schedules into first-class Routines.
- External Connections must have user/organization ownership, support multiple accounts and child
  resources, and migrate from one binding per connector per scope.
- The initial production profile must enforce its one-active-organization policy rather than merely
  document it.
- Runtime policy construction must fail closed; an omitted policy cannot mean allow-all.
- Accepted schedule occurrences and ambiguous external effects need durable outbox/reconciliation
  state.
- Documentation and UI use Organization, Agent, Routine, Connection, Resource, and Grant; `scope`
  remains an implementation detail except in operator diagnostics.
- Coding work may not introduce a second authorization, credential, or execution path.
