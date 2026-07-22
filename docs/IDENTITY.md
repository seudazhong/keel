# Identity, organizations, and persisted Agents

> **Status:** Living subsystem reference
> **Product gap:** browser OIDC authorization-code flow and complete administration UI

Keel has a durable identity and authorization foundation for users, organizations, memberships,
personal/team Agents, explicit resource grants, first-class Agent Access edges (R1B), and session
ownership/visibility (R1B) independent of Agent access.

## Model

| Entity | Purpose |
|---|---|
| User | Global human identity. |
| OIDC identity | `(issuer, subject)` link to a User. |
| Organization | Tenant and administration boundary. |
| Membership | User-to-organization role: owner, admin, member, or viewer. |
| Agent | Persisted personal or team execution identity. |
| Resource grant | Agent capability on one resource inside an organization. |
| Agent Access | `(org, agent, user\|channel principal) -> discover\|use\|manage` edge on a **team** Agent. |
| Session identity | A session's `owner_user_id`/channel identity + `visibility` policy. |
| Session share | An explicit per-user read grant on one session (`visibility = 'explicit'`). |

Personal Agents are private to their owner. Team Agents require an explicit **Agent Access** edge
for discovery/use — bare organization membership no longer implies either. An org admin/owner keeps
an explicit administrative path (equivalent to an implicit `manage` edge on every team Agent in its
org); every other member/viewer needs an active edge. Access levels are ordered — `manage` implies
`use` implies `discover` — and revocation takes effect at the very next authorization check
(admission, worker claim, or an API read), not on a timer.

Session ownership/visibility is a **separate axis** from Agent access: using a team Agent never by
itself grants reading another user's private session (INVARIANTS.md C2/C6).

## Request actors

Each request resolves one actor without global mutable state:

- **user** — verified asymmetric OIDC bearer JWT mapped to a durable User;
- **machine** — hashed/scoped API key;
- **local** — explicit non-cloud preview operator.

A verified OIDC subject grants no organization authority by itself. The user must be an active
member of the selected organization, and — for a **team** Agent — additionally hold an active Agent
Access edge (see below).

## Per-Agent runtime routing

Interactive admission derives the canonical data scope from organization and Agent:

```text
agent:<organization-id>/<agent-id>
```

`web:local` remains only the non-cloud local-preview compatibility scope. Authenticated callers
cannot obtain authority by naming a scope directly. This scope is a **partition**, not authority —
see "Session ownership and visibility" below.

## Authorization

Organization roles provide coarse capabilities:

| Role | Capabilities |
|---|---|
| viewer | read |
| member | read, use |
| admin | read, use, write, manage |
| owner | read, use, write, manage |

### Agent Access (R1B)

A **team** Agent additionally requires an active Agent Access edge for a member/viewer to discover
or use it:

| Level | Grants |
|---|---|
| `discover` | List/read the Agent's existence and profile. |
| `use` | Select/admit runs against the Agent (implies `discover`). |
| `manage` | Edit/archive the Agent and administer its own Agent Access list (implies `use`). |

`principal_type` is `user` (a durable User) or `channel` (an opaque, caller-defined channel
identity, e.g. an IM chat/room key) — a channel edge never authorizes a human actor directly; IM
admission resolves a channel's edge against its own identity
(`IdentityService.authorize_channel_agent_access`). An org admin/owner's administrative path
behaves as an implicit `manage` edge on every team Agent in the org. Personal Agents never carry
Agent Access edges — they remain owner-private via `Agent.kind`.

For an IM mapping, the canonical channel principal id is its pseudonymous `route_key`; mapping API
responses expose it as `channel_principal_id`. A group-chat mapping requires both an authorized
run-as user and an active channel edge at `use` or higher. Admission and worker claim re-check the
edge, so revocation stops new and already-queued work.

An Agent operating on a *resource* (Knowledge/Connectors/Projects) receives the intersection of the
actor's organization capability, its Agent Access level, and the Agent's explicit resource grant.
Personal-Agent ownership and last-owner protection add further constraints.

Membership mutations lock the organization and affected memberships in deterministic order so
concurrent owner changes cannot leave an organization ownerless.

## Session ownership and visibility (R1B)

Every session additively carries an owner/channel identity and a `visibility` policy, independent
of the Agent-scope selection that gates read/write access to the durable data plane:

| Visibility | Who can read (besides the owner) |
|---|---|
| `private` (default for a new session) | Nobody else. |
| `agent_members` | Any principal holding an **active Agent Access edge** on the session's Agent — never bare org membership. |
| `explicit` | Only users with an active `session_access` share row. |

A session created via **durable Web admission** records `owner_user_id` (the admitting user) with
`visibility = 'private'`. A session created via **IM admission** records a channel identity
(`channel_provider`/`channel_external_id`); a private 1:1 chat additionally sets `owner_user_id` to
the mapping's run-as user (`visibility = 'private'`), while a group chat has no single owner
(`visibility = 'agent_members'`). This identity is written **before** the admitted prompt via a
single idempotent `INSERT ... ON CONFLICT DO NOTHING` (`keel_core.session_visibility.
ensure_session_identity`) — first-writer-wins, so a crash or retried admission can never create an
ownerless or ambiguously-visible accepted session.

A legacy session (no organization, owner, or channel identity and backfilled as `agent_members` —
predates this feature, or was created by a surface this PR did not touch: schedules, CLI,
connector-triggered runs) falls back to the pre-R1B Agent-scope-only gate rather than being newly
locked out. Removing an owner from a new private session never turns it into a legacy shared row.

Session message admission, list/history/event-stream/run-detail, and run interrupt/steer endpoints
under `/v1` enforce this visibility in addition to (never instead of) the selected Agent's scope.
Mutating a session's visibility or share list requires ownership or Agent-manage authority.

## Row-level security

Tenant-owned identity tables (including `agent_access`) carry `org_id`, use
`FORCE ROW LEVEL SECURITY`, and are accessed by the non-owner runtime principal. `session_access`
carries `scope_id` and is RLS-scoped the same way as `sessions`/`events`. Repositories set
request-scoped PostgreSQL context before reads and writes.

RLS is defense in depth. API/service authorization remains mandatory.

## Browser authentication

The backend can verify OIDC JWTs, including issuer/audience/signature/time claims and safe
asymmetric algorithms.

The React application does **not** implement the provider redirect/callback login flow. It currently
supports:

- explicit local preview;
- directly supplied API key;
- directly supplied bearer token.

The target production flow is authorization code + PKCE with a secure HTTP-only server-managed
session. API keys remain machine credentials and raw long-lived tokens are not normal browser
storage.

## REST API

Under `/v1/identity`:

- current user and organizations;
- organization membership list/create/update/remove;
- Agent list/create/read/update/archive/select;
- Agent Access list/grant/revoke (`/v1/identity/agents/{id}/access`);
- grant list/create/revoke.

Under `/v1`:

- session visibility read/update (`/v1/sessions/{id}/visibility`);
- session explicit-share list/create/revoke (`/v1/sessions/{id}/shares`).

Agent, membership, grant, Agent-Access, and session-visibility administration remain API-only. The
incomplete Agents preview page was removed from the shipped R1 product navigation; legacy
`/agents` links redirect to Chat.

## Identity erasure

User and organization erasure are cross-tenant maintenance operations. They execute through
locked-down `SECURITY DEFINER` functions using a dedicated maintenance login with function execute
permission but no direct table delete or `BYPASSRLS`. `agent_access` cascades on organization
erasure (`org_id` FK), Agent erasure, grantor erasure, and user-principal erasure. User principals
carry a structurally checked `principal_user_id` FK while channel principals remain opaque.

The current operator path is:

```powershell
python -m keel_core.identity.erase_cli user <user-id> --dry-run --json
python -m keel_core.identity.erase_cli user <user-id> --yes --json
python -m keel_core.identity.erase_cli organization <org-id> --yes --json
```

See [Operations](./OPERATIONS.md) and [Data lifecycle](./DATA-LIFECYCLE.md). Folding this into the
durable erasure API and product UI remains work.

## Current design gaps

- Persisted Agent configuration contains kind, owner, name, persona, state, and version, but not the
  full model/tool/resource/memory/budget/autonomy contract.
- There is no first-class Routine entity.
- No admin-facing UI exists for Agent Access grants or session visibility/shares — API-only.
- Session visibility does not give an org admin/owner a blanket "support access" override; private
  session content is private even from admins in this PR (a documented, deliberate scope boundary).
- The API can create several organizations; the initial production profile does not yet enforce its
  one-active-organization policy.
- Browser login and membership/grant/Agent-Access/session-visibility administration UI are not
  complete product journeys.
