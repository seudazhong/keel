# Identity, organizations, and persisted Agents

> **Status:** Living subsystem reference
> **Product gap:** browser OIDC authorization-code flow and complete administration UI

Keel has a durable identity and authorization foundation for users, organizations, memberships,
personal/team Agents, and explicit resource grants.

## Model

| Entity | Purpose |
|---|---|
| User | Global human identity. |
| OIDC identity | `(issuer, subject)` link to a User. |
| Organization | Tenant and administration boundary. |
| Membership | User-to-organization role: owner, admin, member, or viewer. |
| Agent | Persisted personal or team execution identity. |
| Resource grant | Agent capability on one resource inside an organization. |

Personal Agents are private to their owner. Current team Agents follow organization membership plus
resource grants. The target adds a separate Agent Access edge for user/channel discovery, use, and
management, plus explicit Session visibility.

## Request actors

Each request resolves one actor without global mutable state:

- **user** — verified asymmetric OIDC bearer JWT mapped to a durable User;
- **machine** — hashed/scoped API key;
- **local** — explicit non-cloud preview operator.

A verified OIDC subject grants no organization authority by itself. The user must be an active
member of the selected organization.

## Per-Agent runtime routing

Interactive admission derives the canonical data scope from organization and Agent:

```text
agent:<organization-id>/<agent-id>
```

`web:local` remains only the non-cloud local-preview compatibility scope. Authenticated callers
cannot obtain authority by naming a scope directly.

## Authorization

Organization roles provide coarse capabilities:

| Role | Capabilities |
|---|---|
| viewer | read |
| member | read, use |
| admin | read, use, write, manage |
| owner | read, use, write, manage |

An Agent operating on a resource receives the intersection of the actor's organization capability
and the Agent's explicit resource grant. Personal-Agent ownership and last-owner protection add
further constraints.

Membership mutations lock the organization and affected memberships in deterministic order so
concurrent owner changes cannot leave an organization ownerless.

## Row-level security

Tenant-owned identity tables carry `org_id`, use `FORCE ROW LEVEL SECURITY`, and are accessed by the
non-owner runtime principal. Repositories set request-scoped PostgreSQL context before reads and
writes.

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
- grant list/create/revoke.

The React Agents page consumes the Agent APIs, but membership/grant administration remains
incomplete.

## Identity erasure

User and organization erasure are cross-tenant maintenance operations. They execute through
locked-down `SECURITY DEFINER` functions using a dedicated maintenance login with function execute
permission but no direct table delete or `BYPASSRLS`.

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
- Organization membership currently acts as team-Agent access, and Agent scope currently carries
  too much session-visibility meaning.
- There is no first-class Routine entity.
- The API can create several organizations; the initial production profile does not yet enforce its
  one-active-organization policy.
- Browser login and membership/grant administration are not complete product journeys.
