# Identity, Organizations & Persisted Agents (M3.6)

Keel's durable identity foundation: **users**, **organizations** (tenants),
**memberships** (RBAC), persisted **Agents** (personal/team), and explicit **resource
grants**. It adds a real actor/authorization model under the existing coarse API-key role
tiers, without changing the single-operator Chat/runtime path (see *Limitations* below).

Code: `keel_core.identity` (`models`, `oidc`, `authz`, `store`, `service`, `audit`,
`purge`); request context `keel_server.identity_context`; REST API
`keel_server.api.identity`. Schema: migration `0013_identity_agents_grants`.

## Model

* **User** — a global human identity (`usr_…`). A user may belong to many orgs, so the
  table is not org-partitioned. Soft-deletable.
* **OIDC identity** — a global `(issuer, subject) -> user` link (`oid_…`) used for
  provisioning / account linking.
* **Organization** — the tenant root (`org_…`), a global lookup table, archivable.
* **Membership** — the user↔org RBAC edge (`mem_…`) with a role: `owner`, `admin`,
  `member`, `viewer`. Tenant-owned (carries `org_id`).
* **Agent** — a persisted `personal` or `team` Agent (`agt_…`) owned by a user in an org,
  with an optimistic-concurrency `version`. Tenant-owned.
* **Resource grant** — an explicit `(org, agent, resource_type, resource_id, capability)`
  binding (`grt_…`) with a grantor and lifecycle. Tenant-owned; a composite FK to
  `agents(id, org_id)` makes a cross-org grant structurally impossible.

## Authentication & actors

Three actor kinds are derived per request (no global mutable state), in
`keel_server.identity_context`:

* **user** — a human presenting a verified OIDC **bearer JWT**. The token is validated by
  `keel_core.identity.oidc.OIDCVerifier` (issuer + audience, signature against the issuer's
  JWKS with rotation-aware caching, and `exp`/`nbf`/`iat` with bounded leeway). The
  algorithm allowlist is a **hard-coded asymmetric set** (`RS*`/`ES*`/`PS*`) enforced at
  construction — `HS*`/`none`/octet keys are rejected regardless of configuration
  (alg-confusion defense). A multi-audience token must carry an `azp` equal to the
  configured client id, an unknown `kid` triggers at most one coalesced, rate-limited JWKS
  refresh (then a bounded negative cache), and a provider outage fails closed with `503`
  (never an uncontrolled `500`) while an invalid token is `401`. The subject is resolved to
  a durable user by its `(issuer, subject)` link. A bearer credential that is not a valid
  JWT is retried as a configured API key, so a valid dotted API key still works, but an
  invalid JWT is never laundered into an API-key/open-mode bypass.
* **machine** — an API-key caller (the existing hashed-key path, unchanged). Has a role
  tier but no durable identity; identity APIs require a user (see below).
* **local** — the open-mode single operator. Mapped to a durable *local operator* user so
  identity reads/writes bind to a real user, never the ambient `web:local` scope.

**Provisioning policy.** An OIDC subject resolves to a user by its link. With
`KEEL_IDENTITY_ALLOW_JIT_PROVISIONING=1` a first-seen subject is provisioned just-in-time;
otherwise (default, fail closed) an unlinked subject is rejected and must be linked
explicitly. A verified subject alone grants nothing — the caller must additionally select
an org it is an active member of via the `X-Keel-Org` header. Selecting an org the user
does not belong to returns the same `404` as an unknown org (no cross-org disclosure /
spoofing).

## Authorization

`keel_core.identity.authz.AuthorizationService` composes three inputs and fails closed:

1. **Membership role → org capabilities**: `viewer={read}`, `member={read,use}`,
   `admin=owner={read,use,write,manage}`.
2. **Agent ownership + kind**: a *personal* Agent is private to its owner (org admins/owners
   may view/manage it but not silently *use* it); a *team* Agent follows org membership.
3. **Explicit grants**: an Agent's capabilities on a specific resource.

An Agent acting on a resource never exceeds **both** the acting user's org capabilities and
the Agent's granted capabilities — the effective set is their **intersection** (a
confused-deputy / privilege-escalation defense). Membership administration enforces
**last-owner protection** (an org must retain ≥1 active owner) and restricts owner
assignment to owners. Owner-affecting membership changes, member management, and grant
create/revoke are made **atomic with actor re-validation** in the durable store (a stable
organization-row lock for last-owner, a `FOR SHARE` membership-row lock for actor role
revalidation), so two concurrent demotions cannot leave an org ownerless and a demoted
admin cannot commit a membership change or grant after losing authority. This is
deliberately distinct from the coarse `role >= minimum` endpoint tiers in
`keel_server.auth`; `keel_core.scope.DefaultScopeGuard` is unchanged and still guards the
event-core scope checks.

## Row-Level Security

The tenant-owned tables (`memberships`, `agents`, `resource_grants`) enforce Postgres RLS
keyed by the `app.org_id` GUC, with `FORCE ROW LEVEL SECURITY` and grants to the non-owner
`keel_runtime` role (created in `0011`), consistent with the M3.3/M3.5 safety migrations.
`memberships` additionally layers a **SELECT-only** data-subject self-read keyed by
`app.user_id` (so a user can list its own orgs before selecting one); that self-read never
applies to `UPDATE`/`DELETE`, so a caller holding only `app.user_id` cannot mutate a
cross-org membership — mutations always require the correct operating `app.org_id`.
Repositories set these GUCs on every access.

## REST API (`/v1/identity`)

`GET /me`, `GET|POST /organizations`, `GET /organizations/current`,
`GET|POST /members` + `PATCH|DELETE /members/{user_id}`,
`GET|POST /agents` + `GET|PATCH /agents/{id}`, `POST /agents/{id}/archive`,
`POST /agents/{id}/select` (the compatibility bridge that resolves + authorizes the
persisted Agent for a future run), and `GET|POST /grants` + `DELETE /grants/{id}`.
Org-scoped routes require the `X-Keel-Org` header. All are additive on the frozen `/v1`
OpenAPI surface and mirrored in the typed SDK (`keel_sdk`).

Membership, Agent, and grant changes emit non-sensitive audit records
(`keel_core.identity.audit`) — ids/roles/capabilities only, never a credential, raw JWT,
API key, or an Agent's persona/instruction text.

## Configuration

| Env var | Default | Meaning |
| --- | --- | --- |
| `KEEL_OIDC_ENABLED` | `false` | Enable OIDC bearer-JWT verification. |
| `KEEL_OIDC_ISSUER` | — | Expected `iss`. |
| `KEEL_OIDC_AUDIENCE` | — | Allowed `aud` (comma-separated). |
| `KEEL_OIDC_JWKS_URI` | — | Issuer JWKS (`https://`), fetched + cached. |
| `KEEL_OIDC_CLIENT_ID` | single `aud` | Expected `azp` for multi-audience tokens. |
| `KEEL_OIDC_ALGORITHMS` | safe asymmetric set | Restrict within the hard-coded `RS*`/`ES*`/`PS*` allowlist (`HS*`/`none` always rejected). |
| `KEEL_OIDC_LEEWAY_SECONDS` | `60` | Clock skew tolerance for `exp`/`nbf`/`iat`. |
| `KEEL_OIDC_JWKS_CACHE_TTL_SECONDS` | `3600` | JWKS cache TTL (rotation refresh on unknown `kid`). |
| `KEEL_OIDC_JWKS_MIN_REFRESH_INTERVAL_SECONDS` | `60` | Min interval between unknown-`kid` JWKS refreshes (anti-amplification). |
| `KEEL_IDENTITY_ALLOW_JIT_PROVISIONING` | `false` | JIT-provision a first-seen verified subject. |

With OIDC disabled, only the API-key and local-operator actor paths are available; outside
cloud mode the local operator can still use the identity APIs.

## Limitations (honest status)

* **Fixed runtime scope.** The Chat/runtime and existing `/v1` session routes still operate
  on the single durable scope `web:local`; they are **not** yet multi-user. The identity
  APIs are additive and do not change that path. `POST /v1/identity/agents/{id}/select` is a
  forward-compatible bridge that resolves + authorizes the selected persisted Agent, ready
  for durable-run integration — it does not yet drive a run.
* **Org ≠ scope until durable runs.** Identity is org-partitioned; the runtime is
  scope-partitioned. Organization erasure is therefore a standalone primitive
  (`keel_core.identity.purge`), not yet folded into the scope erasure coordinator (see
  `docs/DATA-LIFECYCLE.md`).
* **Machine (API-key) callers have no durable identity** and cannot use the user-scoped
  identity APIs; configure OIDC for human users.
