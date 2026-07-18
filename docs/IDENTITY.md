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
  (never an uncontrolled `500`) while an invalid token is `401`. Concurrent cold-cache
  fetches are coalesced onto a single in-flight request **including failures**, a bounded
  failure cooldown (negative provider cache) suppresses retry amplification and recovers
  automatically, and a fetched keyset is only cached once it contains a usable supported
  asymmetric signing key — an empty / malformed / only-`oct` / unsupported keyset is a
  provider-availability `503`, not an invalid-token `401`. The subject is resolved to
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
assignment to owners. Membership mutations take locks in a single deterministic order —
the **organization row first** (`FOR UPDATE`, which serializes every owner-affecting change
on that org), then the actor and target membership rows in sorted order — so concurrent
cross-owner demotions/removals can neither deadlock nor both proceed. Crucially, whether
last-owner protection and the owner-actor requirement apply is derived **exclusively from
the target's current (locked) row and an owner recount inside the same transaction**, never
from a possibly-stale service-level read: a stale admin operation can no longer remove a
user who has since become the final owner. The grant create/revoke paths keep their single
`FOR SHARE` membership-row revalidation (they take no org lock and only one membership lock,
so they cannot form a cycle with the org-first membership mutations), so a demoted admin
still cannot commit a grant after losing authority. This is
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

**Erasure under production RLS.** User (data-subject) erasure is inherently *cross-tenant*
(a user belongs to many orgs) and must delete the global `users` row, so it cannot run as a
normal `keel_runtime` request: under `FORCE ROW LEVEL SECURITY` a cross-org enumeration
returns nothing, which would silently skip the sole-owner block/archive guard and cascade an
active org into an ownerless state. Erasure therefore runs through the `keel_erase_user` /
`keel_erase_organization` **`SECURITY DEFINER`** functions installed by migration `0013`
(locked-down `search_path`). Both functions take an **org-first, deterministic (ascending id)
lock order** — identical to the normal membership mutations — so a cross-tenant erasure can
neither deadlock with, nor race the owner-count invariant of, a concurrent invite / promotion
/ demotion / removal; the block/archive decision is made under those org locks.

The erasure privilege is deliberately split across two roles (least privilege):

* **`keel_maintenance` (definer)** — `NOLOGIN` + `BYPASSRLS`. Owns the functions and holds the
  table DML they run with. Because the functions execute with the definer's rights, this is
  the only role that ever touches identity tables across tenants. **Nothing logs in as it and
  no operator/app login is granted membership in it** (that would hand a login direct
  cross-tenant DML + RLS bypass).
* **`keel_maintenance_exec` (executor)** — `NOLOGIN` + `NOBYPASSRLS`, granted **only** EXECUTE
  on the two functions: no table privileges and not a member of the definer, so it can neither
  read/delete identity tables directly nor `SET ROLE` into `keel_maintenance`. A least-
  privilege maintenance **login** is made a member of *only this* role.

`DELETE` on the three global identity tables (`users`, `oidc_identities`, `organizations`) is
**revoked** from `keel_runtime`, and `EXECUTE` on the functions is revoked from `PUBLIC` (and
`keel_runtime`). On managed Postgres that forbids `CREATE ROLE`/`GRANT` the migration skips
role setup with a `NOTICE` and erasure **fails closed** until an operator provisions both
roles + the login manually (see [`docs/OPERATIONS.md`](OPERATIONS.md)). The migration downgrade
drops the functions (fail-closed) and restores the prior `keel_runtime` grants.

The operator entrypoint is `keel_core.identity.purge.IdentityPurgeRepository`, built by
`create_identity_purge_repository(settings)`, which connects on the dedicated
`KEEL_MAINTENANCE_DATABASE_URL` (fail-closed when unset; in cloud mode it must not equal the
runtime URL) and verifies the connected principal has function EXECUTE but **lacks** direct
identity `DELETE` and `BYPASSRLS`. The production-usable command is
`python -m keel_core.identity.erase_cli {user|organization} <id>` (always previews inside a
rolled-back transaction first, requires `--yes` or an interactive id confirmation, emits a
structured result, and reports a blocked owner explicitly).

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
| `KEEL_OIDC_JWKS_FAILURE_COOLDOWN_SECONDS` | `30` | Cooldown after a failed/degenerate JWKS fetch before a retry (negative provider cache). |
| `KEEL_IDENTITY_ALLOW_JIT_PROVISIONING` | `false` | JIT-provision a first-seen verified subject. |
| `KEEL_LEGACY_MACHINE_ORG_ID` | — | Migration only: default org (id or slug) a bare `key:role` credential binds to in cloud mode. Must be set together with `KEEL_LEGACY_MACHINE_AGENT_ID`. |
| `KEEL_LEGACY_MACHINE_AGENT_ID` | — | Migration only: default Agent id paired with `KEEL_LEGACY_MACHINE_ORG_ID`. |
| `KEEL_MAINTENANCE_DATABASE_URL` | — | Dedicated least-privilege maintenance login (member of only `keel_maintenance_exec`) for identity erasure. Fails closed when unset; in cloud mode must differ from `KEEL_DATABASE_URL`. |

### Migrating legacy `key:role` API keys

Before the identity model, API keys were bare `key:role` pairs with no tenant binding. In cloud
mode (`KEEL_CLOUD_MODE=1`) such an unbound credential now **fails closed** (403) because honoring
it would grant an ambient, cross-tenant scope. Two supported paths keep existing deployments
working:

1. **Preferred — scope each key.** Rewrite each entry to `key:role:org=<id-or-slug>:agent=<id>`
   (or `key:role:global` for an explicit cross-tenant admin that selects org/Agent per request).
   The credential then derives its own per-Agent data plane with no ambient access.
2. **Transitional — a default mapping.** Set **both** `KEEL_LEGACY_MACHINE_ORG_ID` and
   `KEEL_LEGACY_MACHINE_AGENT_ID` to a real org+Agent. Every remaining bare `key:role` credential
   binds to exactly that one tenant (never an ambient scope). A client-supplied
   `X-Keel-Org`/`X-Keel-Agent` that selects a *different* tenant is rejected as a spoof, and each
   use is audited. Setting only one of the pair is a hard startup misconfiguration. Remove the
   mapping once every key has been migrated to the scoped form.

With OIDC disabled, only the API-key and local-operator actor paths are available; outside
cloud mode the local operator can still use the identity APIs.

## Limitations (honest status)

* **Fixed runtime scope.** The Chat/runtime and existing `/v1` session routes still operate
  on the single durable scope `web:local`; they are **not** yet multi-user. The identity
  APIs are additive and do not change that path. `POST /v1/identity/agents/{id}/select` is a
  forward-compatible bridge that resolves + authorizes the selected persisted Agent, ready
  for durable-run integration — it does not yet drive a run.
* **Org ≠ scope until durable runs.** Identity is org-partitioned; the runtime is
  scope-partitioned. User (data-subject) and organization erasure are therefore standalone
  maintenance primitives (`keel_core.identity.purge` + the `erase_cli` operator command run on
  the dedicated maintenance login), **not** part of the `/v1/erasure` scope lifecycle API — that
  API erases a scope / session / project and does **not** erase users or organizations. Folding
  identity erasure into the durable lifecycle API is future work (see `docs/DATA-LIFECYCLE.md`).
* **Machine (API-key) callers have no durable identity** and cannot use the user-scoped
  identity APIs; configure OIDC for human users.
