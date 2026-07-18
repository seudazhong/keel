# Operating Keel

Keel's Compose setup is a development deployment, not a production reference architecture.
It runs one hard-coded scope, exposes local ports, and defaults to unauthenticated implicit
admin when `KEEL_API_KEYS` is empty.

For a Kubernetes-based deployment scaffold (hardened manifests, storage/backup/DR, upgrade/
rollback, and clean-install runbooks), see [`deploy/k8s`](../deploy/k8s/README.md). It is a
scaffold toward the M3.8 production-delivery gates below, not evidence those gates are closed.

## Lifecycle and probes

```powershell
docker compose -f docker-compose.yml --profile dev up -d --build
docker compose --profile dev ps
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/readiness
docker compose logs --tail 100 keel-server keel-worker
docker compose --profile dev down
```

Pass `-f docker-compose.yml` so a local `docker-compose.override.yml` cannot silently
change the startup contract. Do not add `-v` to `down` unless permanent deletion of
Postgres and Ollama volumes is intended.

The `dev`/`full` profiles run a **real, authenticated sandbox execution boundary** — not the
old in-process preview. A one-shot `keel-secret-init` generates a random ≥32-byte RPC shared
secret into a dedicated `sandboxsecret` volume (idempotent; never committed to source or the
YAML, never printed to logs/argv), and every service loads it from a read-only file via
`KEEL_SANDBOX_RPC_SECRET_FILE`. `keel-server`/`keel-worker` use the fail-closed
`KEEL_EXECUTION_BACKEND=sandbox`: every model-chosen file/shell tool call is sent over an
**internal-only** RPC network to the dedicated, hardened `keel-sandbox` executor, so the
control plane runs no tool operation in process and mounts no sandbox workspace. Wiring fails
**closed** — server readiness and worker startup actively probe the sandbox's authenticated
`/v1/ping` and refuse to serve/claim rather than silently falling back to local execution. To
rotate the secret, `docker compose down` then delete the `sandboxsecret` volume.

The `keel-sandbox` container is the accepted rootless-OCI floor: non-root (uid 10100),
read-only rootfs, all Linux capabilities dropped, `no-new-privileges`, tmpfs scratch, on an
`internal: true` network with **no** public egress, holding **only** the read-only RPC secret
(no Docker socket, no project storage, no DB/Redis/provider/GitHub credentials). File tools are
isolated per scope by an opaque `ws_<hash>` namespace. **Shell/command execution stays
disabled**: namespaced shell is denied (a namespace dir is not an OS mount boundary —
`KEEL_SANDBOX_NAMESPACE_SHELL_ISOLATED` is never set) and unscoped shell is denied too because
the default workspace and the namespaces coexist in one container, so no shell can be proven
confined to a single workspace (`KEEL_SANDBOX_WORKSPACE_SANITIZED` is left unset). No fake
isolation flag is set anywhere. **Honest downgrades vs. `deploy/k8s`:** this is not a
gVisor/Kata/microVM boundary (a kernel container escape is out of scope here — that gate is
`runtimeClassName`), and Compose networks are bidirectional so it cannot express the
per-direction NetworkPolicy K8s does (the sandbox still cannot reach Postgres/Redis/Ollama or
the internet and holds no credentials). K8s/cloud keep the same fail-closed `sandbox` backend;
never relax these settings toward a hostile multi-tenant workload.

`keel-migrate` runs `alembic upgrade head` before server/worker startup. `keel-secret-init`,
`keel-sandbox`, `keel-server`, `keel-worker`, Postgres, Redis, Ollama, and the nginx-served
React web app are in both current `dev` and `full` profiles; documented full
observability/object-store services are not implemented in Compose.

## Configuration and secrets

- Start from `.env.example`; environment variables override defaults.
- Compose injects Postgres/Redis service URLs.
- Keep provider keys, Gmail OAuth files, connector encryption keys, and API keys out of
  source control and images.
- Configure `KEEL_API_KEYS` before exposing the API. Keys are stored as SHA-256 digests
  and verified in constant time (`hmac.compare_digest`); plaintext keys are never held or
  compared. Set `KEEL_CLOUD_MODE=1` for any exposed deployment so the server fails **closed**
  — an empty `KEEL_API_KEYS` then rejects every request instead of falling back to the local
  implicit-admin open mode.
- `deploy/config/` is reserved for mounted non-secret configuration; current services are
  primarily environment-configured.

### Identity & OIDC (M3.6)

- **Human users** authenticate with an OIDC bearer JWT. Enable with `KEEL_OIDC_ENABLED=1`
  and set `KEEL_OIDC_ISSUER`, `KEEL_OIDC_AUDIENCE` (comma-separated), and
  `KEEL_OIDC_JWKS_URI` (`https://`). Tokens are verified for issuer/audience, JWKS
  signature (rotation-aware cache), and `exp`/`nbf`/`iat` with `KEEL_OIDC_LEEWAY_SECONDS`
  skew; only asymmetric algorithms are accepted. A verified user must select an org it
  belongs to via the `X-Keel-Org` header. See [`docs/IDENTITY.md`](IDENTITY.md).
- `KEEL_IDENTITY_ALLOW_JIT_PROVISIONING=1` provisions a first-seen verified subject a
  durable user; default off (an unlinked subject is rejected). With OIDC disabled only the
  API-key and local-operator actor paths are available.
- Migration `0013` adds `FORCE ROW LEVEL SECURITY` + `keel_runtime` grants on the
  tenant-owned identity tables (`memberships`, `agents`, `resource_grants`), keyed by the
  `app.org_id` GUC — connect as a `keel_runtime` member to make RLS a hard boundary.

### Managed projects & GitHub App (M3.7)

- Managed projects and GitHub synchronization are enabled by configuring the GitHub App:
  `KEEL_GITHUB_APP_ID`, `KEEL_GITHUB_PRIVATE_KEY_REF` (a **reference** — `env:NAME`,
  `file:PATH`, or a path; never the PEM inline), `KEEL_GITHUB_WEBHOOK_SECRET`,
  `KEEL_GITHUB_API_BASE_URL`, `KEEL_GITHUB_WEB_BASE_URL`, `KEEL_GITHUB_ALLOWED_HOSTS`
  (comma-separated clone/API host allowlist), and `KEEL_GITHUB_TOKEN_CACHE_SECONDS`. With
  `KEEL_GITHUB_APP_ID` unset the feature is disabled (blank/local projects still work).
- The webhook endpoint `POST /v1/projects/github/webhook` is authenticated **independently**
  by an `X-Hub-Signature-256` HMAC over the raw body plus the installation→org binding; it
  enforces delivery-id replay protection, an event allowlist, and SSRF/URL allowlist checks.
  Installation tokens are minted just in time, briefly cached in-process, and never persisted
  or logged. See [`docs/PROJECTS.md`](PROJECTS.md).
- Migration `0015` adds `FORCE ROW LEVEL SECURITY` + `keel_runtime` grants on the tenant-owned
  project tables (`projects`, `project_worktrees`, `project_runs`, `repo_sync_ledger`,
  `project_quotas`, `github_repositories`, `github_sync_state`), keyed by `app.org_id`, and
  extends `keel_erase_organization` to purge them with the org.


### Identity erasure (user / organization) — maintenance path (M3.6)

User (data-subject) and organization erasure are **privileged, cross-tenant maintenance
operations** and are **not** part of the `/v1/erasure` scope lifecycle API (that API erases a
scope / session / project only — it does not erase users or orgs). They run through the
`keel_erase_user` / `keel_erase_organization` `SECURITY DEFINER` functions installed by
migration `0013`, which enforce the sole-owner block/archive invariant under an org-first lock
order. Migration `0013` provisions **two** roles (least privilege):

- `keel_maintenance` — **definer**, `NOLOGIN` + `BYPASSRLS`, owns the functions and their table
  DML. Never log in as it and never grant a login membership in it.
- `keel_maintenance_exec` — **executor**, `NOLOGIN` + `NOBYPASSRLS`, granted **only** EXECUTE on
  the two functions (no table DML, not a member of the definer).

Provision a dedicated **maintenance login** that is a member of **only** `keel_maintenance_exec`
and point `KEEL_MAINTENANCE_DATABASE_URL` at it (it must differ from `KEEL_DATABASE_URL`;
erasure fails closed when it is unset, and in cloud mode rejects a copy of the runtime URL):

```sql
CREATE ROLE keel_erase LOGIN PASSWORD '…' NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB;
GRANT keel_maintenance_exec TO keel_erase;   -- executor membership only
```

On **managed Postgres** that forbids `CREATE ROLE`/`ALTER OWNER`/`GRANT`, the migration skips
role setup with a `NOTICE` and erasure **fails closed**. Provision it manually with an
administrative role, in this order: create `keel_maintenance` (BYPASSRLS) and grant it
`SELECT, INSERT, UPDATE, DELETE` on `users, oidc_identities, organizations, memberships,
agents, resource_grants`; `ALTER FUNCTION keel_erase_organization(text) OWNER TO
keel_maintenance` and the same for `keel_erase_user(text)`; create `keel_maintenance_exec`
(NOBYPASSRLS) and `GRANT EXECUTE ON FUNCTION keel_erase_organization(text), keel_erase_user(text)
TO keel_maintenance_exec`; then create the login and grant it `keel_maintenance_exec`.

Run the operator command (destructive; it always previews inside a rolled-back transaction
first, requires `--yes` or typing the id at an interactive prompt, and never prints the URL):

```bash
# Preview only (no writes):
python -m keel_core.identity.erase_cli user   <user_id> --dry-run --json
# Erase (non-interactive):
python -m keel_core.identity.erase_cli user   <user_id> --yes --json
python -m keel_core.identity.erase_cli organization <org_id> --yes --json
```

A user who is the **sole active owner** of an active org that still has **other** active
members is reported **blocked** (exit 3) with the blocking org ids — transfer ownership first.
An org the user solely owns and is the only member of is atomically archived.

### Cloud-safety controls (M3.3)

- **Runtime DB role.** Migration `0011` provisions a non-owner, non-bypass `keel_runtime`
  role (`NOBYPASSRLS`) and `FORCE ROW LEVEL SECURITY` on the scope-bound connector token /
  outbox tables, so RLS binds even the table owner. Point the application's connection at a
  login role that is a member of `keel_runtime` (not the schema owner) to make RLS a hard
  boundary. On managed Postgres that forbids `CREATE ROLE`/`GRANT`, the migration skips role
  setup with a `NOTICE`; grant `keel_runtime` and its table privileges manually.
- **IM webhooks.** Set `KEEL_ONEBOT_SIGNING_SECRET` and `KEEL_TELEGRAM_WEBHOOK_SECRET` so
  inbound OneBot (HMAC-SHA1 body signature) and Telegram (secret header) deliveries are
  verified before dispatch and de-duplicated by a durable replay store. With `KEEL_CLOUD_MODE=1`
  a missing secret fails closed.
- **OAuth state + outbound sends.** The Gmail connect CSRF `state` and outbound-connector
  idempotency are durable (survive restart / a second worker); tune retention with
  `KEEL_OAUTH_STATE_TTL_SECONDS` and `KEEL_WEBHOOK_REPLAY_TTL_SECONDS`.
- **Key rotation.** Register versioned envelope keys with `KEEL_SECRET_KEYS=id:secret,...`
  and select the active id with `KEEL_SECRET_KEY_ACTIVE_ID`; each stored token records its
  `key_id`, so decrypt works across a rotation and `PostgresTokenStore.reencrypt_stale()`
  re-wraps rows onto the active key online. Keep every historical id listed until rotation
  completes.

## Current production-readiness limits

- The default Compose application DB role owns the schema and can bypass RLS. Migration
  `0011` adds a non-bypass `keel_runtime` role plus `FORCE ROW LEVEL SECURITY` on the
  connector token/outbox tables; connect as a `keel_runtime` member (not the owner) to make
  RLS a hard boundary. Extending `FORCE`/grants to the remaining scoped tables is pending.
- The authenticated `keel-sandbox` service boundary is now deployed by the standard Compose
  `dev`/`full` profiles: server/worker use the fail-closed `sandbox` backend against a
  hardened, internal-only executor and refuse to serve/claim if it is unreachable or
  unauthenticated. Remaining gap vs. `deploy/k8s`/production: the Compose executor is the
  rootless-OCI floor (non-root, read-only rootfs, dropped caps, `no-new-privileges`,
  default-deny egress), **not** a gVisor/Kata/microVM boundary, and Compose networks are
  bidirectional (no per-direction NetworkPolicy). Shell/command execution is therefore kept
  disabled in the standard stack (file tools remain, isolated per `ws_<hash>` scope). Treat it
  as a trusted single-org dev deployment, not a hostile multi-tenant platform.
  CLI shell is available only for a workspace validated as free of `.git`, `.env`, links,
  and nested mounts.
- Interactive runs and some approval state are process-local; server restarts can interrupt
  them.
- OAuth CSRF state, IM webhook authentication/replay protection, and outbound-connector
  idempotency are now durable and verified (M3.3); the fixed `web:local` scope still limits
  true multi-tenancy.
- The scope is fixed to `web:local`; there are no users, organizations, or persisted Agents.
  *(M3.6 adds durable users/orgs/memberships/persisted Agents/grants + OIDC and an
  identity API, but the Chat/runtime and existing `/v1` session routes still run on the
  single `web:local` scope and are not yet multi-user — the identity APIs are additive and
  `POST /v1/identity/agents/{id}/select` is only a forward-compatible bridge. See
  `docs/IDENTITY.md`.)*
- The scheduler package is not a separate elected service; worker cron performs scheduling.
- Durable interactive runs (M3.6, WS-M): the `runs`/`run_control` tables + `run_interactive`
  worker job give worker-owned, restart-safe interactive execution under a fenced lease; the
  `reconcile_runs_tick` worker cron recovers admitted-but-undispatched and expired
  running/waiting runs. The server's default web admission still uses the in-process
  `AgentRuntime` (local-preview compatibility path) until authenticated durable admission is
  flipped on; see `docs/STATUS.md`. New migration: `0014_durable_runs` (reversible).
- Event versions exist but upcasters, retention, and full erasure are not implemented.
- Observability, generated SDK/version checks, CI coverage, backup/restore, and delivery
  profiles remain below the target architecture.

Treat [Roadmap M3.3](./ROADMAP.md#m33--cloud-safety-foundation) as a prerequisite for
internet exposure or sensitive multi-user data.

## Data safety

- Use dedicated `keel_test` and `keel_eval` databases for destructive tests/evals.
- Back up Postgres before migrations or demonstrations with valuable data. A tested
  backup/restore runbook and DR drill are not yet available.
- Connector revoke, Gmail send, schedule run/toggle, approval resolution, job cancel, and
  Knowledge delete are real mutations.
- Do not co-host sensitive personal data with public/untrusted gateway traffic until the
  cloud-safety and identity gates are complete.

## Troubleshooting

```powershell
docker compose --profile dev ps
docker compose logs --tail 200 keel-migrate keel-server keel-worker
Invoke-WebRequest http://localhost:8000/openapi.json
```

If `/health` is green but newer routes return `404`, the running image is stale; rebuild
with `docker compose --profile dev up -d --build`. If semantic search is slow, inspect the
`X-Keel-Search-Mode` response header and use lexical results while embeddings catch up.
