# Operating Keel

Keel currently ships a **trusted preview** deployment, not a production reference deployment.

## 1. Supported current profile

The standard Compose `dev`/`full` profiles currently select effectively the same implemented
services:

- PostgreSQL/pgvector;
- Redis;
- one-shot migration, runtime-secret, runtime-role provision, and sandbox-secret services;
- `keel-server`;
- `keel-worker`;
- `keel-sandbox`;
- `keel-web`;
- optional/local Ollama service.

The profile assumes one trusted operator. Do not expose open local-preview mode to an untrusted
network.

## 2. Start, inspect, and stop

```powershell
Set-Location C:\src\keel
Copy-Item .env.example .env
# Configure a chat-capable model/provider.
docker compose -f docker-compose.yml --profile dev up -d --build
docker compose -f docker-compose.yml --profile dev ps
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/readiness
docker compose -f docker-compose.yml logs --tail 100 keel-server keel-worker
```

Expected readiness includes:

```text
postgres = ok
runtime_db_principal = least-privilege (keel_runtime_login)
redis = ok
run_substrate = shared-postgres
run_queue = ok
run_admission = ready
knowledge_dispatch = ready
sandbox = ok
```

Stop without deleting data:

```powershell
docker compose -f docker-compose.yml --profile dev down
```

Do not add `-v` unless permanent deletion of database, model, project, sandbox, and generated-secret
volumes is intended.

Pass `-f docker-compose.yml` so a local ignored override cannot silently change the documented
trust contract.

## 3. Startup and database principals

Startup orders:

```text
keel-migrate
  -> keel-runtime-secret-init
  -> keel-provision
  -> keel-sandbox
  -> keel-server / keel-worker / keel-web
```

Migrations use the privileged owner/migrator connection. Server and worker use
`keel_runtime_login`, a non-owner, non-`BYPASSRLS` login that is a member only of the runtime role.
Compose generates its password into a dedicated volume and exposes it through a `0600` pgpass file,
not a URL, argv, or normal environment variable.

Server readiness and worker startup fail closed when the required runtime principal is a
superuser, table owner, or can bypass RLS.

For non-Compose deployments:

- `KEEL_MIGRATION_DATABASE_URL` — schema owner/migrator;
- `KEEL_DATABASE_URL` — least-privilege runtime login;
- `KEEL_REQUIRE_RUNTIME_DB_PRINCIPAL=true` or cloud mode — enforce the check.

## 4. Authentication modes

### Local preview

Outside cloud mode, an unauthenticated local request may use the explicit local-preview actor. This
is for trusted development only.

### Machine API keys

Configure `KEEL_API_KEYS` and use scoped/hashed API keys. In cloud mode an empty or malformed key
configuration fails closed.

### OIDC

The backend verifies OIDC bearer JWTs when issuer, audience, JWKS URI, and safe asymmetric
algorithms are configured.

Keel does not yet provide a built-in browser authorization-code callback/session flow. The current
React credential entry is preview compatibility, not the production login design.

## 5. Sandbox boundary

`keel-server` and `keel-worker` use `KEEL_EXECUTION_BACKEND=sandbox` and actively probe the signed
RPC endpoint.

The Compose sandbox:

- runs non-root with read-only rootfs, dropped capabilities, and `no-new-privileges`;
- has only the RPC secret;
- has no database, Redis, provider, GitHub, or connector credential;
- has no public egress;
- stores files in opaque per-scope namespaces;
- denies shell.

This is a trusted-preview rootless-OCI floor, not a gVisor/Kata/microVM boundary. The internal
Compose network is bidirectional, and directory namespaces are not sufficient to confine an
arbitrary command. Do not enable shell by setting optimistic environment flags.

## 6. Project storage, review, and patches

Set `KEEL_PROJECT_STORAGE_ROOT` to storage mounted identically in server and worker.

- Local Compose uses a named volume.
- Multi-host deployments require shared RWX storage.
- A review-enabled worker that cannot reach shared storage must fail startup.
- Patch workers share the same root and sandbox transfer service.

Relevant flags:

- `KEEL_REVIEW_ENABLED`
- `KEEL_PATCH_ENABLED`
- review model/budget/price settings
- GitHub App settings and secret references

Patch has no API/UI today. Disable patch capability on workers that should not participate.

## 7. Connections and secrets

Start from `.env.example`. Keep provider keys, connector client secrets/tokens, GitHub private keys,
API keys, and envelope keys out of source and images.

Connector credentials are encrypted and versioned. Secret references may point to environment or
mounted files where supported. Keep historical envelope-key ids available until rotation and
re-encryption finish.

Provider webhooks use provider-specific authentication plus durable replay protection. Cloud
deployments must use routed connector webhook URLs so the delivery resolves to one Agent scope.

### Effect ledger and reconciliation (R1B)

Every outbound connector mutation with an idempotency key is reserved/executed/confirmed through
the durable Effect ledger (`effects` + the global `effect_reconciliation_outbox` pointer, migration
`0026_effect_ledger`). The worker cron `reconcile_effects_tick` (registered unconditionally at
worker startup, alongside a best-effort immediate pass so a restart recovers stranded execution
leases without waiting for the next tick) reaps expired execution leases to `unknown` and drives
provider reconciliation for `unknown` Effects.

Gmail search is eventually consistent and Gmail send is not provider-idempotent. Its reconciler may
confirm a matching deterministic `Message-ID`, but a not-found search never proves permanent
absence and therefore never unlocks an automatic retry. Such an Effect remains `unknown`.

An `unknown` Effect **never** auto-retries. Operators can inspect it via `GET /v1/effects` /
`GET /v1/effects/{id}`, request one on-demand reconciliation attempt via
`POST /v1/effects/{id}/reconcile` (requires the connected provider to implement
`ConnectorProvider.build_reconciler` — today Gmail send and Google Calendar create/update only;
every other provider reports `503` and the Effect stays `unknown`), and check retry eligibility via
`POST /v1/effects/{id}/retry` (the API never re-executes the mutation itself — an eligible Effect
is retried by invoking the owning tool again with the same idempotency key). Treat a persistently
`unknown` Effect from a provider with no reconciler as a manual investigation: check the provider's
own audit log/sent-mail folder/calendar for the mutation before assuming either outcome.

## 8. Data lifecycle

Scope/session/project erasure runs as durable background work. User/organization erasure currently
uses the dedicated maintenance CLI and database principal described in
[Identity](./IDENTITY.md).

The lifecycle data map classifies every migration-created application table, including global
dispatch indices and patch stores that are removed by foreign-key cascade. CI compares migration
DDL with the map, so a new table cannot merge without an explicit retention and erasure decision.

Never run destructive tests against the normal `keel` database. Use dedicated `keel_test` and
`keel_eval` databases.

## 9. Backup and recovery

Current preview safeguards:

- preserve PostgreSQL volumes;
- preserve shared project storage;
- keep deployment secret material available;
- back up before migrations when data matters.

Production is blocked on:

- documented and automated database/project/artifact backup;
- restore verification;
- declared RPO/RTO;
- upgrade and rollback drills;
- erasure-aware backup retention.

## 10. Kubernetes scaffold

[`deploy/k8s`](../deploy/k8s/README.md) is a hardened scaffold and validation suite. It is not a
production-ready Keel installation:

- it does not deploy the live sandbox service/controller used by Compose;
- the separate scheduler is not available;
- capability-worker, telemetry, backup/restore, and hostile-tenant gates remain open.

## 11. Production release checklist

Do not claim the single-organization production profile until:

- browser OIDC and explicit administration replace preview auth;
- all runtime paths require explicit permission policy;
- accepted Routine occurrences and ambiguous effects recover correctly;
- capability-specific workers and a separate scheduler are deployed;
- every enabled command workload uses qualified per-run isolation;
- traces, metrics, SLOs, alerts, cost reconciliation, backup/restore, and DR are proven;
- two-user/private-team adversarial isolation suites pass.

## 12. Troubleshooting

```powershell
docker compose -f docker-compose.yml --profile dev ps
docker compose -f docker-compose.yml logs --tail 200 keel-migrate keel-provision keel-server keel-worker keel-sandbox
Invoke-WebRequest http://localhost:8000/openapi.json
```

Common diagnoses:

- healthy `/health` but degraded `/readiness`: inspect the named readiness check;
- new routes missing: rebuild the images;
- review/project storage degraded: verify the shared mount and write access;
- sandbox degraded: verify service health and generated RPC-secret volume;
- queued work not progressing: inspect worker health, Redis, Postgres outbox/job rows, and worker
  capability flags.
