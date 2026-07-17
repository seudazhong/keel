# Operating Keel

Keel's Compose setup is a development deployment, not a production reference architecture.
It runs one hard-coded scope, exposes local ports, and defaults to unauthenticated implicit
admin when `KEEL_API_KEYS` is empty.

For a Kubernetes-based deployment scaffold (hardened manifests, storage/backup/DR, upgrade/
rollback, and clean-install runbooks), see [`deploy/k8s`](../deploy/k8s/README.md). It is a
scaffold toward the M3.8 production-delivery gates below, not evidence those gates are closed.

## Lifecycle and probes

```powershell
docker compose --profile dev up -d --build
docker compose --profile dev ps
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/readiness
docker compose logs --tail 100 keel-server keel-worker
docker compose --profile dev down
```

Do not add `-v` to `down` unless permanent deletion of Postgres and Ollama volumes is
intended.

`keel-migrate` runs `alembic upgrade head` before server/worker startup. `keel-server`,
`keel-worker`, Postgres, Redis, Ollama, and the nginx-served React web app are in both current
`dev` and `full` profiles; documented full observability/object-store/sandbox services are not
implemented in Compose.

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
- The authenticated `keel-sandbox` service boundary exists and server/worker wiring fails
  closed when it is unavailable or unauthenticated, but Compose does not deploy it yet.
  CLI shell is available only for a workspace validated as free of `.git`, `.env`, links,
  and nested mounts.
- Interactive runs and some approval state are process-local; server restarts can interrupt
  them.
- OAuth CSRF state, IM webhook authentication/replay protection, and outbound-connector
  idempotency are now durable and verified (M3.3); the fixed `web:local` scope still limits
  true multi-tenancy.
- The scope is fixed to `web:local`; there are no users, organizations, or persisted Agents.
- The scheduler package is not a separate elected service; worker cron performs scheduling.
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
