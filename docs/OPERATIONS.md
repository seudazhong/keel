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
- Configure `KEEL_API_KEYS` before exposing the API. Current keys are plaintext config
  entries, not hashed/unscoped production credentials.
- `deploy/config/` is reserved for mounted non-secret configuration; current services are
  primarily environment-configured.

## Current production-readiness limits

- The application DB role owns the schema and can bypass RLS; RLS is not a hard tenant
  boundary yet.
- There is no real sandbox service: shell execution occurs in the server/CLI process.
- Interactive runs and some approval state are process-local; server restarts can interrupt
  them.
- OAuth state is process-local; OneBot and Telegram webhooks are unauthenticated.
- Outbound idempotency is process-local, and permissive policy construction is possible if
  a caller omits an explicit default.
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
