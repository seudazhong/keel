# Data Lifecycle: Retention & Erasure (M3.5)

Keel is event-sourced and multi-scope. This document is the authoritative **data map**,
the **retention defaults**, and the operator **erasure runbook** + **recovery/verification**
procedure for the M3.5 retention & erasure subsystem (WS-K).

The code lives in `keel_core.lifecycle` (policies, data map, durable erasure ledger,
coordinator, purge repository, tombstones, Redis/coding cleanup, worker job) with the
governance API under `keel_server.api.lifecycle` and the worker job wired in
`keel_worker.lifecycle`. Schema is migration `0012_data_lifecycle`.

## 1. Data map

Every persisted store, its retention class, and how erasure treats it. This mirrors
`keel_core.lifecycle.datamap.DATA_MAP`, which
`tests/unit/test_lifecycle_datamap.py` keeps honest: every *erasable* entry must be
reached by a coordinator erasure step, and every `global_preserved`/`external` entry must
not be — so a new store cannot be added without a conscious retention + erasure decision.

| Store (table / medium) | Kind | Retention class | Scope column | Erasure treatment |
| --- | --- | --- | --- | --- |
| `sessions` | table | permanent | `scope_id` | session-scoped |
| `events` | table | permanent | `scope_id` | session-scoped (tombstoned before delete) |
| `message_embeddings` | table | permanent | `scope_id` | session-scoped (cascades from events) |
| `archival` | table | permanent | `scope_id` | scope-bound |
| `memory_blocks` / `memory_block_versions` | table | permanent | `scope_id` | scope-bound |
| `memory_proposals` | table | long (1y) | `scope_id` | scope-bound |
| `consolidation_cursors` | table | permanent | `scope_id` | scope-bound |
| `knowledge_bases` / `kb_documents` / `kb_document_versions` / `kb_chunks` / `knowledge_idempotency` | table | permanent | `scope_id` | scope-bound (FK-safe physical purge) |
| `connector_tokens` | table | permanent | `scope_id` | scope-bound (revoke + purge) |
| `connector_bindings` / `connector_binding_targets` / `connector_resources` / `connector_items` / `connector_cursors` | table | permanent | `scope_id` | typed targets, selected roots, imported-item mappings, and per-resource sync state |
| `connector_deliveries` | table | short (1d) | `scope_id` | scope-bound replay/processing ledger |
| `connector_outbox` | table | short (1d) | `scope_id` | scope-bound |
| `oauth_states` | table | transient (1h) | `scope_id` | scope-bound |
| `webhook_deliveries` | table | short (1d) | *(global)* | **global — preserved** (no personal content) |
| `schedules` | table | permanent | `scope_id` | scope-bound |
| `approvals` | table | standard (30d) | `scope_id` | scope-bound |
| `jobs` | table | standard (30d) | `scope_id` | scope-bound (running erasure job kept) |
| `runs` | table | standard (30d) | `scope_id` | scope-bound (durable interactive runs, M3.6) |
| `run_control` | table | standard (30d) | `scope_id` | scope-bound (durable interrupt/cancel/steer, M3.6) |
| `im_reply_intents` | table | standard (30d) | `scope_id` | scope-bound (durable encrypted IM reply outbox, M3.7) |
| coding artifacts | filesystem | standard | *(by project id)* | project-scoped (repo/snapshots/worktrees/artifacts) |
| tool spill files | filesystem | short | *(by recorded path)* | session-scoped, confined to the spill root |
| Redis event streams (`events:{session_id}`) | redis | permanent | *(by session id)* | session-scoped (bounded key delete) |
| `event_tombstones` | table | long | `scope_id` | **retained** (anti-resurrection marker) |
| `erasure_requests` / `erasure_steps` | table | long | `scope_id` | **retained** (audit trail) |
| `retention_policies` | table | long | `scope_id` | **retained** (operator overrides) |
| `organizations` / `memberships` / `agents` / `resource_grants` | table | permanent | `org_id` | **org-scoped** — erased by organization erasure (M3.6) |
| `im_channel_mappings` | table | permanent | `org_id` | **org-scoped** — erased by organization erasure (FK cascade removes its route index rows, M3.7); a `run_as_user_id` composite FK to `memberships` ties each mapping to an active org member (the run actor, distinct from the platform-admin `created_by` provisioner) |
| `users` / `oidc_identities` | table | permanent | *(global)* | **identity-global** — erased by user (data-subject) erasure (M3.6) |
| provider logs / Langfuse telemetry | external | — | — | **external** — no delete API; recorded incomplete → `partial` |

Notes:

* **User content and connector state are `permanent`** — they are removed only by an explicit erasure request,
  never on a timer. Only derived/operational data (`oauth_states`, `webhook_deliveries`,
  `connector_outbox`, `approvals`, `jobs`, tool spill) carries a finite TTL.
* **`webhook_deliveries` is global** and holds only `(provider, delivery_id)` dedup
  tokens with no personal content, so scope erasure deliberately preserves it (a global
  `purge_all` / TTL sweep exists for full teardown).
* **Tombstones and the erasure ledger are retained on purpose** — deleting them would
  defeat anti-resurrection and lose the audit trail.
* **Identity is org-partitioned, not scope-bound (M3.6).** Durable users/orgs/memberships/
  Agents/grants are *not* reached by scope/session/project erasure (they carry `org_id`,
  not the runtime `scope_id`), and the `/v1/erasure` scope lifecycle API described below does
  **not** erase users or organizations. They are erased by the dedicated identity purge in
  `keel_core.identity.purge`: `purge_organization(org_id)` (tenant offboarding) and
  `purge_user(user_id)` (a data subject; cascades through the user's OIDC links, owned
  Agents, memberships, and issued grants). User erasure **never orphans an active org**: it
  is atomically blocked (`UserErasureBlockedError`, deleting nothing) when the user is the
  sole active owner of an active org that still has other active members (ownership must be
  transferred first), and it atomically archives an active org the user solely owns and is
  the only active member of. Both primitives run through the `keel_erase_user` /
  `keel_erase_organization` **`SECURITY DEFINER`** functions (migration `0013`) with an
  **org-first, deterministic lock order** (matching normal membership mutations, so a
  concurrent invite/promotion/demotion/removal can neither deadlock nor race the owner-count
  invariant). The erasure privilege is split into a `keel_maintenance` **definer**
  (`BYPASSRLS`, owns the functions + table DML) and a `keel_maintenance_exec` **executor**
  (`NOBYPASSRLS`, EXECUTE-only, no table DML, cannot `SET ROLE` into the definer); a dedicated
  maintenance **login** is a member of only the executor. `keel_runtime` can neither execute
  the functions nor `DELETE` the global identity tables (both revoked). The
  production-usable operator path is `python -m keel_core.identity.erase_cli
  {user|organization} <id>` on `KEEL_MAINTENANCE_DATABASE_URL` (fail-closed when unset;
  dry-run/preflight, explicit confirmation, structured result, explicit blocked-owner error —
  see [`docs/OPERATIONS.md`](OPERATIONS.md)). Identity is **not event-sourced**, so no
  projection rebuild can resurrect an erased identity row. Folding identity erasure into the
  durable lifecycle API (once run scope is derived from `(org, agent)`) is future work; until
  then it is a standalone maintenance command (tracked honestly here).

## 2. Retention defaults

Retention is a small set of typed classes (`keel_core.lifecycle.policies.RetentionClass`),
each with a default horizon:

| Class | Default TTL | Used for |
| --- | --- | --- |
| `transient` | 1 hour | one-time OAuth CSRF state |
| `short` | 1 day | webhook/delivery replay dedup, outbound-idempotency claims, tool spill |
| `standard` | 30 days | resolved approvals, finished jobs, finished runs, coding artifacts |
| `long` | 365 days | memory proposals, the erasure/tombstone ledger |
| `permanent` | none | sessions, events, memory, archival, knowledge, connector tokens |

Per-scope overrides are stored durably in `retention_policies` and layered over these
defaults (`ErasureStore.set_retention` / `resolve_policy(..., overrides=...)`). The
retention scheduler primitives (`keel_core.lifecycle.retention.select_expired` /
`next_expiry`) decide which expiring rows are due for cleanup; permanent resources never
appear as due.

## 3. Erasure model

An erasure request targets one of three granularities:

* **`scope`** — everything a scope owns (all scope-bound tables + its Redis streams).
* **`session`** — one session's events + derived rows (scope-global data is preserved).
* **`project`** — one coding project's on-disk artifacts.

Requests run as a **durable background job** (`lifecycle.erase`, ADR-0010) executed by the
restart-safe, idempotent, resumable `ErasureCoordinator`:

* Each step is a physical delete (safe to repeat); a step ledger (`erasure_steps`) lets a
  crash resume at the first unfinished step.
* Before a session's events are deleted, a **tombstone** (`event_tombstones`) is written so
  a projection rebuild can never resurrect erased content from a residual event source
  (Redis backlog, replica, export). Rebuilds pass the tombstone hook from
  `keel_core.lifecycle.tombstones` / `keel_core.rebuild.ProjectionRebuilder`.
* **External** provider/telemetry deletion has no API. Instead of falsely reporting
  success, the step is recorded `unsupported`/`failed` and the request finishes
  **`partial`** with `external_incomplete = true`. Only completed data-store erasure with
  no external gap finishes **`completed`**.

## 4. Operator runbook — performing an erasure

Erasure is admin-gated. With `KEEL_API_KEYS` configured, submitting/retrying requires an
`admin` key; reading status requires `operator`. In open (single-user) mode every caller
is an implicit admin.

### Submit

```bash
# Erase an entire scope
curl -sX POST http://localhost:8000/v1/erasure/requests \
  -H "X-API-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
  -d '{"target_kind":"scope","idempotency_key":"erase-2026-07-17-001","reason":"GDPR erasure"}'

# Erase one session
curl -sX POST http://localhost:8000/v1/erasure/requests \
  -H "X-API-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
  -d '{"target_kind":"session","target_id":"<session-id>","idempotency_key":"erase-sess-001"}'

# Erase one coding project's artifacts
curl -sX POST http://localhost:8000/v1/erasure/requests \
  -H "X-API-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
  -d '{"target_kind":"project","target_id":"<project-id>","idempotency_key":"erase-proj-001"}'
```

The `idempotency_key` makes submission safe to retry — a repeat resolves to the same
request and job. The response carries the `request.id` and the `job_id`.

### Watch status

```bash
curl -s http://localhost:8000/v1/erasure/requests/<request-id> -H "X-API-Key: $OPERATOR_KEY"
```

Terminal statuses:

* **`completed`** — every data-store step done/skipped, no external gap.
* **`partial`** — all data stores erased, but an external provider/telemetry deletion could
  not be verified (`external_incomplete: true`; see the `provider_telemetry` step). Follow
  up out-of-band with the provider, then this request needs no further data action.
* **`failed`** — an internal store step exhausted its retries. Investigate the `last_error`
  and **retry**.

### Retry

```bash
curl -sX POST http://localhost:8000/v1/erasure/requests/<request-id>/retry -H "X-API-Key: $ADMIN_KEY"
```

Retry enqueues a fresh job that **resumes** the same request (idempotent; already-done
steps are skipped). A `completed` request is not retryable (409).

### If the worker is down

Submission still persists the request and enqueues the durable job; the Postgres job
dispatcher (`dispatch_jobs` cron) heals a missed delivery when the worker returns. No data
is lost and the request simply stays `pending`/`running` until executed.

## 5. Recovery & verification procedure

Erasure is **destructive and irreversible** by design. Before a scope erasure in
production, take a database snapshot if a recovery window is required by policy — once the
job runs, the rows are physically gone (there is no soft-delete to undo).

### Verify an erasure completed

1. Status is `completed` (or `partial` only because of the external step):
   ```bash
   curl -s .../v1/erasure/requests/<id> | jq '.request.status, .request.external_incomplete'
   ```
2. No scoped rows remain (run against the DB as an operator):
   ```sql
   SET app.scope_id = '<scope>';
   SELECT count(*) FROM events        WHERE scope_id = '<scope>';   -- expect 0
   SELECT count(*) FROM connector_tokens WHERE scope_id = '<scope>'; -- expect 0
   -- ...repeat for the scope-bound tables in the data map...
   ```
3. The tombstone exists (anti-resurrection):
   ```sql
   SELECT count(*) FROM event_tombstones WHERE scope_id = '<scope>';  -- >= erased sessions
   ```
4. The Redis stream is gone:
   ```bash
   redis-cli EXISTS events:<session-id>    # expect 0
   ```
5. A projection rebuild does not resurrect content — a rebuild loaded with the tombstone
   hook (`load_tombstone_hook`) withholds every erased event (`skipped_tombstones` equals
   `processed`, the sink stays empty). This is asserted by
   `tests/integration/test_lifecycle_postgres.py::test_tombstone_blocks_projection_resurrection`.

### Recovery

* **Before erasure** — a Postgres logical/physical backup is the only recovery path; the
  event log is the source of truth and can be restored from backup + replayed.
* **After a `partial`** — no recovery is needed for Keel-owned data (already erased);
  complete the external provider deletion manually and record it.
* **After a `failed`** — retry (resumes). If a store repeatedly fails, inspect
  `erasure_steps` for the failed step and `erasure_requests.last_error`.

## 6. Validation

* Unit: `tests/unit/test_lifecycle_policies.py`, `test_lifecycle_erasure.py`,
  `test_lifecycle_service.py`, `test_lifecycle_api.py`, `test_lifecycle_datamap.py`
  (data-map ↔ coordinator coverage), and `test_coding_storage.py`
  (`purge_project` on-disk cleanup).
* Integration (live Postgres/Redis): `tests/integration/test_lifecycle_postgres.py` —
  seeded all-store erasure, idempotent repeats, crash/resume, concurrent job claims,
  no-resurrection rebuild, cross-scope preservation, partial external reporting, and
  expired-retention scheduling.
