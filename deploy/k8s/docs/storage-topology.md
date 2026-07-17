# Storage topology

Three physically distinct stores back a Keel Kubernetes deployment, matching the split in the
managed-code-projects design doc's
[§2.3](../../../docs/designs/2026-07-16-managed-code-projects-and-coding-agents-design.md#23-git-object-storage-vs-working-volumes-the-key-split).
**Only Postgres/Redis are actually consumed by current application code** — the Git/object
storage rows are dormant templates for the not-yet-implemented managed-code-projects feature
(see "Git storage and object storage are dormant" below).

| Layer | Contents | Manifest | Active? | Durability | Lifecycle |
|---|---|---|---|---|---|
| **Relational metadata** | events, projections, jobs, schedules, connectors, quotas | `base/datastores/postgres-external-service.example.yaml` (points at a managed Postgres) | consumed via `KEEL_DATABASE_URL`/`KEEL_DATABASE_HOST` | authoritative; requires PITR-capable backups | long-lived, migrated via Alembic |
| **Cache/queue/lock** | arq queue, scheduler leader lock, pub/sub | `base/datastores/redis-external-service.example.yaml` | consumed via `KEEL_REDIS_URL`/`KEEL_REDIS_HOST` | operationally important but reconstructable from Postgres state on total loss | long-lived |
| **Active Git repository** | mutable bare repo: objects, refs, packfiles | `base/datastores/git-pvc.example.yaml` | **dormant — not mounted by any Deployment; no app config references it** | authoritative active state; back up regularly, once real | lives with the project; serialized maintenance/GC |
| **Git snapshots/backups** | immutable bundles/snapshots | object storage (`base/datastores/objectstorage-secret.example.yaml`) | **dormant — no app config references an object-storage endpoint today** | durable recovery copy, not directly mutated | versioned/retained by policy |
| **Working volume (worktree)** | one writable checkout + build outputs, per run | sandbox Job's `emptyDir` (`base/sandbox/job-template.yaml`) | dormant along with the Job template itself (see `docs/security-model.md` "Sandbox Job creation is not wired up") | **disposable** | created per run; reclaimed on run end; hard TTL (`activeDeadlineSeconds`) |
| **Server/worker tool workspace** | `AgentRuntime`/execution-environment scratch (`Path.cwd()`-based; see `docs/security-model.md` "Isolation levels" for the `keel-sandbox` RPC default vs. the in-process opt-out) | `base/server/deployment.yaml` `emptyDir` at `/workspace` | **active** — this is real, and distinct from the Git/object storage rows above | disposable | Pod lifetime |
| **Spilled tool output / artifacts** | large tool output, diffs, build reports | object storage | dormant (see above) | scoped, purgeable (retention_class) | per-run/per-artifact |

## Why no in-cluster Postgres/Redis StatefulSet ships here

This scaffold references managed Postgres/Redis (or a supported operator, e.g.
CloudNativePG for Postgres) via `ExternalName` Services rather than shipping a bespoke
StatefulSet, because:

- Backup/PITR, failover, and patching for a stateful database are a full operational
  discipline of their own; duplicating it in a generic scaffold would be worse than pointing
  at a managed offering or a maintained operator.
- The `KEEL_DATABASE_HOST`/`KEEL_REDIS_HOST` config keys
  (`base/configmap-app.yaml`) and Service names stay stable either way — swapping the
  `ExternalName` for a real in-cluster Service (from an operator) requires no application
  config change.

If you must self-host in-cluster, replace the `ExternalName` Service with your operator's
Service of the same name/namespace and follow its own backup/DR documentation instead of this
scaffold's.

## Git storage and object storage are dormant

Unlike Postgres/Redis, `base/datastores/git-pvc.example.yaml` and
`base/datastores/objectstorage-secret.example.yaml` are **not** part of the active Kustomize
build, and nothing in `packages/keel-core/src/keel_core/config.py` has a Git-storage or
object-storage setting today. Shipping an unconsumed PVC/Secret as an applied resource would
create a volume/credential nothing ever reads or writes — a nonfunctional claim of capability
that does not exist yet (see `docs/security-model.md`).

Promote them back into `base/kustomization.yaml`'s `resources:` — and add the corresponding
`volumeMounts`/env wiring to the service that will own them — once the managed-code-projects
Git-storage feature (design doc §2.3) actually lands. Until then, treat the descriptions below
as the *target* shape, not current behavior.

### Git storage: PVC vs. Git-smart service (target design)

`base/datastores/git-pvc.example.yaml` requests `ReadWriteMany` so multiple
`keel-server`/`keel-worker` replicas could serve concurrent repository operations, assuming a
CSI driver that supports RWX (NFS-backed classes, EFS, Filestore, Azure Files). If your
cluster's storage only supports `ReadWriteOnce`:

- run a single writer with correct repository locking (accept reduced availability), or
- front the volume with an external Git-smart service (e.g. a dedicated Git server/service
  layer) instead of a directly-mounted PVC, matching the design doc's stated alternative.

Either way, the **authoritative repository must never be mounted into a sandbox Pod** —
sandbox Jobs are designed to only ever receive an ephemeral worktree materialized from an
immutable object-storage snapshot by the `keel-worktree-init` init container. This is enforced
structurally today (the Job template has no volume referencing `keel-git-storage`) and checked
by `scripts/validate_manifests.py`, even though the Job template itself is not currently
submitted by anything (`docs/security-model.md` "Sandbox Job creation is not wired up").

## Object storage (target design)

Any S3-compatible endpoint would work (AWS S3, MinIO, GCS via S3 interop), intended to hold:

- immutable Git bundles/snapshots (recovery source, never a directly-mutated mirror),
- backups,
- spilled tool output and build/test artifacts (`sha256` + `retention_class`, purgeable — M3.5
  work per `docs/ROADMAP.md`).

Prefer workload identity (IRSA / GCP Workload Identity / Azure Managed Identity) over static
access keys where your cloud provider supports it — see the comment in
`base/datastores/objectstorage-secret.example.yaml`.

## Server/worker tool workspace (active today)

Distinct from the dormant Git/object storage above: `base/server/deployment.yaml` mounts a
real, active `emptyDir` at `/workspace` and sets `workingDir: /workspace` so keel-server's
`AgentRuntime` workspace and execution-environment fallback (both default to `Path.cwd()`,
`packages/keel-server/src/keel_server/app.py`) have somewhere writable to use that is not the
read-only `/app` source tree — whether tool calls are routed through the default
`keel-sandbox` RPC client or the explicit `unsafe-local-dev` opt-out (see
`docs/security-model.md` "Isolation levels"). This is disposable, Pod-lifetime scratch — it is
not durable storage and is unrelated to the Git PVC.

## Encryption

- At rest: object store server-side encryption (SSE) + volume encryption for the Git PVC, once
  real (your CSI driver's encryption-at-rest option, or LUKS/provider KMS underneath it).
- In transit: TLS to managed Postgres/Redis/object storage; the control-plane↔sandbox RPC is
  mTLS internally (design doc §8.4) — not yet implemented (see `docs/security-model.md`
  "Pending gates").
