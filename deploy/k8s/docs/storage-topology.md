# Storage topology

Three physically distinct stores back a Keel Kubernetes deployment, matching the split in the
managed-code-projects design doc's
[§2.3](../../../docs/designs/2026-07-16-managed-code-projects-and-coding-agents-design.md#23-git-object-storage-vs-working-volumes-the-key-split):

| Layer | Contents | Manifest | Durability | Lifecycle |
|---|---|---|---|---|
| **Relational metadata** | events, projections, jobs, schedules, connectors, quotas | `base/datastores/postgres-external-service.example.yaml` (points at a managed Postgres) | authoritative; requires PITR-capable backups | long-lived, migrated via Alembic |
| **Cache/queue/lock** | arq queue, scheduler leader lock, pub/sub | `base/datastores/redis-external-service.example.yaml` | operationally important but reconstructable from Postgres state on total loss | long-lived |
| **Active Git repository** | mutable bare repo: objects, refs, packfiles | `base/datastores/git-pvc.yaml` | authoritative active state; back up regularly | lives with the project; serialized maintenance/GC |
| **Git snapshots/backups** | immutable bundles/snapshots | object storage (`base/datastores/objectstorage-secret.example.yaml`) | durable recovery copy, not directly mutated | versioned/retained by policy |
| **Working volume (worktree)** | one writable checkout + build outputs, per run | sandbox Job's `emptyDir` (`base/sandbox/job-template.yaml`) | **disposable** | created per run; reclaimed on run end; hard TTL (`activeDeadlineSeconds`) |
| **Spilled tool output / artifacts** | large tool output, diffs, build reports | object storage | scoped, purgeable (retention_class) | per-run/per-artifact |

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

## Git storage: PVC vs. Git-smart service

`base/datastores/git-pvc.yaml` requests `ReadWriteMany` so multiple `keel-server`/`keel-worker`
replicas can serve concurrent repository operations, assuming a CSI driver that supports RWX
(NFS-backed classes, EFS, Filestore, Azure Files). If your cluster's storage only supports
`ReadWriteOnce`:

- run a single writer with correct repository locking (accept reduced availability), or
- front the volume with an external Git-smart service (e.g. a dedicated Git server/service
  layer) instead of a directly-mounted PVC, matching the design doc's stated alternative.

Either way, the **authoritative repository is never mounted into a sandbox Pod** — sandbox
Jobs only ever receive an ephemeral worktree materialized from an immutable object-storage
snapshot by the `keel-worktree-init` init container. This is enforced structurally (the Job
template has no volume referencing `keel-git-storage`) and checked by
`scripts/validate_manifests.py`.

## Object storage

Any S3-compatible endpoint works (AWS S3, MinIO, GCS via S3 interop). It holds:

- immutable Git bundles/snapshots (recovery source, never a directly-mutated mirror),
- backups,
- spilled tool output and build/test artifacts (`sha256` + `retention_class`, purgeable — M3.5
  work per `docs/ROADMAP.md`).

Prefer workload identity (IRSA / GCP Workload Identity / Azure Managed Identity) over static
access keys where your cloud provider supports it — see the comment in
`base/datastores/objectstorage-secret.example.yaml`.

## Encryption

- At rest: object store server-side encryption (SSE) + volume encryption for the Git PVC
  (your CSI driver's encryption-at-rest option, or LUKS/provider KMS underneath it).
- In transit: TLS to managed Postgres/Redis/object storage; the control-plane↔sandbox RPC is
  mTLS internally (design doc §8.4) — not yet implemented (see `docs/security-model.md`
  "Pending gates").
