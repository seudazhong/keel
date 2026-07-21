# Kubernetes storage topology

## Current stores

| Store | Current role | Manifest/wiring | Durability |
|---|---|---|---|
| PostgreSQL | Events, runs, jobs, identity, connectors, Knowledge, lifecycle, Projects, review/patch metadata | External Service + secret URL | Authoritative; requires PITR/backup |
| Redis | arq delivery, live fan-out, locks, coordination | External Service + secret URL | Reconstructable delivery state; not the lifecycle source of truth |
| Project storage | Active managed Git/project state, worktrees, review reports, patch artifacts | Active `keel-project-storage` RWX PVC mounted by server/worker | Authoritative for current file artifacts; must be backed up |
| Pod scratch | Temporary writable paths for read-only root filesystems | `emptyDir` | Disposable |

## Why managed PostgreSQL and Redis

The scaffold uses external Services rather than generic StatefulSets because database backup,
failover, patching, and observability are operator/provider responsibilities. An in-cluster operator
may replace the Service while preserving the application endpoint.

## Project storage

`KEEL_PROJECT_STORAGE_ROOT=/var/lib/keel/projects` is mounted identically into server and worker.
It must support `ReadWriteMany` because:

- multiple workers may write worktrees/reports/artifacts;
- server reads reports and project state;
- review/patch startup fails closed when the shared root is unavailable.

The current storage implementation is filesystem-based. It contains more than disposable
worktrees, so treat it as backup-relevant.

The sandbox does not mount this authoritative root. Patch generation transfers a bounded snapshot
into an isolated sandbox namespace and exports a candidate back.

## Dormant future stores

- `git-pvc.example.yaml` describes a future dedicated active-Git service/volume.
- `objectstorage-secret.example.yaml` describes future S3-compatible artifact/snapshot storage.
- the sandbox Job template's worktree volume is future per-run execution scratch.

These templates are not active resources and should not be described as current application
backends.

## Target split

The production target may separate:

1. active Git repository service/volume;
2. immutable object-store artifacts and snapshots;
3. disposable per-run worktrees;
4. build/dependency caches with bounded retention.

Authoritative Git state is never mounted writable into an untrusted sandbox.

## Encryption

- PostgreSQL/Redis TLS and provider encryption;
- volume encryption for project storage;
- object-store SSE/KMS when introduced;
- envelope encryption for connector credentials;
- authenticated sandbox RPC.

The current sandbox RPC uses HMAC authentication; mTLS is a future hardening option, not a current
claim.
