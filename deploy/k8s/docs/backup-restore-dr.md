# Backup, restore, and disaster recovery

**Status: procedure documented, not yet drilled.** No restore drill has been executed against
a real cluster with this scaffold; treat every RPO/RTO figure below as a *target*, not a
verified guarantee, until a drill has run and its results are recorded here (see M3.8 exit
gate: "restore drill meets RPO/RTO", `docs/ROADMAP.md`).

## What must be backed up

| Store | Backup mechanism | Target RPO | Target RTO |
|---|---|---|---|
| Postgres (events, projections, jobs, schedules, connectors) | managed provider's automated snapshots + WAL/PITR, or `pg_dump` + WAL archiving if self-hosted | ≤ 15 min (via PITR) | ≤ 1 hour to a new instance |
| Active Git repository (PVC) | volume snapshot (CSI `VolumeSnapshot`) on a schedule, plus continuous Git bundle export to object storage | ≤ 1 hour | ≤ 1 hour to a fresh PVC + restored bundle |
| Object storage (Git snapshots, artifacts) | provider-level versioning/replication (S3 versioning + cross-region replication, or MinIO mirroring) | near-zero for already-durable objects | depends on provider; typically minutes |
| Redis | none required for durability — the queue/lock state is reconstructable: in-flight jobs are re-derived from Postgres job rows on worker restart, and the scheduler lock is re-acquired on next tick | n/a | n/a (re-populates on restart) |
| Kubernetes manifests / config | this Git repository (`deploy/k8s`) + your filled-in overlay (kept in your own private/secret-managed location) | n/a (source-controlled) | time to `kubectl apply -k` |

## Restore runbook (target sequence — unverified until drilled)

1. **Provision a fresh namespace** from `base/` + your production overlay, without applying
   any Deployments yet (`kubectl apply -k <overlay> --prune=false -l app.kubernetes.io/component=control-plane --dry-run=client` to sanity-check first).
2. **Restore Postgres** to the desired point-in-time on your managed provider (or via
   `pg_restore` + WAL replay if self-hosted). Confirm `alembic current` matches the expected
   migration head before proceeding.
3. **Restore the Git PVC** from the most recent `VolumeSnapshot`, or provision a fresh PVC and
   replay the latest immutable bundle from object storage for each active project.
4. **Point `base/datastores/*-external-service.example.yaml`-derived Services** at the restored
   Postgres/Redis endpoints (update the copies in your overlay, not the checked-in examples).
5. **Apply the control plane** (`keel-migrate` equivalent job, then `keel-server`,
   `keel-worker`, `keel-scheduler`, `keel-web`) and verify `/health` and `/readiness` are green
   on `keel-server` before allowing sandbox Job creation.
6. **Verify data integrity**: spot-check that recent runs/jobs/schedules referenced in
   Postgres resolve to Git refs that exist in the restored repository, and that no orphaned
   sandbox Jobs remain from before the incident (`kubectl get jobs -l app.kubernetes.io/component=sandbox`).
7. **Record the actual RPO/RTO achieved** in this file, and file a gap if either target was
   missed.

## Disaster recovery scope this scaffold does not cover

- Cross-region/cross-cluster failover automation (DNS cutover, multi-region Postgres
  replication) — this scaffold assumes a single cluster/region and documents only
  same-region restore.
- Automated backup scheduling/verification tooling — CronJobs or provider-native scheduling
  must be configured by the cluster operator; none ship here to avoid silently implying a
  managed backup service exists when it does not.
- Erasure/right-to-be-forgotten guarantees across backups (event versions exist but upcasters,
  retention, and full erasure are not implemented — `docs/OPERATIONS.md`).
