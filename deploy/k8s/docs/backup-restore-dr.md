# Backup, restore, and disaster recovery

> **Status:** target procedure; no verified production drill has been recorded

## Backup set

| Store | Required backup |
|---|---|
| PostgreSQL | Provider snapshots plus WAL/PITR, or equivalent tested backup. |
| Project storage PVC | CSI snapshots or filesystem backup covering active project state, reports, and patch artifacts. |
| Deployment configuration | Private overlay, image digests, non-secret config, and secret-manager references. |
| Secret manager/KMS | Provider-managed durability and documented recovery/rotation process. |
| Redis | No authoritative-data backup requirement; recreate and let durable outboxes/jobs reconcile. |

Future object storage and dedicated Git storage join the backup set when they become active.

## Restore sequence

1. Provision a clean cluster/namespace and managed data services.
2. Restore PostgreSQL to the chosen point and verify the expected Alembic head.
3. Restore project storage and verify server/worker mount the same contents.
4. Restore/configure secret-manager references.
5. Run migration/provision Jobs using the restored database.
6. Deploy the sandbox execution plane and control-plane workloads.
7. Wait for full readiness.
8. Verify recent sessions/runs/jobs, connector state, Projects, review reports, and patch artifacts.
9. Confirm dispatch/reconciliation drains stranded work without duplicate external effects.
10. Record achieved RPO/RTO and gaps.

## Erasure and backups

Production policy must define when erased data leaves backups and how tombstones/erasure ledgers are
preserved without restoring user content into active service.

The lifecycle data map currently needs classification updates for newer dispatch/index and patch
tables before a complete production erasure/restore claim.

## Not covered by this scaffold

- automated backup scheduling and verification;
- cross-region failover and DNS cutover;
- tested point-in-time recovery;
- tested project-storage restore;
- secret-manager disaster recovery;
- declared and achieved production RPO/RTO.

No RPO/RTO number is a guarantee until a real drill records it.
