# Upgrade and rollback

> **Status:** documented scaffold procedure; not verified against a production cluster

## Principles

- pin every image by digest;
- use expand/contract database migrations;
- apply additive migrations before workloads;
- keep old and new application versions compatible during the rollout;
- verify readiness and representative durable work before completion;
- do not roll back the database blindly.

## Current rollout shapes

- `keel-worker` and `keel-web`: Kubernetes rolling update.
- `keel-server`: one replica with `Recreate`.
- no separate scheduler workload.
- sandbox execution plane is operator-supplied and needs its own version/compatibility procedure.

The server's conservative shape remains because multi-server streaming, OAuth callbacks, and
rolling-version compatibility have not been qualified together. Durable runs alone do not prove
the complete topology.

## Forward upgrade

1. Back up PostgreSQL and project storage.
2. Render/validate the new overlay.
3. Run `alembic upgrade head` as the owner/migrator Job.
4. Run runtime-principal provision/verification.
5. Upgrade the sandbox execution plane and verify protocol compatibility.
6. Upgrade `keel-server`.
7. Upgrade `keel-worker`.
8. Upgrade `keel-web`.
9. Verify:
   - `/health` and `/readiness`;
   - runtime DB principal;
   - run admission and worker claim;
   - connector/reconciliation health;
   - Project/review storage;
   - representative session, job, review, and enabled patch flow.

## Rollback

```powershell
kubectl rollout history deployment/keel-server -n keel
kubectl rollout undo deployment/keel-server -n keel
```

Application rollback is safe only when the migrated schema remains backward-compatible. Otherwise
restore the database/project-storage backup to a consistent point before restarting old binaries.

## Release evidence required

- image and manifest digests;
- migration head;
- compatibility window;
- backup references;
- readiness output;
- representative durable-work results;
- rollback rehearsal result.

Zero-downtime, multi-server rolling upgrades are a future production gate, not a current scaffold
claim.
