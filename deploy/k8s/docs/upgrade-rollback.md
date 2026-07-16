# Upgrade and rollback

## Rolling upgrade (control plane)

`keel-server`, `keel-worker`, and `keel-web` use the default `RollingUpdate` strategy and are
safe to upgrade with standard Kubernetes rolling updates:

```powershell
kubectl set image deployment/keel-server keel-server=ghcr.io/OWNER/keel-app:NEW_DIGEST -n keel
kubectl rollout status deployment/keel-server -n keel
```

Repeat for `keel-worker` and `keel-web`. Order matters when a release includes a migration:

1. **Run `alembic upgrade head`** as a one-shot Job (mirrors `keel-migrate` in
   `docker-compose.yml`) against the new image **before** rolling any Deployment. Migrations
   must be additive/backward-compatible with the currently-running previous version for the
   duration of the rollout (standard expand/contract migration discipline) since old and new
   Pods run concurrently mid-rollout.
2. **Roll `keel-server`**, then `keel-worker`, then `keel-web`. `keel-server` is architecturally
   stateless and horizontally scalable, but is pinned to 1 replica in this scaffold today
   (`docs/security-model.md` "Why `keel-server` is pinned to one replica" — interactive
   run/approval state is process-local). A single-replica `RollingUpdate` still briefly runs
   an old and a new Pod together while the new one becomes ready, which carries the same
   caveat: an in-flight run/approval could still land on the Pod that gets terminated. This
   is an accepted gap pending the same durable-coordination work, not a hidden regression —
   avoid rolling `keel-server` during a window with known in-flight interactive runs/approvals
   until M3.3/M3.6 close this gate.

`keel-scheduler` is not part of this rollout: this scaffold does not deploy it (its entrypoint
is currently a stub that would crash-loop as a long-lived Deployment — see
`docs/security-model.md` "Why `keel-scheduler` is not deployed"). Scheduling today is a
`keel-worker` cron tick, so it rolls with `keel-worker` above; there is no separate scheduler
rollout ordering to worry about until a real `keel-scheduler` service exists
(`base/scheduler/deployment.example.yaml` documents the target shape, including the
`strategy: Recreate` a real leader-elected scheduler would need).

## Sandbox Job template changes

The sandbox Job template (`base/sandbox/job-template.yaml`) is not currently submitted by
anything (see `docs/security-model.md` "Sandbox Job creation is not wired up") — there is no
rollout to sequence for it today. Once a sandbox-controller exists, an upgrade to this template
would take effect the next time it renders a new Job, with no rollout of its own. Confirm any
template change still passes `scripts/validate_manifests.py` before shipping it.

## Rollback

Every Deployment keeps Kubernetes' default revision history, so:

```powershell
kubectl rollout undo deployment/keel-server -n keel
kubectl rollout history deployment/keel-server -n keel   # to target a specific revision
```

**Rollback safety depends on the migration discipline above.** If the new version's migration
was additive/backward-compatible, rolling back the application image is safe without a
database rollback. If a migration was not backward-compatible (should not happen under
expand/contract discipline, but verify), a database rollback/point-in-time restore is required
first — see `docs/backup-restore-dr.md`.

## Zero-downtime checklist

- [ ] Migration applied and verified additive/backward-compatible.
- [ ] `keel-server` readiness probe green for all new-revision Pods before `keel-worker` rolls.
- [ ] `keel-web` rolled last (depends only on `keel-server`'s stable API surface).
- [ ] Post-rollout smoke: `/health`, `/readiness`, and a representative run/job complete
  successfully before declaring the upgrade done.

## What is not yet true (M3.8 gate)

Repeatable clean install **and** upgrade, N-worker load/chaos demonstration, and
production-image accuracy are M3.8 exit gates (`docs/ROADMAP.md`) that this scaffold documents
a path toward but does not itself prove — no upgrade has been exercised against a live cluster
using these manifests.
