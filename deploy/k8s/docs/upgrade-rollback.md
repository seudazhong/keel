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
2. **Roll `keel-server`**, then `keel-worker`, then `keel-web`. `keel-server` is stateless and
   horizontally scalable, so a rolling update here causes no downtime as long as
   `readinessProbe` gates traffic correctly (it does — see `base/server/deployment.yaml`).
3. **Roll `keel-scheduler` last, with care.** It uses `strategy: Recreate`
   (`base/scheduler/deployment.yaml`) specifically so two scheduler replicas are never running
   concurrently mid-rollout — this causes a brief scheduling gap (missed tick), which is
   preferable to a double-fired schedule. Confirm only one `keel-scheduler` Pod is `Running`
   before considering the rollout complete (`kubectl get pods -n keel -l app.kubernetes.io/name=keel-scheduler`).

## Sandbox Job template changes

The sandbox Job template (`base/sandbox/job-template.yaml`) is rendered per-run by the control
plane, not deployed once — an upgrade to it takes effect the next time `keel-server` renders a
new Job, with no rollout of its own. Confirm any template change still passes
`scripts/validate_manifests.py` before shipping it in a control-plane release.

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
- [ ] `keel-scheduler` shows exactly one `Running` Pod at all times (never zero, never two).
- [ ] `keel-web` rolled last (depends only on `keel-server`'s stable API surface).
- [ ] Post-rollout smoke: `/health`, `/readiness`, and a representative run/job complete
  successfully before declaring the upgrade done.

## What is not yet true (M3.8 gate)

Repeatable clean install **and** upgrade, N-worker load/chaos demonstration, and
production-image accuracy are M3.8 exit gates (`docs/ROADMAP.md`) that this scaffold documents
a path toward but does not itself prove — no upgrade has been exercised against a live cluster
using these manifests.
