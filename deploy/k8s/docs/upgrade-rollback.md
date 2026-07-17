# Upgrade and rollback

## Rolling upgrade (control plane)

`keel-worker` and `keel-web` use the default `RollingUpdate` strategy and are safe to upgrade
with standard Kubernetes rolling updates. `keel-server` instead uses `strategy: Recreate`
(`base/server/deployment.yaml`) — see "Why keel-server upgrades use `Recreate`, not
`RollingUpdate`" below before assuming it behaves like the other two.

```powershell
kubectl set image deployment/keel-server keel-server=ghcr.io/OWNER/keel-app:NEW_DIGEST -n keel
kubectl rollout status deployment/keel-server -n keel
```

Repeat for `keel-worker` and `keel-web`. Order matters when a release includes a migration:

1. **Run `alembic upgrade head`** as a one-shot Job (mirrors `keel-migrate` in
   `docker-compose.yml`) against the new image **before** rolling any Deployment. Migrations
   must be additive/backward-compatible with the currently-running previous version for the
   duration of the rollout (standard expand/contract migration discipline), since `keel-worker`
   and `keel-web` still run old and new Pods concurrently mid-rollout under `RollingUpdate`
   (`keel-server` does not — see below).
2. **Roll `keel-server`**, then `keel-worker`, then `keel-web`. `keel-server` is architecturally
   stateless and horizontally scalable, but is pinned to 1 replica in this scaffold today
   (`docs/security-model.md` "Why `keel-server` is pinned to one replica" — interactive
   run/approval state is process-local). Because its rollout `strategy` is `Recreate`, the
   running Pod is terminated *before* the new one is created: there is a brief,
   readiness-probe-bounded window with zero `keel-server` Pods (new requests queue at the
   Service/Ingress until the new Pod passes its readiness probe), but an old and a new
   `keel-server` are never scheduled at the same time, so an in-flight run/approval can never
   land on the "wrong" Pod mid-rollout. `scripts/validate_manifests.py` and
   `tests/unit/test_deploy_k8s_manifests.py` enforce `replicas: 1` plus this non-overlapping
   strategy in both the base manifest and the rendered production overlay.

`keel-scheduler` is not part of this rollout: this scaffold does not deploy it (its entrypoint
is currently a stub that would crash-loop as a long-lived Deployment — see
`docs/security-model.md` "Why `keel-scheduler` is not deployed"). Scheduling today is a
`keel-worker` cron tick, so it rolls with `keel-worker` above; there is no separate scheduler
rollout ordering to worry about until a real `keel-scheduler` service exists
(`base/scheduler/deployment.example.yaml` documents the target shape, including the
`strategy: Recreate` a real leader-elected scheduler would need).

## Why keel-server upgrades use `Recreate`, not `RollingUpdate`

`replicas: 1` alone does not stop an old and a new `keel-server` Pod from running at the same
time during an upgrade: Kubernetes' default `RollingUpdate` strategy (25% `maxSurge`/
`maxUnavailable`) rounds `maxSurge` up to 1 even at a single replica, so it starts the
new-revision Pod *before* removing the old one. With interactive runs and pending
tool-approval state still process-local (`docs/security-model.md` "Why `keel-server` is
pinned to one replica"), that overlap window is exactly where a client's SSE stream or an
in-flight approval could land on the "wrong" Pod and be silently lost — the same hazard the
single-replica pin exists to prevent, just re-introduced by the rollout itself.
`base/server/deployment.yaml` therefore sets `strategy: type: Recreate`, trading a brief
full-stop gap during upgrades (typically single-digit seconds, bounded by the readiness
probe's `initialDelaySeconds`/`periodSeconds`) for the guarantee that old and new
`keel-server` Pods are never scheduled at once. Revert to `RollingUpdate` only alongside the
durable interactive run/approval coordination that also lifts the single-replica pin
(`docs/ROADMAP.md` M3.3/M3.6 exit gates) — until then, an explicit `RollingUpdate` with
`maxSurge: 0` would be an equally safe alternative, but `Recreate` is the simpler guarantee to
audit and is what this scaffold ships.

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
first — see `docs/backup-restore-dr.md`. `keel-server`'s rollback goes through the same
`Recreate` strategy as a forward upgrade (it is part of the Deployment spec Kubernetes reverts
to), so a rollback also never overlaps the reverted-to and reverted-from Pods.

## Upgrade checklist

- [ ] Migration applied and verified additive/backward-compatible.
- [ ] `keel-server` readiness probe green for the new-revision Pod before `keel-worker` rolls.
      Expect a brief full-stop gap while the old `keel-server` Pod terminates and the new one
      starts (`strategy: Recreate` — see "Why keel-server upgrades use `Recreate`" above); this
      is not a zero-downtime rollout for `keel-server`, unlike `keel-worker`/`keel-web`.
- [ ] `keel-web` rolled last (depends only on `keel-server`'s stable API surface).
- [ ] Post-rollout smoke: `/health`, `/readiness`, and a representative run/job complete
  successfully before declaring the upgrade done.

## What is not yet true (M3.8 gate)

Repeatable clean install **and** upgrade, N-worker load/chaos demonstration, and
production-image accuracy are M3.8 exit gates (`docs/ROADMAP.md`) that this scaffold documents
a path toward but does not itself prove — no upgrade has been exercised against a live cluster
using these manifests.
