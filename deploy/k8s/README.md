# Keel — Kubernetes deployment scaffold (`deploy/k8s`)

This directory is a **production-delivery scaffold**: a concrete, validated starting point for
running Keel's control plane (`keel-server`, `keel-worker`, `keel-web`) on Kubernetes with
hardened defaults, plus a per-run sandbox Job template for the execution plane. It targets the
topology described in [`docs/ARCHITECTURE.md` §15](../../docs/ARCHITECTURE.md) and §8.4
"Deployment topology (Docker / Kubernetes)" of the
[managed-code-projects design doc](../../docs/designs/2026-07-16-managed-code-projects-and-coding-agents-design.md).

**It is not a claim of hostile multi-tenant readiness.** Read
[`docs/security-model.md`](docs/security-model.md) "Pending gates" before running untrusted,
multi-organization workloads on this scaffold. Today's real sandbox execution is a single
process (`ShellTool` in-process; see [`docs/OPERATIONS.md`](../../docs/OPERATIONS.md)), so the
sandbox Job template describes the *target* isolation shape a future, narrowly-scoped
sandbox-controller will submit to once the real sandbox service and stronger-isolation runtime
integration land — it is not wired up to a live agent runtime yet, and `keel-server` holds no
Kubernetes RBAC to create it directly (see "What is (and is not) enforced here" below).

`keel-scheduler` is **not deployed** by this scaffold: its entrypoint
(`packages/keel-scheduler/src/keel_scheduler/main.py`) is currently a stub that logs once and
exits, so a long-lived Deployment would crash-loop. Scheduling today happens inside
`keel-worker`'s own cron tick. A dormant example
(`base/scheduler/deployment.example.yaml`) documents the target shape for when the entrypoint
becomes a real long-lived, leader-elected loop.

## Layout

```
deploy/k8s/
  base/                    Kustomize base: namespace, ConfigMap, Deployments/Services,
                            NetworkPolicies, sandbox ServiceAccount/Job template, dormant
                            data-layer examples (Git PVC, Postgres/Redis, object storage)
  overlays/
    dev/                   Smaller footprint, PSA warn/audit only — trusted preview use only
    production/            PSA enforce=restricted; keel-worker replica floor raised, but
                            keel-server pinned to 1 replica (see below)
  docs/                    Storage topology, backup/restore/DR, upgrade/rollback,
                            SLO/alerting, clean-install runbook, security model
  scripts/
    validate_manifests.py  Deterministic, cluster-free manifest validation (see below)
```

## Quick start (render only — no cluster required)

```powershell
kubectl kustomize deploy\k8s\base | Out-Null            # base renders cleanly
kubectl kustomize deploy\k8s\overlays\dev | Out-Null     # dev overlay renders cleanly
kubectl kustomize deploy\k8s\overlays\production | Out-Null
```

To actually install, copy `base/secret-app.example.yaml` (set a real, non-empty
`KEEL_API_KEYS` with at least one valid `key:role` entry — required, see "What is (and is
not) enforced here" below) and `base/datastores/objectstorage-secret.example.yaml` (fill from
your secret manager, do not commit the result), point
`base/datastores/*-external-service.example.yaml` at your managed Postgres/Redis, add those to
your own overlay's `resources:`, then `kubectl apply -k <overlay>` against a real cluster. See
[`docs/clean-install-runbook.md`](docs/clean-install-runbook.md) for the full sequence.

## Validation

```powershell
python deploy\k8s\scripts\validate_manifests.py
uv run pytest tests\unit\test_deploy_k8s_manifests.py
```

Both are deterministic and cluster-free: they parse YAML, run `kubectl kustomize` (pure
client-side templating — no cluster contact), optionally run `kubeconform` schema validation
if it happens to be installed (skipped, not failed, otherwise or on any network-looking
failure), and assert the hardening properties described in
[`docs/security-model.md`](docs/security-model.md) — including that no unresolved
`CHANGEME`/`REPLACE_WITH` placeholder ships in anything actually applied, every `cidr:` value
is syntactically valid, and every `secretRef`/`configMapRef`/`serviceAccountName` resolves to
a real object or a documented external template. Neither creates, contacts, or requires a
Kubernetes cluster, kind/minikube, or any credentials.

## What is (and is not) enforced here

- Restricted `securityContext` (non-root, dropped `ALL` capabilities, no privilege escalation,
  `seccompProfile: RuntimeDefault`, read-only root filesystem) on every workload.
- `automountServiceAccountToken: false` on **every** workload, including `keel-server` — a
  process that executes shell/code tools (today, in-process; see
  [`docs/security-model.md`](docs/security-model.md) "Isolation levels") must never hold a
  Kubernetes ServiceAccount token. This scaffold ships **no RBAC at all** (no Role,
  ClusterRole, RoleBinding, or ClusterRoleBinding) for the app's own ServiceAccounts —
  `scripts/validate_manifests.py` fails the build if one is ever added back as an active
  resource. Per-run sandbox Job creation therefore is **not wired up**: it needs a
  narrowly-scoped, separately-namespaced sandbox-controller with an admission policy, which
  this scaffold documents as a pending gate rather than implementing.
- `KEEL_API_KEYS` is **required**, non-empty, contains at least one syntactically valid
  `key:role` entry (role one of `viewer`/`operator`/`admin`), and lives only in a Secret
  (never the ConfigMap) — empty/missing/malformed means every request is an implicit,
  unauthenticated admin. `KEEL_CLOUD_MODE: "true"` is forced in the active config so
  fail-closed auth is the shipped default the day that setting is consumed by application
  code (not yet — see [`docs/security-model.md`](docs/security-model.md) "Pending gates").
- `keel-server`'s tool workspace is redirected to a dedicated writable `emptyDir` at
  `/workspace` via `workingDir`, so its read-only root filesystem never has to make the app's
  own `/app` source tree the place model-chosen tool calls write to.
- `keel-server` is pinned to **1 replica** in both `base/` and the production overlay, even
  though it is architecturally stateless and horizontally scalable — interactive runs and
  pending tool-approval state are currently process-local (see
  [`docs/security-model.md`](docs/security-model.md) "Pending gates"), so a second replica
  today would silently drop an in-flight run or approval. Only `keel-worker` scales up in the
  production overlay.
- No ServiceAccount token in sandbox Jobs; the sandbox `ServiceAccount` has no Role/RoleBinding
  anywhere in this scaffold.
- Explicit `resources.requests`/`limits` on every container; sandbox Jobs additionally carry
  `activeDeadlineSeconds` and `backoffLimit: 0`.
- Default-deny `NetworkPolicy` plus narrow, explicit, **directional** allows: DNS (scoped to
  the `kube-system` namespace by name, not any namespace), control-plane data/HTTPS egress,
  `keel-web` ingress-to-server *and* the matching egress-from-web,
  sandbox-to-egress-proxy-only *and* the matching ingress-on-server for the RPC callback, and
  the matching ingress-on-egress-proxy for the sandbox's proxy traffic.
- `runtimeClassName` hooks for gVisor/Kata/microVM (`base/sandbox/runtimeclass.example.yaml`)
  that are opt-in and cluster-support-gated, never silently required.
- Namespace-per-org isolation guidance (label + doc), not a namespace-provisioning controller.
- The Git PVC and object-storage Secret are dormant `*.example.yaml` templates, not active
  resources: no current application code consumes them (see
  [`docs/storage-topology.md`](docs/storage-topology.md)), so shipping them as applied would
  be a nonfunctional claim.

See [`docs/security-model.md`](docs/security-model.md) for the full list of gates that remain
pending real sandbox, identity, and vertical integration before this scaffold can back a
hostile multi-tenant deployment.

## Boundaries of this change

This scaffold only touches `deploy/k8s/**`, deployment/runbook docs, and manifest-dedicated
validation scripts/tests. It does not modify application Python, the frontend, migrations,
`docker-compose.yml`, or `docs/ARCHITECTURE.md` beyond the links added here.
