# Keel — Kubernetes deployment scaffold (`deploy/k8s`)

This directory is a **production-delivery scaffold**: a concrete, validated starting point for
running Keel's control plane (`keel-server`, `keel-worker`, `keel-scheduler`, `keel-web`) on
Kubernetes with hardened defaults, plus a per-run sandbox Job template for the execution
plane. It targets the topology described in
[`docs/ARCHITECTURE.md` §15](../../docs/ARCHITECTURE.md) and §8.4 "Deployment topology
(Docker / Kubernetes)" of the
[managed-code-projects design doc](../../docs/designs/2026-07-16-managed-code-projects-and-coding-agents-design.md).

**It is not a claim of hostile multi-tenant readiness.** Read
[`docs/security-model.md`](docs/security-model.md) "Pending gates" before running untrusted,
multi-organization workloads on this scaffold. Today's real sandbox execution is a single
process (`ShellTool` in-process; see [`docs/OPERATIONS.md`](../../docs/OPERATIONS.md)), so the
sandbox Job template describes the *target* isolation shape the control plane will submit to
once the real sandbox service and stronger-isolation runtime integration land — it is not
wired up to a live agent runtime yet.

## Layout

```
deploy/k8s/
  base/                    Kustomize base: namespace, ConfigMap, Deployments/Services,
                            RBAC, NetworkPolicies, sandbox ServiceAccount/Job template,
                            data-layer references (Git PVC, Postgres/Redis, object storage)
  overlays/
    dev/                   Smaller footprint, PSA warn/audit only — trusted preview use only
    production/            PSA enforce=restricted, higher replica floors
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

To actually install, copy `base/secret-app.example.yaml` and
`base/datastores/objectstorage-secret.example.yaml` (fill from your secret manager, do not commit
the result), point `base/datastores/*-external-service.example.yaml` at your managed Postgres/Redis,
add those to your own overlay's `resources:`, then `kubectl apply -k <overlay>` against a real
cluster. See [`docs/clean-install-runbook.md`](docs/clean-install-runbook.md) for the full
sequence.

## Validation

```powershell
python deploy\k8s\scripts\validate_manifests.py
uv run pytest tests\unit\test_deploy_k8s_manifests.py
```

Both are deterministic and cluster-free: they parse YAML, run `kubectl kustomize` (pure
client-side templating — no cluster contact), and assert the hardening properties described
in [`docs/security-model.md`](docs/security-model.md). Neither creates, contacts, or requires
a Kubernetes cluster, kind/minikube, or any credentials.

## What is (and is not) enforced here

- Restricted `securityContext` (non-root, dropped `ALL` capabilities, no privilege escalation,
  `seccompProfile: RuntimeDefault`, read-only root filesystem) on every workload.
- No ServiceAccount token in sandbox Jobs; the sandbox `ServiceAccount` has no Role/RoleBinding
  anywhere in this scaffold.
- Explicit `resources.requests`/`limits` on every container; sandbox Jobs additionally carry
  `activeDeadlineSeconds` and `backoffLimit: 0`.
- Default-deny `NetworkPolicy` plus narrow, explicit allows (DNS, control-plane data access,
  sandbox-to-egress-proxy-only, ingress-to-web/server).
- `runtimeClassName` hooks for gVisor/Kata/microVM (`base/sandbox/runtimeclass.example.yaml`)
  that are opt-in and cluster-support-gated, never silently required.
- Namespace-per-org isolation guidance (label + doc), not a namespace-provisioning controller.

See [`docs/security-model.md`](docs/security-model.md) for the full list of gates that remain
pending real sandbox, identity, and vertical integration before this scaffold can back a
hostile multi-tenant deployment.

## Boundaries of this change

This scaffold only touches `deploy/k8s/**`, deployment/runbook docs, and manifest-dedicated
validation scripts/tests. It does not modify application Python, the frontend, migrations,
`docker-compose.yml`, or `docs/ARCHITECTURE.md` beyond the links added here.
