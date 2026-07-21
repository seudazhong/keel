# Kubernetes scaffold security model

This document describes what `deploy/k8s` enforces and what it does **not** provide. The scaffold is
not a production Keel release.

## Enforced by the manifests

| Control | Current effect |
|---|---|
| Restricted security context | Non-root, read-only rootfs, dropped capabilities, no privilege escalation, seccomp runtime default, bounded resources. |
| No ServiceAccount token | `automountServiceAccountToken: false` on application and sandbox-template workloads. |
| No application RBAC | Server/worker cannot create arbitrary Pods/Jobs or read cluster Secrets/PVCs. |
| Cloud-mode auth | Kubernetes config forces fail-closed auth; application credentials live in a Secret. |
| Runtime DB split | Standing workloads use the non-owner runtime login; migration/provision Jobs use owner credentials. |
| Default-deny networking | Narrow paired ingress/egress policies and DNS rules. |
| Project storage | Active RWX PVC mounted by server and worker at `KEEL_PROJECT_STORAGE_ROOT`. |
| Conservative server rollout | One server replica with `Recreate` until scale/rolling-upgrade behavior is qualified. |
| Production overlay PSA | Kubernetes restricted Pod Security admission. |

Manifest validation checks these properties without contacting a cluster.

## Current runtime truth

### Durable runs

Interactive runs and approvals are Postgres-owned and worker-executed. Process-local run state is
no longer the reason for the single-replica pin.

It remains pinned because these manifests have not qualified:

- multi-server SSE/live fan-out;
- OAuth callback/session behavior;
- rolling upgrades with old/new API versions;
- load and failure behavior of the current web/server routing.

Changing the replica count or rollout strategy requires acceptance evidence, not only a code claim.

### Sandbox

Application code defaults to the authenticated `keel-sandbox` RPC backend and readiness probes it.

Standard Compose deploys a hardened file-only sandbox service. This Kubernetes scaffold does not
deploy an equivalent Service/Deployment or a sandbox controller. Therefore a deployment rendered
from the active base will fail the sandbox readiness gate until an operator adds the execution
plane.

The dormant per-run Job template is a target shape, not a live controller:

- no workload has RBAC to create it;
- no controller validates/submits it;
- no runtime route binds a run to that Job;
- the stronger `RuntimeClass` examples are opt-in only.

Do not bypass this gap with in-process execution in a cloud deployment.

### Scheduler

`keel-scheduler` is not deployed. Scheduling and reconciliation currently run as worker cron tasks.
The dormant scheduler Deployment is a target for a future long-lived, leader-elected service.

### Project storage

Managed Projects, review reports, and patch artifacts are implemented and use the active
`keel-project-storage` RWX PVC.

The separate `git-pvc.example.yaml` and object-storage Secret remain dormant. They describe a future
storage split, not the current application contract.

## Why application workloads have no Kubernetes RBAC

A principal allowed to create arbitrary Jobs can indirectly mount namespace Secrets/PVCs and run
arbitrary images. That is too much authority for the API server or model-orchestration worker.

Future per-run execution requires a dedicated sandbox controller in a separate trust boundary or an
admission policy that fixes:

- image;
- mounts;
- identity;
- resources;
- runtime class;
- network policy;
- command contract;
- cleanup and TTL.

The control plane should call that narrow controller over authenticated internal RPC rather than
receive general Kubernetes API authority.

## NetworkPolicy caveats

Kubernetes `NetworkPolicy` is directional; both source egress and destination ingress are required.
The scaffold includes paired rules for its known paths.

Operators must still verify:

- the cluster CNI actually enforces NetworkPolicy;
- managed service CIDRs or provider-specific FQDN policy;
- ingress-controller namespace selection;
- any added sandbox/service path has both policy halves;
- public HTTPS egress is narrowed for the deployed provider set.

Cluster-free validation cannot prove live network enforcement.

## Secrets

Checked-in files contain examples/placeholders only. Use an external secret operator or provider
secret store.

Required categories include:

- API/machine auth;
- runtime and migration database credentials;
- sandbox RPC secret;
- provider/connector/GitHub credentials;
- envelope-encryption keys.

The sandbox must not receive database, Redis, provider, connector-refresh, or GitHub writeback
credentials.

## Pending release gates

Do not call this scaffold production-ready until:

1. a real execution-plane deployment/controller satisfies sandbox readiness;
2. browser OIDC and explicit administration replace machine-key-only setup;
3. a separate scheduler and capability-specific worker pools exist;
4. server scale and rolling upgrades are exercised;
5. OTel/metrics/SLO/alerting are live;
6. Postgres and project-storage backup/restore is drilled;
7. accepted Routine occurrence and ambiguous-effect recovery are proven;
8. two-user/private-team isolation passes;
9. command execution, if enabled, uses qualified per-run isolation.

See [Roadmap](../../../docs/ROADMAP.md) and [Operations](../../../docs/OPERATIONS.md).
