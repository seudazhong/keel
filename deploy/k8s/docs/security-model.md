# Security model — `deploy/k8s`

This document explains the security posture this scaffold provides today, its known
limitations, and — most importantly — **what is explicitly still pending** before it can
support hostile multi-tenant workloads. It complements
[ADR-0005 (sandbox)](../../../docs/adr/0005-sandbox.md),
[ADR-0009 (product form / scope isolation)](../../../docs/adr/0009-product-form-and-primary-use-cases.md),
and §8 "Threat model & tenancy invariants" of the
[managed-code-projects design doc](../../../docs/designs/2026-07-16-managed-code-projects-and-coding-agents-design.md).

## What is enforced by these manifests

| Control | Where | Effect |
|---|---|---|
| Restricted Pod `securityContext` | every Deployment + the sandbox Job template | non-root, dropped `ALL` Linux capabilities (web adds back only `NET_BIND_SERVICE` to bind port 80), no privilege escalation, `seccompProfile: RuntimeDefault`, read-only root filesystem with `emptyDir` scratch for the few writable paths each process needs |
| No sandbox ServiceAccount token | `base/sandbox/serviceaccount.yaml`, `base/sandbox/job-template.yaml` | `automountServiceAccountToken: false` on both the ServiceAccount and the Pod spec; no Role/RoleBinding anywhere targets this ServiceAccount (checked by `scripts/validate_manifests.py`) |
| Bounded resources | every container | explicit `requests`/`limits`; sandbox Jobs additionally set `activeDeadlineSeconds` and `backoffLimit: 0` so a stuck or misbehaving run cannot retry indefinitely or run unbounded |
| Default-deny network | `base/networkpolicy/default-deny-all.yaml` + narrow allows | every Pod denies all ingress/egress unless an explicit policy allows it; the sandbox's only allowed egress is the in-cluster egress-proxy and the control-plane RPC port |
| Narrow control-plane RBAC | `base/serviceaccount-control-plane.yaml` | `keel-server`'s ServiceAccount may only create/get/list/watch/delete Jobs and read Pods/Pod logs in its own namespace — no Secrets, no cluster-scoped access, no other namespaces |
| Namespace-per-org guidance | `base/namespace.yaml` | one namespace per tenant is the documented blast-radius boundary; this scaffold does not ship a controller that provisions those namespaces for you |
| Pod Security Admission | `base/namespace.yaml` (`warn`/`audit`), `overlays/production` (`enforce: restricted`) | production enforces the Kubernetes-defined "restricted" profile at admission time, independent of whether an individual manifest regresses |

## Isolation levels: what "sandbox" means today vs. the target

- **Today (current fidelity, see [`docs/OPERATIONS.md`](../../../docs/OPERATIONS.md) and
  [`docs/STATUS.md`](../../../docs/STATUS.md)):** there is no deployed sandbox service.
  `ShellTool` and other model-chosen execution run in the server/CLI process. Nothing in this
  `deploy/k8s` scaffold changes that fact by itself — the sandbox Job template
  (`base/sandbox/job-template.yaml`) is the *target* shape the control plane will submit to
  once a real sandbox execution backend exists, not a wired-up execution path today.
- **Rootless-OCI floor:** once the real sandbox exists, running it as an unprivileged
  container with the hardening in this scaffold (dropped capabilities, read-only rootfs,
  default-deny egress, resource ceilings) is the accepted floor for a **trusted single-org**
  preview (ADR-0005, design doc §9).
- **gVisor/Kata/microVM gate:** hostile multi-tenant workloads additionally require a
  stronger-isolation container runtime — gVisor, Kata Containers, or a microVM (e.g.
  Firecracker) backend — selected via `runtimeClassName`
  (`base/sandbox/runtimeclass.example.yaml`). This is **opt-in and cluster-support-gated by
  design**: the job template does not set `runtimeClassName` by default, and
  `scripts/validate_manifests.py` fails the build if that ever changes without a deliberate
  edit. Do not enable a `RuntimeClass` your cluster's nodes do not actually support — Pods
  will fail to schedule.

## NetworkPolicy caveats

Standard `networking.k8s.io/v1` `NetworkPolicy` cannot select traffic by Service name/DNS for
out-of-cluster endpoints — only by `ipBlock` CIDR, `podSelector`, or `namespaceSelector`. Since
managed Postgres/Redis/object storage are typically outside the cluster,
`base/networkpolicy/allow-control-plane-egress.yaml` ships with a placeholder CIDR you must
fill in for your environment, plus a broad (but private-range-excluding) HTTPS allow for LLM
provider/GitHub App traffic. If your CNI supports FQDN-based policies (e.g. Cilium
`CiliumNetworkPolicy`), prefer that for materially tighter control instead of the IP-range
allow shipped here.

Also verify your cluster's CNI actually **enforces** `NetworkPolicy` — some CNIs (e.g. the
default `kubenet` on certain managed offerings) accept the objects without enforcing them.
`kubectl kustomize` renders these manifests correctly regardless; enforcement is a
cluster-operator responsibility this scaffold cannot verify without a live cluster (and
verifying it live is explicitly out of scope for `scripts/validate_manifests.py` — see
"Pending gates" below).

## Secrets

No real credentials exist anywhere in this scaffold — `scripts/validate_manifests.py` scans
for live-credential-shaped patterns (AWS/OpenAI/Anthropic/GitHub key shapes, PEM private key
headers) on every commit-adjacent run and fails if one is found. `base/secret-app.example.yaml`
and `base/datastores/objectstorage-secret.example.yaml` are templates excluded from the Kustomize
build; the preferred production pattern is an external-secret operator (External Secrets
Operator, Sealed Secrets, or your cloud provider's CSI secret store driver) syncing from a real
KMS/Vault into the Secret name/shape these files document, not hand-applying filled-in
manifests.

## Pending gates (not yet true; do not represent otherwise)

These are the concrete items still required before this scaffold can back a hostile
multi-tenant deployment. They mirror
[`docs/STATUS.md`](../../../docs/STATUS.md) "Critical and high blockers" and
[`docs/ROADMAP.md`](../../../docs/ROADMAP.md) M3.3/M3.6/M3.8, and the design doc's own gated
rollout language (§8.4, §9):

1. **Real sandbox integration.** No sandbox execution backend is deployed; the Job template is
   unvalidated against a live agent runtime. Per-run Job creation/deletion, worktree
   materialization, and the control-plane↔sandbox mTLS RPC described in the design doc do not
   exist in code yet.
2. **Stronger-isolation runtime, wired end-to-end.** `runtimeClassName` support is a manifest
   hook only; no gVisor/Kata/microVM runtime has been qualified, load-tested, or made the
   default for any real workload.
3. **Identity and tenancy enforcement.** RLS is not a hard tenant boundary yet (the application
   DB role can bypass it); there is no user/org model, so "namespace-per-org" is a Kubernetes
   convention this scaffold documents, not an automated per-tenant provisioning/authorization
   system.
4. **Egress allow-list proxy.** `allow-sandbox-egress-proxy-only.yaml` assumes a
   `keel-egress-proxy` workload exists; this scaffold does not ship that proxy's Deployment or
   its allow-list/registry-mirror logic (design doc §9 "Egress") — only the NetworkPolicy that
   would constrain it once it exists.
5. **Vertical/observability integration.** OTel, Prometheus, and Langfuse are referenced in
   `docs/slo-alerting.md` as targets; no metrics/alerting stack ships in this scaffold, and
   reconciled usage/100%-required-run-tracing (M3.8 exit gate) is not implemented.
6. **Backup/restore/DR drills.** `docs/backup-restore-dr.md` documents the intended procedure
   and RPO/RTO targets; no drill has been executed against a real cluster, and no automation
   exists to run one.
7. **NetworkPolicy enforcement verification.** As noted above, this scaffold cannot verify a
   given cluster's CNI actually enforces the policies it ships — that requires a live cluster
   test this repo's cluster-free validation deliberately does not perform.

Until (1)–(4) are closed, treat any Kubernetes deployment of this scaffold as, at best, the
same **trusted single-org preview** tier as the Compose `full` profile
([`docs/OPERATIONS.md`](../../../docs/OPERATIONS.md)) — not a hostile multi-tenant platform.
