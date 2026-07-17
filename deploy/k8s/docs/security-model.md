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
| No ServiceAccount token anywhere | every workload, `base/sandbox/serviceaccount.yaml`, `base/sandbox/job-template.yaml` | `automountServiceAccountToken: false` everywhere, including `keel-server`/`keel-worker` (which route shell/file tool calls through the `keel-sandbox` RPC boundary by default, or in-process only via an explicit opt-out — see "Isolation levels" below); `scripts/validate_manifests.py` fails the build if any workload omits this |
| No Kubernetes RBAC at all | (nothing — deliberately) | this scaffold grants **zero** Role/ClusterRole/RoleBinding/ClusterRoleBinding to any of its own ServiceAccounts; `scripts/validate_manifests.py` fails the build if one is ever added back as an active resource. See "Sandbox Job creation is not wired up" below for why |
| Required externally-supplied auth, fail-closed | `base/secret-app.example.yaml` (`KEEL_API_KEYS`), `base/configmap-app.yaml` (`KEEL_CLOUD_MODE`) | `KEEL_API_KEYS` must be non-empty, contain at least one syntactically valid `key:role` entry, and lives only in a Secret, never the plaintext ConfigMap. `KEEL_CLOUD_MODE: "true"` is forced in the active config and **is consumed by application code** (M3.3): `Settings.cloud_mode` drives `app.state.auth_required` (`packages/keel-server/src/keel_server/app.py`), which `authenticate()` (`packages/keel-server/src/keel_server/auth.py`) checks — with cloud mode on, an empty *or malformed* `KEEL_API_KEYS` (every entry skipped by `parse_api_keys`, leaving an empty key map) makes every request fail closed with 503, instead of the local implicit-admin open mode. `scripts/validate_manifests.py` fails the build if any of this regresses, and never logs the key value itself when it does |
| Single-replica, non-overlapping-rollout `keel-server` | `base/server/deployment.yaml` (`replicas: 1`, `strategy: Recreate`), `overlays/production/patch-server-single-replica.yaml` | held at exactly 1 replica in both base and production, with a `Recreate` rollout strategy so an old and new `keel-server` Pod are never scheduled at once — see "Why `keel-server` is pinned to one replica" below |
| Non-source-tree tool workspace | `base/server/deployment.yaml` (`workingDir: /workspace` + matching `emptyDir`) | keeps the read-only root filesystem consistent with the app's actual `Path.cwd()`-based workspace (`packages/keel-server/src/keel_server/app.py`) instead of pointing it at the read-only `/app` source tree |
| Bounded resources | every container | explicit `requests`/`limits`; sandbox Jobs additionally set `activeDeadlineSeconds` and `backoffLimit: 0` so a stuck or misbehaving run cannot retry indefinitely or run unbounded |
| Default-deny network, both directions | `base/networkpolicy/default-deny-all.yaml` + narrow, paired allows | every Pod denies all ingress/egress unless an explicit policy allows it; because `NetworkPolicy` is directional, every cross-Pod path ships **both** halves — `allow-web-egress-to-server.yaml` (egress) pairs with `allow-ingress-to-web-and-server.yaml` (ingress); `allow-sandbox-egress-proxy-only.yaml` (egress) pairs with `allow-server-ingress-from-sandbox.yaml` (ingress) for the sandbox RPC callback; and the same egress rule's proxy leg pairs with `allow-egress-proxy-ingress-from-sandbox.yaml` (ingress) for the egress-proxy's side |
| DNS egress scoped to `kube-system` | `base/networkpolicy/allow-dns-egress.yaml` | `namespaceSelector` matches the `kube-system` namespace by its automatic `kubernetes.io/metadata.name` label, not `{}` (any namespace) — a same-labeled Pod outside `kube-system` cannot become an egress target |
| Namespace-per-org guidance | `base/namespace.yaml` | one namespace per tenant is the documented blast-radius boundary; this scaffold does not ship a controller that provisions those namespaces for you |
| Pod Security Admission | `base/namespace.yaml` (`warn`/`audit`), `overlays/production` (`enforce: restricted`) | production enforces the Kubernetes-defined "restricted" profile at admission time, independent of whether an individual manifest regresses |

## Why `keel-server` is pinned to one replica

`keel-server` is architecturally stateless and horizontally scalable (ARCHITECTURE §15), but
both `base/server/deployment.yaml` and the production overlay's
`patch-server-single-replica.yaml` hold it at exactly 1 replica today. Interactive runs and
pending tool-approval state are currently **process-local**
(`docs/OPERATIONS.md` "Current production-readiness limits"): a client's SSE stream or an
in-flight approval is only known to the Pod that admitted it. A second replica behind a
Service would load-balance new requests across both Pods, and a request that lands on the
"wrong" replica for an existing run/approval would silently lose it — worse than refusing to
scale at all. Raise this only once durable interactive run/approval coordination lands
(`docs/ROADMAP.md` M3.3 exit gate "Server restart does not lose an admitted run or pending
approval"; M3.6 exit gate "restart/scale-out preserves run and approval ownership") — update
both the base default and the production patch together so they cannot drift apart.

`replicas: 1` alone is not sufficient to keep this logically single-instance during an
*upgrade*: Kubernetes' default `RollingUpdate` strategy (25% `maxSurge`/`maxUnavailable`)
rounds `maxSurge` up to 1 even at a single replica, so it starts the new-revision Pod *before*
terminating the old one — an old and new `keel-server` would run concurrently for the
duration of the rollout, which is exactly the process-local-state hazard the replica pin
exists to avoid. `base/server/deployment.yaml` therefore also sets `strategy: type:
Recreate`, which terminates the old Pod before creating the new one, so old and new
`keel-server` Pods are never scheduled at once — at the cost of a brief (readiness-probe-
bounded) request gap during upgrades instead of the load-balancer-level overlap a rolling
update would otherwise allow (`docs/upgrade-rollback.md`). `scripts/validate_manifests.py`
and `tests/unit/test_deploy_k8s_manifests.py` enforce both `replicas: 1` and this
non-overlapping strategy against the base manifest text and the rendered base/production
Kustomize output, so neither can silently regress.

`keel-worker` has no such constraint: arq gives each job to exactly one worker and coordinates
the `scheduler_tick` cron across replicas via a Redis lock, so it scales freely
(`docker-compose.yml` comments) — the production overlay raises its replica floor
independently of `keel-server`.



An earlier version of this scaffold granted `keel-server`'s own ServiceAccount RBAC to
`create`/`get`/`list`/`watch`/`delete` `Jobs` and read `Pods`/`Pod` logs, reasoning that this
was "narrow" because it was scoped to one namespace. It was removed: a ServiceAccount that can
create arbitrary `Job`/`Pod` specs can mount arbitrary Secrets/PVCs in that namespace, run as
arbitrary images, and — combined with `keel-server`/`keel-worker` being the processes whose
execution-environment wiring can end up running untrusted, model-chosen shell commands
in-process (see "Isolation levels" below) — gives an attacker who reaches tool execution a
direct path to indirect Secret/PVC/ServiceAccount exfiltration via a crafted Job spec.
"Scoped to a namespace" is not the same as "safe" when the namespace itself holds the
Postgres/Redis credentials and connector tokens.

Standing up real per-run sandbox Job creation safely requires one of:

- a **dedicated sandbox-controller** Deployment, in its own namespace, holding the Job-create
  RBAC instead of `keel-server` — `keel-server` would call that controller over an internal,
  authenticated API rather than the Kubernetes API directly; or
- the same RBAC, but gated by an **admission policy** (Kyverno, OPA Gatekeeper, or
  Kubernetes' built-in `ValidatingAdmissionPolicy`) that restricts exactly which Job shapes
  may be submitted (fixed image allow-list, no Secret/PVC mounts beyond the ephemeral worktree
  pattern in `base/sandbox/job-template.yaml`, mandatory `runAsNonRoot`/dropped-capabilities,
  etc.), so a compromised `keel-server` cannot submit an arbitrary Job even with the RBAC.

Neither exists yet, and neither is the same thing as the `keel-sandbox` RPC service described
below: `base/sandbox/job-template.yaml` documents the target Job shape for whichever
Job-creation mechanism is eventually built; it remains excluded from the Kustomize build for
that reason, not merely because it is per-run, and no manifest here claims sandbox Jobs are
dynamically created — they are not.

## Isolation levels: what "sandbox" means today vs. the target

- **Today (current fidelity, see [`docs/OPERATIONS.md`](../../../docs/OPERATIONS.md) and
  [`docs/STATUS.md`](../../../docs/STATUS.md)):** `keel-server` and `keel-worker` both build
  their tool-execution environment through
  `keel_core.tools.wiring.build_service_execution_environment`, which **defaults to**
  `execution_backend=sandbox` — an authenticated RPC client
  (`keel_core.tools.rpc.SandboxExecutionEnvironment`) that must reach a `keel-sandbox` service
  at `KEEL_SANDBOX_URL` (default `http://keel-sandbox:8090`). That service boundary exists in
  code (`packages/keel-sandbox/src/keel_sandbox/service.py`) and wiring fails **closed** when
  it is unreachable or unauthenticated — but neither `docker-compose.yml` nor this
  `deploy/k8s` scaffold deploys a `keel-sandbox` Deployment/Service yet. Deployed as-is,
  `keel-server`/`keel-worker` start and serve `/health`/`/readiness` normally, but every
  tool-execution call (shell, read, write, edit, list, glob, grep) fails closed
  ("unavailable") until a real, reachable `keel-sandbox` exists — an intentional fail-closed
  gap, not a hidden regression. The only opt-out is explicitly setting
  `KEEL_EXECUTION_BACKEND=unsafe-local-dev` together with
  `KEEL_TRUSTED_PREVIEW_ALLOW_UNSAFE_EXECUTION=true` (neither set by this scaffold's
  ConfigMap), which restores unconditional in-process execution for a trusted single-org
  preview only. This — either the unreachable-by-default RPC path or the explicit in-process
  opt-out — is exactly why `keel-server`/`keel-worker` carry no ServiceAccount token and no
  RBAC: see "Sandbox Job creation is not wired up" above. The per-run sandbox **Job** template
  (`base/sandbox/job-template.yaml`) is a separate, still-dormant concept for a possible future
  execution-backend implementation reached through a sandbox-controller; it is not the
  `keel-sandbox` RPC service above, and neither is wired up or dynamically created today.
  Because this scaffold ships no `keel-sandbox` Service, there is also no NetworkPolicy allow
  rule yet for control-plane egress to it — `base/networkpolicy/allow-control-plane-egress.yaml`
  deliberately excludes private-CIDR destinations, so it would not permit that traffic even if
  the Service existed; standing up `keel-sandbox` in this namespace needs a new, explicit
  allow rule alongside it, not an assumption that today's NetworkPolicies already cover it.
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

## Why `keel-scheduler` is not deployed

`packages/keel-scheduler/src/keel_scheduler/main.py` is currently a stub — it logs one line
and returns. A Kubernetes `Deployment` running that command would crash-loop forever (the
container exits almost immediately, Kubernetes restarts it, repeat). This scaffold does not
ship it as an active resource; `base/scheduler/deployment.example.yaml` is a dormant template
for once the entrypoint is a real long-lived, leader-elected loop (M3.8 —
`docs/ROADMAP.md`). Scheduling today happens inside `keel-worker`'s own `scheduler_tick` cron,
coordinated across replicas via a Redis lock (ADR-0006).

## NetworkPolicy caveats

Standard `networking.k8s.io/v1` `NetworkPolicy` cannot select traffic by Service name/DNS for
out-of-cluster endpoints — only by `ipBlock` CIDR, `podSelector`, or `namespaceSelector`. Since
managed Postgres/Redis/object storage are typically outside the cluster, the CIDR-specific
rule for them lives in a separate, dormant template
(`base/networkpolicy/allow-control-plane-egress-managed-data.example.yaml`) you must copy, fill
in, and add to your own overlay — `base/networkpolicy/allow-control-plane-egress.yaml` itself
ships placeholder-free (broad, private-range-excluding HTTPS only) so it is safe to apply
as-is. If your CNI supports FQDN-based policies (e.g. Cilium `CiliumNetworkPolicy`), prefer
that for materially tighter control instead of the IP-range allow.

**NetworkPolicy is directional.** An egress allow on the source Pod and an ingress allow on
the destination Pod are both required — one without the other is silently dropped by
`default-deny-all.yaml`. This scaffold ships both halves for every cross-Pod path it defines:
`allow-web-egress-to-server.yaml` / `allow-ingress-to-web-and-server.yaml`;
`allow-sandbox-egress-proxy-only.yaml` / `allow-server-ingress-from-sandbox.yaml` for the
sandbox RPC callback; and the same egress rule's proxy leg /
`allow-egress-proxy-ingress-from-sandbox.yaml` for the sandbox-to-egress-proxy path — the
latter is shipped active even though no `keel-egress-proxy` Deployment exists in this scaffold,
because its `podSelector` matches zero Pods until your platform team labels one
`app.kubernetes.io/component: egress-proxy`, so it costs nothing to ship now and avoids an
easy-to-forget manual step later. If you add a new cross-Pod path, add both halves too —
`scripts/validate_manifests.py`'s directionality check only knows about the paths this
scaffold itself defines.

**DNS egress is scoped to `kube-system` by name**
(`allow-dns-egress.yaml`'s `namespaceSelector` matches the `kubernetes.io/metadata.name:
kube-system` label every namespace gets automatically since Kubernetes 1.21), not
`namespaceSelector: {}` (any namespace) — the broader form would let every Pod in this
namespace reach any same-labeled Pod (`k8s-app: kube-dns` or `app.kubernetes.io/name: coredns`)
in *any* namespace, not just the cluster's real DNS. If your cluster's DNS Pods carry a
different label, add a third `to:` entry scoped to `kube-system` rather than widening the
`namespaceSelector`.

Since this scaffold grants **no RBAC** to `keel-server`/`keel-worker` (see above), it also adds
**no NetworkPolicy egress rule to the Kubernetes API server** — there is nothing for these
workloads to call it for. Do not add one unless a real, narrowly-scoped RBAC grant is added
alongside it.

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
manifests. `KEEL_API_KEYS` is required in that Secret (see the table above) — there is no
default that makes an empty value safe.

## Storage: Git PVC and object storage are dormant, not wired up

`base/datastores/git-pvc.example.yaml` and `base/datastores/objectstorage-secret.example.yaml`
are dormant templates, not active Kustomize resources. No current application code
(`packages/keel-core/src/keel_core/config.py` has no Git-storage or object-storage setting)
mounts or consumes them — the "managed code projects" Git-storage design (design doc §2.3) is
not implemented yet. Shipping an unconsumed PVC/Secret as an applied resource would be a
nonfunctional claim of capability that does not exist; see
[`docs/storage-topology.md`](storage-topology.md) for the full picture and what promotes these
back to active resources.

## Pending gates (not yet true; do not represent otherwise)

These are the concrete items still required before this scaffold can back a hostile
multi-tenant deployment. They mirror
[`docs/STATUS.md`](../../../docs/STATUS.md) "Critical and high blockers" and
[`docs/ROADMAP.md`](../../../docs/ROADMAP.md) M3.3/M3.6/M3.8, and the design doc's own gated
rollout language (§8.4, §9):

1. **Real sandbox integration, deployed.** `packages/keel-sandbox/src/keel_sandbox/service.py`
   is a real, authenticated RPC execution boundary that `keel-server`/`keel-worker` call by
   default (`execution_backend=sandbox`), and wiring fails closed when it is unreachable or
   unauthenticated (see "Isolation levels" above) — but neither `docker-compose.yml` nor this
   scaffold deploys a `keel-sandbox` Deployment/Service, and no NetworkPolicy here permits
   control-plane egress to one yet. The **separate** per-run sandbox `Job` template
   (`base/sandbox/job-template.yaml`) remains unvalidated against a live agent runtime; Job
   creation/deletion and worktree materialization described in the design doc do not exist in
   code.
2. **A sandbox-controller (or admission policy) for Job creation.** `keel-server` holds no RBAC
   to create Jobs (see "Sandbox Job creation is not wired up" above) — until a narrowly-scoped
   controller or admission policy exists, there is no safe, automated way to submit
   `base/sandbox/job-template.yaml` at all. This is unrelated to and does not substitute for
   gate 1 above: standing up the `keel-sandbox` RPC service does not require Job-create RBAC,
   only a reachable Service and a new, explicit NetworkPolicy egress rule.
3. **Stronger-isolation runtime, wired end-to-end.** `runtimeClassName` support is a manifest
   hook only; no gVisor/Kata/microVM runtime has been qualified, load-tested, or made the
   default for any real workload.
4. **Identity and tenancy enforcement.** RLS is not a hard tenant boundary yet (the application
   DB role can bypass it); there is no user/org model, so "namespace-per-org" is a Kubernetes
   convention this scaffold documents, not an automated per-tenant provisioning/authorization
   system.
5. **Durable interactive run/approval coordination.** This is *why* `keel-server` is pinned to
   1 replica (see "Why `keel-server` is pinned to one replica" above) rather than scaled out
   like the architecture target — M3.3/M3.6 exit gates in `docs/ROADMAP.md` ("Server restart
   does not lose an admitted run or pending approval"; "restart/scale-out preserves run and
   approval ownership") are not yet met.
7. **Egress allow-list proxy.** `allow-sandbox-egress-proxy-only.yaml` /
   `allow-egress-proxy-ingress-from-sandbox.yaml` assume a `keel-egress-proxy` workload exists;
   this scaffold does not ship that proxy's Deployment or its allow-list/registry-mirror logic
   (design doc §9 "Egress") — only the NetworkPolicy pair that would constrain it once it
   exists.
8. **A real, long-lived `keel-scheduler`.** Its current entrypoint is a stub (see "Why
   `keel-scheduler` is not deployed" above); the leader-elected cron service in
   `docs/ROADMAP.md` M3.8 does not exist in code yet.
9. **Vertical/observability integration.** OTel, Prometheus, and Langfuse are referenced in
   `docs/slo-alerting.md` as targets; no metrics/alerting stack ships in this scaffold, and
   reconciled usage/100%-required-run-tracing (M3.8 exit gate) is not implemented.
10. **Backup/restore/DR drills.** `docs/backup-restore-dr.md` documents the intended procedure
    and RPO/RTO targets; no drill has been executed against a real cluster, and no automation
    exists to run one.
11. **NetworkPolicy enforcement verification.** As noted above, this scaffold cannot verify a
    given cluster's CNI actually enforces the policies it ships — that requires a live cluster
    test this repo's cluster-free validation deliberately does not perform.

Cloud-mode fail-closed auth enforcement (`Settings.cloud_mode` / `app.state.auth_required` /
`packages/keel-server/src/keel_server/auth.py` `authenticate()`) landed in application code as
part of M3.3 and is no longer a pending gate — `KEEL_CLOUD_MODE: "true"` in
`base/configmap-app.yaml` is now an active control, not a value shipped ahead of enforcement;
see the "What is enforced by these manifests" table above.

Until (1)–(5) are closed, treat any Kubernetes deployment of this scaffold as, at best, the
same **trusted single-org preview** tier as the Compose `full` profile
([`docs/OPERATIONS.md`](../../../docs/OPERATIONS.md)) — not a hostile multi-tenant platform.
