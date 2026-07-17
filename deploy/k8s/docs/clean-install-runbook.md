# Clean-install runbook

Aligned to the M3.8 exit gate "repeatable clean install and upgrade"
(`docs/ROADMAP.md`). This is the target sequence for a **trusted single-org** installation —
it does not by itself satisfy the hostile multi-tenant gates in
[`security-model.md`](security-model.md) "Pending gates".

## Prerequisites

- A Kubernetes cluster (1.27+) with a `NetworkPolicy`-enforcing CNI and Pod Security Admission
  enabled (both are true by default on current EKS/GKE/AKS and most self-managed
  distributions with a CNI like Calico/Cilium installed).
- A managed Postgres 16+ with `pgvector` available, and a managed Redis 7+.
- Container images for `keel-app` (server/worker share one image per `docker-compose.yml`) and
  `keel-web`, pushed to a registry your cluster can pull from.
- A real, random value to use as `KEEL_API_KEYS` (`key:role` pairs, e.g.
  `openssl rand -hex 32` piped into a `key:admin` pair) — required, see step 2.
- `kubectl` and `kustomize` (or `kubectl kustomize`) available to whoever runs the install.
- The S3-compatible object store and Git-storage `StorageClass` below are **not** required for
  this install sequence: `base/datastores/git-pvc.example.yaml` and
  `base/datastores/objectstorage-secret.example.yaml` are dormant templates that no current
  application code consumes (see `docs/storage-topology.md`). Skip them unless you are
  standing up the not-yet-implemented managed-code-projects Git-storage feature.

## Steps

1. **Fork/copy this scaffold's overlay** you intend to use (`overlays/production` or a copy
   named for your environment) into your own private deployment repository — do not commit
   filled-in secrets into this repository.

2. **Fill in placeholders:**
   - `base/secret-app.example.yaml` → copy to `secret-app.yaml` (your overlay, not this repo).
     **Set a real, non-empty `KEEL_API_KEYS`** with at least one valid `key:role` entry (role
     one of `viewer`/`operator`/`admin`) — empty/missing/malformed leaves `parse_api_keys`
     with no valid entries, and with `KEEL_CLOUD_MODE: "true"` forced below, that makes every
     request fail **closed** with 503 (`Settings.cloud_mode` / `app.state.auth_required`,
     `packages/keel-core/src/keel_core/config.py`; format matches
     `packages/keel-server/src/keel_server/auth.py` `parse_api_keys`) rather than falling back
     to the local implicit-admin open mode. Fill the rest of the real values or point an
     external-secret operator at this name/shape. Leave `KEEL_CLOUD_MODE: "true"` in
     `base/configmap-app.yaml` unchanged — do not set it to `"false"`.
   - `base/datastores/postgres-external-service.example.yaml` /
     `redis-external-service.example.yaml` → copy, set the real `externalName` (or replace
     with your operator's Service if self-hosting in-cluster).
   - Every Deployment's `image:` → your pinned digest (not `:latest`).
   - `base/networkpolicy/allow-control-plane-egress-managed-data.example.yaml` → copy, fill
     the managed-services CIDR, add it to your overlay (the shipped
     `allow-control-plane-egress.yaml` needs no changes — it ships placeholder-free).
   - `base/networkpolicy/allow-ingress-to-web-and-server.yaml` → fill your ingress
     controller's namespace.
   Add your filled-in copies to your overlay's own `kustomization.yaml resources:` /
   `patches:` — never re-add them to this repo's tracked `.example.yaml` files.
   Do **not** promote `base/datastores/git-pvc.example.yaml` or
   `objectstorage-secret.example.yaml` unless you are also wiring the (not yet implemented)
   application code that would consume them.

3. **Validate before applying** (cluster-free):
   ```powershell
   python deploy\k8s\scripts\validate_manifests.py
   kubectl kustomize <your-overlay> | Out-Null   # confirms it renders
   ```

4. **Create the namespace and apply NetworkPolicy/ConfigMap/Deployments:**
   ```powershell
   kubectl apply -k <your-overlay> --dry-run=server   # optional server-side pre-check
   kubectl apply -k <your-overlay>
   ```
   There is no RBAC to apply — this scaffold intentionally grants none of its own
   ServiceAccounts any Kubernetes API access (see `docs/security-model.md`).

5. **Run the migration Job** (mirrors `keel-migrate` in `docker-compose.yml`; not included as
   a standing manifest here since it is a one-shot Job your overlay should add, e.g.
   `command: ["alembic", "upgrade", "head"]` against the same image, run to completion before
   step 6).

6. **Wait for the control plane to become ready:**
   ```powershell
   kubectl rollout status deployment/keel-server -n keel
   kubectl rollout status deployment/keel-worker -n keel
   kubectl rollout status deployment/keel-web -n keel
   ```
   `keel-scheduler` is not part of this list: this scaffold does not deploy it (its entrypoint
   is currently a stub — see `docs/security-model.md` "Why `keel-scheduler` is not deployed").
   Scheduling happens inside `keel-worker`'s own cron tick. Do not scale `keel-server` beyond
   1 replica (see `docs/security-model.md` "Why `keel-server` is pinned to one replica") — only
   `keel-worker` is meant to be scaled.

7. **Smoke-test:**
   ```powershell
   kubectl port-forward svc/keel-server 8000:8000 -n keel
   Invoke-RestMethod http://localhost:8000/health
   Invoke-RestMethod http://localhost:8000/readiness
   ```
   Confirm an unauthenticated request to a non-health route is rejected (proves
   `KEEL_API_KEYS` actually took effect) before treating this as installed.

8. **Confirm hardening took effect:**
   ```powershell
   kubectl get pods -n keel -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.spec.securityContext.runAsNonRoot}{'\n'}{end}"
   kubectl get pods -n keel -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.spec.automountServiceAccountToken}{'\n'}{end}"
   kubectl get networkpolicy -n keel
   kubectl get rolebinding,clusterrolebinding -n keel   # expect none for this scaffold's own ServiceAccounts
   ```

9. **Only after (1)-(8):** integrate the real sandbox service (including a narrowly-scoped
   sandbox-controller or admission policy for Job creation — see `docs/security-model.md`
   "Sandbox Job creation is not wired up"), identity/tenancy enforcement, and observability
   stack per `security-model.md`, `slo-alerting.md`, and `docs/ROADMAP.md` M3.3/M3.6/M3.8
   before exposing this to untrusted users or multiple organizations.

## Uninstall

```powershell
kubectl delete -k <your-overlay>
```

Does not delete the managed Postgres/Redis or any object storage/Git PVC you separately
provisioned (managed databases are outside Kubernetes entirely, and
`persistentVolumeReclaimPolicy` on your StorageClass governs any PVC) — this is intentional to
prevent accidental data loss. Delete those explicitly and deliberately if a full teardown is
intended.
