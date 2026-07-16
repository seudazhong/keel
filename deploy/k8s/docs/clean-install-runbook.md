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
- An S3-compatible object store (bucket + credentials or workload identity).
- A durable, RWX-capable (or single-writer-safe) `StorageClass` for the Git PVC.
- Container images for `keel-app` (server/worker/scheduler share one image per
  `docker-compose.yml`) and `keel-web`, pushed to a registry your cluster can pull from.
- `kubectl` and `kustomize` (or `kubectl kustomize`) available to whoever runs the install.

## Steps

1. **Fork/copy this scaffold's overlay** you intend to use (`overlays/production` or a copy
   named for your environment) into your own private deployment repository — do not commit
   filled-in secrets into this repository.

2. **Fill in placeholders:**
   - `base/secret-app.example.yaml` → copy to `secret-app.yaml` (your overlay, not this repo),
     fill real values or point an external-secret operator at this name/shape.
   - `base/datastores/objectstorage-secret.example.yaml` → same pattern.
   - `base/datastores/postgres-external-service.example.yaml` /
     `redis-external-service.example.yaml` → copy, set the real `externalName` (or replace
     with your operator's Service if self-hosting in-cluster).
   - `base/datastores/git-pvc.yaml` → set `storageClassName` to a real, durable class.
   - Every Deployment's `image:` → your pinned digest (not `:latest`).
   - `base/networkpolicy/allow-control-plane-egress.yaml` → fill the managed-services CIDR.
   - `base/networkpolicy/allow-ingress-to-web-and-server.yaml` → fill your ingress
     controller's namespace.
   Add your filled-in copies to your overlay's own `kustomization.yaml resources:` /
   `patches:` — never re-add them to this repo's tracked `.example.yaml` files.

3. **Validate before applying** (cluster-free):
   ```powershell
   python deploy\k8s\scripts\validate_manifests.py
   kubectl kustomize <your-overlay> | Out-Null   # confirms it renders
   ```

4. **Create the namespace and apply RBAC/NetworkPolicy/ConfigMap first:**
   ```powershell
   kubectl apply -k <your-overlay> --dry-run=server   # optional server-side pre-check
   kubectl apply -k <your-overlay>
   ```

5. **Run the migration Job** (mirrors `keel-migrate` in `docker-compose.yml`; not included as
   a standing manifest here since it is a one-shot Job your overlay should add, e.g.
   `command: ["alembic", "upgrade", "head"]` against the same image, run to completion before
   step 6).

6. **Wait for the control plane to become ready:**
   ```powershell
   kubectl rollout status deployment/keel-server -n keel
   kubectl rollout status deployment/keel-worker -n keel
   kubectl rollout status deployment/keel-scheduler -n keel
   kubectl rollout status deployment/keel-web -n keel
   ```

7. **Smoke-test:**
   ```powershell
   kubectl port-forward svc/keel-server 8000:8000 -n keel
   Invoke-RestMethod http://localhost:8000/health
   Invoke-RestMethod http://localhost:8000/readiness
   ```

8. **Confirm hardening took effect:**
   ```powershell
   kubectl get pods -n keel -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.spec.securityContext.runAsNonRoot}{'\n'}{end}"
   kubectl get networkpolicy -n keel
   ```

9. **Only after (1)-(8):** integrate the real sandbox service, identity/tenancy enforcement,
   and observability stack per `security-model.md`, `slo-alerting.md`, and `docs/ROADMAP.md`
   M3.3/M3.6/M3.8 before exposing this to untrusted users or multiple organizations.

## Uninstall

```powershell
kubectl delete -k <your-overlay>
```

Does not delete the managed Postgres/Redis/object storage or the Git PVC's backing volume by
default (`persistentVolumeReclaimPolicy` on your StorageClass governs the PVC; managed
databases are outside Kubernetes entirely) — this is intentional to prevent accidental data
loss. Delete those explicitly and deliberately if a full teardown is intended.
