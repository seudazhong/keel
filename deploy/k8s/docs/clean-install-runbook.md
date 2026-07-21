# Kubernetes scaffold install runbook

This is a scaffold integration sequence, not a complete production install. The active base does not
deploy the sandbox service required by application readiness.

## Prerequisites

- Kubernetes 1.27+ with enforced NetworkPolicy and Pod Security Admission;
- managed PostgreSQL 16 + pgvector;
- managed Redis 7;
- pinned `keel-app` and `keel-web` images;
- an RWX StorageClass for `keel-project-storage`;
- external secret management;
- `kubectl` and Kustomize.

## 1. Copy the overlay

Keep filled secrets, managed-service addresses, ingress policy, and image digests in a private
deployment repository. Do not edit checked-in example secrets with real values.

## 2. Provide required secrets and services

Create private copies of:

- `secret-app.example.yaml`;
- `secret-migration.example.yaml`;
- PostgreSQL/Redis external Service examples;
- managed-data NetworkPolicy example.

Configure:

- a scoped `key:role:org=...:agent=...` credential or an explicit bootstrap
  `key:admin:global` credential;
- least-privilege runtime DB URL;
- owner/migration DB URL;
- runtime DB password for the provision Job;
- sandbox RPC secret;
- provider/connector secrets;
- pinned images and service addresses.

Do not disable cloud mode.

## 3. Provide the execution plane

Before expecting `/readiness` to pass, add a real `keel-sandbox` Service/Deployment or the future
sandbox controller that implements the authenticated execution contract.

It must:

- share the configured RPC secret;
- be reachable only over explicit internal policy;
- hold no control-plane credentials;
- enforce the documented file/command isolation level;
- expose the signed readiness endpoint expected by server/worker.

The dormant Job template alone is not sufficient.

## 4. Render and validate

```powershell
python deploy\k8s\scripts\validate_manifests.py
kubectl kustomize <your-overlay> | Out-Null
kubectl apply -k <your-overlay> --dry-run=server
```

## 5. Migrate and provision

Run the owner-privileged migration and runtime-login provision Jobs before standing workloads:

```powershell
kubectl apply -f <your-overlay>\secret-migration.yaml -n keel
kubectl apply -f <your-overlay>\jobs-migrate-provision.yaml -n keel
kubectl wait --for=condition=complete job/keel-migrate -n keel --timeout=300s
kubectl wait --for=condition=complete job/keel-provision -n keel --timeout=120s
```

## 6. Apply workloads

```powershell
kubectl apply -k <your-overlay>
kubectl rollout status deployment/keel-server -n keel
kubectl rollout status deployment/keel-worker -n keel
kubectl rollout status deployment/keel-web -n keel
```

There is no separate scheduler rollout.

## 7. Verify

```powershell
kubectl port-forward svc/keel-server 8000:8000 -n keel
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/readiness
```

Confirm:

- runtime DB principal is least-privilege;
- Postgres and Redis are healthy;
- run queue/admission are ready;
- project storage is writable where review is enabled;
- sandbox is `ok`;
- an unauthenticated protected request is rejected;
- web can reach server;
- no application ServiceAccount has RBAC.

If sandbox is unavailable, the scaffold is incomplete; do not waive readiness.

## 8. Record deployment evidence

Before exposure, record:

- rendered manifest digest;
- image digests;
- migration head;
- readiness output;
- NetworkPolicy enforcement check;
- backup/restore drill reference;
- scale and upgrade test reference;
- enabled capability/worker set.

## Uninstall

```powershell
kubectl delete -k <your-overlay>
```

Managed PostgreSQL/Redis and retained project-storage data are external or governed by PVC reclaim
policy. Delete them only through an explicit data-retention decision.
