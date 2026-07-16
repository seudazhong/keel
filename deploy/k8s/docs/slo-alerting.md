# SLOs and alerting

**Status: targets and integration points documented; no metrics/alerting stack ships in this
scaffold.** `docs/ARCHITECTURE.md` §14 lists OpenTelemetry, Langfuse, and Prometheus as the
target observability stack; current fidelity is "deterministic Memory/Knowledge evals and
basic tracing/cost fields exist" — full cross-service OTel, Prometheus/SLO coverage,
reconciled usage, and production dashboards remain below that target
(`docs/OPERATIONS.md`, `docs/ROADMAP.md` M3.8).

## Proposed SLOs (targets, not measured yet)

| Service | SLI | Target SLO |
|---|---|---|
| `keel-server` | `/health`, `/readiness` success rate | 99.9% over 30 days |
| `keel-server` | p95 API latency (non-streaming routes) | < 500ms |
| `keel-worker` | job pickup latency (enqueue → lease) | p95 < 5s |
| `keel-worker` cron tick | schedule fire accuracy (actual vs. `next_run_at`) | within 60s, exactly once per tick — today's mechanism (see below), not a separate `keel-scheduler` service |
| sandbox Jobs (once real) | run completion within `activeDeadlineSeconds` | ≥ 99% of runs complete or fail cleanly before the deadline (no orphaned Jobs) |

## Alerting hooks this scaffold anticipates but does not wire up

- **Probe failures:** `keel-server`/`keel-web` `readinessProbe`/`livenessProbe` failures are
  visible via `kubectl get events` and any Kubernetes-native alerting
  (e.g. `kube-state-metrics` + Prometheus `Alertmanager`) you already run — no custom
  ServiceMonitor/PodMonitor ships here, since no in-cluster Prometheus/OTel Collector ships
  here either.
- **Scheduler double-run:** this scaffold does not deploy `keel-scheduler` at all today (its
  entrypoint is a stub — see `docs/security-model.md` "Why `keel-scheduler` is not deployed");
  scheduling is a `keel-worker` cron tick coordinated via a Redis lock (`docker-compose.yml`
  comments, ADR-0006), which is itself the double-fire guard. Once a real, long-lived
  `keel-scheduler` exists (`base/scheduler/deployment.example.yaml`), it should use
  `strategy: Recreate` and an alert on "more than one `keel-scheduler` Pod Running" is a cheap,
  high-value addition.
- **Sandbox Job pileup:** sandbox Jobs are not created by anything today (see
  `docs/security-model.md` "Sandbox Job creation is not wired up"). Once a sandbox-controller
  exists, alert on Job count exceeding expected concurrent-run ceilings (a proxy for either a
  controller bug failing to clean up Jobs, or quota enforcement failing) —
  `ttlSecondsAfterFinished` and `activeDeadlineSeconds` in `base/sandbox/job-template.yaml`
  bound the blast radius but do not replace monitoring.
- **NetworkPolicy drift:** `scripts/validate_manifests.py` catches manifest-level regressions
  (e.g. an accidentally widened default-deny, or a missing directional egress/ingress half) in
  CI; it cannot detect a live cluster failing to *enforce* the policies it renders — that
  requires a runtime check against a real CNI, out of scope for this cluster-free validator
  (see `docs/security-model.md` "Pending gates").

## Where to wire in a real stack

When OTel/Prometheus/Langfuse land (M3.8), the natural integration points are:

- an OTel Collector `Deployment`/`DaemonSet` + `Service` alongside `base/`, with `keel-server`/
  `keel-worker` (and `keel-scheduler` once it is a real service) exporting via
  `OTEL_EXPORTER_OTLP_ENDPOINT` in `base/configmap-app.yaml`;
- `ServiceMonitor`/`PodMonitor` CRDs (if using the Prometheus Operator) scraping `/metrics` on
  each Deployment, added as new resources under `base/observability/` (not present yet);
- Langfuse credentials following the same external-secret pattern as
  `base/secret-app.example.yaml` (`KEEL_LANGFUSE_*`, see `.env.example`).

None of this is implemented in this scaffold today; this document exists so the wiring points
are decided in advance rather than improvised later.
