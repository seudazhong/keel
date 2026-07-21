# SLOs and alerting

> **Status:** targets only; the scaffold ships no Prometheus, OTel Collector, dashboards, or alerts

## Candidate service indicators

| Area | Indicator |
|---|---|
| API | readiness success, error rate, non-streaming latency |
| Runs | admission-to-claim latency, terminal rate, lease loss, stranded dispatch |
| Jobs | queue age, claim latency, retries, exhausted jobs, cancellation latency |
| Routines | accepted occurrence age, missed terminalization, duplicate occurrence attempts |
| Effects | `unknown` age, reconciliation failures, duplicate-provider detection |
| Connectors | refresh/revoke/sync health, webhook rejects, rate limits, effect latency |
| Sandbox | readiness, RPC failures, namespace cleanup, execution timeout |
| Projects | sync age/failures, shared-storage errors, orphaned worktrees |
| Review/patch | request age, artifact failures, approval age, writeback reconciliation |
| Data | database saturation, RLS/readiness failure, backup/restore freshness |

## Candidate targets

Targets must be measured before release. Initial examples:

- API readiness success >= 99.9% over 30 days;
- p95 non-streaming API latency < 500 ms excluding provider calls;
- p95 admitted-run claim latency < 5 seconds;
- no accepted Routine occurrence silently lost;
- no duplicate confirmed external effect;
- no `unknown` effect older than the declared reconciliation SLO;
- backup age and restore drill within declared RPO/RTO.

## Required alerts

- readiness/liveness failures;
- runtime DB principal degradation;
- queue/outbox age;
- run/job lease-loss spikes;
- accepted Routine occurrence stuck;
- effect in `unknown` beyond threshold;
- connector auth/revoke failures;
- sandbox unavailable or cleanup backlog;
- project storage unavailable;
- review/patch artifact or writeback reconciliation failure;
- backup freshness/restore verification failure;
- unauthorized-access/approval-bypass audit events.

## Future wiring

The production profile should add:

- OTel Collector and OTLP export;
- Prometheus-compatible metrics;
- trace propagation through run, job, approval, effect, and outbox records;
- dashboards and Alertmanager/on-call integration;
- Langfuse or equivalent model/eval telemetry with privacy controls.

Kubernetes Events and probe status are useful diagnostics, not a complete observability system.
