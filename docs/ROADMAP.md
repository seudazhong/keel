# Keel roadmap

> **Updated:** 2026-07-19 · **Authority:** active execution sequence · **Baseline:** `main` `b885f0d`

This roadmap sequences **small, independently verifiable milestones**. Each milestone lists
explicit dependencies and **machine-verifiable exit gates**. No milestone is "implement all
future plans." Completion evidence lives in [Status](./STATUS.md), not in dated prose.

## Execution policy

- Ground truth is [Status](./STATUS.md). Merged code alone does not complete a milestone; the
  measurable exit gate must pass.
- The Compose `dev`/`full` stack is a **trusted single-operator local preview**. The M3A/M3B
  safety foundation is complete (non-owner runtime DB login + deployed isolated sandbox), but it
  is still never production-safe or multi-tenant and must not be exposed to untrusted networks.
- Safety gates **M3A** (runtime DB role/RLS) and **M3B** (real sandbox) are **complete**; their
  residual limits (single-operator OCI not microVM, bidirectional internal net, shell disabled,
  K8s example manifests) are tracked in [Status](./STATUS.md), not reopened as blockers.
- Code existing (C) or tests passing (T) never counts as a usable product scenario (P). A UI/e2e
  milestone closes only on a real end-to-end scenario.

## Milestones

### M0 — Green Baseline · **Complete**

**Goal:** a reproducible, green baseline on `main`.

**Exit gates (met at `810a64c`):**

- `docker compose -f docker-compose.yml --profile dev up -d --build` starts; server
  `/readiness` = `true`; worker arq health check succeeds; web `/health` and `/` return `200`.
- `ruff check`, `ruff format --check`, `mypy`, and the OpenAPI compatibility check pass.
- Backend non-integration: 1795 passed / 1 skipped. Full integration: 385/385.
- Frontend: 116 passed. `app` and `web` images build.
- Git JIT credential bug fixed (`Authorization` header).

### M1 — Truthful Baseline Docs · **Complete**

**Goal:** README/STATUS/ROADMAP describe exactly what `main` is, using the C/T/D/P maturity
scale, and correct stale claims while preserving the trusted-preview safety contract.

**Dependencies:** M0.

**Exit gates (met):**

- STATUS/README rate capabilities on C/T/D/P and never promote C/T to P.
- Stale claims removed/corrected (e.g. "no identity/onboarding", "static stub instead of React
  bundle") while the local-preview safety limits remain stated.
- The Patch/Draft PR foundation is documented with its correct merge status and **not** as a
  usable product feature.
- Markdown tracked-link check passes and `git diff --check` is clean.

### M2 — Patch Foundation Merge · **Complete**

**Goal:** land the patch-proposal foundation on `main` behind its full regression suite, with no
product-surface exposure yet.

**Dependencies:** M1.

**Exit gates (met at `2ae9dc0`):**

- Migration `0019` and the patch models/store/bundle/generation/approval (with generation-run
  recovery + lease guard)/trusted-writeback/coordinator modules are on `main`; `0019` actually
  applied in the standard Compose database with readiness/worker/web healthy.
- On merged `main`: `ruff` + `mypy` + OpenAPI checks clean; the patch/review targeted suite
  (**45 tests**) passes.
- The full baseline still passes post-merge: non-integration **1830 passed / 1 skipped**,
  integration **387 / 387**, frontend **116**, `app`/`web` images build.
- No patch UI, API, or worker dispatch is enabled (C/T foundation only; those are M4/M5).

### M3A — Runtime DB Role / RLS · **Complete**  *(safety gate)*

**Goal:** the runtime application role is a **non-owner** with enforced row-level security.

**Dependencies:** M1.

**Exit gates (met at `b885f0d`):**

- The data plane connects as the non-owner `keel_runtime_login` via a **passwordless URL +
  `0600` `PGPASSFILE`**; the login has `super`, `bypassrls`, `createrole`/`createdb`, schema
  ownership, identity-delete, erase-exec, and Alembic-DML privileges **all false**; RLS-bypass,
  DDL, and `SET ROLE` are denied.
- `/readiness` reports `runtime_db_principal = 'least-privilege (keel_runtime_login)'`.
- Migration `0020_runtime_role_hardening` provisions/verifies the role; the standard Compose
  startup runs `migrate → runtime-secret-init → provision` before server/worker.
- M3A targeted suite: **121 passed / 1 skipped**; role/RLS assertions in CI.

### M3B — Real Sandbox Deployment · **Complete**  *(safety gate)*

**Goal:** deploy a real isolated execution backend, replacing `unsafe-local-dev` for
shell/file execution.

**Dependencies:** M1.

**Exit gates (met at `b885f0d`):**

- A sandbox service runs shell/file execution out-of-process, reached over **authenticated
  HMAC**, running **nonroot / read-only rootfs / cap-drop** on an **internal-only network**,
  with **no database, Redis, or public-network** reachability.
- It provisions **per-scope files** and **denies shell**; `/readiness` reports `sandbox = ok`;
  the API/worker no longer executes untrusted shell in-process.
- M3B targeted suite: **69 + 10 targeted tests** pass; Playwright browser smoke **18 / 18**.

**Honest residuals (tracked, not blockers):** the Compose sandbox is a **single-operator OCI
container, not a microVM**; its internal network is **bidirectional**; **per-scope shell stays
disabled**; and the Kubernetes path is **example manifests** where an operator runs
`migrate`/`provision` themselves.

### M4 — Patch API / Worker / Outbox · **Next**

**Goal:** make the merged patch foundation operable end-to-end on the backend.

**Dependencies:** M2 (patch foundation merged); M3A/M3B safety foundation (complete). This is
the **single next mainline milestone**.

**Exit gates:**

- Patch API/SDK endpoints create/list/inspect patch proposals; OpenAPI compatibility check
  passes; new schema lands as migration `0021`.
- A durable patch worker job runs generation/writeback via the dispatch outbox with an
  approved/expiry reconciler.
- Integration tests cover admit → dispatch → reconcile (approved and expiry) with restart
  survival.

### M5 — Patch UI + Human Approval → Draft PR e2e

**Goal:** a real product scenario — a human reviews a proposed patch and approves it into a
GitHub **Draft PR**.

**Dependencies:** M4, and a review surface (M2/M4 backend + this UI).

**Exit gates:**

- A React patch/review surface lists proposals, shows diffs, and drives human approval.
- An approved proposal produces a Draft PR via the GitHub App writeback path.
- A Playwright/e2e test drives propose → approve → Draft PR against the preview stack.

### M6 — Personal Agent + Calendar

**Goal:** a useful single-operator personal-agent loop with Calendar as the second native
connector.

**Dependencies:** M3A/M3B safety posture; existing Agents/Memory/Connectors backend.

**Exit gates:**

- A local operator creates/selects a persisted Agent, edits its memory, grants
  Gmail/Calendar, and runs a useful inbox/meeting routine from the React UI.
- Calendar read/draft/create has least-scope consent and approval tests.
- Agent selection changes persona/memory/grants/tool policy without code edits.

### M7 — Browser OIDC + Org/Grant/Review/IM Admin UI

**Goal:** turn the tested identity/review/IM backends into real multi-user product surfaces.

**Dependencies:** M3A (RLS) and M3B (sandbox) closed; M5 review surface; identity/grants APIs.

**Exit gates:**

- A browser OIDC authorization-code flow signs a user in; no route depends on the
  single-operator preview scope.
- Two users and one shared team Agent pass isolation/grant tests through the UI.
- Org/grant admin, the review UI, and an IM admin UI are shipped and covered by e2e tests;
  IM durable routing is worker-owned and restart-safe.

### M8 — Event / Lifecycle Compatibility and Erasure Closure

**Goal:** replay and projection rebuilds survive schema evolution, and erasure is complete and
testable.

**Dependencies:** M3A durable boundaries; M7 multi-user data ownership.

**Exit gates:**

- An upcaster registry + fixtures for every historical event version rebuild identical
  projections; incompatible event changes cannot merge (CI gate).
- Seeded user data is removed from every documented store; rebuild cannot resurrect erased
  content; connector tokens are revoked/purged; erasure jobs are idempotent and observable.

### M9 — Production Delivery / Scale / OTel / DR

**Goal:** a supportable, observable, scalable cloud-native deployment.

**Dependencies:** M3A–M8.

**Exit gates:**

- Production React image and accurate deployment profiles with a real scheduler
  service/leadership; N-worker and multi-server topology proven under load/chaos.
- OTel traces/metrics/alerts/SLOs: 100% of required run traces and reconciled usage.
- Backup/restore and DR drill meet documented RPO/RTO; upgrade/rollback runbooks; no
  static-stub or in-process safety substitutions in production.

## Dependency summary

```
M0 ✓ → M1 ✓ → M2 ✓ → M4 → M5 → ─┐
        ├→ M3A ✓ ──────────────┤
        └→ M3B ✓ ──────────────┼→ M7 → M8 → M9
                  M6 (after M3A/M3B) ┘
```

- ✓ = complete (M0, M1, M2, M3A, M3B). **M4 is the single next mainline milestone.**
- M3A and M3B (complete) are the safety foundation required before M7 (multi-user exposure).
- M6 depends on the M3A/M3B safety posture but not on M4/M5.
- M5 depends on M4 (patch backend) and the review surface.

## Roadmap rules

- [Status](./STATUS.md) supplies completion evidence; dated plans do not.
- Safety gates M3A/M3B are complete; their residual limits are tracked, not bypassed, and do
  not authorize multi-tenant or untrusted-network exposure by themselves.
- The Patch foundation is merged on `main` (M2) as a C/T foundation only; it is not product
  usable until its API/worker (M4) and UI/e2e (M5) close.
- A milestone completes only on its measurable exit gate, never on merged code alone.
