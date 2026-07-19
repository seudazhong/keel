# Keel implementation status

> **Snapshot:** 2026-07-19 · **Branch:** `main` · **HEAD:** `b885f0d`
> **Target:** [PRD](./PRD.md) · **Architecture fidelity:** [ARCHITECTURE](./ARCHITECTURE.md#0-implementation-status-and-fidelity) · **Active execution:** [ROADMAP](./ROADMAP.md)

This document is the authority for **what is true on `main` now**. It is rebuilt from directly
measured evidence, not from historical task ledgers or dated snapshots. When a document
conflicts with this one about current capability, this document wins.

## Maturity scale

Every capability is rated on four independent levels. A higher level never implies a lower
one automatically, and **code or passing tests (C/T) are never reported as a usable product
scenario (P)**.

| Level | Meaning |
|---|---|
| **C — Code** | Implementation exists on `main`. |
| **T — Tested** | Automated tests cover it and pass in CI. |
| **D — Deployable** | It starts and runs in the standard Compose local-preview stack. |
| **P — Product** | A real end-to-end user scenario works through a shipped surface, not just an API or a preview screen. |

`✓` = level met, `~` = partial/preview, `—` = not met.

## Summary

`main` has a **strong, tested, deployable single-operator agent engine** with a broad React
surface, now on a **completed safety foundation**: the data plane runs as a non-owner,
least-privilege runtime DB login with enforced RLS (**M3A**), and shell/file execution runs in a
deployed, authenticated, isolated sandbox service (**M3B**). It is still **not** a multi-user
product: there is no browser login flow, and the surfaces remain a **trusted single-operator
local preview**, not a production or multi-tenant deployment. Safety infrastructure being
complete (C/T/D) does **not** make the platform a usable multi-tenant product (P).

## Verified baseline (M0 green + M2 patch merge + M3A/M3B safety foundation)

Measured on `main` at `b885f0d`. The standard Compose stack runs, in one command, the ordered
startup `migrate → runtime-secret-init → provision → sandbox → server/worker/web`
(migration head `0020_runtime_role_hardening`):

- **Standard stack launches.** `docker compose -f docker-compose.yml --profile dev up -d --build`
  starts cleanly; server `/readiness` returns `true` with
  `runtime_db_principal = 'least-privilege (keel_runtime_login)'` and `sandbox = ok`; the worker
  arq health check succeeds; the web surface returns `200` on `/health` and `/`.
- **Backend CI green.** `ruff check`, `ruff format --check`, `mypy`, and the OpenAPI
  compatibility check all pass.
- **Backend tests.** Non-integration suite: **1924 passed / 2 skipped**. Full Postgres/Redis
  integration suite: **397 passed**.
- **M3A targeted (runtime DB least-privilege).** **121 passed / 1 skipped.** The runtime
  connects via a **passwordless URL + `0600` `PGPASSFILE`**; the runtime login has
  `super`, `bypassrls`, `createrole`/`createdb`, schema ownership, identity-delete,
  erase-exec, and Alembic-DML privileges **all false**; RLS-bypass, DDL, and `SET ROLE`
  attempts are denied.
- **M3B targeted (real sandbox).** **69 + 10 targeted tests pass** — the sandbox service is
  reached over **authenticated HMAC**, runs **nonroot / read-only rootfs / cap-drop** on an
  **internal-only network**, provisions **per-scope files**, **denies shell**, and has **no
  database, Redis, or public-network** reachability.
- **Patch/review targeted tests.** **45** patch/review targeted tests pass on merged `main`.
- **Frontend tests.** **116 passed.** Playwright browser smoke: **18 / 18**.
- **Images build.** Both the `app` and `web` container images build.
- **Git JIT auth fix.** The Git just-in-time credential bug is fixed on `main` — JIT
  credentials are sent as an `Authorization` header.

## Capability maturity

### Agent/data engine (backend)

| Capability | C | T | D | P | Notes |
|---|:--:|:--:|:--:|:--:|---|
| Durable sessions/runs/jobs/approvals/schedules | ✓ | ✓ | ✓ | ~ | Durable job/schedule/approval loops are usable in single-operator preview; multi-user run topology gates remain (see blockers). |
| Memory: core/archival, search, consolidation, evals | ✓ | ✓ | ✓ | ~ | Deterministic Memory evals pass; block/history editing UI is partial. |
| Knowledge Base RAG (lifecycle, hybrid retrieval, citations, taint) | ✓ | ✓ | ✓ | ~ | Full vertical slice with React management/search UI, single-operator preview. |
| Identity / org / agents / grants APIs | ✓ | ✓ | ✓ | — | REST + RBAC exist and are tested, but there is **no browser login flow**, so no end-to-end product scenario. |
| Projects / GitHub App / storage | ✓ | ✓ | ✓ | ~ | Backend + storage exist; product journey is preview-level. |
| Read-only code review API + worker | ✓ | ✓ | ✓ | — | Review generation runs server + worker side; **no review UI** ships. |
| Patch / Draft PR foundation (models/store/bundle/generation/approval/writeback/coordinator) | ✓ | ✓ | ✓ | — | Merged on `main` (M2); C/T foundation only. No API/SDK/worker/outbox/reconciler/UI (M4/M5). |
| Connectors: Gmail native, IM routing (OneBot/Telegram) | ✓ | ✓ | ✓ | ~ | Gmail OAuth/read/status/send preview works; IM message routing exists, IM durable routing + admin UI do not. |
| Runtime DB least-privilege role + enforced RLS (M3A) | ✓ | ✓ | ✓ | — | Data plane runs as non-owner `keel_runtime_login`; RLS/DDL/`SET ROLE` denied. Safety infrastructure, not a multi-tenant product. |
| Real isolated execution sandbox (M3B) | ✓ | ✓ | ✓ | — | Deployed HMAC-authenticated nonroot/read-only/cap-drop service, per-scope files, shell denied. Safety infrastructure, not a product surface. |

### Product surface (React)

| Capability | C | T | D | P | Notes |
|---|:--:|:--:|:--:|:--:|---|
| React app (Chat, Sessions, Jobs, Schedules, Approvals, Memory, Knowledge, Connectors, Observability) | ✓ | ✓ | ✓ | ~ | Compose serves the built React app; several pages are preview or depend on backend/login work not yet shipped. |
| i18n foundation | ✓ | ✓ | ✓ | ~ | Present and tested; not a complete localization. |
| Onboarding / first-run | ✓ | ✓ | ✓ | ~ | Local onboarding flow exists as a single-operator preview. |
| Agents / Projects UI | ✓ | ✓ | ✓ | ~ | Present; gated by missing identity/login and grant journeys. |
| Auth/workspace context (API key / bearer, org/Agent) | ✓ | ✓ | ✓ | — | In-memory/tab-scoped credential context exists; it does **not** implement a browser OIDC authorization-code flow. |

> **Correction of stale claims.** Earlier docs asserted Keel had "no real identity/onboarding"
> and served "a static stub rather than the React bundle." Those claims are **false on `main`**:
> identity/org/agents/grants APIs, a React onboarding flow, and Compose-served React delivery
> all exist. What is still missing is the **browser OIDC login flow** and multi-user product
> journeys — not the code.

## Trusted local-preview safety contract

The Compose `dev` and `full` profiles are a **trusted, single-operator local preview**. The
M3A/M3B safety foundation is now in place — the data plane runs as the non-owner
least-privilege runtime login, and shell/file execution runs in the deployed isolated sandbox —
but the preview is still **not a production or multi-tenant deployment** and must not be exposed
to untrusted networks. Honest residual limits:

- The Compose sandbox is a **single-operator OCI container** (nonroot, read-only rootfs,
  cap-drop, internal-only network), **not a microVM**; container isolation is weaker than a VM
  boundary.
- Sandbox networking is restricted to the internal service network but is **bidirectional**
  within it (not a one-way/egress-only boundary).
- **Per-scope shell execution remains disabled**; the sandbox provisions per-scope files and
  denies shell.
- The Kubernetes path is **example manifests**: an operator must run the `migrate` and
  `provision` steps themselves (the single-command ordering is Compose-only).

Safety infrastructure completing (C/T/D) does **not** by itself deliver a multi-tenant product
(P): there is still no browser login and no multi-user product journey.

## Patch / Draft PR foundation (merged on `main`, C/T foundation only — not product usable)

The controlled patch-proposal foundation is **merged on `main`** (milestone **M2**, complete):

- Migration `0019` (`0019_patch_proposals`), plus the patch models / store / bundle /
  generation / approval / writeback modules and the coordinator, including generation-run
  **recovery + lease guard** and **trusted writeback**.
- On merged `main`: `ruff` + `mypy` clean; the patch/review targeted suite (**45 tests**)
  passes, and the foundation is covered within the green non-integration and Postgres
  integration baselines above.

This is a **C/T foundation only**. It has **no Patch API/SDK, no worker jobs, no dispatch
outbox, no approved/expiry reconciler, and no UI** — those remain milestones **M4** (API /
worker / outbox / reconciler) and **M5** (UI + human approval → Draft PR e2e). Until then the
patch foundation is **not a usable product scenario (not P)** and must not be described as one.

## Critical blockers (before any multi-user or production exposure)

Resolved by the safety foundation: **RLS bypass** (M3A — the data plane now runs as the
non-owner least-privilege `keel_runtime_login` with RLS/DDL/`SET ROLE` denied) and **no real
sandbox** (M3B — shell/file execution runs in the deployed authenticated isolated sandbox).
Remaining:

1. **No browser login:** identity/org/agents/grants APIs exist but there is no browser OIDC
   authorization-code flow, so no real multi-user product scenario (**M7**).
2. **No review UI / IM admin UI:** the review API+worker and IM routing exist headless (**M5/M7**).
3. **Event/data lifecycle:** event versions exist without upcasters, and erasure closure is
   incomplete (**M8**).
4. **Production operations:** no production scheduler service/leadership, OTel/metrics/SLOs,
   or backup/restore/DR drills (**M9**).

## Next work

Follow [Roadmap](./ROADMAP.md): M0, M1, M2, **M3A, and M3B are complete** → **next: M4 Patch
API/worker/outbox** (migration `0021`) → M5 Patch UI + approval → Draft PR e2e → M6 Personal
Agent + Calendar → M7 Browser OIDC + admin/review/IM UI → M8 Event/lifecycle + erasure closure →
M9 Production delivery/scale/OTel/DR.
