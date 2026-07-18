# Keel implementation status

> **Snapshot:** 2026-07-19 · **Branch:** `main` · **HEAD:** `810a64c`
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
surface. It is **not** a multi-user product: there is no browser login flow, no real execution
sandbox, and the runtime database role still owns the schema. Most product surfaces are a
**trusted single-operator local preview**, not a production or multi-tenant deployment.

## Verified baseline (M0 green baseline)

Measured on `main` at `810a64c`:

- **Standard stack launches.** `docker compose -f docker-compose.yml --profile dev up -d --build`
  starts cleanly; server `/readiness` returns `true`, the worker arq health check succeeds, and
  the web surface returns `200` on `/health` and `/`.
- **Backend CI green.** `ruff check`, `ruff format --check`, `mypy`, and the OpenAPI
  compatibility check all pass.
- **Backend tests.** Non-integration suite: **1795 passed / 1 skipped**. Full Postgres/Redis
  integration suite: **385 passed / 385**.
- **Frontend tests.** **116 passed.**
- **Images build.** Both the `app` and `web` container images build.
- **Git JIT auth fix.** The Git just-in-time credential bug is fixed on `main` — JIT
  credentials are sent as an `Authorization` header (commit `810a64c`).

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
| Connectors: Gmail native, IM routing (OneBot/Telegram) | ✓ | ✓ | ✓ | ~ | Gmail OAuth/read/status/send preview works; IM message routing exists, IM durable routing + admin UI do not. |

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

The Compose `dev` and `full` profiles are an **explicit, trusted, single-operator local
preview**. They are **not production-safe** and must not be exposed to untrusted networks.

- Execution uses the opt-in `unsafe-local-dev` backend with a dedicated execution volume and
  **shell execution disabled**; there is **no real `keel-sandbox` service deployed**.
- The runtime database role owns the schema/database and can bypass RLS.
- The server data-plane scope is the single-operator preview scope; there is no browser login.

No level of green tests changes this: a real isolated sandbox and a non-owner runtime DB role
are **not** deployed on `main`.

## Patch / Draft PR foundation (off-main, not a current product feature)

A controlled patch-proposal foundation is stable on branch `feat/future-patch-pr`
(commit `c97fc46`, later synced to the green main baseline at `64c49ce`):

- Migration `0019`, plus models / store / bundle / generation / approval / writeback /
  coordinator modules.
- `ruff` + `mypy` clean; **34 unit tests** and **2 Postgres integration tests** pass.

It is **not merged into `main`** and has **no API/SDK, no worker jobs, no dispatch outbox, no
approved/expiry reconciler, and no UI**. Therefore it is **not a current product capability**
and must not be described as one. Merging it is milestone **M2**.

## Critical blockers (before any multi-user or production exposure)

1. **RLS bypass:** the runtime DB role owns the schema and can bypass RLS (needs a non-owner
   role with enforced RLS — **M3A**).
2. **No real sandbox:** shell/file execution has no deployed isolated backend; only
   `unsafe-local-dev` exists (**M3B**).
3. **No browser login:** identity/org/agents/grants APIs exist but there is no browser OIDC
   authorization-code flow, so no real multi-user product scenario (**M7**).
4. **No review UI / IM admin UI:** the review API+worker and IM routing exist headless (**M5/M7**).
5. **Event/data lifecycle:** event versions exist without upcasters, and erasure closure is
   incomplete (**M8**).
6. **Production operations:** no production scheduler service/leadership, OTel/metrics/SLOs,
   or backup/restore/DR drills (**M9**).

## Next work

Follow [Roadmap](./ROADMAP.md): M1 (this commit) → M2 Patch Foundation Merge → M3A Runtime DB
Role/RLS and M3B Real Sandbox (parallel safety gates) → M4 Patch API/worker/outbox → M5 Patch
UI + approval → Draft PR e2e → M6 Personal Agent + Calendar → M7 Browser OIDC + admin/review/IM
UI → M8 Event/lifecycle + erasure closure → M9 Production delivery/scale/OTel/DR.
