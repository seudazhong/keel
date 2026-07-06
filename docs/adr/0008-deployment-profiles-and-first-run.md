# ADR-0008 — Deployment profiles & first-run

**Status:** Accepted · **Date:** 2026-07-06 · **Resolves:** PRD §13 Q3, Q4, Q5 · **Related:** ADR-0002, ADR-0006, ARCHITECTURE §15

## Context
Keel targets one-command self-hosting (DR-1) across very different footprints: a developer's inner loop, a full production stack, and a single-user/offline CLI. The PRD leaves three deployment questions open: whether MinIO is always bundled (Q3), whether the `lite` single-container/SQLite mode is first-class (Q4), and whether a local model ships for a zero-key first run (Q5).

## Decision
Four Compose **profiles**, with `lite` as a distinct build target:

| Profile | Services | Purpose |
|---|---|---|
| **`lite`** | one `keel` container: embedded `keel-core` + **SQLite** + local FS + in-process scheduler/executor | CLI / offline / single-user |
| **`dev`** | `keel-server`, `keel-worker`, `keel-web`, `postgres(pgvector)`, `redis` | fast inner loop |
| **`full`** | `dev` + `keel-scheduler`, `keel-sandbox`, `minio`, `langfuse(+clickhouse)`, `otel-collector`, `prometheus`, `keel-embed`, `adapters/*` | production |
| **`demo`** (overlay) | `full`/`dev` + **`ollama`** + seed data | zero-key, one-command showcase |

- **Q3 — MinIO is profile-gated:** present only in `full`; `dev`/`lite` use a local volume/FS for blob spill and artifacts. The object-store interface is identical (S3 API vs FS driver).
- **Q4 — `lite` is first-class and CI-tested**, but explicitly **not** the scale target. Documented caveats: **in-process** scheduler (no leader election / cross-node at-most-once), **reduced isolation** (in-process `bubblewrap`/`nsjail` or a trust-reduced toolset instead of the `keel-sandbox` service), single writer (SQLite). Feature parity for the agent loop, tools, memory, skills, and MCP is maintained.
- **Q5 — a local model is optional, via the `demo` overlay** (bundled `ollama`) as the documented **zero-key first-run** path; it is **not** started by default in `dev` to keep the inner loop light. First-run bootstrap (Alembic migrate, seed default agent + admin) is idempotent for every profile.

## Alternatives considered
- **Bundle MinIO everywhere:** simpler code path but a heavier `dev`/`lite` footprint for little gain; the FS driver keeps parity. Rejected.
- **`lite` as dev-only/throwaway:** would strand the large single-user/offline audience and let it rot; making it a **tested** target costs a CI job and an abstraction discipline we want anyway. Rejected.
- **Ollama on by default in `dev`:** pulls a multi-GB model and slows first boot for contributors who use hosted keys. Rejected in favor of the `demo` overlay.

## Consequences
- A clear profile matrix that satisfies DR-1/DR-2 and the zero-key goal without bloating the inner loop.
- **`lite` reduces isolation** — this is a documented security caveat (untrusted input should not be run in `lite`), and the storage/scheduler abstractions must stay backend-agnostic (SQLite ↔ Postgres, in-process ↔ leader-elected), which is enforced by CI running the task-suite on both `lite` and `full`.
