# ADR-0002 — Datastore & persistence

**Status:** Accepted · **Date:** 2026-07-06

## Context
Keel needs durable sessions/events, editable memory blocks, vector + full-text (incl. CJK) search, a scheduler jobstore, a cache, a cross-process event bus, distributed locks, and blob storage — across a multi-container, horizontally-scalable deployment.

## Decision
- **PostgreSQL 16 + pgvector** as the primary store: relational state, event store, memory blocks, `passages`/KB chunks (vector), and full-text search (`tsvector` + `pg_trgm` for CJK).
- **Redis 7** for job queue, pub/sub event fan-out, cache (prompt-cache keys, model catalog), distributed locks (scheduler leader, session lanes), and rate limiting.
- **MinIO (S3 API)** for blobs (tool-output spill, artifacts, uploads) in `full`; a local volume in `dev`.
- **SQLite** powers an optional single-container **`lite`** mode (CLI/offline).
- ORM: **SQLAlchemy 2.0 async** + **Alembic** migrations.

## Alternatives considered
- **SQLite-first everywhere** (à la OpenClaw): ideal for local/single-node, but workers need a shared concurrent store. Kept for `lite` only.
- **Dedicated vector DB (Qdrant/Milvus/Weaviate)**: strong ANN, but adds a service and split-brain between relational and vector data. Offered as an optional backend, not the default.
- **MySQL**: weaker vector/FTS story than Postgres+pgvector. Rejected.

## Consequences
- One store simplifies hybrid search (RRF over FTS + vector) and transactional consistency.
- Postgres/Redis are a required baseline even in `dev` (mitigated by lightweight images and the `lite` mode).
