# ADR-0006 — Scheduler & background queue

**Status:** Accepted · **Date:** 2026-07-06

## Context
Keel needs reliable scheduled tasks (cron/interval/one-shot/ISO) and a background job queue for agent runs and long tasks, across horizontally-scaled workers, with **at-most-once** execution semantics.

## Decision
- **Background queue: arq** (async Redis queue) — async-native, lightweight, matches the asyncio core. Workers consume agent-run and job messages.
- **Scheduler: a custom leader-elected service.** A `schedules` table in Postgres is the source of truth; the scheduler acquires a **Redis leader lock**, ticks periodically, selects due rows, **advances `next_run_at` before enqueueing** (guaranteeing at-most-once even on crash), and pushes a job to arq. Overrunning jobs get a hard interrupt.

## Alternatives considered
- **Celery (+ beat)**: mature and feature-rich, but sync-first and heavier; async integration is awkward. Rejected for the async core.
- **APScheduler with a Postgres jobstore**: simpler, but gives less explicit control over distributed at-most-once semantics and cursor-before-run. Rejected as the primary mechanism (patterns borrowed).
- **Postgres-only queue (SKIP LOCKED)**: removes Redis for queueing, but we already need Redis for pub/sub and locks; arq is simpler for async workers. Reconsider if we drop Redis.

## Consequences
- Precise, testable at-most-once scheduling and a small async queue.
- Redis is on the critical path for scheduling/jobs (already required for bus/locks).
