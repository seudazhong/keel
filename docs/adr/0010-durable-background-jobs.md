# ADR-0010: Durable background-job delivery semantics

- **Status:** Accepted
- **Date:** 2026-07-14
- **Supersedes:** ADR-0006 only for generic durable background jobs; schedule triggering remains unchanged

## Context

ADR-0006 selects arq and an at-most-once schedule cursor. Advancing a schedule cursor
before enqueue is correct for recurring triggers: a crash may produce zero runs, but never
duplicates.

Long-running jobs such as document ingestion have different requirements:

- losing an enqueue forever is unacceptable;
- Redis delivery may be duplicated;
- a worker may crash after partial work;
- progress, cancellation and bounded retries must survive process death;
- terminal results may need to return to a conversation.

Applying schedule-level at-most-once semantics to these jobs would make a crash between
the database write and Redis enqueue permanently lose work.

## Decision

Generic durable background jobs use:

1. **Postgres as lifecycle source of truth**;
2. **at-least-once arq delivery**;
3. DB lease tokens to prevent concurrent active owners;
4. bounded attempts and deterministic backoff;
5. cooperative cancellation;
6. idempotent handlers for external/database side effects;
7. an atomic Postgres transaction for terminal transition plus optional session-result
   injection exactly once;
8. a periodic DB dispatcher that heals missed or expired delivery.

Schedule triggering still follows ADR-0006/I9:

- advance schedule cursor before enqueue;
- one occurrence executes zero or one time.

The schedule may enqueue a durable job; after the job row exists, ADR-0010 semantics
govern its delivery and recovery.

## Consequences

### Positive

- A Redis enqueue loss is healed.
- Duplicate delivery is safe.
- Worker crash recovery is explicit and testable.
- Long jobs expose durable progress/cancellation.
- RAG ingestion can be retried without losing user-visible status.

### Costs

- Handler execution is not exactly once.
- Every handler must use durable idempotency keys/checkpoints for side effects.
- Cooperative cancellation cannot instantly stop a blocking external call.
- Lease/attempt/retry state adds Postgres and test complexity.

## Rejected alternatives

### At-most-once job execution

Rejected because a crash or enqueue loss can permanently discard an accepted document
ingestion request.

### Redis/arq as the only job store

Rejected because product status, progress, cancellation and result injection must survive
Redis/process loss and remain scope-auditable.

### Hard process cancellation

Deferred. It complicates cleanup and does not make external side effects transactional.
The first version uses cooperative checkpoints plus lease recovery.

