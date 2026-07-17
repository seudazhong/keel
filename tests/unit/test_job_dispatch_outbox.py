"""Global Knowledge/durable-job dispatch outbox + cross-scope reconciliation (finding 3).

Unit coverage (no Postgres/Redis) for the outbox that lets a single worker dispatch Knowledge
indexing/deletion jobs across every per-Agent scope: Knowledge admission records a dispatch
intent atomically with the job insert; a lost enqueue leaves the intent so the reconciler
re-dispatches it; a failed intent write rolls back a newly-created job; the fenced lease stops
duplicate workers double-processing; a terminal job's intent is retired; and cross-scope
``run_job`` binds the job's own scope.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from keel_core.config import Settings
from keel_core.job_dispatch import InMemoryJobDispatchOutbox
from keel_core.jobs import CancelMode, InMemoryJobStore
from keel_core.knowledge import (
    CreateKnowledgeBaseCommand,
    CreateKnowledgeDocumentCommand,
    DeleteKnowledgeCommand,
    InMemoryKnowledgeStore,
    KnowledgeSourceType,
)
from keel_core.knowledge.jobs import KNOWLEDGE_DELETE_KIND, KNOWLEDGE_INGEST_KIND
from keel_core.knowledge.service import KnowledgeService
from keel_worker.jobs import reconcile_job_dispatch_tick

_SCOPE_A = "agent:orga/ag1"
_SCOPE_B = "agent:orgb/ag2"
_MODEL = "fake/embed"
_DIM = 3


def _settings() -> Settings:
    return Settings(
        embedding_model=_MODEL,
        embedding_dim=_DIM,
        knowledge_chunk_target_chars=32,
        knowledge_chunk_overlap_chars=4,
    )


def _service(
    scope: str,
    *,
    outbox: InMemoryJobDispatchOutbox | None = None,
    dispatch: Any = None,
) -> tuple[KnowledgeService, InMemoryKnowledgeStore, InMemoryJobStore]:
    store = InMemoryKnowledgeStore(scope)
    jobs = InMemoryJobStore(scope)
    service = KnowledgeService(
        store,
        jobs,
        _settings(),
        searcher=None,
        dispatch_job=dispatch,
        dispatch_outbox=outbox,
        embedding_model=_MODEL,
        embedding_dim=_DIM,
    )
    return service, store, jobs


async def _create_base(service: KnowledgeService, *, key: str = "base") -> str:
    result = await service.create_base(
        CreateKnowledgeBaseCommand(name="Docs", description="d"), key
    )
    return result.resource.id


def _doc(content: str = "Install Keel from the release package.") -> CreateKnowledgeDocumentCommand:
    return CreateKnowledgeDocumentCommand(
        title="Guide.md",
        source_type=KnowledgeSourceType.markdown,
        content=content,
        source_uri=None,
        target_session_id=None,
    )


# --- Outbox semantics ------------------------------------------------------------------------


async def test_record_and_active_scopes() -> None:
    outbox = InMemoryJobDispatchOutbox()
    await outbox.record("job_1", _SCOPE_A, KNOWLEDGE_INGEST_KIND)
    await outbox.record("job_2", _SCOPE_B, KNOWLEDGE_DELETE_KIND)
    assert await outbox.active_scopes() == {_SCOPE_A, _SCOPE_B}
    intents = await outbox.claim_due(worker_id="w1")
    assert {(i.job_id, i.scope_id, i.kind) for i in intents} == {
        ("job_1", _SCOPE_A, KNOWLEDGE_INGEST_KIND),
        ("job_2", _SCOPE_B, KNOWLEDGE_DELETE_KIND),
    }


async def test_lease_blocks_duplicate_worker() -> None:
    outbox = InMemoryJobDispatchOutbox()
    now = datetime(2026, 7, 18, tzinfo=UTC)
    await outbox.record("job_1", _SCOPE_A, KNOWLEDGE_INGEST_KIND, now=now)
    first = await outbox.claim_due(worker_id="w1", now=now, lease_seconds=60)
    assert [i.job_id for i in first] == ["job_1"]
    # A second worker sees nothing while the lease is live (no double processing).
    second = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=1))
    assert second == []
    # After the lease expires the intent is claimable again (crash recovery).
    third = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=120))
    assert [i.job_id for i in third] == ["job_1"]


async def test_reschedule_and_remove() -> None:
    outbox = InMemoryJobDispatchOutbox()
    now = datetime(2026, 7, 18, tzinfo=UTC)
    await outbox.record("job_1", _SCOPE_A, KNOWLEDGE_INGEST_KIND, now=now)
    await outbox.claim_due(worker_id="w1", now=now)
    await outbox.reschedule("job_1", delay_seconds=30, now=now)
    # Deferred: not due until the delay elapses.
    assert await outbox.claim_due(worker_id="w1", now=now + timedelta(seconds=10)) == []
    later = await outbox.claim_due(worker_id="w1", now=now + timedelta(seconds=31))
    assert [i.job_id for i in later] == ["job_1"]
    await outbox.remove("job_1")
    assert await outbox.active_scopes() == set()


# --- Atomic admission (job + intent) ---------------------------------------------------------


async def test_create_document_records_dispatch_intent() -> None:
    outbox = InMemoryJobDispatchOutbox()
    dispatched: list[tuple[str, str]] = []

    async def dispatch(scope_id: str, job_id: str) -> None:
        dispatched.append((scope_id, job_id))

    service, _store, _jobs = _service(_SCOPE_A, outbox=outbox, dispatch=dispatch)
    kb_id = await _create_base(service)
    result = await service.create_document(kb_id, _doc(), "doc-1")
    # The ingest job carries a dispatch intent in the global outbox, keyed by the derived scope.
    intents = await outbox.claim_due(worker_id="w1")
    assert [(i.job_id, i.scope_id, i.kind) for i in intents] == [
        (result.job.id, _SCOPE_A, KNOWLEDGE_INGEST_KIND)
    ]
    assert dispatched[-1] == (_SCOPE_A, result.job.id)


async def test_delete_document_records_dispatch_intent() -> None:
    outbox = InMemoryJobDispatchOutbox()
    service, _store, _jobs = _service(_SCOPE_A, outbox=outbox)
    kb_id = await _create_base(service)
    created = await service.create_document(kb_id, _doc(), "doc-1")
    deleted = await service.delete_document(
        kb_id, created.document.id, DeleteKnowledgeCommand(), "del-1"
    )
    active = {i.job_id: i.kind for i in await outbox.claim_due(worker_id="w1")}
    assert active[deleted.job.id] == KNOWLEDGE_DELETE_KIND


async def test_enqueue_failure_returns_pending_and_is_recorded() -> None:
    # A queue-down dispatch after the durable commit must NOT fail the request: the job + intent
    # are committed, the caller gets its accepted job, and the reconciler heals delivery.
    outbox = InMemoryJobDispatchOutbox()

    async def dispatch(scope_id: str, job_id: str) -> None:
        raise RuntimeError("queue down")

    service, _store, _jobs = _service(_SCOPE_A, outbox=outbox, dispatch=dispatch)
    kb_id = await _create_base(service)
    result = await service.create_document(kb_id, _doc(), "doc-1")
    # The intent survives the failed enqueue so the reconciler can re-dispatch it.
    assert result.job.id in {i.job_id for i in await outbox.claim_due(worker_id="w1")}


class _FailingOutbox(InMemoryJobDispatchOutbox):
    """A dispatch outbox whose ``record`` always fails (intent-store fault injection)."""

    async def record(  # type: ignore[override]
        self, job_id: str, scope_id: str, kind: str, *, now: Any = None
    ) -> None:
        raise RuntimeError("outbox unavailable")


async def test_intent_write_failure_rolls_back_new_job() -> None:
    # Fault at the intent-store boundary: the job insert + intent are atomic, so a failed intent
    # write must NOT leave a job without a discoverable dispatch pointer. The newly-created job is
    # rolled back and the error is not swallowed.
    scope = _SCOPE_A
    store = InMemoryKnowledgeStore(scope)
    jobs = InMemoryJobStore(scope)
    outbox = _FailingOutbox()
    with pytest.raises(RuntimeError, match="outbox unavailable"):
        await jobs.enqueue_once_with_dispatch_intent(
            kind=KNOWLEDGE_INGEST_KIND,
            payload={"kb": "x"},
            target_session_id=None,
            idempotency_key="k1",
            max_attempts=3,
            outbox=outbox,
            cancel_mode=CancelMode.cooperative,
        )
    # No job persisted (rolled back) and no intent recorded.
    assert await jobs.list() == []
    assert await outbox.active_scopes() == set()
    del store


async def test_enqueue_with_intent_is_idempotent() -> None:
    scope = _SCOPE_A
    jobs = InMemoryJobStore(scope)
    outbox = InMemoryJobDispatchOutbox()
    first, created_first = await jobs.enqueue_once_with_dispatch_intent(
        kind=KNOWLEDGE_INGEST_KIND,
        payload={"kb": "x"},
        target_session_id=None,
        idempotency_key="k1",
        max_attempts=3,
        outbox=outbox,
        cancel_mode=CancelMode.cooperative,
    )
    second, created_second = await jobs.enqueue_once_with_dispatch_intent(
        kind=KNOWLEDGE_INGEST_KIND,
        payload={"kb": "x"},
        target_session_id=None,
        idempotency_key="k1",
        max_attempts=3,
        outbox=outbox,
        cancel_mode=CancelMode.cooperative,
    )
    assert created_first is True and created_second is False
    assert first.id == second.id
    intents = await outbox.claim_due(worker_id="w1")
    assert [i.job_id for i in intents] == [first.id]  # exactly one intent


# --- Cross-scope reconciler ------------------------------------------------------------------


def _ctx(
    jobs: InMemoryJobStore,
    outbox: InMemoryJobDispatchOutbox,
    *,
    durable_scope: str = "web:local",
) -> tuple[dict[str, Any], list[tuple[str, tuple[Any, ...]]]]:
    enqueued: list[tuple[str, tuple[Any, ...]]] = []

    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        enqueued.append((name, args))

    ctx: dict[str, Any] = {
        "jobs": jobs,
        "job_dispatch_outbox": outbox,
        "enqueue": _enqueue,
        "durable_scope": durable_scope,
        "job_settings": _settings(),
    }
    return ctx, enqueued


async def test_reconcile_redispatches_agent_scoped_job() -> None:
    # A document ingested under a per-Agent scope with a lost enqueue is re-dispatched by the
    # cross-scope reconciler with the job's own scope (worker processes an Agent-scoped job).
    jobs = InMemoryJobStore(_SCOPE_A)
    outbox = InMemoryJobDispatchOutbox()
    job, _ = await jobs.enqueue_once_with_dispatch_intent(
        kind=KNOWLEDGE_INGEST_KIND,
        payload={"kb": "x"},
        target_session_id=None,
        idempotency_key="k1",
        max_attempts=3,
        outbox=outbox,
        cancel_mode=CancelMode.cooperative,
    )
    ctx, enqueued = _ctx(jobs, outbox)
    dispatched = await reconcile_job_dispatch_tick(ctx)
    assert dispatched == 1
    assert ("run_job", (_SCOPE_A, job.id)) in enqueued
    # Job still queued -> intent deferred, not removed.
    assert await outbox.active_scopes() == {_SCOPE_A}


async def test_reconcile_removes_terminal_intent() -> None:
    jobs = InMemoryJobStore(_SCOPE_A)
    outbox = InMemoryJobDispatchOutbox()
    job, _ = await jobs.enqueue_once_with_dispatch_intent(
        kind=KNOWLEDGE_INGEST_KIND,
        payload={"kb": "x"},
        target_session_id=None,
        idempotency_key="k1",
        max_attempts=3,
        outbox=outbox,
        cancel_mode=CancelMode.cooperative,
    )
    # Drive the job terminal directly.
    now = datetime.now(UTC)
    lease = await jobs.claim(job.id, now, 60)
    assert lease is not None
    from keel_core.jobs import JobResult

    await jobs.succeed(lease, JobResult(data={}, message="done"), now)
    ctx, _enqueued = _ctx(jobs, outbox)
    await reconcile_job_dispatch_tick(ctx)
    assert await outbox.active_scopes() == set()  # terminal intent retired


async def test_reconcile_drops_non_dispatchable_kind() -> None:
    jobs = InMemoryJobStore(_SCOPE_A)
    outbox = InMemoryJobDispatchOutbox()
    # An intent for a kind that is not cross-scope-dispatchable must be dropped (fail closed).
    await outbox.record("job_x", _SCOPE_A, "erasure.execute")
    ctx, enqueued = _ctx(jobs, outbox)
    await reconcile_job_dispatch_tick(ctx)
    assert enqueued == []
    assert await outbox.active_scopes() == set()


async def test_reconcile_drops_malformed_scope() -> None:
    jobs = InMemoryJobStore(_SCOPE_A)
    outbox = InMemoryJobDispatchOutbox()
    await outbox.record("job_x", "not a scope!!", KNOWLEDGE_INGEST_KIND)
    ctx, enqueued = _ctx(jobs, outbox)
    await reconcile_job_dispatch_tick(ctx)
    assert enqueued == []
    assert await outbox.active_scopes() == set()


async def test_two_worker_reconcile_is_idempotent() -> None:
    # Two reconciler workers racing on the same intent: the fenced lease means only one claims it
    # per tick, and re-dispatch is idempotent (deduped by the job claim) even if both ran.
    jobs = InMemoryJobStore(_SCOPE_A)
    outbox = InMemoryJobDispatchOutbox()
    job, _ = await jobs.enqueue_once_with_dispatch_intent(
        kind=KNOWLEDGE_INGEST_KIND,
        payload={"kb": "x"},
        target_session_id=None,
        idempotency_key="k1",
        max_attempts=3,
        outbox=outbox,
        cancel_mode=CancelMode.cooperative,
    )
    now = datetime(2026, 7, 18, tzinfo=UTC)
    first = await outbox.claim_due(worker_id="w1", now=now)
    second = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=1))
    assert [i.job_id for i in first] == [job.id]
    assert second == []  # the second worker is fenced out
