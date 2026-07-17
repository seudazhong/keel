"""Integration tests for M3.5 retention + erasure over live Postgres/Redis."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.events import Event, EventType
from keel_core.jobs import JobLimits, PostgresJobStore
from keel_core.lifecycle.coordinator import ErasureCoordinator, UnsupportedExternalStep
from keel_core.lifecycle.models import ErasureStatus, ErasureTarget, ErasureTargetKind, StepStatus
from keel_core.lifecycle.purge import ScopePurgeRepository
from keel_core.lifecycle.redis import RedisLifecycleCleaner
from keel_core.lifecycle.retention import RetentionCandidate, select_expired
from keel_core.lifecycle.service import ErasureService
from keel_core.lifecycle.store import PostgresErasureStore
from keel_core.lifecycle.tombstones import load_tombstone_hook
from keel_core.rebuild import InMemoryProjectionCheckpoints, ProjectionRebuilder
from keel_core.state import InMemoryEventStore, PostgresEventStore

pytestmark = pytest.mark.integration

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")
_VEC = "[0.1,0.2,0.3]"

# Every scope-bound content/store table that a scope erasure must clear.
_SCOPED_TABLES = (
    "sessions",
    "events",
    "message_embeddings",
    "memory_blocks",
    "memory_block_versions",
    "memory_proposals",
    "consolidation_cursors",
    "archival",
    "knowledge_bases",
    "kb_documents",
    "kb_document_versions",
    "kb_chunks",
    "knowledge_idempotency",
    "connector_bindings",
    "connector_resources",
    "connector_cursors",
    "connector_deliveries",
    "connector_tokens",
    "connector_outbox",
    "oauth_states",
    "schedules",
    "approvals",
    "jobs",
)


async def _seed_scope(engine: AsyncEngine, scope: str) -> str:
    """Insert one row into every scope-bound table. Returns the seeded session id."""
    session_id = f"{scope}-sess"
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope})
        p = {"scope": scope, "sid": session_id}
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id, next_seq) VALUES (:sid, :scope, 2)"), p
        )
        event_id = await conn.scalar(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts, payload) "
                "VALUES (:sid, :scope, 1, 'tool.result', now(), '{}'::jsonb) RETURNING id"
            ),
            p,
        )
        await conn.execute(
            text(
                "INSERT INTO message_embeddings "
                "(event_id, scope_id, session_id, seq, role, content, model, dim, embedding) "
                "VALUES (:eid, :scope, :sid, 1, 'user', 'hi', 'm', 3, CAST(:vec AS vector))"
            ),
            {**p, "eid": event_id, "vec": _VEC},
        )
        await conn.execute(
            text(
                "INSERT INTO memory_blocks (scope_id, key, value) VALUES (:scope, 'persona', 'x')"
            ),
            p,
        )
        await conn.execute(
            text(
                "INSERT INTO memory_block_versions (scope_id, key, version, value) "
                "VALUES (:scope, 'persona', 1, 'x')"
            ),
            p,
        )
        await conn.execute(
            text(
                "INSERT INTO memory_proposals "
                "(id, scope_id, block, expected_version, proposed_value, reason, confidence, "
                "source_event_ids, idempotency_key) "
                "VALUES (:pid, :scope, 'persona', 1, 'v', 'r', 0.9, ARRAY[1]::bigint[], :ik)"
            ),
            {**p, "pid": f"{scope}-prop", "ik": f"{scope}-ik"},
        )
        await conn.execute(
            text("INSERT INTO consolidation_cursors (scope_id, last_event_id) VALUES (:scope, 1)"),
            p,
        )
        await conn.execute(
            text(
                "INSERT INTO archival (scope_id, content, model, dim, embedding) "
                "VALUES (:scope, 'note', 'm', 3, CAST(:vec AS vector))"
            ),
            {**p, "vec": _VEC},
        )
        await conn.execute(
            text(
                "INSERT INTO connector_bindings "
                "(id, scope_id, connector_id, status, display_name) "
                "VALUES (:bid, :scope, 'gmail', 'connected', 'Gmail')"
            ),
            {**p, "bid": f"{scope}-binding"},
        )
        await conn.execute(
            text(
                "INSERT INTO connector_resources "
                "(id, scope_id, connector_id, binding_id, external_id, kind, display_name, "
                "selected) VALUES (:rid, :scope, 'gmail', :bid, 'inbox', 'mailbox', "
                "'Inbox', true)"
            ),
            {**p, "bid": f"{scope}-binding", "rid": f"{scope}-resource"},
        )
        await conn.execute(
            text(
                "INSERT INTO connector_cursors "
                "(id, scope_id, connector_id, binding_id, stream, cursor_value) "
                "VALUES (:cid, :scope, 'gmail', :bid, 'default', 'cursor')"
            ),
            {**p, "bid": f"{scope}-binding", "cid": f"{scope}-cursor"},
        )
        await conn.execute(
            text(
                "INSERT INTO connector_deliveries "
                "(id, scope_id, connector_id, binding_id, delivery_id, payload_hash) "
                "VALUES (:did, :scope, 'gmail', :bid, 'delivery', :hash)"
            ),
            {
                **p,
                "bid": f"{scope}-binding",
                "did": f"{scope}-delivery",
                "hash": "a" * 64,
            },
        )
        await conn.execute(
            text(
                "INSERT INTO connector_tokens (scope_id, connector_id, ciphertext, key_id) "
                "VALUES (:scope, 'gmail', 'ct', 'v1')"
            ),
            p,
        )
        await conn.execute(
            text(
                "INSERT INTO connector_outbox (scope_id, connector_id, idempotency_key, status) "
                "VALUES (:scope, 'gmail', 'ob', 'pending')"
            ),
            p,
        )
        await conn.execute(
            text(
                "INSERT INTO oauth_states (state, scope_id, connector_id, expires_at) "
                "VALUES (:st, :scope, 'gmail', now() + interval '1 hour')"
            ),
            {**p, "st": f"{scope}-state"},
        )
        await conn.execute(
            text(
                "INSERT INTO schedules "
                "(id, scope_id, agent_id, session_id, trigger_kind, spec, next_run_at) "
                "VALUES (:schid, :scope, 'digest', :sid, 'interval', '60', now())"
            ),
            {**p, "schid": f"{scope}-sch"},
        )
        await conn.execute(
            text(
                "INSERT INTO approvals "
                "(id, scope_id, run_id, session_id, tool, args, call_id, idempotency_key, "
                "reason, expires_at) "
                "VALUES (:aid, :scope, 'run', :sid, 'email_send', '{}'::jsonb, 'call', :ik, "
                "'why', now() + interval '1 day')"
            ),
            {**p, "aid": f"{scope}-appr", "ik": f"{scope}-aik"},
        )
        await conn.execute(
            text(
                "INSERT INTO jobs (id, scope_id, kind, idempotency_key, max_attempts) "
                "VALUES (:jid, :scope, 'knowledge.ingest', :ik, 3)"
            ),
            {**p, "jid": f"{scope}-job", "ik": f"{scope}-jik"},
        )
        # Knowledge chain: base -> document -> version -> chunk (+ idempotency).
        await conn.execute(
            text(
                "INSERT INTO knowledge_bases (id, scope_id, name, embedding_model, embedding_dim) "
                "VALUES (:kb, :scope, :name, 'm', 3)"
            ),
            {**p, "kb": f"{scope}-kb", "name": f"{scope}-kb-name"},
        )
        await conn.execute(
            text(
                "INSERT INTO kb_documents (id, scope_id, kb_id, title, source_type) "
                "VALUES (:doc, :scope, :kb, 'title', 'text')"
            ),
            {**p, "doc": f"{scope}-doc", "kb": f"{scope}-kb"},
        )
        await conn.execute(
            text(
                "INSERT INTO kb_document_versions "
                "(id, scope_id, kb_id, document_id, version, content_sha256, index_fingerprint, "
                "mime_type, chunking_version, target_chars, overlap_chars) "
                "VALUES (:ver, :scope, :kb, :doc, 1, 'sha', 'fp', 'text/plain', 'v1', 100, 10)"
            ),
            {**p, "ver": f"{scope}-ver", "kb": f"{scope}-kb", "doc": f"{scope}-doc"},
        )
        await conn.execute(
            text(
                "INSERT INTO kb_chunks "
                "(id, scope_id, kb_id, document_id, document_version_id, ordinal, text, "
                "char_start, char_end, content_hash, model, dim, embedding) "
                "VALUES (:ch, :scope, :kb, :doc, :ver, 0, 'chunk', 0, 5, 'h', 'm', 3, "
                "CAST(:vec AS vector))"
            ),
            {
                **p,
                "ch": f"{scope}-chunk",
                "kb": f"{scope}-kb",
                "doc": f"{scope}-doc",
                "ver": f"{scope}-ver",
                "vec": _VEC,
            },
        )
        await conn.execute(
            text(
                "INSERT INTO knowledge_idempotency "
                "(id, scope_id, operation, idempotency_key, request_fingerprint, resource_kind, "
                "resource_id) VALUES (:iid, :scope, 'create_base', :ik, :fp, 'base', :kb)"
            ),
            {
                **p,
                "iid": f"{scope}-kid",
                "ik": f"{scope}-kik",
                "fp": "a" * 64,
                "kb": f"{scope}-kb",
            },
        )
    return session_id


async def _counts(engine: AsyncEngine, scope: str) -> dict[str, int]:
    out: dict[str, int] = {}
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope})
        for table in _SCOPED_TABLES:
            out[table] = int(
                await conn.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE scope_id = :scope"),
                    {"scope": scope},
                )
                or 0
            )
    return out


async def test_migration_creates_lifecycle_tables(migrated_db: AsyncEngine) -> None:
    async with migrated_db.begin() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT tablename FROM pg_tables WHERE tablename IN "
                    "('erasure_requests','erasure_steps','event_tombstones','retention_policies')"
                )
            )
        ).all()
        assert {r.tablename for r in rows} == {
            "erasure_requests",
            "erasure_steps",
            "event_tombstones",
            "retention_policies",
        }
        rls = (
            await conn.execute(
                text(
                    "SELECT relname FROM pg_class "
                    "WHERE relrowsecurity AND relname = 'erasure_requests'"
                )
            )
        ).all()
        assert rls  # RLS is enabled on the scoped lifecycle tables


async def test_seeded_all_store_erasure_and_cross_scope_preservation(
    migrated_db: AsyncEngine, redis_client: object
) -> None:
    scope_a, scope_b = "erase:a", "keep:b"
    session_a = await _seed_scope(migrated_db, scope_a)
    await _seed_scope(migrated_db, scope_b)

    # A live Redis stream backlog for the erased session.
    from redis.asyncio import Redis

    assert isinstance(redis_client, Redis)
    await redis_client.xadd(f"events:{session_a}", {"data": "x"})

    coord = ErasureCoordinator(
        migrated_db,
        PostgresErasureStore(migrated_db, scope_a),
        redis_cleaner=RedisLifecycleCleaner(redis_client),
    )
    request = await coord.submit(ErasureTarget(scope_a), "req-1", reason="gdpr")
    result = await coord.execute(request.id)

    assert result.status is ErasureStatus.completed
    a_counts = await _counts(migrated_db, scope_a)
    assert all(count == 0 for count in a_counts.values()), a_counts
    b_counts = await _counts(migrated_db, scope_b)
    assert all(count == 1 for count in b_counts.values()), b_counts

    # Tombstone recorded; Redis stream removed.
    store = PostgresErasureStore(migrated_db, scope_a)
    assert await store.is_tombstoned(session_a)
    assert await redis_client.exists(f"events:{session_a}") == 0
    persisted = await store.get(request.id)
    assert persisted is not None and persisted.status is ErasureStatus.completed


async def test_repeated_erasure_is_idempotent(migrated_db: AsyncEngine) -> None:
    scope = "erase:idem"
    await _seed_scope(migrated_db, scope)
    coord = ErasureCoordinator(migrated_db, PostgresErasureStore(migrated_db, scope))
    request = await coord.submit(ErasureTarget(scope), "req-1")
    first = await coord.execute(request.id)
    second = await coord.execute(request.id)
    assert first.status is second.status is ErasureStatus.completed
    assert all(count == 0 for count in (await _counts(migrated_db, scope)).values())


async def test_crash_mid_run_resumes_over_postgres(migrated_db: AsyncEngine) -> None:
    scope = "erase:resume"
    await _seed_scope(migrated_db, scope)

    class _FlakyPurge(ScopePurgeRepository):
        def __init__(self, engine: AsyncEngine) -> None:
            super().__init__(engine)
            self._failed = False

        async def archival(self, scope_id: str) -> int:
            if not self._failed:
                self._failed = True
                raise RuntimeError("boom during archival")
            return await super().archival(scope_id)

    coord = ErasureCoordinator(
        migrated_db, PostgresErasureStore(migrated_db, scope), purge=_FlakyPurge(migrated_db)
    )
    request = await coord.submit(ErasureTarget(scope), "req-1")
    with pytest.raises(RuntimeError):
        await coord.execute(request.id)

    # Resume: the run picks up where it left off and completes.
    result = await coord.execute(request.id)
    assert result.status is ErasureStatus.completed
    assert all(count == 0 for count in (await _counts(migrated_db, scope)).values())


async def test_concurrent_job_claim_is_exclusive(migrated_db: AsyncEngine) -> None:
    scope = "erase:claim"
    jobs = PostgresJobStore(migrated_db, scope, limits=JobLimits.from_settings(Settings()))
    coord = ErasureCoordinator(migrated_db, PostgresErasureStore(migrated_db, scope))
    service = ErasureService(coord, jobs, dispatch_job=None)
    submission = await service.submit(ErasureTarget(scope), "req-1")

    now = datetime.now(UTC)
    lease_a = await jobs.claim(submission.job.id, now, 300)
    lease_b = await jobs.claim(submission.job.id, now, 300)
    assert lease_a is not None
    assert lease_b is None  # a second worker cannot claim the running job


async def test_tombstone_blocks_projection_resurrection(migrated_db: AsyncEngine) -> None:
    scope = "erase:rebuild"
    durable = PostgresEventStore(migrated_db, scope)
    for seq in range(1, 4):
        await durable.append(
            Event(
                type=EventType.message_token,
                seq=0,
                session_id="s-rb",
                scope_id=scope,
                ts=datetime.now(UTC),
                payload={"role": "user", "text": f"m{seq}", "partial": False},
            )
        )

    # Snapshot the raw events into a residual source, then erase the session.
    residual = InMemoryEventStore()
    async for event in durable.read("s-rb"):
        await residual.append(event)

    coord = ErasureCoordinator(migrated_db, PostgresErasureStore(migrated_db, scope))
    request = await coord.submit(ErasureTarget(scope, ErasureTargetKind.session, "s-rb"), "req-1")
    await coord.execute(request.id)

    hook = await load_tombstone_hook(PostgresErasureStore(migrated_db, scope))
    applied: list[Event] = []

    class _Sink:
        async def apply(self, event: Event) -> None:
            applied.append(event)

    rebuilder = ProjectionRebuilder(residual, InMemoryProjectionCheckpoints(), tombstone_hook=hook)
    result = await rebuilder.rebuild("recall", "s-rb", _Sink())
    assert result.processed == 3
    assert result.skipped_tombstones == 3
    assert applied == []


async def test_partial_external_step_is_reported(migrated_db: AsyncEngine) -> None:
    scope = "erase:partial"
    await _seed_scope(migrated_db, scope)
    coord = ErasureCoordinator(
        migrated_db,
        PostgresErasureStore(migrated_db, scope),
        external_steps=[UnsupportedExternalStep("provider_telemetry")],
    )
    request = await coord.submit(ErasureTarget(scope), "req-1")
    result = await coord.execute(request.id)

    assert result.status is ErasureStatus.partial
    assert result.external_incomplete
    telemetry = next(s for s in result.steps if s.step == "provider_telemetry")
    assert telemetry.status is StepStatus.unsupported
    # Data stores are still fully erased even though the request is 'partial'.
    assert all(count == 0 for count in (await _counts(migrated_db, scope)).values())


async def test_expired_retention_scheduling(migrated_db: AsyncEngine) -> None:
    from keel_core.lifecycle import policies

    scope = "retain:a"
    async with migrated_db.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope})
        await conn.execute(
            text(
                "INSERT INTO oauth_states (state, scope_id, connector_id, expires_at) "
                "VALUES ('old', :scope, 'gmail', now() - interval '2 hours'), "
                "('new', :scope, 'gmail', now() + interval '1 hour')"
            ),
            {"scope": scope},
        )
        rows = (
            await conn.execute(
                text("SELECT state, created_at FROM oauth_states WHERE scope_id = :scope"),
                {"scope": scope},
            )
        ).all()

    now = datetime.now(UTC)
    candidates = [
        RetentionCandidate(policies.OAUTH_STATE, r.state, r.created_at - timedelta(hours=2))
        if r.state == "old"
        else RetentionCandidate(policies.OAUTH_STATE, r.state, r.created_at)
        for r in rows
    ]
    due = select_expired(candidates, now)
    assert [c.resource_id for c in due] == ["old"]
