"""Postgres durable-job state machine and exactly-once injection."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.jobs import (
    JobError,
    JobLeaseLostError,
    JobLimits,
    JobStatus,
    JobValidationError,
    PostgresJobStore,
    _job_dedupe_lock_id,
)
from keel_core.loop import admit
from keel_core.state import PostgresEventStore

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


async def _target(engine: AsyncEngine, scope: str, session_id: str) -> None:
    await admit(PostgresEventStore(engine, scope), session_id, scope, "seed")


async def _columns(engine: AsyncEngine, table: str) -> dict[str, tuple[str, str]]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns WHERE table_name = :table"
                ),
                {"table": table},
            )
        ).all()
    return {str(row.column_name): (str(row.data_type), str(row.is_nullable)) for row in rows}


async def test_jobs_migration_has_required_columns_checks_and_indexes(
    migrated_db: AsyncEngine,
) -> None:
    columns = await _columns(migrated_db, "jobs")
    assert columns["id"] == ("text", "NO")
    assert columns["scope_id"] == ("text", "NO")
    assert columns["payload"] == ("jsonb", "NO")
    assert columns["attempt"] == ("integer", "NO")
    assert columns["lease_expires_at"] == ("timestamp with time zone", "YES")
    assert columns["progress_current"] == ("bigint", "NO")
    assert columns["injected_event_seq"] == ("bigint", "YES")

    async with migrated_db.connect() as conn:
        checks = "\n".join(
            str(value)
            for value in (
                await conn.execute(
                    text(
                        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                        "WHERE conrelid = 'jobs'::regclass"
                    )
                )
            ).scalars()
        )
        indexes = {
            str(value)
            for value in (
                await conn.execute(
                    text("SELECT indexname FROM pg_indexes WHERE tablename = 'jobs'")
                )
            ).scalars()
        }
    assert "queued" in checks and "cancelled" in checks
    assert "attempt >= 0" in checks
    assert "max_attempts >= 1" in checks
    assert {
        "jobs_pkey",
        "jobs_scope_id_kind_idempotency_key_key",
        "ix_jobs_dispatch",
        "ix_jobs_lease_expiry",
        "ix_jobs_target_session",
    } <= indexes


async def test_jobs_rls_is_enabled_and_fails_closed(migrated_db: AsyncEngine) -> None:
    role = f"jobs_rls_{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:
        await conn.execute(text(f'CREATE ROLE "{role}" NOSUPERUSER'))
        await conn.execute(text(f'GRANT SELECT ON jobs TO "{role}"'))
        await conn.execute(
            text(
                "INSERT INTO jobs "
                "(id, scope_id, kind, idempotency_key, max_attempts) VALUES "
                "('job_a', 'scope:a', 'test.echo', 'a', 3), "
                "('job_b', 'scope:b', 'test.echo', 'b', 3)"
            )
        )
    try:
        async with migrated_db.connect() as conn:
            await conn.execute(text(f'SET ROLE "{role}"'))
            await conn.execute(text("SET app.scope_id = 'scope:a'"))
            assert (await conn.execute(text("SELECT id FROM jobs"))).scalars().all() == ["job_a"]
            await conn.execute(text("RESET app.scope_id"))
            assert (await conn.execute(text("SELECT id FROM jobs"))).scalars().all() == []
            await conn.execute(text("RESET ROLE"))
    finally:
        async with migrated_db.begin() as conn:
            await conn.execute(text(f'REVOKE ALL PRIVILEGES ON TABLE jobs FROM "{role}"'))
            await conn.execute(text(f'DROP ROLE IF EXISTS "{role}"'))


async def test_postgres_enqueue_once_is_concurrently_deduped(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "jobs:dedupe")

    async def enqueue(value: int) -> tuple[str, bool, dict[str, object]]:
        row, created = await store.enqueue_once(
            kind="test.echo",
            payload={"value": value},
            target_session_id=None,
            idempotency_key="request-1",
            max_attempts=3,
            now=_NOW,
        )
        return row.id, created, row.payload

    first, second = await asyncio.gather(enqueue(1), enqueue(2))
    assert first[0] == second[0]
    assert sorted([first[1], second[1]]) == [False, True]
    assert first[2] == second[2]
    assert first[2] in ({"value": 1}, {"value": 2})


async def test_postgres_existing_dedupe_precedes_retry_payload_and_target_validation(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "jobs:dedupe-order")
    first, created = await store.enqueue_once(
        kind="test.echo",
        payload={"value": 1},
        target_session_id=None,
        idempotency_key="request-1",
        max_attempts=3,
        now=_NOW,
    )
    duplicate, duplicate_created = await store.enqueue_once(
        kind="test.echo",
        payload={"bad": object()},
        target_session_id="missing-on-retry",
        idempotency_key="request-1",
        max_attempts=1,
        now=_NOW + timedelta(seconds=1),
    )

    assert created is True
    assert duplicate_created is False
    assert duplicate.id == first.id
    assert duplicate.payload == {"value": 1}
    assert duplicate.target_session_id is None
    assert duplicate.max_attempts == 3


async def test_postgres_uncommitted_winner_serializes_duplicate_before_validation(
    migrated_db: AsyncEngine,
) -> None:
    scope, kind, key = "jobs:uncommitted", "test.echo", "request-1"
    conn = await migrated_db.connect()
    tx = await conn.begin()
    try:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"), {"scope": scope}
        )
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": _job_dedupe_lock_id(scope, kind, key)},
        )
        await conn.execute(
            text(
                "INSERT INTO jobs "
                "(id, scope_id, kind, payload, idempotency_key, max_attempts, "
                "next_attempt_at, created_at, updated_at) VALUES "
                "('held-winner', :scope, :kind, '{}'::jsonb, :key, 3, :now, :now, :now)"
            ),
            {"scope": scope, "kind": kind, "key": key, "now": _NOW},
        )

        duplicate_task = asyncio.create_task(
            PostgresJobStore(migrated_db, scope).enqueue_once(
                kind=kind,
                payload={"bad": object()},
                target_session_id="missing-on-retry",
                idempotency_key=key,
                max_attempts=3,
                now=_NOW,
            )
        )
        await asyncio.sleep(0.05)
        assert duplicate_task.done() is False
        await tx.commit()
        duplicate, created = await duplicate_task
        assert created is False
        assert duplicate.id == "held-winner"
    finally:
        if tx.is_active:
            await tx.rollback()
        await conn.close()


async def test_postgres_enqueue_target_must_exist_in_bound_scope(
    migrated_db: AsyncEngine,
) -> None:
    await _target(migrated_db, "scope:a", "target-a")
    accepted, _ = await PostgresJobStore(migrated_db, "scope:a").enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target-a",
        idempotency_key="same-scope",
        max_attempts=3,
        now=_NOW,
    )
    assert accepted.target_session_id == "target-a"

    with pytest.raises(JobValidationError, match="target_session_not_found"):
        await PostgresJobStore(migrated_db, "scope:b").enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id="target-a",
            idempotency_key="cross-scope",
            max_attempts=3,
            now=_NOW,
        )


async def test_postgres_enqueue_validates_identities_and_utc_timestamps(
    migrated_db: AsyncEngine,
) -> None:
    with pytest.raises(ValueError, match="scope_id"):
        PostgresJobStore(migrated_db, "bad\x00scope")

    store = PostgresJobStore(migrated_db, " jobs:validation ")
    assert store.scope_id == "jobs:validation"

    for kwargs, code in [
        ({"kind": "bad\x00kind"}, "invalid_kind"),
        ({"idempotency_key": "bad\ud800key"}, "invalid_idempotency_key"),
        ({"target_session_id": "bad\x00target"}, "invalid_target_session_id"),
    ]:
        values = {
            "kind": "test.echo",
            "payload": {},
            "target_session_id": None,
            "idempotency_key": code,
            "max_attempts": 3,
            "now": _NOW,
        }
        values.update(kwargs)
        with pytest.raises(JobValidationError, match=code):
            await store.enqueue_once(**values)  # type: ignore[arg-type]

    local_time = datetime(2026, 7, 14, 17, 0, tzinfo=timezone(timedelta(hours=8)))
    row, _ = await store.enqueue_once(
        kind=" test.echo ",
        payload={},
        target_session_id=None,
        idempotency_key=" aware ",
        max_attempts=3,
        now=local_time,
    )
    assert row.kind == "test.echo"
    assert row.idempotency_key == "aware"
    assert row.created_at == _NOW

    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id=None,
            idempotency_key="naive",
            max_attempts=3,
            now=datetime(2026, 7, 14, 9, 0),
        )
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.enqueue_once(
            kind="test.echo",
            payload={"ignored": True},
            target_session_id="missing-on-retry",
            idempotency_key="aware",
            max_attempts=1,
            now=datetime(2026, 7, 14, 9, 0),
        )
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.dispatchable(datetime(2026, 7, 14, 9, 0), 100)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.exhausted(datetime(2026, 7, 14, 9, 0), 100)
    assert await store.get("bad\x00id") is None
    assert await store.list(kind="bad\x00kind") == []


async def test_postgres_payload_is_bounded_and_records_are_detached(
    migrated_db: AsyncEngine,
) -> None:
    bounded = PostgresJobStore(
        migrated_db,
        "scope:bounded",
        limits=JobLimits(payload_max_bytes=16),
    )
    with pytest.raises(JobValidationError, match="payload_too_large"):
        await bounded.enqueue_once(
            kind="test.echo",
            payload={"value": "0123456789"},
            target_session_id=None,
            idempotency_key="large",
            max_attempts=3,
            now=_NOW,
        )
    with pytest.raises(JobValidationError, match="storage_text_invalid"):
        await bounded.enqueue_once(
            kind="test.echo",
            payload={"value": "\x00"},
            target_session_id=None,
            idempotency_key="unsafe",
            max_attempts=3,
            now=_NOW,
        )

    store = PostgresJobStore(migrated_db, "scope:detached")
    payload = {"nested": {"values": [1]}}
    created, _ = await store.enqueue_once(
        kind="test.echo",
        payload=payload,
        target_session_id=None,
        idempotency_key="detached",
        max_attempts=3,
        now=_NOW,
    )
    payload["nested"]["values"].append(2)
    created.payload["nested"]["values"].append(3)

    fetched = await store.get(created.id)
    assert fetched is not None
    assert fetched.payload == {"nested": {"values": [1]}}
    fetched.payload["nested"]["values"].append(4)
    listed = await store.list()
    assert listed[0].payload == {"nested": {"values": [1]}}


async def test_postgres_get_list_filters_limit_and_scope(
    migrated_db: AsyncEngine,
) -> None:
    a = PostgresJobStore(migrated_db, "scope:list:a")
    b = PostgresJobStore(migrated_db, "scope:list:b")
    older, _ = await a.enqueue_once(
        kind="test.a",
        payload={},
        target_session_id=None,
        idempotency_key="older",
        max_attempts=3,
        now=_NOW,
    )
    newer, _ = await a.enqueue_once(
        kind="test.b",
        payload={},
        target_session_id=None,
        idempotency_key="newer",
        max_attempts=3,
        now=_NOW + timedelta(seconds=1),
    )
    await b.enqueue_once(
        kind="test.a",
        payload={},
        target_session_id=None,
        idempotency_key="other",
        max_attempts=3,
        now=_NOW + timedelta(seconds=2),
    )

    assert (await a.get(older.id)).id == older.id  # type: ignore[union-attr]
    assert await b.get(older.id) is None
    assert [row.id for row in await a.list()] == [newer.id, older.id]
    assert [row.id for row in await a.list(kind="test.a", limit=1)] == [older.id]
    assert await a.list(status=JobStatus.running) == []
    with pytest.raises(ValueError, match="limit"):
        await a.list(limit=0)
    with pytest.raises(ValueError, match="limit"):
        await a.list(limit=101)


async def test_postgres_dispatchable_and_exhausted_queries(
    migrated_db: AsyncEngine,
) -> None:
    scope = "scope:dispatch"
    store = PostgresJobStore(migrated_db, scope)
    due, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="due",
        max_attempts=3,
        now=_NOW,
    )
    later, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="later",
        max_attempts=3,
        now=_NOW + timedelta(minutes=5),
    )
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"), {"scope": scope}
        )
        await conn.execute(
            text(
                "INSERT INTO jobs "
                "(id, scope_id, kind, status, idempotency_key, attempt, max_attempts, "
                "next_attempt_at, lease_expires_at) VALUES "
                "('expired-left', :scope, 'test.echo', 'running', 'expired-left', 1, 3, "
                ":now, :expired), "
                "('expired-done', :scope, 'test.echo', 'running', 'expired-done', 3, 3, "
                ":now, :expired)"
            ),
            {
                "scope": scope,
                "now": _NOW,
                "expired": _NOW - timedelta(seconds=1),
            },
        )

    dispatchable = await store.dispatchable(_NOW, 100)
    assert due.id in dispatchable
    assert later.id not in dispatchable
    assert "expired-left" in dispatchable
    assert "expired-done" not in dispatchable
    assert await store.exhausted(_NOW, 100) == ["expired-done"]


async def _pg_job(
    store: PostgresJobStore,
    key: str,
    *,
    max_attempts: int = 3,
    target_session_id: str | None = None,
) -> str:
    row, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=target_session_id,
        idempotency_key=key,
        max_attempts=max_attempts,
        now=_NOW,
    )
    return row.id


async def test_postgres_concurrent_claim_has_one_winner(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:claim")
    job_id = await _pg_job(store, "one-winner")

    first, second = await asyncio.gather(
        store.claim(job_id, _NOW, 60),
        store.claim(job_id, _NOW, 60),
    )

    assert sum(lease is not None for lease in (first, second)) == 1
    winner = first or second
    assert winner is not None
    assert winner.attempt == 1
    assert await store.claim(job_id, _NOW + timedelta(seconds=59), 60) is None
    row = await store.get(job_id)
    assert row is not None
    assert row.status is JobStatus.running
    assert row.lease_token == winner.token
    assert row.heartbeat_at == _NOW
    assert row.lease_expires_at == _NOW + timedelta(seconds=60)


async def test_postgres_expired_reclaim_increments_attempt_replaces_token_and_resets_progress(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:reclaim")
    job_id = await _pg_job(store, "reclaim")
    first = await store.claim(job_id, _NOW, 10)
    assert first is not None
    await store.progress(
        first,
        current=2,
        total=5,
        message="attempt 1",
        now=_NOW + timedelta(seconds=1),
    )

    second = await store.claim(job_id, _NOW + timedelta(seconds=12), 10)

    assert second is not None
    assert second.attempt == 2
    assert second.token != first.token
    row = await store.get(job_id)
    assert row is not None
    assert row.progress_current == 0
    assert row.progress_total is None
    assert row.progress_message is None
    assert row.progress_updated_at is None
    assert row.started_at == _NOW
    with pytest.raises(JobLeaseLostError):
        await store.heartbeat(first, _NOW + timedelta(seconds=13))
    with pytest.raises(JobLeaseLostError):
        await store.progress(
            first,
            current=3,
            total=5,
            message="stale attempt",
            now=_NOW + timedelta(seconds=13),
        )
    with pytest.raises(JobLeaseLostError):
        await store.requeue(
            first,
            JobError("provider_timeout", "safe"),
            _NOW + timedelta(seconds=20),
            _NOW + timedelta(seconds=13),
        )
    unchanged = await store.get(job_id)
    assert unchanged is not None
    assert unchanged.lease_token == second.token
    assert unchanged.status is JobStatus.running


async def test_postgres_claim_sql_enforces_attempt_ceiling(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:ceiling")
    job_id = await _pg_job(store, "ceiling", max_attempts=1)
    assert await store.claim(job_id, _NOW, 10) is not None
    assert await store.claim(job_id, _NOW + timedelta(seconds=11), 10) is None
    assert await store.dispatchable(_NOW + timedelta(seconds=11), 100) == []
    assert await store.exhausted(_NOW + timedelta(seconds=11), 100) == [job_id]


async def test_postgres_heartbeat_and_progress_extend_current_lease_and_report_cancel(
    migrated_db: AsyncEngine,
) -> None:
    scope = "scope:lease-extension"
    store = PostgresJobStore(migrated_db, scope)
    job_id = await _pg_job(store, "lease-extension")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"), {"scope": scope}
        )
        await conn.execute(
            text(
                "UPDATE jobs SET cancel_requested_at = :requested "
                "WHERE id = :id AND scope_id = :scope"
            ),
            {
                "requested": _NOW + timedelta(seconds=1),
                "id": job_id,
                "scope": scope,
            },
        )

    assert await store.heartbeat(lease, _NOW + timedelta(seconds=5)) is True
    heartbeat_row = await store.get(job_id)
    assert heartbeat_row is not None
    assert heartbeat_row.heartbeat_at == _NOW + timedelta(seconds=5)
    assert heartbeat_row.lease_expires_at == _NOW + timedelta(seconds=65)

    progress = await store.progress(
        lease,
        current=1,
        total=None,
        message=None,
        now=_NOW + timedelta(seconds=6),
    )
    assert progress.cancel_requested is True
    assert progress.record.heartbeat_at == _NOW + timedelta(seconds=6)
    assert progress.record.progress_updated_at == _NOW + timedelta(seconds=6)
    assert progress.record.lease_expires_at == _NOW + timedelta(seconds=66)
    assert await store.claim(job_id, _NOW + timedelta(seconds=65), 60) is None


async def test_postgres_progress_is_monotonic_bigint_bounded_and_storage_safe(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:progress")
    job_id = await _pg_job(store, "progress")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None

    result = await store.progress(
        lease,
        current=4,
        total=10,
        message="batch 2",
        now=_NOW + timedelta(seconds=5),
    )

    assert result.record.progress_current == 4
    assert result.record.progress_total == 10
    assert result.record.progress_message == "batch 2"
    assert result.record.lease_expires_at == _NOW + timedelta(seconds=65)
    assert result.cancel_requested is False
    with pytest.raises(JobValidationError, match="progress_regression"):
        await store.progress(
            lease,
            current=3,
            total=10,
            message=None,
            now=_NOW + timedelta(seconds=6),
        )
    for current, total in [
        (11, 10),
        (-1, None),
        (True, None),
        (1.5, 2),
        (2**63, None),
        (1, 2**63),
    ]:
        with pytest.raises(JobValidationError, match="invalid_progress"):
            await store.progress(  # type: ignore[arg-type]
                lease,
                current=current,
                total=total,
                message=None,
                now=_NOW + timedelta(seconds=6),
            )
    for message in ["bad\x00message", "bad\ud800message"]:
        with pytest.raises(JobValidationError, match="storage_text_invalid"):
            await store.progress(
                lease,
                current=5,
                total=10,
                message=message,
                now=_NOW + timedelta(seconds=6),
            )
    unchanged = await store.get(job_id)
    assert unchanged is not None
    assert unchanged.progress_current == 4
    assert unchanged.lease_expires_at == _NOW + timedelta(seconds=65)


async def test_postgres_expired_owner_cannot_extend_or_requeue_lease(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:expired-owner")
    job_id = await _pg_job(store, "expired-owner", max_attempts=1)
    lease = await store.claim(job_id, _NOW, 10)
    assert lease is not None

    with pytest.raises(JobLeaseLostError):
        await store.heartbeat(lease, _NOW + timedelta(seconds=10))
    with pytest.raises(JobLeaseLostError):
        await store.progress(
            lease,
            current=1,
            total=1,
            message="too late",
            now=_NOW + timedelta(seconds=11),
        )
    with pytest.raises(JobLeaseLostError):
        await store.requeue(
            lease,
            JobError("provider_timeout", "safe"),
            _NOW + timedelta(seconds=20),
            _NOW + timedelta(seconds=11),
        )
    assert await store.exhausted(_NOW + timedelta(seconds=11), 100) == [job_id]


async def test_postgres_lease_transitions_normalize_utc_and_reject_naive_timestamps(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:lease-utc")
    job_id = await _pg_job(store, "lease-utc")
    naive = datetime(2026, 7, 14, 9, 0)
    local_zone = timezone(timedelta(hours=8))

    with pytest.raises(ValueError, match="lease_seconds"):
        await store.claim(job_id, _NOW, 0)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.claim(job_id, naive, 60)

    lease = await store.claim(job_id, _NOW.astimezone(local_zone), 60)
    assert lease is not None
    claimed = await store.get(job_id)
    assert claimed is not None
    assert claimed.heartbeat_at == _NOW
    assert claimed.lease_expires_at == _NOW + timedelta(seconds=60)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.heartbeat(lease, naive)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.progress(
            lease,
            current=1,
            total=None,
            message=None,
            now=naive,
        )
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.requeue(
            lease,
            JobError("provider_timeout", "safe"),
            naive,
            _NOW + timedelta(seconds=1),
        )
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.requeue(
            lease,
            JobError("provider_timeout", "safe"),
            _NOW + timedelta(seconds=5),
            naive,
        )


async def test_postgres_requeue_is_nonterminal_bounded_and_has_no_injection(
    migrated_db: AsyncEngine,
) -> None:
    scope = "scope:requeue"
    target_session_id = "target-requeue"
    await _target(migrated_db, scope, target_session_id)
    store = PostgresJobStore(
        migrated_db,
        scope,
        limits=JobLimits(error_message_max_chars=4),
    )
    job_id = await _pg_job(
        store,
        "requeue",
        target_session_id=target_session_id,
    )
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    local_zone = timezone(timedelta(hours=8))
    retry_at = (_NOW + timedelta(seconds=5)).astimezone(local_zone)
    updated_at = (_NOW + timedelta(seconds=1)).astimezone(local_zone)

    row = await store.requeue(
        lease,
        JobError("provider_timeout", "safe public message"),
        retry_at,
        updated_at,
    )

    assert row.status is JobStatus.queued
    assert row.attempt == 1
    assert row.next_attempt_at == _NOW + timedelta(seconds=5)
    assert row.updated_at == _NOW + timedelta(seconds=1)
    assert row.error_kind == "provider_timeout"
    assert row.error_message == "safe"
    assert row.lease_token is None
    assert row.lease_expires_at is None
    assert row.heartbeat_at is None
    assert row.finished_at is None
    assert row.injected_event_seq is None
    assert job_id not in await store.dispatchable(_NOW + timedelta(seconds=4), 100)
    assert job_id in await store.dispatchable(_NOW + timedelta(seconds=5), 100)
    events = [
        event async for event in PostgresEventStore(migrated_db, scope).read(target_session_id)
    ]
    assert not any(event.payload.get("job_id") == job_id for event in events)
    with pytest.raises(JobLeaseLostError):
        await store.requeue(
            lease,
            JobError("provider_timeout", "safe"),
            _NOW + timedelta(seconds=10),
            _NOW + timedelta(seconds=2),
        )


async def test_postgres_requeue_rejects_exhausted_attempt_without_mutation(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:requeue-exhausted")
    job_id = await _pg_job(store, "requeue-exhausted", max_attempts=1)
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None

    with pytest.raises(JobValidationError, match="attempts_exhausted"):
        await store.requeue(
            lease,
            JobError("provider_timeout", "safe"),
            _NOW + timedelta(seconds=5),
            _NOW + timedelta(seconds=1),
        )

    row = await store.get(job_id)
    assert row is not None
    assert row.status is JobStatus.running
    assert row.lease_token == lease.token
