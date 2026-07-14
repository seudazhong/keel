"""Postgres durable-job state machine and exactly-once injection."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.jobs import (
    JobLimits,
    JobStatus,
    JobValidationError,
    PostgresJobStore,
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
        await store.dispatchable(datetime(2026, 7, 14, 9, 0), 100)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.exhausted(datetime(2026, 7, 14, 9, 0), 100)


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
