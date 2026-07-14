"""Postgres durable-job state machine and exactly-once injection."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


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
