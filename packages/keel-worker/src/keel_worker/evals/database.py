"""Fail-closed eval database access + per-case scope cleanup.

The eval harness only ever connects to ``KEEL_EVAL_DATABASE_URL`` and only when
its database is exactly ``keel_eval`` or ``keel_test``. There is deliberately no
fallback to ``KEEL_DATABASE_URL`` — a missing/live/malformed URL aborts. After
connecting we re-check ``SELECT current_database()`` in case the URL lied.
"""

from __future__ import annotations

import os

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

ALLOWED_EVAL_DATABASES = ("keel_eval", "keel_test")
EVAL_ENV_VAR = "KEEL_EVAL_DATABASE_URL"

# Deletion order respects FKs: message_embeddings.event_id -> events (CASCADE) and
# events.session_id -> sessions; children first so a scoped delete never orphans.
EVAL_CLEANUP_TABLES: tuple[str, ...] = (
    "message_embeddings",
    "events",
    "sessions",
    "memory_block_versions",
    "memory_blocks",
    "memory_proposals",
    "archival",
    "consolidation_cursors",
    "schedules",
    "approvals",
)

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


class EvalDatabaseError(Exception):
    """Raised when the eval DB URL is missing, malformed, or points at a live database."""


def assert_eval_database_name(database: str | None) -> None:
    if database not in ALLOWED_EVAL_DATABASES:
        raise EvalDatabaseError(
            f"refusing eval database {database!r}; expected one of {ALLOWED_EVAL_DATABASES}"
        )


def require_eval_database_url() -> str:
    """Return the eval DB URL after validating its database name (no fallback)."""
    raw = os.environ.get(EVAL_ENV_VAR)
    if not raw:
        raise EvalDatabaseError(
            f"{EVAL_ENV_VAR} must be set explicitly to the isolated keel_eval/keel_test database"
        )
    try:
        database = make_url(raw).database
    except ArgumentError as exc:
        raise EvalDatabaseError(f"{EVAL_ENV_VAR} is not a valid SQLAlchemy URL") from exc
    assert_eval_database_name(database)
    return raw


def create_eval_engine() -> AsyncEngine:
    return create_async_engine(require_eval_database_url())


async def assert_current_database(engine: AsyncEngine) -> None:
    """Defense in depth: re-check the connected database name after connect."""
    async with engine.connect() as conn:
        database = await conn.scalar(text("SELECT current_database()"))
    assert_eval_database_name(str(database) if database is not None else None)


def case_scope(dataset_version: str, case_id: str) -> str:
    return f"eval:{dataset_version}:{case_id}"


async def cleanup_scope(engine: AsyncEngine, scope_id: str) -> None:
    """Delete every eval-owned row for ``scope_id`` (run on start and in finally)."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        for table in EVAL_CLEANUP_TABLES:
            await conn.execute(
                text(f"DELETE FROM {table} WHERE scope_id = :scope"), {"scope": scope_id}
            )
