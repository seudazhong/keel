"""Fixtures for integration tests (live Postgres / Redis).

Postgres tests require an explicit ``KEEL_TEST_DATABASE_URL`` whose database is
named ``keel_test``. Destructive fixtures refuse missing or live-database URLs
before migrations or truncation. Reachable services are still required by CI.
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_DEFAULT_REDIS = "redis://localhost:6379/0"
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _assert_test_database_name(database: str | None) -> None:
    if database != "keel_test":
        raise pytest.UsageError(
            f"integration tests are refusing database {database!r}; expected 'keel_test'"
        )


def _require_test_database_url() -> str:
    raw = os.environ.get("KEEL_TEST_DATABASE_URL")
    if not raw:
        raise pytest.UsageError(
            "KEEL_TEST_DATABASE_URL must be set explicitly to the isolated keel_test database"
        )
    try:
        database = make_url(raw).database
    except ArgumentError as exc:
        raise pytest.UsageError("KEEL_TEST_DATABASE_URL is not a valid SQLAlchemy URL") from exc
    _assert_test_database_name(database)
    return raw


# psycopg's async mode does not support Windows' ProactorEventLoop. On Windows,
# force a SelectorEventLoop; Linux/CI already default to a selector loop, so the
# fixture is overridden only there (avoids pytest-asyncio's deprecation warning).
if sys.platform == "win32":

    @pytest.fixture
    def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
        return asyncio.WindowsSelectorEventLoopPolicy()


@pytest_asyncio.fixture
async def pg_engine() -> AsyncIterator[AsyncEngine]:
    url = _require_test_database_url()
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        await engine.dispose()
        pytest.skip("Postgres not available")
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[aioredis.Redis]:
    url = os.environ.get("KEEL_TEST_REDIS_URL", _DEFAULT_REDIS)
    client: aioredis.Redis = aioredis.Redis.from_url(url, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("Redis not available")
    yield client
    await client.aclose()


def _upgrade_head(url: str) -> None:
    """Run Alembic migrations to head against ``url`` (sync; call in a thread)."""
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(cfg, "head")


@pytest_asyncio.fixture
async def migrated_db(pg_engine: AsyncEngine) -> AsyncIterator[AsyncEngine]:
    """A Postgres engine with the schema migrated to head and a clean slate."""
    url = _require_test_database_url()
    await asyncio.to_thread(_upgrade_head, url)
    async with pg_engine.begin() as conn:
        database = await conn.scalar(text("SELECT current_database()"))
        _assert_test_database_name(str(database) if database is not None else None)
        await conn.execute(
            text(
                "TRUNCATE kb_chunks, knowledge_idempotency, kb_document_versions, "
                "kb_documents, knowledge_bases, job_dispatch_outbox, jobs, "
                "consolidation_cursors, "
                "memory_proposals, message_embeddings, events, sessions, memory_blocks, "
                "memory_block_versions, connector_deliveries, connector_cursors, "
                "connector_items, connector_resources, connector_binding_targets, "
                "connector_bindings, connector_tokens, "
                "connector_outbox, connector_active_scopes, connector_webhook_routes, "
                "oauth_states, "
                "webhook_deliveries, archival, schedules, approvals, run_control, "
                "im_reply_dispatch_index, im_reply_intents, im_route_index, "
                "im_channel_mappings, runs, "
                "run_dispatch_outbox, "
                "github_webhook_deliveries, github_sync_state, github_repositories, "
                "github_installations, repo_sync_ledger, project_runs, project_worktrees, "
                "patch_writeback_ledger, patch_generation_requests, "
                "patch_proposal_outbox, patch_proposals, "
                "project_quotas, projects, "
                "erasure_steps, erasure_requests, event_tombstones, retention_policies, "
                "session_access, agent_access, "
                "resource_grants, agents, memberships, oidc_identities, organizations, users"
            )
        )
    yield pg_engine
