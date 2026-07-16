"""Acceptance tests for the demo data bootstrap (scripts/seed_demo_data.py).

Covers the safety properties required of an opt-in demo seeding tool:
confirmation gating, wrong-environment refusal, idempotent replay, no
destructive mutation of unrelated data, and seeded searchability. Uses the
isolated ``keel_test`` database (via the shared ``migrated_db`` fixture) and
``mode="fake"`` throughout so nothing here depends on network access to an
embedding provider.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings, get_settings
from keel_core.demo_guard import DemoGuardError
from keel_core.loop import admit
from keel_core.state import PostgresEventStore
from keel_worker.demo_bootstrap import run_bootstrap
from keel_worker.demo_bootstrap.cli import (
    EXIT_CONFIRMATION_REQUIRED,
    EXIT_GUARD_REFUSED,
    EXIT_OK,
)
from keel_worker.demo_bootstrap.cli import (
    main as cli_main,
)

pytestmark = pytest.mark.integration


def _demo_settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "app_env": "test",
        "database_url": os.environ["KEEL_TEST_DATABASE_URL"],
        "redis_url": os.environ.get("KEEL_TEST_REDIS_URL", "redis://localhost:6379/0"),
    }
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


async def _counts(engine: AsyncEngine) -> dict[str, int]:
    tables = (
        "knowledge_bases",
        "kb_documents",
        "kb_document_versions",
        "kb_chunks",
        "jobs",
        "events",
        "sessions",
    )
    result: dict[str, int] = {}
    async with engine.connect() as conn:
        for table in tables:
            result[table] = int(await conn.scalar(text(f"SELECT count(*) FROM {table}")))
    return result


async def test_idempotent_replay_reuses_same_resources(migrated_db: AsyncEngine) -> None:
    scope = f"demo-bootstrap:replay:{uuid.uuid4().hex}"
    settings = _demo_settings()

    first = await run_bootstrap(settings=settings, scope_id=scope, mode="fake", engine=migrated_db)
    assert first.knowledge_base.replayed is False
    assert all(not doc.replayed for doc in first.documents)
    assert all(doc.status == "active" for doc in first.documents)
    assert all(doc.job_status == "succeeded" for doc in first.documents)
    counts_after_first = await _counts(migrated_db)

    second = await run_bootstrap(settings=settings, scope_id=scope, mode="fake", engine=migrated_db)
    assert second.knowledge_base.id == first.knowledge_base.id
    assert second.knowledge_base.replayed is True
    assert [doc.id for doc in second.documents] == [doc.id for doc in first.documents]
    assert [doc.active_version_id for doc in second.documents] == [
        doc.active_version_id for doc in first.documents
    ]
    assert all(doc.replayed for doc in second.documents)
    assert second.session.created is False
    assert second.session.message_count == first.session.message_count == 1

    counts_after_second = await _counts(migrated_db)
    assert counts_after_second == counts_after_first, "replay must not duplicate any row"


async def test_seeded_documents_are_searchable_with_citations(migrated_db: AsyncEngine) -> None:
    scope = f"demo-bootstrap:search:{uuid.uuid4().hex}"
    settings = _demo_settings()

    result = await run_bootstrap(settings=settings, scope_id=scope, mode="fake", engine=migrated_db)

    assert len(result.search_samples) == len(result.documents) == 3
    for sample in result.search_samples:
        assert sample.hit_count > 0, f"expected at least one hit for query {sample.query!r}"
        assert sample.mode in {"hybrid", "lexical", "lexical-degraded"}

    # Independently verify citation fields via the same production search
    # contract the HTTP API uses, rather than trusting the bootstrap's own
    # summary numbers.
    from typing import cast

    from keel_core.embeddings import FakeEmbedder
    from keel_core.jobs import PostgresJobStore
    from keel_core.knowledge import KnowledgeService, KnowledgeStore, PostgresKnowledgeStore
    from keel_core.knowledge.search import KnowledgeSearcher
    from keel_worker.demo_bootstrap.runner import FAKE_EMBEDDING_DIM, FAKE_EMBEDDING_MODEL

    store = PostgresKnowledgeStore(
        migrated_db, scope, document_max_bytes=settings.knowledge_document_max_bytes
    )
    jobs = PostgresJobStore(migrated_db, scope)
    embedder = FakeEmbedder(dim=FAKE_EMBEDDING_DIM, model=FAKE_EMBEDDING_MODEL)
    searcher = KnowledgeSearcher(migrated_db, scope, embedder)
    service = KnowledgeService(
        cast(KnowledgeStore, store),
        jobs,
        settings,
        searcher=searcher,
        embedding_model=embedder.model,
        embedding_dim=embedder.dim,
    )
    hits, _status = await service.search(result.knowledge_base.id, "approvals", k=3)
    assert hits, "expected at least one search hit"
    citation = hits[0].citation
    assert citation.kb_id == result.knowledge_base.id
    assert citation.document_id in {doc.id for doc in result.documents}
    assert citation.chunk_id
    assert citation.char_start >= 0
    assert citation.char_end >= citation.char_start


async def test_no_destructive_mutation_of_unrelated_scope_data(migrated_db: AsyncEngine) -> None:
    other_scope = f"demo-bootstrap:unrelated:{uuid.uuid4().hex}"
    other_session_id = f"{other_scope}:preexisting"
    store = PostgresEventStore(migrated_db, other_scope)
    await admit(store, other_session_id, other_scope, "pre-existing message, must survive")
    before = [event async for event in store.read(other_session_id)]
    assert len(before) == 1

    scope = f"demo-bootstrap:destructive:{uuid.uuid4().hex}"
    settings = _demo_settings()
    await run_bootstrap(settings=settings, scope_id=scope, mode="fake", engine=migrated_db)
    # Bootstrap the same demo scope a second time too, to further stress that
    # nothing about a repeat run reaches into unrelated scopes.
    await run_bootstrap(settings=settings, scope_id=scope, mode="fake", engine=migrated_db)

    after = [event async for event in store.read(other_session_id)]
    assert after == before, "unrelated pre-existing session data must be untouched"


async def test_cli_refuses_without_confirmation_or_dry_run(
    monkeypatch: pytest.MonkeyPatch, migrated_db: AsyncEngine
) -> None:
    scope = f"demo-bootstrap:confirm:{uuid.uuid4().hex}"
    monkeypatch.setenv("KEEL_APP_ENV", "test")
    monkeypatch.setenv("KEEL_DATABASE_URL", os.environ["KEEL_TEST_DATABASE_URL"])
    monkeypatch.setenv(
        "KEEL_REDIS_URL", os.environ.get("KEEL_TEST_REDIS_URL", "redis://localhost:6379/0")
    )
    get_settings.cache_clear()
    try:
        before = await _counts(migrated_db)
        exit_code = cli_main(["--scope", scope, "--mode", "fake"])
        assert exit_code == EXIT_CONFIRMATION_REQUIRED
        after = await _counts(migrated_db)
        assert after == before, "refused run must not mutate anything"

        dry_run_exit = cli_main(["--scope", scope, "--mode", "fake", "--dry-run"])
        assert dry_run_exit == EXIT_OK
        after_dry_run = await _counts(migrated_db)
        assert after_dry_run == before, "--dry-run must not mutate anything"
    finally:
        get_settings.cache_clear()


async def test_cli_refuses_outside_dev_demo_environment(
    monkeypatch: pytest.MonkeyPatch, migrated_db: AsyncEngine
) -> None:
    scope = f"demo-bootstrap:wrongenv:{uuid.uuid4().hex}"
    monkeypatch.setenv("KEEL_APP_ENV", "production")
    monkeypatch.setenv("KEEL_DATABASE_URL", os.environ["KEEL_TEST_DATABASE_URL"])
    monkeypatch.setenv(
        "KEEL_REDIS_URL", os.environ.get("KEEL_TEST_REDIS_URL", "redis://localhost:6379/0")
    )
    get_settings.cache_clear()
    try:
        before = await _counts(migrated_db)
        exit_code = cli_main(["--scope", scope, "--mode", "fake", "--yes"])
        assert exit_code == EXIT_GUARD_REFUSED
        after = await _counts(migrated_db)
        assert after == before, "a refused environment must never mutate anything"
    finally:
        get_settings.cache_clear()


async def test_cli_refuses_non_loopback_database_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_APP_ENV", "dev")
    monkeypatch.setenv(
        "KEEL_DATABASE_URL", "postgresql+psycopg://keel:keel@db.example.com:5432/keel_test"
    )
    monkeypatch.setenv("KEEL_REDIS_URL", "redis://localhost:6379/0")
    get_settings.cache_clear()
    try:
        exit_code = cli_main(["--yes", "--mode", "fake"])
        assert exit_code == EXIT_GUARD_REFUSED
    finally:
        get_settings.cache_clear()


async def test_run_bootstrap_raises_guard_error_directly_for_bad_settings() -> None:
    bad_settings = Settings(
        app_env="production",
        database_url="postgresql+psycopg://keel:keel@localhost:5432/keel_test",
        redis_url="redis://localhost:6379/0",
    )
    with pytest.raises(DemoGuardError):
        await run_bootstrap(settings=bad_settings, mode="fake")


async def test_bootstrap_result_is_json_serializable(migrated_db: AsyncEngine) -> None:
    # Sanity check that DemoBootstrapResult.to_dict() round-trips through json,
    # since the CLI's --json flag relies on this.
    import json

    settings = _demo_settings()
    scope = f"demo-bootstrap:json:{uuid.uuid4().hex}"
    result = await run_bootstrap(settings=settings, scope_id=scope, mode="fake", engine=migrated_db)
    payload = json.dumps(result.to_dict(), default=str)
    decoded = json.loads(payload)
    assert decoded["scope_id"] == scope
    assert decoded["knowledge_base"]["id"] == result.knowledge_base.id
