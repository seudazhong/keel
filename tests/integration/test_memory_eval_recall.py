"""Recall executor drives production search; degradation yields lexical-degraded."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import FakeEmbedder
from keel_worker.evals.database import case_scope, cleanup_scope
from keel_worker.evals.memory_runner import run_recall_case
from keel_worker.evals.models import Message, RecallCase, RecallQuery, RecallSession

pytestmark = pytest.mark.integration


async def test_recall_session_hit(migrated_db: AsyncEngine) -> None:
    case = RecallCase(
        version=1,
        suite="recall",
        id="rec-basic",
        sessions=[
            RecallSession(
                label="cycling",
                messages=[
                    Message(role="user", text="I love cycling on weekends near the coast.")
                ],
            ),
            RecallSession(
                label="cooking",
                messages=[Message(role="user", text="I baked sourdough bread yesterday.")],
            ),
        ],
        queries=[
            RecallQuery(query="cycling coast", mode="session", expected_labels=["cycling"], k=5)
        ],
    )
    scope = case_scope("v1", case.id)
    await cleanup_scope(migrated_db, scope)
    actual = await run_recall_case(
        case, engine=migrated_db, embedder=FakeEmbedder(), dataset_version="v1"
    )
    result = actual.results[0]
    assert "cycling" in result.hit_labels
    await cleanup_scope(migrated_db, scope)


async def test_recall_degradation_is_lexical_degraded(migrated_db: AsyncEngine) -> None:
    case = RecallCase(
        version=1,
        suite="recall",
        id="rec-degrade",
        degrade_embeddings=True,
        sessions=[
            RecallSession(
                label="s1",
                messages=[Message(role="user", text="the quick brown fox")],
            )
        ],
        queries=[
            RecallQuery(
                query="quick brown fox",
                mode="session",
                expected_labels=["s1"],
                expected_recall_mode="lexical-degraded",
            )
        ],
    )
    scope = case_scope("v1", case.id)
    await cleanup_scope(migrated_db, scope)
    actual = await run_recall_case(
        case, engine=migrated_db, embedder=FakeEmbedder(), dataset_version="v1"
    )
    assert actual.results[0].recall_mode == "lexical-degraded"
    assert "s1" in actual.results[0].hit_labels
    await cleanup_scope(migrated_db, scope)
