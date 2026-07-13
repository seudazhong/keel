"""Consolidation executor drives the real consolidate_memory chain and reads writes back."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.testing.record_replay import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_worker.evals.database import case_scope, cleanup_scope
from keel_worker.evals.loader import event_id_for
from keel_worker.evals.memory_runner import run_consolidation_case
from keel_worker.evals.models import ConsolidationCase, ConsolidationExpected

pytestmark = pytest.mark.integration


def _propose_turn(case_id: str) -> list[ProviderChunk]:
    return [
        ProviderChunk(
            tool_call=ToolCall(
                id="call_propose",
                name="memory_propose_rewrite",
                arguments={
                    "block": "human",
                    "proposed_value": "The user prefers to be called Sam and writes in English.",
                    "reason": "stated preference",
                    "confidence": 0.95,
                    "source_event_ids": [event_id_for(case_id, 0)],
                },
            ),
            finish_reason=FinishReason.tool_use,
        )
    ]


def _final_turn() -> list[ProviderChunk]:
    return [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]


async def test_consolidation_executor_produces_proposal(migrated_db: AsyncEngine) -> None:
    case = ConsolidationCase(
        version=1,
        suite="consolidation",
        id="con-en-preference",
        model="eval/scripted",
        messages=[
            {"role": "user", "text": "Call me Sam and always reply in English."},
            {"role": "assistant", "text": "Understood."},
        ],
        expected=ConsolidationExpected(
            required_core_claims=["prefers to be called Sam"], min_proposals=1, max_proposals=1
        ),
    )
    scope = case_scope("v1", case.id)
    await cleanup_scope(migrated_db, scope)
    provider = ScriptedProviderGateway([_propose_turn(case.id), _final_turn()])
    actual = await run_consolidation_case(
        case, engine=migrated_db, provider=provider, embedder=FakeEmbedder(), dataset_version="v1"
    )
    assert actual.status == "completed"
    assert actual.cursor_advanced is True
    assert len(actual.proposals) == 1
    assert actual.proposals[0].block == "human"
    assert event_id_for(case.id, 0) in actual.proposals[0].source_event_ids
    await cleanup_scope(migrated_db, scope)
