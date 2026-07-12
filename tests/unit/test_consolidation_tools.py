"""Unit tests for consolidation tool validators + fail-closed behavior."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import create_async_engine

from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.tools import (
    ArchivalConsolidateInsertTool,
    ProposeRewriteTool,
    validate_archival_insert,
    validate_propose_rewrite,
)
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ToolContext

_DUMMY_URL = "postgresql+psycopg://localhost:5432/keel"


def _ctx() -> ConsolidationRunContext:
    return ConsolidationRunContext(
        allowed_event_ids=frozenset({1, 2, 3}),
        allowed_user_event_ids=frozenset({1}),
    )


def _tool_ctx() -> ToolContext:
    return ToolContext(scope_id="web:local", session_id="consolidation:web:local:r1")


def test_propose_validator_accepts_grounded_write() -> None:
    assert (
        validate_propose_rewrite(
            {
                "block": "human",
                "proposed_value": "likes tea",
                "reason": "stated",
                "source_event_ids": [1, 2],
            },
            _ctx(),
            block_max_chars=2000,
        )
        is None
    )


def test_propose_validator_rejects_unknown_block() -> None:
    error = validate_propose_rewrite(
        {"block": "system", "proposed_value": "x", "reason": "y", "source_event_ids": [1]},
        _ctx(),
        block_max_chars=2000,
    )
    assert error is not None and "block" in error


def test_propose_validator_requires_user_evidence() -> None:
    error = validate_propose_rewrite(
        {"block": "human", "proposed_value": "x", "reason": "y", "source_event_ids": [2, 3]},
        _ctx(),
        block_max_chars=2000,
    )
    assert error is not None and "user message" in error


def test_propose_validator_rejects_out_of_batch_citation() -> None:
    error = validate_propose_rewrite(
        {"block": "human", "proposed_value": "x", "reason": "y", "source_event_ids": [1, 99]},
        _ctx(),
        block_max_chars=2000,
    )
    assert error is not None and "current batch" in error


def test_archival_validator_enforces_confidence_floor() -> None:
    error = validate_archival_insert(
        {"content": "fact", "confidence": 0.5, "source_event_ids": [1]},
        _ctx(),
        min_confidence=0.8,
        content_max_chars=2000,
    )
    assert error is not None and "threshold" in error


def test_archival_validator_rejects_boolean_confidence() -> None:
    error = validate_archival_insert(
        {"content": "fact", "confidence": True, "source_event_ids": [1]},
        _ctx(),
        min_confidence=0.8,
        content_max_chars=2000,
    )
    assert error is not None and "number" in error


async def test_propose_tool_records_validation_error_without_db() -> None:
    engine = create_async_engine(_DUMMY_URL)
    run_context = _ctx()
    tool = ProposeRewriteTool(engine, run_context)
    result = await tool.run(
        {"block": "system", "proposed_value": "x", "reason": "y", "source_event_ids": [1]},
        _tool_ctx(),
    )
    assert result.ok is False
    assert run_context.validation_errors == 1
    assert run_context.successful_actions == 0
    await engine.dispose()


async def test_archival_tool_records_validation_error_without_db() -> None:
    engine = create_async_engine(_DUMMY_URL)
    run_context = _ctx()
    tool = ArchivalConsolidateInsertTool(engine, FakeEmbedder(), run_context)
    result = await tool.run(
        {"content": "fact", "confidence": 0.1, "source_event_ids": [1]},
        _tool_ctx(),
    )
    assert result.ok is False
    assert run_context.validation_errors == 1
    await engine.dispose()
