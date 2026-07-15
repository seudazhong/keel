"""Unit tests for the always-tainted, bounded Knowledge search tool."""

from __future__ import annotations

from typing import Any, cast

import pytest

from keel_core.knowledge.models import (
    KnowledgeCitation,
    KnowledgeHit,
    KnowledgeSearchMode,
    KnowledgeSearchStatus,
    KnowledgeStorageError,
    new_knowledge_base_id,
    new_knowledge_chunk_id,
    new_knowledge_document_id,
    new_knowledge_version_id,
)
from keel_core.knowledge.search import KnowledgeSearcher
from keel_core.knowledge.tools import KnowledgeSearchTool
from keel_core.protocols import ToolContext
from keel_core.types import ContentTaint


def _hit(rank: int, snippet: str = "Install Keel.") -> KnowledgeHit:
    return KnowledgeHit(
        snippet=snippet,
        rank=rank,
        heading_path=["Setup"],
        citation=KnowledgeCitation(
            id=f"cite_{rank}",
            kb_id=new_knowledge_base_id(),
            document_id=new_knowledge_document_id(),
            document_version_id=new_knowledge_version_id(),
            chunk_id=new_knowledge_chunk_id(),
            title="Guide.md",
            source_uri="file:///Guide.md",
            ordinal=rank - 1,
            char_start=0,
            char_end=len(snippet),
            label=f"Guide.md#chunk-{rank}",
        ),
    )


class _FakeSearcher:
    def __init__(
        self,
        hits: list[KnowledgeHit],
        status: KnowledgeSearchStatus,
        *,
        scope_id: str = "u:1",
        error: Exception | None = None,
    ) -> None:
        self.scope_id = scope_id
        self.hits = hits
        self.status = status
        self.error = error
        self.calls: list[tuple[str, str, int]] = []

    async def search(
        self, kb_id: str, query: str, *, k: int = 5
    ) -> tuple[list[KnowledgeHit], KnowledgeSearchStatus]:
        self.calls.append((kb_id, query, k))
        if self.error is not None:
            raise self.error
        return self.hits, self.status


def _tool(searcher: _FakeSearcher, *, max_chars: int = 8_000) -> KnowledgeSearchTool:
    return KnowledgeSearchTool(
        cast(KnowledgeSearcher, cast(Any, searcher)),
        output_max_chars=max_chars,
    )


def _ctx(scope_id: str = "u:1") -> ToolContext:
    return ToolContext(scope_id=scope_id, session_id="s1")


def test_kb_search_schema_is_exact_and_read_only() -> None:
    tool = _tool(_FakeSearcher([], KnowledgeSearchStatus(mode=KnowledgeSearchMode.lexical)))

    assert tool.name == "kb_search"
    assert tool.writes is False
    assert tool.input_schema() == {
        "type": "object",
        "properties": {
            "kb_id": {"type": "string"},
            "query": {"type": "string"},
            "k": {"type": "integer", "minimum": 1, "maximum": 10},
        },
        "required": ["kb_id", "query"],
        "additionalProperties": False,
    }


async def test_kb_search_formats_numbered_snippets_citations_and_taint() -> None:
    kb_id = new_knowledge_base_id()
    searcher = _FakeSearcher(
        [_hit(1), _hit(2, "Configure the server.")],
        KnowledgeSearchStatus(mode=KnowledgeSearchMode.hybrid),
    )

    result = await _tool(searcher).run({"kb_id": kb_id, "query": "install", "k": 2}, _ctx())

    assert result.ok is True
    assert result.output == (
        "[1] Guide.md#chunk-1\nInstall Keel.\n\n[2] Guide.md#chunk-2\nConfigure the server."
    )
    assert result.taint is ContentTaint.tainted
    assert [citation.id for citation in result.citations] == ["cite_1", "cite_2"]
    assert result.citations[0].source == "knowledge"
    assert result.citations[0].metadata["document_version_id"]
    assert searcher.calls == [(kb_id, "install", 2)]


@pytest.mark.parametrize(
    "status",
    [
        KnowledgeSearchStatus(mode=KnowledgeSearchMode.lexical),
        KnowledgeSearchStatus(
            mode=KnowledgeSearchMode.lexical_degraded,
            semantic_error="embedding_unavailable",
        ),
    ],
)
async def test_kb_search_empty_and_degraded_results_are_tainted(
    status: KnowledgeSearchStatus,
) -> None:
    result = await _tool(_FakeSearcher([], status)).run(
        {"kb_id": new_knowledge_base_id(), "query": "missing"},
        _ctx(),
    )

    assert result.ok is True
    assert result.output == "No matching knowledge found."
    assert result.citations == []
    assert result.taint is ContentTaint.tainted


async def test_kb_search_bounds_output_and_citations_to_emitted_hits() -> None:
    searcher = _FakeSearcher(
        [_hit(1, "A" * 100), _hit(2, "B" * 100)],
        KnowledgeSearchStatus(mode=KnowledgeSearchMode.hybrid),
    )

    result = await _tool(searcher, max_chars=48).run(
        {"kb_id": new_knowledge_base_id(), "query": "bounded"},
        _ctx(),
    )

    assert len(result.output) == 48
    assert result.output.startswith("[1] Guide.md#chunk-1\n")
    assert [citation.id for citation in result.citations] == ["cite_1"]
    assert result.taint is ContentTaint.tainted


@pytest.mark.parametrize(
    "args",
    [
        {},
        {"kb_id": new_knowledge_base_id(), "query": "x", "extra": True},
        {"kb_id": new_knowledge_base_id(), "query": "x", "k": True},
        {"kb_id": new_knowledge_base_id(), "query": "x", "k": 11},
        {"kb_id": "not-a-kb", "query": "x"},
        {"kb_id": new_knowledge_base_id(), "query": " "},
    ],
)
async def test_kb_search_rejects_invalid_arguments_without_calling_search(
    args: dict[str, object],
) -> None:
    searcher = _FakeSearcher([], KnowledgeSearchStatus(mode=KnowledgeSearchMode.lexical))

    result = await _tool(searcher).run(args, _ctx())

    assert result.ok is False
    assert result.output == "Knowledge search arguments are invalid."
    assert result.taint is ContentTaint.tainted
    assert searcher.calls == []


async def test_kb_search_fails_closed_on_scope_mismatch() -> None:
    searcher = _FakeSearcher([], KnowledgeSearchStatus(mode=KnowledgeSearchMode.lexical))

    result = await _tool(searcher).run(
        {"kb_id": new_knowledge_base_id(), "query": "x"},
        _ctx("u:other"),
    )

    assert result.ok is False
    assert result.output == "Knowledge search scope does not match the agent scope."
    assert result.taint is ContentTaint.tainted
    assert searcher.calls == []


async def test_kb_search_exposes_only_bounded_domain_errors() -> None:
    searcher = _FakeSearcher(
        [],
        KnowledgeSearchStatus(mode=KnowledgeSearchMode.lexical),
        error=KnowledgeStorageError(),
    )

    result = await _tool(searcher).run(
        {"kb_id": new_knowledge_base_id(), "query": "x"},
        _ctx(),
    )

    assert result.ok is False
    assert result.output == "Knowledge storage is temporarily unavailable."
    assert result.taint is ContentTaint.tainted
