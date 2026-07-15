"""Always-tainted Knowledge Base retrieval tool."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from keel_core.protocols import Citation, ToolContext, ToolResult
from keel_core.types import ContentTaint

from .models import KnowledgeError, KnowledgeHit, validate_knowledge_base_id
from .search import KnowledgeSearcher

_DEFAULT_OUTPUT_MAX_CHARS = 8_000
_INVALID_ARGUMENTS = "Knowledge search arguments are invalid."
_SCOPE_MISMATCH = "Knowledge search scope does not match the agent scope."
_NO_RESULTS = "No matching knowledge found."


class _KnowledgeSearchArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kb_id: str
    query: str = Field(min_length=1, max_length=2_000)
    k: int = Field(default=5, ge=1, le=10)

    @field_validator("kb_id")
    @classmethod
    def _validate_kb_id(cls, value: str) -> str:
        try:
            return validate_knowledge_base_id(value)
        except KnowledgeError as exc:
            raise ValueError("kb_id is invalid") from exc

    @field_validator("query")
    @classmethod
    def _validate_query(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("query contains a null byte")
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("query is not valid UTF-8") from exc
        normalized = value.strip()
        if not normalized:
            raise ValueError("query must not be blank")
        return normalized


def _generic_citation(hit: KnowledgeHit) -> Citation:
    domain = hit.citation
    metadata = domain.model_dump(exclude={"id", "label"})
    return Citation(
        id=domain.id,
        label=domain.label,
        source="knowledge",
        metadata=metadata,
    )


def _bounded_output(
    hits: list[KnowledgeHit],
    *,
    maximum: int,
) -> tuple[str, list[Citation]]:
    if not hits:
        return _NO_RESULTS[:maximum], []

    output = ""
    citations: list[Citation] = []
    for hit in hits:
        separator = "\n\n" if output else ""
        header = f"{separator}[{hit.rank}] {hit.citation.label}\n"
        remaining = maximum - len(output)
        if remaining < len(header):
            break
        output += header
        snippet_remaining = maximum - len(output)
        output += hit.snippet[:snippet_remaining]
        citations.append(_generic_citation(hit))
        if len(hit.snippet) > snippet_remaining:
            break
    return output, citations


class KnowledgeSearchTool:
    """Search one scope-bound Knowledge Base and return bounded cited snippets."""

    name = "kb_search"
    description = "Search active content in a Knowledge Base and return cited snippets."
    writes = False

    def __init__(
        self,
        searcher: KnowledgeSearcher,
        *,
        output_max_chars: int = _DEFAULT_OUTPUT_MAX_CHARS,
    ) -> None:
        if type(output_max_chars) is not int or output_max_chars < 1:
            raise ValueError("output_max_chars must be a positive integer")
        self._searcher = searcher
        self._output_max_chars = output_max_chars

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "kb_id": {"type": "string"},
                "query": {"type": "string"},
                "k": {"type": "integer", "minimum": 1, "maximum": 10},
            },
            "required": ["kb_id", "query"],
            "additionalProperties": False,
        }

    def _failure(self, output: str) -> ToolResult:
        return ToolResult(
            ok=False,
            output=output[: self._output_max_chars],
            taint=ContentTaint.tainted,
        )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        try:
            command = _KnowledgeSearchArgs.model_validate(args)
        except ValidationError:
            return self._failure(_INVALID_ARGUMENTS)

        if ctx.scope_id != self._searcher.scope_id:
            return self._failure(_SCOPE_MISMATCH)

        try:
            hits, _status = await self._searcher.search(
                command.kb_id,
                command.query,
                k=command.k,
            )
        except KnowledgeError as exc:
            return self._failure(exc.public_message)

        output, citations = _bounded_output(hits, maximum=self._output_max_chars)
        return ToolResult(
            ok=True,
            output=output,
            citations=citations,
            taint=ContentTaint.tainted,
        )


__all__ = ["KnowledgeSearchTool"]
