"""The two constrained consolidation tools + their validators (spec §8–§10).

Both tools fail closed: an invalid call records a validation error on the shared
``ConsolidationRunContext`` (so the cursor will not advance) and returns a failed
``ToolResult``. The tool executor turns any raised exception into a failed result, so
infra errors are caught here too and counted. Every write must cite ``source_event_ids``
from the current batch, and at least one cited event must be a user message (the
user-evidence rule).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.proposals import MemoryProposalStore
from keel_core.embeddings import Embedder
from keel_core.protocols import ToolContext, ToolResult
from keel_core.search import ArchivalStore

_VALID_BLOCKS = ("persona", "human")


def _coerce_ids(raw: object) -> list[int] | None:
    if not isinstance(raw, list):
        return None
    ids: list[int] = []
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, int):
            return None
        ids.append(item)
    return ids


def _citation_error(ids: list[int] | None, run_context: ConsolidationRunContext) -> str | None:
    if ids is None:
        return "source_event_ids must be a list of integers"
    if not ids:
        return "source_event_ids must cite at least one event"
    cited = set(ids)
    if not cited <= run_context.allowed_event_ids:
        return "source_event_ids must reference events from the current batch"
    if not (cited & run_context.allowed_user_event_ids):
        return "at least one source event must be a user message"
    return None


def validate_propose_rewrite(
    args: dict[str, Any], run_context: ConsolidationRunContext, *, block_max_chars: int
) -> str | None:
    block = str(args.get("block", ""))
    if block not in _VALID_BLOCKS:
        return f"unknown block {block!r}; must be 'persona' or 'human'"
    value = str(args.get("proposed_value", ""))
    if not value.strip():
        return "proposed_value must be non-empty"
    if len(value) > block_max_chars:
        return f"proposed_value exceeds {block_max_chars} chars"
    if not str(args.get("reason", "")).strip():
        return "reason must be non-empty"
    confidence_raw = args.get("confidence")
    if confidence_raw is not None:
        if isinstance(confidence_raw, bool) or not isinstance(confidence_raw, int | float):
            return "confidence must be a number"
        if not (0.0 <= float(confidence_raw) <= 1.0):
            return "confidence must be between 0 and 1"
    return _citation_error(_coerce_ids(args.get("source_event_ids")), run_context)


def validate_archival_insert(
    args: dict[str, Any],
    run_context: ConsolidationRunContext,
    *,
    min_confidence: float,
    content_max_chars: int,
) -> str | None:
    content = str(args.get("content", ""))
    if not content.strip():
        return "content must be non-empty"
    if len(content) > content_max_chars:
        return f"content exceeds {content_max_chars} chars"
    confidence = args.get("confidence")
    if isinstance(confidence, bool) or not isinstance(confidence, int | float):
        return "confidence must be a number"
    if not (min_confidence <= float(confidence) <= 1.0):
        return f"confidence {confidence} is below the threshold {min_confidence}"
    return _citation_error(_coerce_ids(args.get("source_event_ids")), run_context)


class ProposeRewriteTool:
    """``memory_propose_rewrite``: propose a human-reviewed core-memory edit."""

    name = "memory_propose_rewrite"
    description = (
        "Propose a human-reviewed rewrite of a core memory block ('persona' or 'human'). "
        "The proposal is NOT applied automatically."
    )
    writes = True

    def __init__(
        self,
        engine: AsyncEngine,
        run_context: ConsolidationRunContext,
        *,
        block_max_chars: int = 2000,
    ) -> None:
        self._engine = engine
        self._run_context = run_context
        self._block_max_chars = block_max_chars

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "block": {"type": "string", "enum": list(_VALID_BLOCKS)},
                "proposed_value": {"type": "string"},
                "reason": {"type": "string"},
                "confidence": {"type": "number"},
                "source_event_ids": {"type": "array", "items": {"type": "integer"}},
            },
            "required": ["block", "proposed_value", "reason", "source_event_ids"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        error = validate_propose_rewrite(
            args, self._run_context, block_max_chars=self._block_max_chars
        )
        if error is not None:
            self._run_context.validation_errors += 1
            return ToolResult(ok=False, output=f"rejected: {error}")
        try:
            store = MemoryProposalStore(self._engine, ctx.scope_id)
            proposal_id, created = await store.propose(
                block=str(args["block"]),
                proposed_value=str(args["proposed_value"]),
                reason=str(args["reason"]),
                confidence=float(args.get("confidence", 1.0)),
                source_event_ids=_coerce_ids(args["source_event_ids"]) or [],
            )
        except Exception as exc:  # infra failure must block the cursor
            self._run_context.validation_errors += 1
            return ToolResult(ok=False, output=f"error: {exc.__class__.__name__}")
        self._run_context.successful_actions += 1
        state = "created" if created else "already proposed"
        return ToolResult(ok=True, output=f"proposal {proposal_id} {state}")


class ArchivalConsolidateInsertTool:
    """``archival_consolidate_insert``: persist a durable, deduplicated fact."""

    name = "archival_consolidate_insert"
    description = (
        "Persist a durable, standalone fact to archival memory for later recall. "
        "Deduplicated by content; requires a confidence and cited source events."
    )
    writes = True

    def __init__(
        self,
        engine: AsyncEngine,
        embedder: Embedder,
        run_context: ConsolidationRunContext,
        *,
        min_confidence: float = 0.8,
        content_max_chars: int = 2000,
        semantic_dedupe_distance: float = 0.05,
    ) -> None:
        self._engine = engine
        self._embedder = embedder
        self._run_context = run_context
        self._min_confidence = min_confidence
        self._content_max_chars = content_max_chars
        self._semantic_dedupe_distance = semantic_dedupe_distance

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "confidence": {"type": "number"},
                "source_event_ids": {"type": "array", "items": {"type": "integer"}},
            },
            "required": ["content", "confidence", "source_event_ids"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        error = validate_archival_insert(
            args,
            self._run_context,
            min_confidence=self._min_confidence,
            content_max_chars=self._content_max_chars,
        )
        if error is not None:
            self._run_context.validation_errors += 1
            return ToolResult(ok=False, output=f"rejected: {error}")
        try:
            store = ArchivalStore(self._engine, ctx.scope_id, self._embedder)
            row_id, created = await store.add_consolidated(
                str(args["content"]),
                source_event_ids=_coerce_ids(args["source_event_ids"]) or [],
                semantic_dedupe_distance=self._semantic_dedupe_distance,
            )
        except Exception as exc:  # infra failure must block the cursor
            self._run_context.validation_errors += 1
            return ToolResult(ok=False, output=f"error: {exc.__class__.__name__}")
        self._run_context.successful_actions += 1
        state = "inserted" if created else "merged"
        return ToolResult(ok=True, output=f"archival {row_id} {state}")
