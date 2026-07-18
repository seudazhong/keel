"""Structured-output review engine over the existing provider seam (WS-R).

Reuses :class:`keel_core.protocols.ProviderGateway` — the *same* provider path the agent loop
uses — rather than opening a second, policy-bypassing route to a model. The review agent has
no tools, so this is a single bounded provider turn (plus a small, capped repair loop) that
must return one JSON object matching the review contract. A provider failure or an
unrepairable malformed response fails closed with :class:`ReviewProviderError`.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from keel_core.protocols import ProviderGateway, ProviderRequest, Usage

from .errors import ReviewBoundsExceeded, ReviewProviderError, ReviewValidationError
from .models import (
    MAX_LIMITATION_CHARS,
    MAX_LIMITATIONS,
    ReviewFinding,
)
from .prompts import REPAIR_INSTRUCTION

DEFAULT_MAX_REPAIRS = 1


@dataclass(frozen=True, slots=True)
class ReviewEngineResult:
    summary: str
    findings: tuple[ReviewFinding, ...]
    limitations: tuple[str, ...]
    usage: Usage


def _extract_json_object(text: str) -> str:
    """Best-effort isolation of the single JSON object from a model response."""
    stripped = text.strip()
    if stripped.startswith("```"):
        # Drop a leading ```json / ``` fence and the trailing fence, if present.
        without_open = stripped.split("\n", 1)[1] if "\n" in stripped else ""
        stripped = (
            without_open.rsplit("```", 1)[0].strip() if "```" in without_open else without_open
        )
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ReviewProviderError("provider response contained no JSON object")
    return stripped[start : end + 1]


def _parse_result(
    text: str, *, max_findings: int
) -> tuple[str, tuple[ReviewFinding, ...], tuple[str, ...]]:
    try:
        payload = json.loads(_extract_json_object(text))
    except (json.JSONDecodeError, ReviewProviderError) as exc:
        raise ReviewProviderError(f"provider response was not valid JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ReviewProviderError("provider response JSON was not an object")

    raw_findings = payload.get("findings", [])
    if not isinstance(raw_findings, Sequence) or isinstance(raw_findings, str | bytes):
        raise ReviewProviderError("`findings` must be a JSON array")
    if len(raw_findings) > max_findings:
        raise ReviewBoundsExceeded(
            f"provider returned {len(raw_findings)} findings; the limit is {max_findings}"
        )
    findings: list[ReviewFinding] = []
    for item in raw_findings:
        try:
            findings.append(ReviewFinding.from_model_output(item))
        except (ReviewValidationError, ReviewBoundsExceeded) as exc:
            raise ReviewProviderError(f"invalid finding in provider response: {exc}") from exc

    summary_raw = payload.get("summary", "")
    summary = summary_raw.strip() if isinstance(summary_raw, str) else ""
    summary = summary[: MAX_LIMITATION_CHARS * 4]

    raw_limitations = payload.get("limitations", [])
    limitations: list[str] = []
    if isinstance(raw_limitations, Sequence) and not isinstance(raw_limitations, str | bytes):
        for entry in list(raw_limitations)[:MAX_LIMITATIONS]:
            if isinstance(entry, str) and entry.strip():
                limitations.append(entry.strip()[:MAX_LIMITATION_CHARS])
    return summary, tuple(findings), tuple(limitations)


@dataclass
class ReviewEngine:
    """Drive one structured review turn over a provider gateway with a bounded repair loop."""

    provider: ProviderGateway
    max_repairs: int = DEFAULT_MAX_REPAIRS

    async def run(
        self, *, model: str, messages: list[dict[str, Any]], max_findings: int
    ) -> ReviewEngineResult:
        conversation = list(messages)
        total_usage = Usage()
        last_error: Exception | None = None
        attempts = self.max_repairs + 1
        for attempt in range(attempts):
            text, usage = await self._one_turn(model=model, messages=conversation)
            total_usage = total_usage + usage
            try:
                summary, findings, limitations = _parse_result(text, max_findings=max_findings)
            except (ReviewProviderError, ReviewBoundsExceeded) as exc:
                last_error = exc
                if attempt + 1 >= attempts:
                    break
                conversation = [
                    *conversation,
                    {"role": "assistant", "content": text},
                    {"role": "user", "content": REPAIR_INSTRUCTION},
                ]
                continue
            return ReviewEngineResult(
                summary=summary,
                findings=findings,
                limitations=limitations,
                usage=total_usage,
            )
        raise ReviewProviderError(
            f"provider did not return a valid review after {attempts} attempt(s): {last_error}"
        )

    async def _one_turn(self, *, model: str, messages: list[dict[str, Any]]) -> tuple[str, Usage]:
        request = ProviderRequest(model=model, messages=messages, tools=[])
        chunks: list[str] = []
        usage = Usage()
        try:
            async for chunk in self.provider.stream(request):
                if chunk.delta:
                    chunks.append(chunk.delta)
                if chunk.usage is not None:
                    usage = chunk.usage
        except ReviewProviderError:
            raise
        except Exception as exc:  # noqa: BLE001 — provider transport failures fail closed
            raise ReviewProviderError(f"provider call failed: {exc.__class__.__name__}") from exc
        return "".join(chunks), usage


__all__ = ["DEFAULT_MAX_REPAIRS", "ReviewEngine", "ReviewEngineResult"]
