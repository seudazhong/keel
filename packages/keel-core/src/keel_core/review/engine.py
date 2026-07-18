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

from .errors import (
    ReviewBoundsExceeded,
    ReviewProviderError,
    ReviewProviderUnavailable,
    ReviewValidationError,
)
from .models import (
    MAX_LIMITATION_CHARS,
    MAX_LIMITATIONS,
    ReviewBudget,
    ReviewFinding,
)
from .pricing import PriceBook
from .prompts import REPAIR_INSTRUCTION

DEFAULT_MAX_REPAIRS = 1

# Provider exception class-name fragments that indicate a *transient* failure (transport,
# timeout, rate limit, upstream 5xx) — safe to retry rather than terminalize the run.
_TRANSIENT_PROVIDER_MARKERS = (
    "timeout",
    "ratelimit",
    "rate_limit",
    "serviceunavailable",
    "service_unavailable",
    "apiconnection",
    "connection",
    "internalservererror",
    "overloaded",
    "temporar",
    "unavailable",
    "badgateway",
    "gateway",
)


def _is_transient_provider_error(exc: Exception) -> bool:
    name = f"{exc.__class__.__module__}.{exc.__class__.__name__}".lower()
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and status_code in (408, 425, 429, 500, 502, 503, 504):
        return True
    return any(marker in name for marker in _TRANSIENT_PROVIDER_MARKERS)


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

    raw_findings = payload.get("findings", None)
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

    # A structured review MUST carry a non-empty summary. An empty object, a bare refusal, or a
    # ``{}``/``null`` summary is a contract violation that fails closed (the engine's bounded
    # repair loop gets one chance to correct it before the review is failed).
    summary_raw = payload.get("summary", None)
    if not isinstance(summary_raw, str) or not summary_raw.strip():
        raise ReviewProviderError("provider response is missing a non-empty `summary`")
    summary = summary_raw.strip()[: MAX_LIMITATION_CHARS * 4]

    raw_limitations = payload.get("limitations", [])
    if not isinstance(raw_limitations, Sequence) or isinstance(raw_limitations, str | bytes):
        raise ReviewProviderError("`limitations` must be a JSON array")
    limitations: list[str] = []
    for entry in list(raw_limitations)[:MAX_LIMITATIONS]:
        if isinstance(entry, str) and entry.strip():
            limitations.append(entry.strip()[:MAX_LIMITATION_CHARS])
    return summary, tuple(findings), tuple(limitations)


@dataclass
class ReviewEngine:
    """Drive one structured review turn over a provider gateway with a bounded repair loop."""

    provider: ProviderGateway
    max_repairs: int = DEFAULT_MAX_REPAIRS
    # Authoritative pricing. When set, the review's cost is computed from token usage and this
    # price book (not the provider's self-reported ``cost_usd``), so the cost ceiling is always
    # enforceable. An allowed-but-unpriced model fails closed inside :meth:`_authoritative_cost`.
    price_book: PriceBook | None = None

    async def run(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        max_findings: int,
        budget: ReviewBudget | None = None,
    ) -> ReviewEngineResult:
        # A review always runs under an explicit, bounded budget — never unlimited.
        budget = budget or ReviewBudget()
        conversation = list(messages)
        total_usage = Usage()
        last_error: Exception | None = None
        attempts = budget.max_provider_attempts
        for attempt in range(attempts):
            self._enforce_budget(total_usage, budget)
            try:
                text, usage = await self._one_turn(
                    model=model, messages=conversation, budget=budget
                )
            except ReviewProviderUnavailable as exc:
                # Transport/timeout/rate-limit mid-stream: surface the tokens/cost already
                # consumed (this turn's partial usage plus prior turns) so the coordinator can
                # durably charge it — the next attempt then gets only the remaining budget.
                partial = exc.usage if isinstance(exc.usage, Usage) else Usage()
                exc.usage = self._with_authoritative_cost(total_usage + partial, model)
                raise
            total_usage = self._with_authoritative_cost(total_usage + usage, model)
            self._enforce_budget(total_usage, budget)
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

    def _with_authoritative_cost(self, usage: Usage, model: str) -> Usage:
        """Replace the provider-reported cost with a price-book cost when one is configured.

        Fails closed (``ReviewValidationError``) for an allowed-but-unpriced model rather than
        trusting a possibly-zero provider cost that would silently disable the ceiling.
        """
        if self.price_book is None:
            return usage
        cost = self.price_book.cost_for(model, usage.prompt_tokens, usage.completion_tokens)
        return usage.model_copy(update={"cost_usd": cost})

    @staticmethod
    def _enforce_budget(usage: Usage, budget: ReviewBudget) -> None:
        """Fail closed the moment cumulative tokens or cost exceed the review's envelope."""
        total_tokens = usage.prompt_tokens + usage.completion_tokens
        if total_tokens > budget.token_budget:
            raise ReviewBoundsExceeded(
                f"review exceeded its {budget.token_budget}-token budget ({total_tokens} used)"
            )
        if usage.cost_usd > budget.cost_ceiling_usd:
            raise ReviewBoundsExceeded(
                f"review exceeded its ${budget.cost_ceiling_usd} cost ceiling"
            )

    async def _one_turn(
        self, *, model: str, messages: list[dict[str, Any]], budget: ReviewBudget
    ) -> tuple[str, Usage]:
        request = ProviderRequest(
            model=model,
            messages=messages,
            tools=[],
            max_output_tokens=budget.output_max_tokens,
        )
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
            if _is_transient_provider_error(exc):
                # Transport/timeout/rate-limit: retryable, do NOT terminalize the run. Carry the
                # partial usage streamed before the failure so it can be durably charged.
                raise ReviewProviderUnavailable(
                    f"provider temporarily unavailable: {exc.__class__.__name__}",
                    usage=usage,
                ) from exc
            raise ReviewProviderError(f"provider call failed: {exc.__class__.__name__}") from exc
        return "".join(chunks), usage


__all__ = ["DEFAULT_MAX_REPAIRS", "ReviewEngine", "ReviewEngineResult"]
