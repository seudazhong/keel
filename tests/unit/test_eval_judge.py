"""Optional judge: JSON parsing + fail-open behaviour (never blocks the run)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from keel_core.protocols import ProviderChunk
from keel_core.types import FinishReason
from keel_worker.evals.providers import (
    Judge,
    LiteLLMMemoryJudge,
    parse_judge_response,
)


def test_parse_extracts_json_block() -> None:
    result = parse_judge_response('noise {"score": 0.9, "passed": true, "rationale": "ok"} tail')
    assert result.score == 0.9
    assert result.passed is True
    assert result.error is None


def test_parse_is_fail_open_on_garbage() -> None:
    result = parse_judge_response("not json at all")
    assert result.passed is True  # advisory only: never blocks
    assert result.error is not None


def test_parse_is_fail_open_on_non_boolean_passed() -> None:
    result = parse_judge_response('{"score": 0.2, "passed": "false", "rationale": "bad type"}')
    assert result.passed is True
    assert "JSON boolean" in (result.error or "")


class _Gateway:
    def stream(self, request):  # type: ignore[no-untyped-def]
        async def _gen() -> AsyncIterator[ProviderChunk]:
            yield ProviderChunk(delta='{"score": 0.5, "passed": false, "rationale": "meh"}')
            yield ProviderChunk(finish_reason=FinishReason.end_turn)

        return _gen()


class _BoomGateway:
    def stream(self, request):  # type: ignore[no-untyped-def]
        async def _gen() -> AsyncIterator[ProviderChunk]:
            raise RuntimeError("provider down")
            yield  # pragma: no cover

        return _gen()


async def test_judge_reads_stream() -> None:
    judge: Judge = LiteLLMMemoryJudge("eval/judge", gateway=_Gateway())
    result = await judge.judge("grade this")
    assert result.score == 0.5
    assert result.passed is False


async def test_judge_fail_open_on_provider_error() -> None:
    judge = LiteLLMMemoryJudge("eval/judge", gateway=_BoomGateway())
    result = await judge.judge("grade this")
    assert result.passed is True
    assert "provider down" in (result.error or "")
