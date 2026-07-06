"""Observability tests: cost/token accounting and the tracer seam (no live Langfuse)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from keel_core.agents import AgentSpec, Scope
from keel_core.config import Settings
from keel_core.events import Event, EventType
from keel_core.loop import admit, run
from keel_core.protocols import ProviderChunk, ProviderRequest, Usage
from keel_core.state import InMemoryEventStore
from keel_core.tracing import NoopTracer, make_tracer
from keel_core.types import FinishReason, ScopeKind, TrustLevel


def _agent() -> AgentSpec:
    return AgentSpec(
        id="a1",
        name="A",
        model="test/model",
        scope=Scope(id="u:1", kind=ScopeKind.personal, trust=TrustLevel.trusted),
    )


class _UsageGateway:
    """A provider that streams text, a finish, then a usage accounting chunk."""

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        async def _gen() -> AsyncIterator[ProviderChunk]:
            yield ProviderChunk(delta="hello", finish_reason=FinishReason.end_turn)
            yield ProviderChunk(usage=Usage(prompt_tokens=10, completion_tokens=5, cost_usd=0.002))

        return _gen()


class _RecordingTracer:
    def __init__(self) -> None:
        self.events: list[Event] = []
        self.flushed = 0

    def record(self, event: Event) -> None:
        self.events.append(event)

    def flush(self) -> None:
        self.flushed += 1


def test_make_tracer_is_noop_without_keys() -> None:
    tracer = make_tracer(Settings(langfuse_public_key="", langfuse_secret_key=""))
    assert isinstance(tracer, NoopTracer)


async def test_run_accounts_usage_and_feeds_tracer() -> None:
    store = InMemoryEventStore()
    tracer = _RecordingTracer()
    await admit(store, "s1", "u:1", "hi")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=_UsageGateway(),
        on_event=tracer.record,
    )

    # Real usage is accounted on the result (not a len(text) proxy).
    assert result.usage.prompt_tokens == 10
    assert result.usage.completion_tokens == 5
    assert abs(result.usage.cost_usd - 0.002) < 1e-9

    # The run.ended event carries the total usage (cost/token accounting, WS-H).
    ended = next(e for e in store.snapshot("s1") if e.type is EventType.run_ended)
    assert ended.payload["usage"]["cost_usd"] == 0.002
    assert ended.payload["usage"]["completion_tokens"] == 5

    # The assistant message carries its turn's usage (per-turn generation cost).
    msg = next(
        e
        for e in store.snapshot("s1")
        if e.type is EventType.message_token and e.payload.get("role") == "assistant"
    )
    assert msg.payload["usage"]["prompt_tokens"] == 10

    # The tracer observed the whole run lifecycle in order.
    types = [e.type for e in tracer.events]
    assert types[0] is EventType.run_started
    assert types[-1] is EventType.run_ended
