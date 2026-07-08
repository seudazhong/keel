"""LiteLLMGateway tests: streaming normalization + cache-key forwarding.

Deterministic — the ``completion`` callable is injected, so no network call.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

from keel_core.agents import AgentSpec, Scope
from keel_core.loop import admit, run
from keel_core.protocols import ProviderRequest
from keel_core.providers import LiteLLMGateway, _map_finish
from keel_core.state import InMemoryEventStore
from keel_core.types import FinishReason, ScopeKind, StopReason


def _chunk(
    content: str | None = None,
    tool_calls: list[Any] | None = None,
    finish_reason: str | None = None,
) -> SimpleNamespace:
    delta = SimpleNamespace(content=content, tool_calls=tool_calls)
    choice = SimpleNamespace(delta=delta, finish_reason=finish_reason)
    return SimpleNamespace(choices=[choice])


def _tool_delta(
    index: int, id: str | None = None, name: str | None = None, args: str | None = None
) -> SimpleNamespace:
    return SimpleNamespace(index=index, id=id, function=SimpleNamespace(name=name, arguments=args))


def _usage_chunk(prompt: int, completion: int, cached: int = 0) -> SimpleNamespace:
    usage = SimpleNamespace(
        prompt_tokens=prompt,
        completion_tokens=completion,
        prompt_tokens_details=SimpleNamespace(cached_tokens=cached),
    )
    return SimpleNamespace(choices=[], usage=usage)


def _completion(chunks: list[Any], capture: dict[str, Any]):
    async def completion(**kwargs: Any) -> AsyncIterator[Any]:
        capture["kwargs"] = kwargs

        async def gen() -> AsyncIterator[Any]:
            for chunk in chunks:
                yield chunk

        return gen()

    return completion


def _responses(events: list[Any], capture: dict[str, Any]):
    async def responses(**kwargs: Any) -> AsyncIterator[Any]:
        capture["kwargs"] = kwargs

        async def gen() -> AsyncIterator[Any]:
            for event in events:
                yield event

        return gen()

    return responses


def _rev(type_: str, **attrs: Any) -> SimpleNamespace:
    """A Responses API stream event double."""
    return SimpleNamespace(type=type_, **attrs)


def _fn_item(call_id: str, name: str, item_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        type="function_call", call_id=call_id, name=name, id=item_id, arguments=""
    )


async def test_streams_text_and_finish() -> None:
    capture: dict[str, Any] = {}
    chunks = [_chunk(content="he"), _chunk(content="llo"), _chunk(finish_reason="stop")]
    gateway = LiteLLMGateway(completion=_completion(chunks, capture))

    request = ProviderRequest(model="openai/gpt-x", messages=[{"role": "user", "content": "hi"}])
    out = [chunk async for chunk in gateway.stream(request)]

    assert [c.delta for c in out if c.delta] == ["he", "llo"]
    assert out[-1].finish_reason is FinishReason.end_turn
    assert capture["kwargs"]["model"] == "openai/gpt-x"
    assert capture["kwargs"]["stream"] is True


async def test_extracts_usage_chunk() -> None:
    capture: dict[str, Any] = {}
    chunks = [_chunk(content="hi"), _chunk(finish_reason="stop"), _usage_chunk(12, 7, cached=3)]
    gateway = LiteLLMGateway(completion=_completion(chunks, capture))

    request = ProviderRequest(model="test/model", messages=[{"role": "user", "content": "hi"}])
    out = [chunk async for chunk in gateway.stream(request)]

    usage_chunks = [chunk for chunk in out if chunk.usage is not None]
    assert len(usage_chunks) == 1
    usage = usage_chunks[0].usage
    assert usage is not None
    assert usage.prompt_tokens == 12
    assert usage.completion_tokens == 7
    assert usage.cache_read_tokens == 3  # prompt-cache hits accounted (NFR-8)
    assert capture["kwargs"]["stream_options"] == {"include_usage": True}


async def test_streams_tool_call_across_fragments() -> None:
    capture: dict[str, Any] = {}
    chunks = [
        _chunk(tool_calls=[_tool_delta(0, id="c1", name="search", args='{"q":')]),
        _chunk(tool_calls=[_tool_delta(0, args='"cats"}')]),
        _chunk(finish_reason="tool_calls"),
    ]
    gateway = LiteLLMGateway(completion=_completion(chunks, capture))
    out = [chunk async for chunk in gateway.stream(ProviderRequest(model="m", messages=[]))]

    tool_chunks = [c for c in out if c.tool_call is not None]
    assert len(tool_chunks) == 1
    call = tool_chunks[0].tool_call
    assert call is not None
    assert call.id == "c1"
    assert call.name == "search"
    assert call.arguments == {"q": "cats"}  # reassembled from streamed fragments
    assert out[-1].finish_reason is FinishReason.tool_use


async def test_forwards_prompt_cache_key() -> None:
    capture: dict[str, Any] = {}
    gateway = LiteLLMGateway(completion=_completion([_chunk(finish_reason="stop")], capture))
    request = ProviderRequest(model="m", messages=[], prompt_cache_key="abc123")
    _ = [chunk async for chunk in gateway.stream(request)]
    assert capture["kwargs"]["prompt_cache_key"] == "abc123"


async def test_omits_cache_key_when_absent() -> None:
    capture: dict[str, Any] = {}
    gateway = LiteLLMGateway(completion=_completion([_chunk(finish_reason="stop")], capture))
    _ = [chunk async for chunk in gateway.stream(ProviderRequest(model="m", messages=[]))]
    assert "prompt_cache_key" not in capture["kwargs"]


def test_finish_reason_mapping() -> None:
    assert _map_finish("stop") is FinishReason.end_turn
    assert _map_finish("tool_calls") is FinishReason.tool_use
    assert _map_finish("length") is FinishReason.length
    assert _map_finish(None) is None
    assert _map_finish("unrecognized") is FinishReason.end_turn


async def test_gateway_drives_the_loop() -> None:
    capture: dict[str, Any] = {}
    chunks = [_chunk(content="hi"), _chunk(finish_reason="stop")]
    gateway = LiteLLMGateway(completion=_completion(chunks, capture))
    store = InMemoryEventStore()
    agent = AgentSpec(
        id="a", name="n", model="openai/gpt-x", scope=Scope(id="u", kind=ScopeKind.personal)
    )
    await admit(store, "s", "u", "hello")
    result = await run(agent=agent, session_id="s", store=store, provider=gateway)
    assert result.reason is StopReason.completed


def test_default_gateway_binds_litellm() -> None:
    # No injected callables -> lazily binds litellm.acompletion + aresponses (wiring).
    gateway = LiteLLMGateway()
    assert callable(gateway._completion)
    assert callable(gateway._responses)


async def test_responses_streams_text_and_usage() -> None:
    capture: dict[str, Any] = {}
    usage = SimpleNamespace(
        input_tokens=13, output_tokens=28, input_tokens_details=SimpleNamespace(cached_tokens=5)
    )
    events = [
        _rev("response.output_text.delta", delta="Hello "),
        _rev("response.output_text.delta", delta="there"),
        _rev("response.completed", response=SimpleNamespace(usage=usage)),
    ]
    gateway = LiteLLMGateway(completion=_completion([], {}), responses=_responses(events, capture))
    request = ProviderRequest(
        model="github_copilot/gpt-5.3-codex", messages=[{"role": "user", "content": "hi"}]
    )
    out = [chunk async for chunk in gateway._stream_responses(request)]

    assert [c.delta for c in out if c.delta] == ["Hello ", "there"]
    finish = next(c.finish_reason for c in out if c.finish_reason is not None)
    assert finish is FinishReason.end_turn
    usage_chunks = [c for c in out if c.usage is not None]
    assert usage_chunks and usage_chunks[0].usage is not None
    assert usage_chunks[0].usage.prompt_tokens == 13
    assert usage_chunks[0].usage.completion_tokens == 28
    assert usage_chunks[0].usage.cache_read_tokens == 5
    assert capture["kwargs"]["input"] == request.messages  # messages passed as Responses input


async def test_responses_streams_tool_call() -> None:
    capture: dict[str, Any] = {}
    events = [
        _rev("response.output_item.added", item=SimpleNamespace(type="reasoning", id="r1")),
        _rev("response.output_item.added", item=_fn_item("call_9", "get_weather", "enc1")),
        _rev("response.function_call_arguments.delta", item_id="enc1", delta='{"city":'),
        _rev("response.function_call_arguments.done", item_id="enc1", arguments='{"city":"Paris"}'),
        _rev("response.completed", response=SimpleNamespace(usage=None)),
    ]
    gateway = LiteLLMGateway(completion=_completion([], {}), responses=_responses(events, capture))
    request = ProviderRequest(
        model="github_copilot/gpt-5.3-codex",
        messages=[],
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "d",
                    "parameters": {"type": "object"},
                },
            }
        ],
    )
    out = [chunk async for chunk in gateway._stream_responses(request)]

    tool_chunks = [c for c in out if c.tool_call is not None]
    assert len(tool_chunks) == 1
    call = tool_chunks[0].tool_call
    assert call is not None
    assert call.id == "call_9"
    assert call.name == "get_weather"
    assert call.arguments == {"city": "Paris"}
    finish = next(c.finish_reason for c in out if c.finish_reason is not None)
    assert finish is FinishReason.tool_use
    assert capture["kwargs"]["tools"][0] == {
        "type": "function",
        "name": "get_weather",
        "description": "d",
        "parameters": {"type": "object"},
    }


def test_use_responses_routing() -> None:
    gateway = LiteLLMGateway(completion=_completion([], {}), responses=_responses([], {}))
    assert gateway._use_responses("openai/gpt-4o") is False  # non-copilot -> chat
    assert gateway._use_responses("github_copilot/gpt-5.3-codex") is True  # responses-only model
