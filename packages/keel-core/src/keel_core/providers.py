"""Provider gateway over LiteLLM (WS-B, ADR-0003).

``LiteLLMGateway`` implements the ``ProviderGateway`` seam for any
LiteLLM-supported provider (OpenAI, Anthropic, Azure, Bedrock, Ollama, …). It
normalizes the provider's streaming response into ``ProviderChunk``s and forwards
the prompt cache key.

The ``completion`` callable is injectable, so tests run deterministically without
a network call (record/replay-style, NFR-12); the default lazily binds
``litellm.acompletion`` so importing this module stays cheap.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from keel_core.protocols import ProviderChunk, ProviderRequest, ToolCall, Usage
from keel_core.types import FinishReason

CompletionFn = Callable[..., Awaitable[Any]]

# Provider finish reasons (OpenAI/Anthropic/LiteLLM) -> our turn-level FinishReason.
_FINISH_REASONS: dict[str, FinishReason] = {
    "stop": FinishReason.end_turn,
    "end_turn": FinishReason.end_turn,
    "tool_calls": FinishReason.tool_use,
    "tool_use": FinishReason.tool_use,
    "function_call": FinishReason.tool_use,
    "length": FinishReason.length,
    "max_tokens": FinishReason.length,
    "content_filter": FinishReason.error,
}


def _map_finish(reason: str | None) -> FinishReason | None:
    if reason is None:
        return None
    return _FINISH_REASONS.get(reason, FinishReason.end_turn)


def _build_tool_call(fragment: dict[str, str]) -> ToolCall:
    raw_arguments = fragment["arguments"]
    arguments: dict[str, Any] = {}
    if raw_arguments:
        try:
            parsed = json.loads(raw_arguments)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            arguments = {str(key): value for key, value in parsed.items()}
    return ToolCall(id=fragment["id"] or "call", name=fragment["name"], arguments=arguments)


def _compute_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Best-effort USD cost for a turn via LiteLLM's price map (0.0 if unknown)."""
    try:
        from litellm import cost_per_token

        prompt_cost, completion_cost = cost_per_token(
            model=model, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens
        )
        return float(prompt_cost) + float(completion_cost)
    except Exception:  # noqa: BLE001 - unknown model / offline: accounting is best-effort
        return 0.0


def _extract_usage(model: str, raw: Any) -> Usage:
    prompt = int(getattr(raw, "prompt_tokens", 0) or 0)
    completion = int(getattr(raw, "completion_tokens", 0) or 0)
    details = getattr(raw, "prompt_tokens_details", None)
    cached = int(getattr(details, "cached_tokens", 0) or 0) if details is not None else 0
    return Usage(
        prompt_tokens=prompt,
        completion_tokens=completion,
        cache_read_tokens=cached,
        cost_usd=_compute_cost(model, prompt, completion),
    )


class LiteLLMGateway:
    """A ``ProviderGateway`` backed by LiteLLM."""

    def __init__(self, *, completion: CompletionFn | None = None) -> None:
        if completion is None:
            import litellm

            # We surface provider errors ourselves; drop LiteLLM's "Give Feedback"
            # footer so a failed call isn't buried under boilerplate.
            litellm.suppress_debug_info = True
            completion = litellm.acompletion
        self._completion = completion

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        return self._stream(request)

    async def _stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        kwargs: dict[str, Any] = {
            "model": request.model,
            "messages": request.messages,
            "stream": True,
            "stream_options": {"include_usage": True},  # token/cost accounting (WS-H)
        }
        if request.tools:
            kwargs["tools"] = request.tools
        if request.prompt_cache_key is not None:
            kwargs["prompt_cache_key"] = request.prompt_cache_key

        response = await self._completion(**kwargs)
        tool_fragments: dict[int, dict[str, str]] = {}
        usage: Usage | None = None

        async for chunk in response:
            raw_usage = getattr(chunk, "usage", None)
            if raw_usage is not None:
                usage = _extract_usage(request.model, raw_usage)

            choices = getattr(chunk, "choices", None) or []
            if not choices:
                continue
            choice = choices[0]
            delta = getattr(choice, "delta", None)

            content = getattr(delta, "content", None) if delta is not None else None
            if content:
                yield ProviderChunk(delta=content)

            delta_tool_calls = getattr(delta, "tool_calls", None) if delta is not None else None
            for tool_call in delta_tool_calls or []:
                index = getattr(tool_call, "index", 0) or 0
                fragment = tool_fragments.setdefault(index, {"id": "", "name": "", "arguments": ""})
                if getattr(tool_call, "id", None):
                    fragment["id"] = tool_call.id
                function = getattr(tool_call, "function", None)
                if function is not None:
                    if getattr(function, "name", None):
                        fragment["name"] = function.name
                    if getattr(function, "arguments", None):
                        fragment["arguments"] += function.arguments

            finish = _map_finish(getattr(choice, "finish_reason", None))
            if finish is not None:
                for fragment in tool_fragments.values():
                    yield ProviderChunk(tool_call=_build_tool_call(fragment))
                tool_fragments.clear()
                yield ProviderChunk(finish_reason=finish)

        if usage is not None:
            yield ProviderChunk(usage=usage)
