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
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from keel_core.protocols import ProviderChunk, ProviderRequest, ToolCall, Usage
from keel_core.types import FinishReason

CompletionFn = Callable[..., Awaitable[Any]]

logger = logging.getLogger(__name__)

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


def _extract_responses_usage(model: str, raw: Any) -> Usage:
    """Usage from a Responses API ``response.completed`` event (input/output tokens)."""
    prompt = int(getattr(raw, "input_tokens", 0) or 0)
    completion = int(getattr(raw, "output_tokens", 0) or 0)
    details = getattr(raw, "input_tokens_details", None)
    cached = int(getattr(details, "cached_tokens", 0) or 0) if details is not None else 0
    return Usage(
        prompt_tokens=prompt,
        completion_tokens=completion,
        cache_read_tokens=cached,
        cost_usd=_compute_cost(model, prompt, completion),
    )


def _to_responses_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert chat function tools (``{type, function:{...}}``) to the flat Responses
    API shape (``{type:"function", name, description, parameters}``)."""
    out: list[dict[str, Any]] = []
    for tool in tools:
        fn = tool["function"] if isinstance(tool, dict) and "function" in tool else tool
        out.append(
            {
                "type": "function",
                "name": fn.get("name"),
                "description": fn.get("description", ""),
                "parameters": fn.get("parameters") or {"type": "object", "properties": {}},
            }
        )
    return out


def _event_type(event: Any) -> str | None:
    """The Responses stream event type as a plain string (enum or str)."""
    raw = getattr(event, "type", None)
    return getattr(raw, "value", raw)


class LiteLLMGateway:
    """A ``ProviderGateway`` backed by LiteLLM.

    Routes each request to the right endpoint: models that only speak the
    **Responses API** (e.g. ``github_copilot/gpt-5.3-codex``) go through
    ``litellm.aresponses``; everything else uses ``/chat/completions``. Both paths
    normalize to the same ``ProviderChunk`` stream.
    """

    def __init__(
        self,
        *,
        completion: CompletionFn | None = None,
        responses: CompletionFn | None = None,
        fallbacks: list[str] | None = None,
    ) -> None:
        if completion is None or responses is None:
            import litellm

            # We surface provider errors ourselves; drop LiteLLM's "Give Feedback"
            # footer so a failed call isn't buried under boilerplate.
            litellm.suppress_debug_info = True
            if completion is None:
                completion = litellm.acompletion
            if responses is None:
                responses = litellm.aresponses
        self._completion = completion
        self._responses = responses
        if fallbacks is None:
            from keel_core.config import get_settings

            fallbacks = get_settings().fallback_model_list
        self._fallbacks = fallbacks

    def _use_responses(self, model: str) -> bool:
        """True when ``model`` is a github_copilot model that requires /responses."""
        if not model.startswith("github_copilot/"):
            return False
        bare = model.split("/", 1)[1]
        try:
            from litellm.llms.github_copilot.responses.transformation import (
                github_copilot_supports_responses_api,
            )

            return bool(github_copilot_supports_responses_api(bare))
        except Exception:  # noqa: BLE001 - unknown/uninstalled -> fall back to chat
            return False

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        return self._stream_with_failover(request)

    def _dispatch(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        """Route one request to the right endpoint (Responses vs chat completions)."""
        if self._use_responses(request.model):
            return self._stream_responses(request)
        return self._stream(request)

    async def _stream_with_failover(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        """Try ``request.model``, then each fallback, on a pre-output failure (B2).

        Failover only applies while no chunk has been emitted yet: once we've
        streamed output, an error propagates (retrying would duplicate the turn).
        """
        candidates = [request.model]
        for model in self._fallbacks:
            if model not in candidates:
                candidates.append(model)

        last_exc: Exception | None = None
        for model in candidates:
            req = request if model == request.model else request.model_copy(update={"model": model})
            emitted = False
            try:
                async for chunk in self._dispatch(req):
                    emitted = True
                    yield chunk
                return
            except Exception as exc:  # noqa: BLE001 - provider errors drive failover
                if emitted:
                    raise  # partial output already streamed: cannot safely fail over
                last_exc = exc
                logger.warning("provider %s failed; falling over: %s", model, exc)
        if last_exc is not None:
            raise last_exc

    async def _stream_responses(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        """Normalize a ``litellm.aresponses`` (Responses API) stream to ProviderChunks."""
        kwargs: dict[str, Any] = {
            "model": request.model,
            "input": request.messages,
            "stream": True,
        }
        if request.tools:
            kwargs["tools"] = _to_responses_tools(request.tools)
        if request.max_output_tokens is not None:
            kwargs["max_output_tokens"] = request.max_output_tokens

        response = await self._responses(**kwargs)
        # item_id -> accumulating {call_id, name, arguments}; order preserves call order.
        pending: dict[str, dict[str, str]] = {}
        order: list[str] = []
        usage: Usage | None = None

        async for event in response:
            etype = _event_type(event)
            if etype == "response.output_text.delta":
                delta = getattr(event, "delta", None)
                if delta:
                    yield ProviderChunk(delta=delta)
            elif etype == "response.output_item.added":
                item = getattr(event, "item", None)
                if item is not None and getattr(item, "type", None) == "function_call":
                    item_id = str(getattr(item, "id", "") or "")
                    pending[item_id] = {
                        "id": str(getattr(item, "call_id", "") or "call"),
                        "name": str(getattr(item, "name", "") or ""),
                        "arguments": str(getattr(item, "arguments", "") or ""),
                    }
                    order.append(item_id)
            elif etype == "response.function_call_arguments.delta":
                item_id = str(getattr(event, "item_id", "") or "")
                if item_id in pending:
                    pending[item_id]["arguments"] += getattr(event, "delta", "") or ""
            elif etype == "response.function_call_arguments.done":
                item_id = str(getattr(event, "item_id", "") or "")
                if item_id in pending:
                    pending[item_id]["arguments"] = (
                        getattr(event, "arguments", None) or pending[item_id]["arguments"]
                    )
            elif etype == "response.completed":
                result = getattr(event, "response", None)
                raw_usage = getattr(result, "usage", None) if result is not None else None
                if raw_usage is not None:
                    usage = _extract_responses_usage(request.model, raw_usage)

        for item_id in order:
            yield ProviderChunk(tool_call=_build_tool_call(pending[item_id]))
        yield ProviderChunk(finish_reason=FinishReason.tool_use if order else FinishReason.end_turn)
        if usage is not None:
            yield ProviderChunk(usage=usage)

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
        if request.max_output_tokens is not None:
            kwargs["max_tokens"] = request.max_output_tokens

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
