"""Run tracing + cost accounting (WS-H).

A :class:`Tracer` consumes the loop's event stream (the ``on_event`` seam) and maps
it to a trace with per-turn generations and per-tool spans, carrying the token/cost
:class:`~keel_core.protocols.Usage` the loop now records. Tracing is optional:
:func:`make_tracer` returns a :class:`NoopTracer` unless Langfuse is configured, and
:class:`LangfuseTracer` is fully guarded so a tracing failure never touches the run.

The Langfuse adapter targets the v2 client API and is best-effort; the seam and the
cost accounting are the contract, verifiable without a live Langfuse server.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from keel_core.config import Settings, get_settings
from keel_core.events import Event, EventType

logger = logging.getLogger("keel.tracing")


class Tracer(Protocol):
    """Consumes run events and exports them to a tracing backend."""

    def record(self, event: Event) -> None:
        """Record one run event (called for every emitted event, in order)."""
        ...

    def flush(self) -> None:
        """Flush any buffered spans to the backend."""
        ...


class NoopTracer:
    """A tracer that does nothing (the default when Langfuse is not configured)."""

    def record(self, event: Event) -> None:
        return None

    def flush(self) -> None:
        return None


def _is_assistant_message(event: Event) -> bool:
    return (
        event.type is EventType.message_token
        and event.payload.get("role") == "assistant"
        and not event.payload.get("partial")
    )


class LangfuseTracer:
    """Best-effort Langfuse adapter: one trace per run, a generation per turn.

    Every backend call is guarded — a tracing error is logged and swallowed, never
    propagated into the run (observability must not break the agent).
    """

    def __init__(self, public_key: str, secret_key: str, host: str) -> None:
        from langfuse import Langfuse  # lazy: only imported when tracing is enabled

        self._client = Langfuse(public_key=public_key, secret_key=secret_key, host=host)
        self._traces: dict[str, Any] = {}  # run_id -> langfuse trace handle

    def record(self, event: Event) -> None:
        try:
            self._record(event)
        except Exception:  # noqa: BLE001 - tracing is best-effort; never crash a run
            logger.debug("langfuse record failed for %s", event.type, exc_info=True)

    def _record(self, event: Event) -> None:
        run_id = event.run_id
        if run_id is None:
            return
        if event.type is EventType.run_started:
            self._traces[run_id] = self._client.trace(
                id=run_id, name="agent.run", session_id=event.session_id
            )
        elif _is_assistant_message(event):
            trace = self._traces.get(run_id)
            if trace is not None:
                usage = event.payload.get("usage", {})
                trace.generation(
                    name="turn",
                    output=event.payload.get("text", ""),
                    usage_details=usage,
                    metadata={"cost_usd": usage.get("cost_usd", 0.0)},
                )
        elif event.type is EventType.tool_result:
            trace = self._traces.get(run_id)
            if trace is not None:
                trace.span(name="tool", output=event.payload)
        elif event.type is EventType.run_ended:
            trace = self._traces.pop(run_id, None)
            if trace is not None:
                usage = event.payload.get("usage", {})
                trace.update(metadata={"reason": event.payload.get("reason"), "usage": usage})

    def flush(self) -> None:
        try:
            self._client.flush()
        except Exception:  # noqa: BLE001
            logger.debug("langfuse flush failed", exc_info=True)


def make_tracer(settings: Settings | None = None) -> Tracer:
    """Return a Langfuse tracer if configured, else a no-op tracer (fail-open)."""
    settings = settings or get_settings()
    if settings.langfuse_public_key and settings.langfuse_secret_key:
        try:
            return LangfuseTracer(
                settings.langfuse_public_key,
                settings.langfuse_secret_key,
                settings.langfuse_host,
            )
        except Exception:  # noqa: BLE001 - missing package / bad config -> disable tracing
            logger.warning("Langfuse configured but unavailable; tracing disabled")
    return NoopTracer()
