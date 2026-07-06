"""Observability bootstrap: structured logging + OpenTelemetry tracing (WS-H).

M0 wires the plumbing:

- JSON structured logs (with active trace/span ids when present).
- A real ``TracerProvider`` so spans are created and, crucially, **propagatable**.
- W3C trace-context inject/extract helpers so a trace started on the server
  continues on the worker across the arq/Redis boundary.

OTLP export + bundled Langfuse land in M1 (see the m1-observability todo); here
tracing is export-light (optional console exporter) to keep the inner loop fast.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

_tracing_configured = False


class JsonLogFormatter(logging.Formatter):
    """Render log records as one JSON object per line, with trace correlation."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            payload["trace_id"] = format(span_context.trace_id, "032x")
            payload["span_id"] = format(span_context.span_id, "016x")
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str = "INFO") -> None:
    """Install the JSON formatter on the root logger."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


def configure_tracing(service_name: str, *, console: bool = False) -> trace.Tracer:
    """Set the global TracerProvider once and return a tracer for the service."""
    global _tracing_configured
    if not _tracing_configured:
        provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
        if console:
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)
        _tracing_configured = True
    return trace.get_tracer(service_name)


def get_tracer(name: str) -> trace.Tracer:
    """Return a tracer from the configured provider."""
    return trace.get_tracer(name)


def inject_trace_context(carrier: dict[str, str]) -> dict[str, str]:
    """Inject the active trace context into ``carrier`` (e.g. an arq job's kwargs)."""
    propagate.inject(carrier)
    return carrier


def extract_trace_context(carrier: Mapping[str, str]) -> Context:
    """Extract a trace context from ``carrier`` (worker side)."""
    return propagate.extract(carrier)
