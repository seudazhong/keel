"""Observability bootstrap tests: JSON logs + server->worker trace propagation."""

from __future__ import annotations

import json
import logging

from keel_core.observability import (
    JsonLogFormatter,
    configure_tracing,
    extract_trace_context,
    inject_trace_context,
)


def test_configure_tracing_is_idempotent() -> None:
    first = configure_tracing("keel-test")
    second = configure_tracing("keel-test")
    assert first is not None
    assert second is not None


def test_trace_context_propagates_server_to_worker() -> None:
    tracer = configure_tracing("keel-test")

    # Server side: start a span and inject its context into a carrier.
    carrier: dict[str, str] = {}
    with tracer.start_as_current_span("server") as server_span:
        expected_trace_id = server_span.get_span_context().trace_id
        inject_trace_context(carrier)
    assert "traceparent" in carrier

    # Worker side: extract the context and continue the same trace.
    worker_ctx = extract_trace_context(carrier)
    with tracer.start_as_current_span("worker", context=worker_ctx) as worker_span:
        assert worker_span.get_span_context().trace_id == expected_trace_id


def test_json_log_formatter_emits_valid_json() -> None:
    record = logging.LogRecord(
        "keel.test", logging.INFO, __file__, 10, "hello %s", ("world",), None
    )
    payload = json.loads(JsonLogFormatter().format(record))
    assert payload["level"] == "INFO"
    assert payload["logger"] == "keel.test"
    assert payload["msg"] == "hello world"
