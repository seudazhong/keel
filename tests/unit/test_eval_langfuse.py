"""Langfuse reporter is optional and fail-open: it never raises or changes the result."""

from __future__ import annotations

import builtins
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from keel_worker.evals.models import CaseResult, EvalRunReport, SuiteResult
from keel_worker.evals.reporting import LangfuseEvalReporter


def _report() -> EvalRunReport:
    return EvalRunReport(
        run_id="r", dataset_version="v1", dataset_hash="h", mode="replay", exit_code=0
    )


def test_disabled_without_keys_is_noop() -> None:
    report = _report()
    LangfuseEvalReporter(public_key="", secret_key="", host="h").publish(report)
    assert report.reporting_errors == []


def test_import_failure_is_recorded_not_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def _fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "langfuse":
            raise ImportError("langfuse not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    report = _report()
    LangfuseEvalReporter(public_key="pk", secret_key="sk", host="h").publish(report)
    assert report.reporting_errors  # recorded
    assert report.exit_code == 0  # unchanged


def test_publishes_per_run_and_per_case_scores(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fake Langfuse v4 client confirms trace metadata and scores are pushed."""
    fake_client = MagicMock()
    fake_span = MagicMock()
    fake_client.start_as_current_observation.return_value.__enter__.return_value = fake_span
    fake_langfuse_mod = MagicMock()
    fake_langfuse_mod.Langfuse.return_value = fake_client
    monkeypatch.setitem(sys.modules, "langfuse", fake_langfuse_mod)

    report = _report()
    report.suites.append(
        SuiteResult(
            suite="consolidation",
            passed=True,
            cases=[
                CaseResult(case_id="c1", suite="consolidation", status="pass", score=0.9),
                CaseResult(case_id="c2", suite="consolidation", status="fail", score=0.5),
            ],
        )
    )

    LangfuseEvalReporter(
        public_key="pk", secret_key="sk", host="https://cloud.langfuse.com"
    ).publish(report)

    assert report.reporting_errors == []
    fake_langfuse_mod.Langfuse.assert_called_once_with(
        public_key="pk",
        secret_key="sk",
        base_url="https://cloud.langfuse.com",
    )
    fake_client.start_as_current_observation.assert_called_once()
    assert (
        len(fake_client.start_as_current_observation.call_args[1]["trace_context"]["trace_id"])
        == 32
    )
    assert fake_span.update.call_args[1]["metadata"]["weighted_overall"] == 0.0
    assert fake_span.update.call_args[1]["metadata"]["exit_code"] == 0
    # One run-level score plus one score per case.
    assert fake_client.create_score.call_count == 3
    fake_client.flush.assert_called_once()
