"""JSON/JUnit/terminal reporting shapes."""

from __future__ import annotations

import json
from pathlib import Path
from xml.etree import ElementTree

from keel_worker.evals.models import CaseResult, EvalRunReport, GateResult, SuiteResult
from keel_worker.evals.reporting import (
    JsonEvalReporter,
    to_json,
    to_junit_xml,
    to_terminal,
    write_reports,
)


def _report() -> EvalRunReport:
    return EvalRunReport(
        run_id="run-1",
        dataset_version="v1",
        dataset_hash="abc",
        mode="replay",
        suites=[
            SuiteResult(
                suite="safety",
                passed=False,
                cases=[
                    CaseResult(case_id="saf-x", suite="safety", status="fail", score=0.0,
                               failures=["forbidden core claim leaked: 'x'"]),
                    CaseResult(case_id="saf-e", suite="safety", status="error", score=0.0,
                               reason="cassette_miss"),
                ],
            )
        ],
        gates=[GateResult(name="safety_pass_rate", metric_value=0.0, threshold=1.0,
                          comparator="==", passed=False)],
        weighted_overall=0.5,
        exit_code=1,
    )


def test_json_roundtrips() -> None:
    data = json.loads(to_json(_report()))
    assert data["run_id"] == "run-1"
    assert data["gates"][0]["passed"] is False


def test_junit_has_failure_and_error() -> None:
    root = ElementTree.fromstring(to_junit_xml(_report()))
    assert root.tag == "testsuites"
    suite = root.find("testsuite")
    assert suite is not None
    assert suite.get("name") == "safety"
    cases = suite.findall("testcase")
    assert cases[0].find("failure") is not None
    assert cases[1].find("error") is not None


def test_terminal_mentions_gate(tmp_path: Path) -> None:
    text = to_terminal(_report())
    assert "safety_pass_rate" in text
    paths = write_reports(_report(), tmp_path)
    assert paths["json"].exists()
    assert paths["junit"].exists()


def test_json_reporter_publishes_to_disk(tmp_path: Path) -> None:
    reporter = JsonEvalReporter(tmp_path)
    reporter.publish(_report())  # always-on reporter satisfies the EvalReporter protocol
    assert reporter.paths["json"].exists()
    assert reporter.paths["junit"].exists()
