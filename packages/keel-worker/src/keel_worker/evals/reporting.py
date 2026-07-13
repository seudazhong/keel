"""Local reports for the eval run: JSON (always complete), JUnit XML, terminal.

The JSON report is the source of truth (Langfuse is optional, Task 13). JUnit XML
marks a ``fail`` case as ``<failure>`` and an ``error`` (infra/cassette) case as
``<error>`` so CI surfaces the two distinctly.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol
from xml.etree import ElementTree as ET

from keel_worker.evals.models import EvalRunReport


def to_json(report: EvalRunReport) -> str:
    return report.model_dump_json(indent=2)


def to_junit_xml(report: EvalRunReport) -> str:
    root = ET.Element("testsuites", name="memory-evals")
    total_tests = total_failures = total_errors = 0
    for suite in report.suites:
        failures = sum(1 for c in suite.cases if c.status == "fail")
        errors = sum(1 for c in suite.cases if c.status == "error")
        total_tests += len(suite.cases)
        total_failures += failures
        total_errors += errors
        suite_el = ET.SubElement(
            root,
            "testsuite",
            name=suite.suite,
            tests=str(len(suite.cases)),
            failures=str(failures),
            errors=str(errors),
        )
        for case in suite.cases:
            case_el = ET.SubElement(
                suite_el, "testcase", name=case.case_id, classname=suite.suite, time="0"
            )
            if case.status == "fail":
                fail_el = ET.SubElement(
                    case_el, "failure", message="; ".join(case.failures) or "failed"
                )
                fail_el.text = "\n".join(case.failures)
            elif case.status == "error":
                err_el = ET.SubElement(case_el, "error", message=case.reason or "error")
                err_el.text = case.reason or ""
    root.set("tests", str(total_tests))
    root.set("failures", str(total_failures))
    root.set("errors", str(total_errors))
    return ET.tostring(root, encoding="unicode")


def to_terminal(report: EvalRunReport) -> str:
    lines = [
        f"Memory evals {report.run_id} mode={report.mode} dataset={report.dataset_version}"
        f" hash={report.dataset_hash[:12]}",
        "",
        "Gates:",
    ]
    for gate in report.gates:
        mark = "PASS" if gate.passed else "FAIL"
        lines.append(
            f"  [{mark}] {gate.name} {gate.metric_value:.3f} {gate.comparator} {gate.threshold:.2f}"
        )
    lines.append("")
    for suite in report.suites:
        lines.append(f"Suite {suite.suite}: {'PASS' if suite.passed else 'FAIL'}")
        for case in suite.cases:
            lines.append(f"  {case.status.upper():5} {case.case_id} score={case.score:.3f}")
            for failure in case.failures:
                lines.append(f"        - {failure}")
            if case.reason:
                lines.append(f"        ! {case.reason}")
    lines.append("")
    lines.append(f"weighted_overall={report.weighted_overall:.3f} exit_code={report.exit_code}")
    if report.reporting_errors:
        lines.append(f"reporting_errors: {report.reporting_errors}")
    return "\n".join(lines)


def write_reports(report: EvalRunReport, out_dir: Path) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "report.json"
    junit_path = out_dir / "junit.xml"
    json_path.write_text(to_json(report), encoding="utf-8")
    junit_path.write_text(to_junit_xml(report), encoding="utf-8")
    return {"json": json_path, "junit": junit_path}


class EvalReporter(Protocol):
    """A sink for a finished run (spec §14). ``publish`` must be fail-open (never raise)."""

    def publish(self, report: EvalRunReport) -> None: ...


class JsonEvalReporter:
    """Always-on reporter: writes the JSON + JUnit reports to ``out_dir`` (source of truth).

    The runner publishes this **last** so any ``reporting_errors`` appended by an earlier
    reporter (e.g. Langfuse) are captured in the on-disk JSON.
    """

    def __init__(self, out_dir: Path) -> None:
        self._out_dir = out_dir
        self.paths: dict[str, Path] = {}

    def publish(self, report: EvalRunReport) -> None:
        self.paths = write_reports(report, self._out_dir)
