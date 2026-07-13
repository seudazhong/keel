"""Runner pure logic: exit-code precedence, suite filter, enforce defaults, advisory
judge attach, and Langfuse reporter gating."""

from __future__ import annotations

from pathlib import Path

from keel_core.config import Settings
from keel_worker.evals.models import (
    CaseResult,
    ConsolidationActual,
    ConsolidationCase,
    ConsolidationExpected,
    GateResult,
    JudgeResult,
    Message,
)
from keel_worker.evals.reporting import JsonEvalReporter, LangfuseEvalReporter
from keel_worker.evals.runner import (
    _attach_judge,
    build_judge_prompt,
    build_reporters,
    compute_exit_code,
    default_enforce,
    select_cases,
)


def _gate(passed: bool) -> GateResult:
    return GateResult(name="g", metric_value=0.0, threshold=1.0, comparator=">=", passed=passed)


def _consolidation_case() -> ConsolidationCase:
    return ConsolidationCase(
        version=1,
        suite="consolidation",
        id="c1",
        messages=[Message(role="user", text="hi")],
        expected=ConsolidationExpected(),
    )


def test_exit_code_infra_beats_gate() -> None:
    assert compute_exit_code(infra_error=True, gates=[_gate(False)], enforce=True) == 2


def test_exit_code_gate_failure_when_enforced() -> None:
    assert compute_exit_code(infra_error=False, gates=[_gate(False)], enforce=True) == 1


def test_exit_code_no_enforce_passes() -> None:
    assert compute_exit_code(infra_error=False, gates=[_gate(False)], enforce=False) == 0


def test_default_enforce_by_mode() -> None:
    assert default_enforce("replay") is True
    assert default_enforce("live") is False


def test_select_cases_filters_by_suite() -> None:
    case = _consolidation_case()
    assert select_cases([case], {"recall"}) == []
    assert select_cases([case], {"consolidation"}) == [case]
    assert select_cases([case], set(("consolidation", "recall", "safety"))) == [case]


def test_build_judge_prompt_includes_case_and_actual() -> None:
    prompt = build_judge_prompt(
        _consolidation_case(), ConsolidationActual(status="completed", cursor_advanced=True)
    )
    assert "suite=consolidation" in prompt
    assert "case=" in prompt and "actual=" in prompt


async def test_attach_judge_is_advisory_and_never_changes_status() -> None:
    class _Judge:
        async def judge(self, prompt: str) -> JudgeResult:
            assert "actual=" in prompt  # prompt is built from the case + actual
            return JudgeResult(score=0.4, passed=False, rationale="meh")

    result = CaseResult(case_id="c1", suite="consolidation", status="pass", score=1.0)
    await _attach_judge(
        result,
        _Judge(),  # type: ignore[arg-type]
        _consolidation_case(),
        ConsolidationActual(status="completed", cursor_advanced=True),
    )
    assert result.status == "pass"  # advisory: judge.passed=False does NOT flip the gate
    assert result.judge is not None and result.judge.passed is False


def test_build_reporters_gates_langfuse_behind_flag(tmp_path: Path) -> None:
    settings = Settings()
    off = build_reporters(out_dir=tmp_path, langfuse=False, settings=settings)
    assert len(off) == 1 and isinstance(off[0], JsonEvalReporter)
    on = build_reporters(out_dir=tmp_path, langfuse=True, settings=settings)
    assert isinstance(on[0], LangfuseEvalReporter)  # Langfuse publishes first
    assert isinstance(on[-1], JsonEvalReporter)  # JSON reporter writes last
