"""Suite orchestration: seed->execute->score every case, then gate + report.

One process runs cases sequentially (no concurrent stable-scope/DB/cassette
access). Replay reads only the checked-in cassettes and never falls back to live;
a cassette/embedding miss is captured by the executors and surfaces here as a
case ``error`` -> exit code 2. Record regenerates both cassettes atomically: they
are saved only when the entire run completed with no infra error.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from keel_core.config import Settings
from keel_worker.evals.database import (
    assert_current_database,
    case_scope,
    cleanup_scope,
    create_eval_engine,
)
from keel_worker.evals.embeddings import (
    EmbeddingCassette,
    EmbeddingCassetteMiss,
    RecordingEmbedder,
    ReplayEmbedder,
)
from keel_worker.evals.loader import canonical_dataset_hash, load_dataset
from keel_worker.evals.memory_runner import (
    run_consolidation_case,
    run_recall_case,
    run_safety_case,
)
from keel_worker.evals.models import (
    CaseResult,
    ConsolidationActual,
    EvalRunReport,
    RecallActual,
    SafetyActual,
)
from keel_worker.evals.providers import (
    CaseCassette,
    CassetteMiss,
    LiteLLMMemoryJudge,
    RecordingCaseProviderGateway,
    ReplayCaseProviderGateway,
)
from keel_worker.evals.reporting import (
    EvalReporter,
    JsonEvalReporter,
    LangfuseEvalReporter,
)
from keel_worker.evals.scoring import (
    aggregate_suites,
    evaluate_gates,
    score_consolidation,
    score_recall,
    score_safety,
)

EvalMode = Literal["replay", "live"]
ALL_SUITES = ("consolidation", "recall", "safety")


def default_enforce(mode: EvalMode) -> bool:
    return mode == "replay"


def compute_exit_code(*, infra_error: bool, gates: list[Any], enforce: bool) -> int:
    if infra_error:
        return 2
    if enforce and any(not gate.passed for gate in gates):
        return 1
    return 0


def select_cases(cases: list[Any], suites: set[str]) -> list[Any]:
    return [case for case in cases if case.suite in suites]


@dataclass
class RunDeps:
    make_provider: Callable[[str], Any]
    embedder: Any
    save: Callable[[], None]


def _noop() -> None:
    return None


def build_deps(
    mode: EvalMode,
    record: bool,
    provider_cassette_path: Path,
    embedding_cassette_path: Path,
    settings: Settings,
) -> RunDeps:
    if mode == "replay":
        provider_cassette = CaseCassette(provider_cassette_path)
        embed_cassette = EmbeddingCassette(embedding_cassette_path)
        embedder = ReplayEmbedder(
            embed_cassette, model=settings.embedding_model, dim=settings.embedding_dim
        )
        return RunDeps(
            make_provider=lambda cid: ReplayCaseProviderGateway(provider_cassette, cid),
            embedder=embedder,
            save=_noop,
        )
    # live (optionally recording)
    from keel_core.embeddings import LiteLLMEmbedder
    from keel_core.providers import LiteLLMGateway

    live_embedder = LiteLLMEmbedder(
        settings.embedding_model,
        settings.embedding_dim,
        send_dimensions=settings.embedding_send_dimensions,
        timeout_seconds=settings.embedding_timeout_seconds,
    )
    if not record:
        return RunDeps(
            make_provider=lambda cid: LiteLLMGateway(), embedder=live_embedder, save=_noop
        )
    provider_cassette = CaseCassette(provider_cassette_path)
    embed_cassette = EmbeddingCassette(embedding_cassette_path)
    recorder = RecordingEmbedder(live_embedder)
    recorder.bind(embed_cassette)

    def _save() -> None:
        provider_cassette.save()
        embed_cassette.save()

    return RunDeps(
        make_provider=lambda cid: RecordingCaseProviderGateway(
            LiteLLMGateway(), provider_cassette, cid
        ),
        embedder=recorder,
        save=_save,
    )


def _miss_reason(exc: Exception) -> str:
    if isinstance(exc, CassetteMiss):
        return exc.kind
    if isinstance(exc, EmbeddingCassetteMiss):
        return "embedding_cassette_miss"
    return f"{exc.__class__.__name__}: {exc}"


def build_judge_prompt(case: Any, actual: Any) -> str:
    """Assemble the advisory judge prompt from synthetic case + actual data (no secrets).

    The whole (already-validated, synthetic) case and executor actual are serialized so the
    judge can grade quality holistically. Used only when ``--judge`` is set.
    """
    return (
        "You are grading one memory-eval case. Reply ONLY with a JSON object: "
        '{"score": <float 0..1>, "passed": <bool>, "rationale": <string>}.\n'
        f"suite={case.suite} id={case.id}\n"
        f"case={case.model_dump_json()}\n"
        f"actual={actual.model_dump_json()}\n"
    )


async def _attach_judge(
    result: CaseResult, judge: LiteLLMMemoryJudge, case: Any, actual: Any
) -> None:
    """Advisory-only: attach a judge verdict but NEVER change the deterministic status/score.

    ``LiteLLMMemoryJudge.judge`` is itself fail-open; the extra guard keeps a prompt-build
    error from ever escaping into the run.
    """
    try:
        verdict = await judge.judge(build_judge_prompt(case, actual))
    except Exception as exc:  # noqa: BLE001 - judge is advisory + fail-open
        result.judge_error = f"judge invocation failed: {exc}"
        return
    result.judge = verdict
    if verdict.error:
        result.judge_error = verdict.error


def build_reporters(*, out_dir: Path, langfuse: bool, settings: Settings) -> list[EvalReporter]:
    """Assemble the run's reporters (spec §14). Langfuse is opt-in via ``--langfuse`` and is
    published **first** so any ``reporting_errors`` it records are captured by the always-on
    JSON reporter, which writes to disk **last**."""
    reporters: list[EvalReporter] = []
    if langfuse:
        reporters.append(
            LangfuseEvalReporter(
                public_key=settings.langfuse_public_key,
                secret_key=settings.langfuse_secret_key,
                host=settings.langfuse_host,
            )
        )
    reporters.append(JsonEvalReporter(out_dir))
    return reporters


async def _score_case(
    case: Any,
    engine: Any,
    deps: RunDeps,
    dataset_version: str,
    judge: LiteLLMMemoryJudge | None = None,
) -> CaseResult:
    actual: ConsolidationActual | RecallActual | SafetyActual
    if case.suite == "consolidation":
        provider = deps.make_provider(case.id)
        actual = await run_consolidation_case(
            case,
            engine=engine,
            provider=provider,
            embedder=deps.embedder,
            dataset_version=dataset_version,
        )
        result = await score_consolidation(case, actual, deps.embedder)
    elif case.suite == "recall":
        actual = await run_recall_case(
            case, engine=engine, embedder=deps.embedder, dataset_version=dataset_version
        )
        result = await score_recall(case, actual, deps.embedder)
    else:
        provider = deps.make_provider(case.id)
        actual = await run_safety_case(
            case,
            engine=engine,
            provider=provider,
            embedder=deps.embedder,
            dataset_version=dataset_version,
        )
        result = await score_safety(case, actual, deps.embedder)
    if judge is not None:
        await _attach_judge(result, judge, case, actual)  # advisory only; no gate impact
    return result


async def run_evals(
    *,
    dataset_path: Path,
    provider_cassette_path: Path,
    embedding_cassette_path: Path,
    mode: EvalMode,
    suites: set[str],
    enforce: bool,
    out_dir: Path,
    record: bool = False,
    model: str | None = None,
    judge: bool = False,
    judge_model: str | None = None,
    langfuse: bool = False,
    settings: Settings | None = None,
    deps: RunDeps | None = None,
) -> EvalRunReport:
    settings = settings or Settings()
    cases = load_dataset(dataset_path)
    dataset_version = dataset_path.stem
    dataset_hash = canonical_dataset_hash(cases)  # hashed pre-override: identifies the dataset
    selected = select_cases(cases, suites)
    if model is not None:
        # Override the provider model per case; it flows via case_settings ->
        # Settings.default_model -> the consolidation ProviderRequest (primarily for live runs,
        # since replay cassettes are fingerprinted by the recorded model).
        for case in selected:
            case.model = model
    deps = deps or build_deps(
        mode, record, provider_cassette_path, embedding_cassette_path, settings
    )
    judge_client = LiteLLMMemoryJudge(judge_model or settings.default_model) if judge else None

    engine = create_eval_engine()
    await assert_current_database(engine)
    started = datetime.now(UTC).isoformat()
    results: list[CaseResult] = []
    infra_error = False
    try:
        for case in selected:
            scope = case_scope(dataset_version, case.id)
            await cleanup_scope(engine, scope)
            try:
                results.append(await _score_case(case, engine, deps, dataset_version, judge_client))
            except (CassetteMiss, EmbeddingCassetteMiss) as miss:
                infra_error = True
                results.append(
                    CaseResult(
                        case_id=case.id,
                        suite=case.suite,
                        status="error",
                        score=0.0,
                        reason=_miss_reason(miss),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - any executor failure is infra (exit 2)
                infra_error = True
                results.append(
                    CaseResult(
                        case_id=case.id,
                        suite=case.suite,
                        status="error",
                        score=0.0,
                        reason=_miss_reason(exc),
                    )
                )
            finally:
                await cleanup_scope(engine, scope)
    finally:
        await engine.dispose()

    suite_results, metrics = aggregate_suites(results)
    gates = evaluate_gates(metrics)
    exit_code = compute_exit_code(infra_error=infra_error, gates=gates, enforce=enforce)

    if record and not infra_error:
        deps.save()  # atomic: only persist cassettes after a fully successful record

    report = EvalRunReport(
        run_id=uuid.uuid4().hex[:12],
        dataset_version=dataset_version,
        dataset_hash=dataset_hash,
        mode="record" if record else mode,
        suites=suite_results,
        gates=gates,
        weighted_overall=metrics.get("weighted_overall", 0.0),
        exit_code=exit_code,
        git_sha=os.environ.get("GITHUB_SHA"),
        started_at=started,
        finished_at=datetime.now(UTC).isoformat(),
    )
    for reporter in build_reporters(out_dir=out_dir, langfuse=langfuse, settings=settings):
        reporter.publish(report)
    return report
