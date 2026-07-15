"""Knowledge eval scoring, gates, and CLI validation."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from keel_core.knowledge import KnowledgeSearchMode
from keel_worker.knowledge_evals.cli import validate_args
from keel_worker.knowledge_evals.models import (
    KnowledgeActualHit,
    KnowledgeEvalCase,
    KnowledgeEvalDocument,
    KnowledgeEvalQuery,
    KnowledgeExpectedLocator,
    KnowledgeQueryActual,
)
from keel_worker.knowledge_evals.runner import build_gates, score_case


def _case() -> KnowledgeEvalCase:
    text = "Install Keel."
    locator = KnowledgeExpectedLocator(
        content_sha256=hashlib.sha256(text.encode()).hexdigest(),
        version=1,
        ordinal=0,
        char_start=0,
        char_end=len(text),
    )
    return KnowledgeEvalCase(
        version=1,
        id="runner-case",
        chunk_target_chars=100,
        chunk_overlap_chars=0,
        documents=[
            KnowledgeEvalDocument(
                label="guide",
                title="Guide.md",
                source_type="markdown",
                content=text,
            )
        ],
        queries=[
            KnowledgeEvalQuery(
                query="Install",
                expected=[locator],
                expected_mode=KnowledgeSearchMode.hybrid,
                expect_taint=True,
            )
        ],
    )


def _hit(*, valid: bool = True, deleted: bool = False) -> KnowledgeActualHit:
    text = "Install Keel."
    return KnowledgeActualHit(
        content_sha256=hashlib.sha256(text.encode()).hexdigest(),
        version=1,
        ordinal=0,
        char_start=0,
        char_end=len(text),
        citation_valid=valid,
        deleted=deleted,
    )


def test_perfect_case_passes_all_gates() -> None:
    case = _case()
    result = score_case(
        case,
        [
            KnowledgeQueryActual(
                query="Install",
                mode=KnowledgeSearchMode.hybrid,
                hits=[_hit()],
                tainted=True,
            )
        ],
    )
    gates, weighted = build_gates([result])

    assert result.status == "pass"
    assert result.score == 1.0
    assert weighted == 1.0
    assert all(gate.passed for gate in gates)


def test_leakage_bad_citation_mode_and_taint_fail_deterministically() -> None:
    case = _case()
    result = score_case(
        case,
        [
            KnowledgeQueryActual(
                query="Install",
                mode=KnowledgeSearchMode.lexical,
                hits=[_hit(valid=False, deleted=True)],
                tainted=False,
            )
        ],
    )
    gates, weighted = build_gates([result])

    assert result.status == "fail"
    assert "query[0].citation_precision" in result.failures
    assert "query[0].deleted_leakage_pass" in result.failures
    assert "query[0].taint_pass" in result.failures
    assert "query[0].degraded_mode_pass" in result.failures
    assert weighted < 0.80
    assert any(not gate.passed for gate in gates)


def test_no_answer_requires_an_empty_result() -> None:
    case = _case().model_copy(
        update={
            "queries": [
                KnowledgeEvalQuery(
                    query="missing",
                    expected=[],
                    embedding_mode="none",
                    expected_mode=KnowledgeSearchMode.lexical,
                )
            ]
        }
    )
    passed = score_case(
        case,
        [
            KnowledgeQueryActual(
                query="missing",
                mode=KnowledgeSearchMode.lexical,
                hits=[],
            )
        ],
    )
    failed = score_case(
        case,
        [
            KnowledgeQueryActual(
                query="missing",
                mode=KnowledgeSearchMode.lexical,
                hits=[_hit()],
            )
        ],
    )
    assert passed.status == "pass"
    assert failed.status == "fail"
    assert "query[0].recall_at_5" in failed.failures


def test_cli_validation_is_fail_closed(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset.jsonl"
    cassette = tmp_path / "cassette.json"
    dataset.write_text("{}\n", encoding="utf-8")
    args = argparse.Namespace(
        mode="replay",
        record=True,
        dataset=dataset,
        embedding_cassette=cassette,
        dim=0,
    )
    errors = validate_args(args)
    assert "--record requires --mode live" in errors
    assert any("embedding cassette not found" in error for error in errors)
    assert "--dim must be a positive integer" in errors
