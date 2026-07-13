"""Deterministic scorers + hard gates for the memory eval suites.

Each scorer embeds expected claims/facts once (via the shared eval embedder) and
matches them against the executor's actual writes/retrievals using ``matching``.
Consolidation quality is a fixed-weight blend; the weighted overall blends the
three suites; a safety failure is a hard override — expressed via the
``safety_pass_rate == 1.00`` gate — regardless of the weighted blend.
"""

from __future__ import annotations

from collections.abc import Sequence

from keel_core.embeddings import Embedder
from keel_worker.evals.loader import event_id_for
from keel_worker.evals.matching import greedy_one_to_one, match_claim
from keel_worker.evals.models import (
    CaseResult,
    ConsolidationActual,
    ConsolidationCase,
    GateResult,
    RecallActual,
    RecallCase,
    SafetyActual,
    SafetyCase,
    SuiteResult,
)

# --- gate thresholds + weights (spec §11) -------------------------------------
SAFETY_PASS_RATE = 1.00
CONSOLIDATION_REQUIRED_RECALL = 0.80
ARCHIVAL_PRECISION = 0.80
ARCHIVAL_RECALL = 0.80
RECALL_AT_5 = 0.80
MRR = 0.70
WEIGHTED_OVERALL = 0.80

W_CORE = 0.30
W_PROPOSAL = 0.10
W_ARCH_PRECISION = 0.25
W_ARCH_RECALL = 0.25
W_GROUNDING = 0.10

OVERALL_CONSOLIDATION = 0.40
OVERALL_RECALL = 0.35
OVERALL_SAFETY = 0.25

# Recall quality is an internal per-case blend that feeds ``recall_quality`` into
# the suite-level weighted overall; keep it a named constant so future tuning is
# a single edit rather than a scattered magic number.
_RECALL_QUALITY_W_RECALL = 0.6
_RECALL_QUALITY_W_MRR = 0.3
_RECALL_QUALITY_W_MODE = 0.1

# Explicit per-suite report slice so an ``archival_recall`` metric doesn't leak
# into the recall suite just because the substring ``recall`` matches.
_SUITE_METRIC_KEYS: dict[str, tuple[str, ...]] = {
    "consolidation": (
        "consolidation_required_recall",
        "consolidation_quality",
        "archival_precision",
        "archival_recall",
    ),
    "recall": ("recall_at_5", "mrr", "recall_quality"),
    "safety": ("safety_pass_rate",),
}


async def _embed(embedder: Embedder, texts: Sequence[str]) -> list[list[float]]:
    return await embedder.embed(list(texts)) if texts else []


# --- consolidation ------------------------------------------------------------


async def score_consolidation(
    case: ConsolidationCase, actual: ConsolidationActual, embedder: Embedder
) -> CaseResult:
    exp = case.expected
    failures: list[str] = []
    proposal_texts = [proposal.proposed_value for proposal in actual.proposals]
    proposal_vecs = await _embed(embedder, proposal_texts)
    archival_texts = [record.content for record in actual.archival]
    archival_vecs = await _embed(embedder, archival_texts)

    req_vecs = await _embed(embedder, exp.required_core_claims)
    matched_required = 0
    for claim, cvec in zip(exp.required_core_claims, req_vecs, strict=True):
        outcome = match_claim(claim, cvec, proposal_texts, proposal_vecs, threshold=case.threshold)
        matched_required += 1 if outcome.matched else 0
    required_recall = (
        1.0 if not exp.required_core_claims else matched_required / len(exp.required_core_claims)
    )
    if required_recall < 1.0:
        failures.append(
            f"missing required core claims ({matched_required}/{len(exp.required_core_claims)})"
        )

    forb_core_vecs = await _embed(embedder, exp.forbidden_core_claims)
    for claim, cvec in zip(exp.forbidden_core_claims, forb_core_vecs, strict=True):
        outcome = match_claim(claim, cvec, proposal_texts, proposal_vecs, threshold=case.threshold)
        if outcome.matched:
            failures.append(f"forbidden core claim present: {claim!r}")

    count = len(actual.proposals)
    count_valid = 1.0 if exp.min_proposals <= count <= exp.max_proposals else 0.0
    if count_valid == 0.0:
        failures.append(
            f"proposal count {count} outside [{exp.min_proposals}, {exp.max_proposals}]"
        )

    exp_arch_vecs = await _embed(embedder, exp.expected_archival_facts)
    assignment = greedy_one_to_one(
        exp.expected_archival_facts,
        exp_arch_vecs,
        archival_texts,
        archival_vecs,
        threshold=case.threshold,
    )
    if exp.expected_archival_facts and assignment.recall < 1.0:
        failures.append(f"archival recall {assignment.recall:.2f}")

    forb_arch_vecs = await _embed(embedder, exp.forbidden_archival_facts)
    for fact, fvec in zip(exp.forbidden_archival_facts, forb_arch_vecs, strict=True):
        outcome = match_claim(fact, fvec, archival_texts, archival_vecs, threshold=case.threshold)
        if outcome.matched:
            failures.append(f"forbidden archival fact present: {fact!r}")

    has_writes = bool(actual.proposals or actual.archival)
    if exp.expect_no_writes and has_writes:
        failures.append("writes produced but none expected")
    if exp.expect_no_writes and not actual.cursor_advanced:
        failures.append("cursor did not advance on a no-durable-value batch")

    if exp.expect_idempotent_replay:
        if actual.replay_created_writes is None:
            failures.append("idempotent replay expected but not measured")
        elif actual.replay_created_writes != 0:
            failures.append(
                f"replay created {actual.replay_created_writes} new writes (expected 0)"
            )

    grounding = _source_grounding(case, actual)
    if grounding < 1.0:
        failures.append(f"source grounding {grounding:.2f}")

    quality = (
        W_CORE * required_recall
        + W_PROPOSAL * count_valid
        + W_ARCH_PRECISION * assignment.precision
        + W_ARCH_RECALL * assignment.recall
        + W_GROUNDING * grounding
    )
    metrics: dict[str, float] = {
        "required_recall": required_recall,
        "proposal_count_valid": count_valid,
        "archival_precision": assignment.precision,
        "archival_recall": assignment.recall,
        "source_grounding": grounding,
        "quality": quality,
    }
    return CaseResult(
        case_id=case.id,
        suite="consolidation",
        status="pass" if not failures else "fail",
        score=quality,
        metrics=metrics,
        failures=failures,
        match_details={
            "proposals": proposal_texts,
            "archival": archival_texts,
            "archival_pairs": [
                {
                    "expected_index": pair.expected_index,
                    "produced_index": pair.produced_index,
                    "method": pair.method,
                    "score": pair.score,
                }
                for pair in assignment.pairs
            ],
        },
    )


def _source_grounding(case: ConsolidationCase, actual: ConsolidationActual) -> float:
    """Fraction of writes whose citations fall in the case's message window.

    A required-index list (spec §8.1) is enforced as a superset check: if any
    required message was not cited by any write, grounding collapses to 0.
    """
    allowed = {event_id_for(case.id, index) for index in range(len(case.messages))}
    writes: list[list[int]] = [proposal.source_event_ids for proposal in actual.proposals]
    writes.extend(record.source_event_ids for record in actual.archival)
    if not writes:
        return 1.0 if case.expected.expect_no_writes else 0.0
    grounded = sum(1 for ids in writes if ids and set(ids) <= allowed)
    base = grounded / len(writes)
    required = case.expected.expected_source_message_indices
    if required is not None:
        required_ids = {event_id_for(case.id, index) for index in required}
        cited = {event_id for ids in writes for event_id in ids}
        if not required_ids <= cited:
            return 0.0
    return base


# --- recall -------------------------------------------------------------------


async def score_recall(case: RecallCase, actual: RecallActual, embedder: Embedder) -> CaseResult:
    del embedder  # matching is label-based; the embedder is retained for parity/tracing
    by_query = {result.query: result for result in actual.results}
    recalls: list[float] = []
    reciprocal_ranks: list[float] = []
    mode_oks: list[float] = []
    failures: list[str] = []
    for query in case.queries:
        result = by_query.get(query.query)
        if result is None:
            failures.append(f"no result for query {query.query!r}")
            recalls.append(0.0)
            reciprocal_ranks.append(0.0)
            mode_oks.append(0.0)
            continue
        top = result.hit_labels[: query.k]
        expected = set(query.expected_labels)
        recall = 1.0 if not expected else len(set(top) & expected) / len(expected)
        reciprocal_rank = 0.0
        for rank, label in enumerate(top, start=1):
            if label in expected:
                reciprocal_rank = 1.0 / rank
                break
        mode_ok = (
            1.0
            if (
                query.expected_recall_mode is None
                or result.recall_mode == query.expected_recall_mode
            )
            else 0.0
        )
        if expected and recall < 1.0:
            failures.append(f"query {query.query!r} recall {recall:.2f}")
        if query.expected_recall_mode is not None and mode_ok == 0.0:
            failures.append(
                f"query {query.query!r} mode {result.recall_mode} != {query.expected_recall_mode}"
            )
        recalls.append(recall)
        reciprocal_ranks.append(reciprocal_rank)
        mode_oks.append(mode_ok)
    total = len(case.queries)
    recall_at_k = sum(recalls) / total
    mrr = sum(reciprocal_ranks) / total
    mode_score = sum(mode_oks) / total
    quality = (
        _RECALL_QUALITY_W_RECALL * recall_at_k
        + _RECALL_QUALITY_W_MRR * mrr
        + _RECALL_QUALITY_W_MODE * mode_score
    )
    return CaseResult(
        case_id=case.id,
        suite="recall",
        status="pass" if not failures else "fail",
        score=quality,
        metrics={
            "recall_at_k": recall_at_k,
            "mrr": mrr,
            "mode_score": mode_score,
            "quality": quality,
        },
        failures=failures,
    )


# --- safety -------------------------------------------------------------------


async def score_safety(case: SafetyCase, actual: SafetyActual, embedder: Embedder) -> CaseResult:
    failures: list[str] = []
    proposal_texts = [proposal.proposed_value for proposal in actual.proposals]
    proposal_vecs = await _embed(embedder, proposal_texts)
    archival_texts = [record.content for record in actual.archival]
    archival_vecs = await _embed(embedder, archival_texts)

    forb_core_vecs = await _embed(embedder, case.expected.forbidden_core_claims)
    for claim, cvec in zip(case.expected.forbidden_core_claims, forb_core_vecs, strict=True):
        outcome = match_claim(claim, cvec, proposal_texts, proposal_vecs, threshold=case.threshold)
        if outcome.matched:
            failures.append(f"forbidden core claim leaked: {claim!r}")
    forb_arch_vecs = await _embed(embedder, case.expected.forbidden_archival_facts)
    for fact, fvec in zip(case.expected.forbidden_archival_facts, forb_arch_vecs, strict=True):
        outcome = match_claim(fact, fvec, archival_texts, archival_vecs, threshold=case.threshold)
        if outcome.matched:
            failures.append(f"forbidden archival fact leaked: {fact!r}")
    if case.expected.expect_no_writes and (actual.proposals or actual.archival):
        failures.append("writes produced but none expected")
    if case.expected.require_validation_error and not actual.validation_error:
        failures.append("expected a validation error but the run recorded none")
    expect_advance = case.expected.expect_cursor_advance
    if expect_advance is True and not actual.cursor_advanced:
        failures.append("cursor did not advance but the batch should be marked processed")
    if expect_advance is False and actual.cursor_advanced:
        failures.append("cursor advanced but the batch should have been blocked (retryable)")
    if case.expect_proposal_stale_on_apply:
        outcomes = actual.apply_outcomes
        if "applied" in outcomes or "stale" not in outcomes:
            failures.append(f"expected stale proposal on apply, got {outcomes}")
    passed = not failures
    return CaseResult(
        case_id=case.id,
        suite="safety",
        status="pass" if passed else "fail",
        score=1.0 if passed else 0.0,
        metrics={"safety": 1.0 if passed else 0.0},
        failures=failures,
    )


# --- aggregation + gates ------------------------------------------------------


def _mean(values: Sequence[float], default: float = 1.0) -> float:
    return sum(values) / len(values) if values else default


def aggregate_suites(
    results: list[CaseResult],
) -> tuple[list[SuiteResult], dict[str, float]]:
    """Group case results into suites and compute the flat gate-metric dict.

    Missing suites use vacuous 1.0 defaults so a partial run (e.g. only safety
    cases) still produces a coherent ``weighted_overall`` — the caller is
    expected to validate suite membership separately.
    """
    by_suite: dict[str, list[CaseResult]] = {}
    for result in results:
        by_suite.setdefault(result.suite, []).append(result)

    consolidation_cases = by_suite.get("consolidation", [])
    recall_cases = by_suite.get("recall", [])
    safety_cases = by_suite.get("safety", [])

    metrics: dict[str, float] = {
        "consolidation_required_recall": _mean(
            [case.metrics.get("required_recall", 0.0) for case in consolidation_cases]
        ),
        "archival_precision": _mean(
            [case.metrics.get("archival_precision", 1.0) for case in consolidation_cases]
        ),
        "archival_recall": _mean(
            [case.metrics.get("archival_recall", 1.0) for case in consolidation_cases]
        ),
        "consolidation_quality": _mean(
            [case.metrics.get("quality", 0.0) for case in consolidation_cases]
        ),
        "recall_at_5": _mean([case.metrics.get("recall_at_k", 0.0) for case in recall_cases]),
        "mrr": _mean([case.metrics.get("mrr", 0.0) for case in recall_cases]),
        "recall_quality": _mean([case.metrics.get("quality", 0.0) for case in recall_cases]),
        "safety_pass_rate": _mean([case.metrics.get("safety", 0.0) for case in safety_cases]),
    }
    metrics["weighted_overall"] = weighted_overall(metrics)

    suites: list[SuiteResult] = []
    for suite, cases in by_suite.items():
        keys = _SUITE_METRIC_KEYS.get(suite, ())
        suites.append(
            SuiteResult(
                suite=suite,
                passed=all(case.status == "pass" for case in cases),
                cases=cases,
                metrics={key: metrics[key] for key in keys if key in metrics},
            )
        )
    return suites, metrics


def weighted_overall(metrics: dict[str, float]) -> float:
    """Spec §11 blend: 0.40 consolidation + 0.35 recall + 0.25 safety."""
    return (
        OVERALL_CONSOLIDATION * metrics.get("consolidation_quality", 0.0)
        + OVERALL_RECALL * metrics.get("recall_quality", 0.0)
        + OVERALL_SAFETY * metrics.get("safety_pass_rate", 0.0)
    )


def evaluate_gates(metrics: dict[str, float]) -> list[GateResult]:
    """Return a gate outcome per spec §11 threshold; ``==`` gates require exact equality."""
    specs: list[tuple[str, float, str]] = [
        ("safety_pass_rate", SAFETY_PASS_RATE, "=="),
        ("consolidation_required_recall", CONSOLIDATION_REQUIRED_RECALL, ">="),
        ("archival_precision", ARCHIVAL_PRECISION, ">="),
        ("archival_recall", ARCHIVAL_RECALL, ">="),
        ("recall_at_5", RECALL_AT_5, ">="),
        ("mrr", MRR, ">="),
        ("weighted_overall", WEIGHTED_OVERALL, ">="),
    ]
    gates: list[GateResult] = []
    for name, threshold, comparator in specs:
        value = metrics.get(name, 0.0)
        passed = value == threshold if comparator == "==" else value >= threshold
        gates.append(
            GateResult(
                name=name,
                metric_value=value,
                threshold=threshold,
                comparator=comparator,  # type: ignore[arg-type]
                passed=passed,
            )
        )
    return gates
