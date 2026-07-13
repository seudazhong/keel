"""Exact-first + semantic matching and greedy one-to-one assignment."""

from __future__ import annotations

import math

from keel_worker.evals.matching import (
    cosine_similarity,
    greedy_one_to_one,
    match_claim,
    normalize,
)


def test_normalize_casefolds_and_collapses_whitespace() -> None:
    assert normalize("  Prefers   Sam\n") == "prefers sam"


def test_cosine_handles_unnormalized_vectors() -> None:
    assert cosine_similarity([2.0, 0.0], [4.0, 0.0]) == 1.0
    assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == 0.0
    assert cosine_similarity([0.0, 0.0], [1.0, 1.0]) == 0.0  # zero vector guard


def test_match_claim_prefers_exact_substring() -> None:
    outcome = match_claim(
        "called Sam",
        [0.0, 1.0],
        ["The user prefers to be called Sam in replies."],
        [[1.0, 0.0]],
        threshold=0.82,
    )
    assert outcome.matched is True
    assert outcome.method == "exact"
    assert outcome.score == 1.0


def test_match_claim_falls_back_to_semantic() -> None:
    outcome = match_claim(
        "likes bicycles",
        [1.0, 0.0],
        ["enjoys cycling on weekends"],
        [[0.9, 0.1]],
        threshold=0.82,
    )
    assert outcome.matched is True
    assert outcome.method == "semantic"
    assert outcome.score >= 0.82


def test_greedy_one_to_one_precision_and_recall() -> None:
    expected = ["fact a", "fact b"]
    produced = ["fact a", "totally unrelated"]
    # orthogonal vectors: only the exact "fact a" pair matches
    ev = [[1.0, 0.0], [0.0, 1.0]]
    pv = [[1.0, 0.0], [0.0, 0.0, 1.0][:2]]
    assignment = greedy_one_to_one(expected, ev, produced, pv, threshold=0.82)
    assert assignment.recall == 0.5  # 1 of 2 expected matched
    assert assignment.precision == 0.5  # 1 of 2 produced matched
    assert math.isclose(assignment.pairs[0].score, 1.0)


def test_greedy_empty_produced_is_perfect_precision() -> None:
    assignment = greedy_one_to_one([], [], [], [], threshold=0.82)
    assert assignment.precision == 1.0
    assert assignment.recall == 1.0
