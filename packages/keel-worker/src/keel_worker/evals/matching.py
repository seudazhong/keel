"""Deterministic claim matching for eval scoring.

Matching is exact-substring-first (normalized: casefold + whitespace collapse) and
falls back to cosine similarity over caller-provided embeddings. Cosine does NOT
assume unit vectors (the production ``LiteLLMEmbedder`` returns raw bge-m3 output).
Archival facts use a greedy one-to-one assignment so one produced fact cannot
satisfy two expected facts (and vice versa), yielding honest precision/recall.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

_WS = re.compile(r"\s+")


def normalize(text: str) -> str:
    return _WS.sub(" ", text.casefold()).strip()


def contains_normalized(needle: str, haystack: str) -> bool:
    n = normalize(needle)
    return bool(n) and n in normalize(haystack)


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


@dataclass(frozen=True)
class ClaimOutcome:
    claim: str
    matched: bool
    method: str  # "exact" | "semantic" | "none"
    score: float
    matched_label: str | None = None


def match_claim(
    claim: str,
    claim_vec: list[float],
    candidates: list[str],
    candidate_vecs: list[list[float]],
    *,
    threshold: float,
    labels: list[str] | None = None,
) -> ClaimOutcome:
    """Match ``claim`` against candidates: exact substring first, else best cosine."""
    for index, candidate in enumerate(candidates):
        if contains_normalized(claim, candidate):
            label = labels[index] if labels is not None else None
            return ClaimOutcome(claim, True, "exact", 1.0, label)
    best_index = -1
    best_score = 0.0
    for index, vec in enumerate(candidate_vecs):
        score = cosine_similarity(claim_vec, vec)
        if score > best_score:
            best_score, best_index = score, index
    if best_index >= 0 and best_score >= threshold:
        label = labels[best_index] if labels is not None else None
        return ClaimOutcome(claim, True, "semantic", best_score, label)
    return ClaimOutcome(claim, False, "none", best_score, None)


@dataclass(frozen=True)
class MatchPair:
    expected_index: int
    produced_index: int
    method: str
    score: float


@dataclass
class Assignment:
    pairs: list[MatchPair] = field(default_factory=list)
    matched_expected: set[int] = field(default_factory=set)
    matched_produced: set[int] = field(default_factory=set)
    precision: float = 1.0
    recall: float = 1.0


def greedy_one_to_one(
    expected: list[str],
    expected_vecs: list[list[float]],
    produced: list[str],
    produced_vecs: list[list[float]],
    *,
    threshold: float,
) -> Assignment:
    """One-to-one match expected↔produced (exact first, then best-cosine greedy)."""
    assignment = Assignment()
    candidates: list[tuple[float, str, int, int]] = []
    for ei, (etext, evec) in enumerate(zip(expected, expected_vecs, strict=True)):
        for pi, (ptext, pvec) in enumerate(zip(produced, produced_vecs, strict=True)):
            if contains_normalized(etext, ptext) or contains_normalized(ptext, etext):
                candidates.append((1.0, "exact", ei, pi))
            else:
                score = cosine_similarity(evec, pvec)
                if score >= threshold:
                    candidates.append((score, "semantic", ei, pi))
    for score, method, ei, pi in sorted(candidates, key=lambda c: c[0], reverse=True):
        if ei in assignment.matched_expected or pi in assignment.matched_produced:
            continue
        assignment.matched_expected.add(ei)
        assignment.matched_produced.add(pi)
        assignment.pairs.append(MatchPair(ei, pi, method, score))
    assignment.recall = 1.0 if not expected else len(assignment.matched_expected) / len(expected)
    assignment.precision = (
        1.0 if not produced else len(assignment.matched_produced) / len(produced)
    )
    return assignment
