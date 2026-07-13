"""The committed v1 dataset loads to exactly 12 validated cases with full coverage."""

from __future__ import annotations

from pathlib import Path

from keel_worker.evals.loader import load_dataset

DATASET = Path("evals/datasets/memory/v1.jsonl")


def test_dataset_has_twelve_cases() -> None:
    assert len(load_dataset(DATASET)) == 12


def test_suite_distribution() -> None:
    counts = {"consolidation": 0, "recall": 0, "safety": 0}
    for case in load_dataset(DATASET):
        counts[case.suite] += 1
    assert counts == {"consolidation": 5, "recall": 3, "safety": 4}


def test_tag_coverage() -> None:
    tags = {tag for case in load_dataset(DATASET) for tag in case.tags}
    assert {"en", "zh", "preference", "project", "safety", "semantic", "lexical-degraded"} <= tags


def test_ids_unique_and_slot_safe() -> None:
    cases = load_dataset(DATASET)  # loader raises on duplicate id or slot collision
    assert len({case.id for case in cases}) == 12
