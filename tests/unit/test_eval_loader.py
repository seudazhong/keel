"""Loader: id math, slot-collision + duplicate rejection, canonical hashing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from keel_worker.evals.loader import (
    EVAL_EVENT_FLOOR,
    DatasetError,
    canonical_dataset_hash,
    case_event_base,
    cursor_seed_for,
    event_id_for,
    load_dataset,
    stable_case_slot,
)


def test_event_id_math_is_deterministic_and_isolated() -> None:
    slot = stable_case_slot("con-en-preference")
    base = case_event_base("con-en-preference")
    assert 0 <= slot < 1_000_000_000
    assert base == EVAL_EVENT_FLOOR + slot * 1000
    assert event_id_for("con-en-preference", 0) == base
    assert event_id_for("con-en-preference", 3) == base + 3
    assert cursor_seed_for("con-en-preference") == base - 1
    assert base >= EVAL_EVENT_FLOOR  # never collides with production bigserial ids


def _row(case_id: str) -> dict[str, object]:
    return {
        "version": 1,
        "suite": "consolidation",
        "id": case_id,
        "messages": [{"role": "user", "text": "hi"}],
        "expected": {"min_proposals": 0, "max_proposals": 0, "expect_no_writes": True},
    }


def _write(tmp_path: Path, rows: list[dict[str, object]]) -> Path:
    path = tmp_path / "v1.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return path


def test_load_dataset_rejects_duplicate_ids(tmp_path: Path) -> None:
    path = _write(tmp_path, [_row("dup"), _row("dup")])
    with pytest.raises(DatasetError, match="duplicate"):
        load_dataset(path)


def test_load_dataset_rejects_slot_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "keel_worker.evals.loader.stable_case_slot", lambda _case_id: 7
    )
    path = _write(tmp_path, [_row("alpha"), _row("beta")])
    with pytest.raises(DatasetError, match="slot collision"):
        load_dataset(path)


def test_canonical_hash_is_order_independent(tmp_path: Path) -> None:
    a = load_dataset(_write(tmp_path, [_row("a1"), _row("a2")]))
    reordered = _write(tmp_path, [_row("a2"), _row("a1")])
    b = load_dataset(reordered)
    assert canonical_dataset_hash(a) == canonical_dataset_hash(b)
