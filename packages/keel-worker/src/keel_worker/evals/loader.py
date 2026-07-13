"""Load + validate the memory eval dataset and derive eval-only event identifiers.

Event ids are placed far above the production ``events.id`` bigserial range and
derived deterministically from ``case.id`` so a case always re-seeds to the same
ids (idempotent, diff-stable) and never collides with real data. A slot collision
(two ids hashing to the same 1e9 slot) or a duplicate id aborts the load.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from keel_worker.evals.models import (
    ConsolidationCase,
    RecallCase,
    SafetyCase,
    load_case,
)

MAX_CASE_MESSAGES = 1000
EVAL_EVENT_FLOOR = 8_000_000_000_000_000

AnyCase = ConsolidationCase | RecallCase | SafetyCase


class DatasetError(Exception):
    """Raised when the dataset is structurally invalid (duplicate id, slot collision)."""


def stable_case_slot(case_id: str) -> int:
    """Deterministic 1e9-space slot for a case id (spec §5)."""
    digest = hashlib.sha256(case_id.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % 1_000_000_000


def case_event_base(case_id: str) -> int:
    """First eval event id for a case (1000 ids reserved per case)."""
    return EVAL_EVENT_FLOOR + stable_case_slot(case_id) * 1000


def event_id_for(case_id: str, message_index: int) -> int:
    if not 0 <= message_index < MAX_CASE_MESSAGES:
        raise DatasetError(f"message index {message_index} out of range for {case_id!r}")
    return case_event_base(case_id) + message_index


def cursor_seed_for(case_id: str) -> int:
    """Cursor value so the first eligible event is ``case_event_base`` (exclusive cursor)."""
    return case_event_base(case_id) - 1


def canonical_dataset_hash(cases: list[AnyCase]) -> str:
    """Order-independent sha256 over the canonical JSON of every case."""
    blobs = sorted(
        json.dumps(case.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        for case in cases
    )
    joined = "\n".join(blobs).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()


def load_dataset(path: Path) -> list[AnyCase]:
    """Parse one JSONL row per line into validated cases; reject id/slot collisions."""
    cases: list[AnyCase] = []
    seen_ids: set[str] = set()
    slots: dict[int, str] = {}
    for lineno, raw in enumerate(path.read_text("utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DatasetError(f"{path.name}:{lineno} invalid JSON: {exc}") from exc
        case = load_case(payload)
        if case.id in seen_ids:
            raise DatasetError(f"{path.name}:{lineno} duplicate case id {case.id!r}")
        slot = stable_case_slot(case.id)
        if slot in slots:
            raise DatasetError(
                f"{path.name}:{lineno} slot collision: {case.id!r} and {slots[slot]!r} "
                f"both map to slot {slot}; rename one case id"
            )
        seen_ids.add(case.id)
        slots[slot] = case.id
        cases.append(case)
    if not cases:
        raise DatasetError(f"{path.name} contains no cases")
    return cases
