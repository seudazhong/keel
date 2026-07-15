"""Strict Knowledge eval dataset contracts and deterministic hashing."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from keel_worker.knowledge_evals.models import (
    KnowledgeDatasetError,
    canonical_dataset_hash,
    load_case,
    load_dataset,
)


def _payload(case_id: str = "knowledge-case") -> dict[str, object]:
    text = "Install Keel."
    return {
        "version": 1,
        "id": case_id,
        "embedding_model": "ollama/bge-m3",
        "embedding_dim": 1024,
        "chunk_target_chars": 100,
        "chunk_overlap_chars": 0,
        "documents": [
            {
                "label": "guide",
                "title": "Guide.md",
                "source_type": "markdown",
                "content": text,
            }
        ],
        "queries": [
            {
                "query": "Install",
                "expected": [
                    {
                        "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
                        "version": 1,
                        "ordinal": 0,
                        "char_start": 0,
                        "char_end": len(text),
                    }
                ],
                "expect_taint": True,
            }
        ],
    }


def test_load_case_is_strict_and_pins_version_one() -> None:
    case = load_case(_payload())
    assert case.version == 1
    assert case.documents[0].label == "guide"

    with pytest.raises(ValidationError):
        load_case({**_payload(), "unknown": True})
    with pytest.raises(ValidationError):
        load_case({**_payload(), "version": 2})
    with pytest.raises(ValidationError):
        load_case(
            {
                **_payload(),
                "documents": [*_payload()["documents"], *_payload()["documents"]],  # type: ignore[index]
            }
        )


def test_locator_rejects_invalid_offsets() -> None:
    payload = _payload()
    payload["queries"][0]["expected"][0]["char_start"] = 20  # type: ignore[index]
    with pytest.raises(ValidationError):
        load_case(payload)


def test_dataset_rejects_duplicate_ids_and_hash_is_order_independent(tmp_path: Path) -> None:
    first = load_case(_payload("first-case"))
    second = load_case(_payload("second-case"))
    assert canonical_dataset_hash([first, second]) == canonical_dataset_hash([second, first])

    path = tmp_path / "dataset.jsonl"
    line = json.dumps(_payload("duplicate"))
    path.write_text(f"{line}\n{line}\n", encoding="utf-8")
    with pytest.raises(KnowledgeDatasetError, match="duplicate"):
        load_dataset(path)
