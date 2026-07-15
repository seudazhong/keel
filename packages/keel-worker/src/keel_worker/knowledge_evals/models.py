"""Strict contracts for the independent Knowledge eval dataset and report."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from keel_core.knowledge import KnowledgeSearchMode, KnowledgeSourceType

_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]+$"
_SHA256_PATTERN = r"^[0-9a-f]{64}$"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class KnowledgeExpectedLocator(_Strict):
    content_sha256: str = Field(pattern=_SHA256_PATTERN)
    version: int = Field(ge=1)
    ordinal: int = Field(ge=0)
    char_start: int = Field(ge=0)
    char_end: int = Field(ge=0)

    @model_validator(mode="after")
    def _validate_offsets(self) -> KnowledgeExpectedLocator:
        if self.char_end < self.char_start:
            raise ValueError("char_end must be greater than or equal to char_start")
        return self


class KnowledgeEvalDocument(_Strict):
    label: str = Field(pattern=_ID_PATTERN)
    title: str = Field(min_length=1, max_length=300)
    source_type: KnowledgeSourceType
    content: str = Field(min_length=1)
    source_uri: str | None = None
    delete_before_queries: bool = False


class KnowledgeEvalQuery(_Strict):
    query: str = Field(min_length=1, max_length=2_000)
    k: int = Field(default=5, ge=1, le=10)
    expected: list[KnowledgeExpectedLocator] = Field(default_factory=list)
    embedding_mode: Literal["normal", "failing", "none"] = "normal"
    expected_mode: KnowledgeSearchMode | None = None
    expect_taint: bool = False


class KnowledgeEvalCase(_Strict):
    version: Literal[1]
    id: str = Field(pattern=_ID_PATTERN)
    tags: list[str] = Field(default_factory=list)
    embedding_model: str = "ollama/bge-m3"
    embedding_dim: int = Field(default=1024, ge=1)
    chunk_target_chars: int = Field(default=1_600, ge=1)
    chunk_overlap_chars: int = Field(default=200, ge=0)
    documents: list[KnowledgeEvalDocument] = Field(min_length=1)
    queries: list[KnowledgeEvalQuery] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_case(self) -> KnowledgeEvalCase:
        if self.chunk_overlap_chars >= self.chunk_target_chars:
            raise ValueError("chunk overlap must be less than target")
        labels = [document.label for document in self.documents]
        if len(labels) != len(set(labels)):
            raise ValueError("document labels must be unique")
        return self


class KnowledgeActualHit(_Strict):
    content_sha256: str | None = Field(default=None, pattern=_SHA256_PATTERN)
    version: int | None = Field(default=None, ge=1)
    ordinal: int | None = Field(default=None, ge=0)
    char_start: int | None = Field(default=None, ge=0)
    char_end: int | None = Field(default=None, ge=0)
    citation_valid: bool
    deleted: bool


class KnowledgeQueryActual(_Strict):
    query: str
    mode: KnowledgeSearchMode
    hits: list[KnowledgeActualHit] = Field(default_factory=list)
    tainted: bool = True


class KnowledgeCaseResult(_Strict):
    case_id: str
    status: Literal["pass", "fail", "error"]
    score: float
    metrics: dict[str, float] = Field(default_factory=dict)
    failures: list[str] = Field(default_factory=list)
    queries: list[KnowledgeQueryActual] = Field(default_factory=list)
    reason: str | None = None


class KnowledgeGateResult(_Strict):
    name: str
    metric_value: float
    threshold: float
    comparator: Literal[">=", "=="]
    passed: bool


class KnowledgeEvalReport(_Strict):
    run_id: str
    dataset_version: str
    dataset_hash: str
    mode: Literal["replay", "live", "record"]
    cases: list[KnowledgeCaseResult] = Field(default_factory=list)
    gates: list[KnowledgeGateResult] = Field(default_factory=list)
    weighted_overall: float = 0.0
    exit_code: int = 0
    started_at: str
    finished_at: str


class KnowledgeDatasetError(Exception):
    """The Knowledge dataset or its deterministic locators are invalid."""


def load_case(payload: dict[str, Any]) -> KnowledgeEvalCase:
    return KnowledgeEvalCase.model_validate(payload)


def load_dataset(path: Path) -> list[KnowledgeEvalCase]:
    cases: list[KnowledgeEvalCase] = []
    seen: set[str] = set()
    for lineno, raw in enumerate(path.read_text("utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise KnowledgeDatasetError(f"{path.name}:{lineno} invalid JSON") from exc
        case = load_case(payload)
        if case.id in seen:
            raise KnowledgeDatasetError(f"{path.name}:{lineno} duplicate case id {case.id!r}")
        seen.add(case.id)
        cases.append(case)
    if not cases:
        raise KnowledgeDatasetError(f"{path.name} contains no cases")
    return cases


def canonical_dataset_hash(cases: list[KnowledgeEvalCase]) -> str:
    blobs = sorted(
        json.dumps(case.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        for case in cases
    )
    return hashlib.sha256("\n".join(blobs).encode("utf-8")).hexdigest()


def dataset_version(cases: list[KnowledgeEvalCase]) -> str:
    versions = {case.version for case in cases}
    if versions != {1}:
        raise KnowledgeDatasetError("Knowledge dataset must contain only version 1 cases")
    return "knowledge-v1"


def locator_key(locator: KnowledgeExpectedLocator | KnowledgeActualHit) -> tuple[object, ...]:
    return (
        locator.content_sha256,
        locator.version,
        locator.ordinal,
        locator.char_start,
        locator.char_end,
    )


def safe_reason(exc: BaseException) -> str:
    name = type(exc).__name__
    return name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", name) else "EvaluationError"


__all__ = [
    "KnowledgeActualHit",
    "KnowledgeCaseResult",
    "KnowledgeDatasetError",
    "KnowledgeEvalCase",
    "KnowledgeEvalDocument",
    "KnowledgeEvalQuery",
    "KnowledgeEvalReport",
    "KnowledgeExpectedLocator",
    "KnowledgeGateResult",
    "KnowledgeQueryActual",
    "canonical_dataset_hash",
    "dataset_version",
    "load_case",
    "load_dataset",
    "locator_key",
    "safe_reason",
]
