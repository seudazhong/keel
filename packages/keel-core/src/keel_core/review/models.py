"""Typed contracts for the read-only managed-code review MVP (WS-R).

Pure, transport-free dataclasses + enums + validators mirroring the review domain:

* :class:`ReviewRequest`  — an authorized request to review a project branch/commit/PR.
* :class:`ReviewFinding`  — one bounded, evidence-bearing observation.
* :class:`ReviewReport`   — the immutable, content-addressable result record.
* :class:`ReviewRecord`   — the lightweight status projection returned by the status API.

Every field is strictly validated and bounded (P5 fail-closed): a review must never carry
an unbounded model output, an invented file path, or a line reference outside the reviewed
diff. Evidence verification (see :mod:`keel_core.review.evidence`) downgrades or rejects any
finding whose ``file_path``/line range cannot be confirmed against the reviewed diff — the
models here only guarantee *shape*, never *truth*.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from typing import Any

from .errors import ReviewBoundsExceeded, ReviewValidationError

# --- Identifiers ---------------------------------------------------------------------

_REVIEW_PREFIX = "rev_"

type ReviewId = str


def new_review_id() -> ReviewId:
    return f"{_REVIEW_PREFIX}{uuid.uuid4().hex}"


# --- Bounds (fail closed) ------------------------------------------------------------

MAX_FINDINGS = 50
MAX_TITLE_CHARS = 200
MAX_EXPLANATION_CHARS = 4_000
MAX_RECOMMENDATION_CHARS = 2_000
MAX_SNIPPET_CHARS = 2_000
MAX_PATH_CHARS = 1_024
MAX_LIMITATIONS = 20
MAX_LIMITATION_CHARS = 500
MAX_REF_CHARS = 255
MAX_MODEL_CHARS = 128
# Default ceiling on the unified diff handed to the model. Larger diffs are truncated and
# the omission is recorded as an explicit report limitation (never silently dropped).
DEFAULT_MAX_DIFF_BYTES = 1_000_000
MAX_LINE_NUMBER = 100_000_000
# A single finding may only cite a small, contiguous span. This is a hard fail-closed bound:
# it stops a model from citing a 1..100_000_000 "range" (which would make line-overlap checks
# allocate an enormous set) and forces evidence to point at a specific, reviewable location.
MAX_FINDING_LINE_SPAN = 100

# --- Budget policy (fail closed; never unlimited) ------------------------------------
# Every review runs under an explicit, bounded budget. There is no "unlimited" default: a
# token budget, per-turn output cap, cost ceiling, and provider-attempt cap are always set and
# enforced (see :mod:`keel_core.review.engine`). Callers may lower these but never remove them.
DEFAULT_REVIEW_TOKEN_BUDGET = 200_000
MAX_REVIEW_TOKEN_BUDGET = 2_000_000
DEFAULT_REVIEW_OUTPUT_MAX_TOKENS = 8_000
MAX_REVIEW_OUTPUT_MAX_TOKENS = 32_000
DEFAULT_REVIEW_COST_CEILING_USD = 1.0
MAX_REVIEW_COST_CEILING_USD = 50.0
# Total provider turns permitted, INCLUDING the bounded structured-output repair turn.
DEFAULT_REVIEW_MAX_PROVIDER_ATTEMPTS = 2
MAX_REVIEW_PROVIDER_ATTEMPTS = 4


# --- Ordered enums -------------------------------------------------------------------


class Severity(StrEnum):
    """How damaging the issue is if real. Ordered ``info`` < ... < ``critical``."""

    info = "info"
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class Confidence(StrEnum):
    """How sure the reviewer is the issue is real. Ordered ``low`` < ... < ``high``."""

    low = "low"
    medium = "medium"
    high = "high"


class ReviewSource(StrEnum):
    """What is being reviewed."""

    branch = "branch"
    commit = "commit"
    pull_request = "pull_request"


class ReviewStatus(StrEnum):
    """Lifecycle of a review, projected from its durable run."""

    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


_SEVERITY_ORDER = {value: index for index, value in enumerate(Severity)}
_CONFIDENCE_ORDER = {value: index for index, value in enumerate(Confidence)}


def severity_rank(value: Severity) -> int:
    return _SEVERITY_ORDER[value]


def confidence_rank(value: Confidence) -> int:
    return _CONFIDENCE_ORDER[value]


# --- Validation helpers --------------------------------------------------------------


def _bounded_text(value: Any, *, field_name: str, max_chars: int, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ReviewValidationError(f"{field_name} must be a string")
    if "\x00" in value:
        raise ReviewValidationError(f"{field_name} must not contain null bytes")
    text = value.strip() if not allow_empty else value
    if not allow_empty and not text:
        raise ReviewValidationError(f"{field_name} must not be empty")
    if len(value) > max_chars:
        raise ReviewBoundsExceeded(f"{field_name} exceeds {max_chars} characters")
    return text


def _enum_value(enum_cls: type[StrEnum], value: Any, *, field_name: str) -> Any:
    if isinstance(value, enum_cls):
        return value
    if not isinstance(value, str):
        raise ReviewValidationError(f"{field_name} must be one of {[e.value for e in enum_cls]}")
    try:
        return enum_cls(value)
    except ValueError as exc:
        raise ReviewValidationError(
            f"{field_name} must be one of {[e.value for e in enum_cls]}"
        ) from exc


def _line_number(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReviewValidationError(f"{field_name} must be an integer")
    if value < 1:
        raise ReviewValidationError(f"{field_name} must be >= 1")
    if value > MAX_LINE_NUMBER:
        raise ReviewBoundsExceeded(f"{field_name} exceeds {MAX_LINE_NUMBER}")
    return value


def snippet_hash(snippet: str) -> str:
    """Content hash binding a finding to its cited evidence text (tamper-evident)."""
    return hashlib.sha256(snippet.encode("utf-8")).hexdigest()


def _normalize_review_path(value: Any) -> str:
    """A repo-relative, forward-slash, traversal-free path a diff can name.

    Model output is untrusted; a path that escapes the tree, is absolute, or contains a
    traversal component is rejected outright (never verified, never materialized).
    """
    text = _bounded_text(value, field_name="file_path", max_chars=MAX_PATH_CHARS)
    normalized = text.replace("\\", "/").strip()
    if normalized.startswith("/") or normalized.startswith("~"):
        raise ReviewValidationError("file_path must be repository-relative")
    if len(normalized) >= 2 and normalized[1] == ":":
        raise ReviewValidationError("file_path must not be a drive-absolute path")
    parts = normalized.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ReviewValidationError("file_path must not contain empty or traversal components")
    return normalized


# --- Findings ------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReviewFinding:
    """One bounded observation with cited, verifiable evidence."""

    severity: Severity
    confidence: Confidence
    title: str
    explanation: str
    file_path: str
    line_start: int
    line_end: int
    recommendation: str
    snippet: str
    snippet_sha256: str
    verified: bool = False
    verification: str = ""

    def __post_init__(self) -> None:
        if self.line_end < self.line_start:
            raise ReviewValidationError("line_end must be >= line_start")
        if self.line_end - self.line_start + 1 > MAX_FINDING_LINE_SPAN:
            raise ReviewBoundsExceeded(
                f"a finding may cite at most {MAX_FINDING_LINE_SPAN} contiguous lines"
            )
        if self.snippet_sha256 != snippet_hash(self.snippet):
            raise ReviewValidationError("snippet_sha256 does not match snippet")

    @classmethod
    def from_model_output(cls, data: Any) -> ReviewFinding:
        """Parse and strictly validate one finding emitted by the provider (untrusted)."""
        if not isinstance(data, Mapping):
            raise ReviewValidationError("finding must be an object")
        severity = _enum_value(Severity, data.get("severity"), field_name="severity")
        confidence = _enum_value(Confidence, data.get("confidence"), field_name="confidence")
        title = _bounded_text(data.get("title"), field_name="title", max_chars=MAX_TITLE_CHARS)
        explanation = _bounded_text(
            data.get("explanation"), field_name="explanation", max_chars=MAX_EXPLANATION_CHARS
        )
        file_path = _normalize_review_path(data.get("file_path"))
        line_start = _line_number(data.get("line_start"), field_name="line_start")
        line_end_raw = data.get("line_end", line_start)
        line_end = _line_number(line_end_raw, field_name="line_end")
        recommendation = _bounded_text(
            data.get("recommendation"),
            field_name="recommendation",
            max_chars=MAX_RECOMMENDATION_CHARS,
        )
        snippet = _bounded_text(
            data.get("snippet"), field_name="snippet", max_chars=MAX_SNIPPET_CHARS
        )
        return cls(
            severity=severity,
            confidence=confidence,
            title=title,
            explanation=explanation,
            file_path=file_path,
            line_start=line_start,
            line_end=line_end,
            recommendation=recommendation,
            snippet=snippet,
            snippet_sha256=snippet_hash(snippet),
            verified=False,
            verification="",
        )

    def as_verified(self, note: str = "") -> ReviewFinding:
        return replace(self, verified=True, verification=note)

    def downgraded(self, *, confidence: Confidence, note: str) -> ReviewFinding:
        """Lower a finding's confidence (e.g. its evidence could not be fully confirmed)."""
        return replace(self, verified=False, confidence=confidence, verification=note)

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity.value,
            "confidence": self.confidence.value,
            "title": self.title,
            "explanation": self.explanation,
            "file_path": self.file_path,
            "line_start": self.line_start,
            "line_end": self.line_end,
            "recommendation": self.recommendation,
            "snippet": self.snippet,
            "snippet_sha256": self.snippet_sha256,
            "verified": self.verified,
            "verification": self.verification,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReviewFinding:
        snippet = str(data["snippet"])
        return cls(
            severity=Severity(str(data["severity"])),
            confidence=Confidence(str(data["confidence"])),
            title=str(data["title"]),
            explanation=str(data["explanation"]),
            file_path=str(data["file_path"]),
            line_start=int(data["line_start"]),
            line_end=int(data["line_end"]),
            recommendation=str(data["recommendation"]),
            snippet=snippet,
            snippet_sha256=str(data.get("snippet_sha256") or snippet_hash(snippet)),
            verified=bool(data.get("verified", False)),
            verification=str(data.get("verification", "")),
        )


# --- Request -------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReviewBudget:
    """The bounded, always-enforced resource envelope for one review (never unlimited)."""

    token_budget: int = DEFAULT_REVIEW_TOKEN_BUDGET
    output_max_tokens: int = DEFAULT_REVIEW_OUTPUT_MAX_TOKENS
    cost_ceiling_usd: float = DEFAULT_REVIEW_COST_CEILING_USD
    max_provider_attempts: int = DEFAULT_REVIEW_MAX_PROVIDER_ATTEMPTS

    def __post_init__(self) -> None:
        if not (1 <= self.token_budget <= MAX_REVIEW_TOKEN_BUDGET):
            raise ReviewBoundsExceeded(f"token_budget must be 1..{MAX_REVIEW_TOKEN_BUDGET}")
        if not (1 <= self.output_max_tokens <= MAX_REVIEW_OUTPUT_MAX_TOKENS):
            raise ReviewBoundsExceeded(
                f"output_max_tokens must be 1..{MAX_REVIEW_OUTPUT_MAX_TOKENS}"
            )
        if not (0.0 < self.cost_ceiling_usd <= MAX_REVIEW_COST_CEILING_USD):
            raise ReviewBoundsExceeded(
                f"cost_ceiling_usd must be >0 and <= {MAX_REVIEW_COST_CEILING_USD}"
            )
        if not (1 <= self.max_provider_attempts <= MAX_REVIEW_PROVIDER_ATTEMPTS):
            raise ReviewBoundsExceeded(
                f"max_provider_attempts must be 1..{MAX_REVIEW_PROVIDER_ATTEMPTS}"
            )


@dataclass(frozen=True, slots=True)
class ReviewRequest:
    """An authorized request to review one project change set."""

    org_id: str
    project_id: str
    source: ReviewSource
    head: str
    idempotency_key: str
    model: str
    base: str | None = None
    agent_id: str | None = None
    max_findings: int = MAX_FINDINGS
    max_diff_bytes: int = DEFAULT_MAX_DIFF_BYTES
    token_budget: int = DEFAULT_REVIEW_TOKEN_BUDGET
    output_max_tokens: int = DEFAULT_REVIEW_OUTPUT_MAX_TOKENS
    cost_ceiling_usd: float = DEFAULT_REVIEW_COST_CEILING_USD
    max_provider_attempts: int = DEFAULT_REVIEW_MAX_PROVIDER_ATTEMPTS

    def __post_init__(self) -> None:
        if not self.org_id or not self.project_id:
            raise ReviewValidationError("org_id and project_id are required")
        if not (1 <= self.max_findings <= MAX_FINDINGS):
            raise ReviewBoundsExceeded(f"max_findings must be 1..{MAX_FINDINGS}")
        if not (1 <= self.max_diff_bytes <= DEFAULT_MAX_DIFF_BYTES):
            raise ReviewBoundsExceeded(f"max_diff_bytes must be 1..{DEFAULT_MAX_DIFF_BYTES}")
        _bounded_text(self.model, field_name="model", max_chars=MAX_MODEL_CHARS)
        _bounded_text(self.head, field_name="head", max_chars=MAX_REF_CHARS)
        if self.base is not None:
            _bounded_text(self.base, field_name="base", max_chars=MAX_REF_CHARS)
        if self.source is ReviewSource.pull_request and not self.head.isdigit():
            raise ReviewValidationError("pull_request head must be the PR number")
        # Validate the budget envelope (rejects any unlimited / non-positive value).
        self.budget()

    def budget(self) -> ReviewBudget:
        """The strict, always-enforced resource envelope derived from this request."""
        return ReviewBudget(
            token_budget=self.token_budget,
            output_max_tokens=self.output_max_tokens,
            cost_ceiling_usd=self.cost_ceiling_usd,
            max_provider_attempts=self.max_provider_attempts,
        )


# --- Report --------------------------------------------------------------------------

REPORT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class ReviewReport:
    """The immutable, content-addressable result of one review."""

    review_id: ReviewId
    org_id: str
    project_id: str
    run_id: str
    source: ReviewSource
    base_sha: str
    head_sha: str
    model: str
    status: ReviewStatus
    findings: tuple[ReviewFinding, ...]
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    created_at: datetime
    completed_at: datetime | None
    limitations: tuple[str, ...]
    diff_bytes: int
    files_reviewed: int
    truncated: bool
    schema_version: int = REPORT_SCHEMA_VERSION
    # Content hash of the rendered Markdown artifact (the JSON artifact's own hash is the
    # store's content address, recorded on the run/record — a body cannot hold its own hash).
    markdown_sha256: str = ""

    def __post_init__(self) -> None:
        if len(self.findings) > MAX_FINDINGS:
            raise ReviewBoundsExceeded(f"a report holds at most {MAX_FINDINGS} findings")
        if len(self.limitations) > MAX_LIMITATIONS:
            raise ReviewBoundsExceeded(f"a report holds at most {MAX_LIMITATIONS} limitations")
        for limitation in self.limitations:
            _bounded_text(limitation, field_name="limitation", max_chars=MAX_LIMITATION_CHARS)

    @property
    def severity_counts(self) -> dict[str, int]:
        counts = {severity.value: 0 for severity in Severity}
        for finding in self.findings:
            counts[finding.severity.value] += 1
        return counts

    def with_markdown_hash(self, digest: str) -> ReviewReport:
        return replace(self, markdown_sha256=digest)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "review_id": self.review_id,
            "org_id": self.org_id,
            "project_id": self.project_id,
            "run_id": self.run_id,
            "source": self.source.value,
            "base_sha": self.base_sha,
            "head_sha": self.head_sha,
            "model": self.model,
            "status": self.status.value,
            "findings": [finding.to_dict() for finding in self.findings],
            "finding_count": len(self.findings),
            "severity_counts": self.severity_counts,
            "usage": {
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "cost_usd": self.cost_usd,
            },
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "limitations": list(self.limitations),
            "diff_bytes": self.diff_bytes,
            "files_reviewed": self.files_reviewed,
            "truncated": self.truncated,
            "artifacts": {"markdown_sha256": self.markdown_sha256},
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReviewReport:
        usage = data.get("usage", {})
        completed_raw = data.get("completed_at")
        artifacts = data.get("artifacts", {})
        return cls(
            review_id=str(data["review_id"]),
            org_id=str(data["org_id"]),
            project_id=str(data["project_id"]),
            run_id=str(data["run_id"]),
            source=ReviewSource(str(data["source"])),
            base_sha=str(data["base_sha"]),
            head_sha=str(data["head_sha"]),
            model=str(data["model"]),
            status=ReviewStatus(str(data["status"])),
            findings=tuple(ReviewFinding.from_dict(item) for item in data.get("findings", [])),
            prompt_tokens=int(usage.get("prompt_tokens", 0)),
            completion_tokens=int(usage.get("completion_tokens", 0)),
            cost_usd=float(usage.get("cost_usd", 0.0)),
            created_at=datetime.fromisoformat(str(data["created_at"])),
            completed_at=datetime.fromisoformat(str(completed_raw)) if completed_raw else None,
            limitations=tuple(str(item) for item in data.get("limitations", [])),
            diff_bytes=int(data.get("diff_bytes", 0)),
            files_reviewed=int(data.get("files_reviewed", 0)),
            truncated=bool(data.get("truncated", False)),
            schema_version=int(data.get("schema_version", REPORT_SCHEMA_VERSION)),
            markdown_sha256=str(artifacts.get("markdown_sha256", "")),
        )


# --- Status projection ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReviewRecord:
    """The lightweight status projection surfaced by the review status API."""

    review_id: ReviewId
    org_id: str
    project_id: str
    run_id: str
    status: ReviewStatus
    source: ReviewSource
    head: str
    base: str | None
    model: str
    created_at: datetime
    updated_at: datetime
    finding_count: int = 0
    severity_counts: Mapping[str, int] = field(default_factory=dict)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float = 0.0
    report_json_sha256: str | None = None
    report_markdown_sha256: str | None = None
    error_kind: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "review_id": self.review_id,
            "org_id": self.org_id,
            "project_id": self.project_id,
            "run_id": self.run_id,
            "status": self.status.value,
            "source": self.source.value,
            "head": self.head,
            "base": self.base,
            "model": self.model,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "finding_count": self.finding_count,
            "severity_counts": dict(self.severity_counts),
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "cost_usd": self.cost_usd,
            "report_json_sha256": self.report_json_sha256,
            "report_markdown_sha256": self.report_markdown_sha256,
            "error_kind": self.error_kind,
            "error_message": self.error_message,
        }


def sort_findings(findings: Sequence[ReviewFinding]) -> tuple[ReviewFinding, ...]:
    """Deterministic ordering: highest severity, then highest confidence, then path/line."""
    return tuple(
        sorted(
            findings,
            key=lambda f: (
                -severity_rank(f.severity),
                -confidence_rank(f.confidence),
                f.file_path,
                f.line_start,
                f.title,
            ),
        )
    )


__all__ = [
    "DEFAULT_MAX_DIFF_BYTES",
    "DEFAULT_REVIEW_COST_CEILING_USD",
    "DEFAULT_REVIEW_MAX_PROVIDER_ATTEMPTS",
    "DEFAULT_REVIEW_OUTPUT_MAX_TOKENS",
    "DEFAULT_REVIEW_TOKEN_BUDGET",
    "MAX_FINDINGS",
    "MAX_FINDING_LINE_SPAN",
    "MAX_LIMITATIONS",
    "MAX_REVIEW_COST_CEILING_USD",
    "MAX_REVIEW_OUTPUT_MAX_TOKENS",
    "MAX_REVIEW_PROVIDER_ATTEMPTS",
    "MAX_REVIEW_TOKEN_BUDGET",
    "REPORT_SCHEMA_VERSION",
    "Confidence",
    "ReviewBudget",
    "ReviewFinding",
    "ReviewId",
    "ReviewRecord",
    "ReviewReport",
    "ReviewRequest",
    "ReviewSource",
    "ReviewStatus",
    "Severity",
    "confidence_rank",
    "new_review_id",
    "severity_rank",
    "snippet_hash",
    "sort_findings",
]
