"""Read-only managed-code review (MVP, WS-R).

A focused, read-only review pipeline that reuses the existing project/GitHub integration,
coding storage, durable runs/jobs, identity/authz, provider seam, and observability. It
reviews a project branch/commit/PR change set and produces immutable, evidence-verified,
content-addressed report artifacts. It makes **no** code changes, pushes nothing, and posts
no comments — a human validates every finding.
"""

from __future__ import annotations

from .audit import (
    LoggingReviewAuditSink,
    ReviewAuditAction,
    ReviewAuditEvent,
    ReviewAuditSink,
)
from .coordinator import (
    REVIEW_SURFACE,
    ReviewCoordinator,
    ReviewHandle,
    review_fingerprint,
)
from .diff import (
    DiffFile,
    DiffHunk,
    GitDiffComputer,
    ReviewDiff,
    parse_unified_diff,
)
from .engine import ReviewEngine, ReviewEngineResult
from .errors import (
    ReviewBoundsExceeded,
    ReviewError,
    ReviewEvidenceError,
    ReviewLeaseLost,
    ReviewNotFound,
    ReviewProviderError,
    ReviewProviderUnavailable,
    ReviewValidationError,
)
from .evidence import EvidenceVerifier, RejectedFinding, VerificationOutcome
from .github_refs import GitHubPullRequestResolver
from .jobs import (
    REVIEW_RUN_KIND,
    REVIEW_RUN_MAX_ATTEMPTS,
    ReviewJobHandlers,
    ReviewJobPayload,
    review_idempotency_key,
)
from .models import (
    Confidence,
    ReviewBudget,
    ReviewFinding,
    ReviewId,
    ReviewRecord,
    ReviewReport,
    ReviewRequest,
    ReviewSource,
    ReviewStatus,
    Severity,
    new_review_id,
)
from .refs import (
    MaterializationPlan,
    PullRequestResolver,
    ResolvedPullRequest,
    build_materialization_plan,
)
from .report import (
    ReviewArtifactWriter,
    StoredReport,
    render_json_bytes,
    render_markdown,
)
from .service import ReviewOutcome, ReviewService

__all__ = [
    "REVIEW_RUN_KIND",
    "REVIEW_RUN_MAX_ATTEMPTS",
    "REVIEW_SURFACE",
    "Confidence",
    "DiffFile",
    "DiffHunk",
    "EvidenceVerifier",
    "GitDiffComputer",
    "GitHubPullRequestResolver",
    "LoggingReviewAuditSink",
    "MaterializationPlan",
    "PullRequestResolver",
    "RejectedFinding",
    "ReviewArtifactWriter",
    "ReviewAuditAction",
    "ReviewAuditEvent",
    "ReviewAuditSink",
    "ReviewBoundsExceeded",
    "ReviewBudget",
    "ReviewCoordinator",
    "ReviewDiff",
    "ReviewEngine",
    "ReviewEngineResult",
    "ReviewError",
    "ReviewEvidenceError",
    "ReviewFinding",
    "ReviewHandle",
    "ReviewId",
    "ReviewJobHandlers",
    "ReviewJobPayload",
    "ReviewLeaseLost",
    "ReviewNotFound",
    "ReviewOutcome",
    "ReviewProviderError",
    "ReviewProviderUnavailable",
    "ReviewRecord",
    "ReviewReport",
    "ReviewRequest",
    "ReviewService",
    "ReviewSource",
    "ReviewStatus",
    "ReviewValidationError",
    "ResolvedPullRequest",
    "Severity",
    "StoredReport",
    "VerificationOutcome",
    "build_materialization_plan",
    "new_review_id",
    "parse_unified_diff",
    "render_json_bytes",
    "render_markdown",
    "review_fingerprint",
    "review_idempotency_key",
]
