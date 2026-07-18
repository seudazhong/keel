"""ReviewService — the read-only review execution core (WS-R).

Given an authorized :class:`ReviewRequest` and a project's coding storage handle, this service
performs the whole read-only review with no side effects beyond the (retained) report
artifacts:

1. Materialize an **isolated, disposable** worktree (an anonymous clone with no remote and no
   alternates) from the authoritative repository — never a writable mount of it.
2. Compute a **bounded** ``base..head`` unified diff inside that worktree.
3. Run the review agent through the existing provider seam (no tools → strongest read-only
   guarantee), with structured-output validation and a bounded repair loop.
4. **Verify** every finding's file/line/snippet against the diff and worktree; reject or
   downgrade anything that cannot be confirmed (no invented evidence).
5. Render + store immutable, content-addressed JSON + Markdown report artifacts.
6. Always dispose of the worktree (cleanup is idempotent).

Authorization, durable run/job bookkeeping, and audit live in :mod:`.coordinator`; this class
is a pure, deterministic unit that the coordinator drives.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from keel_core.coding.models import CodingRunId, ProjectId
from keel_core.coding.protocols import ArtifactStore, WorktreeStore
from keel_core.protocols import ProviderGateway, Usage

from .diff import GitDiffComputer, ReviewDiff
from .engine import ReviewEngine
from .evidence import EvidenceVerifier
from .models import (
    MAX_LIMITATIONS,
    ReviewReport,
    ReviewRequest,
    ReviewSource,
    ReviewStatus,
    new_review_id,
    sort_findings,
)
from .prompts import build_messages
from .report import ReviewArtifactWriter, StoredReport


@dataclass(frozen=True, slots=True)
class ReviewOutcome:
    review_id: str
    report: ReviewReport
    summary: str
    json_sha256: str
    markdown_sha256: str
    base_sha: str
    head_sha: str
    rejected_count: int
    usage: Usage


@dataclass
class ReviewService:
    """Execute one read-only review end-to-end over isolated storage + the provider seam."""

    worktrees: WorktreeStore
    artifacts: ArtifactStore
    provider: ProviderGateway
    diff_computer: GitDiffComputer | None = None
    max_repairs: int = 1

    async def review(
        self,
        request: ReviewRequest,
        *,
        run_id: str,
        coding_run_id: str,
        project_handle: str,
        review_id: str | None = None,
        now: datetime | None = None,
    ) -> ReviewOutcome:
        review_id = review_id or new_review_id()
        created_at = now or datetime.now(UTC)
        computer = self.diff_computer or GitDiffComputer(max_diff_bytes=request.max_diff_bytes)
        pid = ProjectId(project_handle)
        crid = CodingRunId(coding_run_id)

        head_ref = request.head
        # A crash mid-review can leave a stale worktree; a retry must start clean (idempotent).
        try:
            self.worktrees.remove(pid, crid)
        except Exception:  # noqa: BLE001 — a missing worktree is fine
            pass
        worktree = self.worktrees.materialize(pid, crid, ref=head_ref)
        try:
            worktree_path = worktree.path
            head_sha = computer.resolve(worktree_path, head_ref, field_name="head")
            if request.base is not None:
                base_sha = computer.resolve(worktree_path, request.base, field_name="base")
            else:
                base_sha = computer.parent_of(worktree_path, head_sha)

            diff = computer.compute(worktree_path, base_sha, head_sha)
            engine = ReviewEngine(self.provider, max_repairs=self.max_repairs)
            metadata = self._metadata(request)
            messages = build_messages(
                diff,
                source=request.source.value,
                base_sha=base_sha,
                head_sha=head_sha,
                max_findings=request.max_findings,
                metadata=metadata,
            )
            engine_result = await engine.run(
                model=request.model, messages=messages, max_findings=request.max_findings
            )

            verifier = EvidenceVerifier(worktree_path, diff)
            outcome = verifier.verify(engine_result.findings)
            kept = sort_findings(outcome.kept)
            limitations = self._limitations(diff, engine_result.limitations, outcome.rejected)

            report = ReviewReport(
                review_id=review_id,
                org_id=request.org_id,
                project_id=request.project_id,
                run_id=run_id,
                source=request.source,
                base_sha=base_sha,
                head_sha=head_sha,
                model=request.model,
                status=ReviewStatus.completed,
                findings=kept,
                prompt_tokens=engine_result.usage.prompt_tokens,
                completion_tokens=engine_result.usage.completion_tokens,
                cost_usd=engine_result.usage.cost_usd,
                created_at=created_at,
                completed_at=datetime.now(UTC),
                limitations=limitations,
                diff_bytes=diff.byte_size,
                files_reviewed=len(diff.files),
                truncated=diff.truncated,
            )
            stored = self._store_report(report, request, coding_run_id, engine_result.summary)
            return ReviewOutcome(
                review_id=review_id,
                report=stored.report,
                summary=engine_result.summary,
                json_sha256=stored.json_sha256,
                markdown_sha256=stored.markdown_sha256,
                base_sha=base_sha,
                head_sha=head_sha,
                rejected_count=len(outcome.rejected),
                usage=engine_result.usage,
            )
        finally:
            # Disposable worktree: always reclaimed, even on failure (idempotent).
            try:
                self.worktrees.remove(pid, crid)
            except Exception:  # noqa: BLE001 — cleanup is best-effort and must not mask errors
                pass

    def _store_report(
        self,
        report: ReviewReport,
        request: ReviewRequest,
        coding_run_id: str,
        summary: str,
    ) -> StoredReport:
        writer = ReviewArtifactWriter(self.artifacts)
        return writer.store(
            report,
            project_id=request.project_id,
            coding_run_id=coding_run_id,
            summary=summary,
        )

    @staticmethod
    def _metadata(request: ReviewRequest) -> tuple[tuple[str, str], ...]:
        meta: list[tuple[str, str]] = []
        if request.source is ReviewSource.pull_request:
            meta.append(("pull_request", request.head))
        elif request.source is ReviewSource.branch:
            meta.append(("branch", request.head))
        return tuple(meta)

    @staticmethod
    def _limitations(
        diff: ReviewDiff,
        engine_limitations: tuple[str, ...],
        rejected: tuple[object, ...],
    ) -> tuple[str, ...]:
        limitations: list[str] = list(engine_limitations)
        if rejected:
            limitations.append(
                f"{len(rejected)} finding(s) were rejected because their cited file/line/snippet "
                "could not be verified against the reviewed diff."
            )
        if diff.truncated:
            limitations.append("The diff was truncated to the configured size limit.")
        # Deduplicate while preserving order and honouring the report bound.
        seen: set[str] = set()
        unique: list[str] = []
        for item in limitations:
            if item not in seen:
                seen.add(item)
                unique.append(item)
        return tuple(unique[:MAX_LIMITATIONS])


__all__ = ["ReviewOutcome", "ReviewService"]
