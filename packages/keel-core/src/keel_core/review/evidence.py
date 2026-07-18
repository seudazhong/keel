"""Evidence verification: no finding survives without a confirmable file+line (WS-R).

A model can hallucinate a file, a line number, or a code snippet. Before a finding reaches a
report it is checked against the *actual* reviewed change set and the *actual* materialized
(read-only) worktree:

* A path that is neither in the diff nor present on disk is a fabricated file → **rejected**.
* A path present on disk but outside the reviewed diff → **downgraded** (kept, low confidence,
  annotated) because it is a real file but not part of this change.
* A line range that does not overlap the reviewed hunks → **downgraded**.
* A snippet that appears nowhere in the diff or the file → fabricated evidence → **rejected**.

This is the enforcement home of "no invented file/line evidence". It fails closed: any I/O or
path-safety error on a finding rejects that finding rather than trusting it.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .diff import ReviewDiff
from .models import Confidence, ReviewFinding

# Read at most this many bytes of a cited file when confirming snippet evidence.
_MAX_FILE_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class RejectedFinding:
    finding: ReviewFinding
    reason: str


@dataclass(frozen=True, slots=True)
class VerificationOutcome:
    kept: tuple[ReviewFinding, ...]
    rejected: tuple[RejectedFinding, ...]


def _normalize(text: str) -> str:
    return " ".join(text.split())


def _safe_read(worktree: Path, rel_path: str) -> str | None:
    """Read a repo-relative file from the worktree, refusing traversal/symlink escape."""
    try:
        base = worktree.resolve(strict=True)
    except OSError:
        return None
    candidate = base / rel_path
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        return None
    if resolved != base and base not in resolved.parents:
        return None
    if resolved.is_symlink() or not resolved.is_file():
        return None
    try:
        with resolved.open("rb") as handle:
            data = handle.read(_MAX_FILE_BYTES + 1)
    except OSError:
        return None
    if len(data) > _MAX_FILE_BYTES:
        return None
    return data.decode("utf-8", errors="replace")


def _snippet_supported(snippet: str, *, diff_text: str, file_text: str | None) -> bool:
    """Whether the snippet's substance appears in the diff or the file (not fabricated)."""
    normalized_diff = _normalize(diff_text)
    normalized_file = _normalize(file_text) if file_text is not None else ""
    candidate_lines = [
        _normalize(line) for line in snippet.splitlines() if len(_normalize(line)) >= 4
    ]
    if not candidate_lines:
        # Snippet is trivially short/blank; require the whole normalized snippet to appear.
        whole = _normalize(snippet)
        if not whole:
            return False
        return whole in normalized_diff or (bool(normalized_file) and whole in normalized_file)
    for line in candidate_lines:
        if line in normalized_diff or (normalized_file and line in normalized_file):
            return True
    return False


class EvidenceVerifier:
    """Verify each finding's cited file+line+snippet against the reviewed diff and worktree."""

    def __init__(self, worktree: Path, diff: ReviewDiff) -> None:
        self._worktree = worktree
        self._diff = diff
        self._by_path = diff.by_path()

    def verify(self, findings: tuple[ReviewFinding, ...]) -> VerificationOutcome:
        kept: list[ReviewFinding] = []
        rejected: list[RejectedFinding] = []
        for finding in findings:
            result = self._verify_one(finding)
            if isinstance(result, RejectedFinding):
                rejected.append(result)
            else:
                kept.append(result)
        return VerificationOutcome(kept=tuple(kept), rejected=tuple(rejected))

    def _verify_one(self, finding: ReviewFinding) -> ReviewFinding | RejectedFinding:
        diff_file = self._by_path.get(finding.file_path)
        file_text = _safe_read(self._worktree, finding.file_path)

        if diff_file is None:
            if file_text is None:
                return RejectedFinding(
                    finding, "cited file is not in the diff and does not exist in the worktree"
                )
            # Real file, but not part of this change set: keep, but do not vouch for it.
            if not _snippet_supported(
                finding.snippet, diff_text=self._diff.raw_text, file_text=file_text
            ):
                return RejectedFinding(finding, "snippet not found in cited file")
            return finding.downgraded(
                confidence=Confidence.low, note="file is outside the reviewed diff"
            )

        if not _snippet_supported(
            finding.snippet, diff_text=self._diff.raw_text, file_text=file_text
        ):
            return RejectedFinding(finding, "snippet not found in the reviewed diff or file")

        reviewed = diff_file.reviewed_lines
        finding_lines = set(range(finding.line_start, finding.line_end + 1))
        if reviewed and finding_lines & reviewed:
            return finding.as_verified("file and line verified against the reviewed diff")
        if not reviewed:
            # Binary or metadata-only change: no line evidence to confirm.
            return finding.downgraded(
                confidence=Confidence.low,
                note="no textual diff lines to confirm the cited range",
            )
        return finding.downgraded(
            confidence=Confidence.low,
            note="cited line is outside the reviewed hunks for this file",
        )


__all__ = ["EvidenceVerifier", "RejectedFinding", "VerificationOutcome"]
