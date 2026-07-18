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


# Number of context lines to include on each side of a cited range when confirming that a
# snippet actually appears at the location a finding claims (tolerates small off-by-a-few
# citations without accepting a snippet from a completely different part of the file).
_LINE_CONTEXT = 3
# The shortest normalized snippet line worth matching (skips trivial "{"/"}" style noise).
_MIN_LINE_CHARS = 4


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


def _substantive_lines(snippet: str) -> list[str]:
    return [
        norm for line in snippet.splitlines() if len(norm := _normalize(line)) >= _MIN_LINE_CHARS
    ]


def _snippet_present_in(text: str, snippet: str) -> bool:
    """Whether *any* substantive snippet line appears anywhere in ``text``.

    Used only to tell a *moved* finding (its snippet exists in the cited file, just not at the
    cited line → downgrade) apart from a *fabricated* one (its snippet is nowhere in the cited
    file → reject). ``text`` is always scoped to the cited file, never the whole diff.
    """
    haystack = _normalize(text)
    if not haystack:
        return False
    lines = _substantive_lines(snippet)
    if not lines:
        whole = _normalize(snippet)
        return bool(whole) and whole in haystack
    return any(line in haystack for line in lines)


def _snippet_complete_at(file_text: str, line_start: int, line_end: int, snippet: str) -> bool:
    """Whether the *complete* normalized snippet appears CONTIGUOUSLY at the cited line window.

    The window is the cited ``line_start..line_end`` range (1-based, new-file numbering) padded
    by :data:`_LINE_CONTEXT` lines. A multi-line snippet must appear as a **contiguous, ordered**
    block of the window's lines — matching line 1 here and line 2 fifty lines away is rejected.
    This is the exact-location check that stops a snippet stitched together from independent,
    non-adjacent lines (or lifted from a different part of the file) from satisfying a finding.
    """
    file_lines = file_text.splitlines()
    if line_start > len(file_lines):
        return False
    lo = max(0, line_start - 1 - _LINE_CONTEXT)
    hi = min(len(file_lines), line_end + _LINE_CONTEXT)
    window_lines = [_normalize(line) for line in file_lines[lo:hi]]
    snippet_lines = _substantive_lines(snippet)
    if not snippet_lines:
        # No substantive lines (all trivial/braces): fall back to a whole-snippet substring.
        whole = _normalize(snippet)
        window = " ".join(w for w in window_lines if w)
        return bool(whole) and whole in window
    return _contiguous_ordered_match(window_lines, snippet_lines)


def _contiguous_ordered_match(window_lines: list[str], snippet_lines: list[str]) -> bool:
    """Whether ``snippet_lines`` appear as a contiguous, in-order run within ``window_lines``.

    Empty (normalized-blank) window lines are skipped so incidental blank lines between real
    code don't break contiguity, but the substantive snippet lines must still align consecutively
    and in order. Each aligned window line must *contain* the snippet line (post-normalization),
    tolerating a trailing comment while still enforcing order + adjacency.
    """
    compact = [line for line in window_lines if line]
    n, m = len(compact), len(snippet_lines)
    if m == 0 or m > n:
        return m == 0
    for start in range(n - m + 1):
        if all(snippet_lines[k] in compact[start + k] for k in range(m)):
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

        if diff_file is None and file_text is None:
            return RejectedFinding(
                finding, "cited file is not in the diff and does not exist in the worktree"
            )

        # Snippet confirmation is scoped strictly to the cited file: the file's head content
        # (if present) and its own diff section — never the whole diff. A snippet lifted from a
        # different file therefore cannot satisfy this finding.
        scoped: list[str] = []
        if file_text is not None:
            scoped.append(file_text)
        if diff_file is not None and diff_file.body:
            scoped.append(diff_file.body)
        if not any(_snippet_present_in(text, finding.snippet) for text in scoped):
            return RejectedFinding(finding, "snippet not found in the cited file")

        exact = file_text is not None and _snippet_complete_at(
            file_text, finding.line_start, finding.line_end, finding.snippet
        )

        if diff_file is None:
            # A real file, but outside this change set: keep, but never vouch for it.
            note = (
                "file is outside the reviewed diff"
                if exact
                else "file is outside the reviewed diff and the snippet is not at the cited line"
            )
            return finding.downgraded(confidence=Confidence.low, note=note)

        reviewed = diff_file.reviewed_lines
        # The ENTIRE cited range must lie within one reviewed hunk's new-file bounds. A single
        # overlapping line is not enough — a finding that cites a broad range straddling the
        # hunk boundary (to pull an unchanged line into its evidence window) is not confirmed.
        within = diff_file.range_within_hunk(finding.line_start, finding.line_end)

        if exact and reviewed and within:
            return finding.as_verified("file, line, and snippet verified against the reviewed diff")
        if not reviewed:
            # Binary or metadata-only change: no new-file line evidence to confirm.
            return finding.downgraded(
                confidence=Confidence.low,
                note="no textual diff lines to confirm the cited range",
            )
        if not within:
            return finding.downgraded(
                confidence=Confidence.low,
                note="cited range is not fully within the reviewed hunks for this file",
            )
        return finding.downgraded(
            confidence=Confidence.low,
            note="cited snippet could not be confirmed at the cited line",
        )


__all__ = ["EvidenceVerifier", "RejectedFinding", "VerificationOutcome"]
