"""Unified-diff parsing + isolated local diff computation for the review MVP (WS-R).

Two responsibilities, both control-plane only:

* :func:`parse_unified_diff` turns a ``git diff`` text into structured per-file hunks with
  the exact set of new-file line numbers the diff covers. Evidence verification uses this to
  reject any finding whose line reference is not part of the reviewed change.
* :class:`GitDiffComputer` runs ``git diff`` inside an already-materialized, isolated,
  read-only worktree (an anonymous clone with no remote/alternates) to produce a bounded
  diff for a branch/commit change set. It never touches the authoritative repository and
  never performs a network or write operation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from keel_core.coding.local import GitRunner

from .errors import ReviewBoundsExceeded, ReviewValidationError

_HUNK_HEADER = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
_DIFF_GIT = re.compile(r"^diff --git a/(.+) b/(.+)$")
# A safe git object name for base/head resolution (sha, branch, tag). No options/traversal.
_SAFE_OBJECT = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._/-]{0,254}\Z")


class FileChangeKind(StrEnum):
    added = "added"
    modified = "modified"
    deleted = "deleted"
    renamed = "renamed"


@dataclass(frozen=True, slots=True)
class DiffHunk:
    old_start: int
    old_count: int
    new_start: int
    new_count: int
    # New-file line numbers present in this hunk (added + context) — the lines a finding may
    # cite. Deletions have no new-file line and are excluded.
    new_lines: frozenset[int]
    added_lines: frozenset[int]


@dataclass(frozen=True, slots=True)
class DiffFile:
    path: str
    old_path: str | None
    kind: FileChangeKind
    is_binary: bool
    hunks: tuple[DiffHunk, ...] = ()
    # The raw unified-diff text of *this file only* (its ``diff --git`` section). Evidence
    # verification scopes snippet confirmation to this body so a snippet lifted from another
    # file's hunk can never satisfy a finding cited against this path.
    body: str = ""

    @property
    def reviewed_lines(self) -> frozenset[int]:
        """Every new-file line number this diff touches for the file."""
        result: set[int] = set()
        for hunk in self.hunks:
            result |= hunk.new_lines
        return frozenset(result)

    @property
    def hunk_intervals(self) -> tuple[tuple[int, int], ...]:
        """Contiguous new-file ``[start, end]`` line intervals, one per hunk (new-file numbering).

        A hunk covers the inclusive new-file range ``new_start .. new_start + new_count - 1``
        (context + added lines; deletions consume no new-file line number). Pure-deletion hunks
        (``new_count == 0``) contribute no interval.
        """
        intervals: list[tuple[int, int]] = []
        for hunk in self.hunks:
            if hunk.new_count > 0:
                intervals.append((hunk.new_start, hunk.new_start + hunk.new_count - 1))
        return tuple(intervals)

    def range_within_hunk(self, line_start: int, line_end: int) -> bool:
        """Whether ``[line_start, line_end]`` fits ENTIRELY within a single reviewed hunk.

        A one-line overlap with a hunk is deliberately NOT sufficient: a finding that cites a
        broad range spilling outside the changed hunk (e.g. to smuggle an unchanged line into
        an evidence window) is rejected. The whole cited span must lie inside one hunk's
        new-file bounds.
        """
        if line_start > line_end:
            return False
        return any(start <= line_start and line_end <= end for start, end in self.hunk_intervals)


@dataclass(frozen=True, slots=True)
class ReviewDiff:
    files: tuple[DiffFile, ...]
    raw_text: str
    byte_size: int
    truncated: bool

    @property
    def paths(self) -> frozenset[str]:
        return frozenset(f.path for f in self.files)

    def file_for(self, path: str) -> DiffFile | None:
        for file in self.files:
            if file.path == path:
                return file
        return None

    def by_path(self) -> Mapping[str, DiffFile]:
        return {file.path: file for file in self.files}


def _strip_prefix(path: str) -> str:
    # git prefixes paths with a/ and b/; also handle the "dev/null" sentinel.
    if path in ("/dev/null", "dev/null"):
        return path
    for prefix in ("a/", "b/"):
        if path.startswith(prefix):
            return path[len(prefix) :]
    return path


def parse_unified_diff(text: str) -> tuple[DiffFile, ...]:
    """Parse a ``git diff`` into structured per-file hunks (new-file line tracking).

    Tolerant of the header variations git emits (new/deleted/renamed/binary files). Only the
    new-file side is tracked because findings reference the reviewed (head) content.
    """
    files: list[DiffFile] = []
    lines = text.splitlines()
    index = 0
    total = len(lines)

    def flush(
        path: str | None,
        old_path: str | None,
        kind: FileChangeKind,
        is_binary: bool,
        hunks: list[DiffHunk],
        end_index: int,
    ) -> None:
        if path is None:
            return
        body = "\n".join(lines[current_start:end_index]) if current_start is not None else ""
        files.append(
            DiffFile(
                path=path,
                old_path=old_path if old_path and old_path != path else None,
                kind=kind,
                is_binary=is_binary,
                hunks=tuple(hunks),
                body=body,
            )
        )

    current_path: str | None = None
    current_old: str | None = None
    current_kind = FileChangeKind.modified
    current_binary = False
    current_hunks: list[DiffHunk] = []
    current_start: int | None = None

    while index < total:
        line = lines[index]
        git_header = _DIFF_GIT.match(line)
        if git_header is not None:
            flush(current_path, current_old, current_kind, current_binary, current_hunks, index)
            current_old = _strip_prefix(git_header.group(1))
            current_path = _strip_prefix(git_header.group(2))
            current_kind = FileChangeKind.modified
            current_binary = False
            current_hunks = []
            current_start = index
            index += 1
            continue
        if current_path is None:
            index += 1
            continue
        if line.startswith("new file mode"):
            current_kind = FileChangeKind.added
        elif line.startswith("deleted file mode"):
            current_kind = FileChangeKind.deleted
        elif line.startswith("rename from "):
            current_old = line[len("rename from ") :].strip()
            current_kind = FileChangeKind.renamed
        elif line.startswith("rename to "):
            current_path = line[len("rename to ") :].strip()
            current_kind = FileChangeKind.renamed
        elif line.startswith("Binary files ") or line.startswith("GIT binary patch"):
            current_binary = True
        elif line.startswith("+++ "):
            candidate = _strip_prefix(line[4:].strip())
            if candidate not in ("/dev/null", "dev/null"):
                current_path = candidate
        elif line.startswith("--- "):
            candidate = _strip_prefix(line[4:].strip())
            if candidate not in ("/dev/null", "dev/null"):
                current_old = candidate
        else:
            hunk_header = _HUNK_HEADER.match(line)
            if hunk_header is not None:
                index = _consume_hunk(lines, index, hunk_header, current_hunks)
                continue
        index += 1

    flush(current_path, current_old, current_kind, current_binary, current_hunks, total)
    return tuple(files)


def _consume_hunk(
    lines: list[str], index: int, header: re.Match[str], hunks: list[DiffHunk]
) -> int:
    old_start = int(header.group(1))
    old_count = int(header.group(2) or "1")
    new_start = int(header.group(3))
    new_count = int(header.group(4) or "1")
    new_lines: set[int] = set()
    added: set[int] = set()
    new_cursor = new_start
    index += 1
    total = len(lines)
    while index < total:
        line = lines[index]
        if line.startswith("@@") or _DIFF_GIT.match(line) is not None:
            break
        if line.startswith("+"):
            new_lines.add(new_cursor)
            added.add(new_cursor)
            new_cursor += 1
        elif line.startswith("-"):
            pass  # deleted old-file line: no new-file line number
        elif line.startswith("\\"):
            pass  # "\ No newline at end of file"
        else:
            # context line (starts with a space, or an empty context line)
            new_lines.add(new_cursor)
            new_cursor += 1
        index += 1
    hunks.append(
        DiffHunk(
            old_start=old_start,
            old_count=old_count,
            new_start=new_start,
            new_count=new_count,
            new_lines=frozenset(new_lines),
            added_lines=frozenset(added),
        )
    )
    return index


def _validate_object(value: str, *, field_name: str) -> str:
    if not _SAFE_OBJECT.fullmatch(value) or ".." in value or "@{" in value:
        raise ReviewValidationError(f"unsafe git object for {field_name}")
    return value


@dataclass(frozen=True, slots=True)
class ResolvedRange:
    base_sha: str
    head_sha: str


@dataclass
class GitDiffComputer:
    """Compute a bounded diff inside an isolated, read-only worktree (no network/writes)."""

    git: GitRunner = field(default_factory=GitRunner)
    max_diff_bytes: int = 1_000_000

    def resolve(self, worktree: Path, ref: str, *, field_name: str) -> str:
        """Resolve a ref/sha to a full commit sha within the isolated worktree."""
        _validate_object(ref, field_name=field_name)
        result = self.git.run(
            ["rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
            cwd=worktree,
            check=False,
        )
        sha = result.stdout.strip()
        if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise ReviewValidationError(f"could not resolve {field_name} '{ref}' in the change set")
        return sha

    def parent_of(self, worktree: Path, sha: str) -> str:
        """The first parent of ``sha`` (used as the implicit base for a single-commit review)."""
        result = self.git.run(
            ["rev-parse", "--verify", "--quiet", f"{sha}^1^{{commit}}"],
            cwd=worktree,
            check=False,
        )
        parent = result.stdout.strip()
        if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", parent):
            # Root commit: diff against the empty tree so the whole commit is "added".
            return _EMPTY_TREE
        return parent

    def merge_base(self, worktree: Path, base_sha: str, head_sha: str) -> str:
        """The merge-base of ``base_sha`` and ``head_sha`` (the review base for a branch).

        A branch review with an omitted base diffs from where the branch *diverged* from the
        project default branch — never the raw tip of the default branch — so the review shows
        exactly the branch's own changes. Falls back to the empty tree only when the two commits
        share no history (an unrelated/new branch), so the whole branch is "added".
        """
        for token in (base_sha, head_sha):
            if token != _EMPTY_TREE and not re.fullmatch(r"[0-9a-f]{40}", token):
                raise ReviewValidationError("merge_base requires resolved commit shas")
        result = self.git.run(
            ["merge-base", base_sha, head_sha],
            cwd=worktree,
            check=False,
        )
        base = result.stdout.strip()
        if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", base):
            return _EMPTY_TREE
        return base

    def compute(self, worktree: Path, base: str, head: str) -> ReviewDiff:
        """Produce the bounded ``base..head`` unified diff for the worktree."""
        base_arg = base if base == _EMPTY_TREE else _validate_object(base, field_name="base")
        head_arg = _validate_object(head, field_name="head")
        args = [
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--unified=3",
            "--find-renames",
            base_arg,
            head_arg,
        ]
        result = self.git.run(args, cwd=worktree, check=True)
        raw = result.stdout
        encoded = raw.encode("utf-8", errors="replace")
        truncated = False
        if len(encoded) > self.max_diff_bytes:
            raise ReviewBoundsExceeded(
                f"diff of {len(encoded)} bytes exceeds the {self.max_diff_bytes}-byte limit"
            )
        files = parse_unified_diff(raw)
        return ReviewDiff(files=files, raw_text=raw, byte_size=len(encoded), truncated=truncated)


# git's canonical empty tree object — a stable base for root-commit diffs.
_EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


__all__ = [
    "DiffFile",
    "DiffHunk",
    "FileChangeKind",
    "GitDiffComputer",
    "ResolvedRange",
    "ReviewDiff",
    "parse_unified_diff",
]
