"""Controlled patch generation into an isolated, disposable, writable worktree (WS-PP).

Given an authorized :class:`~keel_core.patch.models.PatchProposalRequest` and a project's
authoritative coding-storage handle, this service produces an **immutable, content-addressed
proposal bundle** with no side effect on the authoritative repo other than pinning the exact
proposed commit for a later human-approved writeback:

1. Materialize an **isolated, disposable, writable** worktree (an anonymous clone with no remote
   and no alternates) at the exact approved base commit — the authoritative bare repo is *never*
   writable-mounted into the sandbox.
2. Run the generation agent (:class:`PatchAuthor`) with controlled file read/write/edit + sandboxed
   command/test tools. The agent has **no** remote-Git credentials, **no** network, **no** deploy
   secrets, and **no** host Docker socket; repo/task text is *tainted* and can never change policy.
3. Commit the change deterministically, capture the exact ``base..head`` diff, changed paths +
   blob hashes, proposed commit/tree, and required test commands/results/log refs.
4. **Validate** the change fails closed against forbidden paths (``.git``/``.env``/secrets),
   symlink/submodule escape, and oversize/binary policy — nothing invalid is ever persisted.
5. Persist everything into an immutable, content-addressed bundle and **pin** the exact proposed
   commit in the authoritative repo (so the disposable worktree can be reclaimed while the commit
   survives for writeback). Always dispose of the worktree.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from keel_core.coding.local import LocalCodingStorage
from keel_core.coding.models import CodingRunId, ProjectId
from keel_core.coding.protocols import ArtifactStore
from keel_core.protocols import Usage

from .bundle import PatchBundleWriter, aggregate_test_status, sha256_hex
from .errors import (
    PatchBoundsExceeded,
    PatchPolicyViolation,
    PatchValidationError,
)
from .models import (
    ChangedFile,
    ChangeKind,
    PatchBundleManifest,
    PatchProposalRequest,
    ProposedCommit,
    TestResult,
    TestStatus,
    changed_path_digest,
    is_forbidden_path,
)

# Control-plane pin namespace: the exact proposed commit is kept alive here until writeback.
PIN_REF_PREFIX = "refs/keel-patch/"

_STATUS_TO_KIND = {
    "A": ChangeKind.added,
    "M": ChangeKind.modified,
    "D": ChangeKind.deleted,
    "R": ChangeKind.renamed,
    "C": ChangeKind.added,
    "T": ChangeKind.modified,
}


@dataclass(frozen=True, slots=True)
class AuthorResult:
    """What a :class:`PatchAuthor` reports back after editing the worktree."""

    usage: Usage
    iterations: int = 0


class PatchAuthor(Protocol):
    """Drives the generation agent to edit the disposable worktree (no network/secrets/socket)."""

    async def author(
        self,
        *,
        worktree_path: Path,
        request: PatchProposalRequest,
        coding_run_id: str,
        interrupt: Callable[[], bool] | None = None,
    ) -> AuthorResult: ...


class TestRunner(Protocol):
    """Runs one required test command in the sandbox over the worktree; returns exit + log bytes."""

    __test__: bool = False

    async def run(
        self, *, worktree_path: Path, command: str, coding_run_id: str
    ) -> tuple[int, bytes]: ...


@dataclass(frozen=True, slots=True)
class GenerationOutcome:
    proposal_id: str
    base_sha: str
    head_sha: str
    bundle_sha256: str
    diff_sha256: str
    changed_path_digest: str
    changed_files: int
    test_status: TestStatus
    manifest: PatchBundleManifest
    pin_ref: str
    usage: Usage


def pin_ref_for(proposal_id: str) -> str:
    safe = "".join(c if c.isalnum() or c in "._-" else "-" for c in proposal_id)
    return f"{PIN_REF_PREFIX}{safe}"


@dataclass
class PatchGenerationService:
    """Execute one controlled patch generation end-to-end over isolated, disposable storage."""

    storage: LocalCodingStorage
    artifacts: ArtifactStore
    author: PatchAuthor
    test_runner: TestRunner | None = None
    bundle_retention_days: int = 90

    async def generate(
        self,
        request: PatchProposalRequest,
        *,
        proposal_id: str,
        run_id: str,
        coding_run_id: str,
        project_handle: str,
        now: datetime | None = None,
        interrupt: Callable[[], bool] | None = None,
    ) -> GenerationOutcome:
        created = now or datetime.now(UTC)
        handle = ProjectId(project_handle)
        crid = CodingRunId(coding_run_id)

        # A crash mid-generation can leave a stale worktree; a retry must start clean.
        await asyncio.to_thread(_safe_remove, self.storage, handle, crid)
        worktree = await asyncio.to_thread(
            self.storage.materialize, handle, crid, ref=request.base_ref
        )
        base_sha = worktree.commit
        try:
            # 1) Controlled editing inside the disposable, writable worktree.
            author_result = await self.author.author(
                worktree_path=worktree.path,
                request=request,
                coding_run_id=coding_run_id,
                interrupt=interrupt,
            )

            # 2) Deterministic commit; an empty change is never a valid proposal.
            head_sha = await asyncio.to_thread(
                self.storage.commit_worktree,
                handle,
                crid,
                message=f"Keel patch proposal {proposal_id}",
            )
            if head_sha is None:
                raise PatchValidationError("generation produced no change (empty proposal)")

            # 3) Capture + validate the exact change set (fail closed).
            files = await self._capture_changed_files(handle, crid, base_sha, head_sha)
            diff_bytes, truncated = await asyncio.to_thread(
                self.storage.worktree_diff,
                handle,
                crid,
                base_sha,
                head_sha,
                max_bytes=request.max_diff_bytes,
            )
            if truncated:
                raise PatchBoundsExceeded("proposal diff exceeds the configured size limit")
            _reject_binary_diff(diff_bytes)

            tree_sha = await asyncio.to_thread(
                self.storage.worktree_head_tree, handle, crid, head_sha
            )

            # 4) Required tests (sandboxed). No runner configured => tests skipped.
            tests = await self._run_tests(
                request, worktree.path, project_handle, coding_run_id, created
            )
            test_status = TestStatus(aggregate_test_status(tests))

            manifest = PatchBundleManifest(
                proposal_id=proposal_id,
                org_id=request.org_id,
                project_id=request.project_id,
                run_id=run_id,
                base_ref=request.base_ref,
                base_sha=base_sha,
                head_sha=head_sha,
                diff_sha256=sha256_hex(diff_bytes),
                diff_bytes=len(diff_bytes),
                files=files,
                commits=(
                    ProposedCommit(
                        sha=head_sha,
                        message=f"Keel patch proposal {proposal_id}",
                        tree_sha=tree_sha,
                    ),
                ),
                tests=tests,
                test_status=test_status,
                created_at=created,
            )
            writer = PatchBundleWriter(self.artifacts, retention_days=self.bundle_retention_days)
            stored = await asyncio.to_thread(
                writer.store,
                manifest,
                diff=diff_bytes,
                project_handle=project_handle,
                coding_run_id=coding_run_id,
                now=created,
            )

            # 5) Pin the exact proposed commit so it survives worktree disposal, for writeback.
            pin_ref = pin_ref_for(proposal_id)
            await asyncio.to_thread(
                self.storage.ingest_worktree_commit, handle, crid, head_sha, pin_ref=pin_ref
            )
            return GenerationOutcome(
                proposal_id=proposal_id,
                base_sha=base_sha,
                head_sha=head_sha,
                bundle_sha256=stored.bundle_sha256,
                diff_sha256=stored.diff_sha256,
                changed_path_digest=changed_path_digest(files),
                changed_files=len(files),
                test_status=test_status,
                manifest=manifest,
                pin_ref=pin_ref,
                usage=author_result.usage,
            )
        finally:
            await asyncio.to_thread(_safe_remove, self.storage, handle, crid)

    async def _capture_changed_files(
        self, handle: ProjectId, crid: CodingRunId, base_sha: str, head_sha: str
    ) -> tuple[ChangedFile, ...]:
        rows = await asyncio.to_thread(
            self.storage.worktree_changed_files, handle, crid, base_sha, head_sha
        )
        files: list[ChangedFile] = []
        for row in rows:
            kind = _STATUS_TO_KIND.get(str(row["status"]), ChangeKind.modified)
            path = str(row["path"])
            # Forbidden-path / secret validation is enforced again in ChangedFile.__post_init__.
            if is_forbidden_path(path):
                raise PatchPolicyViolation(f"generated change touches a forbidden path: {path}")
            if kind is not ChangeKind.deleted:
                is_binary = await asyncio.to_thread(
                    self.storage.worktree_blob_is_binary, handle, crid, base_sha, head_sha, path
                )
                if is_binary:
                    raise PatchPolicyViolation(f"generated change is binary (policy): {path}")
            files.append(
                ChangedFile(
                    path=path,
                    change_kind=kind,
                    blob_sha=str(row["blob_sha"]) or ("0" * 40),
                    size_bytes=int(row["size_bytes"]),
                    old_path=str(row.get("old_path", "")),
                )
            )
        if not files:
            raise PatchValidationError("generation produced no change (empty proposal)")
        return tuple(files)

    async def _run_tests(
        self,
        request: PatchProposalRequest,
        worktree_path: Path,
        project_handle: str,
        coding_run_id: str,
        now: datetime,
    ) -> tuple[TestResult, ...]:
        if not request.test_commands or self.test_runner is None:
            return ()
        writer = PatchBundleWriter(self.artifacts, retention_days=self.bundle_retention_days)
        results: list[TestResult] = []
        for command in request.test_commands:
            exit_code, log = await self.test_runner.run(
                worktree_path=worktree_path, command=command, coding_run_id=coding_run_id
            )
            log_sha = await asyncio.to_thread(
                writer.store_test_log,
                project_id=project_handle,
                coding_run_id=coding_run_id,
                command=command,
                log=log,
                now=now,
            )
            results.append(
                TestResult(
                    command=command,
                    exit_code=exit_code,
                    passed=exit_code == 0,
                    log_sha256=log_sha,
                )
            )
        return tuple(results)


def _safe_remove(storage: LocalCodingStorage, handle: ProjectId, crid: CodingRunId) -> None:
    try:
        storage.remove(handle, crid)
    except Exception:  # noqa: BLE001 - a missing/absent worktree is fine
        pass


def _reject_binary_diff(diff: bytes) -> None:
    if (
        b"\nBinary files " in diff
        or b"GIT binary patch" in diff
        or diff.startswith(b"Binary files ")
    ):
        raise PatchPolicyViolation("proposal contains a binary change (policy)")


__all__ = [
    "AuthorResult",
    "GenerationOutcome",
    "PatchAuthor",
    "PatchGenerationService",
    "TestRunner",
    "pin_ref_for",
]
