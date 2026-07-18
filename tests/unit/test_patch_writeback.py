"""Unit tests for the trusted patch-writeback control plane (WS-PP), fully mocked GitHub/git.

Verifies the security-critical writeback invariants without a network:

* base-drift detection marks the proposal stale and never pushes;
* the exact approved commit is pushed to the dedicated run branch (never the default), no force;
* the Draft PR is created with ``draft=True`` against the approved base branch;
* crash recovery reconciles an already-pushed branch and an already-open PR without duplicates.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from keel_core.patch.errors import PatchStaleError
from keel_core.patch.ledger import InMemoryPatchWritebackLedger
from keel_core.patch.models import (
    ChangedFile,
    ChangeKind,
    PatchBundleManifest,
    PatchProposal,
    PatchStatus,
    ProposedCommit,
    TestStatus,
)
from keel_core.patch.writeback import PatchWritebackService, WritebackTarget


class _FakeGitHubClient:
    def __init__(
        self,
        *,
        base_sha: str,
        existing_branch_sha: str | None = None,
        existing_prs: list[dict[str, Any]] | None = None,
    ) -> None:
        self._base_sha = base_sha
        self._branch_sha = existing_branch_sha
        self._prs = existing_prs or []
        self.created_prs: list[dict[str, Any]] = []

    async def get_ref(self, *, token: str, full_name: str, ref: str) -> dict[str, Any] | None:
        if ref.endswith("/main"):
            return {"object": {"sha": self._base_sha}}
        if self._branch_sha is not None:
            return {"object": {"sha": self._branch_sha}}
        return None

    async def list_pull_requests(
        self, *, token: str, full_name: str, head: str, state: str = "all"
    ):  # type: ignore[no-untyped-def]
        return list(self._prs)

    async def create_pull_request(self, *, token, full_name, title, head, base, body, draft=True):  # type: ignore[no-untyped-def]
        assert draft is True and base == "main"
        pr = {"number": 7, "html_url": "https://github.test/pr/7", "node_id": "N", "draft": True}
        self.created_prs.append({"head": head, "base": base})
        return pr


class _FakeTokens:
    async def get_token(self, installation_id: int):  # type: ignore[no-untyped-def]
        class _T:
            token = "ghs_faketoken"

        return _T()


class _FakeIntegration:
    def __init__(self, client: _FakeGitHubClient) -> None:
        self.client = client
        self.tokens = _FakeTokens()

    def safe_clone_url(self, clone_url: str, full_name: str) -> str:
        return clone_url


class _FakeStorage:
    def __init__(self) -> None:
        self.pushes: list[dict[str, Any]] = []

    def resolve_commit(self, project_id, commit_sha):  # type: ignore[no-untyped-def]
        return commit_sha

    def commit_tree(self, project_id, commit_sha):  # type: ignore[no-untyped-def]
        return "d" * 40

    def push_commit(self, project_id, remote, commit_sha, *, target_branch, auth_header=None):  # type: ignore[no-untyped-def]
        assert target_branch.startswith("keel/patch/") and target_branch != "main"
        assert auth_header and auth_header.startswith("Authorization: Basic ")
        assert "ghs_faketoken" not in target_branch
        self.pushes.append({"branch": target_branch, "sha": commit_sha, "auth": auth_header})


def _proposal(status: PatchStatus = PatchStatus.writing) -> PatchProposal:
    now = datetime.now(UTC)
    return PatchProposal(
        id="pp_wb",
        org_id="o",
        project_id="p",
        run_id="pp_wb",
        run_attempt=1,
        agent_id="patch",
        actor="u",
        base_ref="main",
        base_sha="b" * 40,
        head_sha="c" * 40,
        bundle_sha256="e" * 64,
        diff_sha256="f" * 64,
        changed_path_digest="9" * 64,
        changed_files=1,
        test_status=TestStatus.passed,
        status=status,
        version=5,
        idempotency_key="k",
        fingerprint="k",
        expires_at=now + timedelta(hours=1),
        created_at=now,
        updated_at=now,
        remote_branch="keel/patch/pp_wb",
    )


def _manifest() -> PatchBundleManifest:
    files = (
        ChangedFile(
            path="app.py", change_kind=ChangeKind.modified, blob_sha="a" * 40, size_bytes=5
        ),
    )
    return PatchBundleManifest(
        proposal_id="pp_wb",
        org_id="o",
        project_id="p",
        run_id="pp_wb",
        base_ref="main",
        base_sha="b" * 40,
        head_sha="c" * 40,
        diff_sha256="f" * 64,
        diff_bytes=10,
        files=files,
        commits=(ProposedCommit(sha="c" * 40, message="m", tree_sha="d" * 40),),
        tests=(),
        test_status=TestStatus.passed,
        created_at=datetime.now(UTC),
    )


def _target() -> WritebackTarget:
    return WritebackTarget(
        full_name="o/r",
        installation_id=1,
        clone_url="https://github.test/o/r.git",
        default_branch="main",
    )


@pytest.mark.asyncio
async def test_writeback_pushes_and_opens_draft_pr() -> None:
    client = _FakeGitHubClient(base_sha="b" * 40)
    storage = _FakeStorage()
    ledger = InMemoryPatchWritebackLedger()
    svc = PatchWritebackService(github=_FakeIntegration(client), storage=storage, ledger=ledger)  # type: ignore[arg-type]
    result = await svc.write(
        _proposal(), project_handle="proj", target=_target(), manifest=_manifest()
    )
    assert result.pr_number == 7 and result.remote_branch == "keel/patch/pp_wb"
    assert len(storage.pushes) == 1
    assert storage.pushes[0]["branch"] == "keel/patch/pp_wb"
    assert storage.pushes[0]["sha"] == "c" * 40
    assert storage.pushes[0]["auth"].startswith("Authorization: Basic ")
    assert len(client.created_prs) == 1
    steps = {(e.step, e.status) for e in ledger.entries}
    assert ("verify_base", "succeeded") in steps
    assert ("push_branch", "succeeded") in steps
    assert ("create_pr", "succeeded") in steps


@pytest.mark.asyncio
async def test_writeback_stale_base_never_pushes() -> None:
    client = _FakeGitHubClient(base_sha="z" * 40)  # remote base moved
    storage = _FakeStorage()
    ledger = InMemoryPatchWritebackLedger()
    svc = PatchWritebackService(github=_FakeIntegration(client), storage=storage, ledger=ledger)  # type: ignore[arg-type]
    with pytest.raises(PatchStaleError):
        await svc.write(_proposal(), project_handle="proj", target=_target(), manifest=_manifest())
    assert storage.pushes == []
    assert client.created_prs == []
    assert any(e.step == "verify_base" and e.status == "stale" for e in ledger.entries)


@pytest.mark.asyncio
async def test_writeback_reconciles_existing_branch_and_pr() -> None:
    # Crash recovery: the branch already points at head and a PR already exists.
    client = _FakeGitHubClient(
        base_sha="b" * 40,
        existing_branch_sha="c" * 40,
        existing_prs=[{"number": 7, "html_url": "https://github.test/pr/7", "node_id": "N"}],
    )
    storage = _FakeStorage()
    svc = PatchWritebackService(github=_FakeIntegration(client), storage=storage)  # type: ignore[arg-type]
    result = await svc.write(
        _proposal(), project_handle="proj", target=_target(), manifest=_manifest()
    )
    assert result.reused_branch and result.reused_pr and result.pr_number == 7
    assert storage.pushes == []  # never re-pushed
    assert client.created_prs == []  # never duplicated the PR
