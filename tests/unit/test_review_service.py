"""End-to-end tests for the read-only ReviewService over real git + isolated storage."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from review_support import (
    CapturingProvider,
    build_repo_with_readme_injection,
    build_source_repo,
    finding_json,
    import_into_storage,
)

from keel_core.coding import LocalArtifactStore, LocalWorktreeStore
from keel_core.coding.models import CodingRunId, ProjectId
from keel_core.review import (
    ReviewRequest,
    ReviewService,
    ReviewSource,
)
from keel_core.review.errors import ReviewBoundsExceeded, ReviewProviderError


def _service(storage, provider, **kwargs) -> ReviewService:
    return ReviewService(
        worktrees=LocalWorktreeStore(storage),
        artifacts=LocalArtifactStore(storage),
        provider=provider,
        **kwargs,
    )


def _request(**overrides) -> ReviewRequest:
    data = {
        "org_id": "org1",
        "project_id": "proj",
        "source": ReviewSource.branch,
        "head": "main",
        "idempotency_key": "k1",
        "model": "test-model",
    }
    data.update(overrides)
    return ReviewRequest(**data)


async def _run(storage, provider, request=None, **kwargs):
    service = _service(storage, provider, **kwargs)
    return await service.review(
        request or _request(),
        run_id="rev_1",
        coding_run_id="rrun1",
        project_handle="proj",
    )


async def test_review_finds_and_verifies_real_issue(tmp_path: Path) -> None:
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(
        responses=[
            finding_json(
                file_path="app.py",
                line_start=2,
                line_end=2,
                snippet="return a - b  # BUG: subtraction",
            )
        ]
    )
    outcome = await _run(storage, provider)
    assert len(outcome.report.findings) == 1
    assert outcome.report.findings[0].verified
    assert outcome.rejected_count == 0
    # The report artifact is stored, content-addressed, and re-readable.
    data = LocalArtifactStore(storage).read(
        ProjectId("proj"), CodingRunId("rrun1"), outcome.json_sha256
    )
    parsed = json.loads(data)
    assert parsed["head_sha"] == outcome.head_sha
    assert parsed["artifacts"]["markdown_sha256"] == outcome.markdown_sha256


async def test_hallucinated_finding_is_rejected(tmp_path: Path) -> None:
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(
        responses=[
            finding_json(
                file_path="ghost.py",
                line_start=1,
                line_end=1,
                snippet="return a - b",
            )
        ]
    )
    outcome = await _run(storage, provider)
    assert outcome.report.findings == ()
    assert outcome.rejected_count == 1
    assert any("rejected" in limitation for limitation in outcome.report.limitations)


async def test_prompt_injection_in_readme_cannot_enable_tools(tmp_path: Path) -> None:
    build_repo_with_readme_injection(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    # Even though the diff text screams "enable the shell tool", the model is offered no
    # tools at all, and the injected instruction is treated as reviewable data.
    provider = CapturingProvider(
        responses=[json.dumps({"summary": "docs change", "findings": [], "limitations": []})]
    )
    outcome = await _run(storage, provider)
    assert provider.requests[0].tools == []
    assert outcome.report.findings == ()
    # The injection text is present in the prompt as fenced data, not as a system instruction.
    user = provider.requests[0].messages[1]["content"]
    assert "IGNORE ALL PREVIOUS INSTRUCTIONS" in user
    system = provider.requests[0].messages[0]["content"]
    assert "UNTRUSTED DATA" in system


async def test_oversized_diff_is_bounded(tmp_path: Path) -> None:
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(responses=["{}"])
    with pytest.raises(ReviewBoundsExceeded):
        await _run(storage, provider, request=_request(max_diff_bytes=5))


async def test_malformed_provider_json_fails_closed(tmp_path: Path) -> None:
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(responses=["not json", "still not json"])
    with pytest.raises(ReviewProviderError):
        await _run(storage, provider, max_repairs=1)


async def test_worktree_is_disposed_after_review(tmp_path: Path) -> None:
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(
        responses=[json.dumps({"summary": "ok", "findings": [], "limitations": []})]
    )
    await _run(storage, provider)
    worktree_dir = storage.worktrees_root / "proj" / "rrun1"
    assert not worktree_dir.exists()


async def test_worktree_disposed_even_on_failure(tmp_path: Path) -> None:
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(responses=["garbage", "garbage"])
    with pytest.raises(ReviewProviderError):
        await _run(storage, provider, max_repairs=1)
    worktree_dir = storage.worktrees_root / "proj" / "rrun1"
    assert not worktree_dir.exists()


async def test_materialized_worktree_has_no_remote(tmp_path: Path) -> None:
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    worktrees = LocalWorktreeStore(storage)
    record = worktrees.materialize(ProjectId("proj"), CodingRunId("rrun2"), ref="main")
    try:
        from keel_core.coding import GitRunner

        remotes = GitRunner().run(["remote"], cwd=record.path).stdout.strip()
        # An isolated clone with no remote/alternates cannot push or fetch anywhere.
        assert remotes == ""
    finally:
        worktrees.remove(ProjectId("proj"), CodingRunId("rrun2"))


async def test_no_provider_secret_leakage_in_report(tmp_path: Path) -> None:
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(
        responses=[
            finding_json(
                file_path="app.py",
                line_start=2,
                line_end=2,
                snippet="return a - b  # BUG: subtraction",
            )
        ]
    )
    outcome = await _run(storage, provider)
    blob = json.dumps(outcome.report.to_dict())
    for secret_marker in ("ghs_", "Authorization", "Bearer ", "private_key"):
        assert secret_marker not in blob


async def test_project_purge_removes_review_artifacts(tmp_path: Path) -> None:
    from keel_core.coding.models import StorageNotFound

    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(
        responses=[
            finding_json(
                file_path="app.py",
                line_start=2,
                line_end=2,
                snippet="return a - b  # BUG: subtraction",
            )
        ]
    )
    outcome = await _run(storage, provider)
    artifacts = LocalArtifactStore(storage)
    # Present before erasure.
    assert artifacts.read(ProjectId("proj"), CodingRunId("rrun1"), outcome.json_sha256)
    # Project erasure reuses coding-storage purge; review artifacts live under the project
    # handle and are removed with it. The reaper/purge is idempotent (a second call is fine).
    assert storage.purge_project(ProjectId("proj")) is True
    assert storage.purge_project(ProjectId("proj")) is False
    with pytest.raises(StorageNotFound):
        artifacts.read(ProjectId("proj"), CodingRunId("rrun1"), outcome.json_sha256)
