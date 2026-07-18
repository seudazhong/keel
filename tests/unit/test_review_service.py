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


# --- ref resolution (WS-R finding 2) ---------------------------------------------


def _build_feature_branch_repo(tmp_path: Path):
    """A repo whose ``feature`` branch diverged from ``main`` (which then advanced)."""
    from review_support import git as _git

    src = tmp_path / "source"
    src.mkdir(parents=True)
    _git(src, "init", "--initial-branch=main", ".")
    (src / "app.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "base")
    fork_sha = _git(src, "rev-parse", "HEAD")
    _git(src, "checkout", "-b", "feature")
    (src / "app.py").write_text("def add(a, b):\n    return a - b\n", encoding="utf-8")
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "c1")
    (src / "mod.py").write_text("VALUE = 1\n", encoding="utf-8")
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "c2")
    head_sha = _git(src, "rev-parse", "HEAD")
    # main advances after the divergence — the merge-base, not the main tip, must be the base.
    _git(src, "checkout", "main")
    (src / "unrelated.py").write_text("Z = 9\n", encoding="utf-8")
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "main2")
    return src, fork_sha, head_sha


async def test_numeric_branch_is_not_a_pr(tmp_path: Path) -> None:
    """A branch literally named ``123`` is reviewed as a branch, never as PR #123."""
    from review_support import git as _git

    src = tmp_path / "source"
    src.mkdir(parents=True)
    _git(src, "init", "--initial-branch=main", ".")
    (src / "app.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "base")
    _git(src, "checkout", "-b", "123")
    (src / "app.py").write_text("def add(a, b):\n    return a - b\n", encoding="utf-8")
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "change")
    storage = import_into_storage(tmp_path, src)
    provider = CapturingProvider(
        responses=[
            finding_json(file_path="app.py", line_start=2, line_end=2, snippet="return a - b")
        ]
    )
    outcome = await _run(
        storage, provider, request=_request(source=ReviewSource.branch, head="123")
    )
    assert len(outcome.report.findings) == 1
    assert outcome.report.findings[0].verified


async def test_branch_omitted_base_uses_merge_base(tmp_path: Path) -> None:
    from keel_core.review.refs import build_materialization_plan

    src, fork_sha, head_sha = _build_feature_branch_repo(tmp_path)
    storage = import_into_storage(tmp_path, src)
    provider = CapturingProvider(
        responses=[
            finding_json(file_path="app.py", line_start=2, line_end=2, snippet="return a - b")
        ]
    )
    service = _service(storage, provider)
    request = _request(source=ReviewSource.branch, head="feature", base=None)
    plan = await build_materialization_plan(request, default_branch="main", pr_resolver=None)
    outcome = await service.review(
        request, run_id="rev_1", coding_run_id="rrun1", project_handle="proj", plan=plan
    )
    # Base is the divergence point (merge-base), not the advanced main tip.
    assert outcome.base_sha == fork_sha
    assert outcome.head_sha == head_sha
    # The branch's own changes (app.py + mod.py) are reviewed; main's later change is not.
    assert outcome.report.files_reviewed == 2
    paths = {f.file_path for f in outcome.report.findings}
    assert "app.py" in paths


async def test_pull_request_without_resolver_fails(tmp_path: Path) -> None:
    from keel_core.review.errors import ReviewValidationError

    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source")
    provider = CapturingProvider(responses=["{}"])
    service = _service(storage, provider)
    # No control-plane plan: a PR request must fail closed (PR number never used as a ref).
    with pytest.raises(ReviewValidationError):
        await service.review(
            _request(source=ReviewSource.pull_request, head="7"),
            run_id="rev_1",
            coding_run_id="rrun1",
            project_handle="proj",
        )


async def test_pull_request_reviews_resolved_shas(tmp_path: Path) -> None:
    from keel_core.review.refs import MaterializationPlan

    repo = build_source_repo(tmp_path)  # base_sha, head_sha on main
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
    service = _service(storage, provider)
    # Simulate a control-plane-resolved PR: exact base/head SHAs, materialize at the head sha.
    plan = MaterializationPlan(
        materialize_ref=repo.head_sha,
        base_ref=repo.base_sha,
        default_branch="main",
        derive_base_from_default=False,
        base_sha_hint=repo.base_sha,
        head_sha_hint=repo.head_sha,
    )
    outcome = await service.review(
        _request(source=ReviewSource.pull_request, head="7"),
        run_id="rev_1",
        coding_run_id="rrun1",
        project_handle="proj",
        plan=plan,
    )
    assert outcome.base_sha == repo.base_sha
    assert outcome.head_sha == repo.head_sha
    assert len(outcome.report.findings) == 1
