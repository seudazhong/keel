"""Real-git verification of controlled patch generation + control-plane push plumbing (WS-PP).

Exercises the security-critical git core with a real system Git and no provider/network:

* an isolated, disposable, writable worktree is materialized at the exact base commit;
* a fake author edits it; the change is captured into an immutable, content-addressed bundle
  (diff + changed paths/blob shas + proposed commit/tree) and validated fail-closed;
* the exact proposed commit is pinned in the authoritative repo and survives worktree disposal;
* the pinned commit is pushed to a *dedicated* branch on a local bare remote with an explicit
  refspec (no force, no tags) and the default branch is never touched.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from keel_core.coding import GitRunner, LocalArtifactStore, LocalCodingStorage
from keel_core.coding.models import ProjectId
from keel_core.patch.bundle import PatchBundleReader
from keel_core.patch.errors import PatchPolicyViolation, PatchValidationError
from keel_core.patch.generation import AuthorResult, PatchGenerationService
from keel_core.patch.models import PatchProposalRequest, TestStatus
from keel_core.protocols import Usage


def _git(cwd: Path, *args: str) -> str:
    return (
        GitRunner()
        .run(["-c", "user.name=Test", "-c", "user.email=test@example.test", *args], cwd=cwd)
        .stdout.strip()
    )


def _seed_repo(root: Path) -> Path:
    src = root / "source"
    src.mkdir(parents=True)
    _git(src, "init", "--initial-branch=main", ".")
    (src / "app.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "base")
    return src


def _storage(root: Path) -> LocalCodingStorage:
    storage = LocalCodingStorage(root / "coding", allow_local_remotes=True)
    storage.import_project("proj", _seed_repo(root), default_branch="main")
    return storage


def _request(**overrides: object) -> PatchProposalRequest:
    params: dict[str, object] = {
        "org_id": "o",
        "project_id": "p",
        "actor": "u",
        "task": "Add a subtract helper",
        "base_ref": "main",
        "model": "gpt-test",
        "idempotency_key": "k1",
    }
    params.update(overrides)
    return PatchProposalRequest(**params)  # type: ignore[arg-type]


class _WritingAuthor:
    """A fake author that writes a new file + edits an existing one (no provider/network)."""

    def __init__(self, writes: dict[str, str]) -> None:
        self._writes = writes

    async def author(self, *, worktree_path: Path, request, coding_run_id, interrupt=None):  # type: ignore[no-untyped-def]
        for rel, content in self._writes.items():
            target = worktree_path / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        return AuthorResult(usage=Usage(prompt_tokens=10, completion_tokens=5, cost_usd=0.01))


@pytest.mark.asyncio
async def test_generation_captures_bundle_and_pins_commit(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    artifacts = LocalArtifactStore(storage)
    author = _WritingAuthor(
        {
            "app.py": "def add(a, b):\n    return a + b\n\n\ndef sub(a, b):\n    return a - b\n",
            "lib/util.py": "VALUE = 1\n",
        }
    )
    service = PatchGenerationService(storage=storage, artifacts=artifacts, author=author)

    outcome = await service.generate(
        _request(),
        proposal_id="pp_gen1",
        run_id="run1",
        coding_run_id="cr1",
        project_handle="proj",
    )

    assert outcome.changed_files == 2
    assert outcome.test_status is TestStatus.skipped
    assert outcome.bundle_sha256 and outcome.diff_sha256 and outcome.head_sha
    assert outcome.base_sha != outcome.head_sha

    # The disposable worktree was reclaimed but the exact proposed commit was pinned + survives.
    assert storage.resolve_commit(ProjectId("proj"), outcome.head_sha) == outcome.head_sha
    listed = (
        GitRunner()
        .run(
            [
                "--git-dir",
                str(storage._require_repo(ProjectId("proj"))),
                "rev-parse",
                outcome.pin_ref,
            ],
            cwd=tmp_path,
        )
        .stdout.strip()
    )
    assert listed == outcome.head_sha

    # The immutable bundle round-trips and its content hashes verify.
    reader = PatchBundleReader(artifacts)
    manifest = reader.read_manifest(
        project_id="proj", coding_run_id="cr1", bundle_sha256=outcome.bundle_sha256
    )
    assert manifest.head_sha == outcome.head_sha
    assert manifest.changed_path_digest == outcome.changed_path_digest
    assert all(len(changed.blob_sha) == 40 for changed in manifest.files)
    diff = reader.read_diff(project_id="proj", coding_run_id="cr1", diff_sha256=outcome.diff_sha256)
    assert b"def sub" in diff


@pytest.mark.asyncio
async def test_generation_rejects_empty_change(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    artifacts = LocalArtifactStore(storage)
    service = PatchGenerationService(
        storage=storage, artifacts=artifacts, author=_WritingAuthor({})
    )
    with pytest.raises(PatchValidationError):
        await service.generate(
            _request(),
            proposal_id="pp_empty",
            run_id="run1",
            coding_run_id="cr_empty",
            project_handle="proj",
        )


@pytest.mark.asyncio
async def test_generation_rejects_forbidden_path(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    artifacts = LocalArtifactStore(storage)
    author = _WritingAuthor({".env": "SECRET=1\n"})
    service = PatchGenerationService(storage=storage, artifacts=artifacts, author=author)
    with pytest.raises(PatchPolicyViolation):
        await service.generate(
            _request(),
            proposal_id="pp_forbidden",
            run_id="run1",
            coding_run_id="cr_forbidden",
            project_handle="proj",
        )


@pytest.mark.asyncio
async def test_push_commit_to_dedicated_branch_never_touches_default(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    artifacts = LocalArtifactStore(storage)
    author = _WritingAuthor({"feature.py": "X = 1\n"})
    service = PatchGenerationService(storage=storage, artifacts=artifacts, author=author)
    outcome = await service.generate(
        _request(),
        proposal_id="pp_push",
        run_id="run1",
        coding_run_id="cr_push",
        project_handle="proj",
    )

    # A bare local "remote" seeded with the base commit on main (the default branch).
    remote = tmp_path / "remote.git"
    _git(tmp_path, "clone", "--bare", str(storage._require_repo(ProjectId("proj"))), str(remote))
    default_before = _git(remote, "rev-parse", "main")

    storage.push_commit(
        ProjectId("proj"),
        remote,
        outcome.head_sha,
        target_branch="keel/patch/pp_push",
    )

    # The dedicated branch now points at the exact proposed commit; the default branch is unmoved.
    assert _git(remote, "rev-parse", "keel/patch/pp_push") == outcome.head_sha
    assert _git(remote, "rev-parse", "main") == default_before
