"""Unit tests for the WS-R "final review blocker" fixes.

Covers the pieces added/hardened in the final review pass:

* authoritative pricing + fail-closed on an unpriced model (F5);
* exact hunk-containment evidence (a broad range straddling a hunk is not confirmed) (F6);
* the coding-storage authenticated ref fetch keeping the token out of argv (F2);
* the review artifact reaper tick over an expiring/non-expiring/cross-project store (F7).
"""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from keel_core.coding.local import GitRunner, LocalArtifactStore, LocalCodingStorage
from keel_core.coding.models import (
    ArtifactRetention,
    CodingRunId,
    InvalidStorageInput,
    ProjectId,
    StorageNotFound,
)
from keel_core.review.diff import parse_unified_diff
from keel_core.review.errors import ReviewValidationError
from keel_core.review.pricing import PriceBook, parse_price_overrides
from keel_core.review.ref_materializer import _basic_auth_header


# --- F5: authoritative pricing -----------------------------------------------------------
def test_price_book_known_model_computes_cost() -> None:
    book = PriceBook.from_settings("")
    # gpt-4o-mini: 0.15/0.60 per 1M tokens.
    cost = book.cost_for("gpt-4o-mini", 1_000_000, 1_000_000)
    assert cost == pytest.approx(0.75)


def test_price_book_override_wins_and_strips_provider_prefix() -> None:
    book = PriceBook.from_settings("my-model=1.0/3.0")
    assert book.is_priced("my-model")
    # provider/ prefix is normalized away for lookup.
    assert book.cost_for("openai/my-model", 2_000_000, 1_000_000) == pytest.approx(5.0)


def test_price_book_unpriced_model_fails_closed() -> None:
    book = PriceBook.from_settings("")
    assert not book.is_priced("totally-unknown-model")
    with pytest.raises(ReviewValidationError):
        book.cost_for("totally-unknown-model", 10, 10)


def test_parse_price_overrides_skips_malformed_entries() -> None:
    parsed = parse_price_overrides("good=1/2, bad-no-rates, alsobad=onlyone, neg=-1/2")
    assert set(parsed) == {"good"}


async def test_engine_uses_authoritative_price_and_enforces_ceiling() -> None:
    from collections.abc import AsyncIterator

    from keel_core.protocols import ProviderChunk, ProviderRequest, Usage
    from keel_core.review.engine import ReviewEngine
    from keel_core.review.errors import ReviewBoundsExceeded
    from keel_core.review.models import ReviewBudget

    valid = '{"summary": "ok", "findings": [], "limitations": []}'

    class _Provider:
        def __init__(self, usage: Usage) -> None:
            self._usage = usage

        async def stream(self, _request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
            yield ProviderChunk(delta=valid, usage=self._usage)

    # gpt-4o-mini priced 0.15/0.60 per 1M tokens: 1M prompt + 1M completion => $0.75.
    book = PriceBook.from_settings("")
    engine = ReviewEngine(
        _Provider(Usage(prompt_tokens=1_000_000, completion_tokens=1_000_000, cost_usd=0.0)),
        price_book=book,
    )
    result = await engine.run(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "x"}],
        max_findings=5,
        budget=ReviewBudget(token_budget=2_000_000, cost_ceiling_usd=5.0),
    )
    # Authoritative cost is derived from tokens, NOT the provider's (zero) self-report.
    assert result.usage.cost_usd == pytest.approx(0.75)

    # The same usage under a tight ceiling fails closed.
    tight = ReviewEngine(
        _Provider(Usage(prompt_tokens=1_000_000, completion_tokens=1_000_000, cost_usd=0.0)),
        price_book=book,
    )
    with pytest.raises(ReviewBoundsExceeded):
        await tight.run(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "x"}],
            max_findings=5,
            budget=ReviewBudget(token_budget=2_000_000, cost_ceiling_usd=0.5),
        )


# --- F6: exact hunk-containment evidence -------------------------------------------------
_DIFF = """diff --git a/app.py b/app.py
--- a/app.py
+++ b/app.py
@@ -10,3 +10,4 @@ def f():
 context_a
+added_b
 context_c
 context_d
"""


def test_hunk_intervals_and_full_range_containment() -> None:
    (diff_file,) = parse_unified_diff(_DIFF)
    # new-file lines 10..13 (context 10, added 11, context 12, 13).
    assert diff_file.hunk_intervals == ((10, 13),)
    # A range fully inside the hunk is contained.
    assert diff_file.range_within_hunk(11, 12)
    # A single-line overlap that spills below the hunk is NOT contained (broad-range rejection).
    assert not diff_file.range_within_hunk(13, 40)
    # A range starting above the hunk is not contained either.
    assert not diff_file.range_within_hunk(1, 11)


# --- F2: authenticated ref fetch (token isolation) ---------------------------------------
def _init_repo(path: Path) -> None:
    def git(*args: str) -> None:
        subprocess.run(
            ["git", *args],
            cwd=path,
            check=True,
            capture_output=True,
            text=True,
            env={
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_AUTHOR_NAME": "t",
                "GIT_AUTHOR_EMAIL": "t@example.com",
                "GIT_COMMITTER_NAME": "t",
                "GIT_COMMITTER_EMAIL": "t@example.com",
                "PATH": __import__("os").environ.get("PATH", ""),
            },
        )

    git("init", "-q", "-b", "main")
    (path / "a.txt").write_text("hello\n", encoding="utf-8")
    git("add", "a.txt")
    git("commit", "-q", "-m", "c1")


def _head_sha(path: Path) -> str:
    out = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


def test_fetch_commits_pulls_missing_object_without_token_in_argv(tmp_path: Path) -> None:
    if __import__("shutil").which("git") is None:
        pytest.skip("git not available")
    # A source repo (acts as the remote) with a commit the authoritative repo does not yet have.
    source = tmp_path / "source"
    source.mkdir()
    _init_repo(source)
    subprocess.run(
        ["git", "-C", str(source), "config", "uploadpack.allowAnySHA1InWant", "true"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(source), "config", "uploadpack.allowReachableSHA1InWant", "true"],
        check=True,
        capture_output=True,
    )
    target_sha = _head_sha(source)

    # Record every git argv so we can prove the auth header/token never lands in a command arg.
    recorded_args: list[list[str]] = []
    real_run = subprocess.run

    def _spy_run(command, *args, **kwargs):  # type: ignore[no-untyped-def]
        recorded_args.append(list(command))
        return real_run(command, *args, **kwargs)

    storage = LocalCodingStorage(tmp_path / "storage", allow_local_remotes=True)
    project = ProjectId("p1")
    storage.create_project(project)

    import keel_core.coding.local as local_mod

    original = local_mod.subprocess.run
    local_mod.subprocess.run = _spy_run  # type: ignore[assignment]
    try:
        storage.fetch_commits(
            project,
            source,
            [target_sha],
            auth_header="Authorization: Basic c2VjcmV0LXRva2Vu",
        )
    finally:
        local_mod.subprocess.run = original  # type: ignore[assignment]

    # The object is now present in the authoritative repo.
    repo_dir = storage.get_project(project).repository_path
    probe = GitRunner().run(
        ["--git-dir", str(repo_dir), "cat-file", "-e", f"{target_sha}^{{commit}}"],
        check=False,
    )
    assert probe.returncode == 0
    # The credential value never appears in any git command argument (it is env-only).
    for argv in recorded_args:
        assert not any("c2VjcmV0LXRva2Vu" in str(part) for part in argv)


def test_auth_header_env_is_git_config_env_only() -> None:
    env = LocalCodingStorage._auth_header_env("authorization: Basic abc")
    assert env["GIT_CONFIG_KEY_0"] == "http.extraHeader"
    assert env["GIT_CONFIG_VALUE_0"] == "Authorization: Basic abc"
    with pytest.raises(InvalidStorageInput):
        LocalCodingStorage._auth_header_env("Basic abc")
    with pytest.raises(InvalidStorageInput):
        LocalCodingStorage._auth_header_env("bad\nheader")


def test_github_basic_auth_includes_header_name() -> None:
    assert _basic_auth_header("token").startswith("Authorization: Basic ")


# --- F7: artifact reaper (expiry / non-expiry / cross-project) ---------------------------
def _put_retained(
    store: LocalArtifactStore, project: str, run: str, body: bytes, *, until: datetime
):
    return store.put(
        ProjectId(project),
        CodingRunId(run),
        body,
        name="report.json",
        retention=ArtifactRetention.retained,
        retained_until=until,
    )


def test_reap_expires_only_past_retained_until_across_projects(tmp_path: Path) -> None:
    storage = LocalCodingStorage(tmp_path / "s")
    store = LocalArtifactStore(storage)
    storage.create_project(ProjectId("proj-a"))
    storage.create_project(ProjectId("proj-b"))
    now = datetime.now(UTC)
    expired = _put_retained(store, "proj-a", "run1", b"a", until=now - timedelta(days=1))
    fresh = _put_retained(store, "proj-b", "run2", b"bb", until=now + timedelta(days=30))

    result = store.reap(older_than=datetime.now(UTC) + timedelta(seconds=1))
    assert result.removed == 1

    # The expired artifact (cross-project) is gone; the still-retained one survives.
    with pytest.raises(StorageNotFound):
        store.read(ProjectId("proj-a"), CodingRunId("run1"), expired.content_hash)
    assert store.read(ProjectId("proj-b"), CodingRunId("run2"), fresh.content_hash) == b"bb"
