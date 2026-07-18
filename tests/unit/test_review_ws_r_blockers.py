"""Unit tests for the WS-R read-only review reliability blockers (1..10).

Each test class targets one blocker from the review-reliability pass:

1.  shared-storage probe (unique filename, exclusive create, own cleanup, concurrency-safe)
2.  GitHub API host validation (HTTPS + allowlist + private-IP + credential defenses)
3.  transient GitHub errors -> retryable typed review errors (permanent stays permanent)
4.  durable job-lease heartbeat aborts/fences the review on loss/cancellation
5.  retry-exhaustion / cancellation terminalizes the associated run exactly once
6.  stranded-admission reconciler re-creates a lost dispatch intent idempotently
7.  partial usage/cost captured on a transient failure + remaining-budget carryover
8.  evidence snippets require a contiguous, ordered multi-line match (no line stitching)
9.  completed list projections load real findings and report a corrupt artifact honestly
10. review reaper reclaims stale worktrees under fencing while keeping active ones
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from review_support import CapturingProvider, build_source_repo, finding_json, import_into_storage

from keel_core.coding import LocalArtifactStore, LocalCodingStorage, LocalWorktreeStore
from keel_core.coding.models import CodingRunId, ProjectId, ReapResult
from keel_core.coding.storage_root import SharedStorageUnavailable, verify_shared_storage
from keel_core.identity.service import IdentityService
from keel_core.identity.store import InMemoryIdentityStore
from keel_core.jobs import JobCancellationRequested
from keel_core.projects import (
    InMemoryProjectAuditSink,
    InMemoryProjectStorage,
    InMemoryProjectStore,
    ProjectService,
)
from keel_core.projects.github.urls import UntrustedUrlError, normalize_https_url
from keel_core.protocols import ProviderChunk, ProviderRequest, Usage
from keel_core.review import ReviewCoordinator, ReviewRequest, ReviewService, ReviewSource
from keel_core.review.errors import (
    ReviewLeaseLost,
    ReviewProviderUnavailable,
    ReviewValidationError,
)
from keel_core.review.evidence import _contiguous_ordered_match, _snippet_complete_at
from keel_core.runs import InMemoryRunStore, RunStatus


# =============================================================================================
# Blocker 1 — shared-storage probe: unique, exclusive, own-cleanup, concurrency-safe
# =============================================================================================
class TestStorageProbe:
    def test_probe_leaves_no_files_behind(self, tmp_path: Path) -> None:
        root = tmp_path / "shared"
        verify_shared_storage(root)
        # The uniquely-named probe is always cleaned up by its own invocation.
        assert list(root.glob(".keel-storage-probe*")) == []

    def test_concurrent_probes_never_race(self, tmp_path: Path) -> None:
        root = tmp_path / "shared"
        root.mkdir()
        errors: list[BaseException] = []

        def probe() -> None:
            try:
                for _ in range(20):
                    verify_shared_storage(root)
            except BaseException as exc:  # noqa: BLE001 — capture any race failure
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(probe) for _ in range(8)]
            for future in futures:
                future.result()
        # No probe raised (no cross-process read-back/cleanup collision) and none linger.
        assert errors == []
        assert list(root.glob(".keel-storage-probe*")) == []

    def test_unwritable_root_fails_closed(self, tmp_path: Path) -> None:
        target = tmp_path / "file-not-dir"
        target.write_text("x", encoding="utf-8")
        with pytest.raises(SharedStorageUnavailable):
            verify_shared_storage(target)


# =============================================================================================
# Blocker 2 — GitHub API host validation before any authenticated client
# =============================================================================================
class TestGithubHostValidation:
    _HOSTS = frozenset({"api.github.com"})

    @pytest.mark.parametrize(
        "url",
        [
            "http://api.github.com",  # plain HTTP: token could be sniffed
            "https://169.254.169.254/",  # link-local metadata endpoint
            "https://127.0.0.1/",  # loopback
            "https://10.0.0.5/",  # private
            "https://evil.example.com/",  # off-allowlist
            "https://user:pw@api.github.com/",  # embedded credentials
        ],
    )
    def test_malicious_bases_rejected(self, url: str) -> None:
        with pytest.raises(UntrustedUrlError):
            normalize_https_url(url, allowed_hosts=self._HOSTS)

    def test_valid_base_accepted(self) -> None:
        assert (
            normalize_https_url("https://api.github.com", allowed_hosts=self._HOSTS)
            == "https://api.github.com/"
        )

    def test_factory_disables_on_bad_base(self) -> None:
        from keel_core.config import Settings
        from keel_core.projects.github_factory import build_github_integration

        common = {
            "github_app_id": 1234,
            "github_private_key_ref": "env:GH_KEY_UNUSED",
            "github_allowed_hosts": "api.github.com",
        }
        # A plain-HTTP / off-allowlist base disables the integration (fail closed) so a JIT
        # installation token is never sent to an arbitrary endpoint.
        bad = Settings(github_api_base_url="http://api.github.com", **common)
        assert build_github_integration(bad) is None
        internal = Settings(github_api_base_url="https://169.254.169.254", **common)
        assert build_github_integration(internal) is None
        # A valid HTTPS allow-listed base builds the integration (token loader stays lazy).
        good = Settings(github_api_base_url="https://api.github.com", **common)
        assert build_github_integration(good) is not None


# =============================================================================================
# Blocker 3 — transient GitHub errors become retryable typed review errors
# =============================================================================================
@dataclass
class _FakeRepo:
    installation_id: int = 7
    full_name: str = "acme/widget"
    clone_url: str = "https://github.com/acme/widget.git"


@dataclass
class _FakeProjectsForResolver:
    repo: _FakeRepo | None = field(default_factory=_FakeRepo)

    async def get_project_repository(self, org_id: str, project_id: str) -> _FakeRepo | None:
        return self.repo


@dataclass
class _FakeGithub:
    error: Exception | None = None
    payload: dict | None = None

    async def resolve_pull_request(self, installation_id, full_name, number):  # type: ignore[no-untyped-def]
        if self.error is not None:
            raise self.error
        return self.payload


class TestTransientGithubErrors:
    async def _resolve(self, error: Exception):  # type: ignore[no-untyped-def]
        from keel_core.review.github_refs import GitHubPullRequestResolver

        resolver = GitHubPullRequestResolver(
            projects=_FakeProjectsForResolver(),  # type: ignore[arg-type]
            github=_FakeGithub(error=error),  # type: ignore[arg-type]
        )
        return await resolver.resolve(org_id="o", project_id="p", agent_id=None, pr_number=5)

    async def test_rate_limit_is_retryable(self) -> None:
        from keel_core.projects.github.client import GitHubRateLimitError

        exc = GitHubRateLimitError("rate limited", status_code=429, retry_after=42.0)
        with pytest.raises(ReviewProviderUnavailable) as info:
            await self._resolve(exc)
        assert info.value.retry_after == 42.0

    async def test_server_error_is_retryable(self) -> None:
        from keel_core.projects.github.client import GitHubError

        with pytest.raises(ReviewProviderUnavailable):
            await self._resolve(GitHubError("boom", status_code=503))

    async def test_timeout_is_retryable(self) -> None:
        with pytest.raises(ReviewProviderUnavailable):
            await self._resolve(TimeoutError("upstream timed out"))

    async def test_not_found_is_permanent(self) -> None:
        from keel_core.projects.github.client import GitHubNotFoundError

        with pytest.raises(ReviewValidationError):
            await self._resolve(GitHubNotFoundError("missing", status_code=404))

    async def test_auth_error_is_permanent(self) -> None:
        from keel_core.projects.github.client import GitHubAuthError

        with pytest.raises(ReviewValidationError):
            await self._resolve(GitHubAuthError("nope", status_code=403))


# =============================================================================================
# Shared bootstrap for coordinator-level blockers (4, 5, 6, 9)
# =============================================================================================
@dataclass
class _Env:
    coordinator: ReviewCoordinator
    runs: InMemoryRunStore
    storage: LocalCodingStorage
    artifacts: LocalArtifactStore
    svc: ProjectService
    events: object
    org: str
    actor: str
    project_id: str
    scope_id: str = "scope-1"


async def _bootstrap(tmp_path: Path, provider, *, lease_seconds: int = 900) -> _Env:  # type: ignore[no-untyped-def]
    from keel_core.state import InMemoryEventStore

    identity = InMemoryIdentityStore()
    isvc = IdentityService(identity)
    admin = await isvc.store.create_user(display_name="Admin", email="admin@x.io")
    ctx = await isvc.create_org(admin.id, slug="acme-co", display_name="Acme")
    store = InMemoryProjectStore()
    svc = ProjectService(
        store, identity, storage=InMemoryProjectStorage(), audit=InMemoryProjectAuditSink()
    )
    project = await svc.create_project(ctx.org_id, admin.id, slug="proj-x", display_name="P")
    handle = project.active_git_handle
    assert handle is not None
    build_source_repo(tmp_path)
    storage = import_into_storage(tmp_path, tmp_path / "source", handle=handle)
    artifacts = LocalArtifactStore(storage)
    review_service = ReviewService(
        worktrees=LocalWorktreeStore(storage), artifacts=artifacts, provider=provider
    )
    events = InMemoryEventStore()
    runs = InMemoryRunStore()
    coordinator = ReviewCoordinator(
        projects=svc,
        runs=runs,
        review_service=review_service,
        artifacts=artifacts,
        scope_id="scope-1",
        events=events,
        lease_seconds=lease_seconds,
    )
    return _Env(
        coordinator=coordinator,
        runs=runs,
        storage=storage,
        artifacts=artifacts,
        svc=svc,
        events=events,
        org=ctx.org_id,
        actor=admin.id,
        project_id=project.id,
    )


def _request(env: _Env, **overrides) -> ReviewRequest:  # type: ignore[no-untyped-def]
    data = {
        "org_id": env.org,
        "project_id": env.project_id,
        "source": ReviewSource.branch,
        "head": "main",
        "idempotency_key": "idem-1",
        "model": "test-model",
    }
    data.update(overrides)
    return ReviewRequest(**data)


def _good_provider() -> CapturingProvider:
    return CapturingProvider(
        responses=[
            finding_json(
                file_path="app.py",
                line_start=2,
                line_end=2,
                snippet="return a - b  # BUG: subtraction",
            )
        ]
    )


@dataclass
class _SlowProvider:
    """Blocks long enough for the periodic job-lease heartbeat to fire mid-review."""

    delay: float = 5.0

    async def stream(self, request: ProviderRequest):  # type: ignore[no-untyped-def]
        await asyncio.sleep(self.delay)
        yield ProviderChunk(
            delta='{"summary": "ok", "findings": [], "limitations": []}', usage=Usage()
        )


# =============================================================================================
# Blocker 4 — durable job-lease heartbeat aborts/fences the review on loss/cancellation
# =============================================================================================
class TestJobLeaseHeartbeat:
    async def test_cancellation_via_checkpoint_propagates(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _SlowProvider(), lease_seconds=3)
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)

        async def _cancel_checkpoint() -> None:
            raise JobCancellationRequested

        # A cancellation surfaced by the periodic job checkpoint aborts the in-flight review and
        # propagates untouched (the worker honours the cancel), never a terminal completion.
        with pytest.raises(JobCancellationRequested):
            await env.coordinator.execute_review(
                _request(env), run_id=handle.run_id, job_checkpoint=_cancel_checkpoint
            )
        run = await env.runs.get(handle.run_id)
        assert run is not None and run.status is not RunStatus.completed

    async def test_lost_job_lease_aborts_as_retryable(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _SlowProvider(), lease_seconds=3)
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)

        async def _lost_checkpoint() -> None:
            raise RuntimeError("job lease reclaimed by another worker")

        # A generic checkpoint failure (lost job lease) aborts without terminalizing (retryable).
        with pytest.raises((ReviewLeaseLost, RuntimeError)):
            await env.coordinator.execute_review(
                _request(env), run_id=handle.run_id, job_checkpoint=_lost_checkpoint
            )
        run = await env.runs.get(handle.run_id)
        assert run is not None and not run.is_terminal


# =============================================================================================
# Blocker 5 — retry-exhaustion / cancellation terminalizes the run exactly once
# =============================================================================================
class TestRetryExhaustionTerminalizes:
    async def test_terminalize_orphaned_run_is_once(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _good_provider())
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)
        # Simulate the job exhausting retries with the run left admitted/queued.
        first = await env.coordinator.terminalize_orphaned_run(
            handle.run_id,
            status=RunStatus.failed,
            stop_reason="review_job_failed",
            error_kind="retry_exhausted",
            error_message="max attempts reached",
        )
        assert first is True
        run = await env.runs.get(handle.run_id)
        assert run is not None and run.status is RunStatus.failed
        assert run.error_kind == "retry_exhausted"
        # Idempotent: a second terminalization (or a reconciler retry) is a no-op.
        second = await env.coordinator.terminalize_orphaned_run(
            handle.run_id,
            status=RunStatus.failed,
            stop_reason="review_job_failed",
            error_kind="retry_exhausted",
            error_message="max attempts reached",
        )
        assert second is False

    async def test_cancelled_terminalization(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _good_provider())
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)
        ok = await env.coordinator.terminalize_orphaned_run(
            handle.run_id,
            status=RunStatus.cancelled,
            stop_reason="review_job_cancelled",
            error_kind="review_job_cancelled",
            error_message="cancelled",
        )
        assert ok is True
        run = await env.runs.get(handle.run_id)
        assert run is not None and run.status is RunStatus.cancelled

    def test_run_id_extraction(self) -> None:
        from keel_worker.review import _run_id_from_payload

        assert _run_id_from_payload({"run_id": "rev_x"}) == "rev_x"
        assert _run_id_from_payload({}) is None


# =============================================================================================
# Blocker 6 — stranded-admission reconciler re-creates a lost dispatch intent idempotently
# =============================================================================================
class TestStrandedAdmissionReconciler:
    async def test_reconciler_dispatches_stranded_run(self, tmp_path: Path) -> None:
        from keel_core.jobs import InMemoryJobStore, JobLimits
        from keel_worker.review import reconcile_stranded_reviews_tick

        env = await _bootstrap(tmp_path, _good_provider())
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)
        # The run is admitted but its ``review.run`` job was never enqueued (lost in-line enqueue).
        # Age it past the reconciler grace so it is treated as stranded.
        row = env.runs._rows[handle.run_id]  # type: ignore[attr-defined]
        row.created_at = datetime.now(UTC) - timedelta(minutes=5)

        jobs = InMemoryJobStore(env.scope_id, limits=JobLimits())
        ctx = {
            "review_coordinator": env.coordinator,
            "jobs": jobs,
            "job_dispatch_outbox": None,
            "enqueue": None,
        }
        dispatched = await reconcile_stranded_reviews_tick(ctx)
        assert dispatched == 1
        review_jobs = await jobs.list(kind="review.run")
        assert len(review_jobs) == 1
        assert review_jobs[0].payload["run_id"] == handle.run_id

        # Idempotent: a second tick does not create a duplicate job.
        again = await reconcile_stranded_reviews_tick(ctx)
        assert again == 0
        assert len(await jobs.list(kind="review.run")) == 1

    async def test_fresh_run_not_yet_stranded(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _good_provider())
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)
        # A just-admitted run (within grace) is NOT reported as stranded.
        stranded = await env.coordinator.stranded_review_run_ids(grace_seconds=30)
        assert handle.run_id not in stranded


# =============================================================================================
# Blocker 7 — partial usage/cost captured on transient failure + remaining budget
# =============================================================================================
class TestPartialUsageCarryover:
    async def test_engine_exposes_partial_usage_on_transient(self) -> None:
        from keel_core.review.engine import ReviewEngine
        from keel_core.review.models import ReviewBudget

        class _PartialThenTimeout:
            async def stream(self, request: ProviderRequest):  # type: ignore[no-untyped-def]
                yield ProviderChunk(
                    delta="partial ", usage=Usage(prompt_tokens=500, completion_tokens=40)
                )
                raise TimeoutError("stream dropped mid-flight")

        engine = ReviewEngine(_PartialThenTimeout())
        with pytest.raises(ReviewProviderUnavailable) as info:
            await engine.run(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "x"}],
                max_findings=5,
                budget=ReviewBudget(token_budget=100_000, cost_ceiling_usd=5.0),
            )
        usage = info.value.usage
        assert isinstance(usage, Usage)
        # The tokens streamed before the failure are surfaced for durable charging.
        assert usage.prompt_tokens == 500
        assert usage.completion_tokens == 40

    def test_remaining_budget_subtracts_prior_usage(self) -> None:
        from keel_core.review.models import ReviewBudget
        from keel_core.review.service import _remaining_budget

        budget = ReviewBudget(token_budget=1000, cost_ceiling_usd=1.0)
        prior = Usage(prompt_tokens=300, completion_tokens=100, cost_usd=0.4)
        remaining = _remaining_budget(budget, prior)
        assert remaining.token_budget == 600
        assert remaining.cost_ceiling_usd == pytest.approx(0.6)

    def test_remaining_budget_fails_closed_when_exhausted(self) -> None:
        from keel_core.review.errors import ReviewBoundsExceeded
        from keel_core.review.models import ReviewBudget
        from keel_core.review.service import _remaining_budget

        budget = ReviewBudget(token_budget=1000, cost_ceiling_usd=1.0)
        spent = Usage(prompt_tokens=900, completion_tokens=200, cost_usd=0.9)
        with pytest.raises(ReviewBoundsExceeded):
            _remaining_budget(budget, spent)


# =============================================================================================
# Blocker 8 — evidence requires a contiguous, ordered multi-line match
# =============================================================================================
class TestEvidenceContiguousMatch:
    def test_contiguous_ordered_accepted(self) -> None:
        window = ["def f():", "x = compute()", "return x", "done()"]
        snippet = ["x = compute()", "return x"]
        assert _contiguous_ordered_match(window, snippet)

    def test_independent_lines_rejected(self) -> None:
        # Both snippet lines exist in the window but are NOT adjacent -> rejected.
        window = ["a = first()", "middle_1()", "middle_2()", "b = second()"]
        snippet = ["a = first()", "b = second()"]
        assert not _contiguous_ordered_match(window, snippet)

    def test_out_of_order_rejected(self) -> None:
        window = ["line one", "line two", "line three"]
        snippet = ["line two", "line one"]
        assert not _contiguous_ordered_match(window, snippet)

    def test_snippet_complete_at_multiline_window(self) -> None:
        file_text = "\n".join(
            [
                "def add(a, b):",  # 1
                "    total = a + b",  # 2
                "    log(total)",  # 3
                "    return total",  # 4
            ]
        )
        # Contiguous two-line citation at the cited window is confirmed.
        assert _snippet_complete_at(file_text, 2, 3, "    total = a + b\n    log(total)")
        # A snippet stitched from non-adjacent lines 2 and 4 is NOT confirmed.
        assert not _snippet_complete_at(file_text, 2, 4, "    total = a + b\n    return total")


# =============================================================================================
# Blocker 9 — completed list projections load real findings / report corruption honestly
# =============================================================================================
class TestCompletedProjections:
    async def test_completed_report_read_and_corruption(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _good_provider())
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)
        outcome = await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
        assert outcome is not None
        run = await env.runs.get(handle.run_id)
        assert run is not None and run.result_ref is not None

        # A completed run projects its ACTUAL findings from the immutable report artifact.
        report = env.coordinator.read_report_safe(
            project_handle=env.project_id, run_id=handle.run_id, json_sha256=run.result_ref
        )
        assert report is not None and len(report.findings) == 1

        # A missing/corrupt artifact hash is reported honestly (None), never fabricated.
        missing = env.coordinator.read_report_safe(
            project_handle=env.project_id, run_id=handle.run_id, json_sha256="0" * 64
        )
        assert missing is None

    async def test_build_record_from_report_has_findings(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _good_provider())
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)
        await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
        run = await env.runs.get(handle.run_id)
        assert run is not None and run.result_ref is not None
        report = env.coordinator.read_report_safe(
            project_handle=env.project_id, run_id=handle.run_id, json_sha256=run.result_ref
        )
        record = await env.coordinator.build_review_record(run, report=report)
        # The list projection carries the real finding count + report hash, not a zero default.
        assert record.finding_count == 1
        assert record.report_json_sha256 == run.result_ref


# =============================================================================================
# Blocker 10 — review reaper reclaims stale worktrees under fencing, keeps active ones
# =============================================================================================
@dataclass
class _RecordingWorktrees:
    calls: list[datetime] = field(default_factory=list)
    result: ReapResult = field(default_factory=lambda: ReapResult(2, 4096))

    def reap(self, *, older_than: datetime) -> ReapResult:
        self.calls.append(older_than)
        return self.result


class TestReviewReaper:
    async def test_reaper_reaps_stale_worktrees_with_past_cutoff(self, tmp_path: Path) -> None:
        from keel_worker.review import review_artifact_reaper_tick

        worktrees = _RecordingWorktrees()
        ctx = {
            "review_artifacts": None,
            "review_worktrees": worktrees,
            "review_worktree_stale_hours": 6,
            "redis": None,
        }
        removed = await review_artifact_reaper_tick(ctx)
        assert removed == 2
        assert len(worktrees.calls) == 1
        # The cutoff is in the past (now - stale_hours), so ACTIVE (recent) worktrees are kept.
        assert worktrees.calls[0] < datetime.now(UTC) - timedelta(hours=5)

    def test_active_worktree_survives_reap(self, tmp_path: Path) -> None:
        build_source_repo(tmp_path)
        storage = import_into_storage(tmp_path, tmp_path / "source", handle="proj")
        worktrees = LocalWorktreeStore(storage)
        pid = ProjectId("proj")
        crid = CodingRunId("review-active")
        worktrees.materialize(pid, crid, ref="main")
        # A freshly materialized (active) worktree is NOT reaped by a stale cutoff in the past.
        result = worktrees.reap(older_than=datetime.now(UTC) - timedelta(hours=6))
        assert result.removed == 0
        # But it IS reclaimable once considered orphaned (cutoff in the future).
        reaped = worktrees.reap(older_than=datetime.now(UTC) + timedelta(seconds=1))
        assert reaped.removed == 1
