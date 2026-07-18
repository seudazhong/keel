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
from keel_core.review.coordinator import (
    REVIEW_REQUEST_MARKER,
    REVIEW_REQUEST_METADATA_VERSION,
    _heartbeat_interval,
    review_fingerprint,
)
from keel_core.review.errors import (
    ReviewLeaseLost,
    ReviewProviderUnavailable,
    ReviewValidationError,
)
from keel_core.review.evidence import _contiguous_ordered_match, _snippet_complete_at
from keel_core.runs import InMemoryRunStore, RunAdmissionConflict, RunStatus


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


# =============================================================================================
# Final blocker 1 — durable job-lease heartbeat interval derives from the ACTUAL job lease
# =============================================================================================
class TestJobHeartbeatIntervalDerivation:
    def test_interval_for_300s_job_lease(self) -> None:
        # A 300s job lease -> min(100, cap=30) == 30, strictly below expiry (never equal).
        interval = _heartbeat_interval(300)
        assert interval == 30.0
        assert interval < 300

    @pytest.mark.parametrize("lease", [0.5, 1, 2, 3, 30, 90, 300, 900, 100_000])
    def test_interval_always_strictly_below_lease(self, lease: float) -> None:
        interval = _heartbeat_interval(lease)
        assert 0 < interval < lease

    def test_interval_clock_boundary_small_lease(self) -> None:
        # A 1s lease must NOT beat exactly on expiry: it is halved to stay strictly inside.
        assert _heartbeat_interval(1) == 0.5
        # A 3s lease -> lease/3 == 1.0 (< 3), the value the 300s-lease cap never reaches.
        assert _heartbeat_interval(3) == 1.0

    async def test_heartbeat_uses_job_lease_not_run_lease(self, tmp_path: Path) -> None:
        # Long RUN lease (900s -> run keeper interval 30s) but a short JOB lease (3s -> heartbeat
        # interval 1s). A cancellation surfaced via the job checkpoint must fire within the
        # JOB-lease-derived interval (≈1s) and abort the slow review, proving the heartbeat is
        # driven by the actual job lease, not the run lease (which would never fire in time).
        env = await _bootstrap(tmp_path, _SlowProvider(delay=4.0), lease_seconds=900)
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)
        beats = 0

        async def _cancel_on_second_beat() -> None:
            nonlocal beats
            beats += 1
            if beats >= 1:
                raise JobCancellationRequested

        with pytest.raises(JobCancellationRequested):
            await env.coordinator.execute_review(
                _request(env),
                run_id=handle.run_id,
                job_checkpoint=_cancel_on_second_beat,
                job_lease_seconds=3,
            )
        assert beats >= 1


# =============================================================================================
# Final blocker 2 — cancellation terminalizes; job-lease loss releases; run-lease loss aborts
# =============================================================================================
class TestCancellationAndLeaseLoss:
    async def test_cancellation_terminalizes_run_cancelled(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _SlowProvider(delay=3.0), lease_seconds=3)
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)

        async def _cancel_checkpoint() -> None:
            raise JobCancellationRequested

        # A cancellation while we still own the run fence terminalizes the run CANCELLED
        # atomically under the current lease AND propagates the cancellation (worker honours it).
        with pytest.raises(JobCancellationRequested):
            await env.coordinator.execute_review(
                _request(env),
                run_id=handle.run_id,
                job_checkpoint=_cancel_checkpoint,
                job_lease_seconds=3,
            )
        run = await env.runs.get(handle.run_id)
        assert run is not None and run.status is RunStatus.cancelled
        assert run.is_terminal

    async def test_job_lease_loss_releases_run_to_queued(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _SlowProvider(delay=3.0), lease_seconds=3)
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)

        async def _lost_checkpoint() -> None:
            raise RuntimeError("job lease reclaimed by another worker")

        # A lost job lease while the run lease is still valid releases the run back to QUEUED
        # (retryable) — never left as a live running lease, and never terminalized.
        with pytest.raises(ReviewLeaseLost):
            await env.coordinator.execute_review(
                _request(env),
                run_id=handle.run_id,
                job_checkpoint=_lost_checkpoint,
                job_lease_seconds=3,
            )
        run = await env.runs.get(handle.run_id)
        assert run is not None
        assert run.status is RunStatus.queued
        assert not run.is_terminal
        assert run.worker_id is None and run.lease_token is None

    async def test_run_lease_loss_aborts_without_terminalizing(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _SlowProvider(delay=2.0), lease_seconds=3)
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)

        async def _false_renew(lease, *, lease_seconds, now=None):  # type: ignore[no-untyped-def]
            return False

        # Simulate our run lease being reclaimed by another worker mid-review (renew fails).
        env.runs.renew = _false_renew  # type: ignore[assignment]
        with pytest.raises(ReviewLeaseLost):
            await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
        run = await env.runs.get(handle.run_id)
        # We do NOT own the run any more, so we must NOT write a terminal state for it.
        assert run is not None and not run.is_terminal

    async def test_cancellation_cleanup_is_idempotent_when_already_terminal(
        self, tmp_path: Path
    ) -> None:
        env = await _bootstrap(tmp_path, _SlowProvider(delay=3.0), lease_seconds=3)
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)

        async def _cancel_checkpoint() -> None:
            raise JobCancellationRequested

        # The primary cancellation signal is preserved even though cleanup terminalizes the run;
        # a subsequent orphan terminalization is a no-op (run already terminal).
        with pytest.raises(JobCancellationRequested):
            await env.coordinator.execute_review(
                _request(env), run_id=handle.run_id, job_checkpoint=_cancel_checkpoint
            )
        again = await env.coordinator.terminalize_orphaned_run(
            handle.run_id,
            status=RunStatus.cancelled,
            stop_reason="review_job_cancelled",
            error_kind="review_job_cancelled",
            error_message="cancelled",
        )
        assert again is False


# =============================================================================================
# Final blocker 3 — persist the full budget/policy envelope; bind it into the fingerprint
# =============================================================================================
class TestBudgetMetadataAndFingerprint:
    def test_fingerprint_binds_budget_fields(self) -> None:
        base = ReviewRequest(
            org_id="o",
            project_id="p",
            source=ReviewSource.branch,
            head="main",
            idempotency_key="k",
            model="m",
        )
        bigger = ReviewRequest(
            org_id="o",
            project_id="p",
            source=ReviewSource.branch,
            head="main",
            idempotency_key="k",
            model="m",
            token_budget=base.token_budget + 1,
        )
        assert review_fingerprint(base) != review_fingerprint(bigger)

    async def test_same_key_different_budget_is_a_conflict(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _good_provider())
        await env.coordinator.request_review(
            _request(env, idempotency_key="dup", token_budget=100_000), actor=env.actor
        )
        # Re-using the idempotency key with a DIFFERENT budget mismatches the immutable
        # fingerprint (budget is bound into it) and is rejected rather than silently reused.
        with pytest.raises(RunAdmissionConflict):
            await env.coordinator.request_review(
                _request(env, idempotency_key="dup", token_budget=100_001), actor=env.actor
            )

    async def test_metadata_round_trips_exact_budget(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _good_provider())
        handle = await env.coordinator.request_review(
            _request(
                env,
                token_budget=123_456,
                output_max_tokens=1_234,
                cost_ceiling_usd=0.5,
                max_provider_attempts=1,
            ),
            actor=env.actor,
        )
        loaded = await env.coordinator.load_request_metadata(handle.run_id)
        assert loaded is not None
        assert loaded.token_budget == 123_456
        assert loaded.output_max_tokens == 1_234
        assert loaded.cost_ceiling_usd == 0.5
        assert loaded.max_provider_attempts == 1

    async def test_reconstructed_payload_uses_exact_not_default_budget(
        self, tmp_path: Path
    ) -> None:
        from keel_core.review.jobs import ReviewJobPayload
        from keel_core.review.models import DEFAULT_REVIEW_TOKEN_BUDGET

        env = await _bootstrap(tmp_path, _good_provider())
        assert 123_456 != DEFAULT_REVIEW_TOKEN_BUDGET
        handle = await env.coordinator.request_review(
            _request(env, token_budget=123_456), actor=env.actor
        )
        loaded = await env.coordinator.load_request_metadata(handle.run_id)
        assert loaded is not None
        payload = ReviewJobPayload.from_request(loaded, run_id=handle.run_id)
        # A stranded re-dispatch reconstructs the EXACT admitted budget, never a larger default.
        assert payload.token_budget == 123_456

    async def test_legacy_metadata_missing_budget_fails_closed(self, tmp_path: Path) -> None:
        from datetime import datetime as _dt

        from keel_core.events import Event, EventType

        env = await _bootstrap(tmp_path, _good_provider())
        run_id = "rev_legacy_no_budget"
        # A pre-budget (legacy) metadata record: identity present, budget envelope absent.
        await env.events.append(  # type: ignore[attr-defined]
            Event(
                type=EventType.run_started,
                seq=0,
                session_id=run_id,
                scope_id=env.scope_id,
                run_id=run_id,
                ts=_dt.now(UTC),
                payload={
                    REVIEW_REQUEST_MARKER: {
                        "org_id": env.org,
                        "project_id": env.project_id,
                        "source": "branch",
                        "head": "main",
                        "base": None,
                        "model": "test-model",
                        "agent_id": None,
                        "max_findings": 5,
                        "max_diff_bytes": 1000,
                    },
                    "dedup_key": f"review-meta:{run_id}",
                },
            )
        )
        # Fail closed: never back-fill (possibly larger) implicit budget defaults.
        assert await env.coordinator.load_request_metadata(run_id) is None

    async def test_current_metadata_carries_version(self, tmp_path: Path) -> None:
        env = await _bootstrap(tmp_path, _good_provider())
        handle = await env.coordinator.request_review(_request(env), actor=env.actor)
        found = False
        async for event in env.events.read(handle.run_id):  # type: ignore[attr-defined]
            meta = event.payload.get(REVIEW_REQUEST_MARKER)
            if isinstance(meta, dict):
                assert meta["version"] == REVIEW_REQUEST_METADATA_VERSION
                found = True
        assert found


# =============================================================================================
# Final blocker 4 — worker storage readiness fails fast (or review disabled skips entirely)
# =============================================================================================
class TestWorkerStorageReadiness:
    def _settings(self, **overrides):  # type: ignore[no-untyped-def]
        from keel_core.config import Settings

        return Settings(**overrides)

    def test_review_disabled_skips_storage_and_returns_none(self) -> None:
        from keel_worker.review import resolve_review_storage_root

        calls: list[object] = []

        def _probe(root: object) -> None:
            calls.append(root)

        settings = self._settings(review_enabled=False, project_storage_root="")
        assert resolve_review_storage_root(settings, probe=_probe) is None
        # A review-disabled worker never even probes shared storage.
        assert calls == []

    def test_review_enabled_failfast_on_unavailable_storage(self, tmp_path: Path) -> None:
        from keel_worker.review import ReviewStorageNotReady, resolve_review_storage_root

        def _failing_probe(root: object) -> None:
            raise SharedStorageUnavailable("volume not mounted")

        settings = self._settings(
            review_enabled=True, project_storage_root=str(tmp_path), app_env="production"
        )
        with pytest.raises(ReviewStorageNotReady):
            resolve_review_storage_root(settings, probe=_failing_probe)

    def test_review_enabled_returns_root_when_storage_ok(self, tmp_path: Path) -> None:
        from keel_worker.review import resolve_review_storage_root

        def _ok_probe(root: object) -> None:
            return None

        settings = self._settings(
            review_enabled=True, project_storage_root=str(tmp_path), app_env="production"
        )
        root = resolve_review_storage_root(settings, probe=_ok_probe)
        assert root == tmp_path

    def test_k8s_and_compose_worker_share_project_storage(self) -> None:
        import yaml

        repo_root = Path(__file__).resolve().parents[2]
        # K8s: the worker mounts the shared project-storage PVC at the same path as the server,
        # so a review-enabled worker can always reach the volume its reports live on.
        worker_manifest = (
            repo_root / "deploy" / "k8s" / "base" / "worker" / "deployment.yaml"
        ).read_text(encoding="utf-8")
        assert "keel-project-storage" in worker_manifest
        assert "/var/lib/keel/projects" in worker_manifest

        # Compose: server and worker mount the SAME named volume so review storage is shared.
        compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))
        server_vols = compose["services"]["keel-server"]["volumes"]
        worker_vols = compose["services"]["keel-worker"]["volumes"]
        assert any("projectdata:" in v for v in server_vols)
        assert any("projectdata:" in v for v in worker_vols)
        # Same mount path on both so a worker-written report is readable by the server APIs.
        server_path = [v.split(":", 1)[1] for v in server_vols if "projectdata:" in v][0]
        worker_path = [v.split(":", 1)[1] for v in worker_vols if "projectdata:" in v][0]
        assert server_path == worker_path
