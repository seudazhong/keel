"""Durable read-only review lifecycle: coordinator + job handler + authorization."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from review_support import (
    CapturingProvider,
    build_source_repo,
    finding_json,
    import_into_storage,
)

from keel_core.coding import LocalArtifactStore, LocalCodingStorage, LocalWorktreeStore
from keel_core.errors import PermissionDenied
from keel_core.identity.service import IdentityService
from keel_core.identity.store import InMemoryIdentityStore
from keel_core.jobs import JobResult, PermanentJobError
from keel_core.projects import (
    InMemoryProjectAuditSink,
    InMemoryProjectStorage,
    InMemoryProjectStore,
    ProjectService,
)
from keel_core.review import (
    ReviewCoordinator,
    ReviewRequest,
    ReviewService,
    ReviewSource,
    ReviewStatus,
)
from keel_core.review.coordinator import REVIEW_SURFACE
from keel_core.review.errors import ReviewProviderError
from keel_core.review.jobs import (
    REVIEW_RUN_KIND,
    ReviewJobHandlers,
    ReviewJobPayload,
    review_idempotency_key,
)
from keel_core.runs import InMemoryRunStore, RunStatus


@dataclass
class Env:
    coordinator: ReviewCoordinator
    runs: InMemoryRunStore
    provider: CapturingProvider
    storage: LocalCodingStorage
    svc: ProjectService
    org: str
    actor: str
    stranger: str
    project_id: str
    scope_id: str = "scope-1"


async def _bootstrap(tmp_path: Path, provider: CapturingProvider) -> Env:
    identity = InMemoryIdentityStore()
    isvc = IdentityService(identity)
    admin = await isvc.store.create_user(display_name="Admin", email="admin@x.io")
    stranger = await isvc.store.create_user(display_name="Nobody", email="nobody@x.io")
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
    review_service = ReviewService(
        worktrees=LocalWorktreeStore(storage),
        artifacts=LocalArtifactStore(storage),
        provider=provider,
    )
    coordinator = ReviewCoordinator(
        projects=svc,
        runs=InMemoryRunStore(),
        review_service=review_service,
        artifacts=LocalArtifactStore(storage),
        scope_id="scope-1",
    )
    return Env(
        coordinator=coordinator,
        runs=coordinator._runs,  # type: ignore[attr-defined]
        provider=provider,
        storage=storage,
        svc=svc,
        org=ctx.org_id,
        actor=admin.id,
        stranger=stranger.id,
        project_id=project.id,
    )


def _request(env: Env, **overrides) -> ReviewRequest:
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


async def test_request_creates_run_and_associates(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    handle = await env.coordinator.request_review(_request(env), actor=env.actor)
    assert handle.created
    assert handle.status is ReviewStatus.pending
    run = await env.runs.get(handle.run_id)
    assert run is not None and run.surface == REVIEW_SURFACE
    runs = await env.svc.list_project_runs(env.org, env.actor, env.project_id)
    assert handle.run_id in runs


async def test_duplicate_request_is_idempotent(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    first = await env.coordinator.request_review(_request(env), actor=env.actor)
    second = await env.coordinator.request_review(_request(env), actor=env.actor)
    assert not second.created
    assert first.run_id == second.run_id


async def test_cross_actor_denied(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    with pytest.raises(PermissionDenied):
        await env.coordinator.request_review(_request(env), actor=env.stranger)


async def test_execute_completes_and_terminalizes(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    handle = await env.coordinator.request_review(_request(env), actor=env.actor)
    outcome = await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
    assert outcome is not None
    run = await env.runs.get(handle.run_id)
    assert run is not None
    assert run.status is RunStatus.completed
    assert run.result_ref == outcome.json_sha256
    assert run.cost_usd == pytest.approx(0.001)
    # The report is readable by content hash via the coordinator.
    report = env.coordinator.read_report(
        project_handle=env.project_id, run_id=handle.run_id, json_sha256=run.result_ref
    )
    assert report.status is ReviewStatus.completed
    assert len(report.findings) == 1


async def test_execute_is_restart_safe(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    handle = await env.coordinator.request_review(_request(env), actor=env.actor)
    first = await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
    assert first is not None
    # A retried job on an already-terminal run is a no-op (idempotent).
    second = await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
    assert second is None


async def test_execute_reclaims_crashed_run(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    handle = await env.coordinator.request_review(_request(env), actor=env.actor)
    # Simulate a worker that claimed the run then crashed: running with an expired lease.
    await env.runs.mark_queued(handle.run_id)
    lease = await env.runs.claim(
        handle.run_id, worker_id="dead", now=datetime.now(UTC) - timedelta(hours=1), lease_seconds=1
    )
    assert lease is not None
    outcome = await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
    assert outcome is not None
    run = await env.runs.get(handle.run_id)
    assert run is not None and run.status is RunStatus.completed


async def test_execute_failure_marks_run_failed(tmp_path: Path) -> None:
    provider = CapturingProvider(responses=["garbage", "garbage"])
    env = await _bootstrap(tmp_path, provider)
    handle = await env.coordinator.request_review(_request(env), actor=env.actor)
    with pytest.raises(ReviewProviderError):
        await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
    run = await env.runs.get(handle.run_id)
    assert run is not None and run.status is RunStatus.failed
    assert run.error_kind == "ReviewProviderError"


async def test_github_token_never_reaches_provider(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    handle = await env.coordinator.request_review(_request(env), actor=env.actor)
    await env.coordinator.execute_review(_request(env), run_id=handle.run_id)
    # No GitHub integration is wired into the review path at all; the provider only ever
    # sees the fenced diff + system prompt, never a token/credential.
    blob = json.dumps([req.model_dump() for req in env.provider.requests])
    for marker in ("ghs_", "Authorization", "Bearer", "x-access-token", "private_key"):
        assert marker not in blob


# --- job handler ------------------------------------------------------------------


@dataclass
class _JobCtx:
    scope_id: str = "scope-1"
    job_id: str = "job-1"
    checkpoints: int = 0

    async def checkpoint(self) -> None:
        self.checkpoints += 1


async def test_job_handler_runs_review(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    handle = await env.coordinator.request_review(_request(env), actor=env.actor)
    payload = ReviewJobPayload.from_request(_request(env), run_id=handle.run_id)
    handlers = ReviewJobHandlers(env.coordinator)
    result = await handlers.run(_JobCtx(), payload.model_dump(mode="json"))
    assert isinstance(result, JobResult)
    assert result.data["status"] == "completed"
    assert result.data["finding_count"] == 1


async def test_job_handler_rejects_bad_payload(tmp_path: Path) -> None:
    env = await _bootstrap(tmp_path, _good_provider())
    handlers = ReviewJobHandlers(env.coordinator)
    with pytest.raises(PermanentJobError):
        await handlers.run(_JobCtx(), {"not": "valid"})


def test_review_idempotency_key_stable() -> None:
    assert review_idempotency_key("rev_abc") == "review.run:rev_abc"
    assert REVIEW_RUN_KIND == "review.run"
