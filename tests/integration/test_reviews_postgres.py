"""Durable read-only review over a live Postgres substrate (runs + project_runs) (WS-R).

Requires a live ``keel_test`` Postgres (``KEEL_TEST_DATABASE_URL``). Uses local git remotes for
the authoritative repo and a scripted provider (no live model/GitHub). Verifies the durable
run lifecycle, the project association, idempotent admission, restart-safety, and that the
content-addressed report artifact is stored and readable.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.coding import GitRunner, LocalArtifactStore, LocalCodingStorage, LocalWorktreeStore
from keel_core.coding.models import CodingRunId, ProjectId
from keel_core.identity import MembershipRole, PostgresIdentityStore
from keel_core.projects import PostgresProjectStore, ProjectService
from keel_core.projects.storage import worktree_storage_id
from keel_core.protocols import ProviderChunk, ProviderRequest, Usage
from keel_core.review import ReviewCoordinator, ReviewRequest, ReviewService, ReviewSource
from keel_core.review.jobs import ReviewJobPayload
from keel_core.runs import PostgresRunStore, RunStatus

pytestmark = pytest.mark.integration

_SCOPE = "web:local"


def _git(cwd: Path, *args: str) -> str:
    return (
        GitRunner()
        .run(["-c", "user.name=T", "-c", "user.email=t@x.test", *args], cwd=cwd)
        .stdout.strip()
    )


def _build_repo(root: Path) -> Path:
    src = root / "source"
    src.mkdir(parents=True)
    _git(src, "init", "--initial-branch=main", ".")
    (src / "app.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "base")
    (src / "app.py").write_text(
        "def add(a, b):\n    return a - b  # BUG: subtraction\n", encoding="utf-8"
    )
    _git(src, "add", "-A")
    _git(src, "commit", "-m", "change")
    return src


def _finding_json() -> str:
    return json.dumps(
        {
            "summary": "subtraction bug",
            "findings": [
                {
                    "severity": "high",
                    "confidence": "high",
                    "title": "Wrong operator",
                    "explanation": "add subtracts",
                    "file_path": "app.py",
                    "line_start": 2,
                    "line_end": 2,
                    "snippet": "return a - b  # BUG: subtraction",
                    "recommendation": "use +",
                }
            ],
            "limitations": [],
        }
    )


@dataclass
class _Provider:
    responses: list[str]
    _i: int = 0

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        text_out = self.responses[min(self._i, len(self.responses) - 1)]
        self._i += 1

        async def _gen() -> AsyncIterator[ProviderChunk]:
            yield ProviderChunk(
                delta=text_out,
                usage=Usage(prompt_tokens=50, completion_tokens=10, cost_usd=0.002),
            )

        return _gen()


@dataclass
class _Env:
    coordinator: ReviewCoordinator
    runs: PostgresRunStore
    org: str
    actor: str
    project_id: str
    handle: str
    request: ReviewRequest = field(init=False)


async def _clean(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        for table in ("project_runs", "run_control", "runs", "projects"):
            await conn.execute(text(f"DELETE FROM {table}"))


async def _setup(engine: AsyncEngine, tmp_path: Path, provider: _Provider) -> _Env:
    identity_store = PostgresIdentityStore(engine)
    owner = await identity_store.create_user(display_name="Owner", email="o@x.test")
    org = await identity_store.create_org(slug="acme-rev", display_name="Acme")
    await identity_store.create_membership(
        org_id=org.id, user_id=owner.id, role=MembershipRole.owner
    )
    projects = ProjectService(PostgresProjectStore(engine), identity_store)
    project = await projects.create_project(org.id, owner.id, slug="rev-proj", display_name="P")
    handle = project.active_git_handle or project.id

    src = _build_repo(tmp_path)
    coding = LocalCodingStorage(tmp_path / "coding", allow_local_remotes=True)
    coding.import_project(handle, src, default_branch="main")

    review_service = ReviewService(
        worktrees=LocalWorktreeStore(coding),
        artifacts=LocalArtifactStore(coding),
        provider=provider,
    )
    runs = PostgresRunStore(engine, _SCOPE)
    from keel_core.state import PostgresEventStore

    coordinator = ReviewCoordinator(
        projects=projects,
        runs=runs,
        review_service=review_service,
        artifacts=LocalArtifactStore(coding),
        scope_id=_SCOPE,
        events=PostgresEventStore(engine, _SCOPE),
    )
    env = _Env(
        coordinator=coordinator,
        runs=runs,
        org=org.id,
        actor=owner.id,
        project_id=project.id,
        handle=handle,
    )
    env.request = ReviewRequest(
        org_id=org.id,
        project_id=project.id,
        source=ReviewSource.branch,
        head="main",
        idempotency_key="rev-idem-1",
        model="test-model",
    )
    return env


async def test_durable_review_completes_and_persists(
    migrated_db: AsyncEngine, tmp_path: Path
) -> None:
    await _clean(migrated_db)
    env = await _setup(migrated_db, tmp_path, _Provider([_finding_json()]))

    handle = await env.coordinator.request_review(env.request, actor=env.actor)
    assert handle.created

    # The run and project association are durable in Postgres.
    async with migrated_db.connect() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": _SCOPE})
        run_row = await conn.execute(
            text("SELECT surface, status FROM runs WHERE id = :id"), {"id": handle.run_id}
        )
        surface, status_value = run_row.one()
        assert surface == "review"
        assoc = await conn.scalar(
            text("SELECT project_id FROM project_runs WHERE org_id = :o AND run_id = :r"),
            {"o": env.org, "r": handle.run_id},
        )
    assert assoc == env.project_id

    payload = ReviewJobPayload.from_request(env.request, run_id=handle.run_id)
    outcome = await env.coordinator.execute_review(payload.to_request(), run_id=handle.run_id)
    assert outcome is not None
    assert len(outcome.report.findings) == 1

    run = await env.runs.get(handle.run_id)
    assert run is not None
    assert run.status is RunStatus.completed
    assert run.result_ref == outcome.json_sha256
    assert run.cost_usd == pytest.approx(0.002)

    # The content-addressed report is readable from coding storage by hash.
    artifacts = LocalArtifactStore(LocalCodingStorage(tmp_path / "coding"))
    data = artifacts.read(
        ProjectId(env.handle),
        CodingRunId(worktree_storage_id(handle.run_id)),
        outcome.json_sha256,
    )
    assert json.loads(data)["head_sha"] == outcome.head_sha


async def test_duplicate_request_is_idempotent_in_postgres(
    migrated_db: AsyncEngine, tmp_path: Path
) -> None:
    await _clean(migrated_db)
    env = await _setup(migrated_db, tmp_path, _Provider([_finding_json()]))
    first = await env.coordinator.request_review(env.request, actor=env.actor)
    second = await env.coordinator.request_review(env.request, actor=env.actor)
    assert first.run_id == second.run_id
    assert not second.created

    async with migrated_db.connect() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": _SCOPE})
        count = await conn.scalar(
            text("SELECT count(*) FROM runs WHERE surface = 'review'"),
        )
    assert count == 1


async def test_execute_is_restart_safe_in_postgres(
    migrated_db: AsyncEngine, tmp_path: Path
) -> None:
    await _clean(migrated_db)
    env = await _setup(migrated_db, tmp_path, _Provider([_finding_json()]))
    handle = await env.coordinator.request_review(env.request, actor=env.actor)
    first = await env.coordinator.execute_review(env.request, run_id=handle.run_id)
    assert first is not None
    # A retried job on an already-terminal run is a durable no-op.
    second = await env.coordinator.execute_review(env.request, run_id=handle.run_id)
    assert second is None


async def test_pending_projection_truthful_after_restart_in_postgres(
    migrated_db: AsyncEngine, tmp_path: Path
) -> None:
    await _clean(migrated_db)
    env = await _setup(migrated_db, tmp_path, _Provider([_finding_json()]))
    handle = await env.coordinator.request_review(env.request, actor=env.actor)

    # A fresh coordinator (process restart) over the same durable run + event stores projects
    # the pending review truthfully from the durably-persisted request metadata.
    from keel_core.state import PostgresEventStore

    restarted = ReviewCoordinator(
        projects=env.coordinator._projects,  # type: ignore[attr-defined]
        runs=env.runs,
        review_service=env.coordinator._reviews,  # type: ignore[attr-defined]
        artifacts=env.coordinator._artifacts,  # type: ignore[attr-defined]
        scope_id=_SCOPE,
        events=PostgresEventStore(migrated_db, _SCOPE),
    )
    run = await restarted.get_run(handle.run_id)
    record = await restarted.build_review_record(run)
    assert record.source is ReviewSource.branch
    assert record.head == "main"
    assert record.model == "test-model"
