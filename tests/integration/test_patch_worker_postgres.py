"""Live-Postgres verification of the worker-side patch reconciler (M4, WS-PP, P3b-1).

Drives the real :class:`~keel_worker.patch.PatchOutboxReconciler` against a live database — the
durable proposal store + global dispatch pointer + per-scope Postgres job/event stores + the global
job-dispatch outbox — proving the *worker* half of the controlled-patch lifecycle end-to-end:

* a **non-owner ``keel_runtime`` LOGIN** can reconcile a global pointer under org RLS: claim a due
  pointer, load the proposal, and enqueue a per-scope ``patch.generate`` job + a cross-scope
  dispatch intent in the proposal's canonical scope (no owner/BYPASSRLS, no ``row_security=off``);
* two proposals in two distinct per-Agent scopes are reconciled by concurrent workers under
  ``FOR UPDATE SKIP LOCKED`` with no double-processing and each job enqueued in its *own* scope;
* a TTL-lapsed proposal is expired with the pointer deleted in a single atomic transition;
* a ``ready`` proposal auto-heals to ``approval_pending`` through the real coordinator (durable
  approval create-or-get) and the pointer is retired; and
* a pointer whose scope disagrees with the proposal's canonical scope is deferred, never adopted.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from keel_core.approvals import PostgresApprovalStore
from keel_core.config import Settings
from keel_core.job_dispatch import PostgresJobDispatchOutbox
from keel_core.jobs import JobLimits, PostgresJobStore
from keel_core.patch.coordinator import DEFAULT_PATCH_AGENT_ID, PatchCoordinator
from keel_core.patch.jobs import (
    PATCH_GENERATE_KIND,
    PatchGenerateJobPayload,
    persist_generate_metadata,
)
from keel_core.patch.models import PatchStatus
from keel_core.patch.outbox import PostgresPatchProposalOutbox
from keel_core.patch.store import PostgresPatchProposalStore
from keel_core.runs import PostgresRunStore
from keel_core.runtime_db import provision_runtime_login
from keel_core.scoping import derive_agent_scope
from keel_core.state import PostgresEventStore
from keel_worker.patch import PatchOutboxReconciler

pytestmark = pytest.mark.integration

_ORG = "org-a"
_PROJECT = "proj-a"
_AGENT = DEFAULT_PATCH_AGENT_ID
_SCOPE = derive_agent_scope(_ORG, _AGENT)
_AGENT_B = "agent-b"
_SCOPE_B = derive_agent_scope(_ORG, _AGENT_B)
_RUNTIME_LOGIN = "keel_runtime_login_ppw_test"
_RUNTIME_PASSWORD = "runtime-ppw-test-pw"  # noqa: S105 - throwaway local test-role password


def _exp(hours: float = 1.0) -> datetime:
    return datetime.now(UTC) + timedelta(hours=hours)


async def _seed_org_project(engine: AsyncEngine, org: str = _ORG, project: str = _PROJECT) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text(
                "INSERT INTO organizations(id,slug,display_name,status) "
                "VALUES(:id,:id,:id,'active') ON CONFLICT DO NOTHING"
            ),
            {"id": org},
        )
        await conn.execute(
            text(
                "INSERT INTO projects(id,org_id,slug,display_name) "
                "VALUES(:pid,:org,:pid,:pid) ON CONFLICT DO NOTHING"
            ),
            {"pid": project, "org": org},
        )


class _Authorizer:
    """A minimal patch authorizer: ``request_approval`` only needs ``authorize_read``."""

    async def authorize_read(self, org_id, actor, project_id):  # type: ignore[no-untyped-def]
        return None


async def _create_generating(
    store: PostgresPatchProposalStore,
    outbox: PostgresPatchProposalOutbox,
    *,
    pid: str,
    scope: str = _SCOPE,
    agent_id: str = _AGENT,
    org: str = _ORG,
    expires: datetime | None = None,
) -> str:
    run_id = f"run-{pid}"
    await store.create(
        proposal_id=pid,
        org_id=org,
        project_id=_PROJECT,
        run_id=run_id,
        run_attempt=1,
        agent_id=agent_id,
        actor="u",
        base_ref="main",
        source_ref="",
        task_digest="d",
        idempotency_key=f"idem-{pid}",
        fingerprint="fp",
        expires_at=expires or _exp(),
        outbox=outbox,
        scope_id=scope,
    )
    return run_id


async def _persist_metadata(engine: AsyncEngine, *, scope: str, pid: str, run_id: str) -> None:
    payload = PatchGenerateJobPayload(
        proposal_id=pid,
        run_id=run_id,
        org_id=_ORG,
        project_id=_PROJECT,
        actor="u",
        task="apply the change",
        base_ref="main",
        model="m",
        idempotency_key=f"idem-{pid}",
    )
    events = PostgresEventStore(engine, scope)
    await persist_generate_metadata(events, scope_id=scope, payload=payload)


def _reconciler(engine: AsyncEngine, *, worker_id: str = "rec-1") -> PatchOutboxReconciler:
    limits = JobLimits.from_settings(Settings())

    def _coord(scope: str) -> PatchCoordinator:
        return PatchCoordinator(
            store=PostgresPatchProposalStore(engine),
            outbox=PostgresPatchProposalOutbox(engine),
            run_store_factory=lambda s: PostgresRunStore(engine, s),
            authorizer=_Authorizer(),  # type: ignore[arg-type]
            approval_factory=lambda s: PostgresApprovalStore(engine, s),
        )

    return PatchOutboxReconciler(
        store=PostgresPatchProposalStore(engine),
        outbox=PostgresPatchProposalOutbox(engine),
        coordinator_factory=_coord,
        job_store_factory=lambda s: PostgresJobStore(engine, s, limits=limits),
        run_events_factory=lambda s: PostgresEventStore(engine, s),
        job_dispatch_outbox=PostgresJobDispatchOutbox(engine),
        worker_id=worker_id,
    )


async def _jobs_for(engine: AsyncEngine, *, scope: str, kind: str) -> list[str]:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        rows = (
            await conn.execute(
                text("SELECT id FROM jobs WHERE scope_id = :s AND kind = :k"),
                {"s": scope, "k": kind},
            )
        ).all()
    return [r.id for r in rows]


async def _intents_for(engine: AsyncEngine, *, scope: str) -> list[str]:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        rows = (
            await conn.execute(
                text("SELECT job_id FROM job_dispatch_outbox WHERE scope_id = :s"),
                {"s": scope},
            )
        ).all()
    return [r.job_id for r in rows]


# --- runtime login reconcile under RLS -----------------------------------------------------------


@pytest_asyncio.fixture
async def runtime_login_engine(migrated_db: AsyncEngine):
    """A LOGIN engine for a role provisioned as a member of ONLY ``keel_runtime``."""
    async with migrated_db.connect() as conn:
        has_group = await conn.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime'"))
        can_create = await conn.scalar(
            text("SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname = current_user")
        )
    if not has_group:
        pytest.skip("keel_runtime role not provisioned in this database")
    if not can_create:
        pytest.skip("test principal cannot CREATE ROLE for the runtime-login test")

    await provision_runtime_login(
        migrated_db,
        password=_RUNTIME_PASSWORD,
        login_name=_RUNTIME_LOGIN,
        group_role="keel_runtime",
    )
    url = make_url(os.environ["KEEL_TEST_DATABASE_URL"]).set(
        username=_RUNTIME_LOGIN, password=_RUNTIME_PASSWORD
    )
    engine = create_async_engine(url)
    try:
        yield engine
    finally:
        await engine.dispose()
        async with migrated_db.begin() as conn:
            await conn.execute(text(f"DROP ROLE IF EXISTS {_RUNTIME_LOGIN}"))


async def test_runtime_login_reconcile_generating_enqueues_scoped_job(
    migrated_db: AsyncEngine, runtime_login_engine: AsyncEngine
) -> None:
    await _seed_org_project(migrated_db)
    owner_store = PostgresPatchProposalStore(migrated_db)
    owner_outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    run_id = await _create_generating(owner_store, owner_outbox, pid=pid)
    # The server durably persisted the authorized generate request; the reconciler resumes from it.
    await _persist_metadata(migrated_db, scope=_SCOPE, pid=pid, run_id=run_id)

    # Reconcile as the least-privilege runtime LOGIN (no owner, no row_security=off).
    handled = await _reconciler(runtime_login_engine, worker_id="w-runtime").run()
    assert handled == 1

    # The generate job + dispatch intent landed in the proposal's canonical per-Agent scope.
    jobs = await _jobs_for(migrated_db, scope=_SCOPE, kind=PATCH_GENERATE_KIND)
    assert len(jobs) == 1
    assert await _intents_for(migrated_db, scope=_SCOPE) == jobs
    # The pointer now references that exact job (fenced set_job_id succeeded).
    entry = await owner_outbox.get(pid)
    assert entry is not None and entry.job_id == jobs[0]


async def test_reconcile_generating_two_scopes_skip_locked(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db)
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid_a = f"pp_{uuid.uuid4().hex}"
    pid_b = f"pp_{uuid.uuid4().hex}"
    run_a = await _create_generating(store, outbox, pid=pid_a, scope=_SCOPE, agent_id=_AGENT)
    run_b = await _create_generating(store, outbox, pid=pid_b, scope=_SCOPE_B, agent_id=_AGENT_B)
    await _persist_metadata(migrated_db, scope=_SCOPE, pid=pid_a, run_id=run_a)
    await _persist_metadata(migrated_db, scope=_SCOPE_B, pid=pid_b, run_id=run_b)

    # Two concurrent reconcilers race for the two due pointers under SKIP LOCKED.
    a = _reconciler(migrated_db, worker_id="w-a")
    b = _reconciler(migrated_db, worker_id="w-b")
    total = sum(await asyncio.gather(a.run(), b.run()))
    assert total == 2  # each pointer handled exactly once (no double-processing)

    # Each scope got exactly one generate job — in its OWN scope, never cross-scope.
    assert len(await _jobs_for(migrated_db, scope=_SCOPE, kind=PATCH_GENERATE_KIND)) == 1
    assert len(await _jobs_for(migrated_db, scope=_SCOPE_B, kind=PATCH_GENERATE_KIND)) == 1


async def test_reconcile_ttl_expiry_deletes_pointer_atomically(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db)
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    await _create_generating(store, outbox, pid=pid, expires=_exp(-1))
    # Advance to ready (an expirable status) with the pointer left behind, then let its TTL lapse.
    await store.transition(
        _ORG,
        pid,
        PatchStatus.ready,
        expected_version=1,
        updates={"bundle_sha256": "e" * 64},
        outbox=outbox,
        scope_id=_SCOPE,
    )

    assert await _reconciler(migrated_db).run() == 1
    assert (await store.get(_ORG, pid)).status is PatchStatus.expired
    assert await outbox.get(pid) is None


async def test_reconcile_ready_heals_to_approval_pending(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db)
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    await _create_generating(store, outbox, pid=pid)
    # A crash-recovery ``ready`` (bundle bound) that never completed ready -> approval_pending.
    await store.transition(
        _ORG,
        pid,
        PatchStatus.ready,
        expected_version=1,
        updates={"bundle_sha256": "e" * 64, "base_sha": "b" * 40, "head_sha": "c" * 40},
        outbox=outbox,
        scope_id=_SCOPE,
    )

    assert await _reconciler(migrated_db).run() == 1
    proposal = await store.get(_ORG, pid)
    assert proposal.status is PatchStatus.approval_pending
    assert proposal.approval_id
    assert await outbox.get(pid) is None  # pointer retired inside the heal transaction

    # The durable approval was created under the proposal's canonical scope.
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    assert await approvals.get(proposal.approval_id) is not None


async def test_reconcile_scope_mismatch_is_never_adopted(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db)
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    # Proposal canonical scope == _SCOPE, but the pointer is recorded under a foreign scope.
    await _create_generating(store, outbox, pid=pid, scope=_SCOPE_B)

    assert await _reconciler(migrated_db).run() == 0  # deferred, not handled
    entry = await outbox.get(pid)
    # The foreign scope is never rewritten to the proposal's canonical scope.
    assert entry is not None and entry.scope_id == _SCOPE_B
    assert (await store.get(_ORG, pid)).status is PatchStatus.generating
    # No job was enqueued in either the foreign or the canonical scope.
    assert await _jobs_for(migrated_db, scope=_SCOPE, kind=PATCH_GENERATE_KIND) == []
    assert await _jobs_for(migrated_db, scope=_SCOPE_B, kind=PATCH_GENERATE_KIND) == []
