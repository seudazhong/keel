"""Live-Postgres verification of the patch dispatch outbox + atomic approval seam (M4, WS-PP, P1).

Exercises the real migration + durable implementations against a live database:

* ``0021`` creates a **non-RLS** global ``patch_proposal_outbox`` with the fenced-lease CHECK, both
  ordinary due-scan indexes (no ``now()`` predicate), the composite ``ON DELETE CASCADE`` FK, and
  minimal DML grants to the non-owner runtime + maintenance roles — and downgrades cleanly;
* the least-privilege ``keel_runtime`` LOGIN can read/lease/update/delete pointers (the outbox is
  the cross-org index, so no scope GUC is needed) but cannot run DDL against it;
* ``claim_due`` leases a bounded batch under ``FOR UPDATE SKIP LOCKED`` with a random per-row
  ``lease_token``; a stale token can never ack/defer/retire/annotate a re-leased pointer; an expired
  lease is reclaimable;
* proposal admission records the ``generating`` pointer in the **same** transaction as the proposal
  (both commit or roll back together);
* ``ready -> approval_pending`` create-or-gets the durable approval, bumps the proposal and deletes
  the pointer in a single transaction — a failure leaves **no** residual approval, and two
  concurrent callers resolve the *same* approval id with exactly one version bump; and
* erasing the proposal (transitively: project/org) cascades the pointer away.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from keel_core.approvals import PostgresApprovalStore
from keel_core.patch.models import PatchStatus
from keel_core.patch.outbox import PatchOutboxStatus, PostgresPatchProposalOutbox
from keel_core.patch.store import ApprovalDraft, PostgresPatchProposalStore
from keel_core.runtime_db import provision_runtime_login

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PREV = "0020_runtime_role_hardening"
_SCOPE = "agent:org-a/patch"
_RUNTIME_LOGIN = "keel_runtime_login_pp_test"
_RUNTIME_PASSWORD = "runtime-pp-test-pw"  # noqa: S105 - throwaway local test-role password


def _alembic_cfg(url: str):  # type: ignore[no-untyped-def]
    from alembic.config import Config

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _run_to(url: str, revision: str) -> None:
    from alembic import command

    cfg = _alembic_cfg(url)
    if revision == "head":
        command.upgrade(cfg, "head")
    else:
        command.downgrade(cfg, revision)


def _sync_url() -> str:
    raw = os.environ.get("KEEL_TEST_DATABASE_URL")
    if not raw:
        pytest.skip("KEEL_TEST_DATABASE_URL not set")
    return (
        raw.replace("+psycopg", "")
        .replace("+asyncpg", "")
        .replace("postgresql", "postgresql+psycopg", 1)
    )


async def _seed_org_project(engine: AsyncEngine, org: str, project: str) -> None:
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
            text("INSERT INTO projects(id,org_id,slug,display_name) VALUES(:pid,:org,:pid,:pid)"),
            {"pid": project, "org": org},
        )


def _exp() -> datetime:
    return datetime.now(UTC) + timedelta(hours=1)


async def _create_proposal(
    store: PostgresPatchProposalStore,
    outbox: PostgresPatchProposalOutbox | None,
    *,
    org: str = "org-a",
    project: str = "proj-a",
    run: str = "run-1",
    idem: str | None = None,
) -> str:
    pid = f"pp_{uuid.uuid4().hex}"
    proposal, _ = await store.create(
        proposal_id=pid,
        org_id=org,
        project_id=project,
        run_id=run,
        run_attempt=1,
        agent_id="patch",
        actor="u",
        base_ref="main",
        source_ref="",
        task_digest="d",
        idempotency_key=idem or f"idem-{uuid.uuid4().hex}",
        fingerprint="fp",
        expires_at=_exp(),
        outbox=outbox,
        scope_id=_SCOPE if outbox is not None else None,
    )
    return proposal.id


async def _make_ready(store: PostgresPatchProposalStore, org: str, pid: str) -> int:
    p = await store.transition(
        org, pid, PatchStatus.ready, expected_version=1, updates={"bundle_sha256": "e" * 64}
    )
    return p.version


def _draft(run: str = "run-1", **over: object) -> ApprovalDraft:
    kw: dict[str, object] = dict(
        run_id=run,
        session_id="sess-1",
        tool="patch.apply",
        args={"pid": "x"},
        call_id="call-1",
        idempotency_key="aidem-1",
        reason="human approval required",
        expires_at=_exp(),
        actor="alice",
        action_hash="abc123",
        run_attempt=1,
    )
    kw.update(over)
    return ApprovalDraft(**kw)  # type: ignore[arg-type]


# --- migration 0021 schema + grants + reversibility ----------------------------------


async def test_migration_0021_schema_grants_and_reversibility(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    async with engine.begin() as conn:
        # 1) Table exists and is deliberately NOT under RLS (it is the cross-org index).
        rel = (
            await conn.execute(
                text(
                    "SELECT relrowsecurity, relforcerowsecurity FROM pg_class "
                    "WHERE relname = 'patch_proposal_outbox'"
                )
            )
        ).one()
        assert rel.relrowsecurity is False
        assert rel.relforcerowsecurity is False

        # 2) Both due-scan indexes exist and NO index carries a now() predicate.
        idx = {
            r.indexname: r.indexdef
            for r in (
                await conn.execute(
                    text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = :t"),
                    {"t": "patch_proposal_outbox"},
                )
            ).all()
        }
        assert "ix_patch_proposal_outbox_due" in idx
        assert "ix_patch_proposal_outbox_scope" in idx
        assert all("now(" not in definition.lower() for definition in idx.values())

        # 3) Lease all-or-nothing CHECK + composite CASCADE FK are present.
        checks = (
            await conn.execute(
                text(
                    "SELECT conname, pg_get_constraintdef(oid) AS condef FROM pg_constraint "
                    "WHERE conrelid = 'patch_proposal_outbox'::regclass"
                )
            )
        ).all()
        by_name = {c.conname: c.condef for c in checks}
        assert "ck_patch_proposal_outbox_lease" in by_name
        assert "num_nonnulls" in by_name["ck_patch_proposal_outbox_lease"]
        fk = next((c.condef for c in checks if c.condef.startswith("FOREIGN KEY")), "")
        assert "REFERENCES patch_proposals(id, org_id)" in fk
        assert "ON DELETE CASCADE" in fk

        # 4) Minimal DML granted to the non-owner runtime + maintenance roles; no more, no DDL.
        for role in ("keel_runtime", "keel_maintenance"):
            present = await conn.scalar(
                text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": role}
            )
            if not present:
                continue
            grants = {
                r.privilege_type
                for r in (
                    await conn.execute(
                        text(
                            "SELECT privilege_type FROM information_schema.role_table_grants "
                            "WHERE table_name = 'patch_proposal_outbox' AND grantee = :g"
                        ),
                        {"g": role},
                    )
                ).all()
            }
            assert grants == {"SELECT", "INSERT", "UPDATE", "DELETE"}

    # 5) Reversibility: down to 0020 drops it, back up re-creates it.
    url = _sync_url()
    await asyncio.to_thread(_run_to, url, _PREV)
    assert "patch_proposal_outbox" not in await _table_names(engine)
    await asyncio.to_thread(_run_to, url, "head")
    assert "patch_proposal_outbox" in await _table_names(engine)


# --- runtime login: DML allowed, DDL denied ------------------------------------------


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


async def test_runtime_login_outbox_dml_allowed_ddl_denied(
    migrated_db: AsyncEngine, runtime_login_engine: AsyncEngine
) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    owner_store = PostgresPatchProposalStore(migrated_db)
    pid = await _create_proposal(owner_store, None)

    # The non-owner runtime role can read/lease/update/delete pointers with NO scope GUC (the
    # outbox is the global cross-org index; row_security=off is never used).
    async with runtime_login_engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO patch_proposal_outbox (proposal_id, org_id, scope_id, expires_at) "
                "VALUES (:pid, :org, :scope, :exp)"
            ),
            {"pid": pid, "org": "org-a", "scope": _SCOPE, "exp": _exp()},
        )
        seen = await conn.scalar(
            text("SELECT count(*) FROM patch_proposal_outbox WHERE proposal_id = :pid"),
            {"pid": pid},
        )
        assert seen == 1
        upd = await conn.execute(
            text("UPDATE patch_proposal_outbox SET status_hint = 'ready' WHERE proposal_id = :pid"),
            {"pid": pid},
        )
        assert upd.rowcount == 1

    # A real lease cycle through the durable implementation under the runtime login.
    outbox = PostgresPatchProposalOutbox(runtime_login_engine)
    claimed = await outbox.claim_due(worker_id="w-runtime", lease_seconds=60)
    assert any(c.proposal_id == pid for c in claimed)

    async with runtime_login_engine.begin() as conn:
        deleted = await conn.execute(
            text("DELETE FROM patch_proposal_outbox WHERE proposal_id = :pid"), {"pid": pid}
        )
        assert deleted.rowcount == 1

    # DDL against the outbox is denied for the least-privilege runtime role.
    for stmt in (
        "CREATE INDEX pp_probe ON patch_proposal_outbox (org_id)",
        "ALTER TABLE patch_proposal_outbox ADD COLUMN probe int",
        "DROP TABLE patch_proposal_outbox",
    ):
        async with runtime_login_engine.connect() as conn:
            with pytest.raises((ProgrammingError, DBAPIError)):
                await conn.execute(text(stmt))


# --- claim SKIP LOCKED + fencing + reclaim -------------------------------------------


async def test_claim_skip_locked_fencing_and_reclaim(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid1 = await _create_proposal(store, outbox, run="r1")
    pid2 = await _create_proposal(store, outbox, run="r2")
    # Anchor the clock after admission so both pointers are already due (next_attempt_at <= t0).
    t0 = datetime.now(UTC) + timedelta(seconds=1)

    first = await outbox.claim_due(worker_id="w1", now=t0, lease_seconds=60)
    claimed_ids = {c.proposal_id for c in first}
    assert {pid1, pid2} <= claimed_ids
    tokens = {c.proposal_id: c.lease_token for c in first}
    assert all(tok is not None for tok in tokens.values())
    assert tokens[pid1] != tokens[pid2]  # random per-row fencing token

    # A second worker sees nothing while the leases are live.
    assert await outbox.claim_due(worker_id="w2", now=t0 + timedelta(seconds=1)) == []

    # A stale worker cannot ack/defer/retire/annotate its now-expired-and-reclaimed pointer.
    reclaimed = await outbox.claim_due(worker_id="w2", now=t0 + timedelta(seconds=120))
    reclaimed_ids = {c.proposal_id for c in reclaimed}
    assert {pid1, pid2} <= reclaimed_ids
    new_tokens = {c.proposal_id: c.lease_token for c in reclaimed}
    assert new_tokens[pid1] != tokens[pid1]  # re-leased with a fresh token
    for c in reclaimed:
        if c.proposal_id == pid1:
            assert c.attempts == 2  # reclaim keeps climbing the attempt counter

    stale = tokens[pid1]
    assert stale is not None
    when = t0 + timedelta(seconds=121)
    assert await outbox.complete(pid1, lease_token=stale, now=when) is False
    assert await outbox.reschedule(pid1, lease_token=stale, now=when) is False
    assert await outbox.remove(pid1, lease_token=stale, now=when) is False
    assert await outbox.set_job_id(pid1, "job-x", lease_token=stale, now=when) is False

    # The fresh lease holder can complete (release + reset attempts + due now).
    fresh = new_tokens[pid1]
    assert fresh is not None
    assert await outbox.complete(pid1, lease_token=fresh, now=when) is True
    entry = await outbox.get(pid1)
    assert entry is not None and entry.lease_token is None and entry.attempts == 0


# --- atomic proposal create + pointer ------------------------------------------------


async def test_proposal_create_records_pointer_atomically(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = await _create_proposal(store, outbox)
    entry = await outbox.get(pid)
    assert entry is not None
    assert entry.status_hint is PatchOutboxStatus.generating
    assert entry.org_id == "org-a" and entry.scope_id == _SCOPE


class _FailingRecordOutbox(PostgresPatchProposalOutbox):
    async def record_in_connection(self, *a: object, **k: object) -> None:  # type: ignore[override]
        raise RuntimeError("pointer store down")


async def test_proposal_create_rolls_back_on_pointer_failure(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = _FailingRecordOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    with pytest.raises(RuntimeError, match="pointer store down"):
        await store.create(
            proposal_id=pid,
            org_id="org-a",
            project_id="proj-a",
            run_id="run-x",
            run_attempt=1,
            agent_id="patch",
            actor="u",
            base_ref="main",
            source_ref="",
            task_digest="d",
            idempotency_key=f"idem-{uuid.uuid4().hex}",
            fingerprint="fp",
            expires_at=_exp(),
            outbox=outbox,
            scope_id=_SCOPE,
        )
    # The proposal INSERT is rolled back with the failed pointer write (both or neither).
    assert await store.get("org-a", pid) is None


# --- ready -> approval_pending single transaction ------------------------------------


async def _run_scope_count(engine: AsyncEngine, scope_id: str, run_id: str) -> int:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        count = await conn.scalar(
            text("SELECT count(*) FROM approvals WHERE scope_id = :scope AND run_id = :run"),
            {"scope": scope_id, "run": run_id},
        )
    return int(count or 0)


async def test_ready_to_approval_pending_single_transaction(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    pid = await _create_proposal(store, outbox, run="run-1")
    ready_v = await _make_ready(store, "org-a", pid)

    proposal, approval_id, created = await store.transition_to_approval_pending(
        "org-a",
        pid,
        approvals=approvals,
        outbox=outbox,
        scope_id=_SCOPE,
        draft=_draft(run="run-1"),
        expected_version=ready_v,
    )
    assert created is True
    assert proposal.status is PatchStatus.approval_pending
    assert proposal.version == ready_v + 1
    assert proposal.approval_id == approval_id
    # The durable approval exists (pending) and the dispatch pointer was deleted in the same txn.
    record = await approvals.get(approval_id)
    assert record is not None and record.status == "pending"
    assert await outbox.get(pid) is None


class _FailingDeleteOutbox(PostgresPatchProposalOutbox):
    async def delete_in_connection(self, conn: AsyncConnection | None, proposal_id: str) -> None:  # type: ignore[override]
        raise RuntimeError("pointer delete down")


async def test_failure_rolls_back_with_no_residual_approval(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    good_outbox = PostgresPatchProposalOutbox(migrated_db)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    pid = await _create_proposal(store, good_outbox, run="run-2")
    ready_v = await _make_ready(store, "org-a", pid)

    failing = _FailingDeleteOutbox(migrated_db)
    with pytest.raises(RuntimeError, match="pointer delete down"):
        await store.transition_to_approval_pending(
            "org-a",
            pid,
            approvals=approvals,
            outbox=failing,
            scope_id=_SCOPE,
            draft=_draft(run="run-2"),
            expected_version=ready_v,
        )
    # The single engine.begin() rolled back entirely: proposal still ready, NO residual approval
    # row, and the pointer survives.
    proposal = await store.get("org-a", pid)
    assert proposal is not None and proposal.status is PatchStatus.ready
    assert proposal.version == ready_v
    assert await _run_scope_count(migrated_db, _SCOPE, "run-2") == 0
    assert await good_outbox.get(pid) is not None


async def test_concurrent_transition_returns_same_approval(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    pid = await _create_proposal(store, outbox, run="run-3")
    ready_v = await _make_ready(store, "org-a", pid)

    async def _attempt() -> tuple[str, bool]:
        _, approval_id, created = await store.transition_to_approval_pending(
            "org-a",
            pid,
            approvals=approvals,
            outbox=outbox,
            scope_id=_SCOPE,
            draft=_draft(run="run-3"),
        )
        return approval_id, created

    results = await asyncio.gather(_attempt(), _attempt())
    approval_ids = {aid for aid, _ in results}
    assert len(approval_ids) == 1  # both serialized callers resolve the same approval
    assert sum(1 for _, created in results if created) == 1  # exactly one created it
    final = await store.get("org-a", pid)
    assert final is not None
    assert final.status is PatchStatus.approval_pending
    assert final.version == ready_v + 1  # the FOR UPDATE lock => a single version bump
    assert await _run_scope_count(migrated_db, _SCOPE, "run-3") == 1


# --- cascade erasure -----------------------------------------------------------------


async def test_project_erasure_cascades_pointer(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = await _create_proposal(store, outbox)
    assert await outbox.get(pid) is not None

    # Deleting the project cascades: patch_proposals -> patch_proposal_outbox, so a purge never
    # leaves an orphaned pointer.
    async with migrated_db.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(text("DELETE FROM projects WHERE id = 'proj-a'"))
    assert await outbox.get(pid) is None


async def _table_names(engine: AsyncEngine) -> set[str]:
    async with engine.begin() as conn:
        rows = (
            await conn.execute(
                text("SELECT tablename FROM pg_tables WHERE tablename LIKE 'patch_%'")
            )
        ).all()
    return {r.tablename for r in rows}
