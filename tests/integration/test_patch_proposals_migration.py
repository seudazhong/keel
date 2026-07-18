"""Live-Postgres verification of migration 0019 (patch proposals) + store invariants (WS-PP).

Exercises the real migration + durable store against a live database: the two tables exist at head
with FORCE RLS, the composite ``(project_id, org_id)`` FK rejects a cross-org project binding, RLS
isolates one org's proposals from another, the optimistic ``version`` fence + legal-transition
guard hold, the partial unique index reserves a dedicated branch, org/project erasure cascades the
proposal + ledger rows, and downgrade/upgrade proves reversibility.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.patch.ledger import PostgresPatchWritebackLedger
from keel_core.patch.models import PatchStatus, TestStatus
from keel_core.patch.store import (
    PostgresPatchProposalStore,
    ProposalConflictError,
    StaleProposalVersion,
)

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_HEAD = "0019_patch_proposals"
_PREV = "0018_im_routing"
_PATCH_TABLES = {"patch_proposals", "patch_writeback_ledger"}


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


async def test_migration_0019_up_down_up_and_store_invariants(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    # 1) Tables exist at head with FORCE RLS.
    async with engine.begin() as conn:
        force = {
            r.relname: r.relforcerowsecurity
            for r in (
                await conn.execute(
                    text(
                        "SELECT relname, relforcerowsecurity FROM pg_class WHERE relname = ANY(:n)"
                    ),
                    {"n": list(_PATCH_TABLES)},
                )
            ).all()
        }
    assert force["patch_proposals"] is True
    assert force["patch_writeback_ledger"] is True

    await _seed_org_project(engine, "org-a", "proj-a")
    await _seed_org_project(engine, "org-b", "proj-b")
    store = PostgresPatchProposalStore(engine)

    # 2) Create + idempotency by (org, idempotency_key).
    pid = f"pp_{uuid.uuid4().hex}"
    proposal, created = await store.create(
        proposal_id=pid,
        org_id="org-a",
        project_id="proj-a",
        run_id="run-a",
        run_attempt=1,
        agent_id="patch",
        actor="u",
        base_ref="main",
        source_ref="",
        task_digest="d",
        idempotency_key="idem-1",
        fingerprint="fp",
        expires_at=_exp(),
    )
    assert created and proposal.status is PatchStatus.generating
    _, again = await store.create(
        proposal_id=f"pp_{uuid.uuid4().hex}",
        org_id="org-a",
        project_id="proj-a",
        run_id="run-a",
        run_attempt=1,
        agent_id="patch",
        actor="u",
        base_ref="main",
        source_ref="",
        task_digest="d",
        idempotency_key="idem-1",
        fingerprint="fp",
        expires_at=_exp(),
    )
    assert not again

    # 3) Composite FK rejects a cross-org project binding.
    with pytest.raises(ProposalConflictError):
        await store.create(
            proposal_id=f"pp_{uuid.uuid4().hex}",
            org_id="org-a",
            project_id="proj-b",  # proj-b is org-b's
            run_id="x",
            run_attempt=1,
            agent_id="patch",
            actor="u",
            base_ref="main",
            source_ref="",
            task_digest="d",
            idempotency_key="idem-x",
            fingerprint="fp",
            expires_at=_exp(),
        )

    # 4) Optimistic fence + legal transition.
    ready = await store.transition(
        "org-a",
        pid,
        PatchStatus.ready,
        expected_version=1,
        updates={
            "bundle_sha256": "e" * 64,
            "base_sha": "b" * 40,
            "head_sha": "c" * 40,
            "changed_files": 2,
            "test_status": TestStatus.passed,
            "remote_branch": "keel/patch/" + pid,
        },
    )
    assert ready.status is PatchStatus.ready and ready.version == 2 and ready.ready_at is not None
    with pytest.raises(StaleProposalVersion):
        await store.transition("org-a", pid, PatchStatus.approval_pending, expected_version=1)

    # 5) RLS isolates org-b from org-a's proposal.
    assert await store.get("org-b", pid) is None
    assert await store.get("org-a", pid) is not None

    # 6) Writeback ledger append + list + cascade on project delete.
    ledger = PostgresPatchWritebackLedger(engine)
    await ledger.record(
        proposal_id=pid,
        org_id="org-a",
        project_id="proj-a",
        step="verify_base",
        status="succeeded",
        before_sha="b" * 40,
        after_sha="b" * 40,
    )
    entries = await ledger.list_for_proposal("org-a", pid)
    assert len(entries) == 1 and entries[0].step == "verify_base"

    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(text("DELETE FROM projects WHERE id = 'proj-a'"))
        remaining = await conn.scalar(
            text("SELECT count(*) FROM patch_proposals WHERE id = :id"), {"id": pid}
        )
        led = await conn.scalar(
            text("SELECT count(*) FROM patch_writeback_ledger WHERE proposal_id = :id"), {"id": pid}
        )
    assert remaining == 0 and led == 0  # cascade from projects removed both

    # 7) Reversibility.
    url = _sync_url()
    await asyncio.to_thread(_run_to, url, _PREV)
    assert _PATCH_TABLES & await _table_names(engine) == set()
    await asyncio.to_thread(_run_to, url, "head")
    assert _PATCH_TABLES <= await _table_names(engine)


async def test_branch_collision_partial_unique_index(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    await _seed_org_project(engine, "org-a", "proj-a")
    store = PostgresPatchProposalStore(engine)
    branch = "keel/patch/shared"

    async def _to_writing(idem: str, run: str) -> str:
        pid = f"pp_{uuid.uuid4().hex}"
        await store.create(
            proposal_id=pid,
            org_id="org-a",
            project_id="proj-a",
            run_id=run,
            run_attempt=1,
            agent_id="patch",
            actor="u",
            base_ref="main",
            source_ref="",
            task_digest="d",
            idempotency_key=idem,
            fingerprint="fp",
            expires_at=_exp(),
        )
        p = await store.transition(
            "org-a", pid, PatchStatus.ready, expected_version=1, updates={"bundle_sha256": "e" * 64}
        )
        p = await store.transition(
            "org-a", pid, PatchStatus.approval_pending, expected_version=p.version
        )
        p = await store.transition("org-a", pid, PatchStatus.approved, expected_version=p.version)
        await store.transition(
            "org-a",
            pid,
            PatchStatus.writing,
            expected_version=p.version,
            updates={"remote_branch": branch},
        )
        return pid

    await _to_writing("i1", "r1")
    with pytest.raises(ProposalConflictError):
        await _to_writing("i2", "r2")  # same live branch reserved -> collision


async def _table_names(engine: AsyncEngine) -> set[str]:
    async with engine.begin() as conn:
        rows = (
            await conn.execute(
                text("SELECT tablename FROM pg_tables WHERE tablename LIKE 'patch_%'")
            )
        ).all()
    return {r.tablename for r in rows}
