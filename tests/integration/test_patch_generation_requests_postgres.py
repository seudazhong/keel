"""Live-Postgres verification of the durable, scope-partitioned generation-request payload (P4a).

Exercises the real migration + durable store path against a live database:

* ``0022`` creates the RLS + ``FORCE ROW LEVEL SECURITY`` ``patch_generation_requests`` table keyed
  on the ``app.scope_id`` data-plane GUC, with the payload-object / payload-byte / fingerprint-hex
  CHECKs, the composite ``ON DELETE CASCADE`` FK to ``patch_proposals(id, org_id)`` and minimal DML
  grants to the non-owner runtime + maintenance roles — and downgrades cleanly to ``0021``;
* the least-privilege ``keel_runtime`` LOGIN can read/write the row **only under its own scope GUC**
  (a sibling scope reads nothing) but cannot run DDL against it;
* proposal admission writes the proposal, its scoped request row and the ``generating`` pointer in
  the **same** transaction (commit or roll back together), and ``get_generation_request`` reloads
  the exact immutable record only under the owning scope;
* an idempotent replay verifies the persisted request's identity fingerprint (no re-insert on match,
  a fail-closed :class:`ProposalConflictError` on a diverging or missing payload); and
* erasing the proposal (transitively: project/org) cascades the request payload away.
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

from keel_core.patch.models import PatchProposal, PatchProposalRequest, PatchStatus
from keel_core.patch.outbox import PatchOutboxStatus, PostgresPatchProposalOutbox
from keel_core.patch.payload import PatchGenerationRequestRecord
from keel_core.patch.store import (
    PostgresPatchProposalStore,
    ProposalConflictError,
)
from keel_core.runtime_db import provision_runtime_login

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PREV = "0021_patch_proposal_outbox"
_SCOPE = "agent:org-a/patch"
_OTHER_SCOPE = "agent:org-a/other"
_RUNTIME_LOGIN = "keel_runtime_login_pgr_test"
_RUNTIME_PASSWORD = "runtime-pgr-test-pw"  # noqa: S105 - throwaway local test-role password


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


def _request(
    *,
    pid: str,
    run: str,
    org: str = "org-a",
    idem: str,
    task: str = "apply the requested change",
    scope: str = _SCOPE,
) -> PatchGenerationRequestRecord:
    """Build the immutable request record the way the coordinator does (agent ``patch``)."""
    request = PatchProposalRequest(
        org_id=org,
        project_id="proj-a",
        actor="alice",
        task=task,
        base_ref="main",
        model="m",
        idempotency_key=idem,
        agent_id="patch",
    )
    return PatchGenerationRequestRecord.from_request(
        request, proposal_id=pid, run_id=run, scope_id=scope
    )


async def _create_with_request(
    store: PostgresPatchProposalStore,
    outbox: PostgresPatchProposalOutbox,
    *,
    pid: str,
    idem: str,
    org: str = "org-a",
    project: str = "proj-a",
    run: str = "run-1",
    task: str = "apply the requested change",
    scope: str = _SCOPE,
    generation_request: PatchGenerationRequestRecord | None = None,
) -> tuple[PatchProposal, bool]:
    record = generation_request or _request(
        pid=pid, run=run, org=org, idem=idem, task=task, scope=scope
    )
    return await store.create(
        proposal_id=pid,
        org_id=org,
        project_id=project,
        run_id=run,
        run_attempt=1,
        agent_id="patch",
        actor="alice",
        base_ref="main",
        source_ref="",
        task_digest="d",
        idempotency_key=idem,
        fingerprint="fp",
        expires_at=_exp(),
        outbox=outbox,
        scope_id=scope,
        generation_request=record,
    )


async def _request_count(engine: AsyncEngine, proposal_id: str) -> int:
    """Count request rows for a proposal ignoring RLS (owner + row_security off)."""
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        count = await conn.scalar(
            text("SELECT count(*) FROM patch_generation_requests WHERE proposal_id = :pid"),
            {"pid": proposal_id},
        )
    return int(count or 0)


async def _table_names(engine: AsyncEngine) -> set[str]:
    async with engine.begin() as conn:
        rows = (
            await conn.execute(
                text("SELECT tablename FROM pg_tables WHERE tablename LIKE 'patch_%'")
            )
        ).all()
    return {r.tablename for r in rows}


# --- migration 0022 schema + grants + reversibility ----------------------------------


async def test_migration_0022_schema_grants_and_reversibility(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    async with engine.begin() as conn:
        # 1) Table exists and is under RLS *and* FORCE RLS (it carries the tainted task text).
        rel = (
            await conn.execute(
                text(
                    "SELECT relrowsecurity, relforcerowsecurity FROM pg_class "
                    "WHERE relname = 'patch_generation_requests'"
                )
            )
        ).one()
        assert rel.relrowsecurity is True
        assert rel.relforcerowsecurity is True

        # 2) The scope-isolation policy keys BOTH USING and WITH CHECK on app.scope_id.
        policies = (
            await conn.execute(
                text(
                    "SELECT policyname, qual, with_check FROM pg_policies "
                    "WHERE tablename = 'patch_generation_requests'"
                )
            )
        ).all()
        assert len(policies) == 1
        policy = policies[0]
        assert policy.policyname == "scope_isolation"
        assert "app.scope_id" in (policy.qual or "")
        assert "app.scope_id" in (policy.with_check or "")

        # 3) There is NO cross-scope / global index (the row is loaded by its PK under scope RLS).
        idx = {
            r.indexname
            for r in (
                await conn.execute(
                    text("SELECT indexname FROM pg_indexes WHERE tablename = :t"),
                    {"t": "patch_generation_requests"},
                )
            ).all()
        }
        # Only the primary-key index backs the table.
        assert idx == {"patch_generation_requests_pkey"}

        # 4) Fail-closed CHECKs + composite CASCADE FK are present.
        cons = (
            await conn.execute(
                text(
                    "SELECT conname, pg_get_constraintdef(oid) AS condef FROM pg_constraint "
                    "WHERE conrelid = 'patch_generation_requests'::regclass"
                )
            )
        ).all()
        by_name = {c.conname: c.condef for c in cons}
        assert "jsonb_typeof(payload) = 'object'" in by_name.get(
            "ck_patch_generation_requests_payload_object", ""
        )
        assert "pg_column_size(payload)" in by_name.get(
            "ck_patch_generation_requests_payload_bytes", ""
        )
        assert "[0-9a-f]" in by_name.get("ck_patch_generation_requests_fingerprint", "")
        fk = next((c.condef for c in cons if c.condef.startswith("FOREIGN KEY")), "")
        assert "REFERENCES patch_proposals(id, org_id)" in fk
        assert "ON DELETE CASCADE" in fk

        # 5) Minimal DML granted to the non-owner runtime + maintenance roles; no DDL.
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
                            "WHERE table_name = 'patch_generation_requests' AND grantee = :g"
                        ),
                        {"g": role},
                    )
                ).all()
            }
            assert grants == {"SELECT", "INSERT", "UPDATE", "DELETE"}

    # 6) Reversibility: down to 0021 drops it, back up re-creates it.
    url = _sync_url()
    await asyncio.to_thread(_run_to, url, _PREV)
    assert "patch_generation_requests" not in await _table_names(engine)
    await asyncio.to_thread(_run_to, url, "head")
    assert "patch_generation_requests" in await _table_names(engine)


# --- runtime login: scoped DML allowed, cross-scope hidden, DDL denied ---------------


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


async def _set_scope(conn: AsyncConnection, scope: str) -> None:
    await conn.execute(text("SELECT set_config('app.scope_id', :scope, true)"), {"scope": scope})


async def test_runtime_login_generation_request_dml_scoped_and_ddl_denied(
    migrated_db: AsyncEngine, runtime_login_engine: AsyncEngine
) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    owner_store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    await _create_with_request(owner_store, outbox, pid=pid, idem=f"idem-{uuid.uuid4().hex}")
    # A second proposal admitted WITHOUT a request, so the runtime login can INSERT its request row.
    pid2 = f"pp_{uuid.uuid4().hex}"
    await owner_store.create(
        proposal_id=pid2,
        org_id="org-a",
        project_id="proj-a",
        run_id="run-2",
        run_attempt=1,
        agent_id="patch",
        actor="alice",
        base_ref="main",
        source_ref="",
        task_digest="d",
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        fingerprint="fp",
        expires_at=_exp(),
        outbox=outbox,
        scope_id=_SCOPE,
    )

    # The runtime role cannot write a row it could not read back (a foreign-scope payload): the
    # policy WITH CHECK rejects it even though the FK parent exists and no request row is present.
    async with runtime_login_engine.begin() as conn:
        await _set_scope(conn, _SCOPE)
        with pytest.raises((ProgrammingError, DBAPIError)):
            await conn.execute(
                text(
                    "INSERT INTO patch_generation_requests "
                    "(proposal_id, org_id, scope_id, payload, fingerprint) "
                    "VALUES (:pid, :org, :scope, CAST(:payload AS jsonb), :fp)"
                ),
                {
                    "pid": pid2,
                    "org": "org-a",
                    "scope": _OTHER_SCOPE,
                    "payload": '{"k":"v"}',
                    "fp": "b" * 64,
                },
            )
    # The runtime role CAN INSERT the request under its own scope (grant + WITH CHECK pass).
    async with runtime_login_engine.begin() as conn:
        await _set_scope(conn, _SCOPE)
        ins = await conn.execute(
            text(
                "INSERT INTO patch_generation_requests "
                "(proposal_id, org_id, scope_id, payload, fingerprint) "
                "VALUES (:pid, :org, :scope, CAST(:payload AS jsonb), :fp)"
            ),
            {"pid": pid2, "org": "org-a", "scope": _SCOPE, "payload": '{"k":"v"}', "fp": "a" * 64},
        )
        assert ins.rowcount == 1

    # The non-owner runtime role can read/update/delete the request ONLY under its own scope GUC.
    async with runtime_login_engine.begin() as conn:
        await _set_scope(conn, _SCOPE)
        seen = await conn.scalar(
            text("SELECT count(*) FROM patch_generation_requests WHERE proposal_id = :pid"),
            {"pid": pid},
        )
        assert seen == 1
        upd = await conn.execute(
            text(
                "UPDATE patch_generation_requests SET created_at = now() WHERE proposal_id = :pid"
            ),
            {"pid": pid},
        )
        assert upd.rowcount == 1

    # A sibling scope in the SAME org reads nothing (row-level scope isolation), and its WITH CHECK
    # forbids inserting a row it could not read back.
    async with runtime_login_engine.begin() as conn:
        await _set_scope(conn, _OTHER_SCOPE)
        hidden = await conn.scalar(
            text("SELECT count(*) FROM patch_generation_requests WHERE proposal_id = :pid"),
            {"pid": pid},
        )
        assert hidden == 0
        deleted_foreign = await conn.execute(
            text("DELETE FROM patch_generation_requests WHERE proposal_id = :pid"),
            {"pid": pid},
        )
        assert deleted_foreign.rowcount == 0  # cannot touch another scope's payload

    # The owning scope can delete it.
    async with runtime_login_engine.begin() as conn:
        await _set_scope(conn, _SCOPE)
        deleted = await conn.execute(
            text("DELETE FROM patch_generation_requests WHERE proposal_id = :pid"),
            {"pid": pid},
        )
        assert deleted.rowcount == 1

    # DDL against the table is denied for the least-privilege runtime role.
    for stmt in (
        "CREATE INDEX pgr_probe ON patch_generation_requests (org_id)",
        "ALTER TABLE patch_generation_requests ADD COLUMN probe int",
        "DROP TABLE patch_generation_requests",
    ):
        async with runtime_login_engine.connect() as conn:
            with pytest.raises((ProgrammingError, DBAPIError)):
                await conn.execute(text(stmt))


# --- atomic create + scoped reload ---------------------------------------------------


async def test_atomic_create_writes_proposal_request_and_pointer(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    proposal, created = await _create_with_request(
        store, outbox, pid=pid, idem=f"idem-{uuid.uuid4().hex}", task="do the thing"
    )
    assert created is True
    assert proposal.status is PatchStatus.generating

    # The dispatch pointer committed in the same transaction.
    entry = await outbox.get(pid)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating

    # The scoped request reloads and reconstructs exactly.
    record = await store.get_generation_request("org-a", pid, _SCOPE)
    assert record is not None
    assert record.proposal_id == pid
    assert record.org_id == "org-a"
    assert record.scope_id == _SCOPE
    assert record.payload.task == "do the thing"
    assert record.payload.agent_id == "patch"


async def test_get_generation_request_scope_isolated_under_runtime_login(
    migrated_db: AsyncEngine, runtime_login_engine: AsyncEngine
) -> None:
    # Seed the proposal + request through the owner (superuser bypasses RLS), then reload through
    # the least-privilege runtime login where FORCE ROW LEVEL SECURITY *is* enforced: the store's
    # ``get_generation_request`` returns the row only under its owning scope and reads nothing under
    # a sibling scope, so a worker bound to one Agent can never reload another's tainted request.
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    owner_store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    await _create_with_request(owner_store, outbox, pid=pid, idem=f"idem-{uuid.uuid4().hex}")

    runtime_store = PostgresPatchProposalStore(runtime_login_engine)
    same_scope = await runtime_store.get_generation_request("org-a", pid, _SCOPE)
    assert same_scope is not None and same_scope.scope_id == _SCOPE
    assert await runtime_store.get_generation_request("org-a", pid, _OTHER_SCOPE) is None


async def test_create_rolls_back_when_request_insert_fails(
    migrated_db: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"

    import keel_core.patch.store as store_mod

    async def _boom(conn: AsyncConnection, record: PatchGenerationRequestRecord) -> None:
        raise RuntimeError("request insert down")

    monkeypatch.setattr(store_mod, "insert_generation_request_in_connection", _boom)

    with pytest.raises(RuntimeError, match="request insert down"):
        await _create_with_request(store, outbox, pid=pid, idem=f"idem-{uuid.uuid4().hex}")

    # The single engine.begin() rolled back entirely: NO proposal, NO request row, NO pointer.
    assert await store.get("org-a", pid) is None
    assert await _request_count(migrated_db, pid) == 0
    assert await outbox.get(pid) is None


# --- idempotent replay fingerprint verification --------------------------------------


async def test_idempotent_replay_verifies_request_fingerprint(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    idem = f"idem-{uuid.uuid4().hex}"

    first, created = await _create_with_request(
        store, outbox, pid=pid, idem=idem, run="run-1", task="apply the requested change"
    )
    assert created is True

    # A retry mints a fresh proposal/run id but carries the SAME logical request (same idem + task):
    # the identity fingerprint matches, so the replay is idempotent (created=False, no re-insert).
    replay_pid = f"pp_{uuid.uuid4().hex}"
    replay, replay_created = await _create_with_request(
        store, outbox, pid=replay_pid, idem=idem, run="run-2", task="apply the requested change"
    )
    assert replay_created is False
    assert replay.id == first.id
    assert await _request_count(migrated_db, first.id) == 1
    assert await _request_count(migrated_db, replay_pid) == 0  # never inserted late

    # A DIVERGING task under the same idempotency key is a fail-closed request-binding conflict.
    with pytest.raises(ProposalConflictError):
        await _create_with_request(
            store,
            outbox,
            pid=f"pp_{uuid.uuid4().hex}",
            idem=idem,
            run="run-3",
            task="something completely different",
        )


async def test_replay_missing_stored_request_is_conflict(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    idem = f"idem-{uuid.uuid4().hex}"

    # A legacy/pre-0022 proposal admitted WITHOUT a durable request row.
    _, created = await store.create(
        proposal_id=pid,
        org_id="org-a",
        project_id="proj-a",
        run_id="run-1",
        run_attempt=1,
        agent_id="patch",
        actor="alice",
        base_ref="main",
        source_ref="",
        task_digest="d",
        idempotency_key=idem,
        fingerprint="fp",
        expires_at=_exp(),
        outbox=outbox,
        scope_id=_SCOPE,
    )
    assert created is True
    assert await _request_count(migrated_db, pid) == 0

    # An idempotent replay that now supplies a request must NOT insert it after the fact: with no
    # persisted request to verify against, it fails closed as an unrecoverable conflict.
    with pytest.raises(ProposalConflictError):
        await _create_with_request(
            store, outbox, pid=f"pp_{uuid.uuid4().hex}", idem=idem, run="run-2"
        )
    assert await _request_count(migrated_db, pid) == 0


# --- cascade erasure -----------------------------------------------------------------


async def test_project_erasure_cascades_request(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    store = PostgresPatchProposalStore(migrated_db)
    outbox = PostgresPatchProposalOutbox(migrated_db)
    pid = f"pp_{uuid.uuid4().hex}"
    await _create_with_request(store, outbox, pid=pid, idem=f"idem-{uuid.uuid4().hex}")
    assert await _request_count(migrated_db, pid) == 1

    # Deleting the project cascades: projects -> patch_proposals -> patch_generation_requests, so a
    # purge never leaves an orphaned tainted request payload.
    async with migrated_db.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(text("DELETE FROM projects WHERE id = 'proj-a'"))
    assert await _request_count(migrated_db, pid) == 0
    assert await store.get("org-a", pid) is None
    assert await outbox.get(pid) is None
