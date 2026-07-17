"""Durable managed projects: Postgres store, RLS isolation, cross-org FK denial (M3.7).

Requires a live ``keel_test`` Postgres (``KEEL_TEST_DATABASE_URL``). The two-org RLS test is
skipped when the non-owner ``keel_runtime`` role is not provisioned (managed Postgres that
forbids CREATE ROLE).
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.identity import MembershipRole, PostgresIdentityStore
from keel_core.projects import (
    PostgresProjectStore,
    ProjectConflictError,
    ProjectSource,
    ProjectVisibility,
    StorageBackend,
    SyncKind,
    SyncStatus,
    WebhookStatus,
)

pytestmark = pytest.mark.integration

_PROJECT_TABLES = (
    "github_webhook_deliveries",
    "github_sync_state",
    "github_repositories",
    "github_installations",
    "repo_sync_ledger",
    "project_runs",
    "project_worktrees",
    "project_quotas",
    "projects",
)


async def _clean(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        for table in _PROJECT_TABLES:
            await conn.execute(text(f"DELETE FROM {table}"))


async def _seed_org(identity: PostgresIdentityStore, slug: str) -> tuple[str, str]:
    owner = await identity.create_user(display_name="Owner", email=f"owner-{slug}@x.com")
    org = await identity.create_org(slug=slug, display_name=slug.title())
    await identity.create_membership(org_id=org.id, user_id=owner.id, role=MembershipRole.owner)
    return org.id, owner.id


async def _make_project(store: PostgresProjectStore, org_id: str, slug: str):
    return await store.create_project(
        org_id=org_id,
        slug=slug,
        display_name="P",
        source=ProjectSource.blank,
        visibility=ProjectVisibility.private,
        default_branch="main",
        active_git_handle=None,
        storage_backend=StorageBackend.local,
    )


async def _runtime_role_available(engine: AsyncEngine) -> bool:
    async with engine.connect() as conn:
        found = await conn.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime'"))
    return bool(found)


async def test_store_roundtrip(migrated_db: AsyncEngine) -> None:
    await _clean(migrated_db)
    identity = PostgresIdentityStore(migrated_db)
    store = PostgresProjectStore(migrated_db)
    org_id, _ = await _seed_org(identity, "proj-a")
    project = await _make_project(store, org_id, "roundtrip")
    assert (await store.get_project(org_id, project.id)).slug == "roundtrip"  # type: ignore[union-attr]

    # worktree + run + sync ledger round-trip.
    wt = await store.create_worktree(
        org_id=org_id,
        project_id=project.id,
        run_id="run_1",
        coding_run_id="c1",
        git_ref="HEAD",
        commit_sha=None,
        handle_path="/w",
    )
    assert (await store.get_worktree(org_id, project.id, "run_1")).id == wt.id  # type: ignore[union-attr]
    _, created = await store.associate_run(org_id=org_id, project_id=project.id, run_id="run_1")
    assert created
    entry, made = await store.append_sync(
        org_id=org_id, project_id=project.id, kind=SyncKind.fetch, status=SyncStatus.pending
    )
    assert made
    done = await store.update_sync_status(
        org_id, entry.id, status=SyncStatus.succeeded, after_sha="abc"
    )
    assert done is not None and done.status is SyncStatus.succeeded


async def test_sync_ledger_delivery_unique(migrated_db: AsyncEngine) -> None:
    await _clean(migrated_db)
    identity = PostgresIdentityStore(migrated_db)
    store = PostgresProjectStore(migrated_db)
    org_id, _ = await _seed_org(identity, "proj-d")
    project = await _make_project(store, org_id, "deliv")
    first, c1 = await store.append_sync(
        org_id=org_id,
        project_id=project.id,
        kind=SyncKind.webhook_push,
        status=SyncStatus.pending,
        delivery_id="dd1",
    )
    second, c2 = await store.append_sync(
        org_id=org_id,
        project_id=project.id,
        kind=SyncKind.webhook_push,
        status=SyncStatus.pending,
        delivery_id="dd1",
    )
    assert c1 and not c2 and first.id == second.id


async def test_webhook_delivery_pk_replay(migrated_db: AsyncEngine) -> None:
    await _clean(migrated_db)
    store = PostgresProjectStore(migrated_db)
    _, created = await store.record_delivery(
        delivery_id="ghd-1", event="push", installation_id=1, action=None
    )
    _, created2 = await store.record_delivery(
        delivery_id="ghd-1", event="push", installation_id=1, action=None
    )
    assert created and not created2
    await store.mark_delivery("ghd-1", WebhookStatus.processed)
    assert (await store.get_delivery("ghd-1")).status is WebhookStatus.processed  # type: ignore[union-attr]


async def test_installation_cross_org_binding_rejected(migrated_db: AsyncEngine) -> None:
    await _clean(migrated_db)
    identity = PostgresIdentityStore(migrated_db)
    store = PostgresProjectStore(migrated_db)
    org_a, _ = await _seed_org(identity, "inst-a")
    org_b, _ = await _seed_org(identity, "inst-b")
    await store.upsert_installation(
        org_id=org_a, installation_id=9001, app_id=1, account_login="a", account_type="Org"
    )
    with pytest.raises(ProjectConflictError):
        await store.upsert_installation(
            org_id=org_b, installation_id=9001, app_id=1, account_login="b", account_type="Org"
        )


async def test_cross_org_worktree_fk_denied(migrated_db: AsyncEngine) -> None:
    """A worktree row claiming a project id from a different org is structurally impossible."""
    await _clean(migrated_db)
    identity = PostgresIdentityStore(migrated_db)
    store = PostgresProjectStore(migrated_db)
    org_a, _ = await _seed_org(identity, "fk-a")
    org_b, _ = await _seed_org(identity, "fk-b")
    project_a = await _make_project(store, org_a, "in-a")
    # Insert a worktree in org_b referencing project_a -> composite FK (project_id, org_id)
    # has no matching (project_a.id, org_b) row, so the insert is rejected.
    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.org_id', :o, true)"), {"o": org_b})
        with pytest.raises((IntegrityError, DBAPIError)):
            await conn.execute(
                text(
                    "INSERT INTO project_worktrees (id, org_id, project_id, run_id, "
                    "coding_run_id, git_ref, handle_path) VALUES "
                    "(:id, :org, :pid, :rid, :crid, 'HEAD', '/w')"
                ),
                {
                    "id": "pwt_test",
                    "org": org_b,
                    "pid": project_a.id,
                    "rid": "r1",
                    "crid": "c1",
                },
            )


async def test_two_org_rls_isolation(migrated_db: AsyncEngine) -> None:
    if not await _runtime_role_available(migrated_db):
        pytest.skip("keel_runtime role not provisioned in this database")
    await _clean(migrated_db)
    identity = PostgresIdentityStore(migrated_db)
    store = PostgresProjectStore(migrated_db)
    org_a, _ = await _seed_org(identity, "rls-a")
    org_b, _ = await _seed_org(identity, "rls-b")
    await _make_project(store, org_a, "a-proj")
    await _make_project(store, org_b, "b-proj")

    async with migrated_db.connect() as conn:
        await conn.execute(text("SET ROLE keel_runtime"))
        await conn.execute(text("SELECT set_config('app.org_id', :o, false)"), {"o": org_a})
        rows_a = (await conn.execute(text("SELECT org_id FROM projects"))).fetchall()
        assert {r[0] for r in rows_a} == {org_a}

        await conn.execute(text("SELECT set_config('app.org_id', :o, false)"), {"o": org_b})
        rows_b = (await conn.execute(text("SELECT org_id FROM projects"))).fetchall()
        assert {r[0] for r in rows_b} == {org_b}  # org A invisible

        await conn.execute(text("SELECT set_config('app.org_id', '', false)"))
        rows_none = (await conn.execute(text("SELECT org_id FROM projects"))).fetchall()
        assert rows_none == []  # deny-by-default with no org selected
        await conn.execute(text("RESET ROLE"))


async def test_org_erasure_purges_projects(migrated_db: AsyncEngine) -> None:
    await _clean(migrated_db)
    identity = PostgresIdentityStore(migrated_db)
    store = PostgresProjectStore(migrated_db)
    org_id, _ = await _seed_org(identity, "erase-p")
    project = await _make_project(store, org_id, "to-erase")
    await store.upsert_installation(
        org_id=org_id, installation_id=4242, app_id=1, account_login="e", account_type="Org"
    )
    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT keel_erase_organization(:o)"), {"o": org_id})
    # The project + installation rows are gone with the org.
    async with migrated_db.connect() as conn:
        remaining = await conn.scalar(
            text("SELECT count(*) FROM projects WHERE id = :id"), {"id": project.id}
        )
        installs = await conn.scalar(
            text("SELECT count(*) FROM github_installations WHERE org_id = :o"), {"o": org_id}
        )
    assert remaining == 0 and installs == 0
