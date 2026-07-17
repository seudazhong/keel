"""In-memory ProjectStore invariants: uniqueness, idempotency, cross-org binding (M3.7)."""

from __future__ import annotations

import pytest

from keel_core.projects import (
    InMemoryProjectStore,
    InstallationStatus,
    ProjectConflictError,
    ProjectSource,
    ProjectVisibility,
    StorageBackend,
    SyncKind,
    SyncStatus,
    WebhookStatus,
)


async def _make(store: InMemoryProjectStore, org: str, slug: str = "proj"):
    return await store.create_project(
        org_id=org,
        slug=slug,
        display_name="P",
        source=ProjectSource.blank,
        visibility=ProjectVisibility.private,
        default_branch="main",
        active_git_handle=None,
        storage_backend=StorageBackend.local,
    )


async def test_run_association_conflict() -> None:
    store = InMemoryProjectStore()
    p1 = await _make(store, "org_1", "a")
    p2 = await _make(store, "org_1", "b")
    _, created = await store.associate_run(org_id="org_1", project_id=p1.id, run_id="r1")
    assert created
    with pytest.raises(ProjectConflictError):
        await store.associate_run(org_id="org_1", project_id=p2.id, run_id="r1")


async def test_sync_ledger_delivery_idempotent() -> None:
    store = InMemoryProjectStore()
    p = await _make(store, "org_1")
    first, c1 = await store.append_sync(
        org_id="org_1",
        project_id=p.id,
        kind=SyncKind.webhook_push,
        status=SyncStatus.pending,
        delivery_id="d1",
    )
    second, c2 = await store.append_sync(
        org_id="org_1",
        project_id=p.id,
        kind=SyncKind.webhook_push,
        status=SyncStatus.pending,
        delivery_id="d1",
    )
    assert c1 is True and c2 is False and first.id == second.id


async def test_installation_cross_org_binding_rejected() -> None:
    store = InMemoryProjectStore()
    await store.upsert_installation(
        org_id="org_1", installation_id=100, app_id=1, account_login="a", account_type="Org"
    )
    with pytest.raises(ProjectConflictError):
        await store.upsert_installation(
            org_id="org_2", installation_id=100, app_id=1, account_login="a", account_type="Org"
        )
    binding = await store.get_installation_binding(100)
    assert binding is not None and binding.org_id == "org_1"


async def test_installation_rebind_after_delete() -> None:
    store = InMemoryProjectStore()
    await store.upsert_installation(
        org_id="org_1", installation_id=100, app_id=1, account_login="a", account_type="Org"
    )
    await store.set_installation_status(100, InstallationStatus.deleted)
    # After deletion the installation id can bind to a different org.
    rebound = await store.upsert_installation(
        org_id="org_2", installation_id=100, app_id=1, account_login="a", account_type="Org"
    )
    assert rebound.org_id == "org_2"


async def test_webhook_delivery_replay() -> None:
    store = InMemoryProjectStore()
    _, created = await store.record_delivery(
        delivery_id="dd", event="push", installation_id=1, action=None
    )
    _, created2 = await store.record_delivery(
        delivery_id="dd", event="push", installation_id=1, action=None
    )
    assert created is True and created2 is False
    marked = await store.mark_delivery("dd", WebhookStatus.processed)
    assert marked is not None and marked.status is WebhookStatus.processed


async def test_worktree_unique_per_run() -> None:
    store = InMemoryProjectStore()
    p = await _make(store, "org_1")
    w1 = await store.create_worktree(
        org_id="org_1",
        project_id=p.id,
        run_id="r1",
        coding_run_id="c1",
        git_ref="HEAD",
        commit_sha=None,
        handle_path="/x",
    )
    w2 = await store.create_worktree(
        org_id="org_1",
        project_id=p.id,
        run_id="r1",
        coding_run_id="c1",
        git_ref="HEAD",
        commit_sha=None,
        handle_path="/x",
    )
    assert w1.id == w2.id  # active idempotent
    assert await store.count_active_worktrees("org_1") == 1
