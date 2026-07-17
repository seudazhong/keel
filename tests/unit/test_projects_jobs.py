"""Durable project-sync job handler: payload validation + idempotent sync (M3.7)."""

from __future__ import annotations

import pytest

from keel_core.identity.service import IdentityService
from keel_core.identity.store import InMemoryIdentityStore
from keel_core.jobs import PermanentJobError
from keel_core.projects import (
    InMemoryProjectStorage,
    InMemoryProjectStore,
    ProjectService,
)
from keel_core.projects.jobs import (
    PROJECT_SYNC_KIND,
    ProjectSyncJobHandlers,
    ProjectSyncPayload,
    sync_idempotency_key,
)


class _Ctx:
    scope_id = "scope"
    job_id = "job1"

    async def checkpoint(self) -> None:
        return None


class _FakeGitHub:
    allowed_hosts = frozenset({"github.com"})

    async def resolve_repository(self, installation_id: int, full_name: str) -> dict[str, object]:
        return {
            "id": 1,
            "clone_url": "https://github.com/a/b.git",
            "default_branch": "main",
            "private": True,
        }

    def safe_clone_url(self, clone_url: str, full_name: str) -> str:
        return clone_url


def test_idempotency_key() -> None:
    assert sync_idempotency_key("p1", "d1") == "projects.sync:p1:d1"
    assert sync_idempotency_key("p1", None) == "projects.sync:p1:manual"
    assert PROJECT_SYNC_KIND == "projects.sync"


async def test_handler_rejects_bad_payload() -> None:
    identity = InMemoryIdentityStore()
    svc = ProjectService(InMemoryProjectStore(), identity)
    handler = ProjectSyncJobHandlers(svc)
    with pytest.raises(PermanentJobError):
        await handler.sync(_Ctx(), {"unexpected": "shape"})


async def test_handler_syncs_project() -> None:
    identity = InMemoryIdentityStore()
    isvc = IdentityService(identity)
    admin = await identity.create_user(display_name="A", email="a@x.io")
    ctx = await isvc.create_org(admin.id, slug="acme-co", display_name="Acme")
    store = InMemoryProjectStore()
    storage = InMemoryProjectStorage()
    svc = ProjectService(store, identity, storage=storage)
    svc._github = _FakeGitHub()  # type: ignore[assignment]
    await store.upsert_installation(
        org_id=ctx.org_id, installation_id=1, app_id=1, account_login="a", account_type="Org"
    )
    project = await svc.import_github_project(
        ctx.org_id,
        admin.id,
        slug="job-proj",
        display_name="J",
        installation_id=1,
        repo_full_name="a/b",
    )
    handler = ProjectSyncJobHandlers(svc)
    payload = ProjectSyncPayload(
        org_id=ctx.org_id, project_id=project.id, delivery_id="deliv-9"
    ).model_dump()
    result = await handler.sync(_Ctx(), payload)
    assert result.data["status"] == "succeeded"
