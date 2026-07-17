"""Managed-project service: authorization, quotas, worktrees, import/sync, webhooks (M3.7)."""

from __future__ import annotations

import pytest

from keel_core.errors import PermissionDenied
from keel_core.identity.models import AgentKind, Capability, MembershipRole
from keel_core.identity.service import IdentityService
from keel_core.identity.store import InMemoryIdentityStore
from keel_core.projects import (
    InMemoryProjectAuditSink,
    InMemoryProjectStorage,
    InMemoryProjectStore,
    ProjectConflictError,
    ProjectNotFoundError,
    ProjectOptimisticConcurrencyError,
    ProjectQuota,
    ProjectQuotaExceededError,
    ProjectService,
    ProjectSource,
    ProjectStatus,
    ProjectValidationError,
    WebhookStatus,
)
from keel_core.projects.github.webhooks import parse_event


class _Env:
    def __init__(self) -> None:
        self.identity = InMemoryIdentityStore()
        self.isvc = IdentityService(self.identity)
        self.store = InMemoryProjectStore()
        self.storage = InMemoryProjectStorage()
        self.audit = InMemoryProjectAuditSink()
        self.enqueued: list[tuple[str, str, str | None]] = []

        async def _enqueue(org_id: str, project_id: str, delivery_id: str | None) -> None:
            self.enqueued.append((org_id, project_id, delivery_id))

        self.svc = ProjectService(
            self.store,
            self.identity,
            storage=self.storage,
            audit=self.audit,
            enqueue_sync=_enqueue,
        )


async def _bootstrap() -> tuple[_Env, str, str, str]:
    env = _Env()
    admin = await env.identity.create_user(display_name="Admin", email="admin@x.io")
    member = await env.identity.create_user(display_name="Member", email="member@x.io")
    ctx = await env.isvc.create_org(admin.id, slug="acme-co", display_name="Acme")
    await env.isvc.add_member(ctx.org_id, admin.id, member.id, MembershipRole.member)
    return env, ctx.org_id, admin.id, member.id


async def test_member_cannot_create_but_admin_can() -> None:
    env, org, admin, member = await _bootstrap()
    with pytest.raises(PermissionDenied):
        await env.svc.create_project(org, member, slug="proj-x", display_name="X")
    project = await env.svc.create_project(org, admin, slug="proj-x", display_name="X")
    assert project.source is ProjectSource.blank
    assert project.active_git_handle == project.id
    assert env.storage.projects.get(project.id) is None  # blank repo created


async def test_slug_uniqueness_and_validation() -> None:
    env, org, admin, _ = await _bootstrap()
    await env.svc.create_project(org, admin, slug="dup-slug", display_name="A")
    with pytest.raises(ProjectConflictError):
        await env.svc.create_project(org, admin, slug="dup-slug", display_name="B")
    with pytest.raises(ProjectValidationError):
        await env.svc.create_project(org, admin, slug="Bad Slug!", display_name="C")


async def test_project_quota_enforced() -> None:
    env, org, admin, _ = await _bootstrap()
    await env.store.set_quota(ProjectQuota(org_id=org, max_projects=1))
    await env.svc.create_project(org, admin, slug="only-one", display_name="One")
    with pytest.raises(ProjectQuotaExceededError):
        await env.svc.create_project(org, admin, slug="second-one", display_name="Two")


async def test_optimistic_concurrency_update() -> None:
    env, org, admin, _ = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="opt-proj", display_name="P")
    with pytest.raises(ProjectOptimisticConcurrencyError):
        await env.svc.update_project(
            org, admin, project.id, expected_version=999, display_name="New"
        )
    updated = await env.svc.update_project(
        org, admin, project.id, expected_version=project.version, display_name="New Name"
    )
    assert updated.display_name == "New Name"
    assert updated.version == project.version + 1


async def test_cross_org_isolation() -> None:
    env, org, admin, _ = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="secret", display_name="S")
    other = await env.identity.create_user(display_name="Other", email="o@x.io")
    octx = await env.isvc.create_org(other.id, slug="other-co", display_name="Other")
    with pytest.raises(ProjectNotFoundError):
        await env.svc.get_project(octx.org_id, other.id, project.id)


async def test_worktree_and_run_scope_use_capability() -> None:
    env, org, admin, member = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="wt-proj", display_name="W")
    # member (use) can materialize + associate.
    wt = await env.svc.materialize_worktree(org, member, project.id, "run_1")
    assert wt.is_active and wt.handle_path
    # idempotent second call returns the same active worktree.
    again = await env.svc.materialize_worktree(org, member, project.id, "run_1")
    assert again.id == wt.id
    run_id, created = await env.svc.associate_run(org, member, project.id, "run_1")
    assert run_id == "run_1" and created is True
    _, created2 = await env.svc.associate_run(org, member, project.id, "run_1")
    assert created2 is False
    reclaimed = await env.svc.reclaim_worktree(org, member, project.id, "run_1")
    assert reclaimed.status.value == "reclaimed"


async def test_run_cannot_join_two_projects() -> None:
    env, org, admin, _ = await _bootstrap()
    p1 = await env.svc.create_project(org, admin, slug="proj-a", display_name="A")
    p2 = await env.svc.create_project(org, admin, slug="proj-b", display_name="B")
    await env.svc.associate_run(org, admin, p1.id, "run_shared")
    with pytest.raises(ProjectConflictError):
        await env.svc.associate_run(org, admin, p2.id, "run_shared")


async def test_worktree_quota_enforced() -> None:
    env, org, admin, _ = await _bootstrap()
    await env.store.set_quota(ProjectQuota(org_id=org, max_active_worktrees=1))
    project = await env.svc.create_project(org, admin, slug="wq-proj", display_name="Q")
    await env.svc.materialize_worktree(org, admin, project.id, "run_a")
    with pytest.raises(ProjectQuotaExceededError):
        await env.svc.materialize_worktree(org, admin, project.id, "run_b")


async def test_agent_grant_capability_check() -> None:
    env, org, admin, member = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="agent-proj", display_name="AP")
    agent = await env.isvc.create_agent(org, member, kind=AgentKind.personal, name="Bot")
    # Without a grant, the agent cannot materialize (no active grant on the resource).
    with pytest.raises(PermissionDenied):
        await env.svc.materialize_worktree(org, member, project.id, "run_g", agent_id=agent.id)
    await env.svc.grant_project(
        org, admin, project_id=project.id, agent_id=agent.id, capability=Capability.use
    )
    wt = await env.svc.materialize_worktree(org, member, project.id, "run_g", agent_id=agent.id)
    assert wt.is_active
    grants = await env.svc.list_project_grants(org, admin, project.id)
    assert len(grants) == 1 and grants[0].resource_id == project.id


async def test_member_cannot_grant() -> None:
    env, org, admin, member = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="g-proj", display_name="G")
    agent = await env.isvc.create_agent(org, member, kind=AgentKind.personal, name="Bot")
    with pytest.raises(PermissionDenied):
        await env.svc.grant_project(
            org, member, project_id=project.id, agent_id=agent.id, capability=Capability.use
        )


async def test_archive_delete_purge() -> None:
    env, org, admin, member = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="lifecycle", display_name="L")
    # member cannot archive (needs manage).
    with pytest.raises(PermissionDenied):
        await env.svc.archive_project(org, member, project.id, expected_version=project.version)
    archived = await env.svc.archive_project(
        org, admin, project.id, expected_version=project.version
    )
    assert archived.status is ProjectStatus.archived
    deleted = await env.svc.delete_project(
        org, admin, project.id, expected_version=archived.version
    )
    assert deleted.status is ProjectStatus.deleted
    assert await env.store.get_project(org, project.id) is None
    # lifecycle purge (no actor) removes the row entirely.
    assert await env.svc.purge_project(org, project.id) is True


async def test_purge_reclaims_worktrees() -> None:
    env, org, admin, _ = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="purge-wt", display_name="P")
    await env.svc.materialize_worktree(org, admin, project.id, "run_p")
    assert (env.storage.worktrees) != {}
    await env.svc.purge_project(org, project.id)
    assert env.storage.worktrees == {}


def _github_env_repo_payload() -> dict[str, object]:
    return {
        "id": 424242,
        "clone_url": "https://github.com/acme/repo.git",
        "default_branch": "main",
        "private": True,
    }


class _FakeGitHub:
    """A GitHubIntegration-compatible stub with no network."""

    def __init__(self, allowed_hosts: frozenset[str]) -> None:
        self.allowed_hosts = allowed_hosts
        self.token_calls = 0

    async def resolve_repository(self, installation_id: int, full_name: str) -> dict[str, object]:
        self.token_calls += 1
        return _github_env_repo_payload()

    def safe_clone_url(self, clone_url: str, full_name: str) -> str:
        from keel_core.projects.github.urls import normalize_clone_url

        return normalize_clone_url(
            clone_url, allowed_hosts=self.allowed_hosts, repo_full_name=full_name
        )


async def _github_env() -> tuple[_Env, str, str]:
    env, org, admin, _ = await _bootstrap()
    env.svc._github = _FakeGitHub(frozenset({"github.com"}))  # type: ignore[assignment]
    await env.store.upsert_installation(
        org_id=org, installation_id=777, app_id=1, account_login="acme", account_type="Organization"
    )
    return env, org, admin


async def test_github_import_success_and_atomicity() -> None:
    env, org, admin = await _github_env()
    project = await env.svc.import_github_project(
        org,
        admin,
        slug="imp-proj",
        display_name="Imported",
        installation_id=777,
        repo_full_name="acme/repo",
    )
    assert project.source is ProjectSource.github
    assert project.github_repository_id == 424242
    entries = await env.store.list_sync_entries(org, project.id)
    assert entries and entries[0].status.value == "succeeded"

    # Failure path: storage import raises -> no active project remains, ledger failed.
    env.storage.fail_import = True
    with pytest.raises(RuntimeError):
        await env.svc.import_github_project(
            org,
            admin,
            slug="bad-imp",
            display_name="Bad",
            installation_id=777,
            repo_full_name="acme/repo",
        )
    assert await env.store.get_project_by_slug(org, "bad-imp") is None


async def test_import_requires_installation() -> None:
    env, org, admin, _ = await _bootstrap()
    env.svc._github = _FakeGitHub(frozenset({"github.com"}))  # type: ignore[assignment]
    with pytest.raises(ProjectNotFoundError):
        await env.svc.import_github_project(
            org,
            admin,
            slug="no-inst",
            display_name="N",
            installation_id=999,
            repo_full_name="acme/repo",
        )


async def test_sync_idempotent_by_delivery() -> None:
    env, org, admin = await _github_env()
    project = await env.svc.import_github_project(
        org,
        admin,
        slug="sync-proj",
        display_name="S",
        installation_id=777,
        repo_full_name="acme/repo",
    )
    first = await env.svc.sync_project(org, project.id, delivery_id="deliv-1")
    assert first is not None and first.status.value == "succeeded"
    # Replayed delivery collapses to the same ledger row (no new pending fetch).
    again = await env.svc.sync_project(org, project.id, delivery_id="deliv-1")
    assert again is not None and again.id == first.id


async def test_webhook_installation_binding_and_cross_installation() -> None:
    env, org, admin = await _github_env()
    project = await env.svc.import_github_project(
        org,
        admin,
        slug="hook-proj",
        display_name="H",
        installation_id=777,
        repo_full_name="acme/repo",
    )
    # push for the correct installation + linked repo -> processed + enqueued.
    payload = {
        "ref": "refs/heads/main",
        "before": "aaa",
        "after": "bbb",
        "installation": {"id": 777},
        "repository": {"id": 424242, "default_branch": "main"},
    }
    outcome = await env.svc.process_webhook(
        parse_event(event="push", delivery="d1", payload=payload)
    )
    assert outcome.status is WebhookStatus.processed
    assert project.id in outcome.project_ids
    assert env.enqueued and env.enqueued[-1][2] == "d1"

    # unknown installation -> skipped (no binding).
    payload_unknown = dict(payload, installation={"id": 555555})
    out2 = await env.svc.process_webhook(
        parse_event(event="push", delivery="d2", payload=payload_unknown)
    )
    assert out2.status is WebhookStatus.skipped

    # cross-installation repo id (repo not in this org) -> processed but no project matched.
    payload_foreign = dict(payload, repository={"id": 111, "default_branch": "main"})
    out3 = await env.svc.process_webhook(
        parse_event(event="push", delivery="d3", payload=payload_foreign)
    )
    assert out3.project_ids == ()


async def test_webhook_installation_suspend() -> None:
    env, org, admin = await _github_env()
    payload = {"action": "suspend", "installation": {"id": 777}}
    out = await env.svc.process_webhook(
        parse_event(event="installation", delivery="di", payload=payload)
    )
    assert out.status is WebhookStatus.processed
    binding = await env.store.get_installation_binding(777)
    assert binding is not None and binding.status.value == "suspended"


async def test_sync_rejects_blank_project() -> None:
    env, org, admin, _ = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="blank-proj", display_name="B")
    with pytest.raises(ProjectValidationError):
        await env.svc.request_sync(org, admin, project.id)
