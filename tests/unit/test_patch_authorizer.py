"""Unit tests for the concrete :class:`ProjectServicePatchAuthorizer` (WS-PP, M4 P3a-3).

Exercises the authorizer purely over public :class:`ProjectService` seams with in-memory stores:

* the capability matrix (generation/read == ``use``; approval/writeback == ``write``);
* the canonical per-Agent scope, and the reserved default-Agent label resolving to the actor acting
  *directly* (so re-authorization never spuriously requires an Agent named ``patch``);
* the GitHub target — the exact installation bound to ``project.github_repository_id``, re-verified
  for the repo↔project↔installation binding and active state, with the stored clone URL re-pinned;
* not-found vs unauthorized kept distinct without leaking a foreign resource's existence.
"""

from __future__ import annotations

import pytest

from keel_core.errors import PermissionDenied
from keel_core.identity.models import MembershipRole
from keel_core.identity.service import IdentityService
from keel_core.identity.store import InMemoryIdentityStore
from keel_core.patch.authorizer import ProjectServicePatchAuthorizer
from keel_core.patch.coordinator import DEFAULT_PATCH_AGENT_ID
from keel_core.patch.errors import PatchValidationError
from keel_core.projects import (
    InMemoryProjectStorage,
    InMemoryProjectStore,
    ProjectNotFoundError,
    ProjectService,
)
from keel_core.projects.github.urls import UntrustedUrlError, normalize_clone_url
from keel_core.projects.models import (
    InstallationStatus,
    Project,
    ProjectSource,
    ProjectVisibility,
    StorageBackend,
)
from keel_core.scoping import derive_agent_scope

pytestmark = pytest.mark.asyncio

_ALLOWED = frozenset({"github.com"})


class _FakeGitHub:
    """A GitHubIntegration-compatible stub: URL normalization only, no network."""

    def __init__(self, allowed_hosts: frozenset[str]) -> None:
        self.allowed_hosts = allowed_hosts

    def safe_clone_url(self, clone_url: str, full_name: str) -> str:
        return normalize_clone_url(
            clone_url, allowed_hosts=self.allowed_hosts, repo_full_name=full_name
        )


class _Env:
    def __init__(self) -> None:
        self.identity = InMemoryIdentityStore()
        self.isvc = IdentityService(self.identity)
        self.store = InMemoryProjectStore()
        self.storage = InMemoryProjectStorage()
        self.svc = ProjectService(self.store, self.identity, storage=self.storage)
        self.svc._github = _FakeGitHub(_ALLOWED)  # type: ignore[assignment]
        self.authz = ProjectServicePatchAuthorizer(self.svc)


async def _bootstrap() -> tuple[_Env, str, str, str, str]:
    """Return (env, org, admin_writer, member_user, viewer_user)."""
    env = _Env()
    admin = await env.identity.create_user(display_name="Admin", email="admin@x.io")
    member = await env.identity.create_user(display_name="Member", email="member@x.io")
    viewer = await env.identity.create_user(display_name="Viewer", email="viewer@x.io")
    ctx = await env.isvc.create_org(admin.id, slug="acme-co", display_name="Acme")
    await env.isvc.add_member(ctx.org_id, admin.id, member.id, MembershipRole.member)
    await env.isvc.add_member(ctx.org_id, admin.id, viewer.id, MembershipRole.viewer)
    return env, ctx.org_id, admin.id, member.id, viewer.id


async def _bind_github_project(
    env: _Env,
    org: str,
    *,
    repo_id: int = 424242,
    full_name: str = "acme/repo",
    installation_id: int = 777,
    clone_url: str = "https://github.com/acme/repo.git",
    installation_status: InstallationStatus = InstallationStatus.active,
    link: bool = True,
) -> Project:
    """Build a fully GitHub-bound project directly via the store (no network / import)."""
    await env.store.upsert_installation(
        org_id=org,
        installation_id=installation_id,
        app_id=1,
        account_login="acme",
        account_type="Organization",
    )
    if installation_status is not InstallationStatus.active:
        await env.store.set_installation_status(installation_id, installation_status)
    repo = await env.store.upsert_repository(
        org_id=org,
        installation_id=installation_id,
        repo_id=repo_id,
        full_name=full_name,
        default_branch="main",
        is_private=True,
        clone_url=clone_url,
    )
    project = await env.store.create_project(
        org_id=org,
        slug=f"proj-{repo_id}",
        display_name="Proj",
        source=ProjectSource.github,
        visibility=ProjectVisibility.private,
        default_branch="main",
        active_git_handle=None,
        storage_backend=StorageBackend.local,
        github_repository_id=repo_id,
    )
    await env.store.set_active_git_handle(
        org, project.id, handle=project.id, backend=StorageBackend.local
    )
    if link:
        await env.store.link_repository_project(org, repo.id, project.id)
    refreshed = await env.store.get_project(org, project.id)
    assert refreshed is not None
    return refreshed


async def test_authorize_generation_resolves_scope_and_target() -> None:
    env, org, _admin, member, _viewer = await _bootstrap()
    project = await _bind_github_project(env, org)

    binding = await env.authz.authorize_generation(
        org, member, project.id, agent_id=None, run_id="run-1"
    )

    assert binding.scope_id == derive_agent_scope(org, DEFAULT_PATCH_AGENT_ID)
    assert binding.project_handle == project.id
    assert binding.coding_run_id == "run-1"
    assert binding.target is not None
    assert binding.target.installation_id == 777
    assert binding.target.full_name == "acme/repo"
    assert binding.target.default_branch == "main"
    assert binding.target.clone_url == env.svc.safe_clone_url(
        "https://github.com/acme/repo.git", "acme/repo"
    )


async def test_non_github_project_has_no_target() -> None:
    env, org, admin, member, _viewer = await _bootstrap()
    project = await env.svc.create_project(org, admin, slug="blank", display_name="Blank")

    binding = await env.authz.authorize_generation(
        org, member, project.id, agent_id=None, run_id="r"
    )
    assert binding.target is None
    assert binding.project_handle == project.id


async def test_exact_installation_among_multiple_active() -> None:
    env, org, admin, _member, _viewer = await _bootstrap()
    # An unrelated, also-active installation + repo in the same org.
    await _bind_github_project(
        env, org, repo_id=999999, full_name="acme/other", installation_id=888
    )
    project = await _bind_github_project(env, org)  # repo 424242 -> installation 777

    binding = await env.authz.authorize_writeback(org, admin, project.id, agent_id=None, run_id="r")
    assert binding.target is not None
    assert binding.target.installation_id == 777  # exactly this project's repo's installation


async def test_inactive_installation_fails_closed() -> None:
    env, org, admin, _member, _viewer = await _bootstrap()
    project = await _bind_github_project(env, org, installation_status=InstallationStatus.suspended)
    with pytest.raises(PatchValidationError):
        await env.authz.authorize_writeback(org, admin, project.id, agent_id=None, run_id="r")


async def test_mismatched_repo_binding_fails_closed() -> None:
    env, org, admin, _member, _viewer = await _bootstrap()
    await _bind_github_project(env, org)  # repo 424242 bound to project A
    # Project B records the same repo id, but the repo is bound to A — a mismatch must fail closed.
    project_b = await env.store.create_project(
        org_id=org,
        slug="proj-b",
        display_name="B",
        source=ProjectSource.github,
        visibility=ProjectVisibility.private,
        default_branch="main",
        active_git_handle=None,
        storage_backend=StorageBackend.local,
        github_repository_id=424242,
    )
    await env.store.set_active_git_handle(
        org, project_b.id, handle=project_b.id, backend=StorageBackend.local
    )
    with pytest.raises(PatchValidationError):
        await env.authz.authorize_writeback(org, admin, project_b.id, agent_id=None, run_id="r")


async def test_unregistered_repo_fails_closed() -> None:
    env, org, admin, _member, _viewer = await _bootstrap()
    project = await env.store.create_project(
        org_id=org,
        slug="proj-ghost",
        display_name="Ghost",
        source=ProjectSource.github,
        visibility=ProjectVisibility.private,
        default_branch="main",
        active_git_handle=None,
        storage_backend=StorageBackend.local,
        github_repository_id=555555,  # no repository registered for this id
    )
    await env.store.set_active_git_handle(
        org, project.id, handle=project.id, backend=StorageBackend.local
    )
    with pytest.raises(PatchValidationError):
        await env.authz.authorize_writeback(org, admin, project.id, agent_id=None, run_id="r")


async def test_safe_clone_url_revalidation_rejects_hostile_stored_url() -> None:
    env, org, admin, _member, _viewer = await _bootstrap()
    # A hostile URL persisted on the repo row must be rejected at authorization, not trusted as-is.
    project = await _bind_github_project(
        env, org, repo_id=333, clone_url="https://evil.example/acme/repo.git"
    )
    with pytest.raises(UntrustedUrlError):
        await env.authz.authorize_writeback(org, admin, project.id, agent_id=None, run_id="r")


async def test_not_found_vs_unauthorized_are_distinct() -> None:
    env, org, _admin, member, viewer = await _bootstrap()
    # Not found (also the cross-org non-existence case): a project id absent from this org.
    with pytest.raises(ProjectNotFoundError):
        await env.authz.authorize_generation(
            org, member, "does-not-exist", agent_id=None, run_id="r"
        )
    # Unauthorized: a viewer (read only) cannot run generation (needs 'use').
    project = await _bind_github_project(env, org)
    with pytest.raises(PermissionDenied):
        await env.authz.authorize_generation(org, viewer, project.id, agent_id=None, run_id="r")


async def test_capability_matrix_read_deny_vs_write_approve() -> None:
    env, org, admin, member, viewer = await _bootstrap()
    project = await _bind_github_project(env, org)

    # read is open to a viewer; approval requires write (a member with only 'use' cannot approve).
    await env.authz.authorize_read(org, viewer, project.id)
    with pytest.raises(PermissionDenied):
        await env.authz.authorize_approval(org, member, project.id)
    with pytest.raises(PermissionDenied):
        await env.authz.authorize_approval(org, viewer, project.id)
    # a writer (admin) can approve.
    await env.authz.authorize_approval(org, admin, project.id)


async def test_default_agent_label_authorizes_actor_directly() -> None:
    env, org, _admin, member, _viewer = await _bootstrap()
    project = await _bind_github_project(env, org)

    # The reserved default label is NOT a real Agent principal: it authorizes the actor directly and
    # still resolves the canonical per-Agent scope — no Agent named 'patch' need exist.
    binding = await env.authz.authorize_generation(
        org, member, project.id, agent_id=DEFAULT_PATCH_AGENT_ID, run_id="r"
    )
    assert binding.scope_id == derive_agent_scope(org, DEFAULT_PATCH_AGENT_ID)


async def test_real_agent_id_is_not_treated_as_actor_direct() -> None:
    env, org, _admin, member, _viewer = await _bootstrap()
    project = await _bind_github_project(env, org)
    # A non-default agent id authorizes through an Agent; a missing Agent fails closed as not-found
    # (never silently downgraded to a direct actor authorization).
    with pytest.raises(ProjectNotFoundError):
        await env.authz.authorize_generation(
            org, member, project.id, agent_id="ghost-agent", run_id="r"
        )


async def test_associate_run_delegates_to_service() -> None:
    env, org, admin, member, _viewer = await _bootstrap()
    project = await _bind_github_project(env, org)

    await env.authz.associate_run(org, member, project.id, "run-xyz", agent_id=None)
    runs = await env.svc.list_project_runs(org, admin, project.id)
    assert "run-xyz" in runs
