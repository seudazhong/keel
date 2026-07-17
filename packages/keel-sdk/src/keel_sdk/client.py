"""A minimal typed async client for Keel's stable `/v1` endpoints."""

from __future__ import annotations

from typing import Self

import httpx

from keel_sdk.models import (
    AgentSummary,
    AssociateRunRequest,
    CreateAgentRequest,
    CreateGrantRequest,
    CreateMessageRequest,
    CreateMessageResponse,
    CreateOrganizationRequest,
    CreateProjectRequest,
    GrantProjectRequest,
    GrantSummary,
    ImportProjectRequest,
    InstallationSummary,
    InterruptRunResponse,
    LinkInstallationRequest,
    MaterializeWorktreeRequest,
    MeResponse,
    OrganizationMembership,
    ProjectSummary,
    RunAssociation,
    SyncEntrySummary,
    UpdateProjectRequest,
    WorktreeSummary,
)


class KeelClient:
    """Typed async client with optional caller-owned authentication headers."""

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._owns_client = client is None
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key is not None else {}
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"), headers=self._headers
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the internally-created HTTP client."""
        if self._owns_client:
            await self._client.aclose()

    async def create_message(
        self, session_id: str, request: CreateMessageRequest
    ) -> CreateMessageResponse:
        """Admit a user message and return the scheduled run."""
        response = await self._client.post(
            f"/v1/sessions/{session_id}/messages",
            json=request.model_dump(exclude_none=True),
            headers=self._headers,
        )
        response.raise_for_status()
        return CreateMessageResponse.model_validate(response.json())

    async def interrupt_run(self, run_id: str) -> InterruptRunResponse:
        """Request interruption of an active run."""
        response = await self._client.post(f"/v1/runs/{run_id}/interrupt", headers=self._headers)
        response.raise_for_status()
        return InterruptRunResponse.model_validate(response.json())

    # --- Identity (M3.6) — additive methods ------------------------------------------
    def _org_headers(self, org: str | None) -> dict[str, str]:
        headers = dict(self._headers)
        if org is not None:
            headers["X-Keel-Org"] = org
        return headers

    async def get_me(self) -> MeResponse:
        """Return the authenticated user and its organizations."""
        response = await self._client.get("/v1/identity/me", headers=self._headers)
        response.raise_for_status()
        return MeResponse.model_validate(response.json())

    async def list_organizations(self) -> list[OrganizationMembership]:
        """List the organizations the caller belongs to."""
        response = await self._client.get("/v1/identity/organizations", headers=self._headers)
        response.raise_for_status()
        return [OrganizationMembership.model_validate(item) for item in response.json()]

    async def create_organization(
        self, request: CreateOrganizationRequest
    ) -> OrganizationMembership:
        """Create an organization; the caller becomes its owner."""
        response = await self._client.post(
            "/v1/identity/organizations",
            json=request.model_dump(),
            headers=self._headers,
        )
        response.raise_for_status()
        return OrganizationMembership.model_validate(response.json())

    async def list_agents(self, org: str) -> list[AgentSummary]:
        """List the Agents visible to the caller in ``org``."""
        response = await self._client.get("/v1/identity/agents", headers=self._org_headers(org))
        response.raise_for_status()
        return [AgentSummary.model_validate(item) for item in response.json()]

    async def create_agent(self, org: str, request: CreateAgentRequest) -> AgentSummary:
        """Create a personal/team Agent in ``org``."""
        response = await self._client.post(
            "/v1/identity/agents",
            json=request.model_dump(),
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return AgentSummary.model_validate(response.json())

    async def select_agent(self, org: str, agent_id: str) -> AgentSummary:
        """Resolve + authorize a persisted Agent for a run (compatibility bridge)."""
        response = await self._client.post(
            f"/v1/identity/agents/{agent_id}/select", headers=self._org_headers(org)
        )
        response.raise_for_status()
        return AgentSummary.model_validate(response.json())

    async def list_grants(self, org: str, *, agent_id: str | None = None) -> list[GrantSummary]:
        """List resource grants in ``org`` (optionally for one Agent)."""
        params = {"agent_id": agent_id} if agent_id is not None else None
        response = await self._client.get(
            "/v1/identity/grants", params=params, headers=self._org_headers(org)
        )
        response.raise_for_status()
        return [GrantSummary.model_validate(item) for item in response.json()]

    async def create_grant(self, org: str, request: CreateGrantRequest) -> GrantSummary:
        """Grant an Agent a capability on a resource in ``org``."""
        response = await self._client.post(
            "/v1/identity/grants",
            json=request.model_dump(),
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return GrantSummary.model_validate(response.json())

    async def revoke_grant(self, org: str, grant_id: str) -> GrantSummary:
        """Revoke a resource grant in ``org``."""
        response = await self._client.request(
            "DELETE", f"/v1/identity/grants/{grant_id}", headers=self._org_headers(org)
        )
        response.raise_for_status()
        return GrantSummary.model_validate(response.json())

    # --- Managed projects (M3.7) — additive methods ----------------------------------
    async def list_projects(
        self, org: str, *, include_inactive: bool = False
    ) -> list[ProjectSummary]:
        """List managed projects in ``org``."""
        response = await self._client.get(
            "/v1/projects",
            params={"include_inactive": include_inactive},
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return [ProjectSummary.model_validate(item) for item in response.json()]

    async def create_project(self, org: str, request: CreateProjectRequest) -> ProjectSummary:
        """Create a blank/local managed project in ``org``."""
        response = await self._client.post(
            "/v1/projects", json=request.model_dump(), headers=self._org_headers(org)
        )
        response.raise_for_status()
        return ProjectSummary.model_validate(response.json())

    async def import_project(self, org: str, request: ImportProjectRequest) -> ProjectSummary:
        """Import a project from a GitHub repository in ``org``."""
        response = await self._client.post(
            "/v1/projects/import", json=request.model_dump(), headers=self._org_headers(org)
        )
        response.raise_for_status()
        return ProjectSummary.model_validate(response.json())

    async def get_project(self, org: str, project_id: str) -> ProjectSummary:
        """Fetch one managed project."""
        response = await self._client.get(
            f"/v1/projects/{project_id}", headers=self._org_headers(org)
        )
        response.raise_for_status()
        return ProjectSummary.model_validate(response.json())

    async def update_project(
        self, org: str, project_id: str, request: UpdateProjectRequest
    ) -> ProjectSummary:
        """Optimistically update a managed project."""
        response = await self._client.patch(
            f"/v1/projects/{project_id}",
            json=request.model_dump(exclude_none=True),
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return ProjectSummary.model_validate(response.json())

    async def archive_project(
        self, org: str, project_id: str, *, expected_version: int
    ) -> ProjectSummary:
        """Archive a managed project."""
        response = await self._client.post(
            f"/v1/projects/{project_id}/archive",
            json={"expected_version": expected_version},
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return ProjectSummary.model_validate(response.json())

    async def delete_project(
        self, org: str, project_id: str, *, expected_version: int
    ) -> ProjectSummary:
        """Soft-delete a managed project."""
        response = await self._client.post(
            f"/v1/projects/{project_id}/delete",
            json={"expected_version": expected_version},
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return ProjectSummary.model_validate(response.json())

    async def request_project_sync(self, org: str, project_id: str) -> SyncEntrySummary:
        """Request a durable fetch of a GitHub-sourced project."""
        response = await self._client.post(
            f"/v1/projects/{project_id}/sync", headers=self._org_headers(org)
        )
        response.raise_for_status()
        return SyncEntrySummary.model_validate(response.json())

    async def list_project_sync(self, org: str, project_id: str) -> list[SyncEntrySummary]:
        """List a project's repo sync ledger entries."""
        response = await self._client.get(
            f"/v1/projects/{project_id}/sync", headers=self._org_headers(org)
        )
        response.raise_for_status()
        return [SyncEntrySummary.model_validate(item) for item in response.json()]

    async def list_worktrees(self, org: str, project_id: str) -> list[WorktreeSummary]:
        """List a project's active worktrees."""
        response = await self._client.get(
            f"/v1/projects/{project_id}/worktrees", headers=self._org_headers(org)
        )
        response.raise_for_status()
        return [WorktreeSummary.model_validate(item) for item in response.json()]

    async def materialize_worktree(
        self, org: str, project_id: str, request: MaterializeWorktreeRequest
    ) -> WorktreeSummary:
        """Materialize a run-scoped worktree."""
        response = await self._client.post(
            f"/v1/projects/{project_id}/worktrees",
            json=request.model_dump(exclude_none=True),
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return WorktreeSummary.model_validate(response.json())

    async def reclaim_worktree(self, org: str, project_id: str, run_id: str) -> WorktreeSummary:
        """Reclaim a run-scoped worktree."""
        response = await self._client.request(
            "DELETE",
            f"/v1/projects/{project_id}/worktrees/{run_id}",
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return WorktreeSummary.model_validate(response.json())

    async def list_project_runs(self, org: str, project_id: str) -> list[str]:
        """List durable run ids associated with a project."""
        response = await self._client.get(
            f"/v1/projects/{project_id}/runs", headers=self._org_headers(org)
        )
        response.raise_for_status()
        body = response.json()
        return list(body.get("run_ids", []))

    async def associate_run(
        self, org: str, project_id: str, request: AssociateRunRequest
    ) -> RunAssociation:
        """Associate a durable run with a project."""
        response = await self._client.post(
            f"/v1/projects/{project_id}/runs",
            json=request.model_dump(exclude_none=True),
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return RunAssociation.model_validate(response.json())

    async def grant_project(
        self, org: str, project_id: str, request: GrantProjectRequest
    ) -> GrantSummary:
        """Grant an Agent a capability on a project."""
        response = await self._client.post(
            f"/v1/projects/{project_id}/grants",
            json=request.model_dump(),
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return GrantSummary.model_validate(response.json())

    async def list_installations(self, org: str) -> list[InstallationSummary]:
        """List GitHub App installations bound to ``org``."""
        response = await self._client.get(
            "/v1/projects/github/installations", headers=self._org_headers(org)
        )
        response.raise_for_status()
        return [InstallationSummary.model_validate(item) for item in response.json()]

    async def link_installation(
        self, org: str, request: LinkInstallationRequest
    ) -> InstallationSummary:
        """Bind a GitHub App installation to ``org``."""
        response = await self._client.post(
            "/v1/projects/github/installations",
            json=request.model_dump(),
            headers=self._org_headers(org),
        )
        response.raise_for_status()
        return InstallationSummary.model_validate(response.json())
