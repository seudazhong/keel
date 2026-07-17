"""A minimal typed async client for Keel's stable `/v1` endpoints."""

from __future__ import annotations

from typing import Self

import httpx

from keel_sdk.models import (
    AgentSummary,
    CreateAgentRequest,
    CreateGrantRequest,
    CreateMessageRequest,
    CreateMessageResponse,
    CreateOrganizationRequest,
    GrantSummary,
    InterruptRunResponse,
    MeResponse,
    OrganizationMembership,
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
