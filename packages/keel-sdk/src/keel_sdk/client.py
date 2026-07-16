"""A minimal typed async client for Keel's stable `/v1` endpoints."""

from __future__ import annotations

from typing import Self

import httpx

from keel_sdk.models import CreateMessageRequest, CreateMessageResponse, InterruptRunResponse


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
