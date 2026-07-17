"""Mockable GitHub HTTP client with retry / rate-limit semantics (M3.7, WS-P).

A thin, dependency-injectable transport seam so the control plane can talk to the GitHub REST
API in production and be fully exercised in tests without a network. The client:

* never follows redirects (the transport is configured with ``follow_redirects=False``) — a
  redirect to an off-allowlist host can never be chased;
* retries idempotent GETs and the token-mint POST on transient 5xx / rate-limit responses with
  bounded exponential backoff; and
* performs **no** remote write / push / PR creation in this phase (read + token-mint only).

Tokens minted through this client are never logged; callers must keep them control-plane only.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


class GitHubError(RuntimeError):
    """A GitHub API call failed (non-sensitive message; never carries a token)."""


class GitHubAuthError(GitHubError):
    """Authentication/authorization with GitHub failed (401/403 non-rate-limit)."""


class GitHubRateLimitError(GitHubError):
    """The GitHub API rate limit was exhausted after retries."""


class GitHubNotFoundError(GitHubError):
    """A requested GitHub resource does not exist / is not visible to the installation."""


@dataclass(frozen=True)
class GitHubResponse:
    """A minimal, transport-agnostic HTTP response."""

    status: int
    headers: Mapping[str, str] = field(default_factory=dict)
    json_body: Any = None


@runtime_checkable
class GitHubTransport(Protocol):
    """The injectable HTTP seam (one method; production uses httpx, tests use a fake)."""

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json: Any | None = None,
    ) -> GitHubResponse: ...


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded exponential backoff for transient failures."""

    max_attempts: int = 4
    base_delay_seconds: float = 0.2
    max_delay_seconds: float = 5.0

    def delay_for(self, attempt: int) -> float:
        scaled = self.base_delay_seconds * float(2 ** (attempt - 1))
        return min(scaled, self.max_delay_seconds)


def _rate_limited(response: GitHubResponse) -> bool:
    if response.status == 429:
        return True
    if response.status == 403:
        remaining = response.headers.get("x-ratelimit-remaining")
        return remaining == "0"
    return False


class GitHubClient:
    """High-level GitHub REST client over an injectable :class:`GitHubTransport`.

    ``api_base_url`` must already be a normalized, allow-listed HTTPS base (see
    :mod:`keel_core.projects.github.urls`). The client only ever issues GETs and the
    installation-token POST; it never writes to a repository.
    """

    def __init__(
        self,
        transport: GitHubTransport,
        *,
        api_base_url: str,
        retry: RetryPolicy | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._transport = transport
        self._api_base = api_base_url.rstrip("/")
        self._retry = retry or RetryPolicy()
        self._sleep = sleep or asyncio.sleep

    async def _send(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str],
        json: Any | None = None,
        retry: bool = True,
    ) -> GitHubResponse:
        url = path if path.startswith("https://") else f"{self._api_base}{path}"
        attempts = self._retry.max_attempts if retry else 1
        last_response: GitHubResponse | None = None
        for attempt in range(1, attempts + 1):
            response = await self._transport.request(method, url, headers=headers, json=json)
            last_response = response
            if response.status < 500 and not _rate_limited(response):
                return response
            if attempt < attempts:
                await self._sleep(self._retry.delay_for(attempt))
        assert last_response is not None
        if _rate_limited(last_response):
            raise GitHubRateLimitError("GitHub API rate limit exhausted")
        raise GitHubError(f"GitHub API request failed with status {last_response.status}")

    @staticmethod
    def _raise_for_status(response: GitHubResponse) -> None:
        if response.status in (401, 403):
            if _rate_limited(response):
                raise GitHubRateLimitError("GitHub API rate limit exhausted")
            raise GitHubAuthError("GitHub authentication failed")
        if response.status == 404:
            raise GitHubNotFoundError("GitHub resource not found")
        if response.status >= 400:
            raise GitHubError(f"GitHub API returned status {response.status}")

    async def mint_installation_token(
        self, *, installation_id: int, app_jwt: str
    ) -> GitHubResponse:
        """POST an installation access-token request (the sole non-GET call)."""
        headers = {
            "Authorization": f"Bearer {app_jwt}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        response = await self._send(
            "POST",
            f"/app/installations/{installation_id}/access_tokens",
            headers=headers,
            json={},
        )
        self._raise_for_status(response)
        return response

    async def get_repository(self, *, token: str, full_name: str) -> dict[str, Any]:
        """GET a repository the installation can see (read-only metadata)."""
        response = await self._send(
            "GET",
            f"/repos/{full_name}",
            headers=self._installation_headers(token),
        )
        self._raise_for_status(response)
        if not isinstance(response.json_body, dict):
            raise GitHubError("unexpected GitHub repository payload")
        return response.json_body

    async def list_installation_repositories(self, *, token: str) -> list[dict[str, Any]]:
        """GET the repositories accessible to an installation (read-only)."""
        response = await self._send(
            "GET",
            "/installation/repositories",
            headers=self._installation_headers(token),
        )
        self._raise_for_status(response)
        body = response.json_body
        if isinstance(body, dict) and isinstance(body.get("repositories"), list):
            return [r for r in body["repositories"] if isinstance(r, dict)]
        raise GitHubError("unexpected GitHub repositories payload")

    @staticmethod
    def _installation_headers(token: str) -> dict[str, str]:
        return {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }


__all__ = [
    "GitHubAuthError",
    "GitHubClient",
    "GitHubError",
    "GitHubNotFoundError",
    "GitHubRateLimitError",
    "GitHubResponse",
    "GitHubTransport",
    "HttpxGitHubTransport",
    "RetryPolicy",
]


class HttpxGitHubTransport:
    """Production :class:`GitHubTransport` over ``httpx`` (redirects disabled).

    Redirects are never followed so a 30x to an off-allowlist host can't be chased. ``httpx``
    is imported lazily so the dependency is only required when the real transport is used.
    """

    def __init__(self, *, timeout_seconds: float = 15.0) -> None:
        import httpx

        self._client = httpx.AsyncClient(follow_redirects=False, timeout=timeout_seconds)

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json: Any | None = None,
    ) -> GitHubResponse:
        response = await self._client.request(method, url, headers=dict(headers), json=json)
        body: Any = None
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type or "+json" in content_type:
            try:
                body = response.json()
            except ValueError:
                body = None
        return GitHubResponse(
            status=response.status_code,
            headers={k.lower(): v for k, v in response.headers.items()},
            json_body=body,
        )

    async def aclose(self) -> None:
        await self._client.aclose()
