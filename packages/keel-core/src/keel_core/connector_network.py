"""Bounded public-HTTP client for URL/feed connector providers."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

import httpx

Resolver = Callable[[str, int], Awaitable[tuple[str, ...]]]


class ConnectorNetworkError(Exception):
    pass


class ConnectorRateLimitError(ConnectorNetworkError):
    def __init__(self, retry_after: str | None) -> None:
        self.retry_after = retry_after
        super().__init__("connector provider rate limit exceeded")


@dataclass(frozen=True, slots=True)
class ConnectorHttpPolicy:
    timeout_seconds: float = 10.0
    max_redirects: int = 3
    max_response_bytes: int = 5 * 1024 * 1024
    max_pages: int = 100

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("connector HTTP timeout must be positive")
        if self.max_redirects < 0 or self.max_response_bytes <= 0 or self.max_pages <= 0:
            raise ValueError("connector HTTP bounds must be positive")


def _public_address(value: str) -> bool:
    address = ipaddress.ip_address(value)
    return not (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


async def _resolve(host: str, port: int) -> tuple[str, ...]:
    def resolve() -> tuple[str, ...]:
        rows = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        return tuple(sorted({str(row[4][0]) for row in rows}))

    try:
        return await asyncio.to_thread(resolve)
    except OSError as exc:
        raise ConnectorNetworkError("connector URL host could not be resolved") from exc


async def validate_public_http_url(url: str, *, resolver: Resolver = _resolve) -> str:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        raise ConnectorNetworkError("connector URL must use http or https")
    if parsed.username is not None or parsed.password is not None:
        raise ConnectorNetworkError("connector URL must not contain credentials")
    if not parsed.hostname:
        raise ConnectorNetworkError("connector URL must include a hostname")
    host = parsed.hostname.rstrip(".").lower()
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".local"):
        raise ConnectorNetworkError("connector URL host is not public")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise ConnectorNetworkError("connector URL contains an invalid port") from exc
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        addresses = await resolver(host, port)
        if not addresses:
            raise ConnectorNetworkError("connector URL host did not resolve") from None
    else:
        addresses = (str(literal),)
    if any(not _public_address(address) for address in addresses):
        raise ConnectorNetworkError("connector URL resolves to a non-public address")
    return url


class ConnectorHttpClient:
    """HTTP GET with URL/redirect validation, timeouts, and response-size bounds."""

    def __init__(
        self,
        policy: ConnectorHttpPolicy | None = None,
        *,
        resolver: Resolver = _resolve,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.policy = policy or ConnectorHttpPolicy()
        self._resolver = resolver
        self._transport = transport

    async def get(self, url: str) -> bytes:
        current = url
        async with httpx.AsyncClient(
            timeout=self.policy.timeout_seconds,
            follow_redirects=False,
            transport=self._transport,
        ) as client:
            for redirect_count in range(self.policy.max_redirects + 1):
                await validate_public_http_url(current, resolver=self._resolver)
                async with client.stream("GET", current) as response:
                    if response.status_code == 429:
                        raise ConnectorRateLimitError(response.headers.get("retry-after"))
                    if response.is_redirect:
                        if redirect_count >= self.policy.max_redirects:
                            raise ConnectorNetworkError("connector URL exceeded redirect limit")
                        location = response.headers.get("location")
                        if not location:
                            raise ConnectorNetworkError(
                                "connector provider returned an empty redirect"
                            )
                        current = urljoin(current, location)
                        continue
                    try:
                        response.raise_for_status()
                    except httpx.HTTPStatusError as exc:
                        raise ConnectorNetworkError(
                            f"connector provider returned HTTP {response.status_code}"
                        ) from exc
                    length = response.headers.get("content-length")
                    if length is not None:
                        try:
                            declared = int(length)
                        except ValueError as exc:
                            raise ConnectorNetworkError(
                                "connector provider returned an invalid content length"
                            ) from exc
                        if declared > self.policy.max_response_bytes:
                            raise ConnectorNetworkError(
                                "connector response exceeds the configured size limit"
                            )
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > self.policy.max_response_bytes:
                            raise ConnectorNetworkError(
                                "connector response exceeds the configured size limit"
                            )
                        chunks.append(chunk)
                    return b"".join(chunks)
        raise ConnectorNetworkError("connector HTTP request did not complete")


__all__ = [
    "ConnectorHttpClient",
    "ConnectorHttpPolicy",
    "ConnectorNetworkError",
    "ConnectorRateLimitError",
    "Resolver",
    "validate_public_http_url",
]
