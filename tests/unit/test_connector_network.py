"""URL connector network bounds: SSRF, redirects, rate limits, and response size."""

from __future__ import annotations

import httpx
import pytest

from keel_core.connector_network import (
    ConnectorHttpClient,
    ConnectorHttpPolicy,
    ConnectorNetworkError,
    ConnectorRateLimitError,
    validate_public_http_url,
)


async def _public_resolver(host: str, port: int) -> tuple[str, ...]:
    return ("93.184.216.34",)


@pytest.mark.parametrize(
    "url",
    (
        "file:///etc/passwd",
        "http://localhost/feed",
        "http://127.0.0.1/feed",
        "http://169.254.169.254/latest/meta-data",
        "http://user:pass@example.com/feed",
    ),
)
async def test_rejects_non_public_connector_urls(url: str) -> None:
    with pytest.raises(ConnectorNetworkError):
        await validate_public_http_url(url, resolver=_public_resolver)


async def test_validates_every_redirect_and_bounds_response_size() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/start":
            return httpx.Response(302, headers={"location": "https://example.com/final"})
        return httpx.Response(200, content=b"12345")

    client = ConnectorHttpClient(
        ConnectorHttpPolicy(max_response_bytes=4),
        resolver=_public_resolver,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(ConnectorNetworkError, match="size limit"):
        await client.get("https://example.com/start")


async def test_surfaces_rate_limits_explicitly() -> None:
    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(429, headers={"retry-after": "30"})
        ),
    )
    with pytest.raises(ConnectorRateLimitError) as exc:
        await client.get("https://example.com/feed")
    assert exc.value.retry_after == "30"
