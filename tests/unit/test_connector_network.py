"""URL connector network bounds and exact validated-address handoff."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from keel_core.connector_network import (
    ConnectorHttpClient,
    ConnectorHttpPolicy,
    ConnectorNetworkError,
    ConnectorRateLimitError,
    PinnedHttpResponse,
    PinnedHttpTarget,
    validate_public_http_url,
)


async def _public_resolver(host: str, port: int) -> tuple[str, ...]:
    return ("93.184.216.34",)


class FakeTransport:
    def __init__(self, responses: list[PinnedHttpResponse]) -> None:
        self.responses = responses
        self.targets: list[PinnedHttpTarget] = []

    async def request(
        self, target: PinnedHttpTarget, policy: ConnectorHttpPolicy
    ) -> PinnedHttpResponse:
        self.targets.append(target)
        return self.responses.pop(0)


@pytest.mark.parametrize(
    "url",
    (
        "file:///etc/passwd",
        "http://localhost/feed",
        "http://127.0.0.1/feed",
        "http://169.254.169.254/latest/meta-data",
        "http://100.64.0.1/feed",
        "http://100.100.100.200/latest/meta-data",
        "http://[::ffff:100.100.100.200]/feed",
        "******example.com/feed",
    ),
)
async def test_rejects_non_public_connector_urls(url: str) -> None:
    with pytest.raises(ConnectorNetworkError):
        await validate_public_http_url(url, resolver=_public_resolver)


async def test_validates_every_redirect_and_bounds_response_size() -> None:
    transport = FakeTransport(
        [
            PinnedHttpResponse(302, {"location": "https://other.example/final"}, b""),
            PinnedHttpResponse(200, {}, b"12345"),
        ]
    )
    client = ConnectorHttpClient(
        ConnectorHttpPolicy(max_response_bytes=4),
        resolver=_public_resolver,
        transport=transport,
    )
    with pytest.raises(ConnectorNetworkError, match="size limit"):
        await client.get("https://example.com/start")
    assert [target.host for target in transport.targets] == [
        "example.com",
        "other.example",
    ]
    assert all(target.addresses == ("93.184.216.34",) for target in transport.targets)


async def test_surfaces_rate_limits_explicitly() -> None:
    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=FakeTransport([PinnedHttpResponse(429, {"retry-after": "30"}, b"")]),
    )
    with pytest.raises(ConnectorRateLimitError) as exc:
        await client.get("https://example.com/feed")
    assert exc.value.retry_after == "30"


class FakeWriter:
    def __init__(self) -> None:
        self.data = b""

    def write(self, data: bytes) -> None:
        self.data += data

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


async def test_default_transport_connects_to_pinned_ip_with_original_host_and_sni(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolutions = 0

    async def rebinding_resolver(host: str, port: int) -> tuple[str, ...]:
        nonlocal resolutions
        resolutions += 1
        return ("93.184.216.34",) if resolutions == 1 else ("127.0.0.1",)

    writer = FakeWriter()
    opened: list[tuple[str, int, str | None]] = []

    async def fake_open_connection(
        host: str,
        port: int,
        *,
        ssl: object,
        server_hostname: str | None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        opened.append((host, port, server_hostname))
        reader = asyncio.StreamReader()
        reader.feed_data(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
        reader.feed_eof()
        return reader, cast(asyncio.StreamWriter, writer)

    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
    assert (
        await ConnectorHttpClient(resolver=rebinding_resolver).get("https://example.com/feed")
        == b"ok"
    )
    assert resolutions == 1
    assert opened == [("93.184.216.34", 443, "example.com")]
    assert b"Host: example.com\r\n" in writer.data
