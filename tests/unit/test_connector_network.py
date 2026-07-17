"""URL connector network bounds and exact validated-address handoff."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from keel_core.connector_network import (
    AsyncioPinnedHttpTransport,
    ConnectorHttpClient,
    ConnectorHttpHeaders,
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


def _response(
    status_code: int,
    headers: dict[str, str] | ConnectorHttpHeaders,
    body: bytes,
) -> PinnedHttpResponse:
    normalized = (
        headers if isinstance(headers, ConnectorHttpHeaders) else ConnectorHttpHeaders(headers)
    )
    return PinnedHttpResponse(status_code, normalized, body)


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
            _response(302, {"location": "https://other.example/final"}, b""),
            _response(200, {}, b"12345"),
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
        transport=FakeTransport([_response(429, {"retry-after": "30"}, b"")]),
    )
    with pytest.raises(ConnectorRateLimitError) as exc:
        await client.get("https://example.com/feed")
    assert exc.value.retry_after == "30"


async def test_can_explicitly_accept_rate_limit_response_metadata() -> None:
    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=FakeTransport([_response(429, {"Retry-After": "30"}, b"")]),
    )

    response = await client.get_response(
        "https://example.com/feed",
        accepted_status_codes=frozenset({429}),
    )

    assert response.status_code == 429
    assert response.headers["retry-after"] == "30"


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


async def test_transmits_conditional_request_headers() -> None:
    writer = FakeWriter()

    async def fake_opener(
        host: str,
        port: int,
        *,
        ssl_context: object,
        server_hostname: str | None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader = asyncio.StreamReader()
        reader.feed_data(b'HTTP/1.1 304 Not Modified\r\nETag: "v1"\r\n\r\n')
        reader.feed_eof()
        return reader, cast(asyncio.StreamWriter, writer)

    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=AsyncioPinnedHttpTransport(opener=fake_opener),
    )
    response = await client.get_response(
        "https://example.com/feed",
        request_headers={
            "If-None-Match": '"v1"',
            "if-modified-since": "Fri, 17 Jul 2026 18:00:00 GMT",
        },
        accepted_status_codes=frozenset({200, 304}),
    )

    assert response.status_code == 304
    assert response.body == b""
    assert b'If-None-Match: "v1"\r\n' in writer.data
    assert b"If-Modified-Since: Fri, 17 Jul 2026 18:00:00 GMT\r\n" in writer.data


async def test_returns_304_response_metadata_with_case_insensitive_headers() -> None:
    headers = ConnectorHttpHeaders(
        (
            ("ETag", '"v2"'),
            ("Last-Modified", "Fri, 17 Jul 2026 19:00:00 GMT"),
            ("X-Feed-Marker", "first"),
            ("x-feed-marker", "second"),
        )
    )
    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=FakeTransport([_response(304, headers, b"")]),
    )

    response = await client.get_response(
        "https://example.com/feed",
        accepted_status_codes=frozenset({200, 304}),
    )

    assert response.final_url == "https://example.com/feed"
    assert response.status_code == 304
    assert response.headers["etag"] == '"v2"'
    assert response.headers["LAST-MODIFIED"] == "Fri, 17 Jul 2026 19:00:00 GMT"
    assert response.headers.get_all("X-Feed-Marker") == ("first", "second")
    assert response.body == b""
    with pytest.raises(AttributeError, match="immutable"):
        response.headers._names = ()


async def test_get_remains_bytes_only_compatible() -> None:
    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=FakeTransport([_response(200, {"content-type": "text/plain"}, b"feed")]),
    )

    assert await client.get("https://example.com/feed") == b"feed"


async def test_validates_accepted_status_codes_and_rejects_unaccepted_status() -> None:
    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=FakeTransport([_response(304, {}, b"")]),
    )

    with pytest.raises(ValueError, match="non-empty bounded set"):
        await client.get_response(
            "https://example.com/feed",
            accepted_status_codes=frozenset(),
        )
    with pytest.raises(ValueError, match="100 through 599"):
        await client.get_response(
            "https://example.com/feed",
            accepted_status_codes=frozenset({99}),
        )
    with pytest.raises(ConnectorNetworkError, match="HTTP 304"):
        await client.get_response("https://example.com/feed")


@pytest.mark.parametrize(
    "header",
    ("Authorization", "Cookie", "Proxy-Authorization", "Connection"),
)
async def test_rejects_sensitive_and_hop_by_hop_request_headers(header: str) -> None:
    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=FakeTransport([]),
    )

    with pytest.raises(ConnectorNetworkError, match="header is not allowed"):
        await client.get_response(
            "https://example.com/feed",
            request_headers={header: "secret"},
        )


async def test_rejects_request_header_injection() -> None:
    client = ConnectorHttpClient(
        resolver=_public_resolver,
        transport=FakeTransport([]),
    )

    with pytest.raises(ConnectorNetworkError, match="header value is invalid"):
        await client.get_response(
            "https://example.com/feed",
            request_headers={"If-None-Match": '"v1"\r\nAuthorization: secret'},
        )


async def test_redirects_keep_conditional_headers_only_on_same_origin() -> None:
    transport = FakeTransport(
        [
            _response(302, {"location": "/moved"}, b""),
            _response(302, {"location": "https://other.example/final"}, b""),
            _response(200, {}, b"ok"),
        ]
    )
    client = ConnectorHttpClient(resolver=_public_resolver, transport=transport)

    response = await client.get_response(
        "https://example.com/feed",
        request_headers={"If-None-Match": '"v1"'},
    )

    assert response.final_url == "https://other.example/final"
    assert response.body == b"ok"
    assert [dict(target.request_headers) for target in transport.targets] == [
        {"if-none-match": '"v1"'},
        {"if-none-match": '"v1"'},
        {},
    ]
