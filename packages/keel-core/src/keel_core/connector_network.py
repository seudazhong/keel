"""Single-resolution, IP-pinned HTTP client for URL/feed connector providers."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urljoin, urlsplit

Resolver = Callable[[str, int], Awaitable[tuple[str, ...]]]
_SHARED_IPV4_SPACE = ipaddress.ip_network("100.64.0.0/10")


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


@dataclass(frozen=True, slots=True)
class PinnedHttpTarget:
    url: str
    scheme: str
    host: str
    port: int
    addresses: tuple[str, ...]
    request_target: str
    host_header: str


@dataclass(frozen=True, slots=True)
class PinnedHttpResponse:
    status_code: int
    headers: dict[str, str]
    body: bytes

    @property
    def is_redirect(self) -> bool:
        return self.status_code in {301, 302, 303, 307, 308}


class PinnedHttpTransport(Protocol):
    async def request(
        self, target: PinnedHttpTarget, policy: ConnectorHttpPolicy
    ) -> PinnedHttpResponse: ...


class ConnectionOpener(Protocol):
    async def __call__(
        self,
        host: str,
        port: int,
        *,
        ssl_context: ssl.SSLContext | None,
        server_hostname: str | None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]: ...


async def _open_connection(
    host: str,
    port: int,
    *,
    ssl_context: ssl.SSLContext | None,
    server_hostname: str | None,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_connection(
        host,
        port,
        ssl=ssl_context,
        server_hostname=server_hostname,
    )


def _public_address(value: str) -> bool:
    address = ipaddress.ip_address(value)
    ipv4 = address if isinstance(address, ipaddress.IPv4Address) else address.ipv4_mapped
    if ipv4 is not None and ipv4 in _SHARED_IPV4_SPACE:
        return False
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


async def _validated_target(url: str, resolver: Resolver) -> PinnedHttpTarget:
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
    try:
        normalized = tuple(sorted({str(ipaddress.ip_address(item)) for item in addresses}))
    except ValueError as exc:
        raise ConnectorNetworkError("connector URL resolver returned an invalid address") from exc
    if any(not _public_address(address) for address in normalized):
        raise ConnectorNetworkError("connector URL resolves to a non-public address")
    default_port = 443 if parsed.scheme == "https" else 80
    host_literal = f"[{host}]" if ":" in host else host
    host_header = host_literal if port == default_port else f"{host_literal}:{port}"
    request_target = parsed.path or "/"
    if parsed.query:
        request_target = f"{request_target}?{parsed.query}"
    return PinnedHttpTarget(
        url=url,
        scheme=parsed.scheme,
        host=host,
        port=port,
        addresses=normalized,
        request_target=request_target,
        host_header=host_header,
    )


async def validate_public_http_url(url: str, *, resolver: Resolver = _resolve) -> str:
    await _validated_target(url, resolver)
    return url


class AsyncioPinnedHttpTransport:
    """HTTP/1.1 transport that connects only to validated IPs while retaining Host/SNI."""

    def __init__(self, opener: ConnectionOpener = _open_connection) -> None:
        self._opener = opener

    async def request(
        self, target: PinnedHttpTarget, policy: ConnectorHttpPolicy
    ) -> PinnedHttpResponse:
        last_error: OSError | TimeoutError | None = None
        for address in target.addresses:
            try:
                async with asyncio.timeout(policy.timeout_seconds):
                    return await self._request_address(target, address, policy)
            except (OSError, TimeoutError) as exc:
                last_error = exc
        raise ConnectorNetworkError("connector URL could not be reached") from last_error

    async def _request_address(
        self,
        target: PinnedHttpTarget,
        address: str,
        policy: ConnectorHttpPolicy,
    ) -> PinnedHttpResponse:
        ssl_context = ssl.create_default_context() if target.scheme == "https" else None
        reader, writer = await self._opener(
            address,
            target.port,
            ssl_context=ssl_context,
            server_hostname=target.host if ssl_context is not None else None,
        )
        try:
            request = (
                f"GET {target.request_target} HTTP/1.1\r\n"
                f"Host: {target.host_header}\r\n"
                "User-Agent: Keel-Connector/1\r\n"
                "Accept: */*\r\n"
                "Accept-Encoding: identity\r\n"
                "Connection: close\r\n\r\n"
            )
            writer.write(request.encode("ascii"))
            await writer.drain()
            return await _read_response(reader, policy.max_response_bytes)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass


async def _read_response(
    reader: asyncio.StreamReader, max_response_bytes: int
) -> PinnedHttpResponse:
    try:
        raw_headers = await reader.readuntil(b"\r\n\r\n")
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as exc:
        raise ConnectorNetworkError("connector provider returned invalid HTTP headers") from exc
    if len(raw_headers) > 65_536:
        raise ConnectorNetworkError("connector provider returned oversized HTTP headers")
    lines = raw_headers[:-4].split(b"\r\n")
    try:
        version, status_text, _ = lines[0].decode("ascii").split(" ", 2)
        status_code = int(status_text)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ConnectorNetworkError("connector provider returned an invalid HTTP status") from exc
    if version not in {"HTTP/1.0", "HTTP/1.1"} or not 100 <= status_code <= 599:
        raise ConnectorNetworkError("connector provider returned an invalid HTTP status")
    headers: dict[str, str] = {}
    for raw_line in lines[1:]:
        if raw_line.startswith((b" ", b"\t")) or b":" not in raw_line:
            raise ConnectorNetworkError("connector provider returned invalid HTTP headers")
        raw_name, raw_value = raw_line.split(b":", 1)
        try:
            name = raw_name.decode("ascii").strip().lower()
            value = raw_value.decode("iso-8859-1").strip()
        except UnicodeDecodeError as exc:
            raise ConnectorNetworkError("connector provider returned invalid HTTP headers") from exc
        if not name:
            raise ConnectorNetworkError("connector provider returned invalid HTTP headers")
        if name in headers and name in {"content-length", "location", "transfer-encoding"}:
            raise ConnectorNetworkError("connector provider returned ambiguous HTTP headers")
        headers[name] = f"{headers[name]}, {value}" if name in headers else value
    if status_code in {204, 304} or 100 <= status_code < 200:
        body = b""
    elif headers.get("transfer-encoding", "").lower() == "chunked":
        body = await _read_chunked(reader, max_response_bytes)
    elif "transfer-encoding" in headers:
        raise ConnectorNetworkError("connector provider returned unsupported transfer encoding")
    elif "content-length" in headers:
        try:
            length = int(headers["content-length"])
        except ValueError as exc:
            raise ConnectorNetworkError(
                "connector provider returned an invalid content length"
            ) from exc
        if length < 0 or length > max_response_bytes:
            raise ConnectorNetworkError("connector response exceeds the configured size limit")
        try:
            body = await reader.readexactly(length)
        except asyncio.IncompleteReadError as exc:
            raise ConnectorNetworkError("connector provider truncated the response") from exc
    else:
        body = await _read_to_eof(reader, max_response_bytes)
    return PinnedHttpResponse(status_code, headers, body)


async def _read_to_eof(reader: asyncio.StreamReader, limit: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = await reader.read(min(65_536, limit - size + 1))
        if not chunk:
            return b"".join(chunks)
        size += len(chunk)
        if size > limit:
            raise ConnectorNetworkError("connector response exceeds the configured size limit")
        chunks.append(chunk)


async def _read_chunked(reader: asyncio.StreamReader, limit: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        try:
            line = await reader.readuntil(b"\r\n")
            chunk_size = int(line[:-2].split(b";", 1)[0], 16)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ValueError) as exc:
            raise ConnectorNetworkError("connector provider returned invalid chunked data") from exc
        if chunk_size == 0:
            try:
                while await reader.readuntil(b"\r\n") != b"\r\n":
                    pass
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as exc:
                raise ConnectorNetworkError(
                    "connector provider returned invalid chunk trailers"
                ) from exc
            return b"".join(chunks)
        size += chunk_size
        if size > limit:
            raise ConnectorNetworkError("connector response exceeds the configured size limit")
        try:
            chunk = await reader.readexactly(chunk_size)
            terminator = await reader.readexactly(2)
        except asyncio.IncompleteReadError as exc:
            raise ConnectorNetworkError("connector provider truncated chunked data") from exc
        if terminator != b"\r\n":
            raise ConnectorNetworkError("connector provider returned invalid chunked data")
        chunks.append(chunk)


class ConnectorHttpClient:
    """HTTP GET with one DNS resolution per hop and pinned address handoff."""

    def __init__(
        self,
        policy: ConnectorHttpPolicy | None = None,
        *,
        resolver: Resolver = _resolve,
        transport: PinnedHttpTransport | None = None,
    ) -> None:
        self.policy = policy or ConnectorHttpPolicy()
        self._resolver = resolver
        self._transport = transport or AsyncioPinnedHttpTransport()

    async def get(self, url: str) -> bytes:
        current = url
        for redirect_count in range(self.policy.max_redirects + 1):
            target = await _validated_target(current, self._resolver)
            response = await self._transport.request(target, self.policy)
            if response.status_code == 429:
                raise ConnectorRateLimitError(response.headers.get("retry-after"))
            if response.is_redirect:
                if redirect_count >= self.policy.max_redirects:
                    raise ConnectorNetworkError("connector URL exceeded redirect limit")
                location = response.headers.get("location")
                if not location:
                    raise ConnectorNetworkError("connector provider returned an empty redirect")
                current = urljoin(current, location)
                continue
            if not 200 <= response.status_code < 300:
                raise ConnectorNetworkError(
                    f"connector provider returned HTTP {response.status_code}"
                )
            if len(response.body) > self.policy.max_response_bytes:
                raise ConnectorNetworkError("connector response exceeds the configured size limit")
            return response.body
        raise ConnectorNetworkError("connector HTTP request did not complete")


__all__ = [
    "AsyncioPinnedHttpTransport",
    "ConnectorHttpClient",
    "ConnectorHttpPolicy",
    "ConnectorNetworkError",
    "ConnectorRateLimitError",
    "PinnedHttpResponse",
    "PinnedHttpTarget",
    "PinnedHttpTransport",
    "Resolver",
    "validate_public_http_url",
]
