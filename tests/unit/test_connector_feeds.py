"""RSS and Atom provider parsing, polling, and network safety."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from keel_core.connector_contracts import (
    ConnectorBinding,
    ConnectorBindingStatus,
    ConnectorCursor,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorResource,
)
from keel_core.connector_network import (
    ConnectorHttpClient,
    ConnectorHttpPolicy,
    ConnectorNetworkError,
    PinnedHttpResponse,
    PinnedHttpTarget,
)
from keel_core.connector_providers._feeds import parse_atom, parse_rss
from keel_core.connector_providers.atom import AtomProvider
from keel_core.connector_providers.atom import manifest as atom_manifest
from keel_core.connector_providers.rss import RssProvider
from keel_core.connector_providers.rss import manifest as rss_manifest
from keel_core.connector_registry import (
    ConnectorRegistration,
    ConnectorRegistry,
    discover_connector_registry,
)
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connector_service import ConnectorService
from keel_core.types import ContentTaint

RSS = b"""<?xml version="1.0"?>
<rss version="2.0"><channel><title>Example RSS</title>
<item><guid>item-1</guid><title>Hello</title>
<link>https://example.com/items/1</link><pubDate>Fri, 17 Jul 2026 18:00:00 GMT</pubDate>
<description><![CDATA[<p>Safe <b>text</b></p><script>bad()</script>]]></description></item>
</channel></rss>"""
ATOM = b"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom"><title>Example Atom</title>
<entry><id>tag:example.com,2026:item-1</id><title>Hello Atom</title>
<link href="/items/1"/><updated>2026-07-17T18:00:00Z</updated>
<content type="html">&lt;p&gt;Atom &lt;b&gt;text&lt;/b&gt;&lt;/p&gt;</content></entry>
</feed>"""


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


def _client(
    responses: list[PinnedHttpResponse],
    *,
    resolver: Any = _public_resolver,
) -> tuple[ConnectorHttpClient, FakeTransport]:
    transport = FakeTransport(responses)
    return (
        ConnectorHttpClient(
            ConnectorHttpPolicy(max_response_bytes=1_048_576),
            resolver=resolver,
            transport=transport,
        ),
        transport,
    )


def _response(
    status: int,
    body: bytes = b"",
    headers: Mapping[str, str] | None = None,
) -> PinnedHttpResponse:
    return PinnedHttpResponse(status, headers or {}, body)


def _context(
    connector_id: str,
    *,
    cursor: ConnectorCursor | None = None,
    resource_url: str = "https://example.com/feed",
) -> ConnectorOperationContext:
    binding = ConnectorBinding(
        id=f"{connector_id}-binding",
        scope_id="scope:feed",
        connector_id=connector_id,
        status=ConnectorBindingStatus.connected,
        metadata={"feed_url": resource_url, "feed_title": "Feed"},
    )
    resource = ConnectorResource(
        id=f"{connector_id}-resource",
        scope_id="scope:feed",
        connector_id=connector_id,
        binding_id=binding.id,
        external_id="feed-resource",
        kind=f"{connector_id}_feed",
        display_name="Feed",
        url=resource_url,
        selected=True,
    )
    return ConnectorOperationContext(
        "scope:feed",
        connector_id,
        binding=binding,
        resources=(resource,),
        cursors=(() if cursor is None else (cursor,)),
    )


@pytest.mark.parametrize(
    ("provider_type", "body", "event_type", "clean_content"),
    (
        (RssProvider, RSS, "rss.item", "Safe text"),
        (AtomProvider, ATOM, "atom.item", "Atom text"),
    ),
)
async def test_200_new_item_duplicate_and_304_conditional_polling(
    provider_type: type[RssProvider] | type[AtomProvider],
    body: bytes,
    event_type: str,
    clean_content: str,
) -> None:
    client, transport = _client(
        [
            _response(
                200,
                body,
                {
                    "etag": '"v1"',
                    "last-modified": "Fri, 17 Jul 2026 18:00:00 GMT",
                },
            ),
            _response(200, body, {"etag": '"v1"'}),
            _response(304, headers={"etag": '"v1"'}),
        ]
    )
    provider = provider_type(client)
    first = await provider.sync(_context(provider.manifest.id))
    assert len(first.changes) == 1
    change = first.changes[0]
    assert change.taint is ContentTaint.tainted
    assert change.event is not None
    assert change.event.type == event_type
    assert change.event.payload["item"]["content"] == clean_content
    assert "bad()" not in change.event.payload["item"]["content"]
    update = first.cursor_updates[0]
    cursor = ConnectorCursor(
        id="cursor",
        scope_id="scope:feed",
        connector_id=provider.manifest.id,
        binding_id=f"{provider.manifest.id}-binding",
        resource_id=f"{provider.manifest.id}-resource",
        stream=update.stream,
        value=update.value,
        etag=update.etag,
        last_modified=update.last_modified,
        revision=update.revision,
    )

    duplicate = await provider.sync(_context(provider.manifest.id, cursor=cursor))
    assert duplicate.changes == ()
    not_modified = await provider.sync(_context(provider.manifest.id, cursor=cursor))
    assert not_modified.changes == ()
    assert dict(transport.targets[1].request_headers) == {
        "if-none-match": '"v1"',
        "if-modified-since": "Fri, 17 Jul 2026 18:00:00 GMT",
    }
    assert dict(transport.targets[2].request_headers) == dict(
        transport.targets[1].request_headers
    )


async def test_duplicate_item_ids_in_one_feed_emit_once() -> None:
    duplicate = RSS.replace(
        b"</channel>",
        b"<item><guid>item-1</guid><title>Duplicate</title>"
        b"<description>same delivery</description></item></channel>",
    )
    client, _ = _client([_response(200, duplicate)])
    result = await RssProvider(client).sync(_context("rss"))
    assert len(result.changes) == 1


@pytest.mark.parametrize(
    ("provider_type", "body"),
    ((RssProvider, RSS), (AtomProvider, ATOM)),
)
async def test_setup_and_resources_follow_safe_redirects(
    provider_type: type[RssProvider] | type[AtomProvider],
    body: bytes,
) -> None:
    client, transport = _client(
        [
            _response(302, headers={"location": "/canonical"}),
            _response(200, body),
            _response(200, body),
        ]
    )
    provider = provider_type(client)
    setup = await provider.setup(
        ConnectorOperationContext("scope:feed", provider.manifest.id),
        {"url": "https://example.com/start"},
    )
    assert setup.binding.metadata["feed_url"] == "https://example.com/canonical"
    binding = ConnectorBinding(
        id="binding",
        scope_id="scope:feed",
        connector_id=provider.manifest.id,
        status=ConnectorBindingStatus.connected,
        metadata=setup.binding.metadata,
    )
    resources = await provider.list_resources(
        ConnectorOperationContext("scope:feed", provider.manifest.id, binding=binding)
    )
    assert len(resources.resources) == 1
    assert resources.resources[0].selected
    assert resources.resources[0].url == "https://example.com/canonical"
    assert [target.request_target for target in transport.targets[:2]] == [
        "/start",
        "/canonical",
    ]


@pytest.mark.parametrize(
    ("provider_type", "manifest", "body"),
    ((RssProvider, rss_manifest, RSS), (AtomProvider, atom_manifest, ATOM)),
)
async def test_generic_setup_creates_selected_resource_and_recurring_cadence(
    provider_type: type[RssProvider] | type[AtomProvider],
    manifest: ConnectorManifest,
    body: bytes,
) -> None:
    client, _ = _client([_response(200, body), _response(200, body)])
    provider = provider_type(client)
    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                provider.manifest,
                lambda: provider,
                f"tests.{provider.manifest.id}",
            ),
        )
    )
    repository = InMemoryConnectorRepository("scope:feed")
    service = ConnectorService(registry, repository)
    outcome = await service.setup(
        provider.manifest.id,
        {"url": "https://example.com/feed"},
    )
    assert outcome.binding.sync_cadence_seconds == 900
    assert outcome.binding.next_sync_at is not None
    resources = await service.refresh_resources(provider.manifest.id)
    assert len(resources) == 1
    assert resources[0]["selected"] is True
    assert provider.manifest is manifest


@pytest.mark.parametrize("provider_type", (RssProvider, AtomProvider))
async def test_malformed_feed_is_rejected(
    provider_type: type[RssProvider] | type[AtomProvider],
) -> None:
    client, _ = _client([_response(200, b"<feed><broken>")])
    with pytest.raises(ValueError, match="malformed"):
        await provider_type(client).setup(
            ConnectorOperationContext("scope:feed", provider_type.manifest.id),
            {"url": "https://example.com/feed"},
        )


@pytest.mark.parametrize(
    "url",
    (
        "http://127.0.0.1/feed",
        "http://169.254.169.254/feed",
        "http://100.64.0.1/feed",
    ),
)
async def test_feed_setup_rejects_private_link_local_and_rfc6598(url: str) -> None:
    client, _ = _client([])
    with pytest.raises(ConnectorNetworkError, match="non-public"):
        await RssProvider(client).setup(
            ConnectorOperationContext("scope:feed", "rss"),
            {"url": url},
        )


async def test_feed_redirect_to_private_address_is_rejected() -> None:
    client, transport = _client(
        [_response(302, headers={"location": "http://169.254.169.254/feed"})]
    )
    with pytest.raises(ConnectorNetworkError, match="non-public"):
        await AtomProvider(client).setup(
            ConnectorOperationContext("scope:feed", "atom"),
            {"url": "https://example.com/feed"},
        )
    assert len(transport.targets) == 1


async def test_feed_setup_rejects_private_dns_and_uses_pinned_resolution_once() -> None:
    resolutions = 0

    async def resolver(host: str, port: int) -> tuple[str, ...]:
        nonlocal resolutions
        resolutions += 1
        return ("93.184.216.34",) if host == "example.com" else ("10.0.0.1",)

    private_client, _ = _client([], resolver=resolver)
    with pytest.raises(ConnectorNetworkError, match="non-public"):
        await RssProvider(private_client).setup(
            ConnectorOperationContext("scope:feed", "rss"),
            {"url": "https://private.example/feed"},
        )
    pinned_client, transport = _client([_response(200, RSS)], resolver=resolver)
    await RssProvider(pinned_client).setup(
        ConnectorOperationContext("scope:feed", "rss"),
        {"url": "https://example.com/feed"},
    )
    assert transport.targets[0].addresses == ("93.184.216.34",)
    assert resolutions == 2


def test_feed_manifests_are_separate_recurring_provider_local_connectors() -> None:
    assert rss_manifest.id == "rss"
    assert atom_manifest.id == "atom"
    assert rss_manifest != atom_manifest
    assert rss_manifest.default_sync_cadence_seconds == 900
    assert atom_manifest.default_sync_cadence_seconds == 900
    assert rss_manifest.actions == atom_manifest.actions == ()


def test_feed_providers_are_discovered_separately() -> None:
    registry = discover_connector_registry()
    rss_registration = registry.get("rss")
    atom_registration = registry.get("atom")
    assert rss_registration is not None
    assert atom_registration is not None
    assert rss_registration.manifest == rss_manifest
    assert atom_registration.manifest == atom_manifest


def test_rss_fallbacks_prefer_full_content_and_pubdate_by_semantics() -> None:
    body = b"""<rss version="2.0"
        xmlns:content="http://purl.org/rss/1.0/modules/content/"
        xmlns:dc="http://purl.org/dc/elements/1.1/">
      <channel><title>Preference RSS</title><item>
        <guid>preference-rss</guid>
        <description>Summary first</description>
        <content:encoded><![CDATA[<p>Full RSS content</p>]]></content:encoded>
        <dc:date>2026-07-18T02:00:00Z</dc:date>
        <pubDate>Sat, 18 Jul 2026 01:00:00 GMT</pubDate>
      </item></channel>
    </rss>"""
    item = parse_rss(body, "https://example.com/feed").items[0]
    assert item.content == "Full RSS content"
    assert item.published == "Sat, 18 Jul 2026 01:00:00 GMT"


def test_atom_fallbacks_prefer_full_content_and_published_by_semantics() -> None:
    body = b"""<feed xmlns="http://www.w3.org/2005/Atom">
      <title>Preference Atom</title><entry>
        <id>preference-atom</id>
        <summary>Summary first</summary>
        <content type="html">&lt;p&gt;Full Atom content&lt;/p&gt;</content>
        <updated>2026-07-18T02:00:00Z</updated>
        <published>2026-07-18T01:00:00Z</published>
      </entry>
    </feed>"""
    item = parse_atom(body, "https://example.com/feed").items[0]
    assert item.content == "Full Atom content"
    assert item.published == "2026-07-18T01:00:00Z"


def test_feed_context_rejects_cross_provider_resource() -> None:
    binding = ConnectorBinding(
        id="binding",
        scope_id="scope:feed",
        connector_id="rss",
        status=ConnectorBindingStatus.connected,
    )
    resource = ConnectorResource(
        id="resource",
        scope_id="scope:feed",
        connector_id="atom",
        binding_id="binding",
        external_id="feed",
        kind="atom_feed",
        display_name="Atom",
    )
    with pytest.raises(ValueError, match="crosses"):
        ConnectorOperationContext(
            "scope:feed",
            "rss",
            binding=binding,
            resources=(resource,),
        )
