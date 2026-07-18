"""RSS connector manifest and provider."""

from __future__ import annotations

from keel_core.connector_contracts import (
    ConnectorAuthKind,
    ConnectorCapability,
    ConnectorManifest,
    ConnectorSetupField,
    ConnectorTargetField,
    ConnectorTargetKind,
)
from keel_core.connector_network import ConnectorHttpClient
from keel_core.connector_providers._feeds import (
    FEED_POLL_CADENCE_SECONDS,
    FeedProvider,
    parse_rss,
)

manifest = ConnectorManifest(
    id="rss",
    name="RSS",
    description="Poll an RSS feed and emit new items as tainted events.",
    icon="📰",
    auth_kind=ConnectorAuthKind.url,
    capabilities=(
        ConnectorCapability.read,
        ConnectorCapability.sync,
        ConnectorCapability.resources,
    ),
    setup_fields=(
        ConnectorSetupField(
            "url",
            "RSS feed URL",
            input_type="url",
            help_text="Public HTTP(S) RSS feed URL.",
        ),
    ),
    setup_action_label="Add RSS feed",
    resource_label="RSS feeds",
    target_fields=(
        ConnectorTargetField(
            ConnectorTargetKind.trigger_session,
            "Trigger session",
            help_text="Session that receives new RSS item events.",
        ),
    ),
    default_sync_cadence_seconds=FEED_POLL_CADENCE_SECONDS,
)


class RssProvider(FeedProvider):
    manifest = manifest
    feed_kind = "rss"
    parser = staticmethod(parse_rss)

    def __init__(self, http_client: ConnectorHttpClient | None = None) -> None:
        super().__init__(http_client)


def factory() -> RssProvider:
    return RssProvider()


__all__ = ["RssProvider", "factory", "manifest"]
