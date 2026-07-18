"""Atom connector manifest and provider."""

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
    parse_atom,
)

manifest = ConnectorManifest(
    id="atom",
    name="Atom",
    description="Poll an Atom feed and emit new entries as tainted events.",
    icon="⚛️",
    auth_kind=ConnectorAuthKind.url,
    capabilities=(
        ConnectorCapability.read,
        ConnectorCapability.sync,
        ConnectorCapability.resources,
    ),
    setup_fields=(
        ConnectorSetupField(
            "url",
            "Atom feed URL",
            input_type="url",
            help_text="Public HTTP(S) Atom feed URL.",
        ),
    ),
    setup_action_label="Add Atom feed",
    resource_label="Atom feeds",
    target_fields=(
        ConnectorTargetField(
            ConnectorTargetKind.trigger_session,
            "Trigger session",
            help_text="Session that receives new Atom entry events.",
        ),
    ),
    default_sync_cadence_seconds=FEED_POLL_CADENCE_SECONDS,
)


class AtomProvider(FeedProvider):
    manifest = manifest
    feed_kind = "atom"
    parser = staticmethod(parse_atom)

    def __init__(self, http_client: ConnectorHttpClient | None = None) -> None:
        super().__init__(http_client)


def factory() -> AtomProvider:
    return AtomProvider()


__all__ = ["AtomProvider", "factory", "manifest"]
