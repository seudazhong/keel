"""Real Gmail read connector — formatting + the token-store-backed ActionFn.

Hermetic: the blocking Gmail fetch (:func:`keel_core.gmail._fetch_inbox_sync`) is
monkeypatched, so these never touch the network.
"""

from __future__ import annotations

from typing import Any

import pytest

from keel_core import gmail
from keel_core.config import Settings
from keel_core.connector_contracts import (
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionSemantics,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connector_providers import gmail as gmail_provider
from keel_core.connectors import ConnectorActionUserError, ConnectorTool
from keel_core.gmail import (
    GmailError,
    format_inbox,
    make_gmail_inbox_action,
    make_gmail_send_action,
)
from keel_core.protocols import ToolContext


class FakeTokenStore:
    """Records puts so tests can assert credential rotation is persisted."""

    def __init__(self, initial: str | None) -> None:
        self._value = initial
        self.puts: list[str] = []

    async def get(self, connector_id: str) -> str | None:
        return self._value

    async def put(self, connector_id: str, secret: str) -> None:
        self._value = secret
        self.puts.append(secret)


def _ctx() -> ToolContext:
    return ToolContext(scope_id="web:local", session_id="digest:web:local")


def test_format_inbox_shape() -> None:
    rendered = format_inbox(
        [
            {"from": "a@x.com", "subject": "Hi", "snippet": "hello"},
            {"from": "b@y.com", "subject": "Yo", "snippet": "world"},
        ]
    )
    assert rendered == "[0] a@x.com — Hi: hello\n[1] b@y.com — Yo: world"


def test_format_inbox_tolerates_missing_fields() -> None:
    assert format_inbox([{}]) == "[0]  — : "


async def test_action_raises_when_unauthorized() -> None:
    action = make_gmail_inbox_action(FakeTokenStore(None))
    with pytest.raises(GmailError, match="not authorized"):
        await action({}, _ctx())


async def test_connector_tool_surfaces_reconnect_guidance() -> None:
    async def expired(args: dict[str, Any], ctx: ToolContext) -> str:
        raise ConnectorActionUserError(
            "Gmail authorization expired or was revoked. Reconnect Gmail in Connectors."
        )

    result = await ConnectorTool(
        name="inbox_list",
        description="List inbox messages.",
        action=expired,
    ).run({}, _ctx())
    assert result.ok is False
    assert result.output == (
        "Gmail authorization expired or was revoked. Reconnect Gmail in Connectors."
    )


async def test_action_fetches_and_persists_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeTokenStore("ORIG")

    def fake_fetch(creds_json: str, max_messages: int) -> tuple[str, str]:
        assert creds_json == "ORIG"
        assert max_messages == 5
        return "REAL INBOX", "ROTATED"

    monkeypatch.setattr(gmail, "_fetch_inbox_sync", fake_fetch)
    action = make_gmail_inbox_action(store)

    result = await action({}, _ctx())

    assert result == "REAL INBOX"
    assert store.puts == ["ROTATED"]  # rotated creds persisted


async def test_action_skips_persist_when_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeTokenStore("ORIG")

    def fake_fetch(creds_json: str, max_messages: int) -> tuple[str, str]:
        return "INBOX", "ORIG"  # no rotation

    monkeypatch.setattr(gmail, "_fetch_inbox_sync", fake_fetch)
    action = make_gmail_inbox_action(store, max_messages=3)

    result = await action({}, _ctx())

    assert result == "INBOX"
    assert store.puts == []  # nothing to persist


async def test_action_reads_and_preserves_versioned_credential_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = CredentialEnvelope("oauth", {"refresh_token": "old"}).serialize()
    store = FakeTokenStore(stored)

    def fake_fetch(creds_json: str, max_messages: int) -> tuple[str, str]:
        assert '"refresh_token":"old"' in creds_json
        return "INBOX", '{"refresh_token":"rotated"}'

    monkeypatch.setattr(gmail, "_fetch_inbox_sync", fake_fetch)
    assert await make_gmail_inbox_action(store)({}, _ctx()) == "INBOX"
    rotated = CredentialEnvelope.parse(store.puts[0])
    assert rotated is not None and rotated.values["refresh_token"] == "rotated"


async def test_send_action_sends_and_persists_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeTokenStore("ORIG")
    seen: dict[str, str] = {}

    def fake_send(creds_json: str, to: str, subject: str, body: str) -> tuple[str, str]:
        seen.update(creds=creds_json, to=to, subject=subject, body=body)
        return "msgid123", "ROTATED"

    monkeypatch.setattr(gmail, "_send_sync", fake_send)
    action = make_gmail_send_action(store)

    result = await action({"to": " me@example.com ", "subject": "Hi", "body": "hello"}, _ctx())

    assert result == "sent (id=msgid123)"
    assert seen == {"creds": "ORIG", "to": "me@example.com", "subject": "Hi", "body": "hello"}
    assert store.puts == ["ROTATED"]  # rotated creds persisted


async def test_send_action_requires_to(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_send(creds_json: str, to: str, subject: str, body: str) -> tuple[str, str]:
        raise AssertionError("_send_sync must not be called without a recipient")

    monkeypatch.setattr(gmail, "_send_sync", fake_send)
    action = make_gmail_send_action(FakeTokenStore("ORIG"))
    with pytest.raises(GmailError, match="requires a 'to'"):
        await action({"subject": "x", "body": "y"}, _ctx())


async def test_send_action_raises_when_unauthorized() -> None:
    action = make_gmail_send_action(FakeTokenStore(None))
    with pytest.raises(GmailError, match="not authorized"):
        await action({"to": "me@example.com"}, _ctx())


def test_gmail_provider_declares_and_builds_actions_locally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        gmail_provider,
        "get_settings",
        lambda: Settings(gmail_enabled=True, gmail_send_enabled=True),
    )
    actions = gmail_provider.GmailProvider().build_actions(
        ConnectorActionContext("web:local", credential_store=FakeTokenStore(None))
    )
    assert [action.manifest.name for action in actions] == ["inbox_list", "email_send"]
    assert actions[0].manifest.semantics is ConnectorActionSemantics.read
    assert actions[1].manifest.semantics is ConnectorActionSemantics.outbound
    assert actions[1].manifest.approval is ConnectorActionApproval.tainted
