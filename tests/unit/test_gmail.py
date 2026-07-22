"""Real Gmail read connector — formatting + the token-store-backed ActionFn.

Hermetic: the blocking Gmail fetch (:func:`keel_core.gmail._fetch_inbox_sync`) is
monkeypatched, so these never touch the network.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
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

    def fake_send(
        creds_json: str, to: str, subject: str, body: str, message_id: str
    ) -> tuple[str, str]:
        seen.update(creds=creds_json, to=to, subject=subject, body=body, message_id=message_id)
        return "msgid123", "ROTATED"

    monkeypatch.setattr(gmail, "_send_sync", fake_send)
    action = make_gmail_send_action(store)

    result = await action(
        {
            "to": " me@example.com ",
            "subject": "Hi",
            "body": "hello",
            "idempotency_key": "key-1",
        },
        _ctx(),
    )

    assert json.loads(result) == {"id": "msgid123", "message_id": seen["message_id"]}
    assert seen["creds"] == "ORIG"
    assert seen["to"] == "me@example.com"
    assert seen["subject"] == "Hi"
    assert seen["body"] == "hello"
    assert seen["message_id"] == gmail.gmail_message_id("web:local", "key-1")
    assert store.puts == ["ROTATED"]  # rotated creds persisted


async def test_send_action_requires_to(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_send(
        creds_json: str, to: str, subject: str, body: str, message_id: str
    ) -> tuple[str, str]:
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


# --- R1B: ambiguous-outcome classification + reconciliation (C4) --------------------


async def test_send_action_raises_provider_ambiguous_on_transport_failure_after_submit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timeout while waiting for Gmail's response must not be an ordinary failure —
    the request may have already reached Gmail before the response was lost."""
    from keel_core.connectors import ProviderAmbiguousError

    def fake_send(
        creds_json: str, to: str, subject: str, body: str, message_id: str
    ) -> tuple[str, str]:
        raise TimeoutError("response lost after submission")

    monkeypatch.setattr(gmail, "_send_sync", fake_send)
    action = make_gmail_send_action(FakeTokenStore("ORIG"))
    with pytest.raises(ProviderAmbiguousError):
        await action({"to": "me@example.com", "idempotency_key": "k1"}, _ctx())


async def test_send_action_validation_error_is_ordinary_not_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-send validation failure (missing 'to') is provably not ambiguous."""

    def fake_send(
        creds_json: str, to: str, subject: str, body: str, message_id: str
    ) -> tuple[str, str]:
        raise AssertionError("must not be called")

    monkeypatch.setattr(gmail, "_send_sync", fake_send)
    action = make_gmail_send_action(FakeTokenStore("ORIG"))
    with pytest.raises(GmailError):
        await action({"subject": "x", "body": "y"}, _ctx())


async def test_gmail_reconciler_confirms_when_message_id_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )
    from keel_core.gmail import make_gmail_reconciler

    def fake_search(creds_json: str, message_id: str) -> str | None:
        assert message_id == gmail.gmail_message_id("web:local", "k1")
        return "gmail-msg-999"

    monkeypatch.setattr(gmail, "_search_by_message_id_sync", fake_search)
    reconciler = make_gmail_reconciler(FakeTokenStore("ORIG"))
    outcome = await reconciler.reconcile(
        ConnectorReconciliationRequest(
            scope_id="web:local",
            connector_id="gmail",
            action_name="email_send",
            idempotency_key="k1",
            resource_id="",
            provider_ref="",
            canonical_args="",
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.confirmed
    assert outcome.provider_ref == "gmail-msg-999"


async def test_gmail_reconciler_never_treats_not_found_as_proven_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )
    from keel_core.gmail import make_gmail_reconciler

    def fake_search(creds_json: str, message_id: str) -> str | None:
        return None

    monkeypatch.setattr(gmail, "_search_by_message_id_sync", fake_search)
    reconciler = make_gmail_reconciler(FakeTokenStore("ORIG"))
    outcome = await reconciler.reconcile(
        ConnectorReconciliationRequest(
            scope_id="web:local",
            connector_id="gmail",
            action_name="email_send",
            idempotency_key="k1",
            resource_id="",
            provider_ref="",
            canonical_args="",
            unknown_since=datetime.now(UTC) - timedelta(minutes=5),
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.incapable


async def test_gmail_reconciler_does_not_report_absent_before_search_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )
    from keel_core.gmail import make_gmail_reconciler

    monkeypatch.setattr(gmail, "_search_by_message_id_sync", lambda _creds, _message_id: None)
    outcome = await make_gmail_reconciler(FakeTokenStore("ORIG")).reconcile(
        ConnectorReconciliationRequest(
            scope_id="web:local",
            connector_id="gmail",
            action_name="email_send",
            idempotency_key="k1",
            resource_id="",
            provider_ref="",
            canonical_args="",
            unknown_since=datetime.now(UTC),
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.incapable


def test_gmail_search_query_uses_bare_rfc822_message_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, str] = {}

    class _Request:
        def execute(self) -> dict[str, list[dict[str, str]]]:
            return {"messages": []}

    class _Messages:
        def list(self, *, userId: str, q: str) -> _Request:
            assert userId == "me"
            seen["query"] = q
            return _Request()

    class _Users:
        def messages(self) -> _Messages:
            return _Messages()

    class _Service:
        def users(self) -> _Users:
            return _Users()

        def close(self) -> None:
            return None

    monkeypatch.setattr(gmail, "_load_credentials", lambda _raw: object())
    import googleapiclient.discovery

    monkeypatch.setattr(googleapiclient.discovery, "build", lambda *_args, **_kwargs: _Service())
    message_id = gmail.gmail_message_id("web:local", "key-1")
    assert gmail._search_by_message_id_sync("{}", message_id) is None
    assert seen["query"] == f"rfc822msgid:{message_id[1:-1]}"


async def test_gmail_reconciler_is_incapable_without_credentials() -> None:
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )
    from keel_core.gmail import make_gmail_reconciler

    reconciler = make_gmail_reconciler(FakeTokenStore(None))
    outcome = await reconciler.reconcile(
        ConnectorReconciliationRequest(
            scope_id="web:local",
            connector_id="gmail",
            action_name="email_send",
            idempotency_key="k1",
            resource_id="",
            provider_ref="",
            canonical_args="",
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.incapable


async def test_gmail_provider_exposes_a_reconciler_when_credentials_present() -> None:
    provider = gmail_provider.GmailProvider()
    context = ConnectorActionContext("web:local", credential_store=FakeTokenStore("ORIG"))
    reconciler = provider.build_reconciler(context)
    assert reconciler is not None


async def test_gmail_provider_reconciler_is_none_without_credential_store() -> None:
    provider = gmail_provider.GmailProvider()
    context = ConnectorActionContext("web:local")
    assert provider.build_reconciler(context) is None


async def test_end_to_end_ambiguous_send_becomes_unknown_then_reconciled_via_connector_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full ConnectorTool integration: an ambiguous Gmail send becomes `unknown`, blocks
    retry, and the Gmail reconciler resolves it — the C4 acceptance scenario end to end."""
    from keel_core.connectors import ConnectorTool
    from keel_core.effect_store import InMemoryEffectStore
    from keel_core.effects import EffectStatus

    attempts = {"n": 0}

    def fake_send(
        creds_json: str, to: str, subject: str, body: str, message_id: str
    ) -> tuple[str, str]:
        attempts["n"] += 1
        raise TimeoutError("response lost after submission")

    monkeypatch.setattr(gmail, "_send_sync", fake_send)
    send_action = make_gmail_send_action(FakeTokenStore("ORIG"))
    effects = InMemoryEffectStore()
    tool = ConnectorTool(
        name="email_send",
        description="",
        action=send_action,
        outbound=True,
        effect_store=effects,
        provider="gmail",
    )
    ctx = _ctx()
    args = {"to": "me@example.com", "idempotency_key": "unique-key"}

    result = await tool.run(args, ctx)
    assert result.ok is False
    assert result.effect_status == EffectStatus.unknown.value
    # A second attempt with the same key must not re-invoke the (still ambiguous) send.
    blocked = await tool.run(args, ctx)
    assert blocked.effect_status == EffectStatus.unknown.value
    assert attempts["n"] == 1

    def fake_search_found(creds_json: str, message_id: str) -> str | None:
        return "server-side-id"

    monkeypatch.setattr(gmail, "_search_by_message_id_sync", fake_search_found)
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )
    from keel_core.gmail import make_gmail_reconciler

    reconciler = make_gmail_reconciler(FakeTokenStore("ORIG"))
    stored_effect = effects.snapshot()[0]
    outcome = await reconciler.reconcile(
        ConnectorReconciliationRequest(
            scope_id=ctx.scope_id,
            connector_id="gmail",
            action_name="email_send",
            idempotency_key="unique-key",
            resource_id="",
            provider_ref="",
            canonical_args=stored_effect.canonical_args,
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.confirmed
    reconciled = await effects.reconcile_confirmed(
        ctx.scope_id, stored_effect.id, provider_ref="server-side-id", result="sent"
    )
    assert reconciled.status is EffectStatus.reconciled_confirmed
