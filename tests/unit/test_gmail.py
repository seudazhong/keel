"""Real Gmail read connector — formatting + the token-store-backed ActionFn.

Hermetic: the blocking Gmail fetch (:func:`keel_core.gmail._fetch_inbox_sync`) is
monkeypatched, so these never touch the network.
"""

from __future__ import annotations

import pytest

from keel_core import gmail
from keel_core.gmail import GmailError, format_inbox, make_gmail_inbox_action
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
