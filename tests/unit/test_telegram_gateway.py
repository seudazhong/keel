"""Telegram IM adapter tests: session keys, wake rules, safe run+reply (mirrors OneBot)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_server.gateway import TelegramGateway, parse_telegram_inbound


def _update(text: str, *, chat_id: int, chat_type: str = "private") -> dict[str, Any]:
    return {"message": {"text": text, "chat": {"id": chat_id, "type": chat_type}}}


def test_session_key_maps_group_and_dm() -> None:
    dm = parse_telegram_inbound(
        _update("hi", chat_id=9), bot_username="keelbot", prefixes=("/keel",)
    )
    assert dm is not None and dm.session_key == "tg:private:9" and not dm.is_group
    grp = parse_telegram_inbound(
        _update("@keelbot hi", chat_id=-100, chat_type="supergroup"),
        bot_username="keelbot",
        prefixes=("/keel",),
    )
    assert grp is not None and grp.session_key == "tg:group:-100" and grp.is_group


def test_group_requires_mention_or_prefix() -> None:
    kw = {"bot_username": "keelbot", "prefixes": ("/keel",)}
    # Plain group chatter -> ignored.
    assert (
        parse_telegram_inbound(_update("just chatting", chat_id=-1, chat_type="group"), **kw)
        is None
    )
    # @-mention -> wake, mention stripped.
    m = parse_telegram_inbound(_update("@keelbot what's up", chat_id=-1, chat_type="group"), **kw)
    assert m is not None and m.text == "what's up"
    # Command prefix -> wake, prefix stripped.
    p = parse_telegram_inbound(_update("/keel hello", chat_id=-1, chat_type="group"), **kw)
    assert p is not None and p.text == "hello"


def test_dm_always_wakes() -> None:
    m = parse_telegram_inbound(
        _update("hello there", chat_id=7), bot_username="keelbot", prefixes=("/keel",)
    )
    assert m is not None and m.text == "hello there"


def _reply_provider(text: str) -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [[ProviderChunk(delta=text, finish_reason=FinishReason.end_turn)]]
    )


async def test_gateway_runs_and_replies(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []

    async def send(key: str, text: str) -> None:
        sent.append((key, text))

    gateway = TelegramGateway(
        provider=_reply_provider("hello from keel"),
        send=send,
        workspace=tmp_path,
        bot_username="keelbot",
    )
    await gateway.handle(_update("hi", chat_id=7))
    assert sent == [("tg:private:7", "hello from keel")]


async def test_gateway_stays_quiet_without_wake(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []

    async def send(key: str, text: str) -> None:
        sent.append((key, text))

    gateway = TelegramGateway(
        provider=_reply_provider("hi"), send=send, workspace=tmp_path, bot_username="keelbot"
    )
    await gateway.handle(_update("unrelated chatter", chat_id=-1, chat_type="group"))
    assert sent == []


async def test_untrusted_surface_cannot_reach_write_tools(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []

    async def send(key: str, text: str) -> None:
        sent.append((key, text))

    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c1", name="write", arguments={"path": "p.txt", "content": "x"}
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )
    gateway = TelegramGateway(provider=provider, send=send, workspace=tmp_path, bot_username="k")
    await gateway.handle(_update("write a file", chat_id=7))
    assert not (tmp_path / "p.txt").exists()  # safe toolset holds on the untrusted surface
    assert sent == [("tg:private:7", "done")]
