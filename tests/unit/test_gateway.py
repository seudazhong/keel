"""OneBot IM gateway tests: session keys, wake rules, rate limit, safe toolset."""

from __future__ import annotations

from pathlib import Path

from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_server.gateway import OneBotEvent, OneBotGateway, RateLimiter, session_key, wake_rule


def _group(text: str, *, group_id: int = 1, self_id: int = 100) -> OneBotEvent:
    return OneBotEvent(
        post_type="message",
        message_type="group",
        group_id=group_id,
        self_id=self_id,
        raw_message=text,
    )


def _dm(text: str, *, user_id: int = 7) -> OneBotEvent:
    return OneBotEvent(
        post_type="message", message_type="private", user_id=user_id, raw_message=text
    )


def test_session_key_maps_group_and_dm() -> None:
    assert session_key(_group("hi", group_id=42)) == "qq:group:42"
    assert session_key(_dm("hi", user_id=9)) == "qq:private:9"


def test_wake_rule_group_requires_mention_or_prefix() -> None:
    # Plain group chatter -> stay quiet.
    assert not wake_rule(_group("just chatting"), self_id=100, prefixes=("/keel",)).woke
    # @-mention -> wake, mention stripped.
    d = wake_rule(_group("[CQ:at,qq=100] what's the weather"), self_id=100, prefixes=("/keel",))
    assert d.woke and d.text == "what's the weather"
    # Command prefix -> wake, prefix stripped.
    d2 = wake_rule(_group("/keel hello"), self_id=100, prefixes=("/keel",))
    assert d2.woke and d2.text == "hello"


def test_wake_rule_dm_always_wakes() -> None:
    d = wake_rule(_dm("hello there"), self_id=100, prefixes=("/keel",))
    assert d.woke and d.text == "hello there"


def test_rate_limiter_sliding_window() -> None:
    limiter = RateLimiter(limit=2, window=60.0)
    assert limiter.allow("k", now=0.0)
    assert limiter.allow("k", now=1.0)
    assert not limiter.allow("k", now=2.0)  # third within window -> blocked
    assert limiter.allow("k", now=61.5)  # first hit aged out -> allowed again
    assert limiter.allow("other", now=2.0)  # independent per key


def _reply_provider(text: str) -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [[ProviderChunk(delta=text, finish_reason=FinishReason.end_turn)]]
    )


async def test_gateway_runs_and_replies(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []

    async def send(key: str, text: str) -> None:
        sent.append((key, text))

    gateway = OneBotGateway(
        provider=_reply_provider("hello from keel"),
        send=send,
        workspace=tmp_path,
        self_id=100,
    )
    await gateway.handle(
        {"post_type": "message", "message_type": "private", "user_id": 7, "raw_message": "hi"}
    )
    assert sent == [("qq:private:7", "hello from keel")]


async def test_gateway_stays_quiet_without_wake(tmp_path: Path) -> None:
    sent: list[tuple[str, str]] = []

    async def send(key: str, text: str) -> None:
        sent.append((key, text))

    gateway = OneBotGateway(
        provider=_reply_provider("hi"), send=send, workspace=tmp_path, self_id=100
    )
    await gateway.handle(
        {
            "post_type": "message",
            "message_type": "group",
            "group_id": 1,
            "self_id": 100,
            "raw_message": "unrelated group chatter",
        }
    )
    assert sent == []  # no mention/prefix -> no reply


async def test_untrusted_surface_cannot_reach_write_tools(tmp_path: Path) -> None:
    """An IM message can never trigger a write: the safe toolset + deny gate hold."""
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
    gateway = OneBotGateway(provider=provider, send=send, workspace=tmp_path, self_id=100)
    await gateway.handle(
        {
            "post_type": "message",
            "message_type": "private",
            "user_id": 7,
            "raw_message": "write a file",
        }
    )
    assert not (tmp_path / "p.txt").exists()  # write never happened (untrusted -> safe toolset)
    assert sent == [("qq:private:7", "done")]
