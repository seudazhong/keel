"""Durable IM reply senders: deliver an outbox reply through a provider's HTTP API (WS-E/J).

The restart-safe reply reconciler (:func:`keel_worker.runs.send_im_replies_tick`) decrypts a
leased reply intent and hands it to the provider's :class:`~keel_core.im_routing.ReplySender`,
which posts it to the OneBot HTTP API or the Telegram Bot ``sendMessage`` API. The reply target
(group vs DM, chat id) comes from the durable intent, not from any ambient session key. Bot
credentials come from settings (the single configured bot per provider, matching the inbound
gateway); the durable idempotency key is forwarded where the provider supports it, otherwise
delivery is at-least-once (the outbox marks ``sent`` after a confirmed post so a crash after the
ack does not resend).
"""

from __future__ import annotations

import httpx

from keel_core.config import Settings
from keel_core.im_routing import ImChatKind, ImProvider, ImReplyIntent, ReplySender

_TIMEOUT = 10.0


class OneBotReplySender:
    """Send a reply through the OneBot v11 HTTP API (group or private message)."""

    def __init__(self, api_base: str, access_token: str) -> None:
        self._api_base = api_base.rstrip("/")
        self._headers = {"Authorization": f"Bearer {access_token}"} if access_token else {}

    async def send(self, intent: ImReplyIntent, text_payload: str) -> str:
        if intent.chat_kind is ImChatKind.group:
            endpoint = "send_group_msg"
            data: dict[str, object] = {
                "group_id": int(intent.external_chat_id),
                "message": text_payload,
            }
        else:
            endpoint = "send_private_msg"
            data = {"user_id": int(intent.external_chat_id), "message": text_payload}
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(
                f"{self._api_base}/{endpoint}", json=data, headers=self._headers
            )
            resp.raise_for_status()
            body = resp.json()
        message_id = (body or {}).get("data", {}).get("message_id")
        return str(message_id) if message_id is not None else ""


class TelegramReplySender:
    """Send a reply through the Telegram Bot ``sendMessage`` API."""

    def __init__(self, bot_token: str) -> None:
        self._url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    async def send(self, intent: ImReplyIntent, text_payload: str) -> str:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(
                self._url,
                json={"chat_id": int(intent.external_chat_id), "text": text_payload},
            )
            resp.raise_for_status()
            body = resp.json()
        message_id = (body or {}).get("result", {}).get("message_id")
        return str(message_id) if message_id is not None else ""


def build_im_senders(settings: Settings) -> dict[ImProvider, ReplySender]:
    """Build the configured provider reply senders (only providers with credentials wired)."""
    senders: dict[ImProvider, ReplySender] = {}
    if settings.onebot_api_base:
        senders[ImProvider.onebot] = OneBotReplySender(
            settings.onebot_api_base, settings.onebot_access_token
        )
    if settings.telegram_bot_token:
        senders[ImProvider.telegram] = TelegramReplySender(settings.telegram_bot_token)
    return senders


__all__ = ["OneBotReplySender", "TelegramReplySender", "build_im_senders"]
