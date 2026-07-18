"""OneBot (QQ) + Telegram IM webhooks (WS-E/J), authenticated + replay-safe (M3.3).

``POST /v1/gateway/onebot`` and ``POST /v1/gateway/telegram`` receive IM events. Each
request is **verified before dispatch**: a OneBot HMAC-SHA1 body signature or Telegram
shared-secret header (constant-time), then a durable replay check that drops a re-sent
delivery. Handling runs in a background task so the bot framework gets an immediate ack;
the reply is sent back over the platform's HTTP API. Namespaced under ``/v1``
(additive-only, G14).
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, status

from keel_core.config import get_settings
from keel_core.webhooks import (
    InMemoryWebhookReplayStore,
    WebhookReplayStore,
    onebot_delivery_id,
    telegram_delivery_id,
    verify_onebot_signature,
    verify_telegram_secret,
)
from keel_server.gateway import OneBotGateway, TelegramGateway
from keel_server.gateway.durable import (
    DurableImIngress,
    parse_onebot_inbound,
    parse_telegram_inbound,
)

router = APIRouter(prefix="/v1/gateway", tags=["gateway"])


def _durable_ingress(request: Request) -> DurableImIngress | None:
    """The durable IM ingress, wired only when a Postgres substrate + route index exist."""
    ingress = getattr(request.app.state, "im_ingress", None)
    return ingress if isinstance(ingress, DurableImIngress) else None


def _gateway(request: Request) -> OneBotGateway | None:
    gateway: OneBotGateway | None = getattr(request.app.state, "onebot_gateway", None)
    return gateway


def _telegram_gateway(request: Request) -> TelegramGateway | None:
    gateway: TelegramGateway | None = getattr(request.app.state, "telegram_gateway", None)
    return gateway


def _replay_store(request: Request) -> WebhookReplayStore:
    """The durable replay store (set at startup); falls back to a shared in-memory one."""
    store = getattr(request.app.state, "webhook_replay_store", None)
    if store is None:
        store = request.app.state.webhook_replay_store = InMemoryWebhookReplayStore()
    return store


_WAKE_PREFIXES = ("/keel",)


@router.post("/onebot", status_code=status.HTTP_202_ACCEPTED, summary="OneBot v11 event webhook")
async def onebot_webhook(
    payload: dict[str, Any], request: Request, background: BackgroundTasks
) -> dict[str, bool]:
    """Verify the signature + replay, ack, then admit the event durably (or via the gateway).

    Provider auth (HMAC-SHA1 body signature) + the durable replay check run **before** any
    mapping is resolved. When the durable IM substrate is wired the event is resolved through
    the global route index and admitted as a durable ``surface="im"`` run (fail-closed on an
    unknown/revoked mapping); otherwise it falls back to the in-process gateway (local preview).
    """
    ingress = _durable_ingress(request)
    gateway = _gateway(request)
    if ingress is None and gateway is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "IM gateway not configured")
    settings = get_settings()
    raw = await request.body()
    secret = settings.onebot_signing_secret
    if secret:
        if not verify_onebot_signature(secret, raw, request.headers.get("x-signature")):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid webhook signature")
    elif settings.cloud_mode:
        # Cloud mode fails closed: an unauthenticated public webhook is a misconfiguration.
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "onebot webhook signing secret is required in cloud mode",
        )
    if await _replay_store(request).seen_before("onebot", onebot_delivery_id(raw)):
        return {"accepted": False}  # replay: drop silently
    if ingress is not None:
        inbound = parse_onebot_inbound(
            payload, self_id=settings.onebot_self_id or None, prefixes=_WAKE_PREFIXES
        )
        if inbound is not None:
            background.add_task(ingress.admit, inbound)
    elif gateway is not None:
        background.add_task(gateway.handle, payload)
    return {"accepted": True}


@router.post("/telegram", status_code=status.HTTP_202_ACCEPTED, summary="Telegram update webhook")
async def telegram_webhook(
    payload: dict[str, Any], request: Request, background: BackgroundTasks
) -> dict[str, bool]:
    """Verify the secret header + replay, ack, then admit the update durably (or via gateway)."""
    ingress = _durable_ingress(request)
    gateway = _telegram_gateway(request)
    if ingress is None and gateway is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "IM gateway not configured")
    settings = get_settings()
    secret = settings.telegram_webhook_secret
    if secret:
        header = request.headers.get("x-telegram-bot-api-secret-token")
        if not verify_telegram_secret(secret, header):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid webhook secret")
    elif settings.cloud_mode:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "telegram webhook secret is required in cloud mode",
        )
    delivery_id = telegram_delivery_id(payload)
    if delivery_id is not None and await _replay_store(request).seen_before(
        "telegram", delivery_id
    ):
        return {"accepted": False}  # replay: drop silently
    if ingress is not None:
        bot_id = settings.telegram_bot_username or "default"
        inbound = parse_telegram_inbound(
            payload,
            bot_id=bot_id,
            bot_username=settings.telegram_bot_username or None,
            prefixes=_WAKE_PREFIXES,
        )
        if inbound is not None:
            background.add_task(ingress.admit, inbound)
    elif gateway is not None:
        background.add_task(gateway.handle, payload)
    return {"accepted": True}


def make_onebot_sender(api_base: str, access_token: str) -> Any:
    """Build a ``send(session_key, text)`` that calls the OneBot HTTP API."""
    headers = {"Authorization": f"Bearer {access_token}"} if access_token else {}

    async def send(session_key: str, text: str) -> None:
        _, kind, target = session_key.split(":", 2)
        if kind == "group":
            endpoint, data = "send_group_msg", {"group_id": int(target), "message": text}
        else:
            endpoint, data = "send_private_msg", {"user_id": int(target), "message": text}
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{api_base.rstrip('/')}/{endpoint}", json=data, headers=headers
            )
            resp.raise_for_status()

    return send


def make_telegram_sender(bot_token: str) -> Any:
    """Build a ``send(session_key, text)`` that calls the Telegram Bot ``sendMessage`` API.

    The chat id is recovered from the ``tg:{type}:{chat_id}`` session key.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    async def send(session_key: str, text: str) -> None:
        _, _, chat_id = session_key.split(":", 2)
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json={"chat_id": int(chat_id), "text": text})
            resp.raise_for_status()

    return send
