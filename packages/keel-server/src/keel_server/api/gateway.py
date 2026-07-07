"""OneBot (QQ) IM webhook (WS-E/J).

``POST /v1/gateway/onebot`` receives OneBot v11 events. Handling runs in a background
task so the bot framework gets an immediate ack; the reply is sent back over the
OneBot HTTP API. Namespaced under ``/v1`` (additive-only, G14).
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, status

from keel_server.gateway import OneBotGateway

router = APIRouter(prefix="/v1/gateway", tags=["gateway"])


def _gateway(request: Request) -> OneBotGateway:
    gateway: OneBotGateway | None = getattr(request.app.state, "onebot_gateway", None)
    if gateway is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "IM gateway not configured")
    return gateway


@router.post("/onebot", status_code=status.HTTP_202_ACCEPTED, summary="OneBot v11 event webhook")
async def onebot_webhook(
    payload: dict[str, Any], request: Request, background: BackgroundTasks
) -> dict[str, bool]:
    """Ack immediately; process the event (wake -> run -> reply) in the background."""
    gateway = _gateway(request)
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
