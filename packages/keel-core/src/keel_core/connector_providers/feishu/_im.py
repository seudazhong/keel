"""Feishu event ingress normalization and reply action."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from keel_core.connector_contracts import (
    ConnectorAction,
    ConnectorActionContext,
    ConnectorAuthenticationError,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorEvent,
    ConnectorIngressFailure,
    ConnectorIngressRequest,
    ConnectorIngressResponse,
    ConnectorIngressResult,
    ConnectorOperationContext,
    ConnectorProvenance,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.protocols import ToolContext

from ._auth import FeishuCredential, refresh_credential
from ._client import FeishuClient
from ._crypto import decrypt_event, verify_event_signature

FEISHU_EVENT_TOLERANCE_SECONDS = 300
_NORMALIZATION_FAILURE_RESPONSE = b'{"code":1,"msg":"temporary processing failure"}'


class _VerifiedNormalizationError(ValueError):
    pass


@runtime_checkable
class VersionedActionCredentialStore(Protocol):
    async def get_versioned(self, connector_id: str) -> tuple[str, int] | None: ...

    async def put_if_version(
        self, connector_id: str, secret: str, expected_version: int
    ) -> int | None: ...


def _header(headers: Mapping[str, str], name: str) -> str:
    lowered = name.lower()
    return next((value for key, value in headers.items() if key.lower() == lowered), "")


def _json_object(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError("Feishu event body is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("Feishu event body must be a JSON object")
    return value


def _timestamp_is_current(value: str, now: datetime | None = None) -> bool:
    try:
        timestamp = int(value)
    except ValueError:
        return False
    current = int((now or datetime.now(UTC)).timestamp())
    return abs(current - timestamp) <= FEISHU_EVENT_TOLERANCE_SECONDS


def _verify_token(payload: dict[str, Any], expected: str) -> None:
    header = payload.get("header")
    token = header.get("token") if isinstance(header, dict) else payload.get("token")
    if not isinstance(token, str) or not expected or not _constant_time_text(token, expected):
        raise ConnectorAuthenticationError("Feishu verification token mismatch")


def _constant_time_text(left: str, right: str) -> bool:
    import hmac

    return hmac.compare_digest(left.encode(), right.encode())


def _decode_request(
    request: ConnectorIngressRequest,
    credential: FeishuCredential,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    timestamp = _header(request.headers, "x-lark-request-timestamp")
    nonce = _header(request.headers, "x-lark-request-nonce")
    signature = _header(request.headers, "x-lark-signature")
    try:
        outer = _json_object(request.body)
    except ValueError as exc:
        _verify_signed_request(request, credential, timestamp, nonce, signature, now=now)
        raise _VerifiedNormalizationError("Feishu signed event JSON is invalid") from exc
    encrypted = outer.get("encrypt")
    if isinstance(encrypted, str):
        _verify_signed_request(request, credential, timestamp, nonce, signature, now=now)
        try:
            return _json_object(decrypt_event(credential.encrypt_key, encrypted))
        except ValueError as exc:
            raise _VerifiedNormalizationError(
                "Feishu encrypted event could not be normalized"
            ) from exc

    event_type = outer.get("type")
    is_challenge = event_type == "url_verification" or "challenge" in outer
    if not is_challenge:
        _verify_signed_request(request, credential, timestamp, nonce, signature, now=now)
    return outer


def _verify_signed_request(
    request: ConnectorIngressRequest,
    credential: FeishuCredential,
    timestamp: str,
    nonce: str,
    signature: str,
    *,
    now: datetime | None,
) -> None:
    if not timestamp or not _timestamp_is_current(timestamp, now):
        raise ConnectorAuthenticationError("Feishu webhook timestamp is missing or stale")
    if not nonce or not verify_event_signature(
        timestamp,
        nonce,
        credential.encrypt_key,
        request.body,
        signature,
    ):
        raise ConnectorAuthenticationError("Feishu webhook signature mismatch")


def _challenge(
    payload: dict[str, Any],
    credential: FeishuCredential,
) -> ConnectorIngressResult | None:
    challenge = payload.get("challenge")
    if not isinstance(challenge, str):
        return None
    _verify_token(payload, credential.verification_token)
    return ConnectorIngressResult(
        ConnectorIngressResponse(
            status_code=200,
            body=json.dumps({"challenge": challenge}, separators=(",", ":")).encode(),
        )
    )


def _message_text(message: dict[str, Any]) -> str:
    content = message.get("content")
    if not isinstance(content, str):
        raise _VerifiedNormalizationError("Feishu message content is missing")
    try:
        parsed = json.loads(content)
    except ValueError as exc:
        raise _VerifiedNormalizationError("Feishu message content JSON is invalid") from exc
    if not isinstance(parsed, dict):
        raise _VerifiedNormalizationError("Feishu message content must be an object")
    text = parsed.get("text")
    if isinstance(text, str):
        return text
    if message.get("message_type") != "post":
        raise _VerifiedNormalizationError("Feishu text message content is missing text")
    pieces: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            if isinstance(value.get("text"), str):
                pieces.append(value["text"])
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(parsed)
    return "\n".join(piece for piece in pieces if piece)


def _mention_ids(message: dict[str, Any]) -> tuple[set[str], tuple[str, ...]]:
    ids: set[str] = set()
    keys: list[str] = []
    mentions = message.get("mentions")
    if not isinstance(mentions, list):
        return ids, ()
    for mention in mentions:
        if not isinstance(mention, dict):
            continue
        identifier = mention.get("id")
        if isinstance(identifier, dict):
            open_id = identifier.get("open_id")
            if isinstance(open_id, str):
                ids.add(open_id)
        key = mention.get("key")
        if isinstance(key, str):
            keys.append(key)
    return ids, tuple(keys)


def _chat_selected(context: ConnectorOperationContext, chat_id: str, chat_type: str) -> bool:
    external_id = f"chat:{chat_id}"
    matching = next(
        (
            resource
            for resource in context.resources
            if resource.kind == "chat" and resource.external_id == external_id
        ),
        None,
    )
    if matching is not None:
        return matching.selected
    return chat_type == "p2p"


def _message_change(
    context: ConnectorOperationContext,
    payload: dict[str, Any],
    credential: FeishuCredential,
) -> ConnectorChange | None:
    if context.binding is None:
        raise ValueError("Feishu ingress requires a binding")
    header = payload.get("header")
    if not isinstance(header, dict):
        raise ConnectorAuthenticationError("Feishu event header is missing")
    _verify_token(payload, credential.verification_token)
    tenant_key = header.get("tenant_key")
    if not isinstance(tenant_key, str) or tenant_key != credential.tenant_key:
        raise ConnectorAuthenticationError("Feishu event tenant does not match the binding")
    if header.get("event_type") != "im.message.receive_v1":
        return None
    event = payload.get("event")
    if not isinstance(event, dict):
        raise _VerifiedNormalizationError("Feishu message event payload is missing")
    message = event.get("message")
    sender = event.get("sender")
    if not isinstance(message, dict) or not isinstance(sender, dict):
        raise _VerifiedNormalizationError("Feishu message or sender payload is missing")
    message_type = message.get("message_type")
    if message_type not in {"text", "post"}:
        return None
    chat_id = message.get("chat_id")
    chat_type = message.get("chat_type")
    message_id = message.get("message_id")
    if not all(isinstance(value, str) and value for value in (chat_id, chat_type, message_id)):
        raise _VerifiedNormalizationError("Feishu message identifiers are missing")
    assert isinstance(chat_id, str)
    assert isinstance(chat_type, str)
    assert isinstance(message_id, str)
    if not _chat_selected(context, chat_id, chat_type):
        return None
    text = _message_text(message).strip()
    if not text:
        return None
    mention_ids, mention_keys = _mention_ids(message)
    if chat_type != "p2p":
        if not credential.bot_open_id or credential.bot_open_id not in mention_ids:
            return None
        for key in mention_keys:
            text = text.replace(key, "")
        text = text.strip()
        if not text:
            return None

    sender_id = sender.get("sender_id")
    sender_open_id = sender_id.get("open_id") if isinstance(sender_id, dict) else None
    root_id = message.get("root_id")
    parent_id = message.get("parent_id")
    thread_id = (
        root_id
        if isinstance(root_id, str) and root_id
        else parent_id
        if isinstance(parent_id, str) and parent_id
        else message_id
    )
    event_id = header.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        event_id = message_id
    provenance = ConnectorProvenance(
        connector_id=context.connector_id,
        binding_id=context.binding.id,
        external_resource_id=f"chat:{chat_id}",
        revision=str(message.get("create_time") or header.get("create_time") or ""),
        event_id=event_id,
    )
    normalized = ConnectorEvent(
        type="feishu.message",
        provenance=provenance,
        payload={
            "platform": "feishu",
            "tenant_key": tenant_key,
            "chat_id": chat_id,
            "chat_type": chat_type,
            "thread_id": thread_id,
            "root_id": root_id if isinstance(root_id, str) else None,
            "parent_id": parent_id if isinstance(parent_id, str) else None,
            "message_id": message_id,
            "sender_open_id": sender_open_id if isinstance(sender_open_id, str) else None,
            "sender_type": sender.get("sender_type"),
            "text": text,
        },
    )
    return ConnectorChange(
        kind=ConnectorChangeKind.event,
        provenance=provenance,
        event=normalized,
    )


def handle_ingress(
    context: ConnectorOperationContext,
    request: ConnectorIngressRequest,
    credential: FeishuCredential,
    *,
    now: datetime | None = None,
) -> ConnectorIngressResult:
    payload_hash = hashlib.sha256(request.body).hexdigest()
    try:
        payload = _decode_request(request, credential, now=now)
    except _VerifiedNormalizationError:
        return _normalization_failure(request, payload_hash)
    challenge = _challenge(payload, credential)
    if challenge is not None:
        return challenge
    try:
        change = _message_change(context, payload, credential)
    except _VerifiedNormalizationError:
        return _normalization_failure(request, payload_hash, payload)
    header = payload.get("header")
    event_id = header.get("event_id") if isinstance(header, dict) else None
    delivery_id = event_id if isinstance(event_id, str) and event_id else payload_hash
    return ConnectorIngressResult(
        ConnectorIngressResponse(status_code=200, body=b'{"code":0}'),
        delivery_id=delivery_id,
        payload_hash=payload_hash,
        changes=(change,) if change is not None else (),
    )


def _normalization_failure(
    request: ConnectorIngressRequest,
    payload_hash: str,
    payload: dict[str, Any] | None = None,
) -> ConnectorIngressResult:
    header = payload.get("header") if isinstance(payload, dict) else None
    event_id = header.get("event_id") if isinstance(header, dict) else None
    delivery_id = (
        event_id
        if isinstance(event_id, str) and event_id
        else hashlib.sha256(request.body).hexdigest()
    )
    return ConnectorIngressResult(
        ConnectorIngressResponse(
            status_code=503,
            body=_NORMALIZATION_FAILURE_RESPONSE,
        ),
        delivery_id=delivery_id,
        payload_hash=payload_hash,
        failure=ConnectorIngressFailure(
            "feishu_normalization_failed",
            "Feishu verified event normalization failed.",
            retryable=True,
        ),
    )


def _parse_stored_credential(raw: str) -> FeishuCredential:
    envelope = CredentialEnvelope.parse(raw)
    return FeishuCredential.from_envelope(envelope)


async def _action_credential(
    store: VersionedActionCredentialStore,
    client: FeishuClient,
) -> FeishuCredential:
    stored = await store.get_versioned("feishu")
    if stored is None:
        raise RuntimeError("Feishu credentials are unavailable")
    raw, version = stored
    credential = _parse_stored_credential(raw)
    refreshed, changed = await refresh_credential(client, credential)
    if not changed:
        return credential
    stored_version = await store.put_if_version("feishu", refreshed.envelope().serialize(), version)
    if stored_version is not None:
        return refreshed
    winner = await store.get_versioned("feishu")
    if winner is None:
        raise RuntimeError("Feishu credentials changed during token refresh")
    return _parse_stored_credential(winner[0])


def build_reply_action(
    manifest: Any,
    context: ConnectorActionContext,
    client_factory: Callable[[], FeishuClient],
) -> ConnectorAction:
    if context.credential_store is None:
        raise RuntimeError("encrypted connector credential storage is unavailable")
    store = context.credential_store
    if not isinstance(store, VersionedActionCredentialStore):
        raise RuntimeError("versioned connector credential storage is unavailable")

    async def reply(arguments: dict[str, Any], tool_context: ToolContext) -> str:
        chat_id = str(arguments.get("chat_id", "")).strip()
        message_id = str(arguments.get("message_id", "")).strip()
        text = str(arguments.get("text", "")).strip()
        if not chat_id or not message_id or not text:
            raise ValueError("feishu_reply requires chat_id, message_id, and text")
        await context.require_selected_resource("feishu", f"chat:{chat_id}")
        client = client_factory()
        credential = await _action_credential(store, client)
        body: dict[str, Any] = {
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False, separators=(",", ":")),
        }
        thread_id = str(arguments.get("thread_id", "")).strip()
        if thread_id:
            body["reply_in_thread"] = True
        payload = await client.request(
            "POST",
            f"/open-apis/im/v1/messages/{message_id}/reply",
            token=credential.tenant_access_token,
            body=body,
        )
        data = payload.get("data")
        reply_message_id = data.get("message_id") if isinstance(data, dict) else None
        return f"replied (id={reply_message_id or message_id})"

    return ConnectorAction(manifest, reply)


__all__ = ["FEISHU_EVENT_TOLERANCE_SECONDS", "build_reply_action", "handle_ingress"]
