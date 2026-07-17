"""Feishu app credentials and tenant-token rotation."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from keel_core.connector_credentials import CredentialEnvelope

from ._client import FeishuClient

FEISHU_CREDENTIAL_KIND = "feishu_app"


def _text(values: dict[str, Any], key: str) -> str:
    value = values.get(key)
    return value if isinstance(value, str) else ""


@dataclass(frozen=True, slots=True)
class FeishuCredential:
    app_id: str
    app_secret: str
    tenant_key: str
    verification_token: str
    encrypt_key: str
    tenant_access_token: str
    token_expires_at: datetime
    bot_open_id: str
    bot_name: str

    @classmethod
    def from_envelope(cls, envelope: CredentialEnvelope | None) -> FeishuCredential:
        if envelope is None or envelope.kind != FEISHU_CREDENTIAL_KIND:
            raise ValueError("Feishu credentials are missing or have the wrong kind")
        values = envelope.values
        expires_at = datetime.fromisoformat(_text(values, "token_expires_at"))
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        return cls(
            app_id=_text(values, "app_id"),
            app_secret=_text(values, "app_secret"),
            tenant_key=_text(values, "tenant_key"),
            verification_token=_text(values, "verification_token"),
            encrypt_key=_text(values, "encrypt_key"),
            tenant_access_token=_text(values, "tenant_access_token"),
            token_expires_at=expires_at.astimezone(UTC),
            bot_open_id=_text(values, "bot_open_id"),
            bot_name=_text(values, "bot_name"),
        )

    def envelope(self) -> CredentialEnvelope:
        return CredentialEnvelope(
            kind=FEISHU_CREDENTIAL_KIND,
            values={
                "app_id": self.app_id,
                "app_secret": self.app_secret,
                "tenant_key": self.tenant_key,
                "verification_token": self.verification_token,
                "encrypt_key": self.encrypt_key,
                "tenant_access_token": self.tenant_access_token,
                "token_expires_at": self.token_expires_at.isoformat(),
                "bot_open_id": self.bot_open_id,
                "bot_name": self.bot_name,
            },
        )

    def needs_refresh(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(UTC)
        return self.token_expires_at <= current + timedelta(minutes=5)


async def issue_tenant_token(
    client: FeishuClient,
    app_id: str,
    app_secret: str,
    *,
    now: datetime | None = None,
) -> tuple[str, datetime]:
    payload = await client.request(
        "POST",
        "/open-apis/auth/v3/tenant_access_token/internal",
        body={"app_id": app_id, "app_secret": app_secret},
    )
    token = payload.get("tenant_access_token")
    expires = payload.get("expire")
    if not isinstance(token, str) or not token:
        raise ValueError("Feishu did not return a tenant access token")
    lifetime = int(expires) if isinstance(expires, (int, str)) else 7200
    return token, (now or datetime.now(UTC)) + timedelta(seconds=max(lifetime - 60, 60))


async def tenant_identity(client: FeishuClient, token: str) -> tuple[str, str]:
    payload = await client.request("GET", "/open-apis/tenant/v2/tenant/query", token=token)
    data = payload.get("data")
    tenant = data.get("tenant") if isinstance(data, dict) else None
    if not isinstance(tenant, dict):
        raise ValueError("Feishu tenant identity is unavailable")
    key = tenant.get("tenant_key") or tenant.get("key")
    name = tenant.get("name")
    if not isinstance(key, str) or not key:
        raise ValueError("Feishu tenant identity did not include tenant_key")
    return key, name if isinstance(name, str) else key


async def bot_identity(client: FeishuClient, token: str) -> tuple[str, str]:
    payload = await client.request("GET", "/open-apis/bot/v3/info/", token=token)
    bot = payload.get("bot")
    if not isinstance(bot, dict):
        data = payload.get("data")
        bot = data.get("bot") if isinstance(data, dict) else None
    if not isinstance(bot, dict):
        raise ValueError("Feishu bot identity is unavailable")
    open_id = bot.get("open_id")
    name = bot.get("app_name") or bot.get("name")
    if not isinstance(open_id, str) or not open_id:
        raise ValueError("Feishu bot identity did not include open_id")
    return open_id, name if isinstance(name, str) else "Feishu bot"


async def granted_scopes(client: FeishuClient, token: str) -> set[str]:
    payload = await client.request(
        "GET",
        "/open-apis/application/v6/scopes",
        token=token,
    )
    data = payload.get("data")
    raw = data.get("scopes") if isinstance(data, dict) else None
    if not isinstance(raw, list):
        return set()
    scopes: set[str] = set()
    for item in raw:
        if isinstance(item, str):
            scopes.add(item)
        elif isinstance(item, dict):
            name = item.get("scope_name") or item.get("scope") or item.get("name")
            granted = item.get("grant_status", 1)
            if isinstance(name, str) and granted == 1:
                scopes.add(name)
    return scopes


async def refresh_credential(
    client: FeishuClient,
    credential: FeishuCredential,
    *,
    now: datetime | None = None,
) -> tuple[FeishuCredential, bool]:
    if not credential.needs_refresh(now):
        return credential, False
    token, expires_at = await issue_tenant_token(
        client,
        credential.app_id,
        credential.app_secret,
        now=now,
    )
    return replace(
        credential,
        tenant_access_token=token,
        token_expires_at=expires_at,
    ), True


__all__ = [
    "FEISHU_CREDENTIAL_KIND",
    "FeishuCredential",
    "bot_identity",
    "granted_scopes",
    "issue_tenant_token",
    "refresh_credential",
    "tenant_identity",
]
