"""Global connector webhook routing capability (M3.6 review finding — webhook scope routing).

An inbound provider webhook (Slack event, GitHub delivery, Feishu callback …) arrives with **no**
Keel auth headers and no way to carry the org/Agent it belongs to. Before this table the ingress
route resolved to the app-global ``web:local`` scope, so in a multi-tenant cloud deployment a
delivery for one Agent's connector could never be routed to that Agent's scope.

``connector_webhook_routes`` is a minimal, **global** (non-RLS) routing capability table. Each row
maps a **high-entropy, unguessable route token** (embedded in the webhook URL handed to the
provider at setup) to the ``(scope_id, connector_id, binding_id, status)`` the delivery belongs to.
It carries **no** credential, signing secret, or token payload — only routing metadata — so it is
safe for the non-owner runtime role to read across scopes. The route token is a *routing key*, not
an authorization: after the ingress resolves it to a scope it still runs the provider-specific
signature / endpoint-token / replay verification against that scope's bound credential. The token's
entropy makes valid scopes non-enumerable, and lookups compare it as an opaque key.

Deleting a binding (revoke / lifecycle erasure) removes its route, so a delivery for a
disconnected connector fails closed with no dangling capability.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.types import ScopeId


def mint_route_token() -> str:
    """A high-entropy, URL-safe webhook route token (enumeration-resistant)."""
    return secrets.token_urlsafe(32)


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class ConnectorWebhookRoute:
    """The scope + connector + binding a resolved webhook route token belongs to."""

    route_token: str
    scope_id: ScopeId
    connector_id: str
    binding_id: str
    status: str


class ConnectorWebhookRouteStore(Protocol):
    """The global routing capability table the ingress resolves a webhook route token against."""

    async def put(
        self,
        route_token: str,
        scope_id: ScopeId,
        connector_id: str,
        binding_id: str,
        status: str,
        *,
        now: datetime | None = None,
    ) -> None:
        """Persist (replacing any prior route for this scope+connector) the routing capability."""

    async def resolve(self, route_token: str) -> ConnectorWebhookRoute | None:
        """Resolve a route token to its scope+connector+binding across all scopes, or ``None``."""

    async def delete_for_connector(self, scope_id: ScopeId, connector_id: str) -> None:
        """Remove a connector's route (binding revoke / erasure) so it fails closed."""


class InMemoryConnectorWebhookRouteStore:
    """Process-local webhook route store double for unit tests."""

    def __init__(self) -> None:
        self._by_token: dict[str, ConnectorWebhookRoute] = {}

    async def put(
        self,
        route_token: str,
        scope_id: ScopeId,
        connector_id: str,
        binding_id: str,
        status: str,
        *,
        now: datetime | None = None,
    ) -> None:
        # One route per (scope, connector): drop any prior token for this pair first.
        for token, route in list(self._by_token.items()):
            if route.scope_id == scope_id and route.connector_id == connector_id:
                del self._by_token[token]
        self._by_token[route_token] = ConnectorWebhookRoute(
            route_token=route_token,
            scope_id=scope_id,
            connector_id=connector_id,
            binding_id=binding_id,
            status=status,
        )

    async def resolve(self, route_token: str) -> ConnectorWebhookRoute | None:
        return self._by_token.get(route_token)

    async def delete_for_connector(self, scope_id: ScopeId, connector_id: str) -> None:
        for token, route in list(self._by_token.items()):
            if route.scope_id == scope_id and route.connector_id == connector_id:
                del self._by_token[token]


class PostgresConnectorWebhookRouteStore:
    """Durable global webhook route store over Postgres (no RLS — the cross-scope routing index)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def put(
        self,
        route_token: str,
        scope_id: ScopeId,
        connector_id: str,
        binding_id: str,
        status: str,
        *,
        now: datetime | None = None,
    ) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            # Exactly one route per (scope, connector): replace any prior token for this pair.
            await conn.execute(
                text(
                    "DELETE FROM connector_webhook_routes "
                    "WHERE scope_id = :scope AND connector_id = :connector "
                    "AND route_token <> :token"
                ),
                {"scope": scope_id, "connector": connector_id, "token": route_token},
            )
            await conn.execute(
                text(
                    "INSERT INTO connector_webhook_routes "
                    "(route_token, scope_id, connector_id, binding_id, status, "
                    " created_at, updated_at) "
                    "VALUES (:token, :scope, :connector, :binding, :status, :now, :now) "
                    "ON CONFLICT (route_token) DO UPDATE SET "
                    "  scope_id = EXCLUDED.scope_id, connector_id = EXCLUDED.connector_id, "
                    "  binding_id = EXCLUDED.binding_id, status = EXCLUDED.status, "
                    "  updated_at = EXCLUDED.updated_at"
                ),
                {
                    "token": route_token,
                    "scope": scope_id,
                    "connector": connector_id,
                    "binding": binding_id,
                    "status": status,
                    "now": now,
                },
            )

    async def resolve(self, route_token: str) -> ConnectorWebhookRoute | None:
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT route_token, scope_id, connector_id, binding_id, status "
                        "FROM connector_webhook_routes WHERE route_token = :token"
                    ),
                    {"token": route_token},
                )
            ).one_or_none()
        if row is None:
            return None
        return ConnectorWebhookRoute(
            route_token=row.route_token,
            scope_id=row.scope_id,
            connector_id=row.connector_id,
            binding_id=row.binding_id,
            status=row.status,
        )

    async def delete_for_connector(self, scope_id: ScopeId, connector_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "DELETE FROM connector_webhook_routes "
                    "WHERE scope_id = :scope AND connector_id = :connector"
                ),
                {"scope": scope_id, "connector": connector_id},
            )


__all__ = [
    "ConnectorWebhookRoute",
    "ConnectorWebhookRouteStore",
    "InMemoryConnectorWebhookRouteStore",
    "PostgresConnectorWebhookRouteStore",
    "mint_route_token",
]
