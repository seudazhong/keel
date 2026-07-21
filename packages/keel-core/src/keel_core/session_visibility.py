"""Session ownership + visibility — independent of Agent access (R1B).

Using a team Agent (passing the checks in :mod:`keel_core.identity.authz`) never by itself
grants reading another user's private session: a session additionally records *who* it belongs
to (a durable user, or a channel identity for an IM-admitted session) and a ``visibility``
policy that governs who else may read it. The two axes compose independently — see
INVARIANTS.md C2/C6 and docs/IDENTITY.md.

* ``private`` — only the owner (default for every newly-created session).
* ``agent_members`` — any principal that currently holds an active Agent Access edge on the
  session's Agent (never bare org membership) may read it, in addition to the owner.
* ``explicit`` — only the owner and users with an active :class:`SessionShare` row.

:func:`ensure_session_identity` is the single, idempotent, atomic entrypoint that creates a
session's identity/visibility row (or reads back the *pre-existing* one — first-writer-wins,
never overwritten by a later admission). It is called **before** the admitted prompt is
persisted (see ``keel_core.run_service.DurableRunService.admit`` / ``keel_server``'s IM
ingress), so a crash or a retried admission can never observe an accepted session with no
owner or an ambiguous visibility: either the identity row was never created (and the retry
creates it, atomically, from scratch) or it already exists with the values the *original*
admission set (a retry that recomputes the same values is a pure no-op; it can never
overwrite a session's identity with a different actor's values).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.types import ScopeId, SessionId

_SHARE_PREFIX = "shr_"

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")

_IDENTITY_COLS = (
    "scope_id, id, org_id, owner_user_id, channel_provider, channel_external_id, visibility"
)
_SHARE_COLS = (
    "id, scope_id, session_id, user_id, granted_by_user_id, status, "
    "created_at, updated_at, revoked_at"
)


def new_session_share_id() -> str:
    return f"{_SHARE_PREFIX}{uuid.uuid4().hex}"


class SessionVisibility(StrEnum):
    """A session's read-visibility policy, independent of the selected Agent's access."""

    private = "private"
    agent_members = "agent_members"
    explicit = "explicit"


class ShareStatus(StrEnum):
    active = "active"
    revoked = "revoked"


@dataclass(frozen=True)
class SessionIdentity:
    """The identity/visibility columns of one session row."""

    scope_id: ScopeId
    session_id: SessionId
    org_id: str | None
    owner_user_id: str | None
    channel_provider: str | None
    channel_external_id: str | None
    visibility: SessionVisibility

    @property
    def is_channel_session(self) -> bool:
        return self.channel_provider is not None

    @property
    def is_legacy(self) -> bool:
        """A session never touched by an identity-aware admission path (no org, owner, or channel).

        Such a row predates (or bypasses) R1B session-identity wiring — see the module
        docstring and migration ``0025``'s backfill. ``can_view_session`` falls back to the
        pre-R1B Agent-scope-only gate for these rows rather than denying everyone, so surfaces
        this PR does not touch (schedules, CLI, connector-triggered runs, tests) keep working."""
        return self.owner_user_id is None and self.channel_provider is None and self.org_id is None


@dataclass(frozen=True)
class SessionShare:
    """An explicit per-user read grant on one session (``visibility = 'explicit'``)."""

    id: str
    scope_id: ScopeId
    session_id: SessionId
    user_id: str
    granted_by_user_id: str
    status: ShareStatus = ShareStatus.active
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revoked_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is ShareStatus.active


def can_view_session(
    identity: SessionIdentity,
    *,
    actor_user_id: str | None,
    has_active_agent_access: bool,
    has_explicit_share: bool,
) -> bool:
    """Whether ``actor_user_id`` may read this session — fails closed on every branch.

    ``has_active_agent_access`` must already encode "the actor currently holds at least
    discover-level Agent Access (or the org admin/owner administrative path) on the session's
    Agent" — this function never re-derives that; it composes the *result*. Likewise
    ``has_explicit_share`` must already reflect an active, un-revoked :class:`SessionShare`.
    """
    if actor_user_id is not None and identity.owner_user_id == actor_user_id:
        return True
    if identity.is_legacy:
        return has_active_agent_access
    if identity.visibility is SessionVisibility.agent_members:
        return has_active_agent_access
    if identity.visibility is SessionVisibility.explicit:
        return has_explicit_share
    return False  # private, no owner match: deny (fail closed)


def can_manage_session_visibility(
    identity: SessionIdentity, *, actor_user_id: str | None, actor_can_manage_agent: bool
) -> bool:
    """Whether ``actor_user_id`` may change this session's visibility or share edges.

    Requires ownership, or the "manage" capability on the session's Agent (org admin/owner,
    or a delegated ``manage``-level Agent Access edge holder) — never mere read/agent-use
    access."""
    if actor_user_id is not None and identity.owner_user_id == actor_user_id:
        return True
    return actor_can_manage_agent


def _to_identity(row: Any) -> SessionIdentity:
    visibility = row["visibility"]
    return SessionIdentity(
        scope_id=row["scope_id"],
        session_id=row["id"],
        org_id=row["org_id"],
        owner_user_id=row["owner_user_id"],
        channel_provider=row["channel_provider"],
        channel_external_id=row["channel_external_id"],
        visibility=SessionVisibility(visibility) if visibility else SessionVisibility.private,
    )


def _to_share(row: Any) -> SessionShare:
    return SessionShare(
        id=row["id"],
        scope_id=row["scope_id"],
        session_id=row["session_id"],
        user_id=row["user_id"],
        granted_by_user_id=row["granted_by_user_id"],
        status=ShareStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        revoked_at=row["revoked_at"],
    )


async def ensure_session_identity(
    engine: AsyncEngine,
    scope_id: ScopeId,
    session_id: SessionId,
    *,
    org_id: str | None = None,
    owner_user_id: str | None = None,
    channel_provider: str | None = None,
    channel_external_id: str | None = None,
    visibility: SessionVisibility = SessionVisibility.private,
) -> SessionIdentity:
    """Idempotently create-or-read a session's identity/visibility (first-writer-wins, atomic).

    A single ``INSERT ... ON CONFLICT (scope_id, id) DO NOTHING`` either creates the session
    row (setting every identity/visibility column atomically in that one statement) or, if the
    row already exists (a retry, or a prior non-identity-aware writer such as a plain event
    append), leaves it untouched and this reads back whatever is already there. Call this
    **before** persisting the admitted prompt (:mod:`keel_core.run_service`) so a crash between
    this call and the prompt append is safe: retrying re-derives the identical identity values
    from the same request and finds the row already correctly set (a pure no-op), and a crash
    before this call leaves nothing behind for a retry to disagree with.
    """
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        row = (
            (
                await conn.execute(
                    text(
                        "INSERT INTO sessions "
                        "(id, scope_id, next_seq, org_id, owner_user_id, channel_provider, "
                        "channel_external_id, visibility) "
                        "VALUES (:sid, :scope, 1, :org, :owner, :cprov, :cext, :vis) "
                        "ON CONFLICT (scope_id, id) DO NOTHING "
                        f"RETURNING {_IDENTITY_COLS}"
                    ),
                    {
                        "sid": session_id,
                        "scope": scope_id,
                        "org": org_id,
                        "owner": owner_user_id,
                        "cprov": channel_provider,
                        "cext": channel_external_id,
                        "vis": visibility.value,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_IDENTITY_COLS} FROM sessions "
                            "WHERE scope_id = :scope AND id = :sid"
                        ),
                        {"scope": scope_id, "sid": session_id},
                    )
                )
                .mappings()
                .one()
            )
    return _to_identity(row)


async def get_session_identity(
    engine: AsyncEngine, scope_id: ScopeId, session_id: SessionId
) -> SessionIdentity | None:
    """Read a session's identity/visibility, or ``None`` if it does not exist in this scope."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        row = (
            (
                await conn.execute(
                    text(
                        f"SELECT {_IDENTITY_COLS} FROM sessions "
                        "WHERE scope_id = :scope AND id = :sid"
                    ),
                    {"scope": scope_id, "sid": session_id},
                )
            )
            .mappings()
            .one_or_none()
        )
    return None if row is None else _to_identity(row)


async def set_session_visibility(
    engine: AsyncEngine, scope_id: ScopeId, session_id: SessionId, visibility: SessionVisibility
) -> SessionIdentity | None:
    """Update a session's visibility (caller has already authorized the mutation)."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        row = (
            (
                await conn.execute(
                    text(
                        "UPDATE sessions SET visibility = :vis, updated_at = now() "
                        "WHERE scope_id = :scope AND id = :sid "
                        f"RETURNING {_IDENTITY_COLS}"
                    ),
                    {"vis": visibility.value, "scope": scope_id, "sid": session_id},
                )
            )
            .mappings()
            .one_or_none()
        )
    return None if row is None else _to_identity(row)


@runtime_checkable
class SessionAccessStore(Protocol):
    """Durable seam for explicit per-user session shares."""

    async def create_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str, *, granted_by_user_id: str
    ) -> SessionShare: ...
    async def list_shares(self, scope_id: ScopeId, session_id: SessionId) -> list[SessionShare]: ...
    async def get_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> SessionShare | None: ...
    async def revoke_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> SessionShare | None: ...
    async def has_active_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> bool: ...


class InMemorySessionAccessStore:
    """Non-durable :class:`SessionAccessStore` (unit tests / lite profile)."""

    def __init__(self) -> None:
        self._shares: dict[str, SessionShare] = {}

    def _key(self, scope_id: str, session_id: str, user_id: str) -> str:
        return f"{scope_id}\0{session_id}\0{user_id}"

    async def create_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str, *, granted_by_user_id: str
    ) -> SessionShare:
        key = self._key(scope_id, session_id, user_id)
        now = datetime.now(UTC)
        existing = self._shares.get(key)
        if existing is not None:
            updated = replace(
                existing,
                status=ShareStatus.active,
                granted_by_user_id=granted_by_user_id,
                revoked_at=None,
                updated_at=now,
            )
            self._shares[key] = updated
            return updated
        share = SessionShare(
            id=new_session_share_id(),
            scope_id=scope_id,
            session_id=session_id,
            user_id=user_id,
            granted_by_user_id=granted_by_user_id,
            created_at=now,
            updated_at=now,
        )
        self._shares[key] = share
        return share

    async def list_shares(self, scope_id: ScopeId, session_id: SessionId) -> list[SessionShare]:
        return [
            s
            for s in self._shares.values()
            if s.scope_id == scope_id and s.session_id == session_id
        ]

    async def get_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> SessionShare | None:
        return self._shares.get(self._key(scope_id, session_id, user_id))

    async def revoke_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> SessionShare | None:
        key = self._key(scope_id, session_id, user_id)
        share = self._shares.get(key)
        if share is None:
            return None
        now = datetime.now(UTC)
        updated = replace(share, status=ShareStatus.revoked, revoked_at=now, updated_at=now)
        self._shares[key] = updated
        return updated

    async def has_active_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> bool:
        share = await self.get_share(scope_id, session_id, user_id)
        return share is not None and share.is_active


class PostgresSessionAccessStore:
    """Durable :class:`SessionAccessStore`; scope-owned access sets ``app.scope_id``."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def create_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str, *, granted_by_user_id: str
    ) -> SessionShare:
        share_id = new_session_share_id()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            await conn.execute(
                text(
                    "INSERT INTO session_access "
                    "(id, scope_id, session_id, user_id, granted_by_user_id) "
                    "VALUES (:id, :scope, :sid, :user, :grantor) "
                    "ON CONFLICT (scope_id, session_id, user_id) "
                    "DO UPDATE SET status = 'active', revoked_at = NULL, "
                    "granted_by_user_id = EXCLUDED.granted_by_user_id, updated_at = now()"
                ),
                {
                    "id": share_id,
                    "scope": scope_id,
                    "sid": session_id,
                    "user": user_id,
                    "grantor": granted_by_user_id,
                },
            )
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_SHARE_COLS} FROM session_access "
                            "WHERE scope_id = :scope AND session_id = :sid AND user_id = :user"
                        ),
                        {"scope": scope_id, "sid": session_id, "user": user_id},
                    )
                )
                .mappings()
                .one()
            )
        return _to_share(row)

    async def list_shares(self, scope_id: ScopeId, session_id: SessionId) -> list[SessionShare]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_SHARE_COLS} FROM session_access "
                            "WHERE scope_id = :scope AND session_id = :sid ORDER BY created_at"
                        ),
                        {"scope": scope_id, "sid": session_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_share(row) for row in rows]

    async def get_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> SessionShare | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_SHARE_COLS} FROM session_access "
                            "WHERE scope_id = :scope AND session_id = :sid AND user_id = :user"
                        ),
                        {"scope": scope_id, "sid": session_id, "user": user_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_share(row)

    async def revoke_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> SessionShare | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE session_access SET status = 'revoked', revoked_at = now(), "
                            "updated_at = now() "
                            "WHERE scope_id = :scope AND session_id = :sid AND user_id = :user "
                            "AND status = 'active' "
                            f"RETURNING {_SHARE_COLS}"
                        ),
                        {"scope": scope_id, "sid": session_id, "user": user_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_share(row)

    async def has_active_share(
        self, scope_id: ScopeId, session_id: SessionId, user_id: str
    ) -> bool:
        share = await self.get_share(scope_id, session_id, user_id)
        return share is not None and share.is_active


__all__ = [
    "InMemorySessionAccessStore",
    "PostgresSessionAccessStore",
    "SessionAccessStore",
    "SessionIdentity",
    "SessionShare",
    "SessionVisibility",
    "ShareStatus",
    "can_manage_session_visibility",
    "can_view_session",
    "ensure_session_identity",
    "get_session_identity",
    "new_session_share_id",
    "set_session_visibility",
]
