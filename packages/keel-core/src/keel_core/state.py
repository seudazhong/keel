"""State — in-memory event store (WS-D, α).

An append-only :class:`EventStore` backed by a dict, assigning a monotonic
``seq`` per session on append. Used by the loop, unit tests, and the ``lite``
profile; the durable Postgres-backed store + projections land later in M1.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.errors import CrossScopeError, DuplicateEventError
from keel_core.events import Event
from keel_core.evolution import EVENT_UPCASTERS, upcast_event
from keel_core.types import ScopeId, SessionId


class InMemoryEventStore:
    """Non-durable :class:`~keel_core.protocols.EventStore` implementation."""

    def __init__(self) -> None:
        self._events: dict[str, list[Event]] = {}
        # Mirrors the Postgres partial-unique index on ``payload->>'dedup_key'`` so
        # deterministic in-memory concurrency tests reject a duplicate admission/steer
        # append exactly like production (M3.6).
        self._dedup: set[tuple[str, str]] = set()

    async def append(self, event: Event) -> None:
        dedup_key = event.payload.get("dedup_key")
        if dedup_key is not None:
            marker = (event.scope_id, str(dedup_key))
            if marker in self._dedup:
                raise DuplicateEventError(str(dedup_key))
            self._dedup.add(marker)
        bucket = self._events.setdefault(event.session_id, [])
        event.seq = len(bucket) + 1  # monotonic per session (replay cursor)
        bucket.append(event)

    def read(self, session_id: SessionId, after: int | None = None) -> AsyncIterator[Event]:
        return self._read(session_id, after)

    async def _read(self, session_id: SessionId, after: int | None) -> AsyncIterator[Event]:
        for event in self._events.get(session_id, []):
            if after is None or event.seq > after:
                yield upcast_event(event)

    def has_session(self, session_id: SessionId, scope_id: ScopeId) -> bool:
        return any(event.scope_id == scope_id for event in self._events.get(session_id, []))

    def snapshot(self, session_id: SessionId) -> list[Event]:
        """Return a copy of a session's events (test/debug helper)."""
        return list(self._events.get(session_id, []))

    def _txn_snapshot(self) -> tuple[dict[str, list[Event]], set[tuple[str, str]]]:
        """Capture a rollback snapshot so an atomic multi-store suspension can undo a partial
        batch on an injected failure (the in-memory single-process parity for the Postgres
        one-transaction path). Shallow-copies each session's event list + the dedup set; the
        batch only ever *appends*, so restoring drops exactly the events it added."""
        return ({sid: list(evs) for sid, evs in self._events.items()}, set(self._dedup))

    def _txn_restore(self, snapshot: tuple[dict[str, list[Event]], set[tuple[str, str]]]) -> None:
        events, dedup = snapshot
        self._events = {sid: list(evs) for sid, evs in events.items()}
        self._dedup = set(dedup)


_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


async def purge_scope(engine: AsyncEngine, scope_id: ScopeId) -> int:
    """Erase every event + session for a scope (idempotent). Returns rows removed.

    ``message_embeddings`` cascades from ``events`` (ON DELETE CASCADE); the erasure
    coordinator also purges it explicitly first so a session-scoped erase is exact.
    """
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        events = await conn.execute(
            text("DELETE FROM events WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
        sessions = await conn.execute(
            text("DELETE FROM sessions WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return int(events.rowcount or 0) + int(sessions.rowcount or 0)


async def purge_session(engine: AsyncEngine, scope_id: ScopeId, session_id: SessionId) -> int:
    """Erase one session's events + session row within a scope (idempotent)."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        events = await conn.execute(
            text("DELETE FROM events WHERE scope_id = :scope AND session_id = :session"),
            {"scope": scope_id, "session": session_id},
        )
        sessions = await conn.execute(
            text("DELETE FROM sessions WHERE scope_id = :scope AND id = :session"),
            {"scope": scope_id, "session": session_id},
        )
    return int(events.rowcount or 0) + int(sessions.rowcount or 0)


async def append_event_in_transaction(
    conn: AsyncConnection,
    event: Event,
    *,
    require_existing_session: bool = False,
) -> int:
    """Allocate a session sequence and append an event inside the caller's transaction."""
    active_scope = await conn.scalar(text("SELECT current_setting('app.scope_id', true)"))
    if str(active_scope or "") != event.scope_id:
        raise CrossScopeError(str(active_scope or "<unset>"), event.scope_id)

    payload = json.dumps(event.payload, default=str)
    params = {"sid": event.session_id, "scope": event.scope_id}
    if require_existing_session:
        row = (
            await conn.execute(
                text(
                    "UPDATE sessions SET next_seq = next_seq + 1, updated_at = now() "
                    "WHERE id = :sid AND scope_id = :scope "
                    "RETURNING next_seq - 1 AS seq"
                ),
                params,
            )
        ).one_or_none()
        if row is None:
            # Session identity is composite ``(scope_id, id)`` (M3.6 finding 2): the same
            # external id in another scope is a *different* session, so an absent row here means
            # only that this scope has no such session — never a cross-scope conflict.
            raise LookupError(f"session {event.session_id!r} does not exist in this scope")
    else:
        # Get-or-create this scope's own session row. The conflict target is the composite
        # ``(scope_id, id)`` key, so a session id already used in a *different* scope does not
        # conflict — it inserts a fresh, isolated session for this scope (identical external
        # ids coexist across orgs). Only a repeat within the same scope updates the sequence.
        row = (
            await conn.execute(
                text(
                    "INSERT INTO sessions (id, scope_id, next_seq) "
                    "VALUES (:sid, :scope, 2) "
                    "ON CONFLICT (scope_id, id) DO UPDATE "
                    "SET next_seq = sessions.next_seq + 1, updated_at = now() "
                    "RETURNING next_seq - 1 AS seq"
                ),
                params,
            )
        ).one()
    seq = int(row.seq)
    await conn.execute(
        text(
            "INSERT INTO events "
            "(session_id, scope_id, seq, type, version, run_id, ts, payload) "
            "VALUES (:sid, :scope, :seq, :type, :version, :run_id, :ts, "
            "CAST(:payload AS jsonb))"
        ),
        {
            **params,
            "seq": seq,
            "type": str(event.type),
            "version": event.version,
            "run_id": event.run_id,
            "ts": event.ts,
            "payload": payload,
        },
    )
    return seq


class PostgresEventStore:
    """Durable, scope-bound :class:`~keel_core.protocols.EventStore` over Postgres.

    Bound to one scope: append/read only ever touch that scope (application-layer
    isolation, ADR-0009), and each transaction sets the ``app.scope_id`` GUC for
    RLS defense-in-depth. ``seq`` is assigned atomically per session, so a crash
    loses zero admitted turns — a fresh store over the same DB resumes the session
    from the log (invariant I2).
    """

    def __init__(self, engine: AsyncEngine, scope_id: ScopeId) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def append(self, event: Event) -> None:
        if event.scope_id != self._scope_id:
            raise CrossScopeError(self._scope_id, event.scope_id)
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
                seq = await append_event_in_transaction(conn, event)
        except IntegrityError as exc:
            # A concurrent/duplicate admission or steering append lost the race on the
            # ``ux_events_dedup`` partial-unique index — surface it as an idempotent signal
            # so the caller observes the winner's durable turn instead of duplicating it.
            dedup_key = event.payload.get("dedup_key")
            if dedup_key is not None and "ux_events_dedup" in str(exc.orig):
                raise DuplicateEventError(str(dedup_key)) from exc
            raise
        event.seq = seq

    def read(self, session_id: SessionId, after: int | None = None) -> AsyncIterator[Event]:
        return self._read(session_id, after)

    async def _read(self, session_id: SessionId, after: int | None) -> AsyncIterator[Event]:
        sql = (
            "SELECT session_id, scope_id, seq, type, version, run_id, ts, payload "
            "FROM events WHERE session_id = :sid AND scope_id = :scope"
        )
        params: dict[str, Any] = {"sid": session_id, "scope": self._scope_id}
        if after is not None:
            sql += " AND seq > :after"
            params["after"] = after
        sql += " ORDER BY seq"

        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.stream(text(sql), params)
            async for row in result:
                payload: dict[str, Any] = {}
                if isinstance(row.payload, dict):
                    payload = {str(key): value for key, value in row.payload.items()}
                yield EVENT_UPCASTERS.decode(
                    {
                        "type": row.type,
                        "version": row.version,
                        "seq": row.seq,
                        "session_id": row.session_id,
                        "scope_id": row.scope_id,
                        "run_id": row.run_id,
                        "ts": row.ts,
                        "payload": payload,
                    }
                )


@dataclass(frozen=True)
class SessionSummary:
    """A session row for the Sessions list (derived counts, not the full log)."""

    id: str
    title: str | None
    messages: int
    created_at: datetime
    updated_at: datetime


async def list_sessions(
    engine: AsyncEngine, scope_id: ScopeId, *, limit: int = 100
) -> list[SessionSummary]:
    """List a scope's sessions (newest first) with a preview title + message count.

    ``title`` falls back to the first user ``message.token`` when the session has no
    explicit title. Purely read-only + scope-bound (RLS GUC).
    """
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = (
            await conn.execute(
                text(
                    "SELECT s.id, s.created_at, s.updated_at, "
                    "COALESCE(s.title, ("
                    "  SELECT e.payload->>'text' FROM events e "
                    "  WHERE e.session_id = s.id AND e.scope_id = s.scope_id "
                    "    AND e.type = 'message.token' AND e.payload->>'role' = 'user' "
                    "  ORDER BY e.seq LIMIT 1"
                    ")) AS title, "
                    "(SELECT count(*) FROM events e2 "
                    "  WHERE e2.session_id = s.id AND e2.scope_id = s.scope_id "
                    "    AND e2.type = 'message.token') AS messages "
                    "FROM sessions s WHERE s.scope_id = :scope "
                    "ORDER BY s.updated_at DESC LIMIT :limit"
                ),
                {"scope": scope_id, "limit": limit},
            )
        ).all()
    return [
        SessionSummary(
            id=r.id,
            title=r.title,
            messages=int(r.messages),
            created_at=r.created_at,
            updated_at=r.updated_at,
        )
        for r in rows
    ]


async def session_exists(engine: AsyncEngine, scope_id: ScopeId, session_id: SessionId) -> bool:
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        return bool(
            await conn.scalar(
                text(
                    "SELECT EXISTS(SELECT 1 FROM sessions "
                    "WHERE scope_id = :scope AND id = :session)"
                ),
                {"scope": scope_id, "session": session_id},
            )
        )
