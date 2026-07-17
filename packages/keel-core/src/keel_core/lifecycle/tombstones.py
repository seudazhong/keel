"""Event tombstone semantics for erasure-safe projection rebuilds (M3.5, WS-K).

When a session's events are erased, its ``(scope_id, session_id)`` is recorded in
``event_tombstones``. A projection rebuild (:class:`keel_core.rebuild.ProjectionRebuilder`)
is given a *tombstone hook* built here: it withholds every event belonging to a tombstoned
session, so even if a stale event source (a Redis stream backlog, a replica, an
out-of-band export) still holds the raw events, a rebuild can never resurrect erased
content into a projection.

The hook is deliberately decoupled from the erasure vocabulary: it only needs the set of
tombstoned session ids, which :class:`keel_core.lifecycle.store.ErasureStore` supplies.
"""

from __future__ import annotations

from collections.abc import Collection

from keel_core.events import Event
from keel_core.lifecycle.store import ErasureStore
from keel_core.rebuild import TombstoneHook


class SessionTombstoneSet:
    """A mutable set of tombstoned session ids with a stable membership test."""

    def __init__(self, session_ids: Collection[str] = ()) -> None:
        self._sessions: set[str] = set(session_ids)

    def add(self, session_id: str) -> None:
        self._sessions.add(session_id)

    def __contains__(self, session_id: object) -> bool:
        return session_id in self._sessions

    def __len__(self) -> int:
        return len(self._sessions)

    def snapshot(self) -> frozenset[str]:
        return frozenset(self._sessions)


def make_tombstone_hook(tombstoned: Collection[str]) -> TombstoneHook:
    """Build a rebuild hook that withholds events for any tombstoned session.

    ``tombstoned`` is any container supporting ``in`` (e.g. a ``set`` or
    :class:`SessionTombstoneSet`). The hook is synchronous and does no I/O, so a rebuild
    stays fast: load the tombstone set once, then rebuild.
    """

    def hook(event: Event) -> bool:
        return event.session_id in tombstoned

    return hook


async def load_tombstone_hook(store: ErasureStore) -> TombstoneHook:
    """Load a scope's tombstoned sessions from ``store`` and return a rebuild hook."""
    sessions = await store.tombstoned_sessions()
    return make_tombstone_hook(sessions)


__all__ = ["SessionTombstoneSet", "load_tombstone_hook", "make_tombstone_hook"]
