"""Bounded Redis cleanup for erasure (M3.5, WS-K).

Live event fan-out is mirrored to one Redis stream per session (``events:{session_id}``,
see :class:`keel_core.eventbus.RedisEventStore`). When a session is erased its durable
events are deleted from Postgres, but the Redis stream backlog would still let a live
tail replay the erased content — so erasure deletes those stream keys too.

Cleanup is *bounded*: it only ever deletes keys for the explicit session ids being erased
(``events:{sid}``), never a wildcard flush. A caller with no live Redis client gets a
no-op cleaner so erasure still succeeds against the durable stores.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, runtime_checkable


@runtime_checkable
class RedisDeleter(Protocol):
    """The slice of the async Redis client the cleaner needs."""

    async def delete(self, *names: str) -> int: ...


class RedisLifecycleCleaner:
    """Deletes the per-session event-stream keys for erased sessions."""

    def __init__(self, client: RedisDeleter | None, *, namespace: str = "events") -> None:
        self._client = client
        self._namespace = namespace

    def _key(self, session_id: str) -> str:
        return f"{self._namespace}:{session_id}"

    async def purge_sessions(self, session_ids: Iterable[str]) -> int:
        """Delete the event-stream key for each session id. Returns keys removed.

        Idempotent (deleting an absent key is a no-op) and bounded to the supplied ids.
        A no-op when no Redis client is configured.
        """
        keys = [self._key(sid) for sid in session_ids]
        if not keys or self._client is None:
            return 0
        return int(await self._client.delete(*keys))


class NullRedisLifecycleCleaner:
    """A cleaner used when no Redis client is available (durable-only erasure)."""

    async def purge_sessions(self, session_ids: Iterable[str]) -> int:
        return 0


__all__ = ["NullRedisLifecycleCleaner", "RedisDeleter", "RedisLifecycleCleaner"]
