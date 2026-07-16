"""Generic projection rebuild support with resumable, tombstone-aware replay."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from keel_core.events import Event
from keel_core.evolution import upcast_event
from keel_core.protocols import EventStore
from keel_core.types import SessionId


@dataclass(frozen=True)
class ProjectionCheckpoint:
    """The last event durably applied to a named projection/session pair."""

    projection: str
    session_id: SessionId
    seq: int


class ProjectionCheckpointStore(Protocol):
    """Persistence seam for resumable projection rebuilds."""

    async def load(self, projection: str, session_id: SessionId) -> ProjectionCheckpoint | None: ...

    async def save(self, checkpoint: ProjectionCheckpoint) -> None: ...


class InMemoryProjectionCheckpoints:
    """Small checkpoint implementation useful to CLI tooling and tests."""

    def __init__(self) -> None:
        self._checkpoints: dict[tuple[str, SessionId], ProjectionCheckpoint] = {}

    async def load(self, projection: str, session_id: SessionId) -> ProjectionCheckpoint | None:
        return self._checkpoints.get((projection, session_id))

    async def save(self, checkpoint: ProjectionCheckpoint) -> None:
        self._checkpoints[(checkpoint.projection, checkpoint.session_id)] = checkpoint


class ProjectionSink(Protocol):
    """The write side of a projection; implementations own their storage transaction."""

    async def apply(self, event: Event) -> None: ...


TombstoneHook = Callable[[Event], bool | Awaitable[bool]]
"""Return true to withhold an erased/tombstoned event from the projection."""


@dataclass(frozen=True)
class RebuildResult:
    """Observable outcome of one rebuild pass."""

    processed: int
    skipped_tombstones: int
    last_seq: int | None
    dry_run: bool


class ProjectionRebuilder:
    """Replay one session into a projection, checkpointing after each durable apply.

    A tombstone hook is intentionally event-level: retention/erasure work can later
    supply its policy without coupling this module to lifecycle event vocabulary.
    """

    def __init__(
        self,
        events: EventStore,
        checkpoints: ProjectionCheckpointStore,
        *,
        tombstone_hook: TombstoneHook | None = None,
    ) -> None:
        self._events = events
        self._checkpoints = checkpoints
        self._tombstone_hook = tombstone_hook

    async def rebuild(
        self,
        projection: str,
        session_id: SessionId,
        sink: ProjectionSink,
        *,
        dry_run: bool = False,
        resume: bool = True,
    ) -> RebuildResult:
        """Apply events in sequence order; dry runs never write sink or checkpoints."""
        checkpoint = await self._checkpoints.load(projection, session_id) if resume else None
        after = checkpoint.seq if checkpoint is not None else None
        processed = 0
        skipped = 0
        last_seq = after

        async for stored_event in self._events.read(session_id, after=after):
            event = upcast_event(stored_event)
            tombstoned = False
            if self._tombstone_hook is not None:
                verdict = self._tombstone_hook(event)
                tombstoned = await verdict if isinstance(verdict, Awaitable) else verdict
            if tombstoned:
                skipped += 1
            elif not dry_run:
                await sink.apply(event)
            processed += 1
            last_seq = event.seq
            if not dry_run:
                await self._checkpoints.save(
                    ProjectionCheckpoint(projection, session_id, event.seq)
                )

        return RebuildResult(processed, skipped, last_seq, dry_run)
