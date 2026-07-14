"""Allow-listed durable-job worker orchestration (ADR-0010)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from keel_core.jobs import (
    JobCancellationRequested,
    JobLease,
    JobResult,
    JobStore,
)

JobHandler = Callable[["JobContext", dict[str, Any]], Awaitable[JobResult]]
JobClock = Callable[[], datetime]
EnqueueJob = Callable[..., Awaitable[None]]


def _normalized_kind(kind: str) -> str:
    if not isinstance(kind, str):
        raise ValueError("job kind must be storage-safe UTF-8 text")
    normalized = kind.strip()
    if not normalized:
        raise ValueError("job kind must not be empty")
    if "\x00" in normalized:
        raise ValueError("job kind must be storage-safe UTF-8 text without NUL characters")
    try:
        normalized.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("job kind must be storage-safe UTF-8 text") from exc
    return normalized


@dataclass(frozen=True)
class JobDefinition:
    kind: str
    handler: JobHandler
    max_attempts: int = 3
    lease_seconds: int = 300

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", _normalized_kind(self.kind))
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.lease_seconds < 1:
            raise ValueError("lease_seconds must be at least 1")


class JobRegistry:
    def __init__(self) -> None:
        self._definitions: dict[str, JobDefinition] = {}

    def register(self, definition: JobDefinition) -> None:
        if definition.kind in self._definitions:
            raise ValueError(f"job kind already registered: {definition.kind}")
        self._definitions[definition.kind] = definition

    def get(self, kind: str) -> JobDefinition | None:
        return self._definitions.get(kind)

    def kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._definitions))


class JobContext:
    def __init__(
        self,
        store: JobStore,
        lease: JobLease,
        *,
        clock: JobClock,
    ) -> None:
        self._store = store
        self._lease = lease
        self._clock = clock
        self.job_id = lease.job_id
        self.scope_id = lease.scope_id
        self.attempt = lease.attempt

    async def progress(
        self,
        current: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        update = await self._store.progress(
            self._lease,
            current=current,
            total=total,
            message=message,
            now=self._clock(),
        )
        if update.cancel_requested:
            raise JobCancellationRequested

    async def checkpoint(self) -> None:
        if await self._store.heartbeat(self._lease, self._clock()):
            raise JobCancellationRequested
