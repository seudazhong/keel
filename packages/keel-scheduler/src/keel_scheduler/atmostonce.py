"""At-most-once scheduling core (spike S2, ADR-0006).

Proves the discipline: a due schedule's cursor (``next_run_at``) is **advanced
before** the job is enqueued, via an atomic compare-and-set claim. A crash
between claim and enqueue therefore yields **0 or 1** runs, never two; and two
concurrent leaders racing the same tick produce exactly one enqueue.

The claim is abstracted behind :class:`ClaimStore` (in-memory here; Redis-backed
with leader election in M2).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol


@dataclass
class Schedule:
    """A due-able schedule with its next run time and interval."""

    id: str
    next_run_at: datetime
    interval: timedelta


class ClaimStore(Protocol):
    """Atomic cursor store: advance ``next_run_at`` from ``expected`` to ``new``."""

    def claim(self, schedule_id: str, expected: datetime, new: datetime) -> bool:
        """Return True iff this caller won the compare-and-set."""
        ...


class InMemoryClaimStore:
    """A dict-backed :class:`ClaimStore` (deterministic, for the spike/tests)."""

    def __init__(self, cursors: dict[str, datetime]) -> None:
        self._cursors: dict[str, datetime] = dict(cursors)

    def claim(self, schedule_id: str, expected: datetime, new: datetime) -> bool:
        if self._cursors.get(schedule_id) != expected:
            return False
        self._cursors[schedule_id] = new
        return True

    def snapshot(self) -> dict[str, datetime]:
        return dict(self._cursors)


class AtMostOnceScheduler:
    """Advance-cursor-before-enqueue scheduler."""

    def __init__(self, store: ClaimStore, enqueue: Callable[[str], None]) -> None:
        self._store = store
        self._enqueue = enqueue

    def tick(self, schedules: list[Schedule], now: datetime) -> None:
        """Enqueue every due schedule at most once."""
        for schedule in schedules:
            if schedule.next_run_at > now:
                continue
            new_cursor = schedule.next_run_at + schedule.interval
            # Advance the durable cursor FIRST. If we lose the CAS (another
            # leader / a prior crashed tick already advanced it), skip.
            if not self._store.claim(schedule.id, schedule.next_run_at, new_cursor):
                continue
            schedule.next_run_at = new_cursor
            # A crash here (enqueue raises) leaves the cursor advanced, so a
            # restart will NOT re-run this occurrence: at most once.
            self._enqueue(schedule.id)
