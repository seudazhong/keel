"""Spike S2 acceptance: at-most-once scheduling (0 or 1 runs, never twice)."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from keel_scheduler.atmostonce import AtMostOnceScheduler, InMemoryClaimStore, Schedule

_NOW = datetime(2026, 1, 1, 9, 0, 0)
_HOUR = timedelta(hours=1)


def test_runs_once_when_due() -> None:
    store = InMemoryClaimStore({"job": _NOW})
    runs: list[str] = []
    AtMostOnceScheduler(store, runs.append).tick([Schedule("job", _NOW, _HOUR)], _NOW)
    assert runs == ["job"]
    assert store.snapshot()["job"] == _NOW + _HOUR


def test_not_due_does_not_run() -> None:
    later = _NOW + _HOUR
    runs: list[str] = []
    AtMostOnceScheduler(InMemoryClaimStore({"job": later}), runs.append).tick(
        [Schedule("job", later, _HOUR)], _NOW
    )
    assert runs == []


def test_crash_between_claim_and_enqueue_never_double_runs() -> None:
    store = InMemoryClaimStore({"job": _NOW})

    def crashing_enqueue(_job_id: str) -> None:
        raise RuntimeError("worker crashed before the run was durably recorded")

    # Tick 1: cursor advances, then the enqueue crashes.
    with pytest.raises(RuntimeError):
        AtMostOnceScheduler(store, crashing_enqueue).tick([Schedule("job", _NOW, _HOUR)], _NOW)
    assert store.snapshot()["job"] == _NOW + _HOUR  # advanced despite the crash

    # Restart: rebuild from the durable cursor and tick again at the same instant.
    runs: list[str] = []
    AtMostOnceScheduler(store, runs.append).tick(
        [Schedule("job", store.snapshot()["job"], _HOUR)], _NOW
    )
    assert runs == []  # no longer due -> 0 total runs, never two


def test_two_leaders_enqueue_exactly_once() -> None:
    store = InMemoryClaimStore({"job": _NOW})
    runs: list[str] = []
    expected = store.snapshot()["job"]
    AtMostOnceScheduler(store, runs.append).tick([Schedule("job", expected, _HOUR)], _NOW)
    # A second leader ticks with the same (stale) expected cursor -> CAS fails.
    AtMostOnceScheduler(store, runs.append).tick([Schedule("job", expected, _HOUR)], _NOW)
    assert runs == ["job"]
