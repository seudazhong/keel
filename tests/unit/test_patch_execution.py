"""Unit tests for the atomic generation-execution primitives (WS-PP, P3b-0).

Covers the two building blocks the coordinator's ``execute_generation`` composes, in isolation from
any provider/git/network:

* the RUN-lease keeper (``_RunLeaseKeeper``): renews the run lease on a bounded cadence, and marks
  the lease *lost* (retaining the typed cause) on a ``False`` renewal or a ``SQLAlchemyError`` — the
  signal ``execute_generation`` folds into the author interrupt and re-checks before any finalize;
* the bounded renewal-interval helper (``_run_renew_interval``): always strictly below the lease.

The atomic finalize rollback (run + proposal + pointer all-or-nothing) is exercised end-to-end
against the real store through the coordinator in ``test_patch_coordinator.py``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from keel_core.patch.coordinator import (
    PATCH_RUN_RENEW_MAX_INTERVAL_SECONDS,
    PATCH_RUN_RENEW_MIN_INTERVAL_SECONDS,
    _run_renew_interval,
    _RunLeaseKeeper,
)


class _RenewSpyRunStore:
    """A minimal run store double that scripts ``renew`` (the only method the keeper calls)."""

    def __init__(self, *, result: bool = True, raises: BaseException | None = None) -> None:
        self.result = result
        self.raises = raises
        self.calls = 0

    async def renew(self, lease: Any, *, lease_seconds: int) -> bool:
        self.calls += 1
        if self.raises is not None:
            raise self.raises
        return self.result


def _lease() -> SimpleNamespace:
    # The keeper only reads ``lease.lease_seconds`` (renew arg) and ``lease.run_id`` (log context).
    return SimpleNamespace(run_id="run-1", lease_seconds=30)


def _keeper(store: _RenewSpyRunStore, *, interval: float = 0.01) -> _RunLeaseKeeper:
    return _RunLeaseKeeper(run_store=store, lease=_lease(), interval_seconds=interval)  # type: ignore[arg-type]


# --- interval helper -----------------------------------------------------------------


def test_renew_interval_is_always_strictly_below_the_lease() -> None:
    # A tiny lease still yields a renewal strictly *before* expiry (never lands on the boundary).
    assert _run_renew_interval(1.0) < 1.0
    assert _run_renew_interval(1.0) == pytest.approx(0.5)
    # Non-positive leases fall back to the floor (never zero / negative sleeps).
    assert _run_renew_interval(0.0) == PATCH_RUN_RENEW_MIN_INTERVAL_SECONDS
    assert _run_renew_interval(-5.0) == PATCH_RUN_RENEW_MIN_INTERVAL_SECONDS
    # A large lease is capped and still below the lease.
    big = _run_renew_interval(10_000.0)
    assert big == PATCH_RUN_RENEW_MAX_INTERVAL_SECONDS
    assert big < 10_000.0
    # The mid-range picks lease/3 within the bounds.
    assert _run_renew_interval(30.0) == pytest.approx(10.0)


# --- keeper --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_keeper_renews_periodically_without_losing_the_lease() -> None:
    store = _RenewSpyRunStore(result=True)
    keeper = _keeper(store)
    keeper.start()
    try:
        await asyncio.sleep(0.05)  # ~5 intervals of 0.01s
    finally:
        await keeper.stop()
    assert store.calls >= 1
    assert keeper.lost is False
    assert keeper.error is None


@pytest.mark.asyncio
async def test_keeper_marks_lost_when_renewal_is_superseded() -> None:
    # A ``False`` renewal means the lease was reclaimed by another worker: fail closed (lost), no
    # exception raised out of the loop (it is inspected by the caller, not thrown here).
    store = _RenewSpyRunStore(result=False)
    keeper = _keeper(store)
    keeper.start()
    try:
        await asyncio.wait_for(keeper._task, timeout=1.0)  # type: ignore[arg-type]
    finally:
        await keeper.stop()
    assert keeper.lost is True
    assert keeper.error is None
    assert store.calls == 1  # the loop exits immediately on the first lost renewal


@pytest.mark.asyncio
async def test_keeper_marks_lost_and_retains_cause_on_db_error() -> None:
    # A typed database error renewing the lease is treated as a lost lease AND retains the cause so
    # the abort chains it (never a broad swallow).
    boom = OperationalError("SELECT 1", {}, Exception("db down"))
    store = _RenewSpyRunStore(raises=boom)
    keeper = _keeper(store)
    keeper.start()
    try:
        await asyncio.wait_for(keeper._task, timeout=1.0)  # type: ignore[arg-type]
    finally:
        await keeper.stop()
    assert keeper.lost is True
    assert isinstance(keeper.error, SQLAlchemyError)
    assert keeper.error is boom


@pytest.mark.asyncio
async def test_keeper_stop_is_idempotent_and_deterministic() -> None:
    store = _RenewSpyRunStore(result=True)
    keeper = _keeper(store)
    keeper.start()
    await keeper.stop()
    # A second stop after the task is already cancelled/cleared is a no-op (never raises).
    await keeper.stop()
    assert keeper.lost is False
