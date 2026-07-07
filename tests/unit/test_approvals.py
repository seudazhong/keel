"""Durable ApprovalStore tests (in-memory): scope isolation, single-shot resolve, expiry."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from keel_core.approvals import InMemoryApprovalStore

_T0 = datetime(2026, 7, 7, 9, 0, tzinfo=UTC)


async def _pending(store: InMemoryApprovalStore, **over: object) -> str:
    kw: dict[str, object] = dict(
        scope_id="u:1",
        run_id="r1",
        session_id="s1",
        tool="email.send",
        args={"to": "x"},
        call_id="c1",
        idempotency_key="k1",
        reason="tainted",
        expires_at=_T0 + timedelta(hours=24),
    )
    kw.update(over)
    return await store.create_pending(**kw)  # type: ignore[arg-type]


async def test_create_and_list_pending() -> None:
    store = InMemoryApprovalStore()
    aid = await _pending(store)
    rec = await store.get(aid)
    assert rec is not None and rec.status == "pending"
    assert [r.id for r in await store.list_pending("u:1")] == [aid]
    assert await store.list_pending("u:2") == []  # scope-isolated


async def test_resolve_is_single_shot() -> None:
    store = InMemoryApprovalStore()
    aid = await _pending(store)
    assert await store.resolve(aid, "granted", "dazhongguo") is True
    assert await store.resolve(aid, "denied", "dazhongguo") is False  # already resolved
    rec = await store.get(aid)
    assert rec is not None and rec.status == "granted"


async def test_expire_due_flips_only_past_pending() -> None:
    store = InMemoryApprovalStore()
    fresh = await _pending(store, expires_at=_T0 + timedelta(hours=24))
    stale = await _pending(
        store, call_id="c2", idempotency_key="k2", expires_at=_T0 - timedelta(minutes=1)
    )
    expired = await store.expire_due(_T0)
    assert expired == [stale]
    stale_rec = await store.get(stale)
    fresh_rec = await store.get(fresh)
    assert stale_rec is not None and stale_rec.status == "expired"
    assert fresh_rec is not None and fresh_rec.status == "pending"
