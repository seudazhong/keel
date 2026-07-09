"""Admin overview aggregate: scope-bound object counts + usage totals (B3 dashboards).

Read-only. Every count is scope-isolated (the ``app.scope_id`` RLS GUC plus an explicit
``scope_id`` filter). Usage totals sum each run's ``run.ended`` event usage, so the
numbers reconcile with the per-turn accounting the loop persists.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")
_APPROVAL_STATUSES = ("pending", "granted", "denied", "expired")

_SESSIONS_SQL = text("SELECT count(*) FROM sessions WHERE scope_id = :s")
_CONNECTORS_SQL = text("SELECT count(*) FROM connector_tokens WHERE scope_id = :s")
_SCHEDULES_SQL = text(
    "SELECT count(*) AS total, count(*) FILTER (WHERE enabled) AS enabled "
    "FROM schedules WHERE scope_id = :s"
)
_APPROVALS_SQL = text(
    "SELECT status, count(*) AS c FROM approvals WHERE scope_id = :s GROUP BY status"
)
_USAGE_SQL = text(
    "SELECT count(*) AS runs, "
    "COALESCE(SUM((payload->'usage'->>'prompt_tokens')::bigint), 0) AS prompt, "
    "COALESCE(SUM((payload->'usage'->>'completion_tokens')::bigint), 0) AS completion, "
    "COALESCE(SUM((payload->'usage'->>'cache_read_tokens')::bigint), 0) AS cache, "
    "COALESCE(SUM((payload->'usage'->>'cost_usd')::double precision), 0) AS cost "
    "FROM events WHERE scope_id = :s AND type = 'run.ended'"
)


async def compute_overview(engine: AsyncEngine, scope_id: str) -> dict[str, Any]:
    """Aggregate the scope's sessions/schedules/approvals/connectors + token & cost totals."""
    params = {"s": scope_id}
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        sessions = await conn.scalar(_SESSIONS_SQL, params)
        schedules = (await conn.execute(_SCHEDULES_SQL, params)).mappings().one()
        approval_rows = (await conn.execute(_APPROVALS_SQL, params)).mappings().all()
        connectors = await conn.scalar(_CONNECTORS_SQL, params)
        usage = (await conn.execute(_USAGE_SQL, params)).mappings().one()

    approvals = {status: 0 for status in _APPROVAL_STATUSES}
    for row in approval_rows:
        approvals[str(row["status"])] = int(row["c"])
    return {
        "sessions": int(sessions or 0),
        "schedules": {"total": int(schedules["total"]), "enabled": int(schedules["enabled"])},
        "approvals": approvals,
        "connectors": int(connectors or 0),
        "usage": {
            "runs": int(usage["runs"]),
            "prompt_tokens": int(usage["prompt"]),
            "completion_tokens": int(usage["completion"]),
            "cache_read_tokens": int(usage["cache"]),
            "cost_usd": float(usage["cost"]),
        },
    }
