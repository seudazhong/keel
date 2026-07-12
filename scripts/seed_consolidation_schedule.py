"""Seed one memory-consolidation schedule for a scope (idempotent).

    python -m uv run python scripts/seed_consolidation_schedule.py --scope web:local

Creates a schedule that is immediately due (``next_run_at = now``, interval 1 day) so the
worker's ``scheduler_tick`` picks it up and dispatches a consolidation run. Safe to re-run.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import get_settings
from keel_core.consolidation.agent import consolidation_schedule_id
from keel_core.db import make_async_engine


async def seed(scope_id: str, engine: AsyncEngine) -> str:
    """Insert the consolidation schedule for ``scope_id`` (idempotent); return its id."""
    schedule_id = consolidation_schedule_id(scope_id)
    async with engine.begin() as conn:
        await conn.execute(
            text("select set_config('app.scope_id', :scope, true)"), {"scope": scope_id}
        )
        await conn.execute(
            text(
                "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                "spec, next_run_at, interval_s, enabled) VALUES "
                "(:id, :scope, 'memory-consolidator', :session, 'interval', '86400', "
                ":now, 86400, true) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {
                "id": schedule_id,
                "scope": scope_id,
                "session": f"consolidation:{scope_id}",
                "now": datetime.now(UTC),
            },
        )
    return schedule_id


async def _run(scope_id: str) -> None:
    engine = make_async_engine(get_settings())
    try:
        schedule_id = await seed(scope_id, engine)
        print(f"seeded schedule {schedule_id}")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed a memory-consolidation schedule.")
    parser.add_argument("--scope", default="web:local")
    args = parser.parse_args()
    if sys.platform == "win32":  # psycopg async needs a selector loop on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(_run(args.scope))


if __name__ == "__main__":
    main()
