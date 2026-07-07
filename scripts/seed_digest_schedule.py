"""Seed one digest schedule for a scope (idempotent).

    python -m uv run python scripts/seed_digest_schedule.py --scope web:local

Creates a schedule that is immediately due (``next_run_at = now``, interval 1 day) so the
worker's ``scheduler_tick`` picks it up and starts a digest run. Safe to re-run."""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime

from sqlalchemy import text

from keel_core.config import get_settings
from keel_core.db import make_async_engine
from keel_core.digest import digest_session_id


async def seed(scope_id: str) -> None:
    engine = make_async_engine(get_settings())
    schedule_id = f"digest:{scope_id}"
    async with engine.begin() as conn:
        await conn.execute(
            text("select set_config('app.scope_id', :scope, true)"), {"scope": scope_id}
        )
        await conn.execute(
            text(
                "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                "spec, next_run_at, interval_s, enabled) VALUES "
                "(:id, :scope, 'digest', :session, 'interval', '86400', :now, 86400, true) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {
                "id": schedule_id,
                "scope": scope_id,
                "session": digest_session_id(scope_id),
                "now": datetime.now(UTC),
            },
        )
    await engine.dispose()
    print(f"seeded schedule {schedule_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed a digest schedule for a scope.")
    parser.add_argument("--scope", default="web:local")
    args = parser.parse_args()
    if sys.platform == "win32":  # psycopg async needs a selector loop on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(seed(args.scope))


if __name__ == "__main__":
    main()
