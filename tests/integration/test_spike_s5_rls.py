"""Spike S5 acceptance (defense-in-depth): Postgres RLS enforces scope isolation.

Complements the application-layer ScopeGuard (tests/unit/test_spike_s5_scope_guard).
A row-level policy keyed by a session GUC (``app.scope_id``) makes one scope's
rows invisible to another even on a raw SQL SELECT.

The connection role (``keel``) is a superuser and would bypass RLS, so the test
``SET ROLE`` to a non-superuser role to exercise the policy.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


async def test_rls_enforces_scope_isolation(pg_engine: AsyncEngine) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS rls_demo"))
        await conn.execute(text("DROP ROLE IF EXISTS rls_tester"))
        await conn.execute(text("CREATE ROLE rls_tester NOSUPERUSER"))
        await conn.execute(
            text("CREATE TABLE rls_demo (id serial PRIMARY KEY, scope_id text NOT NULL, val text)")
        )
        await conn.execute(text("ALTER TABLE rls_demo ENABLE ROW LEVEL SECURITY"))
        await conn.execute(text("GRANT SELECT ON rls_demo TO rls_tester"))
        await conn.execute(
            text(
                "CREATE POLICY scope_isolation ON rls_demo "
                "USING (scope_id = current_setting('app.scope_id', true))"
            )
        )
        await conn.execute(
            text("INSERT INTO rls_demo (scope_id, val) VALUES ('A','a1'),('A','a2'),('B','b1')")
        )

    try:
        async with pg_engine.connect() as conn:
            await conn.execute(text("SET ROLE rls_tester"))

            await conn.execute(text("SET app.scope_id = 'A'"))
            rows_a = (await conn.execute(text("SELECT scope_id FROM rls_demo"))).fetchall()
            assert {row[0] for row in rows_a} == {"A"}
            assert len(rows_a) == 2

            await conn.execute(text("SET app.scope_id = 'B'"))
            rows_b = (await conn.execute(text("SELECT scope_id FROM rls_demo"))).fetchall()
            assert {row[0] for row in rows_b} == {"B"}  # scope A's rows are invisible

            await conn.execute(text("RESET app.scope_id"))
            rows_none = (await conn.execute(text("SELECT scope_id FROM rls_demo"))).fetchall()
            assert rows_none == []  # deny-by-default when no scope is set

            await conn.execute(text("RESET ROLE"))
    finally:
        async with pg_engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS rls_demo"))
            await conn.execute(text("DROP ROLE IF EXISTS rls_tester"))
