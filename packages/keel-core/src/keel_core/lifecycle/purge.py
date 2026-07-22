"""Scoped purge repository: the authoritative all-store cleanup for erasure (M3.5).

Each method performs an idempotent, scope-bound (RLS-aware) physical delete for one
persisted store and returns the rows removed. Methods delegate to each store module's
co-located ``purge_scope`` where one exists, so the SQL lives next to the schema it knows;
the two cross-package tables (``schedules``, owned by ``keel-scheduler``) are inlined to
keep ``keel-core`` free of a reverse dependency.

Also here: the read-only helpers the coordinator needs *before* deletion — the scope's
session ids (for tombstones + Redis cleanup) and recorded tool-spill paths.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import purge_scope as _purge_approvals
from keel_core.connector_repository import purge_scope as _purge_connector_state
from keel_core.consolidation.cursor import purge_scope as _purge_cursor
from keel_core.consolidation.proposals import purge_scope as _purge_proposals
from keel_core.effect_store import purge_scope as _purge_effects
from keel_core.im_routing import purge_scope as _purge_im_routing
from keel_core.jobs import purge_scope as _purge_jobs
from keel_core.knowledge.store import purge_scope as _purge_knowledge
from keel_core.memory import purge_scope as _purge_memory
from keel_core.oauth_state import purge_scope as _purge_oauth
from keel_core.outbox import purge_scope as _purge_outbox
from keel_core.recall import purge_scope as _purge_embeddings
from keel_core.recall import purge_session as _purge_embeddings_session
from keel_core.runs import purge_scope as _purge_runs
from keel_core.search import purge_scope as _purge_archival
from keel_core.state import purge_scope as _purge_state
from keel_core.state import purge_session as _purge_state_session
from keel_core.tokens import purge_scope as _purge_tokens

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


class ScopePurgeRepository:
    """All-store scoped purge + pre-deletion read helpers (idempotent)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    # --- read helpers (run before deletion) -------------------------------------------
    async def session_ids(self, scope_id: str) -> list[str]:
        """Every session id in a scope (for tombstones + Redis stream cleanup)."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            rows = (
                await conn.execute(
                    text("SELECT id FROM sessions WHERE scope_id = :scope"),
                    {"scope": scope_id},
                )
            ).all()
        return [row.id for row in rows]

    async def spill_paths(self, scope_id: str, session_id: str | None = None) -> list[str]:
        """Recorded ``tool.result`` spill file paths for a scope (or one session)."""
        sql = (
            "SELECT payload->>'spill_path' AS spill FROM events "
            "WHERE scope_id = :scope AND type = 'tool.result' "
            "AND payload->>'spill_path' IS NOT NULL"
        )
        params: dict[str, str] = {"scope": scope_id}
        if session_id is not None:
            sql += " AND session_id = :session"
            params["session"] = session_id
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            rows = (await conn.execute(text(sql), params)).all()
        return [row.spill for row in rows if row.spill]

    # --- scope purge methods (one per store) ------------------------------------------
    async def message_embeddings(self, scope_id: str) -> int:
        return await _purge_embeddings(self._engine, scope_id)

    async def events_and_sessions(self, scope_id: str) -> int:
        return await _purge_state(self._engine, scope_id)

    async def archival(self, scope_id: str) -> int:
        return await _purge_archival(self._engine, scope_id)

    async def memory(self, scope_id: str) -> int:
        return await _purge_memory(self._engine, scope_id)

    async def memory_proposals(self, scope_id: str) -> int:
        return await _purge_proposals(self._engine, scope_id)

    async def consolidation_cursor(self, scope_id: str) -> int:
        return await _purge_cursor(self._engine, scope_id)

    async def knowledge(self, scope_id: str) -> int:
        return await _purge_knowledge(self._engine, scope_id)

    async def connector_tokens(self, scope_id: str) -> int:
        return await _purge_tokens(self._engine, scope_id)

    async def connector_state(self, scope_id: str) -> int:
        return await _purge_connector_state(self._engine, scope_id)

    async def connector_outbox(self, scope_id: str) -> int:
        return await _purge_outbox(self._engine, scope_id)

    async def effects(self, scope_id: str) -> int:
        """R1B: erase the scope's Effect ledger rows; the reconciliation-outbox pointer
        cascades away with each Effect (composite FK, ``ON DELETE CASCADE``)."""
        return await _purge_effects(self._engine, scope_id)

    async def oauth_states(self, scope_id: str) -> int:
        return await _purge_oauth(self._engine, scope_id)

    async def approvals(self, scope_id: str) -> int:
        return await _purge_approvals(self._engine, scope_id)

    async def jobs(self, scope_id: str, *, exclude_job_id: str | None = None) -> int:
        return await _purge_jobs(self._engine, scope_id, exclude_job_id=exclude_job_id)

    async def runs(self, scope_id: str) -> int:
        return await _purge_runs(self._engine, scope_id)

    async def im_routing(self, scope_id: str) -> int:
        return await _purge_im_routing(self._engine, scope_id)

    async def schedules(self, scope_id: str) -> int:
        # Inlined (schedules is owned by keel-scheduler) to avoid a reverse dependency.
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            result = await conn.execute(
                text("DELETE FROM schedules WHERE scope_id = :scope"),
                {"scope": scope_id},
            )
        return int(result.rowcount or 0)

    # --- session-scoped purge (narrower erasure) --------------------------------------
    async def session_message_embeddings(self, scope_id: str, session_id: str) -> int:
        return await _purge_embeddings_session(self._engine, scope_id, session_id)

    async def session_events(self, scope_id: str, session_id: str) -> int:
        return await _purge_state_session(self._engine, scope_id, session_id)


__all__ = ["ScopePurgeRepository"]
