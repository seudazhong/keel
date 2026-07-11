"""Core memory blocks with versioning (WS-D, FR-D3/FR-D4), scope-bound.

A :class:`PostgresMemoryStore` is bound to one scope and can only read/write that
scope's blocks (application-layer isolation, ADR-0009); Postgres RLS is the
defense-in-depth layer. Every ``set`` bumps the block version and archives the new
value to a history table (undo-able).
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.types import ScopeId

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


def format_core_memory(
    blocks: dict[str, str], *, defaults: tuple[str, ...] = ("persona", "human")
) -> str:
    """Render core-memory blocks as an always-visible system message.

    Default blocks render even when absent (empty tags) so the model knows they
    exist; any extra blocks follow, in insertion order.
    """
    keys = list(defaults) + [key for key in blocks if key not in defaults]
    lines = [
        "<core_memory>",
        "Editable long-term memory, always visible. Keep it accurate with the "
        "memory_* tools; it persists across all your sessions.",
    ]
    lines += [f"<{key}>{blocks.get(key, '')}</{key}>" for key in keys]
    lines.append("</core_memory>")
    return "\n".join(lines)


class PostgresMemoryStore:
    """Scope-bound store for editable memory blocks."""

    def __init__(self, engine: AsyncEngine, scope_id: ScopeId) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def get(self, key: str) -> str | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text("SELECT value FROM memory_blocks WHERE scope_id = :scope AND key = :key"),
                    {"scope": self._scope_id, "key": key},
                )
            ).first()
        return None if row is None else str(row.value)

    async def set(self, key: str, value: str) -> int:
        """Upsert a block, bump its version, and archive the new value. Returns version."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            version = int(
                (
                    await conn.execute(
                        text(
                            "INSERT INTO memory_blocks (scope_id, key, value, version) "
                            "VALUES (:scope, :key, :value, 1) "
                            "ON CONFLICT (scope_id, key) DO UPDATE "
                            "SET value = :value, version = memory_blocks.version + 1, "
                            "updated_at = now() RETURNING version"
                        ),
                        {"scope": self._scope_id, "key": key, "value": value},
                    )
                )
                .one()
                .version
            )
            await conn.execute(
                text(
                    "INSERT INTO memory_block_versions (scope_id, key, version, value) "
                    "VALUES (:scope, :key, :version, :value)"
                ),
                {"scope": self._scope_id, "key": key, "version": version, "value": value},
            )
        return version

    async def history(self, key: str) -> list[tuple[int, str]]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT version, value FROM memory_block_versions "
                        "WHERE scope_id = :scope AND key = :key ORDER BY version"
                    ),
                    {"scope": self._scope_id, "key": key},
                )
            ).all()
        return [(int(row.version), str(row.value)) for row in rows]
