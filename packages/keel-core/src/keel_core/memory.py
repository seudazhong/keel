"""Core memory blocks with versioning (WS-D, FR-D3/FR-D4), scope-bound.

A :class:`PostgresMemoryStore` is bound to one scope and can only read/write that
scope's blocks (application-layer isolation, ADR-0009); Postgres RLS is the
defense-in-depth layer. Every ``set`` bumps the block version and archives the new
value to a history table (undo-able).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.protocols import ToolContext, ToolResult
from keel_core.types import ScopeId

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


async def purge_scope(engine: AsyncEngine, scope_id: ScopeId) -> int:
    """Erase every core-memory block + version for a scope (idempotent). Returns rows removed."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        versions = await conn.execute(
            text("DELETE FROM memory_block_versions WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
        blocks = await conn.execute(
            text("DELETE FROM memory_blocks WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return int(versions.rowcount or 0) + int(blocks.rowcount or 0)


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

    async def blocks(self) -> dict[str, str]:
        """Return all of the scope's memory blocks as ``{key: value}``."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text("SELECT key, value FROM memory_blocks WHERE scope_id = :scope"),
                    {"scope": self._scope_id},
                )
            ).all()
        return {str(row.key): str(row.value) for row in rows}

    async def versions(self) -> dict[str, int]:
        """Return all of the scope's memory blocks as ``{key: version}``."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text("SELECT key, version FROM memory_blocks WHERE scope_id = :scope"),
                    {"scope": self._scope_id},
                )
            ).all()
        return {str(row.key): int(row.version) for row in rows}

    async def snapshot(self) -> list[tuple[str, str, int]]:
        """Return a consistent key/value/version snapshot ordered by block name."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT key, value, version FROM memory_blocks "
                        "WHERE scope_id = :scope ORDER BY key"
                    ),
                    {"scope": self._scope_id},
                )
            ).all()
        return [(str(row.key), str(row.value), int(row.version)) for row in rows]


class _MemoryTool:
    writes = True

    def __init__(self, engine: AsyncEngine, *, max_chars: int = 2000) -> None:
        self._engine = engine
        self._max_chars = max_chars

    def _store(self, ctx: ToolContext) -> PostgresMemoryStore:
        return PostgresMemoryStore(self._engine, ctx.scope_id)


class MemoryAppendTool(_MemoryTool):
    name = "memory_append"
    description = "Append a line to one of your core memory blocks (creates it if absent)."

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"block": {"type": "string"}, "content": {"type": "string"}},
            "required": ["block", "content"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        store = self._store(ctx)
        block, content = str(args.get("block", "")), str(args.get("content", ""))
        current = await store.get(block) or ""
        updated = f"{current}\n{content}".strip() if current else content
        if len(updated) > self._max_chars:
            return ToolResult(
                ok=False,
                output=f"block '{block}' would exceed {self._max_chars} chars; "
                "use memory_rethink to summarize.",
            )
        version = await store.set(block, updated)
        return ToolResult(ok=True, output=f"appended to '{block}' (v{version})")


class MemoryReplaceTool(_MemoryTool):
    name = "memory_replace"
    description = "Replace the first occurrence of old with new in a core memory block."

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "block": {"type": "string"},
                "old": {"type": "string"},
                "new": {"type": "string"},
            },
            "required": ["block", "old", "new"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        store = self._store(ctx)
        block, old, new = (
            str(args.get("block", "")),
            str(args.get("old", "")),
            str(args.get("new", "")),
        )
        current = await store.get(block)
        if current is None or old not in current:
            return ToolResult(ok=False, output=f"'{old}' not found in block '{block}'")
        updated = current.replace(old, new, 1)
        if len(updated) > self._max_chars:
            return ToolResult(
                ok=False, output=f"block '{block}' would exceed {self._max_chars} chars"
            )
        version = await store.set(block, updated)
        return ToolResult(ok=True, output=f"replaced in '{block}' (v{version})")


class MemoryRethinkTool(_MemoryTool):
    name = "memory_rethink"
    description = "Overwrite a core memory block entirely (for compaction or correction)."

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"block": {"type": "string"}, "content": {"type": "string"}},
            "required": ["block", "content"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        block, content = str(args.get("block", "")), str(args.get("content", ""))
        if len(content) > self._max_chars:
            return ToolResult(ok=False, output=f"content exceeds {self._max_chars} chars")
        version = await self._store(ctx).set(block, content)
        return ToolResult(ok=True, output=f"rewrote '{block}' (v{version})")
