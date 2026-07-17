"""Bounded local filesystem cleanup for erasure (M3.5, WS-K).

Two kinds of on-disk state can outlive a database erasure:

* **Coding artifacts** — a managed coding project's git repo, snapshots, worktrees, and
  artifacts under the coding storage root. Erased by project id via
  :meth:`keel_core.coding.local.LocalCodingStorage.purge_project` (path-confined).
* **Tool spill files** — bounded overflow files (``output-<uuid>.txt``) written when a
  tool's output exceeds its budget (:mod:`keel_core.tools.bounding`). A ``tool.result``
  event records the ``spill_path``; erasure deletes those files, but only when they live
  under a configured spill root (defence against deleting an arbitrary path recorded in a
  stale event).

Both cleaners are **bounded**: they never traverse outside their configured root and only
act on explicit ids/paths. With no configured root/store they are no-ops so durable-store
erasure still succeeds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from pathlib import Path
from typing import Protocol, runtime_checkable


@runtime_checkable
class ProjectPurger(Protocol):
    """The coding-storage slice the cleaner needs (satisfied by ``LocalCodingStorage``)."""

    def purge_project(self, project_id: str) -> bool: ...


class CodingArtifactCleaner:
    """Erases one coding project's on-disk trace via the coding storage seam."""

    def __init__(self, storage: ProjectPurger | None) -> None:
        self._storage = storage

    async def purge_project(self, project_id: str) -> bool:
        """Remove every on-disk artifact for ``project_id``. No-op without a store."""
        if self._storage is None:
            return False
        return await asyncio.to_thread(self._storage.purge_project, project_id)


class ToolSpillCleaner:
    """Deletes bounded tool-spill files, confined to a configured spill root."""

    def __init__(self, spill_root: Path | str | None) -> None:
        self._root = Path(spill_root).resolve() if spill_root is not None else None

    def _confined(self, candidate: Path) -> Path | None:
        if self._root is None:
            return None
        try:
            resolved = candidate.resolve()
            resolved.relative_to(self._root)
        except (OSError, ValueError):
            return None
        return resolved

    def _purge(self, paths: Iterable[str]) -> int:
        removed = 0
        for raw in paths:
            if not raw:
                continue
            target = self._confined(Path(raw))
            if target is None:
                continue
            try:
                if target.is_file():
                    target.unlink()
                    removed += 1
            except OSError:
                continue
        return removed

    async def purge_paths(self, paths: Iterable[str]) -> int:
        """Delete each spill file under the root. Returns files removed; no-op without root."""
        materialized = list(paths)
        if self._root is None or not materialized:
            return 0
        return await asyncio.to_thread(self._purge, materialized)


__all__ = ["CodingArtifactCleaner", "ProjectPurger", "ToolSpillCleaner"]
