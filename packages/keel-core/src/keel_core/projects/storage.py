"""Project storage integration over the coding-storage local drivers (M3.7, WS-P).

A narrow :class:`ProjectStorage` seam the :class:`~keel_core.projects.service.ProjectService`
uses to create/import/fetch a project's durable authoritative repository and to
materialize/remove run-scoped worktrees. It is backed by the secure local coding drivers
(:mod:`keel_core.coding`), which already enforce path confinement, storage quotas, safe git
argument arrays, and an HTTPS host allowlist.

The control plane owns the authoritative repository handle; a materialized worktree is an
isolated clone with no remote/alternates, so the sandbox never receives writable access to
the authoritative repo (confinement invariant).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from keel_core.coding import (
    ActiveGitStore,
    WorktreeStore,
)
from keel_core.coding import (
    ProjectId as CodingProjectId,
)
from keel_core.coding import (
    project_id as coding_project_id,
)
from keel_core.coding import (
    run_id as coding_run_id,
)


@dataclass(frozen=True)
class MaterializedWorktree:
    """The on-disk location + resolved commit of a materialized worktree."""

    path: str
    commit: str


@runtime_checkable
class ProjectStorage(Protocol):
    """The storage operations the project service needs (authoritative repo + worktrees)."""

    def create_blank(self, handle: str, *, default_branch: str) -> str | None: ...
    def import_remote(self, handle: str, remote_url: str, *, default_branch: str) -> str | None: ...
    def fetch_remote(self, handle: str, remote_url: str) -> str | None: ...
    def materialize_worktree(
        self, handle: str, worktree_run_id: str, *, ref: str
    ) -> MaterializedWorktree: ...
    def remove_worktree(self, handle: str, worktree_run_id: str) -> bool: ...


def worktree_storage_id(run_id: str) -> str:
    """Derive a portable coding-storage run identifier from a durable run id.

    Durable run ids are opaque tenant strings; the coding stores require a portable lowercase
    identifier, so we hash the run id into a stable, collision-resistant handle.
    """
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:40]
    return f"r{digest}"


class LocalProjectStorage:
    """A :class:`ProjectStorage` over the local coding drivers."""

    def __init__(self, active_git: ActiveGitStore, worktrees: WorktreeStore) -> None:
        self._active_git = active_git
        self._worktrees = worktrees

    def _pid(self, handle: str) -> CodingProjectId:
        return coding_project_id(handle)

    def create_blank(self, handle: str, *, default_branch: str) -> str | None:
        record = self._active_git.create_project(self._pid(handle), default_branch=default_branch)
        return record.head

    def import_remote(self, handle: str, remote_url: str, *, default_branch: str) -> str | None:
        record = self._active_git.import_project(
            self._pid(handle), remote_url, default_branch=default_branch
        )
        return record.head

    def fetch_remote(self, handle: str, remote_url: str) -> str | None:
        record = self._active_git.fetch(self._pid(handle), remote_url)
        return record.head

    def materialize_worktree(
        self, handle: str, worktree_run_id: str, *, ref: str
    ) -> MaterializedWorktree:
        record = self._worktrees.materialize(
            self._pid(handle), coding_run_id(worktree_run_id), ref=ref
        )
        return MaterializedWorktree(path=str(record.path), commit=record.commit)

    def remove_worktree(self, handle: str, worktree_run_id: str) -> bool:
        return self._worktrees.remove(self._pid(handle), coding_run_id(worktree_run_id))


class InMemoryProjectStorage:
    """A dependency-free :class:`ProjectStorage` fake for unit tests.

    Records operations and returns deterministic synthetic commits without touching git.
    """

    def __init__(self) -> None:
        self.projects: dict[str, str | None] = {}
        self.worktrees: dict[tuple[str, str], MaterializedWorktree] = {}
        self.fetches: list[tuple[str, str]] = []
        self.fail_import: bool = False
        self.fail_fetch: bool = False

    def _commit(self, seed: str) -> str:
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()  # noqa: S324 (non-crypto id)

    def create_blank(self, handle: str, *, default_branch: str) -> str | None:
        self.projects[handle] = None
        return None

    def import_remote(self, handle: str, remote_url: str, *, default_branch: str) -> str | None:
        if self.fail_import:
            raise RuntimeError("simulated import failure")
        commit = self._commit(f"{handle}:{remote_url}")
        self.projects[handle] = commit
        self.fetches.append((handle, remote_url))
        return commit

    def fetch_remote(self, handle: str, remote_url: str) -> str | None:
        if self.fail_fetch:
            raise RuntimeError("simulated fetch failure")
        commit = self._commit(f"{handle}:{remote_url}:{len(self.fetches)}")
        self.projects[handle] = commit
        self.fetches.append((handle, remote_url))
        return commit

    def materialize_worktree(
        self, handle: str, worktree_run_id: str, *, ref: str
    ) -> MaterializedWorktree:
        commit = self.projects.get(handle) or self._commit(f"{handle}:{ref}")
        record = MaterializedWorktree(
            path=f"/virtual/worktrees/{handle}/{worktree_run_id}", commit=commit
        )
        self.worktrees[(handle, worktree_run_id)] = record
        return record

    def remove_worktree(self, handle: str, worktree_run_id: str) -> bool:
        return self.worktrees.pop((handle, worktree_run_id), None) is not None


__all__ = [
    "InMemoryProjectStorage",
    "LocalProjectStorage",
    "MaterializedWorktree",
    "ProjectStorage",
    "worktree_storage_id",
]
