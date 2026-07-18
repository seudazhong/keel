"""Durable + in-memory patch-writeback audit ledger (WS-PP).

An append-only record of every control-plane writeback step (``verify_base`` / ``push_branch`` /
``create_pr`` / ``reconcile``) for a proposal, so a human can trace exactly what the trusted
control plane did on a remote — base verification result, the pushed branch/sha, and the opened
Draft PR — without any source or token ever entering the ledger. Org-owned + RLS-scoped, mirroring
:class:`~keel_core.patch.store.PostgresPatchProposalStore`.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_ORG = text("SELECT set_config('app.org_id', :org, true)")


def _new_ledger_id() -> str:
    return f"wbl_{uuid.uuid4().hex}"


@dataclass(frozen=True, slots=True)
class WritebackLedgerEntry:
    id: str
    org_id: str
    project_id: str
    proposal_id: str
    step: str
    status: str
    before_sha: str | None
    after_sha: str | None
    remote_branch: str
    pr_number: int | None
    detail: Mapping[str, Any]
    created_at: datetime


class InMemoryPatchWritebackLedger:
    """Dependency-free append-only ledger (tests / lite)."""

    def __init__(self) -> None:
        self.entries: list[WritebackLedgerEntry] = []

    async def record(
        self,
        *,
        proposal_id: str,
        org_id: str,
        project_id: str,
        step: str,
        status: str,
        before_sha: str | None = None,
        after_sha: str | None = None,
        remote_branch: str = "",
        pr_number: int | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        self.entries.append(
            WritebackLedgerEntry(
                id=_new_ledger_id(),
                org_id=org_id,
                project_id=project_id,
                proposal_id=proposal_id,
                step=step,
                status=status,
                before_sha=before_sha,
                after_sha=after_sha,
                remote_branch=remote_branch,
                pr_number=pr_number,
                detail=dict(detail or {}),
                created_at=datetime.now(UTC),
            )
        )

    async def list_for_proposal(self, org_id: str, proposal_id: str) -> list[WritebackLedgerEntry]:
        return [e for e in self.entries if e.org_id == org_id and e.proposal_id == proposal_id]


class PostgresPatchWritebackLedger:
    """Durable append-only ledger; org-scoped writes set ``app.org_id`` (RLS + FORCE)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(
        self,
        *,
        proposal_id: str,
        org_id: str,
        project_id: str,
        step: str,
        status: str,
        before_sha: str | None = None,
        after_sha: str | None = None,
        remote_branch: str = "",
        pr_number: int | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            await conn.execute(
                text(
                    "INSERT INTO patch_writeback_ledger "
                    "(id, org_id, project_id, proposal_id, step, status, before_sha, after_sha, "
                    " remote_branch, pr_number, detail) "
                    "VALUES (:id, :org, :pid, :prop, :step, :status, :before, :after, "
                    " :branch, :pr, CAST(:detail AS jsonb))"
                ),
                {
                    "id": _new_ledger_id(),
                    "org": org_id,
                    "pid": project_id,
                    "prop": proposal_id,
                    "step": step,
                    "status": status,
                    "before": before_sha,
                    "after": after_sha,
                    "branch": remote_branch,
                    "pr": pr_number,
                    "detail": json.dumps(dict(detail or {})),
                },
            )

    async def list_for_proposal(self, org_id: str, proposal_id: str) -> list[WritebackLedgerEntry]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT id, org_id, project_id, proposal_id, step, status, before_sha, "
                            "after_sha, remote_branch, pr_number, detail, created_at "
                            "FROM patch_writeback_ledger WHERE proposal_id = :prop "
                            "ORDER BY created_at"
                        ),
                        {"prop": proposal_id},
                    )
                )
                .mappings()
                .all()
            )
        return [
            WritebackLedgerEntry(
                id=r["id"],
                org_id=r["org_id"],
                project_id=r["project_id"],
                proposal_id=r["proposal_id"],
                step=r["step"],
                status=r["status"],
                before_sha=r["before_sha"],
                after_sha=r["after_sha"],
                remote_branch=r["remote_branch"],
                pr_number=r["pr_number"],
                detail=r["detail"] if isinstance(r["detail"], Mapping) else {},
                created_at=r["created_at"],
            )
            for r in rows
        ]


__all__ = [
    "InMemoryPatchWritebackLedger",
    "PostgresPatchWritebackLedger",
    "WritebackLedgerEntry",
]
