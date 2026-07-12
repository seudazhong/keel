"""Human-reviewed core-memory proposals + atomic apply (spec §11).

Consolidation never edits core memory directly: it writes a *proposal* keyed by an
idempotency key (safe under whole-batch retry). A human approve/reject resolves it.
Approve is an atomic compare-and-set against the block's expected version — a proposal
raised against version N applies only while the block is still at N, else it goes
``stale`` (the block changed underneath it). Scope-bound + RLS (ADR-0009).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.hashing import consolidation_idempotency_key

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


class ProposalOutcome(StrEnum):
    """The result of resolving a proposal."""

    applied = "applied"
    rejected = "rejected"
    stale = "stale"
    not_found = "not_found"
    already_resolved = "already_resolved"


@dataclass(frozen=True)
class ProposalResolution:
    """Outcome of an approve/reject, plus the new block version when applied."""

    outcome: ProposalOutcome
    version: int | None = None


@dataclass
class MemoryProposal:
    """A pending or resolved core-memory rewrite proposal."""

    id: str
    scope_id: str
    block: str
    expected_version: int
    proposed_value: str
    reason: str
    confidence: float
    source_event_ids: list[int]
    status: str
    created_at: datetime
    resolved_at: datetime | None = None
    resolved_by: str | None = None


def _to_proposal(row: Any) -> MemoryProposal:
    return MemoryProposal(
        id=row["id"],
        scope_id=row["scope_id"],
        block=row["block"],
        expected_version=int(row["expected_version"]),
        proposed_value=row["proposed_value"],
        reason=row["reason"],
        confidence=float(row["confidence"]),
        source_event_ids=[int(i) for i in (row["source_event_ids"] or [])],
        status=row["status"],
        created_at=row["created_at"],
        resolved_at=row["resolved_at"],
        resolved_by=row["resolved_by"],
    )


class MemoryProposalStore:
    """Scope-bound store of core-memory rewrite proposals over ``memory_proposals``."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def propose(
        self,
        *,
        block: str,
        proposed_value: str,
        reason: str,
        confidence: float,
        source_event_ids: list[int],
    ) -> tuple[str, bool]:
        """Insert a proposal idempotently. Returns ``(proposal_id, created)``."""
        proposal_id = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            current = (
                await conn.execute(
                    text(
                        "SELECT version FROM memory_blocks WHERE scope_id = :scope AND key = :block"
                    ),
                    {"scope": self._scope_id, "block": block},
                )
            ).one_or_none()
            expected_version = 0 if current is None else int(current.version)
            key = consolidation_idempotency_key(
                self._scope_id, block, expected_version, proposed_value, source_event_ids
            )
            inserted = (
                await conn.execute(
                    text(
                        "INSERT INTO memory_proposals "
                        "(id, scope_id, block, expected_version, proposed_value, reason, "
                        "confidence, source_event_ids, idempotency_key, status, created_at) "
                        "VALUES (:id, :scope, :block, :expected, :value, :reason, :confidence, "
                        "CAST(:ids AS bigint[]), :key, 'pending', now()) "
                        "ON CONFLICT (scope_id, idempotency_key) DO NOTHING "
                        "RETURNING id"
                    ),
                    {
                        "id": proposal_id,
                        "scope": self._scope_id,
                        "block": block,
                        "expected": expected_version,
                        "value": proposed_value,
                        "reason": reason,
                        "confidence": confidence,
                        "ids": list(source_event_ids),
                        "key": key,
                    },
                )
            ).one_or_none()
            if inserted is not None:
                return str(inserted.id), True
            existing = (
                await conn.execute(
                    text(
                        "SELECT id FROM memory_proposals "
                        "WHERE scope_id = :scope AND idempotency_key = :key"
                    ),
                    {"scope": self._scope_id, "key": key},
                )
            ).one()
        return str(existing.id), False

    async def list_proposals(self, *, status: str | None = None) -> list[MemoryProposal]:
        query = "SELECT * FROM memory_proposals WHERE scope_id = :scope"
        params: dict[str, str] = {"scope": self._scope_id}
        if status is not None:
            query += " AND status = :status"
            params["status"] = status
        query += " ORDER BY created_at"
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (await conn.execute(text(query), params)).mappings().all()
        return [_to_proposal(r) for r in rows]

    async def get(self, proposal_id: str) -> MemoryProposal | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM memory_proposals WHERE scope_id = :scope AND id = :id"),
                        {"scope": self._scope_id, "id": proposal_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_proposal(row)

    async def approve(self, proposal_id: str, resolved_by: str) -> ProposalResolution:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            proposal = (
                await conn.execute(
                    text(
                        "SELECT block, expected_version, proposed_value, status "
                        "FROM memory_proposals WHERE scope_id = :scope AND id = :id FOR UPDATE"
                    ),
                    {"scope": self._scope_id, "id": proposal_id},
                )
            ).one_or_none()
            if proposal is None:
                return ProposalResolution(ProposalOutcome.not_found)
            if proposal.status != "pending":
                return ProposalResolution(ProposalOutcome.already_resolved)

            block = str(proposal.block)
            expected = int(proposal.expected_version)
            value = str(proposal.proposed_value)
            if expected == 0:
                applied = (
                    await conn.execute(
                        text(
                            "INSERT INTO memory_blocks (scope_id, key, value, version) "
                            "VALUES (:scope, :block, :value, 1) "
                            "ON CONFLICT (scope_id, key) DO NOTHING"
                        ),
                        {"scope": self._scope_id, "block": block, "value": value},
                    )
                ).rowcount == 1
                new_version = 1
            else:
                applied = (
                    await conn.execute(
                        text(
                            "UPDATE memory_blocks "
                            "SET value = :value, version = version + 1, updated_at = now() "
                            "WHERE scope_id = :scope AND key = :block AND version = :expected"
                        ),
                        {
                            "value": value,
                            "scope": self._scope_id,
                            "block": block,
                            "expected": expected,
                        },
                    )
                ).rowcount == 1
                new_version = expected + 1

            if not applied:
                await conn.execute(
                    text(
                        "UPDATE memory_proposals SET status = 'stale', resolved_at = now(), "
                        "resolved_by = :by WHERE scope_id = :scope AND id = :id"
                    ),
                    {"by": resolved_by, "scope": self._scope_id, "id": proposal_id},
                )
                return ProposalResolution(ProposalOutcome.stale)

            await conn.execute(
                text(
                    "INSERT INTO memory_block_versions (scope_id, key, version, value) "
                    "VALUES (:scope, :block, :version, :value)"
                ),
                {
                    "scope": self._scope_id,
                    "block": block,
                    "version": new_version,
                    "value": value,
                },
            )
            await conn.execute(
                text(
                    "UPDATE memory_proposals SET status = 'applied', resolved_at = now(), "
                    "resolved_by = :by WHERE scope_id = :scope AND id = :id"
                ),
                {"by": resolved_by, "scope": self._scope_id, "id": proposal_id},
            )
        return ProposalResolution(ProposalOutcome.applied, new_version)

    async def reject(self, proposal_id: str, resolved_by: str) -> ProposalResolution:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE memory_proposals SET status = 'rejected', resolved_at = now(), "
                    "resolved_by = :by "
                    "WHERE scope_id = :scope AND id = :id AND status = 'pending'"
                ),
                {"by": resolved_by, "scope": self._scope_id, "id": proposal_id},
            )
            if result.rowcount == 1:
                return ProposalResolution(ProposalOutcome.rejected)
            exists = (
                await conn.execute(
                    text(
                        "SELECT status FROM memory_proposals WHERE scope_id = :scope AND id = :id"
                    ),
                    {"scope": self._scope_id, "id": proposal_id},
                )
            ).one_or_none()
        if exists is None:
            return ProposalResolution(ProposalOutcome.not_found)
        return ProposalResolution(ProposalOutcome.already_resolved)
