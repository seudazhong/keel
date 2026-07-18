"""Durable + in-memory ``patch_proposals`` repositories (WS-PP).

A thin :class:`PatchProposalStore` seam over the org-owned proposal record, with:

* :class:`InMemoryPatchProposalStore` — a faithful, dependency-free implementation for unit tests
  and the ``lite`` profile that enforces the same idempotency, optimistic-fence, legal-transition
  and branch-collision rules the schema does; and
* :class:`PostgresPatchProposalStore` — the durable implementation. Tenant-owned reads/writes set
  the ``app.org_id`` GUC so Postgres RLS + FORCE is engaged as defense-in-depth. Every mutating
  transition is a single fenced ``UPDATE`` (optimistic ``version`` guard + legal-edge check) so a
  concurrent or stale writer never wins a conflicting transition.

Validation, authorization, bundle materialization and audit live in the coordinator/service; this
repository is intentionally thin and truthful.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from .errors import PatchStateError, PatchValidationError
from .models import (
    PatchProposal,
    PatchStatus,
    TestStatus,
    ensure_transition,
)

_SET_ORG = text("SELECT set_config('app.org_id', :org, true)")

_COLS = (
    "id, org_id, project_id, run_id, run_attempt, agent_id, actor, source_ref, task_digest, "
    "base_ref, base_sha, head_sha, bundle_sha256, diff_sha256, changed_path_digest, changed_files, "
    "test_status, status, version, approval_id, remote_branch, pr_number, pr_url, pr_node_id, "
    "idempotency_key, fingerprint, cost_usd, error_kind, error_message, created_at, updated_at, "
    "ready_at, decided_at, written_at, expires_at"
)

# Columns a fenced transition may set (never id/org/version/created_at — those are structural).
_UPDATABLE = frozenset(
    {
        "base_sha",
        "head_sha",
        "bundle_sha256",
        "diff_sha256",
        "changed_path_digest",
        "changed_files",
        "test_status",
        "approval_id",
        "remote_branch",
        "pr_number",
        "pr_url",
        "pr_node_id",
        "cost_usd",
        "error_kind",
        "error_message",
        "ready_at",
        "decided_at",
        "written_at",
        "expires_at",
    }
)


def _now() -> datetime:
    return datetime.now(UTC)


class ProposalConflictError(PatchValidationError):
    """A durable uniqueness constraint (idempotency key / branch collision) was violated."""


class ProposalNotFoundInStore(PatchStateError):
    """The proposal to transition does not exist in this org (fail closed)."""


class StaleProposalVersion(PatchStateError):
    """An optimistic-fenced transition cited a stale ``version`` (a concurrent writer won)."""


@runtime_checkable
class PatchProposalStore(Protocol):
    """Durable seam for controlled patch-proposal persistence."""

    async def create(
        self,
        *,
        proposal_id: str,
        org_id: str,
        project_id: str,
        run_id: str,
        run_attempt: int,
        agent_id: str,
        actor: str,
        base_ref: str,
        source_ref: str,
        task_digest: str,
        idempotency_key: str,
        fingerprint: str,
        expires_at: datetime,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, bool]: ...

    async def get(self, org_id: str, proposal_id: str) -> PatchProposal | None: ...

    async def get_by_run(self, org_id: str, run_id: str) -> PatchProposal | None: ...

    async def list_for_project(
        self, org_id: str, project_id: str, *, limit: int = 100
    ) -> list[PatchProposal]: ...

    async def transition(
        self,
        org_id: str,
        proposal_id: str,
        target: PatchStatus,
        *,
        expected_version: int | None = None,
        updates: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> PatchProposal: ...

    async def expire_due(self, now: datetime, limit: int) -> list[tuple[str, str]]: ...


def _validate_updates(updates: Mapping[str, Any] | None) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in (updates or {}).items():
        if key not in _UPDATABLE:
            raise PatchValidationError(f"column '{key}' is not a legal fenced update")
        if key == "test_status" and isinstance(value, TestStatus):
            value = value.value
        clean[key] = value
    return clean


_STATUS_TIMESTAMP = {
    PatchStatus.ready: "ready_at",
    PatchStatus.approved: "decided_at",
    PatchStatus.denied: "decided_at",
    PatchStatus.draft_pr_created: "written_at",
}


# --- In-memory -----------------------------------------------------------------------


class InMemoryPatchProposalStore:
    """Dependency-free store enforcing the same invariants as the schema (tests / lite)."""

    def __init__(self) -> None:
        self._rows: dict[str, PatchProposal] = {}
        self._by_idem: dict[tuple[str, str], str] = {}

    async def create(
        self,
        *,
        proposal_id: str,
        org_id: str,
        project_id: str,
        run_id: str,
        run_attempt: int,
        agent_id: str,
        actor: str,
        base_ref: str,
        source_ref: str,
        task_digest: str,
        idempotency_key: str,
        fingerprint: str,
        expires_at: datetime,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, bool]:
        created_at = now or _now()
        key = (org_id, idempotency_key)
        existing_id = self._by_idem.get(key)
        if existing_id is not None:
            return self._rows[existing_id], False
        proposal = PatchProposal(
            id=proposal_id,
            org_id=org_id,
            project_id=project_id,
            run_id=run_id,
            run_attempt=run_attempt,
            agent_id=agent_id,
            actor=actor,
            source_ref=source_ref,
            task_digest=task_digest,
            base_ref=base_ref,
            base_sha="",
            head_sha="",
            bundle_sha256="",
            diff_sha256="",
            changed_path_digest="",
            changed_files=0,
            test_status=TestStatus.unknown,
            status=PatchStatus.generating,
            version=1,
            idempotency_key=idempotency_key,
            fingerprint=fingerprint,
            expires_at=expires_at,
            created_at=created_at,
            updated_at=created_at,
        )
        self._rows[proposal_id] = proposal
        self._by_idem[key] = proposal_id
        return proposal, True

    async def get(self, org_id: str, proposal_id: str) -> PatchProposal | None:
        row = self._rows.get(proposal_id)
        return row if row is not None and row.org_id == org_id else None

    async def get_by_run(self, org_id: str, run_id: str) -> PatchProposal | None:
        for row in self._rows.values():
            if row.org_id == org_id and row.run_id == run_id:
                return row
        return None

    async def list_for_project(
        self, org_id: str, project_id: str, *, limit: int = 100
    ) -> list[PatchProposal]:
        rows = [r for r in self._rows.values() if r.org_id == org_id and r.project_id == project_id]
        rows.sort(key=lambda r: r.created_at, reverse=True)
        return rows[:limit]

    async def transition(
        self,
        org_id: str,
        proposal_id: str,
        target: PatchStatus,
        *,
        expected_version: int | None = None,
        updates: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> PatchProposal:
        clean = _validate_updates(updates)
        row = self._rows.get(proposal_id)
        if row is None or row.org_id != org_id:
            raise ProposalNotFoundInStore(f"proposal not found: {proposal_id}")
        if expected_version is not None and row.version != expected_version:
            raise StaleProposalVersion(
                f"stale proposal version {expected_version} (current {row.version})"
            )
        moment = now or _now()
        # Idempotent re-apply of the same terminal/decision state: no version bump.
        if row.status == target:
            updated = replace(row, updated_at=moment, **_coerce(clean))
            self._rows[proposal_id] = updated
            return updated
        ensure_transition(row.status, target)
        # Branch-collision guard mirrors the partial unique index (live branch reservation).
        new_branch = clean.get("remote_branch", row.remote_branch)
        live = {PatchStatus.approved, PatchStatus.writing, PatchStatus.draft_pr_created}
        if new_branch and target in live:
            for other in self._rows.values():
                if (
                    other.id != proposal_id
                    and other.org_id == org_id
                    and other.project_id == row.project_id
                    and other.remote_branch == new_branch
                    and other.status in live
                ):
                    raise ProposalConflictError("remote branch already reserved for this project")
        fields: dict[str, Any] = dict(_coerce(clean))
        ts_col = _STATUS_TIMESTAMP.get(target)
        if ts_col is not None and ts_col not in fields:
            fields[ts_col] = moment
        updated = replace(row, status=target, version=row.version + 1, updated_at=moment, **fields)
        self._rows[proposal_id] = updated
        return updated

    async def expire_due(self, now: datetime, limit: int) -> list[tuple[str, str]]:
        due = [
            (r.org_id, r.id)
            for r in self._rows.values()
            if r.expires_at <= now
            and r.status
            in {
                PatchStatus.generating,
                PatchStatus.ready,
                PatchStatus.approval_pending,
                PatchStatus.approved,
                PatchStatus.writing,
            }
        ]
        return due[:limit]


def _coerce(clean: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in clean.items():
        if key == "test_status" and isinstance(value, str):
            out[key] = TestStatus(value)
        else:
            out[key] = value
    return out


# --- Postgres ------------------------------------------------------------------------


def _to_proposal(row: Mapping[Any, Any]) -> PatchProposal:
    return PatchProposal(
        id=row["id"],
        org_id=row["org_id"],
        project_id=row["project_id"],
        run_id=row["run_id"],
        run_attempt=row["run_attempt"],
        agent_id=row["agent_id"],
        actor=row["actor"],
        source_ref=row["source_ref"],
        task_digest=row["task_digest"],
        base_ref=row["base_ref"],
        base_sha=row["base_sha"],
        head_sha=row["head_sha"],
        bundle_sha256=row["bundle_sha256"],
        diff_sha256=row["diff_sha256"],
        changed_path_digest=row["changed_path_digest"],
        changed_files=row["changed_files"],
        test_status=TestStatus(row["test_status"]),
        status=PatchStatus(row["status"]),
        version=row["version"],
        approval_id=row["approval_id"],
        remote_branch=row["remote_branch"],
        pr_number=row["pr_number"],
        pr_url=row["pr_url"],
        pr_node_id=row["pr_node_id"],
        idempotency_key=row["idempotency_key"],
        fingerprint=row["fingerprint"],
        cost_usd=float(row["cost_usd"]),
        error_kind=row["error_kind"],
        error_message=row["error_message"],
        expires_at=row["expires_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        ready_at=row["ready_at"],
        decided_at=row["decided_at"],
        written_at=row["written_at"],
    )


class PostgresPatchProposalStore:
    """Durable proposal store; tenant-owned access sets ``app.org_id`` (RLS + FORCE)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def create(
        self,
        *,
        proposal_id: str,
        org_id: str,
        project_id: str,
        run_id: str,
        run_attempt: int,
        agent_id: str,
        actor: str,
        base_ref: str,
        source_ref: str,
        task_digest: str,
        idempotency_key: str,
        fingerprint: str,
        expires_at: datetime,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, bool]:
        created_at = now or _now()
        params = {
            "id": proposal_id,
            "org": org_id,
            "pid": project_id,
            "rid": run_id,
            "attempt": run_attempt,
            "agent": agent_id,
            "actor": actor,
            "base_ref": base_ref,
            "source_ref": source_ref,
            "task_digest": task_digest,
            "idem": idempotency_key,
            "fp": fingerprint,
            "expires": expires_at,
            "now": created_at,
        }
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO patch_proposals "
                                "(id, org_id, project_id, run_id, run_attempt, agent_id, actor, "
                                " source_ref, task_digest, base_ref, idempotency_key, fingerprint, "
                                " created_at, updated_at, expires_at) "
                                "VALUES (:id, :org, :pid, :rid, :attempt, :agent, :actor, "
                                " :source_ref, :task_digest, :base_ref, :idem, :fp, "
                                " :now, :now, :expires) "
                                "ON CONFLICT (org_id, idempotency_key) DO NOTHING "
                                f"RETURNING {_COLS}"
                            ),
                            params,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is not None:
                    return _to_proposal(row), True
                existing = (
                    (
                        await conn.execute(
                            text(
                                f"SELECT {_COLS} FROM patch_proposals "
                                "WHERE org_id = :org AND idempotency_key = :idem"
                            ),
                            {"org": org_id, "idem": idempotency_key},
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:  # composite FK to projects(id, org_id) violated
            raise ProposalConflictError("proposal binding is invalid for this org/project") from exc
        return _to_proposal(existing), False

    async def get(self, org_id: str, proposal_id: str) -> PatchProposal | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLS} FROM patch_proposals WHERE id = :id AND org_id = :org"
                        ),
                        {"id": proposal_id, "org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_proposal(row)

    async def get_by_run(self, org_id: str, run_id: str) -> PatchProposal | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLS} FROM patch_proposals "
                            "WHERE run_id = :rid AND org_id = :org "
                            "ORDER BY created_at DESC LIMIT 1"
                        ),
                        {"rid": run_id, "org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_proposal(row)

    async def list_for_project(
        self, org_id: str, project_id: str, *, limit: int = 100
    ) -> list[PatchProposal]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLS} FROM patch_proposals "
                            "WHERE project_id = :pid AND org_id = :org "
                            "ORDER BY created_at DESC LIMIT :lim"
                        ),
                        {"pid": project_id, "org": org_id, "lim": limit},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_proposal(r) for r in rows]

    async def transition(
        self,
        org_id: str,
        proposal_id: str,
        target: PatchStatus,
        *,
        expected_version: int | None = None,
        updates: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> PatchProposal:
        clean = _validate_updates(updates)
        moment = now or _now()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                current = (
                    (
                        await conn.execute(
                            text(
                                f"SELECT {_COLS} FROM patch_proposals "
                                "WHERE id = :id AND org_id = :org FOR UPDATE"
                            ),
                            {"id": proposal_id, "org": org_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if current is None:
                    raise ProposalNotFoundInStore(f"proposal not found: {proposal_id}")
                proposal = _to_proposal(current)
                if expected_version is not None and proposal.version != expected_version:
                    raise StaleProposalVersion(
                        f"stale proposal version {expected_version} (current {proposal.version})"
                    )
                assignments: dict[str, Any] = {}
                for key, value in clean.items():
                    assignments[key] = value
                bump_version = proposal.status != target
                if bump_version:
                    ensure_transition(proposal.status, target)
                    ts_col = _STATUS_TIMESTAMP.get(target)
                    if ts_col is not None and ts_col not in assignments:
                        assignments[ts_col] = moment
                set_parts = ["status = :__status", "updated_at = :__now"]
                bind: dict[str, Any] = {
                    "__status": target.value,
                    "__now": moment,
                    "id": proposal_id,
                    "org": org_id,
                }
                if bump_version:
                    set_parts.append("version = version + 1")
                for i, (key, value) in enumerate(assignments.items()):
                    param = f"__u{i}"
                    set_parts.append(f"{key} = :{param}")
                    bind[param] = value
                row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE patch_proposals SET "
                                + ", ".join(set_parts)
                                + f" WHERE id = :id AND org_id = :org RETURNING {_COLS}"
                            ),
                            bind,
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:  # branch-collision partial unique index
            raise ProposalConflictError("remote branch already reserved for this project") from exc
        return _to_proposal(row)

    async def expire_due(self, now: datetime, limit: int) -> list[tuple[str, str]]:
        """Cross-org scan for proposals past their TTL (maintenance reconciler path).

        RLS is disabled for the scan (the maintenance role owns the table); only ``(org_id, id)``
        pointers are returned so the caller re-enters each org's RLS context to expire them.
        """
        async with self._engine.begin() as conn:
            await conn.execute(text("SET LOCAL row_security = off"))
            rows = (
                await conn.execute(
                    text(
                        "SELECT org_id, id FROM patch_proposals "
                        "WHERE expires_at <= :now AND status IN "
                        "('generating','ready','approval_pending','approved','writing') "
                        "ORDER BY expires_at LIMIT :lim"
                    ),
                    {"now": now, "lim": limit},
                )
            ).all()
        return [(r.org_id, r.id) for r in rows]


__all__ = [
    "InMemoryPatchProposalStore",
    "PatchProposalStore",
    "PostgresPatchProposalStore",
    "ProposalConflictError",
    "ProposalNotFoundInStore",
    "StaleProposalVersion",
]
