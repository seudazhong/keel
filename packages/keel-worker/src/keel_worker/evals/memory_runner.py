"""Case executors that drive the real production memory chain against the eval DB.

The consolidation executor seeds core memory + a message batch + a cursor into an
isolated eval scope, then calls the production ``consolidate_memory`` **directly**
(not ``run_agent``) with a per-case ``Settings`` so the model id and
``consolidation_min_messages`` can be overridden without the ``get_settings``
lru_cache. ``consolidate_memory`` reads its engine only from ``ctx['engine']``, so
a host's live DB setting cannot bypass the eval-DB guard. Because that function
swallows exceptions into an ``"error"`` status, the executor inspects the replay
gateways' captured ``.miss`` after the call and re-raises so a cassette miss
surfaces as an infra failure (exit 2) instead of a silent quality regression.

The recall executor seeds each ``RecallSession`` as its own scope-bound session
(``evalseed:<case>:<label>``) with deterministic eval event ids, then drives the
production ``hybrid_session_search`` / ``ArchivalStore.search`` chain with the
run's active embedder. For session hits the executor maps ``hit.source``
(``session:<id>``) back to the case-declared label; for archival hits it maps by
normalized content. When ``case.degrade_embeddings`` is set, a
:class:`FailingEmbedder` deterministically triggers the ``lexical-degraded``
path via the same production error handling — so eval graduation and
graceful-degradation live on the same code path production users hit.

The safety executor reuses the same consolidation chain (seed + direct
``consolidate_memory`` + ``raise_on_miss``) so assistant-only, invalid-citation
and prompt-injection scenarios are graded on the exact production tool
validators. It derives ``validation_error`` from the run status (``"error"``
means ``should_advance_cursor`` refused to advance because a tool recorded a
validation error — e.g. an out-of-batch citation); cassette/infra misses are
surfaced earlier by ``raise_on_miss`` so they never masquerade as a validation
error. For the ``core_version_conflict`` scenario, the executor bumps the
targeted block underneath the still-pending proposal and then calls
``MemoryProposalStore.approve`` so the production compare-and-set path
resolves ``stale`` — proving core edits made concurrent with a proposal are
never silently applied.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.consolidation.agent import MEMORY_CONSOLIDATOR_AGENT_ID
from keel_core.consolidation.proposals import MemoryProposalStore
from keel_core.memory import PostgresMemoryStore
from keel_core.recall import RecallMode
from keel_core.search import ArchivalStore, hybrid_session_search
from keel_scheduler.store import InMemoryScheduleStore, ScheduleRow
from keel_worker.evals.embeddings import FailingEmbedder
from keel_worker.evals.loader import case_event_base, cursor_seed_for, event_id_for
from keel_worker.evals.matching import normalize
from keel_worker.evals.models import (
    ArchivalRecord,
    ConsolidationActual,
    ConsolidationCase,
    ProposalRecord,
    RecallActual,
    RecallCase,
    RecallQueryResult,
    SafetyActual,
    SafetyCase,
)

EVAL_SESSION_PREFIX = "evalseed"  # not consolidation:/digest: so the reader includes it
_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


def _eval_session_id(case_id: str) -> str:
    return f"{EVAL_SESSION_PREFIX}:{case_id}"


def _json_payload(role: str, textval: str) -> str:
    return json.dumps({"role": role, "text": textval})


async def seed_core(engine: AsyncEngine, scope: str, blocks: dict[str, str]) -> None:
    store = PostgresMemoryStore(engine, scope)
    for key, value in blocks.items():
        await store.set(key, value)


async def seed_messages(engine: AsyncEngine, scope: str, case_id: str, messages: list[Any]) -> int:
    """Insert one ``message.token`` event per message with a deterministic eval id."""
    session_id = _eval_session_id(case_id)
    now = datetime.now(UTC)
    max_event_id = cursor_seed_for(case_id)
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope})
        await conn.execute(
            text(
                "INSERT INTO sessions (id, scope_id, title, next_seq) "
                "VALUES (:id, :scope, :title, :next_seq) "
                "ON CONFLICT (scope_id, id) DO UPDATE SET next_seq = EXCLUDED.next_seq"
            ),
            {"id": session_id, "scope": scope, "title": case_id, "next_seq": len(messages) + 1},
        )
        for index, message in enumerate(messages):
            event_id = event_id_for(case_id, index)
            max_event_id = max(max_event_id, event_id)
            await conn.execute(
                text(
                    "INSERT INTO events "
                    "(id, session_id, scope_id, seq, type, version, ts, payload) "
                    "VALUES (:id, :sid, :scope, :seq, 'message.token', 1, :ts, CAST(:p AS jsonb)) "
                    "ON CONFLICT (id) DO NOTHING"
                ),
                {
                    "id": event_id,
                    "sid": session_id,
                    "scope": scope,
                    "seq": index + 1,
                    "ts": now,
                    "p": _json_payload(message.role, message.text),
                },
            )
    return max_event_id


async def seed_cursor(engine: AsyncEngine, scope: str, last_event_id: int) -> None:
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope})
        await conn.execute(
            text(
                "INSERT INTO consolidation_cursors "
                "(scope_id, last_event_id, lease_token, lease_expires_at, last_status) "
                "VALUES (:scope, :last, NULL, NULL, NULL) "
                "ON CONFLICT (scope_id) DO UPDATE SET last_event_id = :last, "
                "lease_token = NULL, lease_expires_at = NULL, last_status = NULL"
            ),
            {"scope": scope, "last": last_event_id},
        )


async def read_cursor(engine: AsyncEngine, scope: str) -> int:
    async with engine.connect() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope})
        value = await conn.scalar(
            text("SELECT last_event_id FROM consolidation_cursors WHERE scope_id = :scope"),
            {"scope": scope},
        )
    return int(value) if value is not None else 0


async def read_proposals(engine: AsyncEngine, scope: str) -> list[ProposalRecord]:
    proposals = await MemoryProposalStore(engine, scope).list_proposals()
    return [
        ProposalRecord(
            block=p.block,
            proposed_value=p.proposed_value,
            source_event_ids=list(p.source_event_ids),
            created=p.status in ("pending", "applied"),
        )
        for p in proposals
    ]


async def read_archival(engine: AsyncEngine, scope: str) -> list[ArchivalRecord]:
    async with engine.connect() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope})
        rows = (
            await conn.execute(
                text(
                    "SELECT id, content, source_event_ids FROM archival "
                    "WHERE scope_id = :scope AND origin = 'consolidation' ORDER BY id"
                ),
                {"scope": scope},
            )
        ).all()
    return [
        ArchivalRecord(
            id=int(row.id),
            content=str(row.content),
            source_event_ids=[int(i) for i in (row.source_event_ids or [])],
        )
        for row in rows
    ]


def case_settings(case: ConsolidationCase) -> Settings:
    """Per-case settings: the case model + a min-messages of 1 so any batch is eligible."""
    return Settings(
        default_model=case.model,
        consolidation_min_messages=1,
        consolidation_batch_messages=max(1000, len(case.messages) + 1),
        consolidation_archival_min_confidence=0.5,
    )


def raise_on_miss(provider: Any, embedder: Any) -> None:
    """Surface a captured replay miss (provider swallowed the exception into 'error')."""
    provider_miss = getattr(provider, "miss", None)
    if provider_miss is not None:
        raise provider_miss
    embedder_miss = getattr(embedder, "miss", None)
    if embedder_miss is not None:
        raise embedder_miss
    if getattr(provider, "errored", False):
        raise RuntimeError("provider recording failed for case (no turn recorded)")


def _schedule_row(scope: str, case_id: str) -> ScheduleRow:
    return ScheduleRow(
        id=f"eval-schedule:{case_id}",
        scope_id=scope,
        # Must equal the production consolidator agent id so the schedule row is the
        # same shape run_agent() dispatches on (main.py checks row.agent_id).
        agent_id=MEMORY_CONSOLIDATOR_AGENT_ID,
        session_id=_eval_session_id(case_id),
        trigger_kind="interval",
        spec="",
        next_run_at=datetime.now(UTC),
        interval_s=86_400,
    )


async def _run_once(
    case: ConsolidationCase, *, engine: AsyncEngine, provider: Any, embedder: Any, scope: str
) -> str:
    from keel_worker.main import consolidate_memory

    row = _schedule_row(scope, case.id)
    ctx: dict[str, Any] = {
        "engine": engine,
        "provider": provider,
        "embedder": embedder,
        "schedules": InMemoryScheduleStore([row]),
    }
    status = await consolidate_memory(ctx, row, case_settings(case))
    raise_on_miss(provider, embedder)
    return status


def _preexisting_core(case: ConsolidationCase) -> dict[str, str]:
    # Seed any core blocks the case declares (e.g. an existing ``human`` block a
    # rewrite must preserve). Empty by default.
    return dict(case.preexisting_core)


async def run_consolidation_case(
    case: ConsolidationCase,
    *,
    engine: AsyncEngine,
    provider: Any,
    embedder: Any,
    dataset_version: str,
) -> ConsolidationActual:
    from keel_worker.evals.database import case_scope

    scope = case_scope(dataset_version, case.id)
    await seed_core(engine, scope, _preexisting_core(case))
    await seed_messages(engine, scope, case.id, case.messages)
    await seed_cursor(engine, scope, cursor_seed_for(case.id))

    status = await _run_once(case, engine=engine, provider=provider, embedder=embedder, scope=scope)
    proposals = await read_proposals(engine, scope)
    archival = await read_archival(engine, scope)
    cursor_advanced = await read_cursor(engine, scope) > cursor_seed_for(case.id)

    replay_created: int | None = None
    if case.expected.expect_idempotent_replay:
        before = {p.proposed_value for p in proposals} | {a.content for a in archival}
        await seed_cursor(engine, scope, cursor_seed_for(case.id))  # reprocess same batch
        await _run_once(case, engine=engine, provider=provider, embedder=embedder, scope=scope)
        after_proposals = await read_proposals(engine, scope)
        after_archival = await read_archival(engine, scope)
        after = {p.proposed_value for p in after_proposals} | {a.content for a in after_archival}
        replay_created = len(after - before)
        proposals, archival = after_proposals, after_archival

    return ConsolidationActual(
        status=status,
        cursor_advanced=cursor_advanced,
        proposals=proposals,
        archival=archival,
        replay_created_writes=replay_created,
    )


async def seed_recall_corpus(engine: AsyncEngine, scope: str, case: RecallCase) -> dict[str, str]:
    """Seed session messages (no pre-embedding — recall backfills at query time).

    Returns a ``session_id -> label`` map for resolving ``session:<id>`` hit sources.
    Archival passages are seeded by the caller with the run's active embedder.
    """
    session_label: dict[str, str] = {}
    now = datetime.now(UTC)
    event_id = case_event_base(case.id)
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope})
        for session in case.sessions:
            session_id = f"{EVAL_SESSION_PREFIX}:{case.id}:{session.label}"
            session_label[session_id] = session.label
            await conn.execute(
                text(
                    "INSERT INTO sessions (id, scope_id, title, next_seq) "
                    "VALUES (:id, :scope, :title, :next_seq) ON CONFLICT (scope_id, id) DO NOTHING"
                ),
                {
                    "id": session_id,
                    "scope": scope,
                    "title": session.label,
                    "next_seq": len(session.messages) + 1,
                },
            )
            for m_index, message in enumerate(session.messages):
                await conn.execute(
                    text(
                        "INSERT INTO events "
                        "(id, session_id, scope_id, seq, type, version, ts, payload) "
                        "VALUES (:id, :sid, :scope, :seq, 'message.token', 1, :ts, "
                        "CAST(:p AS jsonb)) ON CONFLICT (id) DO NOTHING"
                    ),
                    {
                        "id": event_id,
                        "sid": session_id,
                        "scope": scope,
                        "seq": m_index + 1,
                        "ts": now,
                        "p": _json_payload(message.role, message.text),
                    },
                )
                event_id += 1
    return session_label


async def run_recall_case(
    case: RecallCase, *, engine: AsyncEngine, embedder: Any, dataset_version: str
) -> RecallActual:
    from keel_worker.evals.database import case_scope

    scope = case_scope(dataset_version, case.id)
    active = FailingEmbedder() if case.degrade_embeddings else embedder

    session_label = await seed_recall_corpus(engine, scope, case)
    content_label: dict[str, str] = {}
    for archival in case.archival:
        await ArchivalStore(engine, scope, active).add(archival.content)
        content_label[normalize(archival.content)] = archival.label

    results: list[RecallQueryResult] = []
    for query in case.queries:
        if query.mode == "session":
            hits, status = await hybrid_session_search(
                engine, scope, query.query, k=query.k, embedder=active
            )
            labels = [
                session_label[hit.source.removeprefix("session:")]
                for hit in hits
                if hit.source.removeprefix("session:") in session_label
            ]
            mode: RecallMode = status.mode
        else:
            hits = await ArchivalStore(engine, scope, active).search(query.query, k=query.k)
            labels = [
                content_label[normalize(hit.content)]
                for hit in hits
                if normalize(hit.content) in content_label
            ]
            mode = "hybrid"
        results.append(
            RecallQueryResult(
                query=query.query, mode=query.mode, hit_labels=labels, recall_mode=mode
            )
        )

    if not case.degrade_embeddings:
        raise_on_miss(None, active)  # a missing recorded query embedding is an infra failure
    return RecallActual(results=results)


def safety_settings(case: SafetyCase) -> Settings:
    """Per-case settings: force one big batch so every scripted message is eligible."""
    return Settings(
        default_model=case.model,
        consolidation_min_messages=1,
        consolidation_batch_messages=max(1000, len(case.messages) + 1),
        consolidation_archival_min_confidence=0.5,
    )


async def run_safety_case(
    case: SafetyCase,
    *,
    engine: AsyncEngine,
    provider: Any,
    embedder: Any,
    dataset_version: str,
) -> SafetyActual:
    """Drive the production consolidation chain for a safety case + optional apply.

    The scripted turn is graded on the exact production tool validators (out-of-batch
    citations, missing user evidence, etc.). For ``core_version_conflict`` the block
    is bumped underneath the still-pending proposal before ``approve`` is called, so
    the production compare-and-set path resolves ``stale`` — proving a concurrent
    core edit is never silently overwritten.

    For ``invalid_citation`` the provider is wrapped with
    :class:`~keel_worker.evals.providers.InvalidCitationWrapper` to deterministically
    inject ``source_event_ids = [12345]`` on every tool call.  This exercises the
    production out-of-batch citation validator regardless of what the model proposes.
    The wrapper mutates the ``ProviderChunk`` in place so the cassette records the
    injected id and replay re-applies the same idempotent injection.
    """
    from keel_worker.evals.database import case_scope
    from keel_worker.evals.providers import InvalidCitationWrapper
    from keel_worker.main import consolidate_memory

    if case.scenario == "invalid_citation":
        provider = InvalidCitationWrapper(provider)

    scope = case_scope(dataset_version, case.id)
    await seed_core(engine, scope, case.preexisting_core)
    await seed_messages(engine, scope, case.id, case.messages)
    await seed_cursor(engine, scope, cursor_seed_for(case.id))

    row = _schedule_row(scope, case.id)
    ctx: dict[str, Any] = {
        "engine": engine,
        "provider": provider,
        "embedder": embedder,
        "schedules": InMemoryScheduleStore([row]),
    }
    status = await consolidate_memory(ctx, row, safety_settings(case))
    raise_on_miss(provider, embedder)

    proposals = await read_proposals(engine, scope)
    archival = await read_archival(engine, scope)
    cursor_advanced = await read_cursor(engine, scope) > cursor_seed_for(case.id)

    # Force a version conflict: bump the block underneath the pending proposal.
    if case.simulate_core_edit_block is not None and case.simulate_core_edit_value is not None:
        await PostgresMemoryStore(engine, scope).set(
            case.simulate_core_edit_block, case.simulate_core_edit_value
        )

    apply_outcomes: list[str] = []
    if case.expect_proposal_stale_on_apply:
        store = MemoryProposalStore(engine, scope)
        for proposal in await store.list_proposals(status="pending"):
            resolution = await store.approve(proposal.id, "eval")
            apply_outcomes.append(resolution.outcome.value)

    # A scripted safety run only reaches "error" when the production chain recorded a
    # validation error (``should_advance_cursor`` stayed False) — e.g. an out-of-batch
    # citation — so the batch is deliberately not marked processed. Cassette/infra misses
    # are surfaced earlier by ``raise_on_miss``, so they never masquerade as a validation
    # error here.
    return SafetyActual(
        status=status,
        cursor_advanced=cursor_advanced,
        validation_error=status == "error",
        proposals=proposals,
        archival=archival,
        apply_outcomes=apply_outcomes,
    )
