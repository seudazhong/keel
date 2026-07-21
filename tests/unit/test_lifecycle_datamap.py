"""The data map is the authoritative checklist: every store has a conscious retention +
erasure decision, and every *erasable* store is actually reached by the coordinator (M3.5).

These tests keep ``keel_core.lifecycle.datamap.DATA_MAP`` honest against the coordinator so
a newly-persisted store cannot be added without either wiring an erasure step or making an
explicit ``global_preserved`` / ``external`` decision.
"""

from __future__ import annotations

import re
from pathlib import Path

from lifecycle_helpers import SCOPE, FakePurge, RecordingRedis

from keel_core.lifecycle.coordinator import ErasureCoordinator, UnsupportedExternalStep
from keel_core.lifecycle.datamap import DATA_MAP, DATA_MAP_BY_NAME, ErasureTreatment
from keel_core.lifecycle.models import ErasureStatus, ErasureTarget, StepStatus
from keel_core.lifecycle.policies import DEFAULT_RETENTION
from keel_core.lifecycle.store import InMemoryErasureStore

# The coordinator step that erases each *erasable* store in the data map. Stores that share
# a purge (memory blocks + versions, the five Knowledge tables) collapse onto one step.
_ENTRY_TO_STEP = {
    "sessions": "events_and_sessions",
    "events": "events_and_sessions",
    "message_embeddings": "message_embeddings",
    "archival": "archival",
    "memory_blocks": "memory",
    "memory_block_versions": "memory",
    "memory_proposals": "memory_proposals",
    "consolidation_cursors": "consolidation_cursor",
    "knowledge_bases": "knowledge",
    "kb_documents": "knowledge",
    "kb_document_versions": "knowledge",
    "kb_chunks": "knowledge",
    "knowledge_idempotency": "knowledge",
    "connector_tokens": "connector_tokens",
    "connector_bindings": "connector_state",
    "connector_binding_targets": "connector_state",
    "connector_resources": "connector_state",
    "connector_items": "connector_state",
    "connector_cursors": "connector_state",
    "connector_deliveries": "connector_state",
    "connector_outbox": "connector_outbox",
    "connector_active_scopes": "connector_state",
    "connector_webhook_routes": "connector_state",
    "oauth_states": "oauth_states",
    "schedules": "schedules",
    "approvals": "approvals",
    "jobs": "jobs",
    "runs": "runs",
    "run_control": "runs",
    "run_dispatch_outbox": "runs",
    "job_dispatch_outbox": "jobs",
    "im_reply_intents": "im_routing",
    "im_route_index": "im_routing",
    "im_reply_dispatch_index": "im_routing",
    "coding_artifacts": "coding_artifacts",
    "tool_spill": "tool_spill",
    "redis_event_streams": "redis_streams",
}

_ERASABLE = {
    ErasureTreatment.scope_bound,
    ErasureTreatment.session_scoped,
    ErasureTreatment.project_scoped,
}


def test_every_data_map_entry_has_a_resolvable_retention_policy() -> None:
    for entry in DATA_MAP:
        assert entry.resource_class in DEFAULT_RETENTION, entry.name
        # The convenience property must resolve (would KeyError on an unknown class).
        assert entry.retention.resource_class == entry.resource_class


def test_every_migration_table_is_classified_exactly_once() -> None:
    migration_root = Path(__file__).parents[2] / "migrations" / "versions"
    create_table = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\"`]?"
        r"([a-zA-Z_][a-zA-Z0-9_]*)",
        re.IGNORECASE,
    )
    migration_tables: set[str] = set()
    for migration in migration_root.glob("*.py"):
        migration_tables.update(create_table.findall(migration.read_text(encoding="utf-8-sig")))

    mapped_tables = [entry.name for entry in DATA_MAP if entry.kind == "table"]
    assert len(mapped_tables) == len(set(mapped_tables)), "duplicate data-map table"
    assert set(mapped_tables) == migration_tables


def test_data_map_treatments_partition_erasable_from_preserved() -> None:
    for entry in DATA_MAP:
        if entry.treatment in _ERASABLE:
            assert entry.name in _ENTRY_TO_STEP, entry.name
        else:
            # global_preserved / external stores must never map to a deletion step.
            assert entry.name not in _ENTRY_TO_STEP, entry.name
    # Every mapped entry is a real data-map store (no stale mapping entries).
    assert set(_ENTRY_TO_STEP) <= set(DATA_MAP_BY_NAME)


async def test_scope_erasure_reaches_every_erasable_store_in_the_data_map() -> None:
    store = InMemoryErasureStore(SCOPE)
    coord = ErasureCoordinator(
        None,
        store,
        purge=FakePurge(sessions=("s1",), spill=("/spill/a.txt",)),  # type: ignore[arg-type]
        redis_cleaner=RecordingRedis(),  # type: ignore[arg-type]
        external_steps=(UnsupportedExternalStep("provider_telemetry"),),
    )
    request = await coord.submit(ErasureTarget(SCOPE), "map-key")
    result = await coord.execute(request.id)

    # 'partial' only because of the honest external step; all data steps run.
    assert result.status is ErasureStatus.partial
    executed = {step.step for step in result.steps}

    for entry in DATA_MAP:
        if entry.treatment in _ERASABLE:
            assert _ENTRY_TO_STEP[entry.name] in executed, entry.name
        elif entry.treatment is ErasureTreatment.external:
            # The external store surfaces as an unsupported (incomplete) step, never a delete.
            assert entry.name in executed
            outcome = next(s for s in result.steps if s.step == entry.name)
            assert outcome.status is StepStatus.unsupported
        else:
            # global_preserved stores are never touched by scope erasure.
            assert entry.name not in executed, entry.name
