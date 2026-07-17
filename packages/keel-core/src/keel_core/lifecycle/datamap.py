"""The authoritative, documented data map (M3.5, WS-K).

Every persisted store Keel writes is enumerated here with its retention class and how
erasure treats it. This is the single source of truth behind ``docs/DATA-LIFECYCLE.md``
and a checklist the erasure coordinator's step list is validated against in tests, so a
new store cannot be added without a conscious retention + erasure decision.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from keel_core.lifecycle import policies
from keel_core.lifecycle.policies import DEFAULT_RETENTION, RetentionPolicy


class ErasureTreatment(StrEnum):
    """How scope / session / project erasure treats a store."""

    scope_bound = "scope_bound"  # erased on scope erasure (scope-partitioned rows)
    session_scoped = "session_scoped"  # also erasable per-session (carries session_id)
    project_scoped = "project_scoped"  # erased on project erasure (on-disk, by project id)
    global_preserved = "global_preserved"  # shared/global config — deliberately preserved
    external = "external"  # lives in an external system — best-effort, may be partial


@dataclass(frozen=True)
class DataMapEntry:
    """One persisted store: where it lives, its retention, and its erasure treatment."""

    name: str
    kind: str  # 'table', 'redis', 'filesystem', 'external'
    resource_class: str
    scope_column: str | None
    treatment: ErasureTreatment
    notes: str

    @property
    def retention(self) -> RetentionPolicy:
        return DEFAULT_RETENTION[self.resource_class]


DATA_MAP: tuple[DataMapEntry, ...] = (
    # --- Event-sourced core + projections -------------------------------------------
    DataMapEntry(
        "sessions",
        "table",
        policies.SESSION,
        "scope_id",
        ErasureTreatment.session_scoped,
        "Session index; erased with its events.",
    ),
    DataMapEntry(
        "events",
        "table",
        policies.EVENT,
        "scope_id",
        ErasureTreatment.session_scoped,
        "Append-only log; a session tombstone is written before deletion so a rebuild "
        "cannot resurrect erased events.",
    ),
    DataMapEntry(
        "message_embeddings",
        "table",
        policies.MESSAGE_EMBEDDING,
        "scope_id",
        ErasureTreatment.session_scoped,
        "Semantic-recall projection; cascades from events but purged explicitly first.",
    ),
    DataMapEntry(
        "archival",
        "table",
        policies.ARCHIVAL,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Archival memory passages + embeddings.",
    ),
    # --- Editable memory + consolidation --------------------------------------------
    DataMapEntry(
        "memory_blocks",
        "table",
        policies.MEMORY_BLOCK,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Core memory blocks (persona/human).",
    ),
    DataMapEntry(
        "memory_block_versions",
        "table",
        policies.MEMORY_BLOCK,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Core memory version history.",
    ),
    DataMapEntry(
        "memory_proposals",
        "table",
        policies.MEMORY_PROPOSAL,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Pending/resolved core-memory rewrite proposals.",
    ),
    DataMapEntry(
        "consolidation_cursors",
        "table",
        policies.CONSOLIDATION_CURSOR,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Per-scope consolidation cursor + lease.",
    ),
    # --- Knowledge base --------------------------------------------------------------
    DataMapEntry(
        "knowledge_bases",
        "table",
        policies.KNOWLEDGE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "KB collections.",
    ),
    DataMapEntry(
        "kb_documents",
        "table",
        policies.KNOWLEDGE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "KB documents.",
    ),
    DataMapEntry(
        "kb_document_versions",
        "table",
        policies.KNOWLEDGE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "KB document versions.",
    ),
    DataMapEntry(
        "kb_chunks",
        "table",
        policies.KNOWLEDGE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "KB chunks + embeddings.",
    ),
    DataMapEntry(
        "knowledge_idempotency",
        "table",
        policies.KNOWLEDGE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "KB operation idempotency ledger.",
    ),
    # --- Connectors + cloud safety ---------------------------------------------------
    DataMapEntry(
        "connector_tokens",
        "table",
        policies.CONNECTOR_TOKEN,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Envelope-encrypted OAuth tokens; revoked + purged.",
    ),
    DataMapEntry(
        "connector_outbox",
        "table",
        policies.CONNECTOR_OUTBOX,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Outbound at-most-once idempotency claims.",
    ),
    DataMapEntry(
        "oauth_states",
        "table",
        policies.OAUTH_STATE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "One-time CSRF states; keyed by the random state but carries scope_id.",
    ),
    DataMapEntry(
        "webhook_deliveries",
        "table",
        policies.WEBHOOK_DELIVERY,
        None,
        ErasureTreatment.global_preserved,
        "GLOBAL replay-dedup ledger (no scope_id, no personal content) — preserved on "
        "scope erasure; TTL-swept and purgeable globally only.",
    ),
    # --- Autonomy + durable jobs -----------------------------------------------------
    DataMapEntry(
        "schedules",
        "table",
        policies.SCHEDULE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Persistent schedules.",
    ),
    DataMapEntry(
        "approvals",
        "table",
        policies.APPROVAL,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Durable tool approvals.",
    ),
    DataMapEntry(
        "jobs",
        "table",
        policies.JOB,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Durable background jobs/results; the running erasure job's own row is kept.",
    ),
    # --- Filesystem ------------------------------------------------------------------
    DataMapEntry(
        "coding_artifacts",
        "filesystem",
        policies.CODING_ARTIFACT,
        None,
        ErasureTreatment.project_scoped,
        "Managed coding project repo/snapshots/worktrees/artifacts; erased by project id.",
    ),
    DataMapEntry(
        "tool_spill",
        "filesystem",
        policies.TOOL_SPILL,
        None,
        ErasureTreatment.session_scoped,
        "Bounded tool overflow files; deleted from a confined spill root by recorded path.",
    ),
    # --- Redis -----------------------------------------------------------------------
    DataMapEntry(
        "redis_event_streams",
        "redis",
        policies.EVENT,
        None,
        ErasureTreatment.session_scoped,
        "Per-session live event streams (events:{session_id}); deleted by session id.",
    ),
    # --- Lifecycle ledger (audit; preserved) -----------------------------------------
    DataMapEntry(
        "event_tombstones",
        "table",
        policies.ERASURE_LEDGER,
        "scope_id",
        ErasureTreatment.global_preserved,
        "Anti-resurrection markers — intentionally retained after erasure.",
    ),
    DataMapEntry(
        "erasure_requests",
        "table",
        policies.ERASURE_LEDGER,
        "scope_id",
        ErasureTreatment.global_preserved,
        "Erasure audit trail — retained.",
    ),
    DataMapEntry(
        "erasure_steps",
        "table",
        policies.ERASURE_LEDGER,
        "scope_id",
        ErasureTreatment.global_preserved,
        "Erasure step ledger — retained.",
    ),
    DataMapEntry(
        "retention_policies",
        "table",
        policies.ERASURE_LEDGER,
        "scope_id",
        ErasureTreatment.global_preserved,
        "Per-scope retention overrides — retained.",
    ),
    # --- External systems ------------------------------------------------------------
    DataMapEntry(
        "provider_telemetry",
        "external",
        policies.EVENT,
        None,
        ErasureTreatment.external,
        "LLM provider logs / Langfuse traces — no delete API; recorded as an incomplete "
        "external step so the request finishes 'partial', never falsely 'completed'.",
    ),
)


DATA_MAP_BY_NAME: Mapping[str, DataMapEntry] = {entry.name: entry for entry in DATA_MAP}


__all__ = ["DATA_MAP", "DATA_MAP_BY_NAME", "DataMapEntry", "ErasureTreatment"]
