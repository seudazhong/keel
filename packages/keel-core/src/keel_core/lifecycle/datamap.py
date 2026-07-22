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
    org_scoped = "org_scoped"  # erased on organization erasure (org-partitioned identity)
    identity_global = "identity_global"  # global identity; erased on user (data-subject) erasure


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
        "Session index; erased with its events. Also carries R1B ownership/channel "
        "identity + visibility columns (owner_user_id/channel_provider/"
        "channel_external_id/visibility) — additive, erased with the row.",
    ),
    DataMapEntry(
        "session_access",
        "table",
        policies.SESSION,
        "scope_id",
        ErasureTreatment.session_scoped,
        "R1B explicit per-user session share edges; cascades from its session (composite "
        "FK to sessions(scope_id, id)) — erased by the same events_and_sessions step, "
        "never a separate one.",
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
        "connector_bindings",
        "table",
        policies.CONNECTOR_STATE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Non-secret connector account/tenant binding state.",
    ),
    DataMapEntry(
        "connector_binding_targets",
        "table",
        policies.CONNECTOR_STATE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Typed Knowledge destination and trigger session/routine targets.",
    ),
    DataMapEntry(
        "connector_resources",
        "table",
        policies.CONNECTOR_STATE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "User-selected provider root resources and non-secret configuration.",
    ),
    DataMapEntry(
        "connector_items",
        "table",
        policies.CONNECTOR_STATE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Provider-synced item mappings to durable destination records.",
    ),
    DataMapEntry(
        "connector_cursors",
        "table",
        policies.CONNECTOR_STATE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Provider delta/sync cursors.",
    ),
    DataMapEntry(
        "connector_deliveries",
        "table",
        policies.CONNECTOR_DELIVERY,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Scoped provider webhook delivery replay and processing ledger.",
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
        "effects",
        "table",
        policies.EFFECT,
        "scope_id",
        ErasureTreatment.scope_bound,
        "R1B durable Effect ledger (C4/C5): reserved/executing/confirmed/unknown/"
        "reconciled_*/failed outbound-mutation records. Purged with its scope "
        "(keel_core.effect_store.purge_scope); its reconciliation-outbox pointer "
        "cascades away with it (composite FK).",
    ),
    DataMapEntry(
        "effect_reconciliation_outbox",
        "table",
        policies.EFFECT_RECONCILIATION_OUTBOX,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Global (non-RLS) cross-scope reconciliation dispatch pointer; carries no "
        "action args/result, only routing keys. Cascades from its Effect on erasure.",
    ),
    DataMapEntry(
        "connector_active_scopes",
        "table",
        policies.CONNECTOR_STATE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Global non-content schedule index; connector scope purge removes the routing row.",
    ),
    DataMapEntry(
        "connector_webhook_routes",
        "table",
        policies.CONNECTOR_STATE,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Global opaque webhook route index; purged explicitly and cascades from its binding.",
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
    DataMapEntry(
        "runs",
        "table",
        policies.RUN,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Durable worker-owned interactive runs (state machine + lease + cost summary).",
    ),
    DataMapEntry(
        "run_control",
        "table",
        policies.RUN,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Durable interrupt/cancel/steering requests against a run.",
    ),
    DataMapEntry(
        "run_dispatch_outbox",
        "table",
        policies.RUN,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Global non-content run dispatch pointer; cascades when scope purge deletes its run.",
    ),
    DataMapEntry(
        "job_dispatch_outbox",
        "table",
        policies.JOB,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Global non-content job dispatch pointer; cascades when scope purge deletes its job.",
    ),
    # --- Durable IM (OneBot/Telegram) routing ----------------------------------------
    DataMapEntry(
        "im_reply_intents",
        "table",
        policies.RUN,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Durable encrypted IM reply outbox; erased on scope erasure (cascades reply dispatch).",
    ),
    DataMapEntry(
        "im_route_index",
        "table",
        policies.RUN,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Global opaque IM route index; deleted explicitly by the IM scope purge.",
    ),
    DataMapEntry(
        "im_reply_dispatch_index",
        "table",
        policies.RUN,
        "scope_id",
        ErasureTreatment.scope_bound,
        "Global non-content IM reply pointer; cascades from the scoped reply intent.",
    ),
    DataMapEntry(
        "im_channel_mappings",
        "table",
        policies.IDENTITY,
        "org_id",
        ErasureTreatment.org_scoped,
        "Org-owned IM channel mappings; erased on organization erasure (FK cascade).",
    ),
    # --- Managed projects and GitHub --------------------------------------------------
    DataMapEntry(
        "projects",
        "table",
        policies.PROJECT_STATE,
        "org_id",
        ErasureTreatment.org_scoped,
        "Org-owned Project metadata; project purge and organization erasure cascade to children.",
    ),
    DataMapEntry(
        "project_worktrees",
        "table",
        policies.PROJECT_STATE,
        "org_id",
        ErasureTreatment.org_scoped,
        "Project worktree records; cascade from Project deletion or organization erasure.",
    ),
    DataMapEntry(
        "project_runs",
        "table",
        policies.PROJECT_STATE,
        "org_id",
        ErasureTreatment.org_scoped,
        "Project-to-run associations; cascade from Project deletion or organization erasure.",
    ),
    DataMapEntry(
        "repo_sync_ledger",
        "table",
        policies.PROJECT_STATE,
        "org_id",
        ErasureTreatment.org_scoped,
        "Project repository sync audit; cascade from Project deletion or organization erasure.",
    ),
    DataMapEntry(
        "project_quotas",
        "table",
        policies.PROJECT_STATE,
        "org_id",
        ErasureTreatment.org_scoped,
        "Per-organization Project storage and concurrency quotas.",
    ),
    DataMapEntry(
        "github_installations",
        "table",
        policies.PROJECT_STATE,
        "org_id",
        ErasureTreatment.org_scoped,
        "GitHub App installation-to-organization binding; erased on organization erasure.",
    ),
    DataMapEntry(
        "github_repositories",
        "table",
        policies.PROJECT_STATE,
        "org_id",
        ErasureTreatment.org_scoped,
        "Organization-visible GitHub repository catalog; cascades from its installation.",
    ),
    DataMapEntry(
        "github_sync_state",
        "table",
        policies.PROJECT_STATE,
        "org_id",
        ErasureTreatment.org_scoped,
        "Per-repository durable sync cursor; explicitly removed by organization erasure.",
    ),
    DataMapEntry(
        "github_webhook_deliveries",
        "table",
        policies.WEBHOOK_DELIVERY,
        None,
        ErasureTreatment.global_preserved,
        "Global GitHub delivery replay ledger with no user content; TTL-swept.",
    ),
    # --- Controlled patch proposals ---------------------------------------------------
    DataMapEntry(
        "patch_proposals",
        "table",
        policies.CODING_ARTIFACT,
        "org_id",
        ErasureTreatment.org_scoped,
        "Org/Project-owned immutable patch proposal metadata; cascades on Project/org erasure.",
    ),
    DataMapEntry(
        "patch_writeback_ledger",
        "table",
        policies.CODING_ARTIFACT,
        "org_id",
        ErasureTreatment.org_scoped,
        "Patch writeback audit; cascades from its proposal/Project on erasure.",
    ),
    DataMapEntry(
        "patch_proposal_outbox",
        "table",
        policies.CODING_ARTIFACT,
        "org_id",
        ErasureTreatment.org_scoped,
        "Global patch dispatch pointer; cascades from its proposal on Project/org erasure.",
    ),
    DataMapEntry(
        "patch_generation_requests",
        "table",
        policies.CODING_ARTIFACT,
        "org_id",
        ErasureTreatment.org_scoped,
        "Bounded tainted patch request payload; cascades from its proposal on Project/org erasure.",
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
    # --- Durable identity (M3.6): erased by the dedicated identity purge, NOT the ----
    # scope/session/project coordinator (identity is org-partitioned, not scope-bound).
    DataMapEntry(
        "organizations",
        "table",
        policies.IDENTITY,
        "id",
        ErasureTreatment.org_scoped,
        "Tenant root; erased on organization erasure (cascades to its identity rows).",
    ),
    DataMapEntry(
        "memberships",
        "table",
        policies.IDENTITY,
        "org_id",
        ErasureTreatment.org_scoped,
        "User<->org RBAC edges; erased on organization erasure (and on user erasure).",
    ),
    DataMapEntry(
        "agents",
        "table",
        policies.IDENTITY,
        "org_id",
        ErasureTreatment.org_scoped,
        "Persisted personal/team Agents; erased on organization erasure (and owner erasure).",
    ),
    DataMapEntry(
        "resource_grants",
        "table",
        policies.IDENTITY,
        "org_id",
        ErasureTreatment.org_scoped,
        "Explicit Agent resource grants; erased on organization erasure (and owner erasure).",
    ),
    DataMapEntry(
        "agent_access",
        "table",
        policies.IDENTITY,
        "org_id",
        ErasureTreatment.org_scoped,
        "R1B team-Agent discover/use/manage access edges; cascades on organization erasure "
        "(org_id FK), Agent deletion, grantor erasure, and user-principal erasure through the "
        "structurally checked principal_user_id FK; channel principals remain opaque.",
    ),
    DataMapEntry(
        "users",
        "table",
        policies.IDENTITY,
        None,
        ErasureTreatment.identity_global,
        "Global human identity; erased on user (data-subject) erasure — cascades to the "
        "user's OIDC links, owned Agents, memberships, and issued grants.",
    ),
    DataMapEntry(
        "oidc_identities",
        "table",
        policies.IDENTITY,
        None,
        ErasureTreatment.identity_global,
        "Global external OIDC subject links; erased on user (data-subject) erasure.",
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
