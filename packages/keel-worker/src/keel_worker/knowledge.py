"""Production durable-job definitions for Knowledge ingestion and deletion."""

from __future__ import annotations

from keel_core.config import Settings
from keel_core.embeddings import Embedder
from keel_core.jobs import CancelMode
from keel_core.knowledge.jobs import (
    KNOWLEDGE_DELETE_KIND,
    KNOWLEDGE_INGEST_KIND,
    KnowledgeJobHandlers,
)
from keel_core.knowledge.store import KnowledgeStore

from .jobs import JobDefinition, JobRegistry

_PG_INTEGER_MAX = 2_147_483_647


def knowledge_job_definitions(
    store: KnowledgeStore,
    embedder: Embedder,
    settings: Settings,
) -> tuple[JobDefinition, JobDefinition]:
    handlers = KnowledgeJobHandlers(store, embedder, settings)
    lease_seconds = settings.job_lease_seconds
    return (
        JobDefinition(
            kind=KNOWLEDGE_INGEST_KIND,
            handler=handlers.ingest,
            max_attempts=3,
            lease_seconds=lease_seconds,
            cancel_mode=CancelMode.cooperative,
            on_cancelled=handlers.ingest_cancelled,
            on_failed=handlers.ingest_failed,
        ),
        JobDefinition(
            kind=KNOWLEDGE_DELETE_KIND,
            handler=handlers.delete,
            max_attempts=_PG_INTEGER_MAX,
            lease_seconds=lease_seconds,
            cancel_mode=CancelMode.disabled,
            on_failed=handlers.delete_failed,
        ),
    )


def knowledge_job_registry(
    store: KnowledgeStore,
    embedder: Embedder,
    settings: Settings,
) -> JobRegistry:
    registry = JobRegistry()
    for definition in knowledge_job_definitions(store, embedder, settings):
        registry.register(definition)
    return registry


__all__ = ["knowledge_job_definitions", "knowledge_job_registry"]
