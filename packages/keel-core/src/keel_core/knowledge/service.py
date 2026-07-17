"""Idempotent Knowledge Base orchestration over lifecycle and durable-job stores."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from keel_core.config import Settings
from keel_core.job_dispatch import JobDispatchOutbox
from keel_core.jobs import CancelMode, JobRecord, JobStore, JobValidationError

from .jobs import (
    KNOWLEDGE_DELETE_CANCEL_MODE,
    KNOWLEDGE_DELETE_KIND,
    KNOWLEDGE_DELETE_MAX_ATTEMPTS,
    KNOWLEDGE_INGEST_CANCEL_MODE,
    KNOWLEDGE_INGEST_KIND,
    KNOWLEDGE_INGEST_MAX_ATTEMPTS,
    KnowledgeDeletePayload,
    KnowledgeIngestPayload,
)
from .models import (
    CreateKnowledgeBaseCommand,
    CreateKnowledgeDocumentCommand,
    DeleteKnowledgeCommand,
    KnowledgeBaseCreate,
    KnowledgeBaseIdempotencyResult,
    KnowledgeBaseRecord,
    KnowledgeBaseTombstone,
    KnowledgeConflict,
    KnowledgeDocumentRecord,
    KnowledgeDocumentReindex,
    KnowledgeDocumentTombstone,
    KnowledgeDocumentVersionCreate,
    KnowledgeDocumentVersionIdempotencyResult,
    KnowledgeDocumentVersionRecord,
    KnowledgeHit,
    KnowledgeIdempotencyAttach,
    KnowledgeIdempotencyBegin,
    KnowledgeOperation,
    KnowledgePublicCode,
    KnowledgeSearchStatus,
    KnowledgeSourceType,
    KnowledgeStorageError,
    KnowledgeValidationError,
    ReindexKnowledgeDocumentCommand,
    UpdateKnowledgeDocumentCommand,
    canonical_request_fingerprint,
)
from .search import KnowledgeSearcher
from .store import KnowledgeStore

logger = logging.getLogger("keel.core.knowledge.service")

KNOWLEDGE_CHUNKING_VERSION = "keel-char-v1"

DispatchJob = Callable[[str, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentJobResult:
    document: KnowledgeDocumentRecord
    version: KnowledgeDocumentVersionRecord
    job: JobRecord
    replayed: bool


@dataclass(frozen=True, slots=True)
class KnowledgeBaseDeleteJobResult:
    base: KnowledgeBaseRecord
    job: JobRecord
    replayed: bool


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentDeleteJobResult:
    document: KnowledgeDocumentRecord
    job: JobRecord
    replayed: bool


class KnowledgeService:
    """Coordinate idempotent HTTP mutations with durable Knowledge jobs."""

    def __init__(
        self,
        store: KnowledgeStore,
        jobs: JobStore,
        settings: Settings,
        *,
        searcher: KnowledgeSearcher | None = None,
        dispatch_job: DispatchJob | None = None,
        dispatch_outbox: JobDispatchOutbox | None = None,
        embedding_model: str | None = None,
        embedding_dim: int | None = None,
    ) -> None:
        if store.scope_id != jobs.scope_id:
            raise ValueError("Knowledge and job stores must use the same scope")
        if searcher is not None and searcher.scope_id != store.scope_id:
            raise ValueError("Knowledge searcher must use the same scope")
        self._store = store
        self._jobs = jobs
        self._searcher = searcher
        self._dispatch_job = dispatch_job
        self._dispatch_outbox = dispatch_outbox
        self._embedding_model = (
            embedding_model.strip()
            if isinstance(embedding_model, str) and embedding_model.strip()
            else None
        )
        self._embedding_dim = (
            embedding_dim if type(embedding_dim) is int and embedding_dim > 0 else None
        )
        self._chunk_target = settings.knowledge_chunk_target_chars
        self._chunk_overlap = settings.knowledge_chunk_overlap_chars

    @property
    def scope_id(self) -> str:
        return self._store.scope_id

    @property
    def store(self) -> KnowledgeStore:
        return self._store

    async def create_base(
        self,
        command: CreateKnowledgeBaseCommand,
        idempotency_key: str,
    ) -> KnowledgeBaseIdempotencyResult:
        if self._embedding_model is None or self._embedding_dim is None:
            raise KnowledgeValidationError(
                KnowledgePublicCode.embeddings_not_configured,
                "Knowledge embeddings are not configured.",
            )
        operation = KnowledgeOperation.create_base
        fingerprint = canonical_request_fingerprint("POST", operation, (), command)
        return await self._store.create_base_idempotent(
            KnowledgeBaseCreate(
                name=command.name,
                description=command.description,
                embedding_model=self._embedding_model,
                embedding_dim=self._embedding_dim,
            ),
            KnowledgeIdempotencyBegin(operation, idempotency_key, fingerprint),
        )

    async def list_bases(self) -> list[KnowledgeBaseRecord]:
        return await self._store.list_bases()

    async def get_base(self, kb_id: str) -> KnowledgeBaseRecord | None:
        return await self._store.get_base(kb_id)

    async def create_document(
        self,
        kb_id: str,
        command: CreateKnowledgeDocumentCommand,
        idempotency_key: str,
    ) -> KnowledgeDocumentJobResult:
        operation = KnowledgeOperation.create_document
        fingerprint = canonical_request_fingerprint(
            "POST",
            operation,
            {"kb_id": kb_id},
            command,
        )
        result = await self._store.create_document_version_idempotent(
            self._version_create(kb_id, command),
            KnowledgeIdempotencyBegin(operation, idempotency_key, fingerprint),
        )
        return await self._ensure_ingest_job(
            result,
            operation=operation,
            idempotency_key=idempotency_key,
            fingerprint=fingerprint,
            target_session_id=command.target_session_id,
        )

    async def update_document(
        self,
        kb_id: str,
        document_id: str,
        command: UpdateKnowledgeDocumentCommand,
        idempotency_key: str,
    ) -> KnowledgeDocumentJobResult:
        operation = KnowledgeOperation.update_document
        fingerprint = canonical_request_fingerprint(
            "PUT",
            operation,
            {"kb_id": kb_id, "document_id": document_id},
            command,
        )
        result = await self._store.update_document_version_idempotent(
            self._version_create(kb_id, command, document_id=document_id),
            KnowledgeIdempotencyBegin(operation, idempotency_key, fingerprint),
        )
        return await self._ensure_ingest_job(
            result,
            operation=operation,
            idempotency_key=idempotency_key,
            fingerprint=fingerprint,
            target_session_id=command.target_session_id,
        )

    async def reindex_document(
        self,
        kb_id: str,
        document_id: str,
        command: ReindexKnowledgeDocumentCommand,
        idempotency_key: str,
    ) -> KnowledgeDocumentJobResult:
        operation = KnowledgeOperation.reindex_document
        fingerprint = canonical_request_fingerprint(
            "POST",
            operation,
            {"kb_id": kb_id, "document_id": document_id},
            command,
        )
        result = await self._store.reindex_document_idempotent(
            KnowledgeDocumentReindex(
                kb_id=kb_id,
                document_id=document_id,
                chunking_version=KNOWLEDGE_CHUNKING_VERSION,
                target_chars=self._chunk_target,
                overlap_chars=self._chunk_overlap,
            ),
            KnowledgeIdempotencyBegin(operation, idempotency_key, fingerprint),
        )
        return await self._ensure_ingest_job(
            result,
            operation=operation,
            idempotency_key=idempotency_key,
            fingerprint=fingerprint,
            target_session_id=command.target_session_id,
        )

    async def list_documents(self, kb_id: str) -> list[KnowledgeDocumentRecord]:
        return await self._store.list_documents(kb_id)

    async def get_document(
        self,
        kb_id: str,
        document_id: str,
    ) -> KnowledgeDocumentRecord | None:
        return await self._store.get_document(kb_id, document_id)

    async def list_versions(
        self,
        kb_id: str,
        document_id: str,
    ) -> list[KnowledgeDocumentVersionRecord]:
        return await self._store.list_versions(kb_id, document_id)

    async def delete_base(
        self,
        kb_id: str,
        command: DeleteKnowledgeCommand,
        idempotency_key: str,
    ) -> KnowledgeBaseDeleteJobResult:
        operation = KnowledgeOperation.delete_base
        fingerprint = canonical_request_fingerprint(
            "DELETE",
            operation,
            {"kb_id": kb_id},
            command,
        )
        result = await self._store.tombstone_base_idempotent(
            KnowledgeBaseTombstone(kb_id),
            KnowledgeIdempotencyBegin(operation, idempotency_key, fingerprint),
        )
        job = await self._ensure_delete_job(
            operation=operation,
            idempotency_key=idempotency_key,
            fingerprint=fingerprint,
            kb_id=kb_id,
            document_id=None,
        )
        return KnowledgeBaseDeleteJobResult(result.resource, job, result.replayed)

    async def delete_document(
        self,
        kb_id: str,
        document_id: str,
        command: DeleteKnowledgeCommand,
        idempotency_key: str,
    ) -> KnowledgeDocumentDeleteJobResult:
        operation = KnowledgeOperation.delete_document
        fingerprint = canonical_request_fingerprint(
            "DELETE",
            operation,
            {"kb_id": kb_id, "document_id": document_id},
            command,
        )
        result = await self._store.tombstone_document_idempotent(
            KnowledgeDocumentTombstone(kb_id, document_id),
            KnowledgeIdempotencyBegin(operation, idempotency_key, fingerprint),
        )
        job = await self._ensure_delete_job(
            operation=operation,
            idempotency_key=idempotency_key,
            fingerprint=fingerprint,
            kb_id=kb_id,
            document_id=document_id,
        )
        return KnowledgeDocumentDeleteJobResult(result.resource, job, result.replayed)

    async def search(
        self,
        kb_id: str,
        query: str,
        *,
        k: int,
    ) -> tuple[list[KnowledgeHit], KnowledgeSearchStatus]:
        if self._searcher is None:
            raise KnowledgeStorageError()
        return await self._searcher.search(kb_id, query, k=k)

    def _version_create(
        self,
        kb_id: str,
        command: CreateKnowledgeDocumentCommand | UpdateKnowledgeDocumentCommand,
        *,
        document_id: str | None = None,
    ) -> KnowledgeDocumentVersionCreate:
        mime_type = (
            "text/markdown" if command.source_type is KnowledgeSourceType.markdown else "text/plain"
        )
        return KnowledgeDocumentVersionCreate(
            kb_id=kb_id,
            document_id=document_id,
            title=command.title,
            source_type=command.source_type,
            source_uri=command.source_uri,
            content=command.content,
            mime_type=mime_type,
            chunking_version=KNOWLEDGE_CHUNKING_VERSION,
            target_chars=self._chunk_target,
            overlap_chars=self._chunk_overlap,
        )

    async def _enqueue_job_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        cancel_mode: CancelMode,
    ) -> tuple[JobRecord, bool]:
        """Enqueue a durable Knowledge job, atomically recording a cross-scope dispatch intent.

        When an outbox is wired (durable Postgres substrate) the job insert and the global
        ``job_dispatch_outbox`` intent commit in ONE transaction, so a committed Knowledge job
        always has a discoverable dispatch pointer the worker reconciler can recover after a lost
        enqueue — the per-Agent-scope indexing job is never orphaned (finding 3). Without an
        outbox (in-memory/lite profile) it falls back to the plain enqueue.
        """
        if self._dispatch_outbox is not None:
            return await self._jobs.enqueue_once_with_dispatch_intent(
                kind=kind,
                payload=payload,
                target_session_id=target_session_id,
                idempotency_key=idempotency_key,
                max_attempts=max_attempts,
                cancel_mode=cancel_mode,
                outbox=self._dispatch_outbox,
            )
        return await self._jobs.enqueue_once(
            kind=kind,
            payload=payload,
            target_session_id=target_session_id,
            idempotency_key=idempotency_key,
            max_attempts=max_attempts,
            cancel_mode=cancel_mode,
        )

    async def _ensure_ingest_job(
        self,
        result: KnowledgeDocumentVersionIdempotencyResult,
        *,
        operation: KnowledgeOperation,
        idempotency_key: str,
        fingerprint: str,
        target_session_id: str | None,
    ) -> KnowledgeDocumentJobResult:
        version = result.resource.version
        payload = KnowledgeIngestPayload(
            kb_id=version.kb_id,
            document_id=version.document_id,
            document_version_id=version.id,
        )
        try:
            job, _ = await self._enqueue_job_once(
                kind=KNOWLEDGE_INGEST_KIND,
                payload=payload.model_dump(mode="json"),
                target_session_id=target_session_id,
                idempotency_key=f"ingest:{version.id}",
                max_attempts=KNOWLEDGE_INGEST_MAX_ATTEMPTS,
                cancel_mode=KNOWLEDGE_INGEST_CANCEL_MODE,
            )
        except JobValidationError as exc:
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                exc.public_message,
            ) from None
        await self._store.attach_idempotent_job(
            KnowledgeIdempotencyAttach(
                operation=operation,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                job_id=job.id,
            )
        )
        authoritative = await self._store.get_version(
            version.kb_id,
            version.document_id,
            version.id,
        )
        if authoritative is None:
            raise KnowledgeStorageError()
        await self._dispatch(job)
        return KnowledgeDocumentJobResult(
            document=result.resource.document,
            version=authoritative,
            job=job,
            replayed=result.replayed,
        )

    async def _ensure_delete_job(
        self,
        *,
        operation: KnowledgeOperation,
        idempotency_key: str,
        fingerprint: str,
        kb_id: str,
        document_id: str | None,
    ) -> JobRecord:
        payload = KnowledgeDeletePayload(kb_id=kb_id, document_id=document_id)
        resource_key = document_id if document_id is not None else "all"
        job, _ = await self._enqueue_job_once(
            kind=KNOWLEDGE_DELETE_KIND,
            payload=payload.model_dump(mode="json"),
            target_session_id=None,
            idempotency_key=f"delete:{kb_id}:{resource_key}",
            max_attempts=KNOWLEDGE_DELETE_MAX_ATTEMPTS,
            cancel_mode=KNOWLEDGE_DELETE_CANCEL_MODE,
        )
        await self._store.attach_idempotent_job(
            KnowledgeIdempotencyAttach(
                operation=operation,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                job_id=job.id,
            )
        )
        await self._dispatch(job)
        return job

    async def _dispatch(self, job: JobRecord) -> None:
        if self._dispatch_job is None:
            return
        try:
            await self._dispatch_job(self.scope_id, job.id)
        except Exception as exc:  # noqa: BLE001 - Postgres dispatcher heals missed delivery
            logger.warning(
                "knowledge job dispatch deferred scope=%s job=%s kind=%s error_type=%s",
                self.scope_id,
                job.id,
                job.kind,
                type(exc).__name__,
            )


__all__ = [
    "KNOWLEDGE_CHUNKING_VERSION",
    "DispatchJob",
    "KnowledgeBaseDeleteJobResult",
    "KnowledgeDocumentDeleteJobResult",
    "KnowledgeDocumentJobResult",
    "KnowledgeService",
]
