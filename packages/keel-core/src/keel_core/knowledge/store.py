"""Knowledge lifecycle store protocol and deterministic in-memory implementation."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import replace
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from .models import (
    KnowledgeActivationResult,
    KnowledgeBaseCreate,
    KnowledgeBaseRecord,
    KnowledgeBaseStatus,
    KnowledgeChunkRecord,
    KnowledgeChunkReplacement,
    KnowledgeChunkReplacementResult,
    KnowledgeConflict,
    KnowledgeDocumentRecord,
    KnowledgeDocumentReindex,
    KnowledgeDocumentStatus,
    KnowledgeDocumentVersionCreate,
    KnowledgeDocumentVersionRecord,
    KnowledgeDocumentVersionResult,
    KnowledgeEmbeddingMismatch,
    KnowledgeIdempotencyAttach,
    KnowledgeIdempotencyBegin,
    KnowledgeIdempotencyRecord,
    KnowledgeIdempotencyResult,
    KnowledgeIndexingResult,
    KnowledgeNotFound,
    KnowledgeOperation,
    KnowledgePublicCode,
    KnowledgePurgeResult,
    KnowledgeValidationError,
    KnowledgeVersionCancellation,
    KnowledgeVersionFailure,
    KnowledgeVersionStatus,
    content_sha256,
    index_fingerprint,
    new_knowledge_base_id,
    new_knowledge_chunk_id,
    new_knowledge_document_id,
    new_knowledge_idempotency_id,
    new_knowledge_version_id,
    validate_idempotency_key,
    validate_knowledge_base_id,
    validate_knowledge_document_id,
    validate_knowledge_version_id,
    validate_scope_id,
)

_CURRENT_FINGERPRINT_STATUSES = frozenset(
    {
        KnowledgeVersionStatus.pending,
        KnowledgeVersionStatus.indexing,
        KnowledgeVersionStatus.active,
    }
)
_DOCUMENT_WRITE_STATUSES = frozenset(
    {
        KnowledgeDocumentStatus.pending,
        KnowledgeDocumentStatus.active,
    }
)
_IMMUTABLE_TERMINAL_VERSION_STATUSES = frozenset(
    {
        KnowledgeVersionStatus.active,
        KnowledgeVersionStatus.superseded,
        KnowledgeVersionStatus.failed,
        KnowledgeVersionStatus.cancelled,
        KnowledgeVersionStatus.deleted,
        KnowledgeVersionStatus.purged,
    }
)


def _copy[T](value: T) -> T:
    return copy.deepcopy(value)


def _timestamp(now: datetime | None) -> datetime:
    value = datetime.now(UTC) if now is None else now
    if value.tzinfo is None or value.utcoffset() is None:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            "Knowledge timestamps must include a timezone offset.",
        )
    return value.astimezone(UTC)


@runtime_checkable
class KnowledgeStore(Protocol):
    """Scope-bound Knowledge metadata, version, chunk, and request-ledger contract."""

    @property
    def scope_id(self) -> str: ...

    async def create_base(
        self,
        command: KnowledgeBaseCreate,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord: ...

    async def list_bases(
        self,
        *,
        include_deleted: bool = False,
    ) -> list[KnowledgeBaseRecord]: ...

    async def get_base(self, kb_id: str) -> KnowledgeBaseRecord | None: ...

    async def create_document_version(
        self,
        command: KnowledgeDocumentVersionCreate,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionResult: ...

    async def reindex_document(
        self,
        command: KnowledgeDocumentReindex,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionResult: ...

    async def get_document(
        self,
        kb_id: str,
        document_id: str,
    ) -> KnowledgeDocumentRecord | None: ...

    async def list_documents(
        self,
        kb_id: str,
        *,
        include_deleted: bool = False,
    ) -> list[KnowledgeDocumentRecord]: ...

    async def get_version(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> KnowledgeDocumentVersionRecord | None: ...

    async def list_versions(
        self,
        kb_id: str,
        document_id: str,
    ) -> list[KnowledgeDocumentVersionRecord]: ...

    async def list_version_chunks(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> list[KnowledgeChunkRecord]: ...

    async def attach_version_job(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        job_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord: ...

    async def mark_indexing(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeIndexingResult: ...

    async def replace_version_chunks(
        self,
        command: KnowledgeChunkReplacement,
        *,
        now: datetime | None = None,
    ) -> KnowledgeChunkReplacementResult: ...

    async def activate_version(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeActivationResult: ...

    async def mark_version_failed(
        self,
        command: KnowledgeVersionFailure,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord: ...

    async def mark_version_cancelled(
        self,
        command: KnowledgeVersionCancellation,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord: ...

    async def tombstone_document(
        self,
        kb_id: str,
        document_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentRecord: ...

    async def tombstone_base(
        self,
        kb_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord: ...

    async def purge_document(
        self,
        kb_id: str,
        document_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgePurgeResult: ...

    async def purge_base(
        self,
        kb_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgePurgeResult: ...

    async def begin_idempotent_request(
        self,
        command: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeIdempotencyResult: ...

    async def get_idempotent_request(
        self,
        operation: KnowledgeOperation,
        idempotency_key: str,
    ) -> KnowledgeIdempotencyRecord | None: ...

    async def attach_idempotent_job(
        self,
        command: KnowledgeIdempotencyAttach,
        *,
        now: datetime | None = None,
    ) -> KnowledgeIdempotencyRecord: ...


class InMemoryKnowledgeStore:
    """Scope-bound lifecycle store with detached reads and serialized mutations."""

    def __init__(self, scope_id: str) -> None:
        self._scope_id = validate_scope_id(scope_id)
        self._bases: dict[str, KnowledgeBaseRecord] = {}
        self._documents: dict[str, KnowledgeDocumentRecord] = {}
        self._versions: dict[str, KnowledgeDocumentVersionRecord] = {}
        self._chunks: dict[str, KnowledgeChunkRecord] = {}
        self._idempotency: dict[tuple[KnowledgeOperation, str], KnowledgeIdempotencyRecord] = {}
        self._lock = asyncio.Lock()

    @property
    def scope_id(self) -> str:
        return self._scope_id

    def _base_locked(self, kb_id: str) -> KnowledgeBaseRecord:
        validated = validate_knowledge_base_id(kb_id)
        record = self._bases.get(validated)
        if record is None:
            raise KnowledgeNotFound(
                KnowledgePublicCode.knowledge_base_not_found,
                "Knowledge Base was not found.",
            )
        return record

    def _document_locked(self, kb_id: str, document_id: str) -> KnowledgeDocumentRecord:
        base_id = validate_knowledge_base_id(kb_id)
        validated = validate_knowledge_document_id(document_id)
        record = self._documents.get(validated)
        if record is None or record.kb_id != base_id:
            raise KnowledgeNotFound(
                KnowledgePublicCode.knowledge_document_not_found,
                "Knowledge document was not found.",
            )
        return record

    def _version_locked(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> KnowledgeDocumentVersionRecord:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        validated = validate_knowledge_version_id(document_version_id)
        record = self._versions.get(validated)
        if record is None or record.kb_id != base_id or record.document_id != doc_id:
            raise KnowledgeNotFound(
                KnowledgePublicCode.knowledge_version_not_found,
                "Knowledge document version was not found.",
            )
        return record

    def _delete_version_chunks_locked(self, document_version_id: str) -> int:
        chunk_ids = [
            chunk_id
            for chunk_id, chunk in self._chunks.items()
            if chunk.document_version_id == document_version_id
        ]
        for chunk_id in chunk_ids:
            del self._chunks[chunk_id]
        return len(chunk_ids)

    def _purge_version_locked(
        self,
        version: KnowledgeDocumentVersionRecord,
        timestamp: datetime,
    ) -> tuple[KnowledgeDocumentVersionRecord, int, bool]:
        removed = self._delete_version_chunks_locked(version.id)
        changed = (
            version.status is not KnowledgeVersionStatus.purged
            or version.content is not None
            or removed > 0
        )
        if changed:
            version = replace(
                version,
                content=None,
                status=KnowledgeVersionStatus.purged,
                deleted_at=version.deleted_at or timestamp,
                purged_at=version.purged_at or timestamp,
            )
            self._versions[version.id] = version
        return version, removed, changed

    def _purge_document_locked(
        self,
        document: KnowledgeDocumentRecord,
        timestamp: datetime,
    ) -> KnowledgePurgeResult:
        versions_purged = 0
        chunks_removed = 0
        for version in tuple(self._versions.values()):
            if version.document_id != document.id or version.kb_id != document.kb_id:
                continue
            _, removed, changed = self._purge_version_locked(version, timestamp)
            chunks_removed += removed
            versions_purged += int(changed)
        document_changed = (
            document.status is not KnowledgeDocumentStatus.deleted
            or document.source_uri is not None
            or document.desired_version_id is not None
            or document.active_version_id is not None
        )
        if document_changed:
            document = replace(
                document,
                source_uri=None,
                status=KnowledgeDocumentStatus.deleted,
                desired_version_id=None,
                active_version_id=None,
                updated_at=timestamp,
                deleted_at=document.deleted_at or timestamp,
            )
            self._documents[document.id] = document
        return KnowledgePurgeResult(
            documents_purged=int(document_changed or versions_purged > 0 or chunks_removed > 0),
            versions_purged=versions_purged,
            chunks_removed=chunks_removed,
        )

    def _find_current_fingerprint_locked(
        self,
        document: KnowledgeDocumentRecord,
        fingerprint: str,
    ) -> KnowledgeDocumentVersionRecord | None:
        seen: set[str] = set()
        for version_id in (document.desired_version_id, document.active_version_id):
            if version_id is None or version_id in seen:
                continue
            seen.add(version_id)
            version = self._versions.get(version_id)
            if (
                version is not None
                and version.index_fingerprint == fingerprint
                and version.status in _CURRENT_FINGERPRINT_STATUSES
            ):
                return version
        return None

    def _create_document_version_locked(
        self,
        command: KnowledgeDocumentVersionCreate,
        timestamp: datetime,
    ) -> KnowledgeDocumentVersionResult:
        base = self._base_locked(command.kb_id)
        if base.status is not KnowledgeBaseStatus.active:
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_base_deleted,
                "Knowledge Base is deleted.",
            )
        digest = content_sha256(command.content)
        fingerprint = index_fingerprint(
            digest,
            command.chunking_version,
            base.embedding_model,
            base.embedding_dim,
            command.target_chars,
            command.overlap_chars,
        )
        if command.document_id is None:
            document_id = command.new_document_id or new_knowledge_document_id()
            while document_id in self._documents:
                if command.new_document_id is not None:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.knowledge_conflict,
                        "Knowledge document identifier already exists.",
                    )
                document_id = new_knowledge_document_id()
            document = None
        else:
            document_id = command.document_id
            document = self._document_locked(base.id, document_id)
            if document.status is KnowledgeDocumentStatus.deleted:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_document_deleted,
                    "Knowledge document is deleted.",
                )
            reusable = self._find_current_fingerprint_locked(document, fingerprint)
            if reusable is not None:
                document = replace(
                    document,
                    title=command.title,
                    source_type=command.source_type,
                    source_uri=command.source_uri,
                    status=(
                        KnowledgeDocumentStatus.active
                        if document.active_version_id is not None
                        else KnowledgeDocumentStatus.pending
                    ),
                    desired_version_id=reusable.id,
                    last_error_kind=None,
                    last_error_message=None,
                    updated_at=timestamp,
                )
                self._documents[document.id] = document
                return KnowledgeDocumentVersionResult(
                    document=_copy(document),
                    version=_copy(reusable),
                    reused=True,
                )

        version_id = command.document_version_id or new_knowledge_version_id()
        if version_id in self._versions:
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge document version identifier already exists.",
            )
        if document is None:
            version_number = 1
        else:
            version_number = (
                max(
                    (
                        version.version
                        for version in self._versions.values()
                        if version.document_id == document.id and version.kb_id == base.id
                    ),
                    default=0,
                )
                + 1
            )
        version = KnowledgeDocumentVersionRecord(
            id=version_id,
            scope_id=self._scope_id,
            kb_id=base.id,
            document_id=document_id,
            version=version_number,
            content=command.content,
            content_sha256=digest,
            index_fingerprint=fingerprint,
            mime_type=command.mime_type,
            chunking_version=command.chunking_version,
            ingest_job_id=None,
            status=KnowledgeVersionStatus.pending,
            error_kind=None,
            error_message=None,
            created_at=timestamp,
            activated_at=None,
            deleted_at=None,
            purged_at=None,
        )
        if document is None:
            document = KnowledgeDocumentRecord(
                id=document_id,
                scope_id=self._scope_id,
                kb_id=base.id,
                title=command.title,
                source_type=command.source_type,
                source_uri=command.source_uri,
                status=KnowledgeDocumentStatus.pending,
                desired_version_id=version.id,
                active_version_id=None,
                last_error_kind=None,
                last_error_message=None,
                created_at=timestamp,
                updated_at=timestamp,
                deleted_at=None,
            )
        else:
            document = replace(
                document,
                title=command.title,
                source_type=command.source_type,
                source_uri=command.source_uri,
                status=(
                    KnowledgeDocumentStatus.active
                    if document.active_version_id is not None
                    else KnowledgeDocumentStatus.pending
                ),
                desired_version_id=version.id,
                last_error_kind=None,
                last_error_message=None,
                updated_at=timestamp,
            )
        self._versions[version.id] = version
        self._documents[document.id] = document
        return KnowledgeDocumentVersionResult(
            document=_copy(document),
            version=_copy(version),
            reused=False,
        )

    async def create_base(
        self,
        command: KnowledgeBaseCreate,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord:
        timestamp = _timestamp(now)
        async with self._lock:
            if any(
                base.status is KnowledgeBaseStatus.active and base.name == command.name
                for base in self._bases.values()
            ):
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_base_name_conflict,
                    "An active Knowledge Base already uses this name.",
                )
            base_id = command.base_id or new_knowledge_base_id()
            if base_id in self._bases:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_conflict,
                    "Knowledge Base identifier already exists.",
                )
            record = KnowledgeBaseRecord(
                id=base_id,
                scope_id=self._scope_id,
                name=command.name,
                description=command.description,
                embedding_model=command.embedding_model,
                embedding_dim=command.embedding_dim,
                status=KnowledgeBaseStatus.active,
                created_at=timestamp,
                updated_at=timestamp,
                deleted_at=None,
            )
            self._bases[record.id] = record
            return _copy(record)

    async def list_bases(
        self,
        *,
        include_deleted: bool = False,
    ) -> list[KnowledgeBaseRecord]:
        async with self._lock:
            records = [
                record
                for record in self._bases.values()
                if include_deleted or record.status is KnowledgeBaseStatus.active
            ]
            records.sort(key=lambda record: (record.created_at, record.id))
            return _copy(records)

    async def get_base(self, kb_id: str) -> KnowledgeBaseRecord | None:
        validated = validate_knowledge_base_id(kb_id)
        async with self._lock:
            record = self._bases.get(validated)
            return None if record is None else _copy(record)

    async def create_document_version(
        self,
        command: KnowledgeDocumentVersionCreate,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionResult:
        timestamp = _timestamp(now)
        async with self._lock:
            return self._create_document_version_locked(command, timestamp)

    async def reindex_document(
        self,
        command: KnowledgeDocumentReindex,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionResult:
        timestamp = _timestamp(now)
        async with self._lock:
            base = self._base_locked(command.kb_id)
            if base.status is not KnowledgeBaseStatus.active:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_base_deleted,
                    "Knowledge Base is deleted.",
                )
            document = self._document_locked(base.id, command.document_id)
            if document.status is KnowledgeDocumentStatus.deleted:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_document_deleted,
                    "Knowledge document is deleted.",
                )
            if document.active_version_id is None:
                raise KnowledgeConflict(
                    KnowledgePublicCode.no_active_version,
                    "Knowledge document has no active version to reindex.",
                )
            active = self._version_locked(
                base.id,
                document.id,
                document.active_version_id,
            )
            if active.status is not KnowledgeVersionStatus.active or active.content is None:
                raise KnowledgeConflict(
                    KnowledgePublicCode.no_active_version,
                    "Knowledge document has no active version to reindex.",
                )
            return self._create_document_version_locked(
                KnowledgeDocumentVersionCreate(
                    kb_id=base.id,
                    document_id=document.id,
                    title=document.title,
                    source_type=document.source_type,
                    source_uri=document.source_uri,
                    content=active.content,
                    mime_type=active.mime_type,
                    chunking_version=command.chunking_version,
                    target_chars=command.target_chars,
                    overlap_chars=command.overlap_chars,
                    document_version_id=command.document_version_id,
                ),
                timestamp,
            )

    async def get_document(
        self,
        kb_id: str,
        document_id: str,
    ) -> KnowledgeDocumentRecord | None:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        async with self._lock:
            record = self._documents.get(doc_id)
            if record is None or record.kb_id != base_id:
                return None
            return _copy(record)

    async def list_documents(
        self,
        kb_id: str,
        *,
        include_deleted: bool = False,
    ) -> list[KnowledgeDocumentRecord]:
        validated = validate_knowledge_base_id(kb_id)
        async with self._lock:
            self._base_locked(validated)
            records = [
                record
                for record in self._documents.values()
                if record.kb_id == validated
                and (include_deleted or record.status is not KnowledgeDocumentStatus.deleted)
            ]
            records.sort(key=lambda record: (record.created_at, record.id))
            return _copy(records)

    async def get_version(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> KnowledgeDocumentVersionRecord | None:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        version_id = validate_knowledge_version_id(document_version_id)
        async with self._lock:
            record = self._versions.get(version_id)
            if record is None or record.kb_id != base_id or record.document_id != doc_id:
                return None
            return _copy(record)

    async def list_versions(
        self,
        kb_id: str,
        document_id: str,
    ) -> list[KnowledgeDocumentVersionRecord]:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        async with self._lock:
            self._document_locked(base_id, doc_id)
            records = [
                record
                for record in self._versions.values()
                if record.kb_id == base_id and record.document_id == doc_id
            ]
            records.sort(key=lambda record: record.version)
            return _copy(records)

    async def list_version_chunks(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> list[KnowledgeChunkRecord]:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        version_id = validate_knowledge_version_id(document_version_id)
        async with self._lock:
            self._version_locked(base_id, doc_id, version_id)
            records = [
                record
                for record in self._chunks.values()
                if record.kb_id == base_id
                and record.document_id == doc_id
                and record.document_version_id == version_id
            ]
            records.sort(key=lambda record: record.ordinal)
            return _copy(records)

    async def attach_version_job(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        job_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord:
        _timestamp(now)
        if not isinstance(job_id, str) or not job_id.strip() or "\x00" in job_id:
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Knowledge job identifier is invalid.",
            )
        try:
            job_id.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Knowledge job identifier is invalid.",
            ) from exc
        async with self._lock:
            version = self._version_locked(kb_id, document_id, document_version_id)
            if version.ingest_job_id is not None and version.ingest_job_id != job_id:
                raise KnowledgeConflict(
                    KnowledgePublicCode.idempotency_job_conflict,
                    "Knowledge version already has a different job.",
                )
            if version.ingest_job_id is None:
                version = replace(version, ingest_job_id=job_id)
                self._versions[version.id] = version
            return _copy(version)

    async def mark_indexing(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeIndexingResult:
        timestamp = _timestamp(now)
        async with self._lock:
            base = self._base_locked(kb_id)
            document = self._document_locked(base.id, document_id)
            version = self._version_locked(base.id, document.id, document_version_id)
            if (
                base.status is KnowledgeBaseStatus.deleted
                or document.status is KnowledgeDocumentStatus.deleted
            ):
                version, _, _ = self._purge_version_locked(version, timestamp)
                return KnowledgeIndexingResult(_copy(version), started=False, stale=False)
            if version.status in _IMMUTABLE_TERMINAL_VERSION_STATUSES:
                return KnowledgeIndexingResult(_copy(version), started=False, stale=False)
            if document.desired_version_id != version.id:
                self._delete_version_chunks_locked(version.id)
                version = replace(
                    version,
                    status=KnowledgeVersionStatus.superseded,
                    error_kind=None,
                    error_message=None,
                )
                self._versions[version.id] = version
                return KnowledgeIndexingResult(_copy(version), started=False, stale=True)
            if version.status is KnowledgeVersionStatus.indexing:
                return KnowledgeIndexingResult(_copy(version), started=False, stale=False)
            version = replace(
                version,
                status=KnowledgeVersionStatus.indexing,
                error_kind=None,
                error_message=None,
            )
            self._versions[version.id] = version
            return KnowledgeIndexingResult(_copy(version), started=True, stale=False)

    async def replace_version_chunks(
        self,
        command: KnowledgeChunkReplacement,
        *,
        now: datetime | None = None,
    ) -> KnowledgeChunkReplacementResult:
        timestamp = _timestamp(now)
        async with self._lock:
            base = self._base_locked(command.kb_id)
            document = self._document_locked(base.id, command.document_id)
            version = self._version_locked(
                base.id,
                document.id,
                command.document_version_id,
            )
            if (
                base.status is not KnowledgeBaseStatus.active
                or document.status not in _DOCUMENT_WRITE_STATUSES
                or version.status is not KnowledgeVersionStatus.indexing
            ):
                raise KnowledgeConflict(
                    KnowledgePublicCode.chunk_write_rejected,
                    "Knowledge chunks may only be written to an indexing version.",
                )
            for chunk in command.chunks:
                if chunk.model != base.embedding_model or chunk.dim != base.embedding_dim:
                    raise KnowledgeEmbeddingMismatch()
                if (
                    version.content is None
                    or chunk.char_end > len(version.content)
                    or version.content[chunk.char_start : chunk.char_end] != chunk.text
                ):
                    raise KnowledgeConflict(
                        KnowledgePublicCode.chunk_write_rejected,
                        "Knowledge chunk offsets do not match the document version.",
                    )
            existing_by_ordinal = {
                chunk.ordinal: chunk
                for chunk in self._chunks.values()
                if chunk.document_version_id == version.id
            }
            new_records: list[KnowledgeChunkRecord] = []
            used_ids: set[str] = set()
            for chunk in command.chunks:
                existing = existing_by_ordinal.get(chunk.ordinal)
                chunk_id = chunk.chunk_id or (existing.id if existing is not None else None)
                chunk_id = chunk_id or new_knowledge_chunk_id()
                if chunk_id in used_ids:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.knowledge_conflict,
                        "Knowledge chunk identifiers must be unique.",
                    )
                conflicting = self._chunks.get(chunk_id)
                if conflicting is not None and (
                    conflicting.document_version_id != version.id
                    or conflicting.ordinal != chunk.ordinal
                ):
                    raise KnowledgeConflict(
                        KnowledgePublicCode.knowledge_conflict,
                        "Knowledge chunk identifier already exists.",
                    )
                used_ids.add(chunk_id)
                new_records.append(
                    KnowledgeChunkRecord(
                        id=chunk_id,
                        scope_id=self._scope_id,
                        kb_id=base.id,
                        document_id=document.id,
                        document_version_id=version.id,
                        ordinal=chunk.ordinal,
                        text=chunk.text,
                        char_start=chunk.char_start,
                        char_end=chunk.char_end,
                        content_hash=chunk.content_hash,
                        heading_path=tuple(chunk.heading_path),
                        metadata=_copy(chunk.metadata),
                        model=chunk.model,
                        dim=chunk.dim,
                        embedding=tuple(chunk.embedding),
                        created_at=existing.created_at if existing is not None else timestamp,
                    )
                )
            self._delete_version_chunks_locked(version.id)
            for record in new_records:
                self._chunks[record.id] = record
            return KnowledgeChunkReplacementResult(
                document_version_id=version.id,
                chunk_count=len(new_records),
            )

    async def activate_version(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeActivationResult:
        timestamp = _timestamp(now)
        async with self._lock:
            base = self._base_locked(kb_id)
            document = self._document_locked(base.id, document_id)
            version = self._version_locked(base.id, document.id, document_version_id)
            previous_active_id = document.active_version_id
            if (
                base.status is KnowledgeBaseStatus.deleted
                or document.status is KnowledgeDocumentStatus.deleted
            ):
                version, _, _ = self._purge_version_locked(version, timestamp)
                return KnowledgeActivationResult(
                    document=_copy(document),
                    version=_copy(version),
                    previous_active_version_id=previous_active_id,
                    activated=False,
                    stale=False,
                )
            if (
                version.status is KnowledgeVersionStatus.active
                and document.active_version_id == version.id
            ):
                return KnowledgeActivationResult(
                    document=_copy(document),
                    version=_copy(version),
                    previous_active_version_id=previous_active_id,
                    activated=False,
                    stale=False,
                )
            if version.status is not KnowledgeVersionStatus.indexing:
                return KnowledgeActivationResult(
                    document=_copy(document),
                    version=_copy(version),
                    previous_active_version_id=previous_active_id,
                    activated=False,
                    stale=False,
                )
            if document.desired_version_id != version.id:
                self._delete_version_chunks_locked(version.id)
                version = replace(
                    version,
                    status=KnowledgeVersionStatus.superseded,
                    error_kind=None,
                    error_message=None,
                )
                self._versions[version.id] = version
                return KnowledgeActivationResult(
                    document=_copy(document),
                    version=_copy(version),
                    previous_active_version_id=previous_active_id,
                    activated=False,
                    stale=True,
                )
            if previous_active_id is not None and previous_active_id != version.id:
                previous = self._versions.get(previous_active_id)
                if previous is not None and previous.status is KnowledgeVersionStatus.active:
                    self._versions[previous.id] = replace(
                        previous,
                        status=KnowledgeVersionStatus.superseded,
                    )
            version = replace(
                version,
                status=KnowledgeVersionStatus.active,
                error_kind=None,
                error_message=None,
                activated_at=version.activated_at or timestamp,
            )
            document = replace(
                document,
                status=KnowledgeDocumentStatus.active,
                active_version_id=version.id,
                desired_version_id=version.id,
                last_error_kind=None,
                last_error_message=None,
                updated_at=timestamp,
            )
            self._versions[version.id] = version
            self._documents[document.id] = document
            return KnowledgeActivationResult(
                document=_copy(document),
                version=_copy(version),
                previous_active_version_id=previous_active_id,
                activated=True,
                stale=False,
            )

    def _mark_terminal_locked(
        self,
        *,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        status: KnowledgeVersionStatus,
        error_kind: str,
        error_message: str,
        timestamp: datetime,
    ) -> KnowledgeDocumentVersionRecord:
        base = self._base_locked(kb_id)
        document = self._document_locked(base.id, document_id)
        version = self._version_locked(base.id, document.id, document_version_id)
        if (
            base.status is KnowledgeBaseStatus.deleted
            or document.status is KnowledgeDocumentStatus.deleted
        ):
            version, _, _ = self._purge_version_locked(version, timestamp)
            return _copy(version)
        if version.status in _IMMUTABLE_TERMINAL_VERSION_STATUSES:
            return _copy(version)
        self._delete_version_chunks_locked(version.id)
        version = replace(
            version,
            status=status,
            error_kind=error_kind,
            error_message=error_message,
        )
        self._versions[version.id] = version
        if document.active_version_id is None and document.desired_version_id == version.id:
            document = replace(
                document,
                status=KnowledgeDocumentStatus.failed,
                last_error_kind=error_kind,
                last_error_message=error_message,
                updated_at=timestamp,
            )
            self._documents[document.id] = document
        return _copy(version)

    async def mark_version_failed(
        self,
        command: KnowledgeVersionFailure,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord:
        timestamp = _timestamp(now)
        async with self._lock:
            return self._mark_terminal_locked(
                kb_id=command.kb_id,
                document_id=command.document_id,
                document_version_id=command.document_version_id,
                status=KnowledgeVersionStatus.failed,
                error_kind=command.error_kind,
                error_message=command.error_message,
                timestamp=timestamp,
            )

    async def mark_version_cancelled(
        self,
        command: KnowledgeVersionCancellation,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord:
        timestamp = _timestamp(now)
        async with self._lock:
            return self._mark_terminal_locked(
                kb_id=command.kb_id,
                document_id=command.document_id,
                document_version_id=command.document_version_id,
                status=KnowledgeVersionStatus.cancelled,
                error_kind=command.error_kind,
                error_message=command.error_message,
                timestamp=timestamp,
            )

    async def tombstone_document(
        self,
        kb_id: str,
        document_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentRecord:
        timestamp = _timestamp(now)
        async with self._lock:
            self._base_locked(kb_id)
            document = self._document_locked(kb_id, document_id)
            if document.status is not KnowledgeDocumentStatus.deleted:
                document = replace(
                    document,
                    status=KnowledgeDocumentStatus.deleted,
                    desired_version_id=None,
                    active_version_id=None,
                    updated_at=timestamp,
                    deleted_at=timestamp,
                )
                self._documents[document.id] = document
            return _copy(document)

    async def tombstone_base(
        self,
        kb_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord:
        timestamp = _timestamp(now)
        async with self._lock:
            base = self._base_locked(kb_id)
            if base.status is not KnowledgeBaseStatus.deleted:
                base = replace(
                    base,
                    status=KnowledgeBaseStatus.deleted,
                    updated_at=timestamp,
                    deleted_at=timestamp,
                )
                self._bases[base.id] = base
            return _copy(base)

    async def purge_document(
        self,
        kb_id: str,
        document_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgePurgeResult:
        timestamp = _timestamp(now)
        async with self._lock:
            base = self._base_locked(kb_id)
            document = self._document_locked(base.id, document_id)
            if (
                base.status is KnowledgeBaseStatus.active
                and document.status is not KnowledgeDocumentStatus.deleted
            ):
                raise KnowledgeConflict(
                    KnowledgePublicCode.invalid_transition,
                    "Knowledge document must be tombstoned before purge.",
                )
            return self._purge_document_locked(document, timestamp)

    async def purge_base(
        self,
        kb_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgePurgeResult:
        timestamp = _timestamp(now)
        async with self._lock:
            base = self._base_locked(kb_id)
            if base.status is not KnowledgeBaseStatus.deleted:
                raise KnowledgeConflict(
                    KnowledgePublicCode.invalid_transition,
                    "Knowledge Base must be tombstoned before purge.",
                )
            documents_purged = 0
            versions_purged = 0
            chunks_removed = 0
            for document in tuple(self._documents.values()):
                if document.kb_id != base.id:
                    continue
                result = self._purge_document_locked(document, timestamp)
                documents_purged += result.documents_purged
                versions_purged += result.versions_purged
                chunks_removed += result.chunks_removed
            if base.description is not None:
                base = replace(base, description=None, updated_at=timestamp)
                self._bases[base.id] = base
            return KnowledgePurgeResult(
                documents_purged=documents_purged,
                versions_purged=versions_purged,
                chunks_removed=chunks_removed,
            )

    async def begin_idempotent_request(
        self,
        command: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeIdempotencyResult:
        timestamp = _timestamp(now)
        key = (command.operation, command.idempotency_key)
        async with self._lock:
            existing = self._idempotency.get(key)
            if existing is not None:
                if existing.request_fingerprint != command.request_fingerprint:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.idempotency_key_reused,
                        "Idempotency key was already used for different input.",
                    )
                return KnowledgeIdempotencyResult(record=_copy(existing), replayed=True)
            ledger_id = command.ledger_id or new_knowledge_idempotency_id()
            if any(record.id == ledger_id for record in self._idempotency.values()):
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_conflict,
                    "Knowledge idempotency identifier already exists.",
                )
            record = KnowledgeIdempotencyRecord(
                id=ledger_id,
                scope_id=self._scope_id,
                operation=command.operation,
                idempotency_key=command.idempotency_key,
                request_fingerprint=command.request_fingerprint,
                resource_kind=command.resource_kind,
                resource_id=command.resource_id,
                document_version_id=command.document_version_id,
                job_id=None,
                created_at=timestamp,
                updated_at=timestamp,
            )
            self._idempotency[key] = record
            return KnowledgeIdempotencyResult(record=_copy(record), replayed=False)

    async def get_idempotent_request(
        self,
        operation: KnowledgeOperation,
        idempotency_key: str,
    ) -> KnowledgeIdempotencyRecord | None:
        if not isinstance(operation, KnowledgeOperation):
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Knowledge operation is invalid.",
            )
        validated = validate_idempotency_key(idempotency_key)
        async with self._lock:
            record = self._idempotency.get((operation, validated))
            return None if record is None else _copy(record)

    async def attach_idempotent_job(
        self,
        command: KnowledgeIdempotencyAttach,
        *,
        now: datetime | None = None,
    ) -> KnowledgeIdempotencyRecord:
        timestamp = _timestamp(now)
        key = (command.operation, command.idempotency_key)
        async with self._lock:
            record = self._idempotency.get(key)
            if record is None:
                raise KnowledgeNotFound(
                    KnowledgePublicCode.idempotency_not_found,
                    "Knowledge idempotency request was not found.",
                )
            if record.request_fingerprint != command.request_fingerprint:
                raise KnowledgeConflict(
                    KnowledgePublicCode.idempotency_key_reused,
                    "Idempotency key was already used for different input.",
                )
            if record.job_id is not None and record.job_id != command.job_id:
                raise KnowledgeConflict(
                    KnowledgePublicCode.idempotency_job_conflict,
                    "Knowledge request already has a different job.",
                )
            if record.document_version_id is not None:
                version = self._versions.get(record.document_version_id)
                if version is None:
                    raise KnowledgeNotFound(
                        KnowledgePublicCode.knowledge_version_not_found,
                        "Knowledge document version was not found.",
                    )
                if version.ingest_job_id is not None and version.ingest_job_id != command.job_id:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.idempotency_job_conflict,
                        "Knowledge version already has a different job.",
                    )
                if version.ingest_job_id is None:
                    self._versions[version.id] = replace(
                        version,
                        ingest_job_id=command.job_id,
                    )
            if record.job_id is None:
                record = replace(
                    record,
                    job_id=command.job_id,
                    updated_at=timestamp,
                )
                self._idempotency[key] = record
            return _copy(record)


__all__ = ["InMemoryKnowledgeStore", "KnowledgeStore"]
