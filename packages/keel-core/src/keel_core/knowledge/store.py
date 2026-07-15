"""Knowledge lifecycle store protocol and deterministic in-memory implementation."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from functools import wraps
from typing import Any, NoReturn, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    IntegrityError,
    InvalidRequestError,
)
from sqlalchemy.exc import (
    TimeoutError as SQLAlchemyTimeoutError,
)
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .models import (
    DEFAULT_KNOWLEDGE_DOCUMENT_MAX_BYTES,
    KnowledgeActivationResult,
    KnowledgeBaseCreate,
    KnowledgeBaseIdempotencyResult,
    KnowledgeBaseRecord,
    KnowledgeBaseStatus,
    KnowledgeBaseTombstone,
    KnowledgeChunkRecord,
    KnowledgeChunkReplacement,
    KnowledgeChunkReplacementResult,
    KnowledgeChunkWrite,
    KnowledgeConflict,
    KnowledgeDocumentIdempotencyResult,
    KnowledgeDocumentRecord,
    KnowledgeDocumentReindex,
    KnowledgeDocumentStatus,
    KnowledgeDocumentTombstone,
    KnowledgeDocumentVersionCreate,
    KnowledgeDocumentVersionIdempotencyResult,
    KnowledgeDocumentVersionRecord,
    KnowledgeDocumentVersionResult,
    KnowledgeEmbeddingMismatch,
    KnowledgeIdempotencyAttach,
    KnowledgeIdempotencyBegin,
    KnowledgeIdempotencyRecord,
    KnowledgeIndexingResult,
    KnowledgeNotFound,
    KnowledgeOperation,
    KnowledgePublicCode,
    KnowledgePurgeResult,
    KnowledgeResourceKind,
    KnowledgeSourceType,
    KnowledgeStorageError,
    KnowledgeValidationError,
    KnowledgeVersionCancellation,
    KnowledgeVersionFailure,
    KnowledgeVersionStatus,
    content_sha256,
    index_fingerprint,
    knowledge_source_type_from_mime_type,
    new_knowledge_base_id,
    new_knowledge_chunk_id,
    new_knowledge_document_id,
    new_knowledge_idempotency_id,
    new_knowledge_version_id,
    validate_document_max_bytes,
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

    @property
    def document_max_bytes(self) -> int: ...

    async def create_base(
        self,
        command: KnowledgeBaseCreate,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord: ...

    async def create_base_idempotent(
        self,
        command: KnowledgeBaseCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseIdempotencyResult: ...

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

    async def create_document_version_idempotent(
        self,
        command: KnowledgeDocumentVersionCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult: ...

    async def update_document_version_idempotent(
        self,
        command: KnowledgeDocumentVersionCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult: ...

    async def reindex_document(
        self,
        command: KnowledgeDocumentReindex,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionResult: ...

    async def reindex_document_idempotent(
        self,
        command: KnowledgeDocumentReindex,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult: ...

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

    async def tombstone_document_idempotent(
        self,
        command: KnowledgeDocumentTombstone,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentIdempotencyResult: ...

    async def tombstone_base(
        self,
        kb_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord: ...

    async def tombstone_base_idempotent(
        self,
        command: KnowledgeBaseTombstone,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseIdempotencyResult: ...

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

    def __init__(
        self,
        scope_id: str,
        *,
        document_max_bytes: int = DEFAULT_KNOWLEDGE_DOCUMENT_MAX_BYTES,
    ) -> None:
        self._scope_id = validate_scope_id(scope_id)
        self._document_max_bytes = validate_document_max_bytes(document_max_bytes)
        self._bases: dict[str, KnowledgeBaseRecord] = {}
        self._documents: dict[str, KnowledgeDocumentRecord] = {}
        self._versions: dict[str, KnowledgeDocumentVersionRecord] = {}
        self._chunks: dict[str, KnowledgeChunkRecord] = {}
        self._idempotency: dict[tuple[KnowledgeOperation, str], KnowledgeIdempotencyRecord] = {}
        self._lock = asyncio.Lock()

    @property
    def scope_id(self) -> str:
        return self._scope_id

    @property
    def document_max_bytes(self) -> int:
        return self._document_max_bytes

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
        mime_type: str,
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
                and version.mime_type == mime_type
                and version.status in _CURRENT_FINGERPRINT_STATUSES
            ):
                return version
        return None

    def _validate_document_size(self, content: str) -> None:
        if len(content.encode("utf-8")) > self._document_max_bytes:
            raise KnowledgeValidationError(
                KnowledgePublicCode.content_too_large,
                "Document content exceeds the configured size limit.",
            )

    def _create_base_locked(
        self,
        command: KnowledgeBaseCreate,
        timestamp: datetime,
    ) -> KnowledgeBaseRecord:
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

    def _prepare_idempotency_locked(
        self,
        command: KnowledgeIdempotencyBegin,
        expected_operation: KnowledgeOperation,
    ) -> tuple[KnowledgeIdempotencyRecord | None, str | None]:
        if command.operation is not expected_operation:
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Idempotency operation does not match the Knowledge mutation.",
            )
        key = (command.operation, command.idempotency_key)
        existing = self._idempotency.get(key)
        if existing is not None:
            if existing.request_fingerprint != command.request_fingerprint:
                raise KnowledgeConflict(
                    KnowledgePublicCode.idempotency_key_reused,
                    "Idempotency key was already used for different input.",
                )
            return existing, None
        ledger_id = command.ledger_id
        if ledger_id is not None:
            if any(record.id == ledger_id for record in self._idempotency.values()):
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_conflict,
                    "Knowledge idempotency identifier already exists.",
                )
            return None, ledger_id
        ledger_id = new_knowledge_idempotency_id()
        while any(record.id == ledger_id for record in self._idempotency.values()):
            ledger_id = new_knowledge_idempotency_id()
        return None, ledger_id

    def _record_idempotency_locked(
        self,
        command: KnowledgeIdempotencyBegin,
        ledger_id: str,
        *,
        resource_kind: KnowledgeResourceKind,
        resource_id: str,
        document_version_id: str | None,
        timestamp: datetime,
    ) -> KnowledgeIdempotencyRecord:
        record = KnowledgeIdempotencyRecord(
            id=ledger_id,
            scope_id=self._scope_id,
            operation=command.operation,
            idempotency_key=command.idempotency_key,
            request_fingerprint=command.request_fingerprint,
            resource_kind=resource_kind,
            resource_id=resource_id,
            document_version_id=document_version_id,
            job_id=None,
            created_at=timestamp,
            updated_at=timestamp,
        )
        self._idempotency[(record.operation, record.idempotency_key)] = record
        return record

    @staticmethod
    def _validate_ledger_resource(
        record: KnowledgeIdempotencyRecord,
        *,
        operation: KnowledgeOperation,
        resource_kind: KnowledgeResourceKind,
        resource_id: str,
        document_version_id: str | None,
    ) -> None:
        if (
            record.operation is not operation
            or record.resource_kind is not resource_kind
            or record.resource_id != resource_id
            or record.document_version_id != document_version_id
        ):
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge idempotency ledger is inconsistent.",
            )

    def _base_from_ledger_locked(
        self,
        record: KnowledgeIdempotencyRecord,
        operation: KnowledgeOperation,
        *,
        expected_kb_id: str | None = None,
    ) -> KnowledgeBaseRecord:
        base = self._bases.get(record.resource_id)
        if base is None or (expected_kb_id is not None and base.id != expected_kb_id):
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge idempotency ledger is inconsistent.",
            )
        self._validate_ledger_resource(
            record,
            operation=operation,
            resource_kind=KnowledgeResourceKind.base,
            resource_id=base.id,
            document_version_id=None,
        )
        return _copy(base)

    def _document_from_ledger_locked(
        self,
        record: KnowledgeIdempotencyRecord,
        operation: KnowledgeOperation,
        *,
        expected_kb_id: str,
        expected_document_id: str,
    ) -> KnowledgeDocumentRecord:
        document = self._documents.get(record.resource_id)
        if (
            document is None
            or document.kb_id != expected_kb_id
            or document.id != expected_document_id
        ):
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge idempotency ledger is inconsistent.",
            )
        self._validate_ledger_resource(
            record,
            operation=operation,
            resource_kind=KnowledgeResourceKind.document,
            resource_id=document.id,
            document_version_id=None,
        )
        return _copy(document)

    def _document_version_from_ledger_locked(
        self,
        record: KnowledgeIdempotencyRecord,
        operation: KnowledgeOperation,
        *,
        expected_kb_id: str,
        expected_document_id: str | None,
    ) -> KnowledgeDocumentVersionResult:
        document = self._documents.get(record.resource_id)
        version = (
            None
            if record.document_version_id is None
            else self._versions.get(record.document_version_id)
        )
        if (
            document is None
            or version is None
            or document.kb_id != expected_kb_id
            or version.kb_id != expected_kb_id
            or version.document_id != document.id
            or (expected_document_id is not None and document.id != expected_document_id)
        ):
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge idempotency ledger is inconsistent.",
            )
        self._validate_ledger_resource(
            record,
            operation=operation,
            resource_kind=KnowledgeResourceKind.document,
            resource_id=document.id,
            document_version_id=version.id,
        )
        return KnowledgeDocumentVersionResult(
            document=_copy(document),
            version=_copy(version),
            reused=True,
        )

    def _create_document_version_locked(
        self,
        command: KnowledgeDocumentVersionCreate,
        timestamp: datetime,
        *,
        enforce_content_limit: bool = True,
    ) -> KnowledgeDocumentVersionResult:
        base = self._base_locked(command.kb_id)
        if base.status is not KnowledgeBaseStatus.active:
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_base_deleted,
                "Knowledge Base is deleted.",
            )
        digest = content_sha256(command.content)
        if enforce_content_limit:
            self._validate_document_size(command.content)
        fingerprint = index_fingerprint(
            digest,
            command.source_type,
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
            reusable = self._find_current_fingerprint_locked(
                document,
                fingerprint,
                command.mime_type,
            )
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
            target_chars=command.target_chars,
            overlap_chars=command.overlap_chars,
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
            return self._create_base_locked(command, timestamp)

    async def create_base_idempotent(
        self,
        command: KnowledgeBaseCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseIdempotencyResult:
        timestamp = _timestamp(now)
        async with self._lock:
            existing, ledger_id = self._prepare_idempotency_locked(
                idempotency,
                KnowledgeOperation.create_base,
            )
            if existing is not None:
                return KnowledgeBaseIdempotencyResult(
                    resource=self._base_from_ledger_locked(
                        existing,
                        KnowledgeOperation.create_base,
                    ),
                    ledger=_copy(existing),
                    replayed=True,
                )
            assert ledger_id is not None
            base = self._create_base_locked(command, timestamp)
            ledger = self._record_idempotency_locked(
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.base,
                resource_id=base.id,
                document_version_id=None,
                timestamp=timestamp,
            )
            self._validate_ledger_resource(
                ledger,
                operation=KnowledgeOperation.create_base,
                resource_kind=KnowledgeResourceKind.base,
                resource_id=base.id,
                document_version_id=None,
            )
            return KnowledgeBaseIdempotencyResult(
                resource=base,
                ledger=_copy(ledger),
                replayed=False,
            )

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

    async def create_document_version_idempotent(
        self,
        command: KnowledgeDocumentVersionCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult:
        if command.document_id is not None:
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Create document mutation cannot target an existing document.",
            )
        timestamp = _timestamp(now)
        async with self._lock:
            existing, ledger_id = self._prepare_idempotency_locked(
                idempotency,
                KnowledgeOperation.create_document,
            )
            if existing is not None:
                return KnowledgeDocumentVersionIdempotencyResult(
                    resource=self._document_version_from_ledger_locked(
                        existing,
                        KnowledgeOperation.create_document,
                        expected_kb_id=command.kb_id,
                        expected_document_id=None,
                    ),
                    ledger=_copy(existing),
                    replayed=True,
                )
            assert ledger_id is not None
            resource = self._create_document_version_locked(command, timestamp)
            ledger = self._record_idempotency_locked(
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
                timestamp=timestamp,
            )
            self._validate_ledger_resource(
                ledger,
                operation=KnowledgeOperation.create_document,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
            )
            return KnowledgeDocumentVersionIdempotencyResult(
                resource=resource,
                ledger=_copy(ledger),
                replayed=False,
            )

    async def update_document_version_idempotent(
        self,
        command: KnowledgeDocumentVersionCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult:
        if command.document_id is None:
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Update document mutation must target an existing document.",
            )
        timestamp = _timestamp(now)
        async with self._lock:
            existing, ledger_id = self._prepare_idempotency_locked(
                idempotency,
                KnowledgeOperation.update_document,
            )
            if existing is not None:
                return KnowledgeDocumentVersionIdempotencyResult(
                    resource=self._document_version_from_ledger_locked(
                        existing,
                        KnowledgeOperation.update_document,
                        expected_kb_id=command.kb_id,
                        expected_document_id=command.document_id,
                    ),
                    ledger=_copy(existing),
                    replayed=True,
                )
            assert ledger_id is not None
            resource = self._create_document_version_locked(command, timestamp)
            ledger = self._record_idempotency_locked(
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
                timestamp=timestamp,
            )
            self._validate_ledger_resource(
                ledger,
                operation=KnowledgeOperation.update_document,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
            )
            return KnowledgeDocumentVersionIdempotencyResult(
                resource=resource,
                ledger=_copy(ledger),
                replayed=False,
            )

    def _reindex_document_locked(
        self,
        command: KnowledgeDocumentReindex,
        timestamp: datetime,
    ) -> KnowledgeDocumentVersionResult:
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
        source_type = knowledge_source_type_from_mime_type(active.mime_type)
        return self._create_document_version_locked(
            KnowledgeDocumentVersionCreate(
                kb_id=base.id,
                document_id=document.id,
                title=document.title,
                source_type=source_type,
                source_uri=document.source_uri,
                content=active.content,
                mime_type=active.mime_type,
                chunking_version=command.chunking_version,
                target_chars=command.target_chars,
                overlap_chars=command.overlap_chars,
                document_version_id=command.document_version_id,
            ),
            timestamp,
            enforce_content_limit=False,
        )

    async def reindex_document(
        self,
        command: KnowledgeDocumentReindex,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionResult:
        timestamp = _timestamp(now)
        async with self._lock:
            return self._reindex_document_locked(command, timestamp)

    async def reindex_document_idempotent(
        self,
        command: KnowledgeDocumentReindex,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult:
        timestamp = _timestamp(now)
        async with self._lock:
            existing, ledger_id = self._prepare_idempotency_locked(
                idempotency,
                KnowledgeOperation.reindex_document,
            )
            if existing is not None:
                return KnowledgeDocumentVersionIdempotencyResult(
                    resource=self._document_version_from_ledger_locked(
                        existing,
                        KnowledgeOperation.reindex_document,
                        expected_kb_id=command.kb_id,
                        expected_document_id=command.document_id,
                    ),
                    ledger=_copy(existing),
                    replayed=True,
                )
            assert ledger_id is not None
            resource = self._reindex_document_locked(command, timestamp)
            ledger = self._record_idempotency_locked(
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
                timestamp=timestamp,
            )
            self._validate_ledger_resource(
                ledger,
                operation=KnowledgeOperation.reindex_document,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
            )
            return KnowledgeDocumentVersionIdempotencyResult(
                resource=resource,
                ledger=_copy(ledger),
                replayed=False,
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
        ingest_job_id: str,
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
        if version.ingest_job_id != ingest_job_id:
            return _copy(version)
        if version.status in {
            KnowledgeVersionStatus.failed,
            KnowledgeVersionStatus.cancelled,
            KnowledgeVersionStatus.deleted,
            KnowledgeVersionStatus.purged,
        }:
            return _copy(version)
        target_was_active = (
            version.status is KnowledgeVersionStatus.active
            and document.active_version_id == version.id
        )
        self._delete_version_chunks_locked(version.id)
        version = replace(
            version,
            status=status,
            error_kind=error_kind,
            error_message=error_message,
        )
        self._versions[version.id] = version
        if target_was_active:
            desired_was_target = document.desired_version_id == version.id
            has_newer_desired = document.desired_version_id is not None and not desired_was_target
            predecessors = [
                candidate
                for candidate in self._versions.values()
                if candidate.kb_id == base.id
                and candidate.document_id == document.id
                and candidate.id != version.id
                and candidate.status is KnowledgeVersionStatus.superseded
                and candidate.activated_at is not None
            ]
            predecessor = max(
                predecessors,
                key=lambda candidate: (
                    candidate.activated_at or candidate.created_at,
                    candidate.version,
                    candidate.id,
                ),
                default=None,
            )
            if predecessor is None:
                if has_newer_desired:
                    document = replace(
                        document,
                        status=KnowledgeDocumentStatus.pending,
                        active_version_id=None,
                        last_error_kind=None,
                        last_error_message=None,
                        updated_at=timestamp,
                    )
                else:
                    document = replace(
                        document,
                        status=KnowledgeDocumentStatus.failed,
                        active_version_id=None,
                        desired_version_id=None,
                        last_error_kind=error_kind,
                        last_error_message=error_message,
                        updated_at=timestamp,
                    )
            else:
                predecessor = replace(
                    predecessor,
                    status=KnowledgeVersionStatus.active,
                    error_kind=None,
                    error_message=None,
                )
                self._versions[predecessor.id] = predecessor
                document = replace(
                    document,
                    status=KnowledgeDocumentStatus.active,
                    active_version_id=predecessor.id,
                    desired_version_id=(
                        predecessor.id if desired_was_target else document.desired_version_id
                    ),
                    last_error_kind=None,
                    last_error_message=None,
                    updated_at=timestamp,
                )
            self._documents[document.id] = document
        elif document.active_version_id is None and document.desired_version_id == version.id:
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
                ingest_job_id=command.ingest_job_id,
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
                ingest_job_id=command.ingest_job_id,
                status=KnowledgeVersionStatus.cancelled,
                error_kind=command.error_kind,
                error_message=command.error_message,
                timestamp=timestamp,
            )

    def _tombstone_document_locked(
        self,
        kb_id: str,
        document_id: str,
        timestamp: datetime,
    ) -> KnowledgeDocumentRecord:
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

    async def tombstone_document(
        self,
        kb_id: str,
        document_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentRecord:
        timestamp = _timestamp(now)
        async with self._lock:
            return self._tombstone_document_locked(kb_id, document_id, timestamp)

    async def tombstone_document_idempotent(
        self,
        command: KnowledgeDocumentTombstone,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentIdempotencyResult:
        timestamp = _timestamp(now)
        async with self._lock:
            existing, ledger_id = self._prepare_idempotency_locked(
                idempotency,
                KnowledgeOperation.delete_document,
            )
            if existing is not None:
                return KnowledgeDocumentIdempotencyResult(
                    resource=self._document_from_ledger_locked(
                        existing,
                        KnowledgeOperation.delete_document,
                        expected_kb_id=command.kb_id,
                        expected_document_id=command.document_id,
                    ),
                    ledger=_copy(existing),
                    replayed=True,
                )
            assert ledger_id is not None
            document = self._tombstone_document_locked(
                command.kb_id,
                command.document_id,
                timestamp,
            )
            ledger = self._record_idempotency_locked(
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=document.id,
                document_version_id=None,
                timestamp=timestamp,
            )
            self._validate_ledger_resource(
                ledger,
                operation=KnowledgeOperation.delete_document,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=document.id,
                document_version_id=None,
            )
            return KnowledgeDocumentIdempotencyResult(
                resource=document,
                ledger=_copy(ledger),
                replayed=False,
            )

    def _tombstone_base_locked(
        self,
        kb_id: str,
        timestamp: datetime,
    ) -> KnowledgeBaseRecord:
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

    async def tombstone_base(
        self,
        kb_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord:
        timestamp = _timestamp(now)
        async with self._lock:
            return self._tombstone_base_locked(kb_id, timestamp)

    async def tombstone_base_idempotent(
        self,
        command: KnowledgeBaseTombstone,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseIdempotencyResult:
        timestamp = _timestamp(now)
        async with self._lock:
            existing, ledger_id = self._prepare_idempotency_locked(
                idempotency,
                KnowledgeOperation.delete_base,
            )
            if existing is not None:
                return KnowledgeBaseIdempotencyResult(
                    resource=self._base_from_ledger_locked(
                        existing,
                        KnowledgeOperation.delete_base,
                        expected_kb_id=command.kb_id,
                    ),
                    ledger=_copy(existing),
                    replayed=True,
                )
            assert ledger_id is not None
            base = self._tombstone_base_locked(command.kb_id, timestamp)
            ledger = self._record_idempotency_locked(
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.base,
                resource_id=base.id,
                document_version_id=None,
                timestamp=timestamp,
            )
            self._validate_ledger_resource(
                ledger,
                operation=KnowledgeOperation.delete_base,
                resource_kind=KnowledgeResourceKind.base,
                resource_id=base.id,
                document_version_id=None,
            )
            return KnowledgeBaseIdempotencyResult(
                resource=base,
                ledger=_copy(ledger),
                replayed=False,
            )

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
                if version is None or version.document_id != record.resource_id:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.knowledge_conflict,
                        "Knowledge idempotency ledger is inconsistent.",
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


_PG_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")
_PG_BASE_COLUMNS = (
    "id, scope_id, name, description, embedding_model, embedding_dim, status, "
    "created_at, updated_at, deleted_at"
)
_PG_DOCUMENT_COLUMNS = (
    "id, scope_id, kb_id, title, source_type, source_uri, status, desired_version_id, "
    "active_version_id, last_error_kind, last_error_message, created_at, updated_at, deleted_at"
)
_PG_VERSION_COLUMNS = (
    "id, scope_id, kb_id, document_id, version, content, content_sha256, index_fingerprint, "
    "mime_type, chunking_version, target_chars, overlap_chars, ingest_job_id, status, "
    "error_kind, error_message, created_at, activated_at, deleted_at, purged_at"
)
_PG_CHUNK_COLUMNS = (
    "id, scope_id, kb_id, document_id, document_version_id, ordinal, text, char_start, "
    "char_end, content_hash, heading_path, metadata, model, dim, embedding::text AS embedding, "
    "created_at"
)
_PG_IDEMPOTENCY_COLUMNS = (
    "id, scope_id, operation, idempotency_key, request_fingerprint, resource_kind, "
    "resource_id, document_version_id, job_id, created_at, updated_at"
)


def _pg_base_record(row: Mapping[Any, Any]) -> KnowledgeBaseRecord:
    return KnowledgeBaseRecord(
        id=str(row["id"]),
        scope_id=str(row["scope_id"]),
        name=str(row["name"]),
        description=None if row["description"] is None else str(row["description"]),
        embedding_model=str(row["embedding_model"]),
        embedding_dim=int(row["embedding_dim"]),
        status=KnowledgeBaseStatus(str(row["status"])),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        deleted_at=row["deleted_at"],
    )


def _pg_document_record(row: Mapping[Any, Any]) -> KnowledgeDocumentRecord:
    return KnowledgeDocumentRecord(
        id=str(row["id"]),
        scope_id=str(row["scope_id"]),
        kb_id=str(row["kb_id"]),
        title=str(row["title"]),
        source_type=KnowledgeSourceType(str(row["source_type"])),
        source_uri=None if row["source_uri"] is None else str(row["source_uri"]),
        status=KnowledgeDocumentStatus(str(row["status"])),
        desired_version_id=(
            None if row["desired_version_id"] is None else str(row["desired_version_id"])
        ),
        active_version_id=(
            None if row["active_version_id"] is None else str(row["active_version_id"])
        ),
        last_error_kind=(None if row["last_error_kind"] is None else str(row["last_error_kind"])),
        last_error_message=(
            None if row["last_error_message"] is None else str(row["last_error_message"])
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        deleted_at=row["deleted_at"],
    )


def _pg_version_record(row: Mapping[Any, Any]) -> KnowledgeDocumentVersionRecord:
    return KnowledgeDocumentVersionRecord(
        id=str(row["id"]),
        scope_id=str(row["scope_id"]),
        kb_id=str(row["kb_id"]),
        document_id=str(row["document_id"]),
        version=int(row["version"]),
        content=None if row["content"] is None else str(row["content"]),
        content_sha256=str(row["content_sha256"]),
        index_fingerprint=str(row["index_fingerprint"]),
        mime_type=str(row["mime_type"]),
        chunking_version=str(row["chunking_version"]),
        target_chars=int(row["target_chars"]),
        overlap_chars=int(row["overlap_chars"]),
        ingest_job_id=None if row["ingest_job_id"] is None else str(row["ingest_job_id"]),
        status=KnowledgeVersionStatus(str(row["status"])),
        error_kind=None if row["error_kind"] is None else str(row["error_kind"]),
        error_message=None if row["error_message"] is None else str(row["error_message"]),
        created_at=row["created_at"],
        activated_at=row["activated_at"],
        deleted_at=row["deleted_at"],
        purged_at=row["purged_at"],
    )


def _pg_embedding(value: object) -> tuple[float, ...]:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        if not isinstance(parsed, list | tuple):
            raise TypeError
        return tuple(float(item) for item in parsed)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise KnowledgeConflict(
            KnowledgePublicCode.knowledge_conflict,
            "Stored Knowledge data is inconsistent.",
        ) from exc


def _pg_chunk_record(row: Mapping[Any, Any]) -> KnowledgeChunkRecord:
    raw_heading_path = row["heading_path"]
    heading_path = (
        tuple(str(item) for item in raw_heading_path)
        if isinstance(raw_heading_path, list | tuple)
        else ()
    )
    raw_metadata = row["metadata"]
    metadata = copy.deepcopy(dict(raw_metadata)) if isinstance(raw_metadata, Mapping) else {}
    return KnowledgeChunkRecord(
        id=str(row["id"]),
        scope_id=str(row["scope_id"]),
        kb_id=str(row["kb_id"]),
        document_id=str(row["document_id"]),
        document_version_id=str(row["document_version_id"]),
        ordinal=int(row["ordinal"]),
        text=str(row["text"]),
        char_start=int(row["char_start"]),
        char_end=int(row["char_end"]),
        content_hash=str(row["content_hash"]),
        heading_path=heading_path,
        metadata=metadata,
        model=str(row["model"]),
        dim=int(row["dim"]),
        embedding=_pg_embedding(row["embedding"]),
        created_at=row["created_at"],
    )


def _pg_idempotency_record(row: Mapping[Any, Any]) -> KnowledgeIdempotencyRecord:
    return KnowledgeIdempotencyRecord(
        id=str(row["id"]),
        scope_id=str(row["scope_id"]),
        operation=KnowledgeOperation(str(row["operation"])),
        idempotency_key=str(row["idempotency_key"]),
        request_fingerprint=str(row["request_fingerprint"]),
        resource_kind=KnowledgeResourceKind(str(row["resource_kind"])),
        resource_id=str(row["resource_id"]),
        document_version_id=(
            None if row["document_version_id"] is None else str(row["document_version_id"])
        ),
        job_id=None if row["job_id"] is None else str(row["job_id"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _pg_idempotency_lock_id(
    scope_id: str,
    operation: KnowledgeOperation,
    idempotency_key: str,
) -> int:
    encoded = json.dumps(
        [scope_id, operation.value, idempotency_key],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return int.from_bytes(hashlib.sha256(encoded).digest()[:8], byteorder="big", signed=True)


def _pg_integrity_error_code(exc: IntegrityError) -> KnowledgePublicCode:
    original = getattr(exc, "orig", None)
    diagnostic = getattr(original, "diag", None)
    constraint = getattr(diagnostic, "constraint_name", None)
    if constraint == "uq_kb_scope_active_name":
        return KnowledgePublicCode.knowledge_base_name_conflict
    return KnowledgePublicCode.knowledge_conflict


def _pg_raise_integrity_error(code: KnowledgePublicCode) -> NoReturn:
    if code is KnowledgePublicCode.knowledge_base_name_conflict:
        raise KnowledgeConflict(
            code,
            "An active Knowledge Base already uses this name.",
        )
    raise KnowledgeConflict(
        code,
        "Knowledge resource state conflicts with this operation.",
    )


_PG_INFRASTRUCTURE_ERRORS = (
    DBAPIError,
    DisconnectionError,
    SQLAlchemyTimeoutError,
)


def _pg_is_exhausted_disconnect(exc: InvalidRequestError) -> bool:
    return type(exc) is InvalidRequestError and exc.args == ("This connection is closed",)


def _pg_error_boundary[**P, R](
    operation: Callable[P, Awaitable[R]],
) -> Callable[P, Awaitable[R]]:
    @wraps(operation)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        conflict_code: KnowledgePublicCode | None = None
        storage_failure = False
        try:
            return await operation(*args, **kwargs)
        except KnowledgeStorageError:
            storage_failure = True
        except IntegrityError as exc:
            conflict_code = _pg_integrity_error_code(exc)
        except _PG_INFRASTRUCTURE_ERRORS:
            storage_failure = True
        except InvalidRequestError as exc:
            if not _pg_is_exhausted_disconnect(exc):
                raise
            storage_failure = True

        del args, kwargs
        if conflict_code is not None:
            _pg_raise_integrity_error(conflict_code)
        if storage_failure:
            raise KnowledgeStorageError()
        raise AssertionError("unreachable Postgres error boundary state")

    return wrapped


def _pg_harden_engine_logging(engine: AsyncEngine) -> None:
    sync_engine = engine.sync_engine
    sync_engine.hide_parameters = True
    engine.echo = False

    # echo=False removes SQLAlchemy's echo adapter, but a DEBUG-configured
    # sqlalchemy.engine logger can still expose result rows. Gate only this
    # engine at WARNING without changing unrelated application or engine loggers.
    instance_logger = logging.getLogger(
        f"sqlalchemy.engine.Engine.keel_knowledge_{id(sync_engine):x}"
    )
    instance_logger.setLevel(logging.WARNING)
    sync_engine.logger = instance_logger


class PostgresKnowledgeStore:
    """Production scope-bound Knowledge lifecycle store over Postgres."""

    def __init__(
        self,
        engine: AsyncEngine,
        scope_id: str,
        *,
        document_max_bytes: int = DEFAULT_KNOWLEDGE_DOCUMENT_MAX_BYTES,
    ) -> None:
        if not isinstance(engine, AsyncEngine):
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Knowledge database engine is invalid.",
            )
        _pg_harden_engine_logging(engine)
        self._engine = engine
        self._scope_id = validate_scope_id(scope_id)
        self._document_max_bytes = validate_document_max_bytes(document_max_bytes)

    @property
    def scope_id(self) -> str:
        return self._scope_id

    @property
    def document_max_bytes(self) -> int:
        return self._document_max_bytes

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncConnection]:
        async with self._engine.begin() as conn:
            await conn.execute(_PG_SET_SCOPE, {"scope": self._scope_id})
            yield conn

    async def _get_base_tx(
        self,
        conn: AsyncConnection,
        kb_id: str,
        *,
        lock: str = "",
    ) -> KnowledgeBaseRecord | None:
        base_id = validate_knowledge_base_id(kb_id)
        suffix = " FOR UPDATE" if lock == "update" else " FOR SHARE" if lock == "share" else ""
        row = (
            (
                await conn.execute(
                    text(
                        f"SELECT {_PG_BASE_COLUMNS} FROM knowledge_bases "
                        f"WHERE scope_id = :scope AND id = :kb{suffix}"
                    ),
                    {"scope": self._scope_id, "kb": base_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _pg_base_record(row)

    async def _require_base_tx(
        self,
        conn: AsyncConnection,
        kb_id: str,
        *,
        lock: str = "",
    ) -> KnowledgeBaseRecord:
        record = await self._get_base_tx(conn, kb_id, lock=lock)
        if record is None:
            raise KnowledgeNotFound(
                KnowledgePublicCode.knowledge_base_not_found,
                "Knowledge Base was not found.",
            )
        return record

    async def _get_document_tx(
        self,
        conn: AsyncConnection,
        kb_id: str,
        document_id: str,
        *,
        lock: bool = False,
    ) -> KnowledgeDocumentRecord | None:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        suffix = " FOR UPDATE" if lock else ""
        row = (
            (
                await conn.execute(
                    text(
                        f"SELECT {_PG_DOCUMENT_COLUMNS} FROM kb_documents "
                        "WHERE scope_id = :scope AND kb_id = :kb AND id = :document"
                        f"{suffix}"
                    ),
                    {"scope": self._scope_id, "kb": base_id, "document": doc_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _pg_document_record(row)

    async def _get_document_by_id_tx(
        self,
        conn: AsyncConnection,
        document_id: str,
        *,
        lock: bool = False,
    ) -> KnowledgeDocumentRecord | None:
        doc_id = validate_knowledge_document_id(document_id)
        suffix = " FOR UPDATE" if lock else ""
        row = (
            (
                await conn.execute(
                    text(
                        f"SELECT {_PG_DOCUMENT_COLUMNS} FROM kb_documents "
                        f"WHERE scope_id = :scope AND id = :document{suffix}"
                    ),
                    {"scope": self._scope_id, "document": doc_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _pg_document_record(row)

    async def _require_document_tx(
        self,
        conn: AsyncConnection,
        kb_id: str,
        document_id: str,
        *,
        lock: bool = False,
    ) -> KnowledgeDocumentRecord:
        record = await self._get_document_tx(conn, kb_id, document_id, lock=lock)
        if record is None:
            raise KnowledgeNotFound(
                KnowledgePublicCode.knowledge_document_not_found,
                "Knowledge document was not found.",
            )
        return record

    async def _get_version_tx(
        self,
        conn: AsyncConnection,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        *,
        lock: bool = False,
    ) -> KnowledgeDocumentVersionRecord | None:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        version_id = validate_knowledge_version_id(document_version_id)
        suffix = " FOR UPDATE" if lock else ""
        row = (
            (
                await conn.execute(
                    text(
                        f"SELECT {_PG_VERSION_COLUMNS} FROM kb_document_versions "
                        "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document "
                        f"AND id = :version{suffix}"
                    ),
                    {
                        "scope": self._scope_id,
                        "kb": base_id,
                        "document": doc_id,
                        "version": version_id,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _pg_version_record(row)

    async def _require_version_tx(
        self,
        conn: AsyncConnection,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        *,
        lock: bool = False,
    ) -> KnowledgeDocumentVersionRecord:
        record = await self._get_version_tx(
            conn,
            kb_id,
            document_id,
            document_version_id,
            lock=lock,
        )
        if record is None:
            raise KnowledgeNotFound(
                KnowledgePublicCode.knowledge_version_not_found,
                "Knowledge document version was not found.",
            )
        return record

    async def _get_idempotency_tx(
        self,
        conn: AsyncConnection,
        operation: KnowledgeOperation,
        idempotency_key: str,
        *,
        lock: bool = False,
    ) -> KnowledgeIdempotencyRecord | None:
        suffix = " FOR UPDATE" if lock else ""
        row = (
            (
                await conn.execute(
                    text(
                        f"SELECT {_PG_IDEMPOTENCY_COLUMNS} FROM knowledge_idempotency "
                        "WHERE scope_id = :scope AND operation = :operation "
                        f"AND idempotency_key = :key{suffix}"
                    ),
                    {
                        "scope": self._scope_id,
                        "operation": operation.value,
                        "key": idempotency_key,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _pg_idempotency_record(row)

    async def _lock_idempotency_tx(
        self,
        conn: AsyncConnection,
        operation: KnowledgeOperation,
        idempotency_key: str,
    ) -> None:
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {
                "lock_id": _pg_idempotency_lock_id(
                    self._scope_id,
                    operation,
                    idempotency_key,
                )
            },
        )

    async def _prepare_idempotency_tx(
        self,
        conn: AsyncConnection,
        command: KnowledgeIdempotencyBegin,
        expected_operation: KnowledgeOperation,
    ) -> tuple[KnowledgeIdempotencyRecord | None, str | None]:
        if command.operation is not expected_operation:
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Idempotency operation does not match the Knowledge mutation.",
            )
        await self._lock_idempotency_tx(conn, command.operation, command.idempotency_key)
        existing = await self._get_idempotency_tx(
            conn,
            command.operation,
            command.idempotency_key,
            lock=True,
        )
        if existing is not None:
            if existing.request_fingerprint != command.request_fingerprint:
                raise KnowledgeConflict(
                    KnowledgePublicCode.idempotency_key_reused,
                    "Idempotency key was already used for different input.",
                )
            return existing, None
        ledger_id = command.ledger_id or new_knowledge_idempotency_id()
        while (
            await conn.execute(
                text("SELECT 1 FROM knowledge_idempotency WHERE scope_id = :scope AND id = :id"),
                {"scope": self._scope_id, "id": ledger_id},
            )
        ).one_or_none() is not None:
            if command.ledger_id is not None:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_conflict,
                    "Knowledge idempotency identifier already exists.",
                )
            ledger_id = new_knowledge_idempotency_id()
        return None, ledger_id

    async def _insert_idempotency_tx(
        self,
        conn: AsyncConnection,
        command: KnowledgeIdempotencyBegin,
        ledger_id: str,
        *,
        resource_kind: KnowledgeResourceKind,
        resource_id: str,
        document_version_id: str | None,
        timestamp: datetime,
    ) -> KnowledgeIdempotencyRecord:
        row = (
            (
                await conn.execute(
                    text(
                        "INSERT INTO knowledge_idempotency "
                        "(id, scope_id, operation, idempotency_key, request_fingerprint, "
                        "resource_kind, resource_id, document_version_id, created_at, updated_at) "
                        "VALUES (:id, :scope, :operation, :key, :fingerprint, :resource_kind, "
                        ":resource_id, :version_id, :now, :now) "
                        f"RETURNING {_PG_IDEMPOTENCY_COLUMNS}"
                    ),
                    {
                        "id": ledger_id,
                        "scope": self._scope_id,
                        "operation": command.operation.value,
                        "key": command.idempotency_key,
                        "fingerprint": command.request_fingerprint,
                        "resource_kind": resource_kind.value,
                        "resource_id": resource_id,
                        "version_id": document_version_id,
                        "now": timestamp,
                    },
                )
            )
            .mappings()
            .one()
        )
        return _pg_idempotency_record(row)

    @staticmethod
    def _validate_ledger_resource(
        record: KnowledgeIdempotencyRecord,
        *,
        operation: KnowledgeOperation,
        resource_kind: KnowledgeResourceKind,
        resource_id: str,
        document_version_id: str | None,
    ) -> None:
        if (
            record.operation is not operation
            or record.resource_kind is not resource_kind
            or record.resource_id != resource_id
            or record.document_version_id != document_version_id
        ):
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge idempotency ledger is inconsistent.",
            )

    async def _base_from_ledger_tx(
        self,
        conn: AsyncConnection,
        record: KnowledgeIdempotencyRecord,
        operation: KnowledgeOperation,
        *,
        expected_kb_id: str | None = None,
    ) -> KnowledgeBaseRecord:
        base = await self._get_base_tx(conn, record.resource_id)
        if base is None or (expected_kb_id is not None and base.id != expected_kb_id):
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge idempotency ledger is inconsistent.",
            )
        self._validate_ledger_resource(
            record,
            operation=operation,
            resource_kind=KnowledgeResourceKind.base,
            resource_id=base.id,
            document_version_id=None,
        )
        return base

    async def _document_from_ledger_tx(
        self,
        conn: AsyncConnection,
        record: KnowledgeIdempotencyRecord,
        operation: KnowledgeOperation,
        *,
        expected_kb_id: str,
        expected_document_id: str,
    ) -> KnowledgeDocumentRecord:
        document = await self._get_document_tx(
            conn,
            expected_kb_id,
            record.resource_id,
        )
        if document is None or document.id != expected_document_id:
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge idempotency ledger is inconsistent.",
            )
        self._validate_ledger_resource(
            record,
            operation=operation,
            resource_kind=KnowledgeResourceKind.document,
            resource_id=document.id,
            document_version_id=None,
        )
        return document

    async def _document_version_from_ledger_tx(
        self,
        conn: AsyncConnection,
        record: KnowledgeIdempotencyRecord,
        operation: KnowledgeOperation,
        *,
        expected_kb_id: str,
        expected_document_id: str | None,
    ) -> KnowledgeDocumentVersionResult:
        document = await self._get_document_tx(
            conn,
            expected_kb_id,
            record.resource_id,
        )
        version = (
            None
            if document is None or record.document_version_id is None
            else await self._get_version_tx(
                conn,
                expected_kb_id,
                document.id,
                record.document_version_id,
            )
        )
        if (
            document is None
            or version is None
            or (expected_document_id is not None and document.id != expected_document_id)
        ):
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_conflict,
                "Knowledge idempotency ledger is inconsistent.",
            )
        self._validate_ledger_resource(
            record,
            operation=operation,
            resource_kind=KnowledgeResourceKind.document,
            resource_id=document.id,
            document_version_id=version.id,
        )
        return KnowledgeDocumentVersionResult(
            document=document,
            version=version,
            reused=True,
        )

    def _validate_document_size(self, content: str) -> None:
        if len(content.encode("utf-8")) > self._document_max_bytes:
            raise KnowledgeValidationError(
                KnowledgePublicCode.content_too_large,
                "Document content exceeds the configured size limit.",
            )

    async def _create_base_tx(
        self,
        conn: AsyncConnection,
        command: KnowledgeBaseCreate,
        timestamp: datetime,
    ) -> KnowledgeBaseRecord:
        duplicate_name = (
            await conn.execute(
                text(
                    "SELECT 1 FROM knowledge_bases "
                    "WHERE scope_id = :scope AND name = :name AND status = 'active'"
                ),
                {"scope": self._scope_id, "name": command.name},
            )
        ).one_or_none()
        if duplicate_name is not None:
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_base_name_conflict,
                "An active Knowledge Base already uses this name.",
            )
        base_id = command.base_id or new_knowledge_base_id()
        while (
            await conn.execute(
                text("SELECT 1 FROM knowledge_bases WHERE scope_id = :scope AND id = :kb"),
                {"scope": self._scope_id, "kb": base_id},
            )
        ).one_or_none() is not None:
            if command.base_id is not None:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_conflict,
                    "Knowledge Base identifier already exists.",
                )
            base_id = new_knowledge_base_id()
        row = (
            (
                await conn.execute(
                    text(
                        "INSERT INTO knowledge_bases "
                        "(id, scope_id, name, description, embedding_model, embedding_dim, "
                        "status, created_at, updated_at) "
                        "VALUES (:id, :scope, :name, :description, :model, :dim, "
                        "'active', :now, :now) "
                        f"RETURNING {_PG_BASE_COLUMNS}"
                    ),
                    {
                        "id": base_id,
                        "scope": self._scope_id,
                        "name": command.name,
                        "description": command.description,
                        "model": command.embedding_model,
                        "dim": command.embedding_dim,
                        "now": timestamp,
                    },
                )
            )
            .mappings()
            .one()
        )
        return _pg_base_record(row)

    async def _find_current_fingerprint_tx(
        self,
        conn: AsyncConnection,
        document: KnowledgeDocumentRecord,
        fingerprint: str,
        mime_type: str,
    ) -> KnowledgeDocumentVersionRecord | None:
        if document.desired_version_id is None and document.active_version_id is None:
            return None
        row = (
            (
                await conn.execute(
                    text(
                        f"SELECT {_PG_VERSION_COLUMNS} FROM kb_document_versions "
                        "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document "
                        "AND (id = :desired OR id = :active) "
                        "AND index_fingerprint = :fingerprint AND mime_type = :mime_type "
                        "AND status IN ('pending', 'indexing', 'active') "
                        "ORDER BY CASE WHEN id = :desired THEN 0 ELSE 1 END LIMIT 1"
                    ),
                    {
                        "scope": self._scope_id,
                        "kb": document.kb_id,
                        "document": document.id,
                        "desired": document.desired_version_id,
                        "active": document.active_version_id,
                        "fingerprint": fingerprint,
                        "mime_type": mime_type,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _pg_version_record(row)

    async def _unique_document_id_tx(
        self,
        conn: AsyncConnection,
        requested: str | None,
    ) -> str:
        document_id = requested or new_knowledge_document_id()
        while (
            await conn.execute(
                text("SELECT 1 FROM kb_documents WHERE scope_id = :scope AND id = :document"),
                {"scope": self._scope_id, "document": document_id},
            )
        ).one_or_none() is not None:
            if requested is not None:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_conflict,
                    "Knowledge document identifier already exists.",
                )
            document_id = new_knowledge_document_id()
        return document_id

    async def _unique_version_id_tx(
        self,
        conn: AsyncConnection,
        requested: str | None,
    ) -> str:
        version_id = requested or new_knowledge_version_id()
        while (
            await conn.execute(
                text(
                    "SELECT 1 FROM kb_document_versions WHERE scope_id = :scope AND id = :version"
                ),
                {"scope": self._scope_id, "version": version_id},
            )
        ).one_or_none() is not None:
            if requested is not None:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_conflict,
                    "Knowledge document version identifier already exists.",
                )
            version_id = new_knowledge_version_id()
        return version_id

    async def _create_document_version_tx(
        self,
        conn: AsyncConnection,
        command: KnowledgeDocumentVersionCreate,
        timestamp: datetime,
        *,
        enforce_content_limit: bool = True,
    ) -> KnowledgeDocumentVersionResult:
        base = await self._require_base_tx(conn, command.kb_id, lock="share")
        if base.status is not KnowledgeBaseStatus.active:
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_base_deleted,
                "Knowledge Base is deleted.",
            )
        digest = content_sha256(command.content)
        if enforce_content_limit:
            self._validate_document_size(command.content)
        fingerprint = index_fingerprint(
            digest,
            command.source_type,
            command.chunking_version,
            base.embedding_model,
            base.embedding_dim,
            command.target_chars,
            command.overlap_chars,
        )

        is_new_document = command.document_id is None
        if is_new_document:
            document_id = await self._unique_document_id_tx(conn, command.new_document_id)
            inserted_document = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO kb_documents "
                            "(id, scope_id, kb_id, title, source_type, source_uri, status, "
                            "desired_version_id, active_version_id, created_at, updated_at) "
                            "VALUES (:id, :scope, :kb, :title, :source_type, :source_uri, "
                            "'pending', NULL, NULL, :now, :now) "
                            f"RETURNING {_PG_DOCUMENT_COLUMNS}"
                        ),
                        {
                            "id": document_id,
                            "scope": self._scope_id,
                            "kb": base.id,
                            "title": command.title,
                            "source_type": command.source_type.value,
                            "source_uri": command.source_uri,
                            "now": timestamp,
                        },
                    )
                )
                .mappings()
                .one()
            )
            document = _pg_document_record(inserted_document)
        else:
            assert command.document_id is not None
            document = await self._require_document_tx(
                conn,
                base.id,
                command.document_id,
                lock=True,
            )
            if document.status is KnowledgeDocumentStatus.deleted:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_document_deleted,
                    "Knowledge document is deleted.",
                )
            reusable = await self._find_current_fingerprint_tx(
                conn,
                document,
                fingerprint,
                command.mime_type,
            )
            if reusable is not None:
                updated_row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE kb_documents SET title = :title, "
                                "source_type = :source_type, "
                                "source_uri = :source_uri, status = :status, "
                                "desired_version_id = :desired, last_error_kind = NULL, "
                                "last_error_message = NULL, updated_at = :now "
                                "WHERE scope_id = :scope AND kb_id = :kb AND id = :document "
                                f"RETURNING {_PG_DOCUMENT_COLUMNS}"
                            ),
                            {
                                "title": command.title,
                                "source_type": command.source_type.value,
                                "source_uri": command.source_uri,
                                "status": (
                                    KnowledgeDocumentStatus.active.value
                                    if document.active_version_id is not None
                                    else KnowledgeDocumentStatus.pending.value
                                ),
                                "desired": reusable.id,
                                "now": timestamp,
                                "scope": self._scope_id,
                                "kb": base.id,
                                "document": document.id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                return KnowledgeDocumentVersionResult(
                    document=_pg_document_record(updated_row),
                    version=reusable,
                    reused=True,
                )

        version_id = await self._unique_version_id_tx(conn, command.document_version_id)
        if is_new_document:
            version_number = 1
        else:
            maximum = await conn.scalar(
                text(
                    "SELECT max(version) FROM kb_document_versions "
                    "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document"
                ),
                {
                    "scope": self._scope_id,
                    "kb": base.id,
                    "document": document.id,
                },
            )
            version_number = int(maximum or 0) + 1
        version_row = (
            (
                await conn.execute(
                    text(
                        "INSERT INTO kb_document_versions "
                        "(id, scope_id, kb_id, document_id, version, content, content_sha256, "
                        "index_fingerprint, mime_type, chunking_version, target_chars, "
                        "overlap_chars, status, created_at) "
                        "VALUES (:id, :scope, :kb, :document, :number, :content, :content_hash, "
                        ":fingerprint, :mime_type, :chunking_version, :target_chars, "
                        ":overlap_chars, 'pending', :now) "
                        f"RETURNING {_PG_VERSION_COLUMNS}"
                    ),
                    {
                        "id": version_id,
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                        "number": version_number,
                        "content": command.content,
                        "content_hash": digest,
                        "fingerprint": fingerprint,
                        "mime_type": command.mime_type,
                        "chunking_version": command.chunking_version,
                        "target_chars": command.target_chars,
                        "overlap_chars": command.overlap_chars,
                        "now": timestamp,
                    },
                )
            )
            .mappings()
            .one()
        )
        document_row = (
            (
                await conn.execute(
                    text(
                        "UPDATE kb_documents SET title = :title, source_type = :source_type, "
                        "source_uri = :source_uri, status = :status, "
                        "desired_version_id = :desired, "
                        "last_error_kind = NULL, last_error_message = NULL, updated_at = :now "
                        "WHERE scope_id = :scope AND kb_id = :kb AND id = :document "
                        f"RETURNING {_PG_DOCUMENT_COLUMNS}"
                    ),
                    {
                        "title": command.title,
                        "source_type": command.source_type.value,
                        "source_uri": command.source_uri,
                        "status": (
                            KnowledgeDocumentStatus.active.value
                            if document.active_version_id is not None
                            else KnowledgeDocumentStatus.pending.value
                        ),
                        "desired": version_id,
                        "now": timestamp,
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                    },
                )
            )
            .mappings()
            .one()
        )
        return KnowledgeDocumentVersionResult(
            document=_pg_document_record(document_row),
            version=_pg_version_record(version_row),
            reused=False,
        )

    async def _reindex_document_tx(
        self,
        conn: AsyncConnection,
        command: KnowledgeDocumentReindex,
        timestamp: datetime,
    ) -> KnowledgeDocumentVersionResult:
        base = await self._require_base_tx(conn, command.kb_id, lock="share")
        if base.status is not KnowledgeBaseStatus.active:
            raise KnowledgeConflict(
                KnowledgePublicCode.knowledge_base_deleted,
                "Knowledge Base is deleted.",
            )
        document = await self._require_document_tx(
            conn,
            base.id,
            command.document_id,
            lock=True,
        )
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
        active = await self._require_version_tx(
            conn,
            base.id,
            document.id,
            document.active_version_id,
            lock=True,
        )
        if active.status is not KnowledgeVersionStatus.active or active.content is None:
            raise KnowledgeConflict(
                KnowledgePublicCode.no_active_version,
                "Knowledge document has no active version to reindex.",
            )
        source_type = knowledge_source_type_from_mime_type(active.mime_type)
        return await self._create_document_version_tx(
            conn,
            KnowledgeDocumentVersionCreate(
                kb_id=base.id,
                document_id=document.id,
                title=document.title,
                source_type=source_type,
                source_uri=document.source_uri,
                content=active.content,
                mime_type=active.mime_type,
                chunking_version=command.chunking_version,
                target_chars=command.target_chars,
                overlap_chars=command.overlap_chars,
                document_version_id=command.document_version_id,
            ),
            timestamp,
            enforce_content_limit=False,
        )

    @_pg_error_boundary
    async def create_base(
        self,
        command: KnowledgeBaseCreate,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            return await self._create_base_tx(conn, command, timestamp)

    @_pg_error_boundary
    async def create_base_idempotent(
        self,
        command: KnowledgeBaseCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseIdempotencyResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            existing, ledger_id = await self._prepare_idempotency_tx(
                conn,
                idempotency,
                KnowledgeOperation.create_base,
            )
            if existing is not None:
                return KnowledgeBaseIdempotencyResult(
                    resource=await self._base_from_ledger_tx(
                        conn,
                        existing,
                        KnowledgeOperation.create_base,
                    ),
                    ledger=existing,
                    replayed=True,
                )
            assert ledger_id is not None
            base = await self._create_base_tx(conn, command, timestamp)
            ledger = await self._insert_idempotency_tx(
                conn,
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.base,
                resource_id=base.id,
                document_version_id=None,
                timestamp=timestamp,
            )
            return KnowledgeBaseIdempotencyResult(
                resource=base,
                ledger=ledger,
                replayed=False,
            )

    @_pg_error_boundary
    async def list_bases(
        self,
        *,
        include_deleted: bool = False,
    ) -> list[KnowledgeBaseRecord]:
        status_filter = "" if include_deleted else " AND status = 'active'"
        async with self._transaction() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PG_BASE_COLUMNS} FROM knowledge_bases "
                            f"WHERE scope_id = :scope{status_filter} ORDER BY created_at, id"
                        ),
                        {"scope": self._scope_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_pg_base_record(row) for row in rows]

    @_pg_error_boundary
    async def get_base(self, kb_id: str) -> KnowledgeBaseRecord | None:
        validated = validate_knowledge_base_id(kb_id)
        async with self._transaction() as conn:
            return await self._get_base_tx(conn, validated)

    @_pg_error_boundary
    async def create_document_version(
        self,
        command: KnowledgeDocumentVersionCreate,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            return await self._create_document_version_tx(conn, command, timestamp)

    @_pg_error_boundary
    async def create_document_version_idempotent(
        self,
        command: KnowledgeDocumentVersionCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult:
        if command.document_id is not None:
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Create document mutation cannot target an existing document.",
            )
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            existing, ledger_id = await self._prepare_idempotency_tx(
                conn,
                idempotency,
                KnowledgeOperation.create_document,
            )
            if existing is not None:
                return KnowledgeDocumentVersionIdempotencyResult(
                    resource=await self._document_version_from_ledger_tx(
                        conn,
                        existing,
                        KnowledgeOperation.create_document,
                        expected_kb_id=command.kb_id,
                        expected_document_id=None,
                    ),
                    ledger=existing,
                    replayed=True,
                )
            assert ledger_id is not None
            resource = await self._create_document_version_tx(conn, command, timestamp)
            ledger = await self._insert_idempotency_tx(
                conn,
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
                timestamp=timestamp,
            )
            return KnowledgeDocumentVersionIdempotencyResult(
                resource=resource,
                ledger=ledger,
                replayed=False,
            )

    @_pg_error_boundary
    async def update_document_version_idempotent(
        self,
        command: KnowledgeDocumentVersionCreate,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult:
        if command.document_id is None:
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Update document mutation must target an existing document.",
            )
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            existing, ledger_id = await self._prepare_idempotency_tx(
                conn,
                idempotency,
                KnowledgeOperation.update_document,
            )
            if existing is not None:
                return KnowledgeDocumentVersionIdempotencyResult(
                    resource=await self._document_version_from_ledger_tx(
                        conn,
                        existing,
                        KnowledgeOperation.update_document,
                        expected_kb_id=command.kb_id,
                        expected_document_id=command.document_id,
                    ),
                    ledger=existing,
                    replayed=True,
                )
            assert ledger_id is not None
            resource = await self._create_document_version_tx(conn, command, timestamp)
            ledger = await self._insert_idempotency_tx(
                conn,
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
                timestamp=timestamp,
            )
            return KnowledgeDocumentVersionIdempotencyResult(
                resource=resource,
                ledger=ledger,
                replayed=False,
            )

    @_pg_error_boundary
    async def reindex_document(
        self,
        command: KnowledgeDocumentReindex,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            return await self._reindex_document_tx(conn, command, timestamp)

    @_pg_error_boundary
    async def reindex_document_idempotent(
        self,
        command: KnowledgeDocumentReindex,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionIdempotencyResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            existing, ledger_id = await self._prepare_idempotency_tx(
                conn,
                idempotency,
                KnowledgeOperation.reindex_document,
            )
            if existing is not None:
                return KnowledgeDocumentVersionIdempotencyResult(
                    resource=await self._document_version_from_ledger_tx(
                        conn,
                        existing,
                        KnowledgeOperation.reindex_document,
                        expected_kb_id=command.kb_id,
                        expected_document_id=command.document_id,
                    ),
                    ledger=existing,
                    replayed=True,
                )
            assert ledger_id is not None
            resource = await self._reindex_document_tx(conn, command, timestamp)
            ledger = await self._insert_idempotency_tx(
                conn,
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=resource.document.id,
                document_version_id=resource.version.id,
                timestamp=timestamp,
            )
            return KnowledgeDocumentVersionIdempotencyResult(
                resource=resource,
                ledger=ledger,
                replayed=False,
            )

    @_pg_error_boundary
    async def get_document(
        self,
        kb_id: str,
        document_id: str,
    ) -> KnowledgeDocumentRecord | None:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        async with self._transaction() as conn:
            return await self._get_document_tx(conn, base_id, doc_id)

    @_pg_error_boundary
    async def list_documents(
        self,
        kb_id: str,
        *,
        include_deleted: bool = False,
    ) -> list[KnowledgeDocumentRecord]:
        base_id = validate_knowledge_base_id(kb_id)
        status_filter = "" if include_deleted else " AND status <> 'deleted'"
        async with self._transaction() as conn:
            await self._require_base_tx(conn, base_id)
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PG_DOCUMENT_COLUMNS} FROM kb_documents "
                            "WHERE scope_id = :scope AND kb_id = :kb"
                            f"{status_filter} ORDER BY created_at, id"
                        ),
                        {"scope": self._scope_id, "kb": base_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_pg_document_record(row) for row in rows]

    @_pg_error_boundary
    async def get_version(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> KnowledgeDocumentVersionRecord | None:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        version_id = validate_knowledge_version_id(document_version_id)
        async with self._transaction() as conn:
            return await self._get_version_tx(conn, base_id, doc_id, version_id)

    @_pg_error_boundary
    async def list_versions(
        self,
        kb_id: str,
        document_id: str,
    ) -> list[KnowledgeDocumentVersionRecord]:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        async with self._transaction() as conn:
            await self._require_document_tx(conn, base_id, doc_id)
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PG_VERSION_COLUMNS} FROM kb_document_versions "
                            "WHERE scope_id = :scope AND kb_id = :kb "
                            "AND document_id = :document ORDER BY version"
                        ),
                        {
                            "scope": self._scope_id,
                            "kb": base_id,
                            "document": doc_id,
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [_pg_version_record(row) for row in rows]

    @_pg_error_boundary
    async def list_version_chunks(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> list[KnowledgeChunkRecord]:
        base_id = validate_knowledge_base_id(kb_id)
        doc_id = validate_knowledge_document_id(document_id)
        version_id = validate_knowledge_version_id(document_version_id)
        async with self._transaction() as conn:
            await self._require_version_tx(conn, base_id, doc_id, version_id)
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PG_CHUNK_COLUMNS} FROM kb_chunks "
                            "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document "
                            "AND document_version_id = :version ORDER BY ordinal"
                        ),
                        {
                            "scope": self._scope_id,
                            "kb": base_id,
                            "document": doc_id,
                            "version": version_id,
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [_pg_chunk_record(row) for row in rows]

    @staticmethod
    def _validate_job_id(job_id: object) -> str:
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
        return job_id

    @_pg_error_boundary
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
        validated_job_id = self._validate_job_id(job_id)
        async with self._transaction() as conn:
            base = await self._require_base_tx(conn, kb_id, lock="share")
            document = await self._require_document_tx(
                conn,
                base.id,
                document_id,
                lock=True,
            )
            version = await self._require_version_tx(
                conn,
                base.id,
                document.id,
                document_version_id,
                lock=True,
            )
            if version.ingest_job_id is not None and version.ingest_job_id != validated_job_id:
                raise KnowledgeConflict(
                    KnowledgePublicCode.idempotency_job_conflict,
                    "Knowledge version already has a different job.",
                )
            if version.ingest_job_id is None:
                row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE kb_document_versions SET ingest_job_id = :job_id "
                                "WHERE scope_id = :scope AND kb_id = :kb "
                                "AND document_id = :document AND id = :version "
                                f"RETURNING {_PG_VERSION_COLUMNS}"
                            ),
                            {
                                "job_id": validated_job_id,
                                "scope": self._scope_id,
                                "kb": base.id,
                                "document": document.id,
                                "version": version.id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                version = _pg_version_record(row)
            return version

    async def _delete_version_chunks_tx(
        self,
        conn: AsyncConnection,
        version: KnowledgeDocumentVersionRecord,
    ) -> int:
        rows = (
            await conn.execute(
                text(
                    "DELETE FROM kb_chunks WHERE scope_id = :scope AND kb_id = :kb "
                    "AND document_id = :document AND document_version_id = :version "
                    "RETURNING id"
                ),
                {
                    "scope": self._scope_id,
                    "kb": version.kb_id,
                    "document": version.document_id,
                    "version": version.id,
                },
            )
        ).all()
        return len(rows)

    async def _purge_version_tx(
        self,
        conn: AsyncConnection,
        version: KnowledgeDocumentVersionRecord,
        timestamp: datetime,
    ) -> tuple[KnowledgeDocumentVersionRecord, int, bool]:
        removed = await self._delete_version_chunks_tx(conn, version)
        changed = (
            version.status is not KnowledgeVersionStatus.purged
            or version.content is not None
            or removed > 0
        )
        if not changed:
            return version, removed, False
        row = (
            (
                await conn.execute(
                    text(
                        "UPDATE kb_document_versions SET content = NULL, status = 'purged', "
                        "deleted_at = COALESCE(deleted_at, :now), "
                        "purged_at = COALESCE(purged_at, :now) "
                        "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document "
                        "AND id = :version "
                        f"RETURNING {_PG_VERSION_COLUMNS}"
                    ),
                    {
                        "now": timestamp,
                        "scope": self._scope_id,
                        "kb": version.kb_id,
                        "document": version.document_id,
                        "version": version.id,
                    },
                )
            )
            .mappings()
            .one()
        )
        return _pg_version_record(row), removed, True

    @_pg_error_boundary
    async def mark_indexing(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeIndexingResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            base = await self._require_base_tx(conn, kb_id, lock="share")
            document = await self._require_document_tx(
                conn,
                base.id,
                document_id,
                lock=True,
            )
            version = await self._require_version_tx(
                conn,
                base.id,
                document.id,
                document_version_id,
                lock=True,
            )
            if (
                base.status is KnowledgeBaseStatus.deleted
                or document.status is KnowledgeDocumentStatus.deleted
            ):
                version, _, _ = await self._purge_version_tx(conn, version, timestamp)
                return KnowledgeIndexingResult(version=version, started=False, stale=False)
            if version.status in _IMMUTABLE_TERMINAL_VERSION_STATUSES:
                return KnowledgeIndexingResult(version=version, started=False, stale=False)
            if document.desired_version_id != version.id:
                await self._delete_version_chunks_tx(conn, version)
                row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE kb_document_versions SET status = 'superseded', "
                                "error_kind = NULL, error_message = NULL "
                                "WHERE scope_id = :scope AND kb_id = :kb "
                                "AND document_id = :document AND id = :version "
                                f"RETURNING {_PG_VERSION_COLUMNS}"
                            ),
                            {
                                "scope": self._scope_id,
                                "kb": base.id,
                                "document": document.id,
                                "version": version.id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                return KnowledgeIndexingResult(
                    version=_pg_version_record(row),
                    started=False,
                    stale=True,
                )
            if version.status is KnowledgeVersionStatus.indexing:
                return KnowledgeIndexingResult(version=version, started=False, stale=False)
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE kb_document_versions SET status = 'indexing', "
                            "error_kind = NULL, error_message = NULL "
                            "WHERE scope_id = :scope AND kb_id = :kb "
                            "AND document_id = :document AND id = :version "
                            f"RETURNING {_PG_VERSION_COLUMNS}"
                        ),
                        {
                            "scope": self._scope_id,
                            "kb": base.id,
                            "document": document.id,
                            "version": version.id,
                        },
                    )
                )
                .mappings()
                .one()
            )
            return KnowledgeIndexingResult(
                version=_pg_version_record(row),
                started=True,
                stale=False,
            )

    async def _available_chunk_id_tx(
        self,
        conn: AsyncConnection,
        requested: str | None,
        existing: KnowledgeChunkRecord | None,
        *,
        ordinal: int,
        version: KnowledgeDocumentVersionRecord,
    ) -> str:
        chunk_id = requested or (existing.id if existing is not None else new_knowledge_chunk_id())
        while True:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT id, kb_id, document_id, document_version_id, ordinal "
                            "FROM kb_chunks WHERE scope_id = :scope AND id = :id"
                        ),
                        {"scope": self._scope_id, "id": chunk_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return chunk_id
            if (
                str(row["kb_id"]) == version.kb_id
                and str(row["document_id"]) == version.document_id
                and str(row["document_version_id"]) == version.id
                and int(row["ordinal"]) == ordinal
            ):
                return chunk_id
            if requested is not None or existing is not None:
                raise KnowledgeConflict(
                    KnowledgePublicCode.knowledge_conflict,
                    "Knowledge chunk identifier already exists.",
                )
            chunk_id = new_knowledge_chunk_id()

    @_pg_error_boundary
    async def replace_version_chunks(
        self,
        command: KnowledgeChunkReplacement,
        *,
        now: datetime | None = None,
    ) -> KnowledgeChunkReplacementResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            base = await self._require_base_tx(conn, command.kb_id, lock="share")
            document = await self._require_document_tx(
                conn,
                base.id,
                command.document_id,
                lock=True,
            )
            version = await self._require_version_tx(
                conn,
                base.id,
                document.id,
                command.document_version_id,
                lock=True,
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
                    or content_sha256(chunk.text) != chunk.content_hash
                ):
                    raise KnowledgeConflict(
                        KnowledgePublicCode.chunk_write_rejected,
                        "Knowledge chunk offsets do not match the document version.",
                    )

            existing_rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PG_CHUNK_COLUMNS} FROM kb_chunks "
                            "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document "
                            "AND document_version_id = :version FOR UPDATE"
                        ),
                        {
                            "scope": self._scope_id,
                            "kb": base.id,
                            "document": document.id,
                            "version": version.id,
                        },
                    )
                )
                .mappings()
                .all()
            )
            existing_by_ordinal = {
                record.ordinal: record
                for record in (_pg_chunk_record(row) for row in existing_rows)
            }
            prepared: list[tuple[KnowledgeChunkWrite, str, datetime]] = []
            used_ids: set[str] = set()
            for chunk in command.chunks:
                existing = existing_by_ordinal.get(chunk.ordinal)
                chunk_id = await self._available_chunk_id_tx(
                    conn,
                    chunk.chunk_id,
                    existing,
                    ordinal=chunk.ordinal,
                    version=version,
                )
                if chunk_id in used_ids:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.knowledge_conflict,
                        "Knowledge chunk identifiers must be unique.",
                    )
                used_ids.add(chunk_id)
                prepared.append(
                    (
                        chunk,
                        chunk_id,
                        existing.created_at if existing is not None else timestamp,
                    )
                )

            desired_by_ordinal = {chunk.ordinal: chunk_id for chunk, chunk_id, _ in prepared}
            for ordinal, existing in existing_by_ordinal.items():
                desired_id = desired_by_ordinal.get(ordinal)
                if desired_id is not None and desired_id == existing.id:
                    continue
                await conn.execute(
                    text(
                        "DELETE FROM kb_chunks WHERE scope_id = :scope AND kb_id = :kb "
                        "AND document_id = :document AND document_version_id = :version "
                        "AND id = :id"
                    ),
                    {
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                        "version": version.id,
                        "id": existing.id,
                    },
                )

            for chunk, chunk_id, created_at in prepared:
                await conn.execute(
                    text(
                        "INSERT INTO kb_chunks "
                        "(id, scope_id, kb_id, document_id, document_version_id, ordinal, text, "
                        "char_start, char_end, content_hash, heading_path, metadata, model, dim, "
                        "embedding, created_at) "
                        "VALUES (:id, :scope, :kb, :document, :version, :ordinal, :text, "
                        ":char_start, :char_end, :content_hash, CAST(:heading_path AS jsonb), "
                        "CAST(:metadata AS jsonb), :model, :dim, CAST(:embedding AS vector), "
                        ":created_at) "
                        "ON CONFLICT (scope_id, document_version_id, ordinal) DO UPDATE SET "
                        "id = EXCLUDED.id, kb_id = EXCLUDED.kb_id, "
                        "document_id = EXCLUDED.document_id, text = EXCLUDED.text, "
                        "char_start = EXCLUDED.char_start, char_end = EXCLUDED.char_end, "
                        "content_hash = EXCLUDED.content_hash, "
                        "heading_path = EXCLUDED.heading_path, metadata = EXCLUDED.metadata, "
                        "model = EXCLUDED.model, dim = EXCLUDED.dim, "
                        "embedding = EXCLUDED.embedding, created_at = kb_chunks.created_at"
                    ),
                    {
                        "id": chunk_id,
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                        "version": version.id,
                        "ordinal": chunk.ordinal,
                        "text": chunk.text,
                        "char_start": chunk.char_start,
                        "char_end": chunk.char_end,
                        "content_hash": chunk.content_hash,
                        "heading_path": json.dumps(
                            list(chunk.heading_path),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        "metadata": json.dumps(
                            chunk.metadata,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            allow_nan=False,
                        ),
                        "model": chunk.model,
                        "dim": chunk.dim,
                        "embedding": json.dumps(
                            list(chunk.embedding),
                            separators=(",", ":"),
                            allow_nan=False,
                        ),
                        "created_at": created_at,
                    },
                )
            return KnowledgeChunkReplacementResult(
                document_version_id=version.id,
                chunk_count=len(prepared),
            )

    @_pg_error_boundary
    async def activate_version(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeActivationResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            base = await self._require_base_tx(conn, kb_id, lock="share")
            document = await self._require_document_tx(
                conn,
                base.id,
                document_id,
                lock=True,
            )
            version = await self._require_version_tx(
                conn,
                base.id,
                document.id,
                document_version_id,
                lock=True,
            )
            previous_active_id = document.active_version_id
            if (
                base.status is KnowledgeBaseStatus.deleted
                or document.status is KnowledgeDocumentStatus.deleted
            ):
                version, _, _ = await self._purge_version_tx(conn, version, timestamp)
                return KnowledgeActivationResult(
                    document=document,
                    version=version,
                    previous_active_version_id=previous_active_id,
                    activated=False,
                    stale=False,
                )
            if (
                version.status is KnowledgeVersionStatus.active
                and document.active_version_id == version.id
            ):
                return KnowledgeActivationResult(
                    document=document,
                    version=version,
                    previous_active_version_id=previous_active_id,
                    activated=False,
                    stale=False,
                )
            if version.status is not KnowledgeVersionStatus.indexing:
                return KnowledgeActivationResult(
                    document=document,
                    version=version,
                    previous_active_version_id=previous_active_id,
                    activated=False,
                    stale=False,
                )
            if document.desired_version_id != version.id:
                await self._delete_version_chunks_tx(conn, version)
                version_row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE kb_document_versions SET status = 'superseded', "
                                "error_kind = NULL, error_message = NULL "
                                "WHERE scope_id = :scope AND kb_id = :kb "
                                "AND document_id = :document AND id = :version "
                                f"RETURNING {_PG_VERSION_COLUMNS}"
                            ),
                            {
                                "scope": self._scope_id,
                                "kb": base.id,
                                "document": document.id,
                                "version": version.id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                return KnowledgeActivationResult(
                    document=document,
                    version=_pg_version_record(version_row),
                    previous_active_version_id=previous_active_id,
                    activated=False,
                    stale=True,
                )
            if previous_active_id is not None and previous_active_id != version.id:
                await conn.execute(
                    text(
                        "UPDATE kb_document_versions SET status = 'superseded' "
                        "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document "
                        "AND id = :version AND status = 'active'"
                    ),
                    {
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                        "version": previous_active_id,
                    },
                )
            version_row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE kb_document_versions SET status = 'active', "
                            "error_kind = NULL, error_message = NULL, "
                            "activated_at = COALESCE(activated_at, :now) "
                            "WHERE scope_id = :scope AND kb_id = :kb "
                            "AND document_id = :document AND id = :version "
                            f"RETURNING {_PG_VERSION_COLUMNS}"
                        ),
                        {
                            "now": timestamp,
                            "scope": self._scope_id,
                            "kb": base.id,
                            "document": document.id,
                            "version": version.id,
                        },
                    )
                )
                .mappings()
                .one()
            )
            document_row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE kb_documents SET status = 'active', "
                            "active_version_id = :version, desired_version_id = :version, "
                            "last_error_kind = NULL, last_error_message = NULL, updated_at = :now "
                            "WHERE scope_id = :scope AND kb_id = :kb AND id = :document "
                            f"RETURNING {_PG_DOCUMENT_COLUMNS}"
                        ),
                        {
                            "version": version.id,
                            "now": timestamp,
                            "scope": self._scope_id,
                            "kb": base.id,
                            "document": document.id,
                        },
                    )
                )
                .mappings()
                .one()
            )
            return KnowledgeActivationResult(
                document=_pg_document_record(document_row),
                version=_pg_version_record(version_row),
                previous_active_version_id=previous_active_id,
                activated=True,
                stale=False,
            )

    async def _mark_terminal_tx(
        self,
        conn: AsyncConnection,
        *,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        ingest_job_id: str,
        status: KnowledgeVersionStatus,
        error_kind: str,
        error_message: str,
        timestamp: datetime,
    ) -> KnowledgeDocumentVersionRecord:
        base = await self._require_base_tx(conn, kb_id, lock="share")
        document = await self._require_document_tx(
            conn,
            base.id,
            document_id,
            lock=True,
        )
        version = await self._require_version_tx(
            conn,
            base.id,
            document.id,
            document_version_id,
            lock=True,
        )
        if (
            base.status is KnowledgeBaseStatus.deleted
            or document.status is KnowledgeDocumentStatus.deleted
        ):
            version, _, _ = await self._purge_version_tx(conn, version, timestamp)
            return version
        if version.ingest_job_id != ingest_job_id:
            return version
        if version.status in {
            KnowledgeVersionStatus.failed,
            KnowledgeVersionStatus.cancelled,
            KnowledgeVersionStatus.deleted,
            KnowledgeVersionStatus.purged,
        }:
            return version
        target_was_active = (
            version.status is KnowledgeVersionStatus.active
            and document.active_version_id == version.id
        )
        await self._delete_version_chunks_tx(conn, version)
        version_row = (
            (
                await conn.execute(
                    text(
                        "UPDATE kb_document_versions SET status = :status, "
                        "error_kind = :error_kind, error_message = :error_message "
                        "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document "
                        "AND id = :version "
                        f"RETURNING {_PG_VERSION_COLUMNS}"
                    ),
                    {
                        "status": status.value,
                        "error_kind": error_kind,
                        "error_message": error_message,
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                        "version": version.id,
                    },
                )
            )
            .mappings()
            .one()
        )
        if target_was_active:
            desired_was_target = document.desired_version_id == version.id
            has_newer_desired = document.desired_version_id is not None and not desired_was_target
            predecessor_row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PG_VERSION_COLUMNS} FROM kb_document_versions "
                            "WHERE scope_id = :scope AND kb_id = :kb "
                            "AND document_id = :document AND id <> :version "
                            "AND status = 'superseded' AND activated_at IS NOT NULL "
                            "ORDER BY activated_at DESC, version DESC, id DESC "
                            "LIMIT 1 FOR UPDATE"
                        ),
                        {
                            "scope": self._scope_id,
                            "kb": base.id,
                            "document": document.id,
                            "version": version.id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if predecessor_row is None:
                if has_newer_desired:
                    await conn.execute(
                        text(
                            "UPDATE kb_documents SET status = 'pending', "
                            "active_version_id = NULL, last_error_kind = NULL, "
                            "last_error_message = NULL, updated_at = :now "
                            "WHERE scope_id = :scope AND kb_id = :kb AND id = :document"
                        ),
                        {
                            "now": timestamp,
                            "scope": self._scope_id,
                            "kb": base.id,
                            "document": document.id,
                        },
                    )
                else:
                    await conn.execute(
                        text(
                            "UPDATE kb_documents SET status = 'failed', "
                            "active_version_id = NULL, desired_version_id = NULL, "
                            "last_error_kind = :error_kind, "
                            "last_error_message = :error_message, updated_at = :now "
                            "WHERE scope_id = :scope AND kb_id = :kb AND id = :document"
                        ),
                        {
                            "error_kind": error_kind,
                            "error_message": error_message,
                            "now": timestamp,
                            "scope": self._scope_id,
                            "kb": base.id,
                            "document": document.id,
                        },
                    )
            else:
                predecessor = _pg_version_record(predecessor_row)
                await conn.execute(
                    text(
                        "UPDATE kb_document_versions SET status = 'active', "
                        "error_kind = NULL, error_message = NULL "
                        "WHERE scope_id = :scope AND kb_id = :kb "
                        "AND document_id = :document AND id = :version "
                        "AND status = 'superseded'"
                    ),
                    {
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                        "version": predecessor.id,
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE kb_documents SET status = 'active', "
                        "active_version_id = :version, "
                        "desired_version_id = CASE WHEN desired_version_id = :target "
                        "THEN :version ELSE desired_version_id END, "
                        "last_error_kind = NULL, last_error_message = NULL, updated_at = :now "
                        "WHERE scope_id = :scope AND kb_id = :kb AND id = :document"
                    ),
                    {
                        "version": predecessor.id,
                        "target": version.id,
                        "now": timestamp,
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                    },
                )
        elif document.active_version_id is None and document.desired_version_id == version.id:
            await conn.execute(
                text(
                    "UPDATE kb_documents SET status = 'failed', last_error_kind = :error_kind, "
                    "last_error_message = :error_message, updated_at = :now "
                    "WHERE scope_id = :scope AND kb_id = :kb AND id = :document"
                ),
                {
                    "error_kind": error_kind,
                    "error_message": error_message,
                    "now": timestamp,
                    "scope": self._scope_id,
                    "kb": base.id,
                    "document": document.id,
                },
            )
        return _pg_version_record(version_row)

    @_pg_error_boundary
    async def mark_version_failed(
        self,
        command: KnowledgeVersionFailure,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            return await self._mark_terminal_tx(
                conn,
                kb_id=command.kb_id,
                document_id=command.document_id,
                document_version_id=command.document_version_id,
                ingest_job_id=command.ingest_job_id,
                status=KnowledgeVersionStatus.failed,
                error_kind=command.error_kind,
                error_message=command.error_message,
                timestamp=timestamp,
            )

    @_pg_error_boundary
    async def mark_version_cancelled(
        self,
        command: KnowledgeVersionCancellation,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            return await self._mark_terminal_tx(
                conn,
                kb_id=command.kb_id,
                document_id=command.document_id,
                document_version_id=command.document_version_id,
                ingest_job_id=command.ingest_job_id,
                status=KnowledgeVersionStatus.cancelled,
                error_kind=command.error_kind,
                error_message=command.error_message,
                timestamp=timestamp,
            )

    async def _tombstone_document_tx(
        self,
        conn: AsyncConnection,
        kb_id: str,
        document_id: str,
        timestamp: datetime,
    ) -> KnowledgeDocumentRecord:
        base = await self._require_base_tx(conn, kb_id, lock="share")
        document = await self._require_document_tx(
            conn,
            base.id,
            document_id,
            lock=True,
        )
        if document.status is KnowledgeDocumentStatus.deleted:
            return document
        row = (
            (
                await conn.execute(
                    text(
                        "UPDATE kb_documents SET status = 'deleted', desired_version_id = NULL, "
                        "active_version_id = NULL, updated_at = :now, deleted_at = :now "
                        "WHERE scope_id = :scope AND kb_id = :kb AND id = :document "
                        f"RETURNING {_PG_DOCUMENT_COLUMNS}"
                    ),
                    {
                        "now": timestamp,
                        "scope": self._scope_id,
                        "kb": base.id,
                        "document": document.id,
                    },
                )
            )
            .mappings()
            .one()
        )
        return _pg_document_record(row)

    @_pg_error_boundary
    async def tombstone_document(
        self,
        kb_id: str,
        document_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentRecord:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            return await self._tombstone_document_tx(
                conn,
                kb_id,
                document_id,
                timestamp,
            )

    @_pg_error_boundary
    async def tombstone_document_idempotent(
        self,
        command: KnowledgeDocumentTombstone,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentIdempotencyResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            existing, ledger_id = await self._prepare_idempotency_tx(
                conn,
                idempotency,
                KnowledgeOperation.delete_document,
            )
            if existing is not None:
                return KnowledgeDocumentIdempotencyResult(
                    resource=await self._document_from_ledger_tx(
                        conn,
                        existing,
                        KnowledgeOperation.delete_document,
                        expected_kb_id=command.kb_id,
                        expected_document_id=command.document_id,
                    ),
                    ledger=existing,
                    replayed=True,
                )
            assert ledger_id is not None
            document = await self._tombstone_document_tx(
                conn,
                command.kb_id,
                command.document_id,
                timestamp,
            )
            ledger = await self._insert_idempotency_tx(
                conn,
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.document,
                resource_id=document.id,
                document_version_id=None,
                timestamp=timestamp,
            )
            return KnowledgeDocumentIdempotencyResult(
                resource=document,
                ledger=ledger,
                replayed=False,
            )

    async def _tombstone_base_tx(
        self,
        conn: AsyncConnection,
        kb_id: str,
        timestamp: datetime,
    ) -> KnowledgeBaseRecord:
        base = await self._require_base_tx(conn, kb_id, lock="update")
        if base.status is KnowledgeBaseStatus.deleted:
            return base
        row = (
            (
                await conn.execute(
                    text(
                        "UPDATE knowledge_bases SET status = 'deleted', updated_at = :now, "
                        "deleted_at = :now WHERE scope_id = :scope AND id = :kb "
                        f"RETURNING {_PG_BASE_COLUMNS}"
                    ),
                    {"now": timestamp, "scope": self._scope_id, "kb": base.id},
                )
            )
            .mappings()
            .one()
        )
        return _pg_base_record(row)

    @_pg_error_boundary
    async def tombstone_base(
        self,
        kb_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseRecord:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            return await self._tombstone_base_tx(conn, kb_id, timestamp)

    @_pg_error_boundary
    async def tombstone_base_idempotent(
        self,
        command: KnowledgeBaseTombstone,
        idempotency: KnowledgeIdempotencyBegin,
        *,
        now: datetime | None = None,
    ) -> KnowledgeBaseIdempotencyResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            existing, ledger_id = await self._prepare_idempotency_tx(
                conn,
                idempotency,
                KnowledgeOperation.delete_base,
            )
            if existing is not None:
                return KnowledgeBaseIdempotencyResult(
                    resource=await self._base_from_ledger_tx(
                        conn,
                        existing,
                        KnowledgeOperation.delete_base,
                        expected_kb_id=command.kb_id,
                    ),
                    ledger=existing,
                    replayed=True,
                )
            assert ledger_id is not None
            base = await self._tombstone_base_tx(conn, command.kb_id, timestamp)
            ledger = await self._insert_idempotency_tx(
                conn,
                idempotency,
                ledger_id,
                resource_kind=KnowledgeResourceKind.base,
                resource_id=base.id,
                document_version_id=None,
                timestamp=timestamp,
            )
            return KnowledgeBaseIdempotencyResult(
                resource=base,
                ledger=ledger,
                replayed=False,
            )

    async def _purge_document_tx(
        self,
        conn: AsyncConnection,
        document: KnowledgeDocumentRecord,
        timestamp: datetime,
    ) -> KnowledgePurgeResult:
        version_rows = (
            (
                await conn.execute(
                    text(
                        f"SELECT {_PG_VERSION_COLUMNS} FROM kb_document_versions "
                        "WHERE scope_id = :scope AND kb_id = :kb AND document_id = :document "
                        "ORDER BY version FOR UPDATE"
                    ),
                    {
                        "scope": self._scope_id,
                        "kb": document.kb_id,
                        "document": document.id,
                    },
                )
            )
            .mappings()
            .all()
        )
        versions_purged = 0
        chunks_removed = 0
        for row in version_rows:
            _, removed, changed = await self._purge_version_tx(
                conn,
                _pg_version_record(row),
                timestamp,
            )
            chunks_removed += removed
            versions_purged += int(changed)
        document_changed = (
            document.status is not KnowledgeDocumentStatus.deleted
            or document.source_uri is not None
            or document.desired_version_id is not None
            or document.active_version_id is not None
        )
        if document_changed:
            await conn.execute(
                text(
                    "UPDATE kb_documents SET source_uri = NULL, status = 'deleted', "
                    "desired_version_id = NULL, active_version_id = NULL, updated_at = :now, "
                    "deleted_at = COALESCE(deleted_at, :now) "
                    "WHERE scope_id = :scope AND kb_id = :kb AND id = :document"
                ),
                {
                    "now": timestamp,
                    "scope": self._scope_id,
                    "kb": document.kb_id,
                    "document": document.id,
                },
            )
        return KnowledgePurgeResult(
            documents_purged=int(document_changed or versions_purged > 0 or chunks_removed > 0),
            versions_purged=versions_purged,
            chunks_removed=chunks_removed,
        )

    @_pg_error_boundary
    async def purge_document(
        self,
        kb_id: str,
        document_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgePurgeResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            base = await self._require_base_tx(conn, kb_id, lock="share")
            document = await self._require_document_tx(
                conn,
                base.id,
                document_id,
                lock=True,
            )
            if (
                base.status is KnowledgeBaseStatus.active
                and document.status is not KnowledgeDocumentStatus.deleted
            ):
                raise KnowledgeConflict(
                    KnowledgePublicCode.invalid_transition,
                    "Knowledge document must be tombstoned before purge.",
                )
            return await self._purge_document_tx(conn, document, timestamp)

    @_pg_error_boundary
    async def purge_base(
        self,
        kb_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgePurgeResult:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            base = await self._require_base_tx(conn, kb_id, lock="update")
            if base.status is not KnowledgeBaseStatus.deleted:
                raise KnowledgeConflict(
                    KnowledgePublicCode.invalid_transition,
                    "Knowledge Base must be tombstoned before purge.",
                )
            document_rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PG_DOCUMENT_COLUMNS} FROM kb_documents "
                            "WHERE scope_id = :scope AND kb_id = :kb ORDER BY created_at, id "
                            "FOR UPDATE"
                        ),
                        {"scope": self._scope_id, "kb": base.id},
                    )
                )
                .mappings()
                .all()
            )
            documents_purged = 0
            versions_purged = 0
            chunks_removed = 0
            for row in document_rows:
                result = await self._purge_document_tx(
                    conn,
                    _pg_document_record(row),
                    timestamp,
                )
                documents_purged += result.documents_purged
                versions_purged += result.versions_purged
                chunks_removed += result.chunks_removed
            if base.description is not None:
                await conn.execute(
                    text(
                        "UPDATE knowledge_bases SET description = NULL, updated_at = :now "
                        "WHERE scope_id = :scope AND id = :kb"
                    ),
                    {"now": timestamp, "scope": self._scope_id, "kb": base.id},
                )
            return KnowledgePurgeResult(
                documents_purged=documents_purged,
                versions_purged=versions_purged,
                chunks_removed=chunks_removed,
            )

    @_pg_error_boundary
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
        validated_key = validate_idempotency_key(idempotency_key)
        async with self._transaction() as conn:
            return await self._get_idempotency_tx(conn, operation, validated_key)

    @_pg_error_boundary
    async def attach_idempotent_job(
        self,
        command: KnowledgeIdempotencyAttach,
        *,
        now: datetime | None = None,
    ) -> KnowledgeIdempotencyRecord:
        timestamp = _timestamp(now)
        async with self._transaction() as conn:
            await self._lock_idempotency_tx(
                conn,
                command.operation,
                command.idempotency_key,
            )
            record = await self._get_idempotency_tx(
                conn,
                command.operation,
                command.idempotency_key,
                lock=True,
            )
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
                if record.resource_kind is not KnowledgeResourceKind.document:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.knowledge_conflict,
                        "Knowledge idempotency ledger is inconsistent.",
                    )
                document = await self._get_document_by_id_tx(
                    conn,
                    record.resource_id,
                    lock=True,
                )
                version = (
                    None
                    if document is None
                    else await self._get_version_tx(
                        conn,
                        document.kb_id,
                        document.id,
                        record.document_version_id,
                        lock=True,
                    )
                )
                if version is None:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.knowledge_conflict,
                        "Knowledge idempotency ledger is inconsistent.",
                    )
                if version.ingest_job_id is not None and version.ingest_job_id != command.job_id:
                    raise KnowledgeConflict(
                        KnowledgePublicCode.idempotency_job_conflict,
                        "Knowledge version already has a different job.",
                    )
                if version.ingest_job_id is None:
                    await conn.execute(
                        text(
                            "UPDATE kb_document_versions SET ingest_job_id = :job_id "
                            "WHERE scope_id = :scope AND kb_id = :kb "
                            "AND document_id = :document AND id = :version"
                        ),
                        {
                            "job_id": command.job_id,
                            "scope": self._scope_id,
                            "kb": version.kb_id,
                            "document": version.document_id,
                            "version": version.id,
                        },
                    )
            if record.job_id is None:
                row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE knowledge_idempotency SET job_id = :job_id, "
                                "updated_at = :now WHERE scope_id = :scope "
                                "AND operation = :operation AND idempotency_key = :key "
                                "AND id = :id "
                                f"RETURNING {_PG_IDEMPOTENCY_COLUMNS}"
                            ),
                            {
                                "job_id": command.job_id,
                                "now": timestamp,
                                "scope": self._scope_id,
                                "operation": command.operation.value,
                                "key": command.idempotency_key,
                                "id": record.id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                record = _pg_idempotency_record(row)
            return record


__all__ = ["InMemoryKnowledgeStore", "KnowledgeStore", "PostgresKnowledgeStore"]
