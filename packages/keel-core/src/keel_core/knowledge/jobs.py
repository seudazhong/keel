"""Worker-agnostic durable Knowledge ingest and deletion handlers."""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, StrictStr, ValidationError, field_validator

from keel_core.config import Settings
from keel_core.embeddings import Embedder
from keel_core.jobs import (
    CancelMode,
    JobCancellationRequested,
    JobError,
    JobRecord,
    JobResult,
    JobTerminalIntent,
    PermanentJobError,
    RetryableJobError,
)

from .chunking import chunk_document, normalize_document_text
from .models import (
    KnowledgeBaseRecord,
    KnowledgeChunkReplacement,
    KnowledgeChunkWrite,
    KnowledgeConflict,
    KnowledgeDocumentVersionRecord,
    KnowledgeError,
    KnowledgeNotFound,
    KnowledgePublicCode,
    KnowledgeStorageError,
    KnowledgeValidationError,
    KnowledgeVersionCancellation,
    KnowledgeVersionFailure,
    KnowledgeVersionStatus,
    index_fingerprint,
    knowledge_source_type_from_mime_type,
    validate_knowledge_base_id,
    validate_knowledge_document_id,
    validate_knowledge_version_id,
)
from .store import KnowledgeStore

KNOWLEDGE_INGEST_KIND = "knowledge.ingest"
KNOWLEDGE_DELETE_KIND = "knowledge.delete"
KNOWLEDGE_INGEST_MAX_ATTEMPTS = 3
KNOWLEDGE_DELETE_MAX_ATTEMPTS = 2_147_483_647
KNOWLEDGE_INGEST_CANCEL_MODE = CancelMode.cooperative
KNOWLEDGE_DELETE_CANCEL_MODE = CancelMode.disabled

_PUBLIC_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_MAX_DOMAIN_ERROR_MESSAGE_CHARS = 512
_INVALID_PAYLOAD_CODE = "invalid_knowledge_job_payload"
_INVALID_PAYLOAD_MESSAGE = "Knowledge job payload is invalid."
_SCOPE_MISMATCH_CODE = "knowledge_scope_mismatch"
_SCOPE_MISMATCH_MESSAGE = "Knowledge job scope does not match the configured store."
_EMBEDDING_RESPONSE_CODE = "embedding_response_invalid"
_EMBEDDING_RESPONSE_MESSAGE = "Embedding provider returned an invalid response."
_EMBEDDING_UNAVAILABLE_CODE = "embedding_unavailable"
_EMBEDDING_UNAVAILABLE_MESSAGE = "Embedding provider is temporarily unavailable."


class _StrictPayload(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
        strict=True,
    )


class KnowledgeIngestPayload(_StrictPayload):
    kb_id: StrictStr
    document_id: StrictStr
    document_version_id: StrictStr

    @field_validator("kb_id")
    @classmethod
    def _validate_kb_id(cls, value: str) -> str:
        return _validated_id(validate_knowledge_base_id, value)

    @field_validator("document_id")
    @classmethod
    def _validate_document_id(cls, value: str) -> str:
        return _validated_id(validate_knowledge_document_id, value)

    @field_validator("document_version_id")
    @classmethod
    def _validate_version_id(cls, value: str) -> str:
        return _validated_id(validate_knowledge_version_id, value)


class KnowledgeDeletePayload(_StrictPayload):
    kb_id: StrictStr
    document_id: StrictStr | None = None

    @field_validator("kb_id")
    @classmethod
    def _validate_kb_id(cls, value: str) -> str:
        return _validated_id(validate_knowledge_base_id, value)

    @field_validator("document_id")
    @classmethod
    def _validate_document_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validated_id(validate_knowledge_document_id, value)


@runtime_checkable
class KnowledgeJobContext(Protocol):
    job_id: str
    scope_id: str
    attempt: int
    max_attempts: int

    async def progress(
        self,
        current: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None: ...

    async def checkpoint(self) -> None: ...


def _validated_id(validator: Any, value: str) -> str:
    try:
        return str(validator(value))
    except KnowledgeValidationError as exc:
        raise ValueError(exc.public_message) from None


def _parse_payload[T: BaseModel](model: type[T], payload: object) -> T:
    try:
        return model.model_validate(payload, strict=True)
    except ValidationError:
        raise PermanentJobError(_INVALID_PAYLOAD_CODE, _INVALID_PAYLOAD_MESSAGE) from None


def _bounded_error(
    kind: object,
    message: object,
    *,
    fallback_kind: str,
    fallback_message: str,
) -> tuple[str, str]:
    safe_kind = kind if isinstance(kind, str) else ""
    if _PUBLIC_CODE_RE.fullmatch(safe_kind) is None:
        safe_kind = fallback_kind

    safe_message = message if isinstance(message, str) else ""
    try:
        safe_message.encode("utf-8")
    except UnicodeEncodeError:
        safe_message = ""
    if "\x00" in safe_message:
        safe_message = ""
    safe_message = safe_message.strip()[:_MAX_DOMAIN_ERROR_MESSAGE_CHARS]
    if not safe_message:
        safe_message = fallback_message
    return safe_kind, safe_message


def _permanent_from_domain(exc: KnowledgeError) -> PermanentJobError:
    return PermanentJobError(exc.code, exc.public_message)


def _retryable_from_storage(exc: KnowledgeStorageError) -> RetryableJobError:
    return RetryableJobError(exc.code, exc.public_message)


def _ingest_result(payload: KnowledgeIngestPayload, chunk_count: int) -> JobResult:
    return JobResult(
        data={
            "kb_id": payload.kb_id,
            "document_id": payload.document_id,
            "document_version_id": payload.document_version_id,
            "chunk_count": chunk_count,
        },
        message="Knowledge indexing completed.",
    )


def _delete_result(
    payload: KnowledgeDeletePayload,
    *,
    documents_purged: int,
    versions_purged: int,
    chunks_removed: int,
) -> JobResult:
    return JobResult(
        data={
            "kb_id": payload.kb_id,
            "document_id": payload.document_id,
            "documents_purged": documents_purged,
            "versions_purged": versions_purged,
            "chunks_removed": chunks_removed,
        },
        message="Knowledge deletion completed.",
    )


class KnowledgeJobHandlers:
    """Scope-bound Knowledge job handlers with idempotent terminal hooks."""

    def __init__(self, store: KnowledgeStore, embedder: Embedder, settings: Settings) -> None:
        self._store = store
        self._embedder = embedder
        self._settings = settings

    async def ingest(
        self,
        context: KnowledgeJobContext,
        raw_payload: dict[str, Any],
    ) -> JobResult:
        payload = _parse_payload(KnowledgeIngestPayload, raw_payload)
        self._require_context_scope(context)

        try:
            await self._store.attach_version_job(
                payload.kb_id,
                payload.document_id,
                payload.document_version_id,
                context.job_id,
            )
        except KnowledgeStorageError as exc:
            raise _retryable_from_storage(exc) from None
        except KnowledgeError as exc:
            raise _permanent_from_domain(exc) from None

        base, version = await self._load_ingest_records(payload)

        try:
            indexing = await self._store.mark_indexing(
                payload.kb_id,
                payload.document_id,
                payload.document_version_id,
            )
        except KnowledgeStorageError as exc:
            raise _retryable_from_storage(exc) from None
        except KnowledgeError as exc:
            raise _permanent_from_domain(exc) from None

        terminal = await self._terminal_ingest_result(payload, indexing.version)
        if terminal is not None:
            return terminal

        worker_model = self._embedder.model
        worker_dim = self._embedder.dim
        if (
            not isinstance(worker_model, str)
            or type(worker_dim) is not int
            or worker_model != base.embedding_model
            or worker_dim != base.embedding_dim
        ):
            raise PermanentJobError(
                KnowledgePublicCode.embedding_configuration_mismatch,
                "Embedding configuration does not match the Knowledge Base pin.",
            )

        persisted_content = indexing.version.content
        if persisted_content is None:
            raise PermanentJobError(
                KnowledgePublicCode.content_invalid,
                "Knowledge document content is unavailable for indexing.",
            )

        try:
            normalized = normalize_document_text(
                persisted_content,
                max_bytes=max(1, len(persisted_content.encode("utf-8"))),
            )
            source_type = knowledge_source_type_from_mime_type(indexing.version.mime_type)
            if normalized != persisted_content:
                raise PermanentJobError(
                    KnowledgePublicCode.content_invalid,
                    "Stored Knowledge document content is not normalized.",
                )
            expected_fingerprint = index_fingerprint(
                indexing.version.content_sha256,
                source_type,
                indexing.version.chunking_version,
                base.embedding_model,
                base.embedding_dim,
                indexing.version.target_chars,
                indexing.version.overlap_chars,
            )
            if expected_fingerprint != indexing.version.index_fingerprint:
                raise PermanentJobError(
                    KnowledgePublicCode.fingerprint_invalid,
                    "Stored Knowledge index configuration is invalid.",
                )
            await context.progress(0, total=None, message="chunking")
            drafts = chunk_document(
                normalized,
                source_type,
                target_chars=indexing.version.target_chars,
                overlap_chars=indexing.version.overlap_chars,
            )
        except PermanentJobError:
            raise
        except KnowledgeStorageError as exc:
            raise _retryable_from_storage(exc) from None
        except KnowledgeError as exc:
            raise _permanent_from_domain(exc) from None
        except (TypeError, ValueError, UnicodeError):
            raise PermanentJobError(
                "invalid_knowledge_index_state",
                "Stored Knowledge index configuration is invalid.",
            ) from None

        total = len(drafts)
        await context.progress(0, total=total, message="embedding")
        vectors: list[tuple[float, ...]] = []
        batch_size = self._settings.knowledge_embedding_batch_size
        for start in range(0, total, batch_size):
            batch = drafts[start : start + batch_size]
            try:
                response = await self._embedder.embed([draft.text for draft in batch])
            except (PermanentJobError, RetryableJobError):
                raise
            except Exception:
                raise RetryableJobError(
                    _EMBEDDING_UNAVAILABLE_CODE,
                    _EMBEDDING_UNAVAILABLE_MESSAGE,
                ) from None
            vectors.extend(
                _validated_vectors(
                    response,
                    expected_count=len(batch),
                    expected_dim=base.embedding_dim,
                )
            )
            await context.checkpoint()
            await context.progress(
                min(start + len(batch), total),
                total=total,
                message="embedding",
            )

        await context.progress(total, total=total, message="writing")
        replacement = KnowledgeChunkReplacement(
            kb_id=payload.kb_id,
            document_id=payload.document_id,
            document_version_id=payload.document_version_id,
            chunks=tuple(
                KnowledgeChunkWrite(
                    ordinal=draft.ordinal,
                    text=draft.text,
                    char_start=draft.char_start,
                    char_end=draft.char_end,
                    content_hash=draft.content_hash,
                    heading_path=draft.heading_path,
                    metadata={},
                    model=base.embedding_model,
                    dim=base.embedding_dim,
                    embedding=vector,
                )
                for draft, vector in zip(drafts, vectors, strict=True)
            ),
        )
        try:
            written = await self._store.replace_version_chunks(replacement)
        except KnowledgeStorageError as exc:
            raise _retryable_from_storage(exc) from None
        except KnowledgeConflict as exc:
            if exc.code == KnowledgePublicCode.chunk_write_rejected:
                terminal = await self._resolve_ingest_race(payload)
                if terminal is not None:
                    return terminal
            raise _permanent_from_domain(exc) from None
        except KnowledgeError as exc:
            raise _permanent_from_domain(exc) from None

        await context.progress(total, total=total, message="activating")
        try:
            activation = await self._store.activate_version(
                payload.kb_id,
                payload.document_id,
                payload.document_version_id,
            )
        except KnowledgeStorageError as exc:
            raise _retryable_from_storage(exc) from None
        except KnowledgeError as exc:
            raise _permanent_from_domain(exc) from None

        terminal = await self._terminal_ingest_result(
            payload,
            activation.version,
            active_chunk_count=written.chunk_count,
        )
        if terminal is not None:
            return terminal
        raise RetryableJobError(
            "knowledge_activation_incomplete",
            "Knowledge activation did not reach a terminal state.",
        )

    async def delete(
        self,
        context: KnowledgeJobContext,
        raw_payload: dict[str, Any],
    ) -> JobResult:
        payload = _parse_payload(KnowledgeDeletePayload, raw_payload)
        self._require_context_scope(context)
        return await self._purge(payload)

    async def ingest_cancelled(self, row: JobRecord) -> None:
        if row.terminal_intent not in {None, JobTerminalIntent.cancelled}:
            return
        payload = self._hook_ingest_payload(row)
        if payload is None:
            return
        try:
            version = await self._store.get_version(
                payload.kb_id,
                payload.document_id,
                payload.document_version_id,
            )
            if version is None or version.ingest_job_id != row.id:
                return
            await self._store.mark_version_cancelled(
                KnowledgeVersionCancellation(
                    kb_id=payload.kb_id,
                    document_id=payload.document_id,
                    document_version_id=payload.document_version_id,
                    ingest_job_id=row.id,
                )
            )
        except KnowledgeStorageError:
            raise
        except KnowledgeError:
            return

    async def ingest_failed(self, row: JobRecord, error: JobError) -> None:
        if row.terminal_intent not in {None, JobTerminalIntent.failed}:
            return
        payload = self._hook_ingest_payload(row)
        if payload is None:
            return
        error_kind, error_message = _bounded_error(
            error.kind,
            error.message,
            fallback_kind="indexing_failed",
            fallback_message="Knowledge indexing failed.",
        )
        try:
            version = await self._store.get_version(
                payload.kb_id,
                payload.document_id,
                payload.document_version_id,
            )
            if version is None or version.ingest_job_id != row.id:
                return
            await self._store.mark_version_failed(
                KnowledgeVersionFailure(
                    kb_id=payload.kb_id,
                    document_id=payload.document_id,
                    document_version_id=payload.document_version_id,
                    ingest_job_id=row.id,
                    error_kind=error_kind,
                    error_message=error_message,
                )
            )
        except KnowledgeStorageError:
            raise
        except KnowledgeError:
            return

    async def delete_failed(self, row: JobRecord, error: JobError) -> None:
        del error
        if row.terminal_intent not in {None, JobTerminalIntent.failed}:
            return
        payload = self._hook_delete_payload(row)
        if payload is None:
            return
        try:
            await self._purge(payload)
        except RetryableJobError:
            raise
        except PermanentJobError:
            return

    def _require_context_scope(self, context: KnowledgeJobContext) -> None:
        if context.scope_id != self._store.scope_id:
            raise PermanentJobError(_SCOPE_MISMATCH_CODE, _SCOPE_MISMATCH_MESSAGE)

    async def _load_ingest_records(
        self,
        payload: KnowledgeIngestPayload,
    ) -> tuple[KnowledgeBaseRecord, KnowledgeDocumentVersionRecord]:
        try:
            base = await self._store.get_base(payload.kb_id)
            if base is None:
                raise KnowledgeNotFound(
                    KnowledgePublicCode.knowledge_base_not_found,
                    "Knowledge Base was not found.",
                )
            document = await self._store.get_document(payload.kb_id, payload.document_id)
            if document is None:
                raise KnowledgeNotFound(
                    KnowledgePublicCode.knowledge_document_not_found,
                    "Knowledge document was not found.",
                )
            version = await self._store.get_version(
                payload.kb_id,
                payload.document_id,
                payload.document_version_id,
            )
            if version is None:
                raise KnowledgeNotFound(
                    KnowledgePublicCode.knowledge_version_not_found,
                    "Knowledge document version was not found.",
                )
        except KnowledgeStorageError as exc:
            raise _retryable_from_storage(exc) from None
        except KnowledgeError as exc:
            raise _permanent_from_domain(exc) from None

        expected_scope = self._store.scope_id
        if (
            base.scope_id != expected_scope
            or document.scope_id != expected_scope
            or version.scope_id != expected_scope
        ):
            raise PermanentJobError(_SCOPE_MISMATCH_CODE, _SCOPE_MISMATCH_MESSAGE)
        if (
            base.id != payload.kb_id
            or document.id != payload.document_id
            or document.kb_id != base.id
            or version.id != payload.document_version_id
            or version.kb_id != base.id
            or version.document_id != document.id
        ):
            raise PermanentJobError(
                KnowledgePublicCode.knowledge_not_found,
                "Knowledge resource was not found.",
            )
        return base, version

    async def _terminal_ingest_result(
        self,
        payload: KnowledgeIngestPayload,
        version: KnowledgeDocumentVersionRecord,
        *,
        active_chunk_count: int | None = None,
    ) -> JobResult | None:
        if version.status is KnowledgeVersionStatus.indexing:
            return None
        if version.status is KnowledgeVersionStatus.failed:
            kind, message = _bounded_error(
                version.error_kind,
                version.error_message,
                fallback_kind="indexing_failed",
                fallback_message="Knowledge indexing failed.",
            )
            raise PermanentJobError(kind, message)
        if version.status is KnowledgeVersionStatus.cancelled:
            raise JobCancellationRequested
        if version.status is KnowledgeVersionStatus.active:
            if active_chunk_count is None:
                try:
                    chunks = await self._store.list_version_chunks(
                        payload.kb_id,
                        payload.document_id,
                        payload.document_version_id,
                    )
                except KnowledgeStorageError as exc:
                    raise _retryable_from_storage(exc) from None
                except KnowledgeError as exc:
                    raise _permanent_from_domain(exc) from None
                active_chunk_count = len(chunks)
            return _ingest_result(payload, active_chunk_count)
        if version.status in {
            KnowledgeVersionStatus.superseded,
            KnowledgeVersionStatus.deleted,
            KnowledgeVersionStatus.purged,
        }:
            return _ingest_result(payload, 0)
        raise PermanentJobError(
            "invalid_knowledge_index_state",
            "Stored Knowledge index state is invalid.",
        )

    async def _resolve_ingest_race(
        self,
        payload: KnowledgeIngestPayload,
    ) -> JobResult | None:
        try:
            indexing = await self._store.mark_indexing(
                payload.kb_id,
                payload.document_id,
                payload.document_version_id,
            )
        except KnowledgeStorageError as exc:
            raise _retryable_from_storage(exc) from None
        except KnowledgeError as exc:
            raise _permanent_from_domain(exc) from None
        return await self._terminal_ingest_result(payload, indexing.version)

    async def _purge(self, payload: KnowledgeDeletePayload) -> JobResult:
        try:
            base = await self._store.get_base(payload.kb_id)
            if base is None:
                raise KnowledgeNotFound(
                    KnowledgePublicCode.knowledge_base_not_found,
                    "Knowledge Base was not found.",
                )
            if base.scope_id != self._store.scope_id:
                raise PermanentJobError(_SCOPE_MISMATCH_CODE, _SCOPE_MISMATCH_MESSAGE)
            if base.id != payload.kb_id:
                raise PermanentJobError(
                    KnowledgePublicCode.knowledge_not_found,
                    "Knowledge resource was not found.",
                )
            if payload.document_id is None:
                result = await self._store.purge_base(payload.kb_id)
            else:
                document = await self._store.get_document(payload.kb_id, payload.document_id)
                if document is None:
                    raise KnowledgeNotFound(
                        KnowledgePublicCode.knowledge_document_not_found,
                        "Knowledge document was not found.",
                    )
                if document.scope_id != self._store.scope_id:
                    raise PermanentJobError(_SCOPE_MISMATCH_CODE, _SCOPE_MISMATCH_MESSAGE)
                if document.id != payload.document_id or document.kb_id != base.id:
                    raise PermanentJobError(
                        KnowledgePublicCode.knowledge_not_found,
                        "Knowledge resource was not found.",
                    )
                result = await self._store.purge_document(
                    payload.kb_id,
                    payload.document_id,
                )
        except PermanentJobError:
            raise
        except KnowledgeStorageError as exc:
            raise _retryable_from_storage(exc) from None
        except KnowledgeError as exc:
            raise _permanent_from_domain(exc) from None
        return _delete_result(
            payload,
            documents_purged=result.documents_purged,
            versions_purged=result.versions_purged,
            chunks_removed=result.chunks_removed,
        )

    def _hook_ingest_payload(self, row: JobRecord) -> KnowledgeIngestPayload | None:
        if row.kind != KNOWLEDGE_INGEST_KIND or row.scope_id != self._store.scope_id:
            return None
        try:
            return KnowledgeIngestPayload.model_validate(row.payload, strict=True)
        except ValidationError:
            return None

    def _hook_delete_payload(self, row: JobRecord) -> KnowledgeDeletePayload | None:
        if row.kind != KNOWLEDGE_DELETE_KIND or row.scope_id != self._store.scope_id:
            return None
        try:
            return KnowledgeDeletePayload.model_validate(row.payload, strict=True)
        except ValidationError:
            return None


def _validated_vectors(
    response: object,
    *,
    expected_count: int,
    expected_dim: int,
) -> list[tuple[float, ...]]:
    if not isinstance(response, Sequence) or isinstance(response, str | bytes):
        raise RetryableJobError(_EMBEDDING_RESPONSE_CODE, _EMBEDDING_RESPONSE_MESSAGE)
    if len(response) != expected_count:
        raise RetryableJobError(_EMBEDDING_RESPONSE_CODE, _EMBEDDING_RESPONSE_MESSAGE)

    vectors: list[tuple[float, ...]] = []
    for vector in response:
        if not isinstance(vector, Sequence) or isinstance(vector, str | bytes):
            raise RetryableJobError(_EMBEDDING_RESPONSE_CODE, _EMBEDDING_RESPONSE_MESSAGE)
        if len(vector) != expected_dim:
            raise RetryableJobError(_EMBEDDING_RESPONSE_CODE, _EMBEDDING_RESPONSE_MESSAGE)
        normalized: list[float] = []
        for value in vector:
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise RetryableJobError(
                    _EMBEDDING_RESPONSE_CODE,
                    _EMBEDDING_RESPONSE_MESSAGE,
                )
            converted = float(value)
            if not math.isfinite(converted):
                raise RetryableJobError(
                    _EMBEDDING_RESPONSE_CODE,
                    _EMBEDDING_RESPONSE_MESSAGE,
                )
            normalized.append(converted)
        vectors.append(tuple(normalized))
    return vectors


__all__ = [
    "KNOWLEDGE_DELETE_CANCEL_MODE",
    "KNOWLEDGE_DELETE_KIND",
    "KNOWLEDGE_DELETE_MAX_ATTEMPTS",
    "KNOWLEDGE_INGEST_CANCEL_MODE",
    "KNOWLEDGE_INGEST_KIND",
    "KNOWLEDGE_INGEST_MAX_ATTEMPTS",
    "KnowledgeDeletePayload",
    "KnowledgeIngestPayload",
    "KnowledgeJobContext",
    "KnowledgeJobHandlers",
]
