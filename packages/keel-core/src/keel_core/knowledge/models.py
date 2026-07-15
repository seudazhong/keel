"""Strict Knowledge domain models, commands, identifiers, and fingerprints."""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import ClassVar, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from keel_core.errors import KeelError

_ID_HEX_CHARS = 32
_SHA256_HEX_CHARS = 64
_MAX_PUBLIC_CODE_CHARS = 128
_MAX_PUBLIC_MESSAGE_CHARS = 512
_MAX_IDEMPOTENCY_KEY_BYTES = 512
_MAX_IDENTITY_BYTES = 512
_PUBLIC_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_CITATION_ID_RE = re.compile(r"^cite_[1-9][0-9]*$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_KNOWLEDGE_DOCUMENT_MAX_BYTES = 1_048_576

type KnowledgeBaseId = str
type KnowledgeDocumentId = str
type KnowledgeVersionId = str
type KnowledgeDocumentVersionId = KnowledgeVersionId
type KnowledgeChunkId = str
type KnowledgeIdempotencyId = str


class KnowledgeIdKind(StrEnum):
    base = "base"
    document = "document"
    version = "version"
    chunk = "chunk"
    idempotency = "idempotency"


_ID_PREFIXES: dict[KnowledgeIdKind, str] = {
    KnowledgeIdKind.base: "kb_",
    KnowledgeIdKind.document: "doc_",
    KnowledgeIdKind.version: "kbv_",
    KnowledgeIdKind.chunk: "kbc_",
    KnowledgeIdKind.idempotency: "kbi_",
}
_ID_PATTERNS = {
    kind: re.compile(rf"^{re.escape(prefix)}[0-9a-f]{{{_ID_HEX_CHARS}}}$")
    for kind, prefix in _ID_PREFIXES.items()
}


class KnowledgeSourceType(StrEnum):
    text = "text"
    markdown = "markdown"


_MIME_SOURCE_TYPES = {
    "text/plain": KnowledgeSourceType.text,
    "text/markdown": KnowledgeSourceType.markdown,
}


class KnowledgeBaseStatus(StrEnum):
    active = "active"
    deleted = "deleted"


class KnowledgeDocumentStatus(StrEnum):
    pending = "pending"
    active = "active"
    failed = "failed"
    deleted = "deleted"


class KnowledgeVersionStatus(StrEnum):
    pending = "pending"
    indexing = "indexing"
    active = "active"
    superseded = "superseded"
    failed = "failed"
    cancelled = "cancelled"
    deleted = "deleted"
    purged = "purged"


class KnowledgeSearchMode(StrEnum):
    hybrid = "hybrid"
    lexical = "lexical"
    lexical_degraded = "lexical-degraded"


class KnowledgeOperation(StrEnum):
    create_base = "create_base"
    delete_base = "delete_base"
    create_document = "create_document"
    update_document = "update_document"
    reindex_document = "reindex_document"
    delete_document = "delete_document"


class KnowledgeResourceKind(StrEnum):
    base = "base"
    document = "document"


class KnowledgePublicCode(StrEnum):
    invalid_id = "invalid_knowledge_id"
    invalid_scope_id = "invalid_scope_id"
    invalid_input = "invalid_knowledge_input"
    content_invalid = "content_invalid"
    content_too_large = "content_too_large"
    fingerprint_invalid = "fingerprint_invalid"
    knowledge_not_found = "knowledge_not_found"
    knowledge_base_not_found = "knowledge_base_not_found"
    knowledge_document_not_found = "knowledge_document_not_found"
    knowledge_version_not_found = "knowledge_version_not_found"
    knowledge_conflict = "knowledge_conflict"
    knowledge_base_name_conflict = "knowledge_base_name_conflict"
    knowledge_base_deleted = "knowledge_base_deleted"
    knowledge_document_deleted = "knowledge_document_deleted"
    invalid_transition = "invalid_knowledge_transition"
    chunk_write_rejected = "chunk_write_rejected"
    idempotency_key_reused = "idempotency_key_reused"
    idempotency_not_found = "idempotency_not_found"
    idempotency_job_conflict = "idempotency_job_conflict"
    no_active_version = "no_active_version"
    embedding_configuration_mismatch = "embedding_configuration_mismatch"
    embeddings_not_configured = "embeddings_not_configured"
    storage_failure = "knowledge_storage_failure"


KnowledgeErrorCode = KnowledgePublicCode


def _storage_safe_text(
    value: object,
    *,
    field: str,
    allow_empty: bool = False,
    max_chars: int | None = None,
) -> str:
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError(f"{field} must be storage-safe UTF-8 text")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} must be storage-safe UTF-8 text") from exc
    if not allow_empty and not value:
        raise ValueError(f"{field} must not be empty")
    if max_chars is not None and len(value) > max_chars:
        raise ValueError(f"{field} must not exceed {max_chars} characters")
    return value


def _required_text(value: object, *, field: str, max_chars: int | None = None) -> str:
    safe = _storage_safe_text(value, field=field, max_chars=max_chars)
    normalized = safe.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    return normalized


def _optional_text(value: object, *, field: str, max_chars: int | None = None) -> str | None:
    if value is None:
        return None
    safe = _storage_safe_text(value, field=field, allow_empty=True, max_chars=max_chars)
    normalized = safe.strip()
    return normalized or None


def _validate_public_code(value: object, *, field: str = "code") -> str:
    code = _required_text(value, field=field, max_chars=_MAX_PUBLIC_CODE_CHARS)
    if _PUBLIC_CODE_RE.fullmatch(code) is None:
        raise ValueError(f"{field} must use lowercase snake_case")
    return code


def _validate_hash(value: object, *, field: str) -> str:
    safe = _storage_safe_text(value, field=field)
    if _HASH_RE.fullmatch(safe) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return safe


def _validate_timestamp(value: datetime | None, *, field: str) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{field} must include a timezone offset")


def _require_enum[T: StrEnum](value: object, enum_type: type[T], *, field: str) -> T:
    if not isinstance(value, enum_type):
        raise ValueError(f"{field} must be a {enum_type.__name__}")
    return value


def _strict_int(value: object, *, field: str, minimum: int | None = None) -> int:
    if type(value) is not int:
        raise ValueError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        if minimum == 1:
            raise ValueError(f"{field} must be a positive integer")
        raise ValueError(f"{field} must be an integer greater than or equal to {minimum}")
    return value


def _validate_chunk_settings(target_chars: object, overlap_chars: object) -> tuple[int, int]:
    target = _strict_int(target_chars, field="target_chars", minimum=1)
    overlap = _strict_int(overlap_chars, field="overlap_chars", minimum=0)
    if overlap >= target:
        raise ValueError("chunk overlap must be non-negative and less than target")
    return target, overlap


class KnowledgeError(KeelError):
    """Base class for bounded, client-safe Knowledge domain failures."""

    def __init__(self, code: str | KnowledgePublicCode, public_message: str) -> None:
        self.code = _validate_public_code(str(code))
        self.public_message = _required_text(
            public_message,
            field="public_message",
            max_chars=_MAX_PUBLIC_MESSAGE_CHARS,
        )
        super().__init__(self.public_message)

    def __str__(self) -> str:
        return f"{self.code}: {self.public_message}"


class KnowledgeNotFound(KnowledgeError):
    def __init__(
        self,
        code: str | KnowledgePublicCode = KnowledgePublicCode.knowledge_not_found,
        public_message: str = "Knowledge resource was not found.",
    ) -> None:
        super().__init__(code, public_message)


class KnowledgeConflict(KnowledgeError):
    def __init__(
        self,
        code: str | KnowledgePublicCode = KnowledgePublicCode.knowledge_conflict,
        public_message: str = "Knowledge resource state conflicts with this operation.",
    ) -> None:
        super().__init__(code, public_message)


class KnowledgeValidationError(KnowledgeError):
    def __init__(
        self,
        code: str | KnowledgePublicCode = KnowledgePublicCode.invalid_input,
        public_message: str = "Knowledge input is invalid.",
    ) -> None:
        super().__init__(code, public_message)


class KnowledgeStorageError(KnowledgeError):
    """Bounded, retryable failure accessing persistent Knowledge storage."""

    retryable: ClassVar[Literal[True]] = True

    def __init__(self) -> None:
        super().__init__(
            KnowledgePublicCode.storage_failure,
            "Knowledge storage is temporarily unavailable.",
        )


class KnowledgeEmbeddingMismatch(KnowledgeConflict):
    def __init__(self) -> None:
        super().__init__(
            KnowledgePublicCode.embedding_configuration_mismatch,
            "Embedding configuration does not match the Knowledge Base pin.",
        )


def new_knowledge_id(kind: KnowledgeIdKind) -> str:
    return f"{_ID_PREFIXES[kind]}{uuid.uuid4().hex}"


def validate_knowledge_id(value: object, kind: KnowledgeIdKind) -> str:
    try:
        safe = _storage_safe_text(value, field=f"{kind.value}_id")
    except ValueError as exc:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_id,
            "Knowledge identifier is invalid.",
        ) from exc
    if _ID_PATTERNS[kind].fullmatch(safe) is None:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_id,
            "Knowledge identifier is invalid.",
        )
    return safe


def new_knowledge_base_id() -> KnowledgeBaseId:
    return new_knowledge_id(KnowledgeIdKind.base)


def new_knowledge_document_id() -> KnowledgeDocumentId:
    return new_knowledge_id(KnowledgeIdKind.document)


def new_knowledge_version_id() -> KnowledgeVersionId:
    return new_knowledge_id(KnowledgeIdKind.version)


def new_knowledge_document_version_id() -> KnowledgeDocumentVersionId:
    return new_knowledge_version_id()


def new_knowledge_chunk_id() -> KnowledgeChunkId:
    return new_knowledge_id(KnowledgeIdKind.chunk)


def new_knowledge_idempotency_id() -> KnowledgeIdempotencyId:
    return new_knowledge_id(KnowledgeIdKind.idempotency)


def validate_knowledge_base_id(value: object) -> KnowledgeBaseId:
    return validate_knowledge_id(value, KnowledgeIdKind.base)


def validate_knowledge_document_id(value: object) -> KnowledgeDocumentId:
    return validate_knowledge_id(value, KnowledgeIdKind.document)


def validate_knowledge_version_id(value: object) -> KnowledgeVersionId:
    return validate_knowledge_id(value, KnowledgeIdKind.version)


def validate_knowledge_document_version_id(value: object) -> KnowledgeDocumentVersionId:
    return validate_knowledge_version_id(value)


def validate_knowledge_chunk_id(value: object) -> KnowledgeChunkId:
    return validate_knowledge_id(value, KnowledgeIdKind.chunk)


def validate_knowledge_idempotency_id(value: object) -> KnowledgeIdempotencyId:
    return validate_knowledge_id(value, KnowledgeIdKind.idempotency)


def validate_scope_id(value: object) -> str:
    try:
        safe = _required_text(value, field="scope_id")
    except ValueError as exc:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_scope_id,
            "Knowledge scope identifier is invalid.",
        ) from exc
    if len(safe.encode("utf-8")) > _MAX_IDENTITY_BYTES:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_scope_id,
            "Knowledge scope identifier is invalid.",
        )
    return safe


def validate_idempotency_key(value: object) -> str:
    try:
        safe = _storage_safe_text(value, field="idempotency_key")
    except ValueError as exc:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            "Idempotency key is invalid.",
        ) from exc
    if safe != safe.strip() or len(safe.encode("utf-8")) > _MAX_IDEMPOTENCY_KEY_BYTES:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            "Idempotency key is invalid.",
        )
    return safe


def validate_document_max_bytes(value: object) -> int:
    try:
        return _strict_int(value, field="document_max_bytes", minimum=1)
    except ValueError as exc:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            "Document maximum size must be a positive integer.",
        ) from exc


def knowledge_source_type_from_mime_type(mime_type: object) -> KnowledgeSourceType:
    if not isinstance(mime_type, str):
        source_type = None
    else:
        source_type = _MIME_SOURCE_TYPES.get(mime_type)
    if source_type is None:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            "Knowledge document MIME type is unsupported.",
        )
    return source_type


def content_sha256(content: str) -> str:
    try:
        safe = _storage_safe_text(content, field="content", allow_empty=True)
    except ValueError as exc:
        raise KnowledgeValidationError(
            KnowledgePublicCode.content_invalid,
            "Document content is not storage-safe UTF-8 text.",
        ) from exc
    return hashlib.sha256(safe.encode("utf-8")).hexdigest()


def index_fingerprint(
    content_digest: str,
    source_type: KnowledgeSourceType,
    chunking_version: str,
    embedding_model: str,
    embedding_dim: int,
    target_chars: int,
    overlap_chars: int,
) -> str:
    try:
        digest = _validate_hash(content_digest, field="content_sha256")
        source = _require_enum(source_type, KnowledgeSourceType, field="source_type")
        chunker = _required_text(chunking_version, field="chunking_version")
        model = _required_text(embedding_model, field="embedding_model")
        dim = _strict_int(embedding_dim, field="embedding_dim", minimum=1)
        target, overlap = _validate_chunk_settings(target_chars, overlap_chars)
    except ValueError as exc:
        raise KnowledgeValidationError(
            KnowledgePublicCode.fingerprint_invalid,
            "Index fingerprint input is invalid.",
        ) from exc
    serialized = json.dumps(
        {
            "chunking_version": chunker,
            "content_sha256": digest,
            "embedding_dim": dim,
            "embedding_model": model,
            "overlap_chars": overlap,
            "source_type": source.value,
            "target_chars": target,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _canonical_json_value(value: object, *, depth: int = 0) -> object:
    if depth > 100:
        raise KnowledgeValidationError(
            KnowledgePublicCode.fingerprint_invalid,
            "Request fingerprint body is invalid.",
        )
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise KnowledgeValidationError(
                KnowledgePublicCode.fingerprint_invalid,
                "Request fingerprint body is invalid.",
            )
        return value
    if isinstance(value, str):
        try:
            return _storage_safe_text(value, field="request body text", allow_empty=True)
        except ValueError as exc:
            raise KnowledgeValidationError(
                KnowledgePublicCode.fingerprint_invalid,
                "Request fingerprint body is invalid.",
            ) from exc
    if isinstance(value, list):
        return [_canonical_json_value(item, depth=depth + 1) for item in value]
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            try:
                safe_key = _storage_safe_text(key, field="request body key", allow_empty=True)
            except ValueError as exc:
                raise KnowledgeValidationError(
                    KnowledgePublicCode.fingerprint_invalid,
                    "Request fingerprint body is invalid.",
                ) from exc
            normalized[safe_key] = _canonical_json_value(item, depth=depth + 1)
        return normalized
    raise KnowledgeValidationError(
        KnowledgePublicCode.fingerprint_invalid,
        "Request fingerprint body is invalid.",
    )


def _canonical_path_ids(path_ids: Mapping[str, str] | Sequence[str]) -> object:
    if isinstance(path_ids, Mapping):
        normalized: dict[str, str] = {}
        validators = {
            "kb_id": validate_knowledge_base_id,
            "document_id": validate_knowledge_document_id,
            "document_version_id": validate_knowledge_version_id,
            "chunk_id": validate_knowledge_chunk_id,
        }
        for key, value in path_ids.items():
            try:
                safe_key = _required_text(key, field="path identifier name")
            except ValueError as exc:
                raise KnowledgeValidationError(
                    KnowledgePublicCode.fingerprint_invalid,
                    "Request fingerprint path is invalid.",
                ) from exc
            validator = validators.get(safe_key)
            if validator is not None:
                normalized[safe_key] = validator(value)
            else:
                try:
                    normalized[safe_key] = _required_text(value, field="path identifier")
                except ValueError as exc:
                    raise KnowledgeValidationError(
                        KnowledgePublicCode.fingerprint_invalid,
                        "Request fingerprint path is invalid.",
                    ) from exc
        return normalized
    if isinstance(path_ids, str | bytes):
        raise KnowledgeValidationError(
            KnowledgePublicCode.fingerprint_invalid,
            "Request fingerprint path is invalid.",
        )
    normalized_sequence: list[str] = []
    for value in path_ids:
        try:
            normalized_sequence.append(_required_text(value, field="path identifier"))
        except ValueError as exc:
            raise KnowledgeValidationError(
                KnowledgePublicCode.fingerprint_invalid,
                "Request fingerprint path is invalid.",
            ) from exc
    return normalized_sequence


def request_fingerprint(
    method: str,
    operation: KnowledgeOperation | str,
    path_ids: Mapping[str, str] | Sequence[str],
    body: BaseModel | Mapping[str, object] | None,
) -> str:
    try:
        normalized_method = _required_text(method, field="method").upper()
        normalized_operation = KnowledgeOperation(operation)
    except (ValueError, TypeError) as exc:
        raise KnowledgeValidationError(
            KnowledgePublicCode.fingerprint_invalid,
            "Request fingerprint metadata is invalid.",
        ) from exc
    if re.fullmatch(r"[A-Z]+", normalized_method) is None:
        raise KnowledgeValidationError(
            KnowledgePublicCode.fingerprint_invalid,
            "Request fingerprint metadata is invalid.",
        )
    if isinstance(body, BaseModel):
        body_value: object = body.model_dump(mode="json")
    elif body is None:
        body_value = {}
    elif isinstance(body, Mapping):
        body_value = body
    else:
        raise KnowledgeValidationError(
            KnowledgePublicCode.fingerprint_invalid,
            "Request fingerprint body is invalid.",
        )
    payload = {
        "body": _canonical_json_value(body_value),
        "method": normalized_method,
        "operation": normalized_operation.value,
        "path_ids": _canonical_path_ids(path_ids),
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def canonical_request_fingerprint(
    method: str,
    operation: KnowledgeOperation | str,
    path_ids: Mapping[str, str] | Sequence[str],
    body: BaseModel | Mapping[str, object] | None,
) -> str:
    return request_fingerprint(method, operation, path_ids, body)


class _StrictKnowledgeModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
    )


class CreateKnowledgeBaseCommand(_StrictKnowledgeModel):
    name: StrictStr = Field(min_length=1, max_length=300)
    description: StrictStr | None = Field(default=None, max_length=2_000)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _required_text(value, field="name", max_chars=300)

    @field_validator("description")
    @classmethod
    def _validate_description(cls, value: str | None) -> str | None:
        return _optional_text(value, field="description", max_chars=2_000)


class _KnowledgeDocumentWriteCommand(_StrictKnowledgeModel):
    title: StrictStr = Field(min_length=1, max_length=300)
    source_type: KnowledgeSourceType
    content: StrictStr = Field(min_length=1)
    source_uri: StrictStr | None = None
    target_session_id: StrictStr | None = None

    @field_validator("title")
    @classmethod
    def _validate_title(cls, value: str) -> str:
        return _required_text(value, field="title", max_chars=300)

    @field_validator("content")
    @classmethod
    def _validate_content(cls, value: str) -> str:
        try:
            safe = _storage_safe_text(value, field="content")
        except ValueError as exc:
            raise ValueError("content must be storage-safe UTF-8 text") from exc
        if not safe.strip():
            raise ValueError("content must not be blank")
        return safe

    @field_validator("source_uri")
    @classmethod
    def _validate_source_uri(cls, value: str | None) -> str | None:
        return _optional_text(value, field="source_uri")

    @field_validator("target_session_id")
    @classmethod
    def _validate_target_session_id(cls, value: str | None) -> str | None:
        normalized = _optional_text(value, field="target_session_id")
        if normalized is not None and len(normalized.encode("utf-8")) > _MAX_IDENTITY_BYTES:
            raise ValueError("target_session_id is too large")
        return normalized


class CreateKnowledgeDocumentCommand(_KnowledgeDocumentWriteCommand):
    pass


class UpdateKnowledgeDocumentCommand(_KnowledgeDocumentWriteCommand):
    pass


class ReindexKnowledgeDocumentCommand(_StrictKnowledgeModel):
    target_session_id: StrictStr | None = None

    @field_validator("target_session_id")
    @classmethod
    def _validate_target_session_id(cls, value: str | None) -> str | None:
        normalized = _optional_text(value, field="target_session_id")
        if normalized is not None and len(normalized.encode("utf-8")) > _MAX_IDENTITY_BYTES:
            raise ValueError("target_session_id is too large")
        return normalized


class DeleteKnowledgeCommand(_StrictKnowledgeModel):
    pass


class KnowledgeSearchCommand(_StrictKnowledgeModel):
    query: StrictStr = Field(min_length=1, max_length=2_000)
    k: StrictInt = Field(default=5, ge=1, le=10)

    @field_validator("query")
    @classmethod
    def _validate_query(cls, value: str) -> str:
        return _required_text(value, field="query", max_chars=2_000)


class KnowledgeCitation(_StrictKnowledgeModel):
    id: StrictStr
    kb_id: StrictStr
    document_id: StrictStr
    document_version_id: StrictStr
    chunk_id: StrictStr
    title: StrictStr
    source_uri: StrictStr | None
    ordinal: StrictInt = Field(ge=0)
    char_start: StrictInt = Field(ge=0)
    char_end: StrictInt = Field(ge=0)
    label: StrictStr

    @field_validator("id")
    @classmethod
    def _validate_citation_id(cls, value: str) -> str:
        if _CITATION_ID_RE.fullmatch(value) is None:
            raise ValueError("citation id is invalid")
        return value

    @field_validator("kb_id")
    @classmethod
    def _validate_kb_id(cls, value: str) -> str:
        try:
            return validate_knowledge_base_id(value)
        except KnowledgeValidationError as exc:
            raise ValueError("kb_id is invalid") from exc

    @field_validator("document_id")
    @classmethod
    def _validate_document_id(cls, value: str) -> str:
        try:
            return validate_knowledge_document_id(value)
        except KnowledgeValidationError as exc:
            raise ValueError("document_id is invalid") from exc

    @field_validator("document_version_id")
    @classmethod
    def _validate_version_id(cls, value: str) -> str:
        try:
            return validate_knowledge_version_id(value)
        except KnowledgeValidationError as exc:
            raise ValueError("document_version_id is invalid") from exc

    @field_validator("chunk_id")
    @classmethod
    def _validate_chunk_id(cls, value: str) -> str:
        try:
            return validate_knowledge_chunk_id(value)
        except KnowledgeValidationError as exc:
            raise ValueError("chunk_id is invalid") from exc

    @field_validator("title", "label")
    @classmethod
    def _validate_required_text(cls, value: str) -> str:
        return _required_text(value, field="citation text")

    @field_validator("source_uri")
    @classmethod
    def _validate_source_uri(cls, value: str | None) -> str | None:
        return _optional_text(value, field="source_uri")

    @model_validator(mode="after")
    def _validate_offsets(self) -> KnowledgeCitation:
        if self.char_end < self.char_start:
            raise ValueError("char_end must be greater than or equal to char_start")
        return self


class KnowledgeHit(_StrictKnowledgeModel):
    snippet: StrictStr
    rank: StrictInt = Field(ge=1)
    citation: KnowledgeCitation
    heading_path: list[StrictStr] = Field(default_factory=list)

    @field_validator("snippet")
    @classmethod
    def _validate_snippet(cls, value: str) -> str:
        return _storage_safe_text(value, field="snippet", allow_empty=True)

    @field_validator("heading_path")
    @classmethod
    def _validate_heading_path(cls, value: list[str]) -> list[str]:
        return [_required_text(item, field="heading") for item in value]


class KnowledgeSearchStatus(_StrictKnowledgeModel):
    mode: KnowledgeSearchMode
    semantic_error: StrictStr | None = None

    @field_validator("semantic_error")
    @classmethod
    def _validate_semantic_error(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_public_code(value, field="semantic_error")


@dataclass(frozen=True, slots=True)
class ChunkDraft:
    ordinal: int
    text: str
    char_start: int
    char_end: int
    content_hash: str
    heading_path: tuple[str, ...]

    def __post_init__(self) -> None:
        _strict_int(self.ordinal, field="ordinal", minimum=0)
        _storage_safe_text(self.text, field="text", allow_empty=True)
        _strict_int(self.char_start, field="char_start", minimum=0)
        _strict_int(self.char_end, field="char_end", minimum=0)
        if self.char_end < self.char_start:
            raise ValueError("char_end must be greater than or equal to char_start")
        _validate_hash(self.content_hash, field="content_hash")
        if self.char_end - self.char_start != len(self.text):
            raise ValueError("chunk offsets must span the chunk text")
        if self.content_hash != content_sha256(self.text):
            raise ValueError("content_hash does not match chunk text")
        for heading in self.heading_path:
            _required_text(heading, field="heading")


@dataclass(frozen=True, slots=True)
class KnowledgeBaseRecord:
    id: KnowledgeBaseId
    scope_id: str
    name: str
    description: str | None
    embedding_model: str
    embedding_dim: int
    status: KnowledgeBaseStatus
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None

    def __post_init__(self) -> None:
        validate_knowledge_base_id(self.id)
        validate_scope_id(self.scope_id)
        _required_text(self.name, field="name")
        _optional_text(self.description, field="description")
        _required_text(self.embedding_model, field="embedding_model")
        _strict_int(self.embedding_dim, field="embedding_dim", minimum=1)
        _require_enum(self.status, KnowledgeBaseStatus, field="status")
        _validate_timestamp(self.created_at, field="created_at")
        _validate_timestamp(self.updated_at, field="updated_at")
        _validate_timestamp(self.deleted_at, field="deleted_at")


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentRecord:
    id: KnowledgeDocumentId
    scope_id: str
    kb_id: KnowledgeBaseId
    title: str
    source_type: KnowledgeSourceType
    source_uri: str | None
    status: KnowledgeDocumentStatus
    desired_version_id: KnowledgeVersionId | None
    active_version_id: KnowledgeVersionId | None
    last_error_kind: str | None
    last_error_message: str | None
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None

    def __post_init__(self) -> None:
        validate_knowledge_document_id(self.id)
        validate_scope_id(self.scope_id)
        validate_knowledge_base_id(self.kb_id)
        _required_text(self.title, field="title")
        _require_enum(self.source_type, KnowledgeSourceType, field="source_type")
        _optional_text(self.source_uri, field="source_uri")
        _require_enum(self.status, KnowledgeDocumentStatus, field="status")
        if self.desired_version_id is not None:
            validate_knowledge_version_id(self.desired_version_id)
        if self.active_version_id is not None:
            validate_knowledge_version_id(self.active_version_id)
        if self.last_error_kind is not None:
            _validate_public_code(self.last_error_kind, field="last_error_kind")
        if self.last_error_message is not None:
            _required_text(
                self.last_error_message,
                field="last_error_message",
                max_chars=_MAX_PUBLIC_MESSAGE_CHARS,
            )
        _validate_timestamp(self.created_at, field="created_at")
        _validate_timestamp(self.updated_at, field="updated_at")
        _validate_timestamp(self.deleted_at, field="deleted_at")


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentVersionRecord:
    id: KnowledgeVersionId
    scope_id: str
    kb_id: KnowledgeBaseId
    document_id: KnowledgeDocumentId
    version: int
    content: str | None
    content_sha256: str
    index_fingerprint: str
    mime_type: str
    chunking_version: str
    target_chars: int
    overlap_chars: int
    ingest_job_id: str | None
    status: KnowledgeVersionStatus
    error_kind: str | None
    error_message: str | None
    created_at: datetime
    activated_at: datetime | None
    deleted_at: datetime | None
    purged_at: datetime | None

    def __post_init__(self) -> None:
        validate_knowledge_version_id(self.id)
        validate_scope_id(self.scope_id)
        validate_knowledge_base_id(self.kb_id)
        validate_knowledge_document_id(self.document_id)
        _strict_int(self.version, field="version", minimum=1)
        if self.content is not None:
            _storage_safe_text(self.content, field="content", allow_empty=True)
        _validate_hash(self.content_sha256, field="content_sha256")
        if self.content is not None and self.content_sha256 != content_sha256(self.content):
            raise ValueError("content_sha256 does not match content")
        _validate_hash(self.index_fingerprint, field="index_fingerprint")
        knowledge_source_type_from_mime_type(self.mime_type)
        _required_text(self.chunking_version, field="chunking_version")
        _validate_chunk_settings(self.target_chars, self.overlap_chars)
        _require_enum(self.status, KnowledgeVersionStatus, field="status")
        if self.ingest_job_id is not None:
            _required_text(self.ingest_job_id, field="ingest_job_id")
        if self.error_kind is not None:
            _validate_public_code(self.error_kind, field="error_kind")
        if self.error_message is not None:
            _required_text(
                self.error_message,
                field="error_message",
                max_chars=_MAX_PUBLIC_MESSAGE_CHARS,
            )
        _validate_timestamp(self.created_at, field="created_at")
        _validate_timestamp(self.activated_at, field="activated_at")
        _validate_timestamp(self.deleted_at, field="deleted_at")
        _validate_timestamp(self.purged_at, field="purged_at")


@dataclass(frozen=True, slots=True)
class KnowledgeChunkRecord:
    id: KnowledgeChunkId
    scope_id: str
    kb_id: KnowledgeBaseId
    document_id: KnowledgeDocumentId
    document_version_id: KnowledgeVersionId
    ordinal: int
    text: str
    char_start: int
    char_end: int
    content_hash: str
    heading_path: tuple[str, ...]
    metadata: dict[str, object]
    model: str
    dim: int
    embedding: tuple[float, ...]
    created_at: datetime

    def __post_init__(self) -> None:
        validate_knowledge_chunk_id(self.id)
        validate_scope_id(self.scope_id)
        validate_knowledge_base_id(self.kb_id)
        validate_knowledge_document_id(self.document_id)
        validate_knowledge_version_id(self.document_version_id)
        _strict_int(self.ordinal, field="ordinal", minimum=0)
        _storage_safe_text(self.text, field="text", allow_empty=True)
        _strict_int(self.char_start, field="char_start", minimum=0)
        _strict_int(self.char_end, field="char_end", minimum=0)
        if self.char_end < self.char_start:
            raise ValueError("chunk offsets are invalid")
        _validate_hash(self.content_hash, field="content_hash")
        if self.content_hash != content_sha256(self.text):
            raise ValueError("content_hash does not match chunk text")
        for heading in self.heading_path:
            _required_text(heading, field="heading")
        _canonical_json_value(self.metadata)
        _required_text(self.model, field="model")
        _strict_int(self.dim, field="dim", minimum=1)
        if len(self.embedding) != self.dim:
            raise ValueError("embedding dimension is invalid")
        if any(not math.isfinite(value) for value in self.embedding):
            raise ValueError("embedding values must be finite")
        _validate_timestamp(self.created_at, field="created_at")


@dataclass(frozen=True, slots=True)
class KnowledgeIdempotencyRecord:
    id: KnowledgeIdempotencyId
    scope_id: str
    operation: KnowledgeOperation
    idempotency_key: str
    request_fingerprint: str
    resource_kind: KnowledgeResourceKind
    resource_id: str
    document_version_id: KnowledgeVersionId | None
    job_id: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        validate_knowledge_idempotency_id(self.id)
        validate_scope_id(self.scope_id)
        _require_enum(self.operation, KnowledgeOperation, field="operation")
        validate_idempotency_key(self.idempotency_key)
        _validate_hash(self.request_fingerprint, field="request_fingerprint")
        _require_enum(self.resource_kind, KnowledgeResourceKind, field="resource_kind")
        if self.resource_kind is KnowledgeResourceKind.base:
            validate_knowledge_base_id(self.resource_id)
        else:
            validate_knowledge_document_id(self.resource_id)
        if self.document_version_id is not None:
            validate_knowledge_version_id(self.document_version_id)
        if self.job_id is not None:
            _required_text(self.job_id, field="job_id")
        base_operation = self.operation in {
            KnowledgeOperation.create_base,
            KnowledgeOperation.delete_base,
        }
        if base_operation != (self.resource_kind is KnowledgeResourceKind.base):
            raise ValueError("operation and resource_kind are inconsistent")
        version_operation = self.operation in {
            KnowledgeOperation.create_document,
            KnowledgeOperation.update_document,
            KnowledgeOperation.reindex_document,
        }
        if version_operation != (self.document_version_id is not None):
            raise ValueError("operation and document_version_id are inconsistent")
        _validate_timestamp(self.created_at, field="created_at")
        _validate_timestamp(self.updated_at, field="updated_at")


@dataclass(frozen=True, slots=True)
class KnowledgeBaseCreate:
    name: str
    description: str | None
    embedding_model: str
    embedding_dim: int
    base_id: KnowledgeBaseId | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _required_text(self.name, field="name", max_chars=300))
        object.__setattr__(
            self,
            "description",
            _optional_text(self.description, field="description", max_chars=2_000),
        )
        object.__setattr__(
            self,
            "embedding_model",
            _required_text(self.embedding_model, field="embedding_model"),
        )
        _strict_int(self.embedding_dim, field="embedding_dim", minimum=1)
        if self.base_id is not None:
            validate_knowledge_base_id(self.base_id)


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentVersionCreate:
    kb_id: KnowledgeBaseId
    title: str
    source_type: KnowledgeSourceType
    content: str
    mime_type: str
    chunking_version: str
    target_chars: int
    overlap_chars: int
    document_id: KnowledgeDocumentId | None = None
    new_document_id: KnowledgeDocumentId | None = None
    source_uri: str | None = None
    document_version_id: KnowledgeVersionId | None = None

    def __post_init__(self) -> None:
        validate_knowledge_base_id(self.kb_id)
        if self.document_id is not None:
            validate_knowledge_document_id(self.document_id)
        if self.new_document_id is not None:
            validate_knowledge_document_id(self.new_document_id)
        if self.document_id is not None and self.new_document_id is not None:
            raise ValueError("document_id and new_document_id are mutually exclusive")
        if self.document_version_id is not None:
            validate_knowledge_version_id(self.document_version_id)
        object.__setattr__(
            self,
            "title",
            _required_text(self.title, field="title", max_chars=300),
        )
        _require_enum(self.source_type, KnowledgeSourceType, field="source_type")
        object.__setattr__(self, "source_uri", _optional_text(self.source_uri, field="source_uri"))
        try:
            safe_content = _storage_safe_text(self.content, field="content")
        except ValueError as exc:
            raise KnowledgeValidationError(
                KnowledgePublicCode.content_invalid,
                "Document content is not storage-safe UTF-8 text.",
            ) from exc
        if not safe_content.strip():
            raise KnowledgeValidationError(
                KnowledgePublicCode.content_invalid,
                "Document content must not be blank.",
            )
        object.__setattr__(self, "content", safe_content)
        mime_source_type = knowledge_source_type_from_mime_type(self.mime_type)
        if mime_source_type is not self.source_type:
            raise ValueError("mime_type does not match source_type")
        object.__setattr__(
            self,
            "chunking_version",
            _required_text(self.chunking_version, field="chunking_version"),
        )
        _validate_chunk_settings(self.target_chars, self.overlap_chars)


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentReindex:
    kb_id: KnowledgeBaseId
    document_id: KnowledgeDocumentId
    chunking_version: str
    target_chars: int
    overlap_chars: int
    document_version_id: KnowledgeVersionId | None = None

    def __post_init__(self) -> None:
        validate_knowledge_base_id(self.kb_id)
        validate_knowledge_document_id(self.document_id)
        if self.document_version_id is not None:
            validate_knowledge_version_id(self.document_version_id)
        object.__setattr__(
            self,
            "chunking_version",
            _required_text(self.chunking_version, field="chunking_version"),
        )
        _validate_chunk_settings(self.target_chars, self.overlap_chars)


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentTombstone:
    kb_id: KnowledgeBaseId
    document_id: KnowledgeDocumentId

    def __post_init__(self) -> None:
        validate_knowledge_base_id(self.kb_id)
        validate_knowledge_document_id(self.document_id)


@dataclass(frozen=True, slots=True)
class KnowledgeBaseTombstone:
    kb_id: KnowledgeBaseId

    def __post_init__(self) -> None:
        validate_knowledge_base_id(self.kb_id)


@dataclass(frozen=True, slots=True)
class KnowledgeChunkWrite:
    ordinal: int
    text: str
    char_start: int
    char_end: int
    content_hash: str
    heading_path: tuple[str, ...]
    metadata: dict[str, object]
    model: str
    dim: int
    embedding: tuple[float, ...]
    chunk_id: KnowledgeChunkId | None = None

    def __post_init__(self) -> None:
        if self.chunk_id is not None:
            validate_knowledge_chunk_id(self.chunk_id)
        _strict_int(self.ordinal, field="ordinal", minimum=0)
        _storage_safe_text(self.text, field="text", allow_empty=True)
        _strict_int(self.char_start, field="char_start", minimum=0)
        _strict_int(self.char_end, field="char_end", minimum=0)
        if self.char_end < self.char_start:
            raise ValueError("chunk offsets are invalid")
        _validate_hash(self.content_hash, field="content_hash")
        if self.content_hash != content_sha256(self.text):
            raise ValueError("content_hash does not match chunk text")
        for heading in self.heading_path:
            _required_text(heading, field="heading")
        _canonical_json_value(self.metadata)
        _required_text(self.model, field="model")
        _strict_int(self.dim, field="dim", minimum=1)
        if len(self.embedding) != self.dim:
            raise ValueError("embedding dimension is invalid")
        if any(not math.isfinite(value) for value in self.embedding):
            raise ValueError("embedding values must be finite")


@dataclass(frozen=True, slots=True)
class KnowledgeChunkReplacement:
    kb_id: KnowledgeBaseId
    document_id: KnowledgeDocumentId
    document_version_id: KnowledgeVersionId
    chunks: tuple[KnowledgeChunkWrite, ...]

    def __post_init__(self) -> None:
        validate_knowledge_base_id(self.kb_id)
        validate_knowledge_document_id(self.document_id)
        validate_knowledge_version_id(self.document_version_id)
        object.__setattr__(self, "chunks", tuple(self.chunks))
        ordinals = [chunk.ordinal for chunk in self.chunks]
        if len(ordinals) != len(set(ordinals)):
            raise ValueError("chunk ordinals must be unique")


@dataclass(frozen=True, slots=True)
class KnowledgeVersionFailure:
    kb_id: KnowledgeBaseId
    document_id: KnowledgeDocumentId
    document_version_id: KnowledgeVersionId
    ingest_job_id: str
    error_kind: str
    error_message: str

    def __post_init__(self) -> None:
        validate_knowledge_base_id(self.kb_id)
        validate_knowledge_document_id(self.document_id)
        validate_knowledge_version_id(self.document_version_id)
        _required_text(self.ingest_job_id, field="ingest_job_id")
        object.__setattr__(
            self,
            "error_kind",
            _validate_public_code(self.error_kind, field="error_kind"),
        )
        object.__setattr__(
            self,
            "error_message",
            _required_text(
                self.error_message,
                field="error_message",
                max_chars=_MAX_PUBLIC_MESSAGE_CHARS,
            ),
        )


@dataclass(frozen=True, slots=True)
class KnowledgeVersionCancellation:
    kb_id: KnowledgeBaseId
    document_id: KnowledgeDocumentId
    document_version_id: KnowledgeVersionId
    ingest_job_id: str
    error_kind: str = "indexing_cancelled"
    error_message: str = "Knowledge indexing was cancelled."

    def __post_init__(self) -> None:
        validate_knowledge_base_id(self.kb_id)
        validate_knowledge_document_id(self.document_id)
        validate_knowledge_version_id(self.document_version_id)
        _required_text(self.ingest_job_id, field="ingest_job_id")
        object.__setattr__(
            self,
            "error_kind",
            _validate_public_code(self.error_kind, field="error_kind"),
        )
        object.__setattr__(
            self,
            "error_message",
            _required_text(
                self.error_message,
                field="error_message",
                max_chars=_MAX_PUBLIC_MESSAGE_CHARS,
            ),
        )


@dataclass(frozen=True, slots=True)
class KnowledgeIdempotencyBegin:
    operation: KnowledgeOperation
    idempotency_key: str
    request_fingerprint: str
    ledger_id: KnowledgeIdempotencyId | None = None

    def __post_init__(self) -> None:
        _require_enum(self.operation, KnowledgeOperation, field="operation")
        object.__setattr__(
            self,
            "idempotency_key",
            validate_idempotency_key(self.idempotency_key),
        )
        _validate_hash(self.request_fingerprint, field="request_fingerprint")
        if self.ledger_id is not None:
            validate_knowledge_idempotency_id(self.ledger_id)


@dataclass(frozen=True, slots=True)
class KnowledgeIdempotencyAttach:
    operation: KnowledgeOperation
    idempotency_key: str
    request_fingerprint: str
    job_id: str

    def __post_init__(self) -> None:
        _require_enum(self.operation, KnowledgeOperation, field="operation")
        object.__setattr__(
            self,
            "idempotency_key",
            validate_idempotency_key(self.idempotency_key),
        )
        _validate_hash(self.request_fingerprint, field="request_fingerprint")
        object.__setattr__(
            self,
            "job_id",
            _required_text(
                self.job_id,
                field="job_id",
                max_chars=_MAX_IDENTITY_BYTES,
            ),
        )


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentVersionResult:
    document: KnowledgeDocumentRecord
    version: KnowledgeDocumentVersionRecord
    reused: bool


@dataclass(frozen=True, slots=True)
class KnowledgeIndexingResult:
    version: KnowledgeDocumentVersionRecord
    started: bool
    stale: bool


@dataclass(frozen=True, slots=True)
class KnowledgeActivationResult:
    document: KnowledgeDocumentRecord
    version: KnowledgeDocumentVersionRecord
    previous_active_version_id: KnowledgeVersionId | None
    activated: bool
    stale: bool


@dataclass(frozen=True, slots=True)
class KnowledgeChunkReplacementResult:
    document_version_id: KnowledgeVersionId
    chunk_count: int

    def __post_init__(self) -> None:
        validate_knowledge_version_id(self.document_version_id)
        _strict_int(self.chunk_count, field="chunk_count", minimum=0)


@dataclass(frozen=True, slots=True)
class KnowledgePurgeResult:
    documents_purged: int
    versions_purged: int
    chunks_removed: int

    def __post_init__(self) -> None:
        _strict_int(self.documents_purged, field="documents_purged", minimum=0)
        _strict_int(self.versions_purged, field="versions_purged", minimum=0)
        _strict_int(self.chunks_removed, field="chunks_removed", minimum=0)


@dataclass(frozen=True, slots=True)
class KnowledgeBaseIdempotencyResult:
    resource: KnowledgeBaseRecord
    ledger: KnowledgeIdempotencyRecord
    replayed: bool


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentVersionIdempotencyResult:
    resource: KnowledgeDocumentVersionResult
    ledger: KnowledgeIdempotencyRecord
    replayed: bool


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentIdempotencyResult:
    resource: KnowledgeDocumentRecord
    ledger: KnowledgeIdempotencyRecord
    replayed: bool


__all__ = [
    "ChunkDraft",
    "CreateKnowledgeBaseCommand",
    "CreateKnowledgeDocumentCommand",
    "DEFAULT_KNOWLEDGE_DOCUMENT_MAX_BYTES",
    "DeleteKnowledgeCommand",
    "KnowledgeActivationResult",
    "KnowledgeBaseCreate",
    "KnowledgeBaseId",
    "KnowledgeBaseIdempotencyResult",
    "KnowledgeBaseRecord",
    "KnowledgeBaseStatus",
    "KnowledgeBaseTombstone",
    "KnowledgeChunkId",
    "KnowledgeChunkRecord",
    "KnowledgeChunkReplacement",
    "KnowledgeChunkReplacementResult",
    "KnowledgeChunkWrite",
    "KnowledgeCitation",
    "KnowledgeConflict",
    "KnowledgeDocumentId",
    "KnowledgeDocumentIdempotencyResult",
    "KnowledgeDocumentRecord",
    "KnowledgeDocumentReindex",
    "KnowledgeDocumentStatus",
    "KnowledgeDocumentTombstone",
    "KnowledgeDocumentVersionId",
    "KnowledgeDocumentVersionCreate",
    "KnowledgeDocumentVersionIdempotencyResult",
    "KnowledgeDocumentVersionRecord",
    "KnowledgeDocumentVersionResult",
    "KnowledgeEmbeddingMismatch",
    "KnowledgeError",
    "KnowledgeErrorCode",
    "KnowledgeHit",
    "KnowledgeIdKind",
    "KnowledgeIdempotencyAttach",
    "KnowledgeIdempotencyBegin",
    "KnowledgeIdempotencyId",
    "KnowledgeIdempotencyRecord",
    "KnowledgeIndexingResult",
    "KnowledgeNotFound",
    "KnowledgeOperation",
    "KnowledgePublicCode",
    "KnowledgePurgeResult",
    "KnowledgeResourceKind",
    "KnowledgeSearchCommand",
    "KnowledgeSearchMode",
    "KnowledgeSearchStatus",
    "KnowledgeSourceType",
    "KnowledgeStorageError",
    "KnowledgeValidationError",
    "KnowledgeVersionCancellation",
    "KnowledgeVersionFailure",
    "KnowledgeVersionId",
    "KnowledgeVersionStatus",
    "ReindexKnowledgeDocumentCommand",
    "UpdateKnowledgeDocumentCommand",
    "content_sha256",
    "canonical_request_fingerprint",
    "index_fingerprint",
    "knowledge_source_type_from_mime_type",
    "new_knowledge_base_id",
    "new_knowledge_chunk_id",
    "new_knowledge_document_id",
    "new_knowledge_document_version_id",
    "new_knowledge_id",
    "new_knowledge_idempotency_id",
    "new_knowledge_version_id",
    "request_fingerprint",
    "validate_idempotency_key",
    "validate_document_max_bytes",
    "validate_knowledge_base_id",
    "validate_knowledge_chunk_id",
    "validate_knowledge_document_id",
    "validate_knowledge_document_version_id",
    "validate_knowledge_id",
    "validate_knowledge_idempotency_id",
    "validate_knowledge_version_id",
    "validate_scope_id",
]
