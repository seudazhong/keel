"""Strict Knowledge domain contracts and fingerprint helpers."""

from __future__ import annotations

import re
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from keel_core.knowledge import (
    ChunkDraft,
    CreateKnowledgeBaseCommand,
    CreateKnowledgeDocumentCommand,
    KnowledgeBaseCreate,
    KnowledgeChunkReplacementResult,
    KnowledgeChunkWrite,
    KnowledgeCitation,
    KnowledgeDocumentVersionCreate,
    KnowledgeDocumentVersionRecord,
    KnowledgeEmbeddingMismatch,
    KnowledgeHit,
    KnowledgeOperation,
    KnowledgePurgeResult,
    KnowledgeSearchMode,
    KnowledgeSearchStatus,
    KnowledgeSourceType,
    KnowledgeValidationError,
    KnowledgeVersionStatus,
    canonical_request_fingerprint,
    content_sha256,
    index_fingerprint,
    knowledge_source_type_from_mime_type,
    new_knowledge_base_id,
    new_knowledge_chunk_id,
    new_knowledge_document_id,
    new_knowledge_idempotency_id,
    new_knowledge_version_id,
    request_fingerprint,
    validate_knowledge_base_id,
    validate_knowledge_chunk_id,
    validate_knowledge_document_id,
    validate_knowledge_idempotency_id,
    validate_knowledge_version_id,
)

_HEX_ID = re.compile(r"^[a-z]+_[0-9a-f]{32}$")


@pytest.mark.parametrize(
    ("factory", "validator", "prefix"),
    [
        (new_knowledge_base_id, validate_knowledge_base_id, "kb_"),
        (new_knowledge_document_id, validate_knowledge_document_id, "doc_"),
        (new_knowledge_version_id, validate_knowledge_version_id, "kbv_"),
        (new_knowledge_chunk_id, validate_knowledge_chunk_id, "kbc_"),
        (new_knowledge_idempotency_id, validate_knowledge_idempotency_id, "kbi_"),
    ],
)
def test_knowledge_ids_are_strict_prefixed_storage_ids(
    factory: object,
    validator: object,
    prefix: str,
) -> None:
    generated = factory()  # type: ignore[operator]
    assert generated.startswith(prefix)
    assert _HEX_ID.fullmatch(generated)
    assert validator(generated) == generated  # type: ignore[operator]

    for invalid in (
        generated.upper(),
        generated.replace("_", "-", 1),
        f"{prefix}short",
        f" {generated}",
        f"{generated}\x00",
        f"{prefix}{'g' * 32}",
    ):
        with pytest.raises(KnowledgeValidationError):
            validator(invalid)  # type: ignore[operator]


def test_external_commands_are_strict_safe_and_hide_raw_content() -> None:
    command = CreateKnowledgeDocumentCommand(
        title="Guide.md",
        source_type=KnowledgeSourceType.markdown,
        content="# Guide",
        source_uri=None,
        target_session_id=None,
    )
    assert command.source_type is KnowledgeSourceType.markdown

    with pytest.raises(ValidationError):
        CreateKnowledgeBaseCommand(name="Docs", embedding_model="client/model")  # type: ignore[call-arg]
    assert (
        CreateKnowledgeDocumentCommand(
            title="Guide",
            source_type="markdown",
            content="safe",
        ).source_type
        is KnowledgeSourceType.markdown
    )
    with pytest.raises(ValidationError):
        CreateKnowledgeDocumentCommand(
            title=123,
            source_type=KnowledgeSourceType.markdown,
            content="safe",
        )

    secret = "raw-secret-content"
    with pytest.raises(ValidationError) as caught:
        CreateKnowledgeDocumentCommand(
            title="Guide",
            source_type=KnowledgeSourceType.text,
            content=f"{secret}\x00",
        )
    assert secret not in str(caught.value)


def test_content_sha256_is_exact_and_rejects_storage_unsafe_text() -> None:
    assert (
        content_sha256("Keel\n")
        == "5c5d33177a67fd7bb560c1ffbf47377823c6abaa4175e01d6fb7dde2410a588e"
    )
    assert content_sha256("Keel") != content_sha256("Keel\n")
    with pytest.raises(KnowledgeValidationError, match="content_invalid"):
        content_sha256("secret\x00payload")
    with pytest.raises(KnowledgeValidationError, match="content_invalid") as caught:
        content_sha256("secret\ud800payload")
    assert "secret" not in caught.value.public_message


def test_index_fingerprint_changes_with_every_index_input() -> None:
    base = index_fingerprint(
        "a" * 64,
        KnowledgeSourceType.markdown,
        "keel-char-v1",
        "fake/embed",
        16,
        1600,
        200,
    )
    alternatives = (
        index_fingerprint(
            "b" * 64,
            KnowledgeSourceType.markdown,
            "keel-char-v1",
            "fake/embed",
            16,
            1600,
            200,
        ),
        index_fingerprint(
            "a" * 64,
            KnowledgeSourceType.text,
            "keel-char-v1",
            "fake/embed",
            16,
            1600,
            200,
        ),
        index_fingerprint(
            "a" * 64,
            KnowledgeSourceType.markdown,
            "keel-char-v2",
            "fake/embed",
            16,
            1600,
            200,
        ),
        index_fingerprint(
            "a" * 64,
            KnowledgeSourceType.markdown,
            "keel-char-v1",
            "other/embed",
            16,
            1600,
            200,
        ),
        index_fingerprint(
            "a" * 64,
            KnowledgeSourceType.markdown,
            "keel-char-v1",
            "fake/embed",
            32,
            1600,
            200,
        ),
        index_fingerprint(
            "a" * 64,
            KnowledgeSourceType.markdown,
            "keel-char-v1",
            "fake/embed",
            16,
            1700,
            200,
        ),
        index_fingerprint(
            "a" * 64,
            KnowledgeSourceType.markdown,
            "keel-char-v1",
            "fake/embed",
            16,
            1600,
            100,
        ),
    )
    assert len(base) == 64
    assert all(base != candidate for candidate in alternatives)


def test_index_fingerprint_uses_unambiguous_canonical_json() -> None:
    first = index_fingerprint(
        "a" * 64,
        KnowledgeSourceType.text,
        "chunker|model",
        "embed",
        16,
        1600,
        200,
    )
    second = index_fingerprint(
        "a" * 64,
        KnowledgeSourceType.text,
        "chunker",
        "model|embed",
        16,
        1600,
        200,
    )
    assert first != second


def test_source_type_from_mime_type_is_strict_and_public() -> None:
    assert knowledge_source_type_from_mime_type("text/plain") is KnowledgeSourceType.text
    assert knowledge_source_type_from_mime_type("text/markdown") is KnowledgeSourceType.markdown

    for invalid in ("text/html", "text/plain; charset=utf-8", " text/plain", ""):
        with pytest.raises(KnowledgeValidationError, match="invalid_knowledge_input"):
            knowledge_source_type_from_mime_type(invalid)


def test_document_version_record_persists_strict_chunk_settings() -> None:
    now = datetime(2026, 7, 15, 8, 0, tzinfo=UTC)
    record = KnowledgeDocumentVersionRecord(
        id=new_knowledge_version_id(),
        scope_id="web:local",
        kb_id=new_knowledge_base_id(),
        document_id=new_knowledge_document_id(),
        version=1,
        content="# Guide",
        content_sha256=content_sha256("# Guide"),
        index_fingerprint="a" * 64,
        mime_type="text/markdown",
        chunking_version="keel-char-v1",
        target_chars=1600,
        overlap_chars=200,
        ingest_job_id=None,
        status=KnowledgeVersionStatus.pending,
        error_kind=None,
        error_message=None,
        created_at=now,
        activated_at=None,
        deleted_at=None,
        purged_at=None,
    )

    serialized = asdict(record)
    assert serialized["target_chars"] == 1600
    assert serialized["overlap_chars"] == 200

    with pytest.raises(ValueError, match="target_chars"):
        replace(record, target_chars=0)
    with pytest.raises(ValueError, match="integer"):
        replace(record, target_chars=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="overlap_chars"):
        replace(record, overlap_chars=-1)
    with pytest.raises(ValueError, match="integer"):
        replace(record, overlap_chars=1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="chunk overlap"):
        replace(record, overlap_chars=1600)


def test_request_fingerprint_is_canonical_and_path_bound() -> None:
    kb_id = new_knowledge_base_id()
    document_id = new_knowledge_document_id()
    first = CreateKnowledgeDocumentCommand(
        title="Guide",
        source_type=KnowledgeSourceType.text,
        content="Install Keel.",
    )
    second = CreateKnowledgeDocumentCommand(
        content="Install Keel.",
        source_type=KnowledgeSourceType.text,
        title="Guide",
        source_uri=None,
        target_session_id=None,
    )

    digest = request_fingerprint(
        "post",
        KnowledgeOperation.create_document,
        {"document_id": document_id, "kb_id": kb_id},
        first,
    )
    assert digest == request_fingerprint(
        "POST",
        KnowledgeOperation.create_document,
        {"kb_id": kb_id, "document_id": document_id},
        second,
    )
    assert digest == canonical_request_fingerprint(
        "POST",
        KnowledgeOperation.create_document,
        {"kb_id": kb_id, "document_id": document_id},
        second,
    )
    assert digest != request_fingerprint(
        "PUT",
        KnowledgeOperation.update_document,
        {"kb_id": kb_id, "document_id": document_id},
        second,
    )
    assert digest != request_fingerprint(
        "POST",
        KnowledgeOperation.create_document,
        {"kb_id": new_knowledge_base_id(), "document_id": document_id},
        second,
    )


def test_chunk_citation_and_search_models_are_strict() -> None:
    kb_id = new_knowledge_base_id()
    document_id = new_knowledge_document_id()
    version_id = new_knowledge_version_id()
    chunk_id = new_knowledge_chunk_id()
    citation = KnowledgeCitation(
        id="cite_1",
        kb_id=kb_id,
        document_id=document_id,
        document_version_id=version_id,
        chunk_id=chunk_id,
        title="Guide.md",
        source_uri=None,
        ordinal=0,
        char_start=0,
        char_end=5,
        label="Guide.md#chunk-1",
    )
    hit = KnowledgeHit(snippet="Keel.", rank=1, citation=citation)
    status = KnowledgeSearchStatus(mode=KnowledgeSearchMode.lexical_degraded)
    assert hit.heading_path == []
    assert status.semantic_error is None

    with pytest.raises(ValidationError):
        KnowledgeCitation(
            **{
                **citation.model_dump(),
                "char_end": -1,
            }
        )
    with pytest.raises(ValidationError):
        KnowledgeCitation(
            **{
                **citation.model_dump(),
                "kb_id": "not-a-kb-id",
            }
        )
    with pytest.raises(ValidationError):
        KnowledgeSearchStatus(
            mode=KnowledgeSearchMode.lexical_degraded,
            semantic_error="provider said secret details",
        )


def test_chunk_draft_is_frozen_and_validates_offsets() -> None:
    draft = ChunkDraft(
        ordinal=0,
        text="hello",
        char_start=0,
        char_end=5,
        content_hash=content_sha256("hello"),
        heading_path=("Guide",),
    )
    with pytest.raises(FrozenInstanceError):
        draft.ordinal = 1  # type: ignore[misc]
    with pytest.raises(ValueError, match="char_end"):
        ChunkDraft(
            ordinal=0,
            text="hello",
            char_start=2,
            char_end=1,
            content_hash=content_sha256("hello"),
            heading_path=(),
        )


def test_frozen_dataclass_integer_fields_are_strict() -> None:
    digest = content_sha256("hello")
    kb_id = new_knowledge_base_id()

    with pytest.raises(ValueError, match="integer"):
        ChunkDraft(
            ordinal=0.5,  # type: ignore[arg-type]
            text="hello",
            char_start=0,
            char_end=5,
            content_hash=digest,
            heading_path=(),
        )
    with pytest.raises(ValueError, match="integer"):
        KnowledgeBaseCreate(
            name="Docs",
            description=None,
            embedding_model="fake/embed",
            embedding_dim=True,
        )
    with pytest.raises(ValueError, match="integer"):
        KnowledgeDocumentVersionCreate(
            kb_id=kb_id,
            title="Guide",
            source_type=KnowledgeSourceType.text,
            content="hello",
            mime_type="text/plain",
            chunking_version="keel-char-v1",
            target_chars=1600.5,  # type: ignore[arg-type]
            overlap_chars=200,
        )
    with pytest.raises(ValueError, match="integer"):
        KnowledgeChunkWrite(
            ordinal=0,
            text="hello",
            char_start=0,
            char_end=5.0,  # type: ignore[arg-type]
            content_hash=digest,
            heading_path=(),
            metadata={},
            model="fake/embed",
            dim=3,
            embedding=(1.0, 0.0, 0.0),
        )
    with pytest.raises(ValueError, match="integer"):
        KnowledgeChunkReplacementResult(
            document_version_id=new_knowledge_version_id(),
            chunk_count=1.0,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="integer"):
        KnowledgePurgeResult(
            documents_purged=0,
            versions_purged=False,
            chunks_removed=0,
        )


def test_public_knowledge_errors_are_bounded_and_stable() -> None:
    mismatch = KnowledgeEmbeddingMismatch()
    assert mismatch.code == "embedding_configuration_mismatch"
    assert "embedding" in mismatch.public_message.lower()
    assert len(mismatch.code) <= 128
    assert len(mismatch.public_message) <= 512

    with pytest.raises(ValueError, match="code"):
        KnowledgeValidationError("x" * 129, "safe")
    with pytest.raises(ValueError, match="public_message"):
        KnowledgeValidationError("invalid_input", "x" * 513)


def test_root_package_exports_key_knowledge_contracts() -> None:
    import keel_core

    assert keel_core.KnowledgeStore.__name__ == "KnowledgeStore"
    assert keel_core.InMemoryKnowledgeStore.__name__ == "InMemoryKnowledgeStore"
    assert keel_core.canonical_request_fingerprint is canonical_request_fingerprint
    assert keel_core.index_fingerprint is index_fingerprint
