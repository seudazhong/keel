"""Scope-bound Knowledge Base REST API with idempotent mutations."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from keel_core.api import JobResponse
from keel_core.knowledge.models import (
    CreateKnowledgeBaseCommand,
    CreateKnowledgeDocumentCommand,
    DeleteKnowledgeCommand,
    KnowledgeBaseRecord,
    KnowledgeBaseStatus,
    KnowledgeConflict,
    KnowledgeDocumentRecord,
    KnowledgeDocumentStatus,
    KnowledgeDocumentVersionRecord,
    KnowledgeError,
    KnowledgeHit,
    KnowledgeNotFound,
    KnowledgePublicCode,
    KnowledgeSearchStatus,
    KnowledgeSourceType,
    KnowledgeStorageError,
    KnowledgeValidationError,
    KnowledgeVersionStatus,
    ReindexKnowledgeDocumentCommand,
    UpdateKnowledgeDocumentCommand,
    validate_knowledge_base_id,
    validate_knowledge_document_id,
)
from keel_core.knowledge.service import (
    KnowledgeBaseDeleteJobResult,
    KnowledgeDocumentDeleteJobResult,
    KnowledgeDocumentJobResult,
    KnowledgeService,
)
from keel_server.endpoint_auth import (
    EndpointAuth,
    EndpointPrivilege,
    require_authenticated,
    require_privilege,
)

router = APIRouter(
    prefix="/v1/knowledge-bases",
    tags=["knowledge"],
    dependencies=[Depends(require_authenticated)],
)


class _ResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)


class KnowledgeBaseResponse(_ResponseModel):
    id: str
    name: str
    description: str | None
    embedding_model: str
    embedding_dim: int
    status: KnowledgeBaseStatus
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None

    @classmethod
    def from_record(cls, record: KnowledgeBaseRecord) -> KnowledgeBaseResponse:
        return cls.model_validate(record)


class KnowledgeDocumentResponse(_ResponseModel):
    id: str
    kb_id: str
    title: str
    source_type: KnowledgeSourceType
    source_uri: str | None
    status: KnowledgeDocumentStatus
    desired_version_id: str | None
    active_version_id: str | None
    last_error_kind: str | None
    last_error_message: str | None
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None

    @classmethod
    def from_record(cls, record: KnowledgeDocumentRecord) -> KnowledgeDocumentResponse:
        return cls.model_validate(record)


class KnowledgeVersionResponse(_ResponseModel):
    id: str
    kb_id: str
    document_id: str
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

    @classmethod
    def from_record(cls, record: KnowledgeDocumentVersionRecord) -> KnowledgeVersionResponse:
        return cls.model_validate(record)


class KnowledgeDocumentDetailResponse(_ResponseModel):
    document: KnowledgeDocumentResponse
    versions: list[KnowledgeVersionResponse]


class KnowledgeDocumentJobResponse(_ResponseModel):
    document: KnowledgeDocumentResponse
    version: KnowledgeVersionResponse
    job: JobResponse
    replayed: bool

    @classmethod
    def from_result(cls, result: KnowledgeDocumentJobResult) -> KnowledgeDocumentJobResponse:
        return cls(
            document=KnowledgeDocumentResponse.from_record(result.document),
            version=KnowledgeVersionResponse.from_record(result.version),
            job=JobResponse.from_record(result.job),
            replayed=result.replayed,
        )


class KnowledgeBaseDeleteResponse(_ResponseModel):
    base: KnowledgeBaseResponse
    job: JobResponse
    replayed: bool

    @classmethod
    def from_result(cls, result: KnowledgeBaseDeleteJobResult) -> KnowledgeBaseDeleteResponse:
        return cls(
            base=KnowledgeBaseResponse.from_record(result.base),
            job=JobResponse.from_record(result.job),
            replayed=result.replayed,
        )


class KnowledgeDocumentDeleteResponse(_ResponseModel):
    document: KnowledgeDocumentResponse
    job: JobResponse
    replayed: bool

    @classmethod
    def from_result(
        cls,
        result: KnowledgeDocumentDeleteJobResult,
    ) -> KnowledgeDocumentDeleteResponse:
        return cls(
            document=KnowledgeDocumentResponse.from_record(result.document),
            job=JobResponse.from_record(result.job),
            replayed=result.replayed,
        )


class KnowledgeSearchResponse(_ResponseModel):
    hits: list[KnowledgeHit]
    status: KnowledgeSearchStatus


def _service(request: Request, auth: EndpointAuth) -> KnowledgeService:
    """Resolve a Knowledge service bound to the caller's derived data-plane scope.

    A per-request scoped service is built from the canonical ``auth.scope_id`` (an OIDC user's
    ``agent:<org>/<agent>``, a bound/global machine credential's selected Agent, or the non-cloud
    ``web:local`` local-preview scope) via the app's bounded ``knowledge_factory`` — so an
    authenticated call never touches a foreign scope's Knowledge and a cross-org/Agent KB/document
    id is invisible (404). A directly-wired singleton (lite profile / unit doubles) is used only
    when it already matches the resolved scope; a mismatch fails closed rather than leak another
    tenant's Knowledge.
    """
    scope = auth.scope_id
    factory = getattr(request.app.state, "knowledge_factory", None)
    if factory is not None:
        service: KnowledgeService | None = factory(scope)
        if service is not None:
            if service.scope_id != scope:
                raise HTTPException(
                    status.HTTP_503_SERVICE_UNAVAILABLE, "knowledge scope misconfigured"
                )
            return service
    singleton: KnowledgeService | None = getattr(request.app.state, "knowledge", None)
    if singleton is not None and singleton.scope_id == scope:
        return singleton
    raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "knowledge unavailable")


def _kb_id(value: str) -> str:
    try:
        return validate_knowledge_base_id(value)
    except KnowledgeValidationError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "knowledge base not found") from None


def _document_id(value: str) -> str:
    try:
        return validate_knowledge_document_id(value)
    except KnowledgeValidationError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "knowledge document not found") from None


def _idempotency_key(value: str) -> str:
    return value


@router.post(
    "",
    response_model=KnowledgeBaseResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_base(
    body: CreateKnowledgeBaseCommand,
    request: Request,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> KnowledgeBaseResponse:
    result = await _service(request, auth).create_base(body, _idempotency_key(idempotency_key))
    return KnowledgeBaseResponse.from_record(result.resource)


@router.get("", response_model=list[KnowledgeBaseResponse])
async def list_bases(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> list[KnowledgeBaseResponse]:
    return [
        KnowledgeBaseResponse.from_record(record)
        for record in await _service(request, auth).list_bases()
    ]


@router.get("/{kb_id}", response_model=KnowledgeBaseResponse)
async def get_base(
    kb_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> KnowledgeBaseResponse:
    record = await _service(request, auth).get_base(_kb_id(kb_id))
    if record is None or record.status is KnowledgeBaseStatus.deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "knowledge base not found")
    return KnowledgeBaseResponse.from_record(record)


@router.delete(
    "/{kb_id}",
    response_model=KnowledgeBaseDeleteResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def delete_base(
    kb_id: str,
    request: Request,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> KnowledgeBaseDeleteResponse:
    result = await _service(request, auth).delete_base(
        _kb_id(kb_id),
        DeleteKnowledgeCommand(),
        _idempotency_key(idempotency_key),
    )
    return KnowledgeBaseDeleteResponse.from_result(result)


@router.get("/{kb_id}/documents", response_model=list[KnowledgeDocumentResponse])
async def list_documents(
    kb_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> list[KnowledgeDocumentResponse]:
    return [
        KnowledgeDocumentResponse.from_record(record)
        for record in await _service(request, auth).list_documents(_kb_id(kb_id))
    ]


@router.post(
    "/{kb_id}/documents",
    response_model=KnowledgeDocumentJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_document(
    kb_id: str,
    body: CreateKnowledgeDocumentCommand,
    request: Request,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> KnowledgeDocumentJobResponse:
    result = await _service(request, auth).create_document(
        _kb_id(kb_id),
        body,
        _idempotency_key(idempotency_key),
    )
    return KnowledgeDocumentJobResponse.from_result(result)


@router.get(
    "/{kb_id}/documents/{document_id}",
    response_model=KnowledgeDocumentDetailResponse,
)
async def get_document(
    kb_id: str,
    document_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> KnowledgeDocumentDetailResponse:
    base_id = _kb_id(kb_id)
    doc_id = _document_id(document_id)
    service = _service(request, auth)
    document = await service.get_document(base_id, doc_id)
    if document is None or document.status is KnowledgeDocumentStatus.deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "knowledge document not found")
    versions = await service.list_versions(base_id, doc_id)
    return KnowledgeDocumentDetailResponse(
        document=KnowledgeDocumentResponse.from_record(document),
        versions=[KnowledgeVersionResponse.from_record(version) for version in versions],
    )


@router.put(
    "/{kb_id}/documents/{document_id}",
    response_model=KnowledgeDocumentJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def update_document(
    kb_id: str,
    document_id: str,
    body: UpdateKnowledgeDocumentCommand,
    request: Request,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> KnowledgeDocumentJobResponse:
    result = await _service(request, auth).update_document(
        _kb_id(kb_id),
        _document_id(document_id),
        body,
        _idempotency_key(idempotency_key),
    )
    return KnowledgeDocumentJobResponse.from_result(result)


@router.post(
    "/{kb_id}/documents/{document_id}/reindex",
    response_model=KnowledgeDocumentJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def reindex_document(
    kb_id: str,
    document_id: str,
    body: ReindexKnowledgeDocumentCommand,
    request: Request,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> KnowledgeDocumentJobResponse:
    result = await _service(request, auth).reindex_document(
        _kb_id(kb_id),
        _document_id(document_id),
        body,
        _idempotency_key(idempotency_key),
    )
    return KnowledgeDocumentJobResponse.from_result(result)


@router.delete(
    "/{kb_id}/documents/{document_id}",
    response_model=KnowledgeDocumentDeleteResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def delete_document(
    kb_id: str,
    document_id: str,
    request: Request,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> KnowledgeDocumentDeleteResponse:
    result = await _service(request, auth).delete_document(
        _kb_id(kb_id),
        _document_id(document_id),
        DeleteKnowledgeCommand(),
        _idempotency_key(idempotency_key),
    )
    return KnowledgeDocumentDeleteResponse.from_result(result)


@router.get("/{kb_id}/search", response_model=KnowledgeSearchResponse)
async def search(
    kb_id: str,
    request: Request,
    q: Annotated[str, Query(min_length=1, max_length=2_000)],
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
    k: Annotated[int, Query(ge=1, le=10)] = 5,
) -> KnowledgeSearchResponse:
    hits, search_status = await _service(request, auth).search(_kb_id(kb_id), q, k=k)
    return KnowledgeSearchResponse(hits=hits, status=search_status)


async def _knowledge_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, KnowledgeError)
    code = exc.code
    if isinstance(exc, KnowledgeNotFound) or code == KnowledgePublicCode.invalid_id.value:
        http_status = status.HTTP_404_NOT_FOUND
    elif isinstance(exc, KnowledgeConflict):
        http_status = status.HTTP_409_CONFLICT
    elif code == KnowledgePublicCode.content_too_large.value:
        http_status = status.HTTP_413_CONTENT_TOO_LARGE
    elif isinstance(exc, KnowledgeStorageError) or code in {
        KnowledgePublicCode.embeddings_not_configured.value,
        KnowledgePublicCode.storage_failure.value,
    }:
        http_status = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        http_status = status.HTTP_422_UNPROCESSABLE_CONTENT
    return JSONResponse(
        status_code=http_status,
        content={"detail": {"code": code, "message": exc.public_message}},
    )


async def _sanitized_validation_handler(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    errors: list[dict[str, Any]] = []
    for raw in exc.errors():
        errors.append(
            {
                "loc": list(raw.get("loc", ()))[:16],
                "type": str(raw.get("type", "validation_error"))[:128],
                "msg": str(raw.get("msg", "Invalid request."))[:512],
            }
        )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": errors},
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(KnowledgeError, _knowledge_error_handler)
    app.add_exception_handler(RequestValidationError, _sanitized_validation_handler)


__all__ = [
    "KnowledgeBaseDeleteResponse",
    "KnowledgeBaseResponse",
    "KnowledgeDocumentDeleteResponse",
    "KnowledgeDocumentDetailResponse",
    "KnowledgeDocumentJobResponse",
    "KnowledgeDocumentResponse",
    "KnowledgeSearchResponse",
    "KnowledgeVersionResponse",
    "register_exception_handlers",
    "router",
]
