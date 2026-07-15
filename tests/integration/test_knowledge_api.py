"""Integration: idempotent scope-bound Knowledge REST API and RBAC."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.embeddings import FakeEmbedder
from keel_core.jobs import PostgresJobStore
from keel_core.knowledge import (
    CreateKnowledgeDocumentCommand,
    KnowledgeBaseCreate,
    KnowledgeChunkReplacement,
    KnowledgeChunkWrite,
    KnowledgeDocumentVersionCreate,
    KnowledgeIdempotencyBegin,
    KnowledgeOperation,
    KnowledgeVersionFailure,
    PostgresKnowledgeStore,
    canonical_request_fingerprint,
    content_sha256,
)
from keel_core.knowledge.search import KnowledgeSearcher
from keel_core.knowledge.service import KNOWLEDGE_CHUNKING_VERSION, KnowledgeService
from keel_server.api import knowledge as knowledge_api
from keel_server.api import v1
from keel_server.auth import parse_api_keys

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 16, 3, 0, tzinfo=UTC)
_MODEL = "fake/knowledge-api"
_DIM = 3


@dataclass(slots=True)
class _ApiFixture:
    client: httpx.AsyncClient
    scope: str
    store: PostgresKnowledgeStore
    jobs: PostgresJobStore
    dispatched: list[tuple[str, str]]


@pytest_asyncio.fixture
async def knowledge_client(migrated_db: AsyncEngine) -> AsyncIterator[_ApiFixture]:
    scope = "api:knowledge"
    settings = Settings(
        embedding_model=_MODEL,
        embedding_dim=_DIM,
        knowledge_document_max_bytes=64,
        knowledge_chunk_target_chars=32,
        knowledge_chunk_overlap_chars=4,
    )
    store = PostgresKnowledgeStore(
        migrated_db,
        scope,
        document_max_bytes=settings.knowledge_document_max_bytes,
    )
    jobs = PostgresJobStore(migrated_db, scope)
    searcher = KnowledgeSearcher(migrated_db, scope, FakeEmbedder(dim=_DIM, model=_MODEL))
    dispatched: list[tuple[str, str]] = []

    async def dispatch(scope_id: str, job_id: str) -> None:
        dispatched.append((scope_id, job_id))

    service = KnowledgeService(
        store,
        jobs,
        settings,
        searcher=searcher,
        dispatch_job=dispatch,
        embedding_model=_MODEL,
        embedding_dim=_DIM,
    )
    app = FastAPI()
    knowledge_api.register_exception_handlers(app)
    app.include_router(knowledge_api.router)
    app.include_router(v1.router)
    app.state.knowledge = service
    app.state.jobs = jobs
    app.state.durable_scope = scope
    app.state.api_keys = parse_api_keys("vw:viewer,op:operator")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield _ApiFixture(client, scope, store, jobs, dispatched)


def _operator(key: str) -> dict[str, str]:
    return {"X-API-Key": "op", "Idempotency-Key": key}


def _document_body(content: str = "Install Keel from the release package.") -> dict[str, object]:
    return {
        "title": "Guide.md",
        "source_type": "markdown",
        "content": content,
        "source_uri": "https://example.test/guide",
        "target_session_id": None,
    }


async def _create_base(fixture: _ApiFixture, *, key: str = "create-base") -> dict[str, object]:
    response = await fixture.client.post(
        "/v1/knowledge-bases",
        json={"name": "Product docs", "description": "Keel documentation"},
        headers=_operator(key),
    )
    assert response.status_code == 201
    return response.json()


async def _activate_version(
    fixture: _ApiFixture,
    kb_id: str,
    document_id: str,
    version_id: str,
    *,
    text: str,
) -> None:
    await fixture.store.mark_indexing(kb_id, document_id, version_id, now=_NOW)
    await fixture.store.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=kb_id,
            document_id=document_id,
            document_version_id=version_id,
            chunks=(
                KnowledgeChunkWrite(
                    ordinal=0,
                    text=text,
                    char_start=0,
                    char_end=len(text),
                    content_hash=content_sha256(text),
                    heading_path=("Guide",),
                    metadata={},
                    model=_MODEL,
                    dim=_DIM,
                    embedding=(1.0, 0.0, 0.0),
                ),
            ),
        ),
        now=_NOW,
    )
    activated = await fixture.store.activate_version(
        kb_id,
        document_id,
        version_id,
        now=_NOW,
    )
    assert activated.activated


async def test_crud_rbac_idempotency_search_and_disabled_delete(
    knowledge_client: _ApiFixture,
) -> None:
    fixture = knowledge_client
    viewer = {"X-API-Key": "vw"}
    body = {"name": "Product docs", "description": "Keel documentation"}

    assert (
        await fixture.client.post(
            "/v1/knowledge-bases",
            json=body,
            headers={"X-API-Key": "vw", "Idempotency-Key": "viewer-denied"},
        )
    ).status_code == 403

    created = await _create_base(fixture)
    kb_id = str(created["id"])
    assert created["embedding_model"] == _MODEL
    assert created["embedding_dim"] == _DIM

    replay = await fixture.client.post(
        "/v1/knowledge-bases",
        json=body,
        headers=_operator("create-base"),
    )
    assert replay.status_code == 201
    assert replay.json()["id"] == kb_id
    conflict = await fixture.client.post(
        "/v1/knowledge-bases",
        json={"name": "Different", "description": None},
        headers=_operator("create-base"),
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "idempotency_key_reused"
    assert [
        row["id"]
        for row in (await fixture.client.get("/v1/knowledge-bases", headers=viewer)).json()
    ] == [kb_id]

    missing_key = await fixture.client.post(
        f"/v1/knowledge-bases/{kb_id}/documents",
        json=_document_body(),
        headers={"X-API-Key": "op"},
    )
    assert missing_key.status_code == 422
    assert "Install Keel" not in missing_key.text

    document_response = await fixture.client.post(
        f"/v1/knowledge-bases/{kb_id}/documents",
        json=_document_body(),
        headers=_operator("create-document"),
    )
    assert document_response.status_code == 202
    document_body = document_response.json()
    document_id = document_body["document"]["id"]
    version_id = document_body["version"]["id"]
    ingest_job_id = document_body["job"]["id"]
    assert document_body["version"]["ingest_job_id"] == ingest_job_id
    assert document_body["job"]["kind"] == "knowledge.ingest"
    assert document_body["job"]["cancel_mode"] == "cooperative"
    assert fixture.dispatched[-1] == (fixture.scope, ingest_job_id)

    replayed_document = await fixture.client.post(
        f"/v1/knowledge-bases/{kb_id}/documents",
        json=_document_body(),
        headers=_operator("create-document"),
    )
    assert replayed_document.status_code == 202
    assert replayed_document.json()["replayed"] is True
    assert replayed_document.json()["document"]["id"] == document_id
    assert replayed_document.json()["version"]["id"] == version_id
    assert replayed_document.json()["job"]["id"] == ingest_job_id

    await _activate_version(
        fixture,
        kb_id,
        document_id,
        version_id,
        text="Install Keel from the release package.",
    )
    search = await fixture.client.get(
        f"/v1/knowledge-bases/{kb_id}/search",
        params={"q": "Install Keel", "k": 5},
        headers=viewer,
    )
    assert search.status_code == 200
    assert search.json()["hits"][0]["citation"]["document_version_id"] == version_id

    reindex = await fixture.client.post(
        f"/v1/knowledge-bases/{kb_id}/documents/{document_id}/reindex",
        json={"target_session_id": None},
        headers=_operator("reindex-document"),
    )
    assert reindex.status_code == 202
    assert reindex.json()["version"]["id"] == version_id
    assert reindex.json()["job"]["id"] == ingest_job_id

    deleted = await fixture.client.delete(
        f"/v1/knowledge-bases/{kb_id}/documents/{document_id}",
        headers=_operator("delete-document"),
    )
    assert deleted.status_code == 202
    delete_job_id = deleted.json()["job"]["id"]
    assert deleted.json()["job"]["kind"] == "knowledge.delete"
    assert deleted.json()["job"]["cancel_mode"] == "disabled"
    assert (
        await fixture.client.get(
            f"/v1/knowledge-bases/{kb_id}/documents/{document_id}",
            headers=viewer,
        )
    ).status_code == 404
    cancel = await fixture.client.post(
        f"/v1/jobs/{delete_job_id}/cancel",
        headers={"X-API-Key": "op"},
    )
    assert cancel.status_code == 409


async def test_validation_recovery_and_cross_scope_are_fail_closed(
    knowledge_client: _ApiFixture,
    migrated_db: AsyncEngine,
) -> None:
    fixture = knowledge_client
    viewer = {"X-API-Key": "vw"}
    base = await _create_base(fixture, key="validation-base")
    kb_id = str(base["id"])

    oversized = await fixture.client.post(
        f"/v1/knowledge-bases/{kb_id}/documents",
        json=_document_body("x" * 65),
        headers=_operator("oversized-document"),
    )
    assert oversized.status_code == 413

    secret = "TOP-SECRET-RAW-INPUT"
    invalid = await fixture.client.post(
        f"/v1/knowledge-bases/{kb_id}/documents",
        json=_document_body(secret + "\u0000"),
        headers=_operator("invalid-document"),
    )
    assert invalid.status_code == 422
    assert secret not in invalid.text
    assert (
        await fixture.client.post(
            "/v1/knowledge-bases",
            json={"name": "Bad header", "description": None},
            headers=_operator("x" * 513),
        )
    ).status_code == 422

    created = await fixture.client.post(
        f"/v1/knowledge-bases/{kb_id}/documents",
        json=_document_body("first attempt"),
        headers=_operator("first-attempt"),
    )
    assert created.status_code == 202
    resource = created.json()
    job_id = resource["job"]["id"]
    await fixture.store.mark_indexing(
        kb_id,
        resource["document"]["id"],
        resource["version"]["id"],
    )
    await fixture.store.mark_version_failed(
        KnowledgeVersionFailure(
            kb_id=kb_id,
            document_id=resource["document"]["id"],
            document_version_id=resource["version"]["id"],
            ingest_job_id=job_id,
            error_kind="embedding_failed",
            error_message="Embedding failed.",
        )
    )
    recovered = await fixture.client.put(
        f"/v1/knowledge-bases/{kb_id}/documents/{resource['document']['id']}",
        json=_document_body("recovery content"),
        headers=_operator("recovery-update"),
    )
    assert recovered.status_code == 202
    assert recovered.json()["version"]["id"] != resource["version"]["id"]

    other_store = PostgresKnowledgeStore(migrated_db, "api:other")
    other = await other_store.create_base(
        KnowledgeBaseCreate(
            name="Other",
            description=None,
            embedding_model=_MODEL,
            embedding_dim=_DIM,
        )
    )
    assert (
        await fixture.client.get(
            f"/v1/knowledge-bases/{other.id}",
            headers=viewer,
        )
    ).status_code == 404
    assert (
        await fixture.client.get(
            "/v1/knowledge-bases/not-a-knowledge-id",
            headers=viewer,
        )
    ).status_code == 404


async def test_retry_after_resource_commit_attaches_the_same_job(
    knowledge_client: _ApiFixture,
) -> None:
    fixture = knowledge_client
    base = await _create_base(fixture, key="crash-base")
    kb_id = str(base["id"])
    request = CreateKnowledgeDocumentCommand.model_validate(_document_body("crash recovery"))
    operation = KnowledgeOperation.create_document
    fingerprint = canonical_request_fingerprint(
        "POST",
        operation,
        {"kb_id": kb_id},
        request,
    )
    committed = await fixture.store.create_document_version_idempotent(
        KnowledgeDocumentVersionCreate(
            kb_id=kb_id,
            title=request.title,
            source_type=request.source_type,
            content=request.content,
            mime_type="text/markdown",
            chunking_version=KNOWLEDGE_CHUNKING_VERSION,
            target_chars=32,
            overlap_chars=4,
            source_uri=request.source_uri,
        ),
        KnowledgeIdempotencyBegin(operation, "crash-document", fingerprint),
    )
    assert committed.ledger.job_id is None
    assert committed.resource.version.ingest_job_id is None

    recovered = await fixture.client.post(
        f"/v1/knowledge-bases/{kb_id}/documents",
        json=_document_body("crash recovery"),
        headers=_operator("crash-document"),
    )
    assert recovered.status_code == 202
    body = recovered.json()
    assert body["document"]["id"] == committed.resource.document.id
    assert body["version"]["id"] == committed.resource.version.id
    assert body["version"]["ingest_job_id"] == body["job"]["id"]
    ledger = await fixture.store.get_idempotent_request(operation, "crash-document")
    assert ledger is not None and ledger.job_id == body["job"]["id"]


async def test_create_base_requires_configured_embeddings(
    migrated_db: AsyncEngine,
) -> None:
    scope = "api:no-embeddings"
    settings = Settings(embedding_model="")
    store = PostgresKnowledgeStore(migrated_db, scope)
    jobs = PostgresJobStore(migrated_db, scope)
    app = FastAPI()
    knowledge_api.register_exception_handlers(app)
    app.include_router(knowledge_api.router)
    app.state.knowledge = KnowledgeService(
        store,
        jobs,
        settings,
        embedding_model=None,
        embedding_dim=None,
    )
    app.state.durable_scope = scope
    app.state.api_keys = parse_api_keys("op:operator")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/v1/knowledge-bases",
            json={"name": "Unavailable", "description": None},
            headers={"X-API-Key": "op", "Idempotency-Key": "no-embeddings"},
        )
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "embeddings_not_configured"
