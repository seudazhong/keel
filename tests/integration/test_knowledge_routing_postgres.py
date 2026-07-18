"""Per-scope Knowledge routing + cross-scope Knowledge job dispatch (M3.6 finding 3).

Live-Postgres coverage for making the Knowledge API per-request scoped and driving its indexing
jobs across every per-Agent scope:

* the durable job + its global dispatch intent commit atomically (a lost enqueue leaves a
  discoverable intent; a failed intent write rolls the job back);
* the global ``job_dispatch_outbox`` indexes jobs across scopes and fences duplicate workers;
* the worker reconciler re-dispatches an Agent-scoped ingest job and ``run_job`` binds the job's
  own scope to actually index the document;
* a job's intent cascades away when the job is purged;
* two orgs' Knowledge is isolated over HTTP under both machine and OIDC auth, and a guessed
  cross-scope KB/document id is denied 404.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.embeddings import FakeEmbedder
from keel_core.identity import (
    IdentityService,
    InMemoryIdentityStore,
    LoggingAuditSink,
)
from keel_core.identity.models import AgentKind
from keel_core.identity.oidc import OIDCClaims, OIDCVerifier
from keel_core.job_dispatch import PostgresJobDispatchOutbox
from keel_core.jobs import JobStatus, PostgresJobStore
from keel_core.knowledge import (
    CreateKnowledgeDocumentCommand,
    KnowledgeSourceType,
    PostgresKnowledgeStore,
)
from keel_core.knowledge.jobs import KNOWLEDGE_INGEST_KIND
from keel_core.knowledge.search import KnowledgeSearcher
from keel_core.knowledge.service import KnowledgeService
from keel_core.scoping import derive_agent_scope
from keel_server.auth import parse_api_keys
from keel_worker.jobs import reconcile_job_dispatch_tick, run_job

pytestmark = pytest.mark.integration

_MODEL = "fake/knowledge-routing"
_DIM = 3


def _settings() -> Settings:
    return Settings(
        embedding_model=_MODEL,
        embedding_dim=_DIM,
        knowledge_document_max_bytes=4096,
        knowledge_chunk_target_chars=64,
        knowledge_chunk_overlap_chars=8,
    )


def _service(
    engine: AsyncEngine,
    scope: str,
    outbox: PostgresJobDispatchOutbox | None,
    dispatched: list[tuple[str, str]],
    *,
    enqueue_ok: bool = True,
) -> KnowledgeService:
    settings = _settings()
    store = PostgresKnowledgeStore(
        engine, scope, document_max_bytes=settings.knowledge_document_max_bytes
    )
    jobs = PostgresJobStore(engine, scope)
    searcher = KnowledgeSearcher(engine, scope, FakeEmbedder(dim=_DIM, model=_MODEL))

    async def dispatch(scope_id: str, job_id: str) -> None:
        if not enqueue_ok:
            raise RuntimeError("queue down")
        dispatched.append((scope_id, job_id))

    return KnowledgeService(
        store,
        jobs,
        settings,
        searcher=searcher,
        dispatch_job=dispatch,
        dispatch_outbox=outbox,
        embedding_model=_MODEL,
        embedding_dim=_DIM,
    )


async def _create_base(service: KnowledgeService, *, key: str) -> str:
    from keel_core.knowledge import CreateKnowledgeBaseCommand

    result = await service.create_base(
        CreateKnowledgeBaseCommand(name="Docs", description=None), key
    )
    return result.resource.id


def _doc(
    content: str = "Install Keel from the official release package archive.",
) -> CreateKnowledgeDocumentCommand:
    return CreateKnowledgeDocumentCommand(
        title="Guide.md",
        source_type=KnowledgeSourceType.markdown,
        content=content,
        source_uri=None,
        target_session_id=None,
    )


def _worker_ctx(
    engine: AsyncEngine,
    outbox: PostgresJobDispatchOutbox,
    enqueued: list[tuple[str, tuple[Any, ...]]],
) -> dict[str, Any]:
    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        enqueued.append((name, args))

    return {
        "engine": engine,
        "durable_scope": "web:local",
        "jobs": PostgresJobStore(engine, "web:local"),
        "job_dispatch_outbox": outbox,
        "enqueue": _enqueue,
        "job_settings": _settings(),
        "embedder": FakeEmbedder(dim=_DIM, model=_MODEL),
    }


# --- Atomic admission + intent semantics -----------------------------------------------------


async def test_admission_records_job_and_intent_atomically(migrated_db: AsyncEngine) -> None:
    scope = "agent:orga/ag1"
    outbox = PostgresJobDispatchOutbox(migrated_db)
    dispatched: list[tuple[str, str]] = []
    service = _service(migrated_db, scope, outbox, dispatched)
    kb_id = await _create_base(service, key="base")
    result = await service.create_document(kb_id, _doc(), "doc-1")

    intents = await outbox.claim_due(worker_id="w1")
    assert [(i.job_id, i.scope_id, i.kind) for i in intents] == [
        (result.job.id, scope, KNOWLEDGE_INGEST_KIND)
    ]
    assert dispatched[-1] == (scope, result.job.id)


async def test_enqueue_failure_returns_pending_and_intent_survives(
    migrated_db: AsyncEngine,
) -> None:
    # A queue-down dispatch after the durable commit must not fail the request: the accepted job
    # + its intent are committed so the reconciler heals delivery (dispatch_pending semantics).
    scope = "agent:orgp/agp"
    outbox = PostgresJobDispatchOutbox(migrated_db)
    dispatched: list[tuple[str, str]] = []
    service = _service(migrated_db, scope, outbox, dispatched, enqueue_ok=False)
    kb_id = await _create_base(service, key="base-p")
    result = await service.create_document(kb_id, _doc(), "doc-p")
    assert dispatched == []  # dispatch swallowed
    assert result.job.id in {i.job_id for i in await outbox.claim_due(worker_id="w1")}


async def test_intent_cascade_deleted_with_job(migrated_db: AsyncEngine) -> None:
    # ``job_dispatch_outbox.job_id`` FKs ``jobs(id) ON DELETE CASCADE``: a lifecycle purge that
    # deletes the job removes its dispatch intent (no orphaned metadata).
    scope = "agent:orgc/agc"
    outbox = PostgresJobDispatchOutbox(migrated_db)
    dispatched: list[tuple[str, str]] = []
    service = _service(migrated_db, scope, outbox, dispatched)
    kb_id = await _create_base(service, key="base-c")
    result = await service.create_document(kb_id, _doc(), "doc-c")
    assert await outbox.active_scopes() == {scope}

    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": scope})
        await conn.execute(text("DELETE FROM jobs WHERE id = :j"), {"j": result.job.id})
    assert await outbox.active_scopes() == set()  # cascade removed the intent


# --- Cross-scope outbox + reconciler ---------------------------------------------------------


async def test_outbox_leases_across_scopes_and_fences_duplicate_worker(
    migrated_db: AsyncEngine,
) -> None:
    outbox = PostgresJobDispatchOutbox(migrated_db)
    # The outbox FKs jobs(id), so create real jobs (in their own scopes) before the intents.
    service_a = _service(migrated_db, "agent:orga/agta", outbox, [])
    service_b = _service(migrated_db, "agent:orgb/agtb", outbox, [])
    kb_a = await _create_base(service_a, key="a")
    kb_b = await _create_base(service_b, key="b")
    job_a = (await service_a.create_document(kb_a, _doc(), "da")).job.id
    job_b = (await service_b.create_document(kb_b, _doc(), "db")).job.id
    assert await outbox.active_scopes() == {"agent:orga/agta", "agent:orgb/agtb"}

    # Anchor the claim clock to (real) now *after* the intents were recorded so the intents are
    # always due at claim time — the enqueue (create_document) and claim share one monotonic
    # clock, rather than pairing a real-now intent with a wall-clock-dependent fixed claim time.
    now = datetime.now(UTC)
    first = await outbox.claim_due(worker_id="w1", now=now, lease_seconds=60)
    assert {i.job_id for i in first} == {job_a, job_b}
    # A second worker is fenced out while the lease is live (SKIP LOCKED + lease window).
    assert await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=1)) == []
    # After the lease expires the intents are claimable again (crash recovery).
    third = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=120))
    assert {i.job_id for i in third} == {job_a, job_b}


async def test_reconciler_dispatches_agent_scoped_job_and_run_job_indexes(
    migrated_db: AsyncEngine,
) -> None:
    # End-to-end finding 3: a document created under a per-Agent scope with a lost enqueue is
    # re-dispatched by the cross-scope reconciler with the job's own scope, and ``run_job`` binds
    # that scope to actually index the document (never orphaned in web:local).
    scope = "agent:orgx/agx"
    outbox = PostgresJobDispatchOutbox(migrated_db)
    service = _service(migrated_db, scope, outbox, [], enqueue_ok=False)
    kb_id = await _create_base(service, key="base-x")
    created = await service.create_document(kb_id, _doc(), "doc-x")
    job_id = created.job.id

    enqueued: list[tuple[str, tuple[Any, ...]]] = []
    ctx = _worker_ctx(migrated_db, outbox, enqueued)
    dispatched = await reconcile_job_dispatch_tick(ctx)
    assert dispatched == 1
    assert ("run_job", (scope, job_id)) in enqueued

    # Execute the dispatched job: run_job builds the scoped job store + Knowledge registry.
    assert await run_job(ctx, scope, job_id) == JobStatus.succeeded.value
    # The version is now active/indexed and searchable within its scope.
    store = PostgresKnowledgeStore(migrated_db, scope)
    versions = await store.list_versions(kb_id, created.document.id)
    assert any(v.status.value == "active" for v in versions)


async def test_two_worker_reconcile_claims_once(migrated_db: AsyncEngine) -> None:
    scope = "agent:orgt/agt"
    outbox = PostgresJobDispatchOutbox(migrated_db)
    service = _service(migrated_db, scope, outbox, [], enqueue_ok=False)
    kb_id = await _create_base(service, key="base-t")
    job_id = (await service.create_document(kb_id, _doc(), "doc-t")).job.id
    # Anchor to now *after* recording the intent so the claim clock matches the enqueue clock.
    now = datetime.now(UTC)
    first = await outbox.claim_due(worker_id="w1", now=now)
    second = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=1))
    assert [i.job_id for i in first] == [job_id]
    assert second == []  # the racing worker is fenced out


async def test_crash_after_claim_reclaims_after_lease(migrated_db: AsyncEngine) -> None:
    scope = "agent:orgk/agk"
    outbox = PostgresJobDispatchOutbox(migrated_db)
    service = _service(migrated_db, scope, outbox, [], enqueue_ok=False)
    kb_id = await _create_base(service, key="base-k")
    job_id = (await service.create_document(kb_id, _doc(), "doc-k")).job.id
    # Anchor to now *after* recording the intent so the claim clock matches the enqueue clock.
    now = datetime.now(UTC)
    claimed = await outbox.claim_due(worker_id="w1", now=now, lease_seconds=60)
    assert [i.job_id for i in claimed] == [job_id]
    # Worker w1 "crashes" without acking. Before the lease expires no one else can claim it.
    assert await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=30)) == []
    # After the lease expires another worker reclaims it (crash recovery).
    reclaimed = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=120))
    assert [i.job_id for i in reclaimed] == [job_id]


# --- HTTP: two-org isolation under machine + OIDC auth ----------------------------------------


class _FakeVerifier(OIDCVerifier):  # type: ignore[misc]
    """An OIDC verifier double that returns pre-seeded claims for a known token."""

    def __init__(self, claims: OIDCClaims) -> None:
        self._claims = claims

    async def verify(self, token: str) -> OIDCClaims:  # noqa: D401
        return self._claims


async def _seed_two_orgs() -> tuple[IdentityService, dict[str, str]]:
    svc = IdentityService(
        InMemoryIdentityStore(), audit=LoggingAuditSink(), allow_jit_provisioning=True
    )
    owner = await svc.store.create_user(display_name="Owner", email=None)
    org_a = await svc.create_org(owner.id, slug="acme", display_name="Acme")
    org_b = await svc.create_org(owner.id, slug="globex", display_name="Globex")
    agent_a = await svc.create_agent(
        org_a.org_id, owner.id, kind=AgentKind.team, name="Agent Acme", persona=""
    )
    agent_b = await svc.create_agent(
        org_b.org_id, owner.id, kind=AgentKind.team, name="Agent Globex", persona=""
    )
    return svc, {
        "owner": owner.id,
        "org_a": org_a.org_id,
        "agent_a": agent_a.id,
        "org_b": org_b.org_id,
        "agent_b": agent_b.id,
    }


def _knowledge_app(
    engine: AsyncEngine,
    identity: IdentityService,
    *,
    api_keys: str = "",
    oidc_verifier: OIDCVerifier | None = None,
) -> TestClient:
    from keel_server.app import _build_job_store, _build_knowledge_service, create_app

    settings = _settings()
    outbox = PostgresJobDispatchOutbox(engine)
    embedder = FakeEmbedder(dim=_DIM, model=_MODEL)
    cache: dict[str, KnowledgeService] = {}

    async def _dispatch(scope_id: str, job_id: str) -> None:
        return None

    def _factory(scope_id: str) -> KnowledgeService | None:
        cached = cache.get(scope_id)
        if cached is not None:
            return cached
        service = _build_knowledge_service(
            engine,
            scope_id,
            settings,
            _build_job_store(engine, scope_id, settings),
            embedder=embedder,
            dispatch_job=_dispatch,
            dispatch_outbox=outbox,
        )
        if service is not None:
            cache[scope_id] = service
        return service

    app = create_app()
    app.state.engine = engine
    app.state.settings = settings
    app.state.durable_scope = "web:local"
    app.state.auth_required = True  # cloud mode: no open-mode fallback
    app.state.api_keys = parse_api_keys(api_keys)
    app.state.identity = identity
    app.state.oidc_verifier = oidc_verifier
    app.state.job_dispatch_outbox = outbox
    app.state.knowledge_factory = _factory
    app.state.knowledge_mutation_enabled = True
    return TestClient(app)


def _kb_body() -> dict[str, object]:
    return {"name": "Product docs", "description": None}


async def test_two_orgs_knowledge_is_isolated_under_machine_auth(migrated_db: AsyncEngine) -> None:
    svc, ids = await _seed_two_orgs()
    keys = (
        f"akey:operator:org={ids['org_a']}:agent={ids['agent_a']},"
        f"bkey:operator:org={ids['org_b']}:agent={ids['agent_b']}"
    )
    client = _knowledge_app(migrated_db, svc, api_keys=keys)
    hdr_a = {"X-API-Key": "akey"}
    hdr_b = {"X-API-Key": "bkey"}

    # Org A creates a KB + document in its own derived scope.
    created = client.post(
        "/v1/knowledge-bases", json=_kb_body(), headers={**hdr_a, "Idempotency-Key": "a-kb"}
    )
    assert created.status_code == 201
    kb_a = created.json()["id"]
    doc = client.post(
        f"/v1/knowledge-bases/{kb_a}/documents",
        json=_doc().model_dump(mode="json"),
        headers={**hdr_a, "Idempotency-Key": "a-doc"},
    )
    assert doc.status_code == 202
    doc_a = doc.json()["document"]["id"]

    # Org B cannot see org A's KB or document (a guessed cross-scope id is denied 404).
    assert client.get(f"/v1/knowledge-bases/{kb_a}", headers=hdr_b).status_code == 404
    assert (
        client.get(f"/v1/knowledge-bases/{kb_a}/documents/{doc_a}", headers=hdr_b).status_code
        == 404
    )
    assert client.get("/v1/knowledge-bases", headers=hdr_b).json() == []

    # Org B independently creates its own KB; org A never sees it.
    created_b = client.post(
        "/v1/knowledge-bases", json=_kb_body(), headers={**hdr_b, "Idempotency-Key": "b-kb"}
    )
    assert created_b.status_code == 201
    kb_b = created_b.json()["id"]
    assert [row["id"] for row in client.get("/v1/knowledge-bases", headers=hdr_a).json()] == [kb_a]
    assert [row["id"] for row in client.get("/v1/knowledge-bases", headers=hdr_b).json()] == [kb_b]

    # A spoofed org header on a scoped credential is rejected (no cross-tenant widening).
    spoof = client.get(
        "/v1/knowledge-bases",
        headers={**hdr_a, "X-Keel-Org": ids["org_b"], "X-Keel-Agent": ids["agent_b"]},
    )
    assert spoof.status_code == 403


async def test_oidc_user_knowledge_is_scoped_to_selected_agent(migrated_db: AsyncEngine) -> None:
    svc, ids = await _seed_two_orgs()
    # Link an OIDC subject to the owner user so a verified bearer resolves to that durable user.
    claims = OIDCClaims(
        issuer="https://idp.test",
        subject="user-oidc-1",
        audience=("keel",),
        email="owner@acme.test",
        email_verified=True,
        expires_at=4102444800,
        issued_at=None,
    )
    await svc.store.link_identity(
        user_id=ids["owner"], issuer=claims.issuer, subject=claims.subject, email="owner@acme.test"
    )
    client = _knowledge_app(migrated_db, svc, oidc_verifier=_FakeVerifier(claims))
    bearer = {
        "Authorization": "Bearer aaa.bbb.ccc",
        "X-Keel-Org": ids["org_a"],
        "X-Keel-Agent": ids["agent_a"],
    }
    created = client.post(
        "/v1/knowledge-bases", json=_kb_body(), headers={**bearer, "Idempotency-Key": "oidc-kb"}
    )
    assert created.status_code == 201
    kb_id = created.json()["id"]

    # The KB is stored under the derived agent scope, and selecting a different Agent (org B)
    # does not see org A's KB (per-Agent isolation for the same user).
    scope_a = derive_agent_scope(ids["org_a"], ids["agent_a"])
    store = PostgresKnowledgeStore(migrated_db, scope_a)
    assert await store.get_base(kb_id) is not None
    other = client.get(
        f"/v1/knowledge-bases/{kb_id}",
        headers={
            "Authorization": "Bearer aaa.bbb.ccc",
            "X-Keel-Org": ids["org_b"],
            "X-Keel-Agent": ids["agent_b"],
        },
    )
    assert other.status_code == 404
