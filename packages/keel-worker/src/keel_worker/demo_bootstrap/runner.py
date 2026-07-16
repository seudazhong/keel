"""Core logic for the demo data bootstrap.

Seeds one Knowledge Base with a few active Markdown/text documents (hybrid or
lexical-only retrieval, depending on embedding availability) and one welcome
session — the two shipped surfaces that most need visible content for a demo.

Every mutation goes through the same store/service/job contracts the server and
worker use in production (``KnowledgeService``, ``PostgresKnowledgeStore``,
``PostgresJobStore``, the real ``knowledge.ingest`` job handler run through the
worker's ``run_job`` lifecycle, and ``keel_core.loop.admit`` for session
history) — there is no ad-hoc SQL and nothing is ever deleted. Every write is
idempotent: identical idempotency keys mean a second run reuses the same
Knowledge Base, documents, and job rows instead of duplicating them.

Callers MUST run :func:`keel_core.demo_guard.assert_demo_environment` (done by
:func:`run_bootstrap`) before anything here touches a database.
"""

from __future__ import annotations

import asyncio
import dataclasses
from dataclasses import dataclass
from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from keel_core.config import Settings
from keel_core.demo_guard import assert_demo_environment
from keel_core.embeddings import Embedder, FakeEmbedder, LiteLLMEmbedder
from keel_core.jobs import JobLimits, PostgresJobStore
from keel_core.knowledge import (
    CreateKnowledgeBaseCommand,
    CreateKnowledgeDocumentCommand,
    KnowledgeService,
    KnowledgeSourceType,
    KnowledgeStore,
    PostgresKnowledgeStore,
)
from keel_core.knowledge.chunking import normalize_document_text
from keel_core.knowledge.search import KnowledgeSearcher
from keel_core.loop import admit
from keel_core.state import PostgresEventStore
from keel_worker.jobs import run_job
from keel_worker.knowledge import knowledge_job_registry

# Fixed, version-pinned identity for the deterministic offline embedder used when
# a real embedding provider is not configured/reachable. Kept independent from
# ``Settings.embedding_dim`` so the demo Knowledge Base's (model, dim) pin never
# silently drifts if the real provider config changes between runs.
FAKE_EMBEDDING_MODEL = "keel-demo/fake-lexical-v1"
FAKE_EMBEDDING_DIM = 32

_PROBE_TEXT = "keel demo bootstrap embedding probe"
_PROBE_TIMEOUT_SECONDS = 5.0

DEFAULT_SCOPE_ID = "web:local"
_IDEMPOTENCY_NAMESPACE = "demo-bootstrap:v1"

_KB_NAME = "Keel Demo Knowledge Base"
_KB_DESCRIPTION = (
    "Seeded by scripts/seed_demo_data.py to demonstrate Knowledge ingest, retrieval, "
    "and citations. Safe to delete through the normal API; safe to re-run the script."
)

_SESSION_SUFFIX = "demo-welcome"
_WELCOME_MESSAGE = (
    "This is a seeded demo session. Ask about the Knowledge Base, approvals, or "
    "schedules to see sessions/history alongside Knowledge search."
)

_OVERVIEW_MD = """# Keel Overview

Keel is a durable agent runtime for private personal agents and explicitly shared
team agents, reached through web, IM, and operator tools.

## Current surfaces

- Minimal chat with a tool timeline and approvals
- Sessions and history with hybrid search
- Schedules and durable background jobs, with cancellation/retry/recovery
- Gmail read, with outbound send gated behind an approval
- Core and archival memory with consolidation
- Knowledge Base ingest, hybrid retrieval, and citations

## Safety posture

Every mutating action is idempotent and scope-bound. Approvals gate any outbound
side effect, such as sending email, before it happens.
"""

_APPROVALS_MD = """# Approvals Runbook

Approvals gate any tool call flagged as an outbound side effect, such as sending
an email.

## Reviewing a pending approval

1. Open `/approvals`, or call `GET /v1/approvals?status=pending`.
2. Inspect the proposed action and its target before deciding.
3. Approve or deny. A denied approval fails closed: the run does not perform the
   side effect.

## Timeout behavior

An approval left unattended for the configured timeout expires and is treated as
denied — fail closed, never an implicit approval.
"""

_FAQ_TXT = """Q: How does Knowledge search combine keyword and semantic matches?
A: Each query runs a lexical pass (trigram similarity and full-text ranking) and,
when a matching embedder is configured, a semantic pass over chunk vectors. The
two ranked lists are combined with Reciprocal Rank Fusion.

Q: What happens if the embedding provider is unavailable?
A: Search degrades to lexical-only ranking instead of failing; the response
reports the degraded mode explicitly so a caller can tell it apart from a
healthy hybrid search.

Q: What does a citation contain?
A: A citation names the knowledge base, document, document version, and chunk
id, plus the character range the chunk came from and a human-readable label.

Q: Can old versions of a document be searched?
A: No. Retrieval only ranks the active version of each active document; deleted
or superseded content never leaks into results.
"""


class DemoBootstrapError(Exception):
    """Raised for a bootstrap-level failure that should abort with a clear message."""


@dataclass(frozen=True, slots=True)
class DemoDocumentSpec:
    slug: str
    title: str
    source_type: KnowledgeSourceType
    content: str
    sample_query: str


def _normalized(content: str, *, max_bytes: int) -> str:
    """Pre-normalize document text the same way ``knowledge.ingest`` requires.

    The ingest job treats stored content that is not already in canonical
    normalized form as a permanent ``content_invalid`` failure (defense in
    depth against callers who skip normalization). Document creation itself
    does not normalize on the caller's behalf, so this bootstrap must do it
    before calling ``KnowledgeService.create_document``.
    """

    return normalize_document_text(content, max_bytes=max_bytes)


def _document_specs(*, max_bytes: int) -> tuple[DemoDocumentSpec, ...]:
    return (
        DemoDocumentSpec(
            slug="overview",
            title="Keel Overview",
            source_type=KnowledgeSourceType.markdown,
            content=_normalized(_OVERVIEW_MD, max_bytes=max_bytes),
            sample_query="approvals",
        ),
        DemoDocumentSpec(
            slug="approvals-runbook",
            title="Approvals Runbook",
            source_type=KnowledgeSourceType.markdown,
            content=_normalized(_APPROVALS_MD, max_bytes=max_bytes),
            sample_query="expires",
        ),
        DemoDocumentSpec(
            slug="knowledge-search-faq",
            title="Knowledge Search FAQ",
            source_type=KnowledgeSourceType.text,
            content=_normalized(_FAQ_TXT, max_bytes=max_bytes),
            sample_query="citation",
        ),
    )


@dataclass(frozen=True, slots=True)
class EmbedderChoice:
    embedder: Embedder
    mode: str  # "live" | "fake"
    detail: str


async def _noop_enqueue(name: str, *args: object, **options: object) -> None:
    """No scheduled-retry queue is available from a one-shot script.

    Retryable job failures stay ``queued`` for their ``next_attempt_at``; simply
    re-running this script later will pick them back up (``run_job`` is a no-op
    on an already-terminal job and re-claims a still-queued one).
    """
    del name, args, options


async def resolve_embedder(settings: Settings, requested_mode: str) -> EmbedderChoice:
    """Pick the embedder used to seed and search the demo Knowledge Base.

    ``"fake"`` always uses a deterministic, offline, hashed bag-of-words embedder
    (no network call — safe for CI and for environments with no embedding
    provider). ``"live"`` requires the configured provider to answer a probe
    embed call or the run fails. ``"auto"`` tries the configured provider first
    and falls back to the deterministic embedder if it is unavailable, making
    the explicit limitation visible in :attr:`EmbedderChoice.detail`.
    """
    if requested_mode == "fake":
        return EmbedderChoice(
            embedder=FakeEmbedder(dim=FAKE_EMBEDDING_DIM, model=FAKE_EMBEDDING_MODEL),
            mode="fake",
            detail="forced by --mode fake (deterministic, offline; no semantic meaning)",
        )
    if requested_mode not in {"auto", "live"}:
        raise DemoBootstrapError(f"unknown embedding mode {requested_mode!r}")

    live = LiteLLMEmbedder(
        settings.embedding_model,
        settings.embedding_dim,
        send_dimensions=settings.embedding_send_dimensions,
        timeout_seconds=min(settings.embedding_timeout_seconds, _PROBE_TIMEOUT_SECONDS),
    )
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT_SECONDS):
            vectors = await live.embed([_PROBE_TEXT])
        if len(vectors) != 1 or len(vectors[0]) != settings.embedding_dim:
            raise ValueError("embedding probe returned an unexpected shape")
        return EmbedderChoice(
            embedder=live,
            mode="live",
            detail=f"configured embedder {settings.embedding_model!r} answered the probe",
        )
    except Exception as exc:  # noqa: BLE001 - any provider failure is a safe, reported fallback
        if requested_mode == "live":
            raise DemoBootstrapError(
                "--mode live requested but the configured embedder "
                f"{settings.embedding_model!r} failed: {type(exc).__name__}: {exc}"
            ) from exc
        return EmbedderChoice(
            embedder=FakeEmbedder(dim=FAKE_EMBEDDING_DIM, model=FAKE_EMBEDDING_MODEL),
            mode="fake",
            detail=(
                f"configured embedder {settings.embedding_model!r} unavailable "
                f"({type(exc).__name__}); using the deterministic offline fallback — "
                "semantic ranking is a hashed bag-of-words approximation only, but "
                "lexical search is fully functional"
            ),
        )


@dataclass(frozen=True, slots=True)
class DemoKnowledgeBaseResult:
    id: str
    name: str
    embedding_model: str
    embedding_dim: int
    replayed: bool


@dataclass(frozen=True, slots=True)
class DemoDocumentResult:
    slug: str
    id: str
    title: str
    status: str
    active_version_id: str | None
    version_status: str | None
    job_status: str
    replayed: bool


@dataclass(frozen=True, slots=True)
class DemoSessionResult:
    id: str
    created: bool
    message_count: int


@dataclass(frozen=True, slots=True)
class DemoSearchSample:
    query: str
    mode: str
    hit_count: int
    semantic_error: str | None


@dataclass(frozen=True, slots=True)
class DemoBootstrapResult:
    scope_id: str
    embedding_mode: str
    embedding_detail: str
    knowledge_base: DemoKnowledgeBaseResult
    documents: tuple[DemoDocumentResult, ...]
    session: DemoSessionResult
    search_samples: tuple[DemoSearchSample, ...]

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


async def _ensure_knowledge_base(
    service: KnowledgeService,
    embedder_choice: EmbedderChoice,
) -> DemoKnowledgeBaseResult:
    idempotency_key = f"{_IDEMPOTENCY_NAMESPACE}:kb:{embedder_choice.mode}"
    result = await service.create_base(
        CreateKnowledgeBaseCommand(name=_KB_NAME, description=_KB_DESCRIPTION),
        idempotency_key,
    )
    record = result.resource
    return DemoKnowledgeBaseResult(
        id=record.id,
        name=record.name,
        embedding_model=record.embedding_model,
        embedding_dim=record.embedding_dim,
        replayed=result.replayed,
    )


async def _ensure_document(
    service: KnowledgeService,
    jobs: PostgresJobStore,
    job_ctx: dict[str, Any],
    kb_id: str,
    spec: DemoDocumentSpec,
    embedder_mode: str,
) -> DemoDocumentResult:
    idempotency_key = f"{_IDEMPOTENCY_NAMESPACE}:doc:{spec.slug}:{embedder_mode}"
    result = await service.create_document(
        kb_id,
        CreateKnowledgeDocumentCommand(
            title=spec.title,
            source_type=spec.source_type,
            content=spec.content,
        ),
        idempotency_key,
    )
    # run_job is a safe no-op if the ingest job already reached a terminal state
    # (idempotent replay) and re-claims a still-queued job otherwise.
    job_status_value = await run_job(job_ctx, jobs.scope_id, result.job.id)

    document = await service.get_document(kb_id, result.document.id)
    version_status: str | None = None
    if document is not None:
        for version in await service.list_versions(kb_id, document.id):
            if version.id == result.version.id:
                version_status = version.status.value
                break
    return DemoDocumentResult(
        slug=spec.slug,
        id=result.document.id,
        title=spec.title,
        status=(document.status.value if document is not None else "unknown"),
        active_version_id=(document.active_version_id if document is not None else None),
        version_status=version_status,
        job_status=job_status_value,
        replayed=result.replayed,
    )


async def _ensure_demo_session(engine: AsyncEngine, scope_id: str) -> DemoSessionResult:
    session_id = f"{scope_id}:{_SESSION_SUFFIX}"
    store = PostgresEventStore(engine, scope_id)
    existing = [event async for event in store.read(session_id)]
    if existing:
        return DemoSessionResult(id=session_id, created=False, message_count=len(existing))
    await admit(store, session_id, scope_id, _WELCOME_MESSAGE)
    return DemoSessionResult(id=session_id, created=True, message_count=1)


async def _sample_search(
    service: KnowledgeService,
    kb_id: str,
    spec: DemoDocumentSpec,
) -> DemoSearchSample:
    hits, status = await service.search(kb_id, spec.sample_query, k=3)
    return DemoSearchSample(
        query=spec.sample_query,
        mode=status.mode.value,
        hit_count=len(hits),
        semantic_error=status.semantic_error,
    )


async def run_bootstrap(
    *,
    settings: Settings,
    scope_id: str = DEFAULT_SCOPE_ID,
    mode: str = "auto",
    engine: AsyncEngine | None = None,
) -> DemoBootstrapResult:
    """Seed the demo Knowledge Base + welcome session; safe to call repeatedly.

    Raises :class:`~keel_core.demo_guard.DemoGuardError` if ``settings`` is not an
    unmistakable dev/demo target, and :class:`DemoBootstrapError` for bootstrap-
    level failures (e.g. ``mode="live"`` with no reachable embedder). Never
    deletes or overwrites existing data outside its own idempotency-keyed rows.
    """
    assert_demo_environment(settings)
    embedder_choice = await resolve_embedder(settings, mode)

    owns_engine = engine is None
    active_engine = engine or create_async_engine(
        settings.database_url, pool_pre_ping=True, future=True
    )
    try:
        knowledge_store = PostgresKnowledgeStore(
            active_engine,
            scope_id,
            document_max_bytes=settings.knowledge_document_max_bytes,
        )
        jobs = PostgresJobStore(active_engine, scope_id, limits=JobLimits.from_settings(settings))
        searcher = KnowledgeSearcher(active_engine, scope_id, embedder_choice.embedder)
        service = KnowledgeService(
            cast(KnowledgeStore, knowledge_store),
            jobs,
            settings,
            searcher=searcher,
            embedding_model=embedder_choice.embedder.model,
            embedding_dim=embedder_choice.embedder.dim,
        )
        job_ctx: dict[str, Any] = {
            "jobs": jobs,
            "job_registry": knowledge_job_registry(
                cast(KnowledgeStore, knowledge_store),
                embedder_choice.embedder,
                settings,
            ),
            "durable_scope": scope_id,
            "enqueue": _noop_enqueue,
            "job_settings": settings,
        }

        specs = _document_specs(max_bytes=settings.knowledge_document_max_bytes)
        kb_result = await _ensure_knowledge_base(service, embedder_choice)
        document_results = tuple(
            [
                await _ensure_document(
                    service, jobs, job_ctx, kb_result.id, spec, embedder_choice.mode
                )
                for spec in specs
            ]
        )
        search_samples = tuple(
            [await _sample_search(service, kb_result.id, spec) for spec in specs]
        )
        session_result = await _ensure_demo_session(active_engine, scope_id)

        return DemoBootstrapResult(
            scope_id=scope_id,
            embedding_mode=embedder_choice.mode,
            embedding_detail=embedder_choice.detail,
            knowledge_base=kb_result,
            documents=document_results,
            session=session_result,
            search_samples=search_samples,
        )
    finally:
        if owns_engine:
            await active_engine.dispose()


def describe_plan(*, scope_id: str, mode: str, settings: Settings) -> str:
    """Human-readable preview of what a real run would do (used by --dry-run)."""
    lines = [
        "Demo bootstrap plan (no changes have been made):",
        f"  scope:            {scope_id}",
        f"  database:         {settings.database_url}",
        f"  embedding mode:   {mode}",
        f"  knowledge base:   {_KB_NAME!r} (create-if-missing, reused on repeat runs)",
        "  documents:",
    ]
    for spec in _document_specs(max_bytes=settings.knowledge_document_max_bytes):
        lines.append(
            f"    - {spec.title!r} ({spec.source_type.value}, sample query {spec.sample_query!r})"
        )
    lines.append(f"  session:          welcome session at scope '{scope_id}:{_SESSION_SUFFIX}'")
    lines.append(
        "Nothing is ever deleted or overwritten; re-running reuses the same "
        "idempotency-keyed resources."
    )
    return "\n".join(lines)


def render_result(result: DemoBootstrapResult) -> str:
    lines = [
        f"scope: {result.scope_id}",
        f"embedding mode: {result.embedding_mode} ({result.embedding_detail})",
        (
            f"knowledge base: {result.knowledge_base.id} "
            f"({'reused' if result.knowledge_base.replayed else 'created'})"
        ),
        "documents:",
    ]
    for doc in result.documents:
        lines.append(
            f"  - {doc.title}: status={doc.status} version={doc.version_status} "
            f"job={doc.job_status} ({'reused' if doc.replayed else 'created'})"
        )
    lines.append("search samples:")
    for sample in result.search_samples:
        lines.append(
            f"  - {sample.query!r}: mode={sample.mode} hits={sample.hit_count} "
            f"semantic_error={sample.semantic_error}"
        )
    lines.append(
        f"session: {result.session.id} "
        f"({'created' if result.session.created else 'already present'})"
    )
    return "\n".join(lines)


__all__ = [
    "DEFAULT_SCOPE_ID",
    "DemoBootstrapError",
    "DemoBootstrapResult",
    "DemoDocumentResult",
    "DemoDocumentSpec",
    "DemoKnowledgeBaseResult",
    "DemoSearchSample",
    "DemoSessionResult",
    "EmbedderChoice",
    "FAKE_EMBEDDING_DIM",
    "FAKE_EMBEDDING_MODEL",
    "describe_plan",
    "render_result",
    "resolve_embedder",
    "run_bootstrap",
]
