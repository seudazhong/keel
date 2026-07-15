"""Replay/live Knowledge retrieval eval runner over production ingestion/search code."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.embeddings import Embedder, LiteLLMEmbedder
from keel_core.jobs import PostgresJobStore
from keel_core.knowledge import (
    KNOWLEDGE_INGEST_CANCEL_MODE,
    KNOWLEDGE_INGEST_KIND,
    KNOWLEDGE_INGEST_MAX_ATTEMPTS,
    KnowledgeBaseCreate,
    KnowledgeDocumentVersionCreate,
    KnowledgeDocumentVersionResult,
    KnowledgeHit,
    KnowledgeIngestPayload,
    KnowledgeJobHandlers,
    KnowledgeSearchTool,
    KnowledgeSourceType,
    KnowledgeStore,
    PostgresKnowledgeStore,
)
from keel_core.knowledge.models import KnowledgeChunkRecord
from keel_core.knowledge.search import KnowledgeSearcher
from keel_core.protocols import ToolContext
from keel_core.types import ContentTaint
from keel_worker.evals.database import (
    assert_current_database,
    case_scope,
    cleanup_scope,
    create_eval_engine,
)
from keel_worker.evals.embeddings import (
    EmbeddingCassette,
    EmbeddingCassetteMiss,
    FailingEmbedder,
    RecordingEmbedder,
    ReplayEmbedder,
)

from .models import (
    KnowledgeActualHit,
    KnowledgeCaseResult,
    KnowledgeDatasetError,
    KnowledgeEvalCase,
    KnowledgeEvalQuery,
    KnowledgeEvalReport,
    KnowledgeGateResult,
    KnowledgeQueryActual,
    canonical_dataset_hash,
    dataset_version,
    load_dataset,
    locator_key,
    safe_reason,
)

_CHUNKING_VERSION = "keel-char-v1"


@dataclass(slots=True)
class _EvalJobContext:
    job_id: str
    scope_id: str
    attempt: int = 1
    max_attempts: int = 1

    async def progress(
        self,
        current: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        del current, total, message

    async def checkpoint(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class _SeededChunk:
    record: KnowledgeChunkRecord
    version: int
    deleted: bool
    title: str
    source_uri: str | None


def _mime_type(source_type: KnowledgeSourceType) -> str:
    return "text/markdown" if source_type is KnowledgeSourceType.markdown else "text/plain"


def _query_metrics(
    expected_query: KnowledgeEvalQuery,
    actual: KnowledgeQueryActual,
) -> tuple[dict[str, float], list[str]]:
    expected = {locator_key(locator) for locator in expected_query.expected}
    hit_keys = [locator_key(hit) for hit in actual.hits if hit.content_sha256 is not None]
    if expected:
        recall = len(expected & set(hit_keys[:5])) / len(expected)
        reciprocal_rank = next(
            (1.0 / rank for rank, key in enumerate(hit_keys, start=1) if key in expected),
            0.0,
        )
    else:
        recall = 1.0 if not actual.hits else 0.0
        reciprocal_rank = recall
    citation_precision = (
        sum(1 for hit in actual.hits if hit.citation_valid) / len(actual.hits)
        if actual.hits
        else 1.0
    )
    leakage_pass = 1.0 if not any(hit.deleted for hit in actual.hits) else 0.0
    taint_pass = 1.0 if not expected_query.expect_taint or actual.tainted else 0.0
    degraded_pass = (
        1.0
        if expected_query.expected_mode is None or actual.mode is expected_query.expected_mode
        else 0.0
    )
    metrics = {
        "recall_at_5": recall,
        "mrr": reciprocal_rank,
        "citation_precision": citation_precision,
        "deleted_leakage_pass": leakage_pass,
        "taint_pass": taint_pass,
        "degraded_mode_pass": degraded_pass,
    }
    failures = [name for name, value in metrics.items() if value < 1.0]
    return metrics, failures


def score_case(
    case: KnowledgeEvalCase,
    actuals: list[KnowledgeQueryActual],
) -> KnowledgeCaseResult:
    if len(actuals) != len(case.queries):
        return KnowledgeCaseResult(
            case_id=case.id,
            status="error",
            score=0.0,
            failures=[],
            queries=actuals,
            reason="QueryCountMismatch",
        )
    metric_totals: dict[str, float] = {}
    failures: list[str] = []
    for index, (query, actual) in enumerate(zip(case.queries, actuals, strict=True)):
        metrics, query_failures = _query_metrics(query, actual)
        for name, value in metrics.items():
            metric_totals[name] = metric_totals.get(name, 0.0) + value
        failures.extend(f"query[{index}].{failure}" for failure in query_failures)
    metrics = {name: value / len(actuals) for name, value in metric_totals.items()}
    score = sum(metrics.values()) / len(metrics)
    return KnowledgeCaseResult(
        case_id=case.id,
        status="pass" if not failures else "fail",
        score=score,
        metrics=metrics,
        failures=failures,
        queries=actuals,
    )


def build_gates(cases: list[KnowledgeCaseResult]) -> tuple[list[KnowledgeGateResult], float]:
    successful = [case for case in cases if case.status != "error"]

    def average(name: str) -> float:
        return (
            sum(case.metrics.get(name, 0.0) for case in successful) / len(successful)
            if successful
            else 0.0
        )

    values = {
        "recall_at_5": average("recall_at_5"),
        "mrr": average("mrr"),
        "citation_precision": average("citation_precision"),
        "deleted_leakage_pass": average("deleted_leakage_pass"),
        "taint_pass": average("taint_pass"),
        "degraded_mode_pass": average("degraded_mode_pass"),
    }
    gates = [
        KnowledgeGateResult(
            name="recall_at_5",
            metric_value=values["recall_at_5"],
            threshold=0.80,
            comparator=">=",
            passed=values["recall_at_5"] >= 0.80,
        ),
        KnowledgeGateResult(
            name="mrr",
            metric_value=values["mrr"],
            threshold=0.70,
            comparator=">=",
            passed=values["mrr"] >= 0.70,
        ),
        *[
            KnowledgeGateResult(
                name=name,
                metric_value=values[name],
                threshold=1.0,
                comparator="==",
                passed=values[name] == 1.0,
            )
            for name in (
                "citation_precision",
                "deleted_leakage_pass",
                "taint_pass",
                "degraded_mode_pass",
            )
        ],
    ]
    weighted = (
        values["recall_at_5"] * 0.25
        + values["mrr"] * 0.20
        + values["citation_precision"] * 0.20
        + values["deleted_leakage_pass"] * 0.15
        + values["taint_pass"] * 0.10
        + values["degraded_mode_pass"] * 0.10
    )
    gates.append(
        KnowledgeGateResult(
            name="weighted_overall",
            metric_value=weighted,
            threshold=0.80,
            comparator=">=",
            passed=weighted >= 0.80,
        )
    )
    return gates, weighted


def _actual_hit(
    hit: KnowledgeHit,
    seeded: dict[str, _SeededChunk],
) -> KnowledgeActualHit:
    citation = hit.citation
    seeded_chunk = seeded.get(citation.chunk_id)
    if seeded_chunk is None:
        return KnowledgeActualHit(citation_valid=False, deleted=False)
    record = seeded_chunk.record
    citation_valid = (
        citation.kb_id == record.kb_id
        and citation.document_id == record.document_id
        and citation.document_version_id == record.document_version_id
        and citation.ordinal == record.ordinal
        and citation.char_start == record.char_start
        and citation.char_end == record.char_end
        and citation.title == seeded_chunk.title
        and citation.source_uri == seeded_chunk.source_uri
        and citation.label == f"{seeded_chunk.title}#chunk-{record.ordinal + 1}"
        and hit.heading_path == list(record.heading_path)
    )
    return KnowledgeActualHit(
        content_sha256=record.content_hash,
        version=seeded_chunk.version,
        ordinal=record.ordinal,
        char_start=record.char_start,
        char_end=record.char_end,
        citation_valid=citation_valid,
        deleted=seeded_chunk.deleted,
    )


async def _run_case(
    case: KnowledgeEvalCase,
    engine: AsyncEngine,
    embedder: Embedder,
    settings: Settings,
    version: str,
) -> KnowledgeCaseResult:
    scope = case_scope(version, case.id)
    store = PostgresKnowledgeStore(engine, scope)
    base = await store.create_base(
        KnowledgeBaseCreate(
            name=f"Eval {case.id}",
            description=None,
            embedding_model=embedder.model,
            embedding_dim=embedder.dim,
        )
    )
    handlers = KnowledgeJobHandlers(
        cast(KnowledgeStore, store),
        embedder,
        settings.model_copy(
            update={
                "knowledge_chunk_target_chars": case.chunk_target_chars,
                "knowledge_chunk_overlap_chars": case.chunk_overlap_chars,
            }
        ),
    )
    seeded: dict[str, _SeededChunk] = {}
    documents: list[tuple[KnowledgeDocumentVersionResult, bool]] = []
    jobs = PostgresJobStore(engine, scope)
    for index, document in enumerate(case.documents):
        created = await store.create_document_version(
            KnowledgeDocumentVersionCreate(
                kb_id=base.id,
                title=document.title,
                source_type=document.source_type,
                source_uri=document.source_uri,
                content=document.content,
                mime_type=_mime_type(document.source_type),
                chunking_version=_CHUNKING_VERSION,
                target_chars=case.chunk_target_chars,
                overlap_chars=case.chunk_overlap_chars,
            )
        )
        payload = KnowledgeIngestPayload(
            kb_id=base.id,
            document_id=created.document.id,
            document_version_id=created.version.id,
        )
        job, _ = await jobs.enqueue_once(
            kind=KNOWLEDGE_INGEST_KIND,
            payload=payload.model_dump(mode="json"),
            target_session_id=None,
            idempotency_key=f"eval:{case.id}:{index}",
            max_attempts=KNOWLEDGE_INGEST_MAX_ATTEMPTS,
            cancel_mode=KNOWLEDGE_INGEST_CANCEL_MODE,
        )
        context = _EvalJobContext(job_id=job.id, scope_id=scope)
        await handlers.ingest(
            context,
            payload.model_dump(mode="json"),
        )
        chunks = await store.list_version_chunks(
            base.id,
            created.document.id,
            created.version.id,
        )
        for chunk in chunks:
            seeded[chunk.id] = _SeededChunk(
                record=chunk,
                version=created.version.version,
                deleted=document.delete_before_queries,
                title=document.title,
                source_uri=document.source_uri,
            )
        documents.append((created, document.delete_before_queries))

    for created, should_delete in documents:
        if not should_delete:
            continue
        await store.tombstone_document(base.id, created.document.id)
        await store.purge_document(base.id, created.document.id)

    actuals: list[KnowledgeQueryActual] = []
    for query in case.queries:
        query_embedder: Embedder | None
        if query.embedding_mode == "normal":
            query_embedder = embedder
        elif query.embedding_mode == "failing":
            query_embedder = FailingEmbedder(model=embedder.model, dim=embedder.dim)
        else:
            query_embedder = None
        searcher = KnowledgeSearcher(engine, scope, query_embedder)
        hits, status = await searcher.search(base.id, query.query, k=query.k)
        tainted = True
        if query.expect_taint:
            result = await KnowledgeSearchTool(searcher).run(
                {"kb_id": base.id, "query": query.query, "k": query.k},
                ToolContext(scope_id=scope, session_id=f"eval:{case.id}"),
            )
            tainted = result.taint is ContentTaint.tainted
        actual_hits = [_actual_hit(hit, seeded) for hit in hits]
        actuals.append(
            KnowledgeQueryActual(
                query=query.query,
                mode=status.mode,
                hits=actual_hits,
                tainted=tainted,
            )
        )
    return score_case(case, actuals)


async def run_evals(
    *,
    dataset_path: Path,
    embedding_cassette_path: Path,
    mode: Literal["replay", "live"],
    enforce: bool,
    out_dir: Path,
    record: bool = False,
    model: str | None = None,
    dim: int | None = None,
) -> KnowledgeEvalReport:
    cases = load_dataset(dataset_path)
    version = dataset_version(cases)
    dataset_hash = canonical_dataset_hash(cases)
    settings = Settings()
    cassette = EmbeddingCassette(embedding_cassette_path)
    started = datetime.now(UTC).isoformat()
    engine = create_eval_engine()
    await assert_current_database(engine)
    results: list[KnowledgeCaseResult] = []
    infra_error = False
    try:
        for case in cases:
            scope = case_scope(version, case.id)
            await cleanup_scope(engine, scope)
            try:
                selected_model = model or case.embedding_model
                selected_dim = dim or case.embedding_dim
                if mode == "replay":
                    embedder: Embedder = ReplayEmbedder(
                        cassette,
                        model=selected_model,
                        dim=selected_dim,
                    )
                else:
                    live = LiteLLMEmbedder(
                        selected_model,
                        selected_dim,
                        send_dimensions=settings.embedding_send_dimensions,
                        timeout_seconds=settings.embedding_timeout_seconds,
                    )
                    if record:
                        recording = RecordingEmbedder(live)
                        recording.bind(cassette)
                        embedder = recording
                    else:
                        embedder = live
                results.append(await _run_case(case, engine, embedder, settings, version))
            except (EmbeddingCassetteMiss, KnowledgeDatasetError) as exc:
                infra_error = True
                results.append(
                    KnowledgeCaseResult(
                        case_id=case.id,
                        status="error",
                        score=0.0,
                        reason=safe_reason(exc),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - report bounded infra failure per case
                infra_error = True
                results.append(
                    KnowledgeCaseResult(
                        case_id=case.id,
                        status="error",
                        score=0.0,
                        reason=safe_reason(exc),
                    )
                )
            finally:
                await cleanup_scope(engine, scope)
    finally:
        await engine.dispose()

    if record and mode == "live" and not infra_error:
        cassette.save()
    gates, weighted = build_gates(results)
    gate_failed = any(not gate.passed for gate in gates)
    exit_code = 2 if infra_error else (1 if enforce and gate_failed else 0)
    report = KnowledgeEvalReport(
        run_id=f"knowledge-{uuid.uuid4().hex[:12]}",
        dataset_version=version,
        dataset_hash=dataset_hash,
        mode="record" if record else mode,
        cases=results,
        gates=gates,
        weighted_overall=weighted,
        exit_code=exit_code,
        started_at=started,
        finished_at=datetime.now(UTC).isoformat(),
    )
    await asyncio.to_thread(_write_report, report, out_dir)
    return report


def _write_report(report: KnowledgeEvalReport, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "knowledge-report.json").write_text(
        report.model_dump_json(indent=2),
        encoding="utf-8",
    )


def to_terminal(report: KnowledgeEvalReport) -> str:
    lines = [
        f"Knowledge evals {report.run_id} mode={report.mode} "
        f"dataset={report.dataset_version} hash={report.dataset_hash[:12]}",
        "",
        "Gates:",
    ]
    for gate in report.gates:
        mark = "PASS" if gate.passed else "FAIL"
        lines.append(
            f"  [{mark}] {gate.name} {gate.metric_value:.3f} {gate.comparator} {gate.threshold:.2f}"
        )
    lines.append("")
    for case in report.cases:
        lines.append(f"  {case.status.upper():5} {case.case_id} score={case.score:.3f}")
        for failure in case.failures:
            lines.append(f"        - {failure}")
        if case.reason:
            lines.append(f"        ! {case.reason}")
    lines.append("")
    lines.append(f"weighted_overall={report.weighted_overall:.3f} exit_code={report.exit_code}")
    return "\n".join(lines)


__all__ = ["build_gates", "run_evals", "score_case", "to_terminal"]
