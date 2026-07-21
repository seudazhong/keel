# Knowledge Base

> **Status:** Living subsystem reference

Knowledge is versioned, cited, scope-bound external or user-provided content. It is distinct from
Agent memory and remains untrusted/tainted when retrieved.

## Model

| Entity | Purpose |
|---|---|
| Knowledge Base | Collection and embedding-policy boundary |
| Document | Stable source identity and lifecycle |
| Document version | Immutable normalized content plus indexing configuration |
| Chunk | Bounded retrievable text with source offsets, heading path, hash, and embedding |
| Idempotency record | Safe retry of lifecycle operations |

Schema lives in migration `0010_knowledge_base`.

## Ingest lifecycle

1. Create a Knowledge Base.
2. Add or update a document.
3. Normalize UTF-8 text and line endings before storage.
4. Persist the document version and durable ingest job.
5. Chunk and embed in the worker.
6. Atomically activate the indexed version.

Lost queue delivery is recovered through the durable job/outbox substrate. A failed version does
not replace the previous active version. The durable `desired_version_id` fence also prevents an
older or slower ingest job from activating after a newer requested version.

## Chunking

Defaults are approximately 1,600 characters with 200 characters of overlap and are stored on each
document version.

- Markdown keeps heading paths and structural units.
- Paragraphs, lists, and code fences are kept together where bounds allow.
- Oversized units are split deterministically.
- Chunk text is an unchanged substring of normalized content.
- Every chunk records character offsets and a content hash.

## Retrieval

Search combines:

- PostgreSQL lexical ranking;
- pgvector semantic ranking;
- reciprocal-rank fusion;
- optional degradation to lexical-only mode;
- bounded result and tool-output sizes.

Results carry document/version/chunk identity, source URL, heading path, offsets, score details, and
citation text.

Embedding collections are pinned to model and dimension. Cross-model vector search fails closed.

## Taint and provenance

Knowledge content is data, never instructions. Retrieval results remain tainted and preserve source
provenance. When such content influences an outbound effect, the configured approval policy still
applies.

Connector imports use the same lifecycle and retain connector, binding, external-resource,
revision, and source-URL provenance.

## Deletion

Delete first tombstones/hides the document, then durable worker cleanup removes Knowledge versions,
chunks, and embeddings. Connector-driven deletion removes its connector item mapping only after the
Knowledge delete handoff succeeds; generic Knowledge deletion does not own connector mappings.
Idempotent retries cannot resurrect a deleted source.

Scope/session/project erasure and projection rebuild use lifecycle tombstones where applicable.

## API and UI

The React Knowledge page and `/v1/knowledge-bases` API support:

- Knowledge Base CRUD;
- document create/update/version/reindex/delete;
- job-backed indexing;
- cited search.

## Evaluation

```powershell
uv run python scripts/run_knowledge_evals.py --help
```

The suite covers retrieval quality, citations, degradation, isolation, and deterministic reporting.

## Current limits

- Supported source formats are text/Markdown-oriented; provider importers intentionally reject many
  binary formats.
- Production-sized indexing/latency SLOs are not established.
- Connector product maturity differs even though they share the Knowledge sink.
- The lifecycle data map still needs explicit classification for stores added after the original
  lifecycle slice.
