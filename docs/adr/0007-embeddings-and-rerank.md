# ADR-0007 — Embeddings & reranking

**Status:** Accepted · **Date:** 2026-07-06 · **Resolves:** PRD §13 Q1 · **Related:** ADR-0002, DESIGN-REVIEW G8

## Context
Keel's memory (archival `passages`), session semantic search, and RAG/KB all need **embeddings**, and hybrid retrieval benefits from an optional **reranker**. Requirements: strong **CJK** + multilingual quality (NFR-9), a **zero-API-key** first-run path (PRD Q5), and one seam so hosted and local models are interchangeable. pgvector columns are fixed-dimension, so the embedding model cannot change silently without breaking similarity (DESIGN-REVIEW G8).

## Decision
- **Default embedding model: `BAAI/bge-m3`** (multilingual, strong Chinese, 1024-dim), served locally by a small **`keel-embed`** service (or via the bundled `ollama`/a text-embeddings image) so `dev`/`lite` need **no API key**.
- **Hosted embeddings** (OpenAI `text-embedding-3-*`, Cohere, Voyage, …) are first-class alternatives routed through the `ProviderGateway` **`embed` slot**; production may prefer a hosted model per cost/quality.
- **Reranking: `BAAI/bge-reranker-v2-m3`**, exposed via the gateway **`rerank` slot**, **off by default** (RRF fusion is sufficient for the MVP) and enabled in `full`/when configured.
- **Model pinning (correctness):** every `passage`/`kb_chunk` stores its `embedding_model` and `dim`; a **collection is pinned to one `(model, dim)`**; cross-model KNN is refused. Changing the model requires a **re-embed migration job**; mixed dimensions are never compared.
- Embeddings and rerank use the **same failover/routing/cost** machinery as chat models (one gateway, one accounting path).

## Alternatives considered
- **Hosted-only default (OpenAI):** best turnkey quality but breaks the zero-key promise and adds cost/data-egress for a self-hosted product. Rejected as the *default*; kept as a first-class option.
- **`e5`/`multilingual-e5-large`** as the local default: viable; `bge-m3` chosen for stronger CJK + long-context + dense/sparse support.
- **Always-on reranker:** better recall→precision but adds latency/compute and a required model; deferred to opt-in.

## Consequences
- Zero-key memory/RAG out of the box with good CJK quality; production can switch to hosted via config only.
- The `(model, dim)` pin is a hard constraint: switching embedding models is a **migration**, not a config flip — surfaced in admin and covered by a re-embed job.
- A local embed service adds one lightweight container in `full` (and an in-process option in `lite`).
