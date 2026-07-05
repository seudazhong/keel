# ADR-0001 — Language & runtime

**Status:** Accepted · **Date:** 2026-07-06

## Context
Keel needs one backend language for the agent core, server, worker, scheduler, and CLI, plus a language for the web UI. The core must integrate LLM SDKs, MCP, embeddings/vector search, and a rich tool ecosystem, with strong async I/O.

## Decision
- **Backend / core / CLI: Python 3.12+ (asyncio).**
- **Web frontend: TypeScript + React.**
- `keel-core` is a pure library; server/worker/scheduler/CLI import it. All surfaces are clients of one protocol (no agent logic in a UI).

## Alternatives considered
- **TypeScript/Node full-stack** (à la OpenClaw/OpenCode): excellent runtime and one-language stack, but weaker parity for embeddings/RAG/agent/MCP tooling and data-science glue. Rejected for the backend; adopted for the web only.
- **Go**: great for the gateway/concurrency, weak AI ecosystem. Rejected.
- **Rust**: performance/safety, immature AI ecosystem, slower iteration. Rejected for v1.

## Consequences
- Fast iteration and best-in-class AI libraries; must be disciplined about `async` correctness and typing (Pydantic v2, mypy).
- Polyglot repo (Python + TS) — mitigated by a clean HTTP/SSE/WS contract and generated clients.
