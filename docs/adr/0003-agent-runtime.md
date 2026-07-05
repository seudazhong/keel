# ADR-0003 — Agent runtime & provider layer

**Status:** Accepted · **Date:** 2026-07-06

## Context
The core loop must support features beyond a vanilla agent framework: multi-surface gateways, shared budgets across a delegation tree, interrupt/steer, durable prompt admission, byte-stable prompt caching, cron autonomy, and a first-class permission/sandbox layer. Separately, we must talk to many LLM providers with streaming, tool-calling, cost, and failover.

## Decision
- **Custom async agent core** implementing the field manual's two-loop pattern (outer tool loop + inner retry/failover), stop-reason gate, budgets, guardrails, and named termination. We *own the policy*.
- **Provider plumbing via LiteLLM**, wrapped by our `ProviderGateway` that adds routing, failover classification, credential pools, rate-limit guards, prompt-cache keys, and event normalization. We *borrow the plumbing*.

## Alternatives considered
- **LangGraph** as the runtime: excellent durable graph execution/checkpointing, but bending it to gateway/budget/steering/cron semantics costs more than a purpose-built loop. May be embedded later for complex sub-flows.
- **Pydantic-AI / vanilla SDK loops**: too thin for our control-plane requirements.
- **Hand-rolled provider adapters** instead of LiteLLM: maximal control but re-implements a large amount of undifferentiated plumbing. Rejected for v1; `ProviderGateway` keeps the seam so we can swap later.

## Consequences
- Full control over correctness-critical behavior (budgets, caching, failover identity reconciliation).
- We depend on LiteLLM's provider coverage/quirks; isolated behind `ProviderGateway` so it is replaceable.
