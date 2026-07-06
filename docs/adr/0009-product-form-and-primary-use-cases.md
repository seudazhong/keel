# ADR-0009 — Product form & primary use cases

**Status:** Accepted · **Date:** 2026-07-06 · **Supersedes framing in:** PRD §1–§4 · **Related:** ADR-0002, ADR-0005, ADR-0008, DESIGN-REVIEW G4

## Context
The original brief asked for a broadly **general-purpose** agent across every surface and feature. That is under-constrained on scope and leaves a load-bearing ambiguity unresolved: **is Keel a server-side agent that acts in its own sandbox, or a personal agent that acts on the user's machine?** The two pull the architecture in opposite directions (where tools execute, whose data is at risk, which surfaces are natural, ops footprint).

To resolve it we anchored on the **primary use cases** the product must nail first. The chosen anchors are:
- **(UC-A) Team / IM assistant** — a shared assistant in group chats and a team web app: Q&A, look-things-up, draft text, for many users. *Forces server-side* (a bot can't live on each member's machine) and involves **untrusted input**.
- **(UC-B) Personal connected assistant** — an assistant that works with **my** email, calendar, docs and knowledge, for me. Most of this data lives in **cloud accounts reached via OAuth**, so it is *also server-side*; only "my local files" truly needs local execution.

Both are **conversational assistants**, not a developer/code-execution tool. That reframes the product's center of gravity.

## Decision

### 1. Form: **server-primary**, with a pluggable execution seam
- Keel's default form is a **server/sandbox** agent. Personal-data access is via **cloud connectors (OAuth)**, so UC-B is served server-side without touching the user's machine.
- **Tool execution is a pluggable `ExecutionEnvironment`** (à la Hermes `BaseEnvironment`: `local`/`docker`/`ssh`/…). v1 ships the **sandbox-container** backend; a **`LocalDaemon`** backend (a light executor the user runs on their own machine) is **deferred** and unlocks the "local files / desktop" slice later without rework.
- Consequence: the earlier "server vs personal-machine" fork is **demoted from an architecture choice to a backend choice**.

### 2. Unifying abstraction: **an agent is a scoped, persisted entity**
A **group agent** and a **personal agent** are the *same* abstraction with different **scope**: persona + memory + toolset + **connectors** + permission boundary + provider + trust level.
- **Group agent:** scoped to a chat; shared memory; **safe toolset**; **no** personal connectors; untrusted input.
- **Personal agent:** scoped to a user; private memory; the user's connectors; trusted toolset.

### 3. **Connectors** become a first-class subsystem
Personal-data + messaging integrations (email, calendar, contacts, docs, knowledge, IM) are elevated from "generic MCP tools" to a first-class **Connectors** subsystem with **OAuth token management** and **per-agent scoping**. This is the biggest capability shift versus the pre-pivot design.

### 4. Re-prioritized center of gravity
- **Toolbox:** connectors + retrieval/RAG + messaging **first**; `bash`/code/file execution **second** (kept, not the star).
- **Memory is core value**, not trimmable — both use cases live on it.
- **Autonomy/proactivity** (digests, reminders) is central to UC-B.
- **Surfaces:** **Web + IM primary**; **CLI demoted** to admin/power-user; desktop/local-files deferred behind `LocalDaemon`.

### 5. Data isolation & a new headline security goal
- Cross-agent/scope **data isolation** and the **user/scope model** move from "later" to **core (M1–M2)**: a group agent must never see a personal agent's connectors or memory.
- New headline security objective: **cross-scope data exfiltration / the confused-deputy problem** — injection (from a group message, a web page, even an email) coercing an agent to leak private data — ranked *above* sandbox escape for these use cases.

### 6. Co-hosting guidance
Design for **hard per-scope isolation** so a public group bot and a private personal agent *can* share one instance; **recommend separate instances** for the most sensitive personal use.

## Alternatives considered
- **Dev/coding-first (personal local executor):** rejected as the *primary* — the user did not choose it; it stays a later `LocalDaemon` scenario.
- **Personal-machine form as primary:** rejected — ~80% of UC-B (email/calendar/docs) is cloud data reachable server-side via OAuth; only local files need it, and that is deferred behind the seam.
- **Co-host without hard isolation:** rejected — mixing public/untrusted and private/sensitive scopes without isolation is unsafe.
- **Keep "generic MCP only" (no first-class Connectors):** rejected — OAuth token management, per-scope data boundaries, and curated connector UX are not served by generic tool import.

## Consequences
- The pre-pivot, sandbox/shell-centric emphasis is rebalanced toward **connectors + memory + retrieval + messaging**; ARCHITECTURE and PRD are updated accordingly.
- A new `keel-core/connectors` subsystem, OAuth flows, and a per-scope data-isolation model are added to the M1–M2 plan; **local files / desktop** move out of MVP behind `LocalDaemon`.
- Privacy/retention (DESIGN-REVIEW G4, NFR-13) and the user/scope model are pulled earlier.
- CLI remains (admin/power-user, `lite`) but is no longer the headline surface.
- WeChat remains ToS-bounded/best-effort; **WeCom / Telegram** are the realistic v1 IM channels (ADR framing unchanged, now more central).
