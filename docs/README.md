# Keel documentation

The documentation is split by **authority**, not by age or file size. Read the smallest document
that answers the question and do not copy current-state facts into several places.

## Living canon

| Order | Document | Owns |
|---:|---|---|
| 1 | [Status](./STATUS.md) | What is implemented, deployable, product-usable, partial, or missing now. |
| 2 | [Roadmap](./ROADMAP.md) | What comes next, in what order, and the exit evidence. |
| 3 | [Architecture](./ARCHITECTURE.md) | Current system shape, target contracts, trust boundaries, and known deltas. |
| 4 | [Product requirements](./PRD.md) | Product boundary, users, journeys, requirements, success measures, and non-goals. |
| 5 | [Invariants](./INVARIANTS.md) | Merge-blocking correctness and safety properties. |
| 6 | [ADRs](./adr/README.md) | Accepted architectural decisions and explicit supersession history. |

When documents conflict about the present, **Status wins**. When they conflict about future
sequencing, **Roadmap wins**. ADRs explain why a decision was made; they do not prove that it is
implemented.

## Suggested reading paths

- **Five-minute state:** Status -> Roadmap.
- **Product/design:** PRD -> Architecture -> relevant ADR.
- **Run the preview:** Operations -> Usage or Demo.
- **Implement a subsystem:** its subsystem reference -> code/tests; use History only when original
  rationale is needed.

## Practical guides

| Guide | Purpose |
|---|---|
| [Demo](./DEMO.md) | Safe walkthrough of the trusted local preview. |
| [Usage](./USAGE.md) | Current UI, CLI, API, and workflow usage. |
| [Operations](./OPERATIONS.md) | Compose lifecycle, trust profile, readiness, secrets, storage, and production gaps. |
| [Development](./DEVELOPMENT.md) | Setup, checks, tests, frontend development, and documentation validation. |

Repository-specific guides:

- [`web/README.md`](../web/README.md)
- [`tests/README.md`](../tests/README.md)
- [`adapters/README.md`](../adapters/README.md)
- [`deploy/k8s/README.md`](../deploy/k8s/README.md)
- [`deploy/config/README.md`](../deploy/config/README.md)

## Subsystem references

| Reference | Scope |
|---|---|
| [Identity, organizations, and Agents](./IDENTITY.md) | Actors, OIDC verification, memberships, Agents, grants, and RLS. |
| [Managed projects](./PROJECTS.md) | Project ownership, Git storage, GitHub App integration, worktrees, and grants. |
| [Read-only code review](./CODE-REVIEW.md) | Durable review request, evidence verification, artifacts, and API. |
| [Controlled patch proposals](./PATCHES.md) | Implemented patch backend, approval/writeback lifecycle, and missing product surfaces. |
| [Connector providers](./connectors/README.md) | Shared connector contract and provider-specific setup/reference docs. |
| [Memory and recall](./MEMORY.md) | Session recall, core/archival memory, consolidation, proposals, and evals. |
| [Knowledge Base](./KNOWLEDGE.md) | Document lifecycle, chunking, retrieval, citations, taint, and evals. |
| [Data lifecycle](./DATA-LIFECYCLE.md) | Retention, erasure, data map, and anti-resurrection behavior. |
| [Event and API versioning](./EVENT-VERSIONING.md) | Event upcasting and additive `/v1` compatibility. |

## Historical and supporting material

- [Historical documentation](./HISTORY.md) indexes removed plans, designs, and mockups and explains
  how to retrieve them from Git.
- [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) and
  [`DESIGN-REVIEW.md`](./DESIGN-REVIEW.md) are short compatibility summaries.
- [`diagrams/`](./diagrams/) contains the maintained architecture diagrams.

## Maintenance rules

1. Current capability and snapshot evidence belong only in Status. Status may record the reviewed
   implementation SHA and migration head; avoid commit hashes and test counts in README,
   Architecture, PRD, and Roadmap.
2. Roadmap contains future outcomes and exit gates, not a second status ledger.
3. Architecture separates **current implementation** from **target contract** explicitly.
4. A changed decision gets a new ADR that names what it supersedes.
5. Superseded implementation plans/designs live in Git history, not the current documentation tree.
6. Every local Markdown path and anchor must pass:

```powershell
uv run python scripts/check_markdown_links.py
git diff --check
```
