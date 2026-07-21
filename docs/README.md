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
| [Data lifecycle](./DATA-LIFECYCLE.md) | Retention, erasure, data map, and anti-resurrection behavior. |
| [Event and API versioning](./EVENT-VERSIONING.md) | Event upcasting and additive `/v1` compatibility. |

## Historical and supporting material

- [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) and
  [`DESIGN-REVIEW.md`](./DESIGN-REVIEW.md) preserve the original plan and review.
- [`designs/`](./designs/) and [`plans/`](./plans/) are dated snapshots, not an active backlog.
- [`diagrams/`](./diagrams/) visualize target and current concepts; Architecture owns fidelity.
- [`mockups/`](./mockups/) are historical UX exploration, not the shipped React UI.

## Maintenance rules

1. Current capability and snapshot evidence belong only in Status. Status may record the reviewed
   implementation SHA and migration head; avoid commit hashes and test counts in README,
   Architecture, PRD, and Roadmap.
2. Roadmap contains future outcomes and exit gates, not a second status ledger.
3. Architecture separates **current implementation** from **target contract** explicitly.
4. A changed decision gets a new ADR that names what it supersedes.
5. Dated designs and plans remain historical; do not continuously rewrite them to look current.
6. Every local Markdown path and anchor must pass:

```powershell
uv run python scripts/check_markdown_links.py
git diff --check
```
