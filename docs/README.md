# Keel documentation

This index defines which documents describe Keel **now**, which describe the target, and
which are retained as historical design evidence.

## Living canon

| Document | Authority |
|---|---|
| [Product requirements](./PRD.md) | Target users, product thesis, requirements, and success measures. Target features are explicitly separated from current implementation. |
| [Architecture](./ARCHITECTURE.md) | Target architecture and current implementation fidelity. |
| [Status](./STATUS.md) | Verified current capability, maturity, and blockers. Prefer this over dated plans for "what works now." |
| [Roadmap](./ROADMAP.md) | Active milestone order, dependencies, and exit gates. This is the only active roadmap. |
| [Invariants](./INVARIANTS.md) | Non-negotiable correctness and safety acceptance specifications. |
| [ADRs](./adr/) | Accepted architectural decisions. An ADR can describe a target that is not fully implemented; check Status and Architecture fidelity. |

## Practical guides

| Guide | Purpose |
|---|---|
| [Demo](./DEMO.md) | Safe 10–15 minute Windows demo, including stale-stack and rebuild paths. |
| [Usage](./USAGE.md) | Current CLI, server API, minimal web UI, React UI, and connector usage. |
| [Operations](./OPERATIONS.md) | Compose lifecycle, health checks, configuration, safety, backup caveats, and troubleshooting. |
| [Data lifecycle](./DATA-LIFECYCLE.md) | Retention defaults, the data map, and the operator erasure runbook + recovery/verification procedure. |
| [Development](./DEVELOPMENT.md) | Workspace setup, tests, lint/type checks, React development, and documentation validation. |

Repository-specific notes also live in
[`web/README.md`](../web/README.md), [`tests/README.md`](../tests/README.md),
[`adapters/README.md`](../adapters/README.md), and
[`deploy/config/README.md`](../deploy/config/README.md).

## Historical and supporting material

- [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) is the original M0–M4 plan. It is
  useful for intent and acceptance-test history, but it is not the active execution plan.
- [`DESIGN-REVIEW.md`](./DESIGN-REVIEW.md) is the pre-implementation design review plus
  later addenda. Its claims are not current implementation evidence.
- [`designs/`](./designs/) and [`plans/`](./plans/) contain dated feature snapshots and
  implementation checklists. They may describe superseded names, sequencing, or intended
  behavior.
- [`diagrams/`](./diagrams/) support the target architecture. Read them with the
  implementation-fidelity section in Architecture.
- [`mockups/`](./mockups/) are historical UX exploration rather than a statement of the
  shipped UI.

When documents conflict, use this order: **Status → Roadmap → Architecture fidelity →
PRD target → ADR/design history**.
