# Keel — Diagrams

> These diagrams primarily visualize the **target architecture**. Read
> [`../ARCHITECTURE.md`](../ARCHITECTURE.md), especially its implementation-fidelity table,
> before treating a container, boundary, or flow as deployed.

Source diagrams are authored in **Mermaid** (renders natively on GitHub). They visualize
the architecture and accepted decisions, including event versions, embedding pinning,
durable approvals, scoped Agents, connectors, and pluggable execution.

| # | Diagram | View |
|---|---|---|
| 01 | System context (C4 L1) | [`01-context.md`](./01-context.md) |
| 02 | Containers (C4 L2) | [`02-containers.md`](./02-containers.md) |
| 03 | `keel-core` components (C4 L3) | [`03-core-components.md`](./03-core-components.md) |
| 04 | Agent runtime — turn lifecycle | [`04-agent-loop.md`](./04-agent-loop.md) |
| 05 | Key sequences (run · approval · schedule · IM · personal) | [`05-sequences.md`](./05-sequences.md) |
| 06 | Data model (ER) | [`06-data-model.md`](./06-data-model.md) |
| 07 | Deployment profiles | [`07-deployment.md`](./07-deployment.md) |
| 08 | Agent scope, connectors & isolation | [`08-scope-and-connectors.md`](./08-scope-and-connectors.md) |

> Notation: C4 levels are drawn with plain `flowchart` (not the experimental `C4*` syntax) for portable rendering. Cylinders `[( )]` are datastores; dashed edges denote progression/optional paths.
