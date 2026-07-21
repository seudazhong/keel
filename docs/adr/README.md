# Architecture decision records

ADRs record accepted decisions and their consequences. They are append-only decision history, not
implementation status.

| ADR | Decision |
|---|---|
| [0001](./0001-language-and-runtime.md) | Python backend, TypeScript/React frontend, thin clients. |
| [0002](./0002-datastore.md) | PostgreSQL/pgvector primary data store and Redis delivery substrate. |
| [0003](./0003-agent-runtime.md) | Custom bounded agent loop over LiteLLM provider plumbing. |
| [0004](./0004-frontend.md) | React, Vite, Tailwind, and component-oriented web UI. |
| [0005](./0005-sandbox.md) | Dedicated least-privilege execution boundary. |
| [0006](./0006-scheduler-and-queue.md) | arq queue and schedule-trigger semantics. |
| [0007](./0007-embeddings-and-rerank.md) | Pinned embedding model/dimension and optional reranking. |
| [0008](./0008-deployment-profiles-and-first-run.md) | Deployment-profile and first-run direction. |
| [0009](./0009-product-form-and-primary-use-cases.md) | Server-primary connected personal/team assistant. |
| [0010](./0010-durable-background-jobs.md) | Postgres-owned durable jobs with at-least-once delivery. |
| [0011](./0011-product-boundary-and-domain-model.md) | Product boundary, authority model, trust zones, and release strategy. |
| [0012](./0012-user-mailboxes-todos-notifications.md) | Per-user Keel mailboxes, user-owned ToDos, and durable notifications. |
| [0013](./0013-mailbox-portfolio-and-todo-experience.md) | Primary plus purpose mailboxes, Mail/ToDo UX, and Agent ToDo tools. |

To change an accepted decision, add a new ADR with `Supersedes` or `Refines`; do not rewrite the
old ADR into a different historical decision.
