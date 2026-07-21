# Historical documentation index

Detailed implementation plans, dated design snapshots, and HTML mockups were removed from the
working tree after the 2026-07-21 documentation reset. They are anchored by Git tag
`docs-archive-2026-07-21` at commit `d0dd310`.

Retrieve any file without changing the working tree:

```powershell
git show docs-archive-2026-07-21:docs/plans/<file>
git show docs-archive-2026-07-21:docs/designs/<file>
git show docs-archive-2026-07-21:docs/mockups/<file>
git show docs-archive-2026-07-21:docs/diagrams/<file>
```

In a shallow clone, fetch the repository history before using these commands.

The current decision and implementation sources are [ADRs](./adr/README.md),
[Architecture](./ARCHITECTURE.md), [Status](./STATUS.md), and subsystem references.

## Removed implementation plans

- `README.md` — original directory/index policy
- `2026-07-07-keel-autonomy-slice.md`
- `2026-07-08-keel-gmail-read-connector.md`
- `2026-07-08-keel-product-shell-approvals.md`
- `2026-07-08-keel-web-chat.md`
- `2026-07-08-keel-web-connectors.md`
- `2026-07-08-keel-web-sessions.md`
- `2026-07-11-memory-retrieval-foundation.md`
- `2026-07-12-memory-consolidation.md`
- `2026-07-12-semantic-session-search.md`
- `2026-07-13-memory-evals.md`
- `2026-07-14-durable-background-jobs.md`
- `2026-07-14-rag-knowledge-base.md`

These were task-level handoff documents containing thousands of lines of code snippets and
checklists. Implemented code, tests, ADRs, and Git history now supersede them.

## Removed design snapshots

- `README.md` — original directory/index policy
- `2026-07-07-keel-autonomy-slice-design.md`
- `2026-07-07-keel-ux-design.md`
- `2026-07-08-keel-product-shell-approvals-design.md`
- `2026-07-09-core-memory-notes-zh.md`
- `2026-07-09-memory-retrieval-foundation-design.md`
- `2026-07-11-semantic-session-search-design.md`
- `2026-07-12-memory-consolidation-design.md`
- `2026-07-12-memory-evals-design.md`
- `2026-07-14-durable-background-jobs-design.md`
- `2026-07-14-rag-knowledge-base-design.md`
- `2026-07-16-managed-code-projects-and-coding-agents-design.md`

Long-lived Memory and Knowledge contracts were consolidated into
[Memory](./MEMORY.md) and [Knowledge](./KNOWLEDGE.md). Coding decisions are covered by
[Projects](./PROJECTS.md), [Patches](./PATCHES.md), Architecture, and ADR-0011.

## Removed UX mockups

The removed `docs/mockups/` snapshot contained standalone HTML screens for admin, agents,
approvals, chat, connectors, delegation, extensions, memory, observability, onboarding, schedules,
sessions, and early product slices, plus shared CSS.

The shipped React application is the only current UI source.

## Replaced diagrams

Eight earlier diagrams for context, containers, core components, loop, sequences, data model,
deployment, and scope/connectors were replaced by the four maintained ADR-0011 diagrams. The old
sources remain available under `docs-archive-2026-07-21:docs/diagrams/`.

## Original review and implementation plan

The repository retains short compatibility summaries at:

- [Original design review](./DESIGN-REVIEW.md)
- [Original implementation plan](./IMPLEMENTATION-PLAN.md)

Their full pre-pruning text is available from `docs-archive-2026-07-21` with `git show`.
