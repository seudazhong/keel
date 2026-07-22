# Memory and recall

> **Status:** Living subsystem reference
> **Product policy:** current direct mutation and consolidation behavior remains in place.
> Proposal-first learning is a deferred option pending comparative product evidence.

Keel separates durable conversation history, small Agent memory, archival passages, learned-memory
proposals, and temporary run state.

## Data classes

| Class | Purpose | Current storage |
|---|---|---|
| Session history | Durable conversation and tool/event history | `sessions`, append-only `events` |
| Core/profile memory | Small user-visible Agent context | `memory_blocks`, `memory_block_versions` |
| Archival memory | Searchable retained passages | `archival` + embeddings |
| Learned-memory proposals | Model-suggested core-memory changes with provenance | `memory_proposals` |
| Run-local scratch | Temporary execution state | run/job context; must not become memory implicitly |

Knowledge documents are separate; see [Knowledge](./KNOWLEDGE.md).

## Core memory

Core memory is a set of labeled, bounded blocks with optimistic versions and history. Current tools
can append, replace, or rethink a block.

The React Memory page exposes blocks and consolidation proposals. Keel currently supports direct
governed mutation as well as proposals; R1 does not change the default.

## Session recall

Session search combines:

- PostgreSQL lexical search;
- trigram/sub-string support for CJK and partial matches;
- pgvector semantic similarity when embeddings are available;
- reciprocal-rank fusion;
- bounded catch-up embedding of recent messages.

The event log remains the source of truth. Search indexes are derived and rebuildable.

## Archival memory

Archival passages are scope-bound text plus embedding metadata. Search refuses incompatible
embedding model/dimension combinations. Changing the embedding model requires an explicit re-embed
operation.

Current interactive Agents can insert archival passages directly. A proposal-only policy may be
evaluated later, but is not assumed to be superior without product evidence.

## Consolidation

Memory consolidation is durable, cursor-based background work:

1. claim a per-scope lease;
2. read a bounded batch after the cursor;
3. run the constrained consolidation Agent;
4. validate cited source event ids;
5. write proposals rather than editing core memory;
6. advance the cursor only after a clean completion.

Failure releases the lease without advancing the source window. Human approval applies a proposal
under an optimistic block-version check.

### Current trust and commit behavior

The current batch reader consumes completed user/assistant messages across every session in a scope,
excluding only digest and consolidation sessions. It does not yet filter by surface trust or Session
visibility. This is unsafe for a future scope containing both trusted Web and untrusted IM traffic.

Current consolidation behavior differs by memory class:

- core-memory changes are proposals;
- archival facts at confidence `>= 0.8` are written directly with source-event validation and
  semantic deduplication.

The normal product target admits only explicitly eligible trusted sources and routes both core and
archival learned-memory changes through reviewable proposals. Any future auto-archive policy must be
an explicit, audited Routine policy that cannot include untrusted IM input by default.

## Evaluation

The deterministic Memory eval suite covers recall, consolidation, safety, matching, and reporting.

```powershell
uv run python scripts/run_memory_evals.py --help
```

Replay mode must not call a live provider.

Only deterministic replay gates are enforced by default. Live mode and optional LLM judging are
advisory; judge failures do not change deterministic scores or fail the run.

## API and UI

Current `/v1` surfaces include:

- session list/search/history;
- memory block list;
- proposal list/approve/reject;
- consolidation trigger.

## Current limits

- Agent records do not yet own a versioned memory policy.
- Direct interactive mutation tools are enabled.
- Consolidation eligibility is scope-wide rather than surface/trust-aware.
- High-confidence archival consolidation auto-commits instead of producing a proposal.
- Session visibility is primarily Agent-scope based rather than owner/share-policy based.
- Consolidation is tied to current schedule primitives rather than a first-class Routine.
