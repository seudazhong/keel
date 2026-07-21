# Event and API versioning

> **Status:** Event upcasting and tombstone-aware projection rebuild are implemented. The current
> history contains one real event-version transition; an operator-facing rebuild workflow remains
> future work.

Events are append-only facts. Each envelope carries a positive, per-event-type
`version`; writers emit the version in `keel_core.evolution.CURRENT_EVENT_VERSIONS`.
Readers upcast copies through one explicit, deterministic `vN → vN+1` step for
every historical version before projections, replay, or runtime use. Stored rows
are never migrated or rewritten.

Unknown event types, missing historical transitions, malformed historical payloads,
and future versions fail closed. Upcasters must be pure and idempotent: applying
the read path to an already-current event is a no-op. New event types begin at
version 1; changing a payload requires a current-version increment, an upcaster,
and fixtures for each historical version.

The HTTP contract is `/v1` and additive-only. `tests/fixtures/openapi-v1-baseline.json`
is the committed compatibility baseline. Run
`uv run python scripts/check_openapi_compat.py --write-baseline` intentionally for
an accepted additive change and commit the snapshot; CI runs the check without
that flag. The checked-in `keel-sdk` exposes typed DTOs and client methods without
depending on an external generator.
