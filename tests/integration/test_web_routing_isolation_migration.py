"""Live-Postgres verification of migration 0017 up/down (web-routing rollback compatibility).

0017 makes session identity composite ``(scope_id, id)`` so two scopes may own the *same*
external session id. Its ``downgrade`` restores the pre-0017 *global* session namespace, which
cannot represent such a collision — so before restoring the global constraints it must
deterministically remap every non-canonical scoped session to a stable, collision-free id and
repoint all referencing rows in lock-step, with no data loss.

This exercises the real migration against a live database: seed identical session ids in two
scopes (plus attached events and runs and a non-colliding control session), downgrade one
revision, and assert the collision is collapsed losslessly and every event/run stays attached
to the correct session. Then re-upgrade to head to prove reversibility.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.runs import (
    RunBudgetSpec,
    _fingerprint_matches,
    admission_fingerprint,
    legacy_admission_fingerprint,
)

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_HEAD = "0017_web_routing_isolation"
_PREV = "0016_connector_foundation"

_SCOPE_A = "agent:acme/support"
_SCOPE_B = "agent:globex/support"
_DUP = "external-session-42"
_SOLO = "external-session-solo"


def _alembic_cfg(url: str):  # type: ignore[no-untyped-def]
    from alembic.config import Config

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _run_to(url: str, revision: str) -> None:
    from alembic import command

    cfg = _alembic_cfg(url)
    if revision == "head":
        command.upgrade(cfg, "head")
    else:
        command.downgrade(cfg, revision)


def _expected_remap(scope_id: str, old_id: str) -> str:
    """Mirror the deterministic derivation in 0017.downgrade for assertion."""
    base = old_id[:180]
    digest = hashlib.md5(f"{scope_id}|{old_id}".encode()).hexdigest()  # noqa: S324
    return f"{base}.scope-{digest}"


async def _session_pk_columns(conn: AsyncConnection) -> list[str]:
    """The column names of the ``sessions`` primary key (order-independent)."""
    result = await conn.execute(
        text(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_attribute a "
            "ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            "WHERE i.indrelid = 'sessions'::regclass AND i.indisprimary"
        )
    )
    return list(result.scalars().all())


async def _seed(engine: AsyncEngine) -> None:
    expires = datetime.now(UTC) + timedelta(hours=1)
    ts = datetime.now(UTC)
    async with engine.begin() as conn:
        # Two scopes owning the *same* external session id — only representable post-0016.
        for scope in (_SCOPE_A, _SCOPE_B):
            await conn.execute(
                text("INSERT INTO sessions (id, scope_id) VALUES (:id, :scope)"),
                {"id": _DUP, "scope": scope},
            )
            # An event whose payload names its scope, so we can prove it stays attached.
            await conn.execute(
                text(
                    "INSERT INTO events (session_id, scope_id, seq, type, ts) "
                    "VALUES (:sid, :scope, 1, :type, :ts)"
                ),
                {"sid": _DUP, "scope": scope, "type": f"marker::{scope}", "ts": ts},
            )
            await conn.execute(
                text(
                    "INSERT INTO runs (id, scope_id, org_id, actor, agent_id, session_id, "
                    "surface, idempotency_key, expires_at) VALUES (:id, :scope, :org, :actor, "
                    ":agent, :sid, 'web', :key, :expires)"
                ),
                {
                    "id": f"run-{scope}",
                    "scope": scope,
                    "org": scope.split(":")[1].split("/")[0],
                    "actor": "user-1",
                    "agent": "agent-1",
                    "sid": _DUP,
                    "key": "k1",
                    "expires": expires,
                },
            )
        # A non-colliding control session in scope A — must be left untouched by the remap.
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id) VALUES (:id, :scope)"),
            {"id": _SOLO, "scope": _SCOPE_A},
        )
        await conn.execute(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts) "
                "VALUES (:sid, :scope, 1, :type, :ts)"
            ),
            {"sid": _SOLO, "scope": _SCOPE_A, "type": "marker::solo", "ts": ts},
        )


async def test_downgrade_remaps_cross_scope_session_collisions(migrated_db: AsyncEngine) -> None:
    url = os.environ["KEEL_TEST_DATABASE_URL"]
    await _seed(migrated_db)

    await asyncio.to_thread(_run_to, url, _PREV)

    # Canonical holder = lexicographically smallest scope for the id (scope A keeps the id);
    # scope B is deterministically remapped.
    remapped = _expected_remap(_SCOPE_B, _DUP)
    async with migrated_db.connect() as conn:
        # 1) The global sessions PK (id) is restored and every id is now globally unique.
        pk_cols = await _session_pk_columns(conn)
        assert list(pk_cols) == ["id"]

        rows = (await conn.execute(text("SELECT scope_id, id FROM sessions ORDER BY id"))).all()
        ids = [r.id for r in rows]
        assert len(ids) == len(set(ids)), f"session ids not globally unique after downgrade: {ids}"

        by_scope = {(r.scope_id, r.id) for r in rows}
        assert (_SCOPE_A, _DUP) in by_scope  # canonical scope kept the original id
        assert (_SCOPE_B, remapped) in by_scope  # colliding scope was renamed deterministically
        assert (_SCOPE_A, _SOLO) in by_scope  # non-colliding control untouched
        assert (_SCOPE_B, _DUP) not in by_scope  # the collision is gone

        # 2) No event was detached: each scope's marker event points at that scope's session id.
        events = (await conn.execute(text("SELECT scope_id, session_id, type FROM events"))).all()
        attached = {(e.scope_id, e.session_id, e.type) for e in events}
        assert (_SCOPE_A, _DUP, f"marker::{_SCOPE_A}") in attached
        assert (_SCOPE_B, remapped, f"marker::{_SCOPE_B}") in attached
        assert (_SCOPE_A, _SOLO, "marker::solo") in attached
        # Every event still resolves to an existing session row in its own scope.
        for e in events:
            assert (e.scope_id, e.session_id) in by_scope

        # 3) The FORCE-RLS runs table was repointed across *both* scopes (proves the FORCE toggle).
        runs = (await conn.execute(text("SELECT scope_id, session_id FROM runs"))).all()
        run_map = {r.scope_id: r.session_id for r in runs}
        assert run_map[_SCOPE_A] == _DUP
        assert run_map[_SCOPE_B] == remapped

    # 4) Reversible: re-upgrade to head restores composite identity with the data intact.
    await asyncio.to_thread(_run_to, url, "head")
    async with migrated_db.connect() as conn:
        pk_cols = await _session_pk_columns(conn)
        assert set(pk_cols) == {"scope_id", "id"}
        count = await conn.scalar(text("SELECT count(*) FROM sessions"))
        assert count == 3


async def test_downgrade_is_idempotent_and_noop_without_collisions(
    migrated_db: AsyncEngine,
) -> None:
    """No cross-scope collision -> the remap is a pure no-op and downgrade succeeds cleanly."""
    url = os.environ["KEEL_TEST_DATABASE_URL"]
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id) VALUES (:id, :scope)"),
            {"id": _SOLO, "scope": _SCOPE_A},
        )

    await asyncio.to_thread(_run_to, url, _PREV)
    async with migrated_db.connect() as conn:
        rows = (await conn.execute(text("SELECT scope_id, id FROM sessions"))).all()
        assert [(r.scope_id, r.id) for r in rows] == [(_SCOPE_A, _SOLO)]

    # Restore head for subsequent tests.
    await asyncio.to_thread(_run_to, url, "head")


_MODEL = "model-A"
_CONTENT = "please triage the overnight inbox"


def _binding(scope_id: str) -> dict[str, str]:
    """The persisted-identity binding (org/actor/agent/surface) for a scope's run."""
    return {
        "org_id": scope_id.split(":")[1].split("/")[0],
        "actor": "user-1",
        "agent_id": "agent-1",
        "surface": "web",
    }


async def _seed_admitted_run(
    engine: AsyncEngine,
    *,
    scope_id: str,
    session_id: str,
    content: str | None = _CONTENT,
    fingerprint: str,
) -> str:
    """Seed a session + an admitted run with a real fingerprint and (optionally) its admission
    user event carrying the immutable content, mirroring the durable admission path."""
    run_id = f"run-{scope_id}"
    expires = datetime.now(UTC) + timedelta(hours=1)
    ts = datetime.now(UTC)
    binding = _binding(scope_id)
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id) VALUES (:id, :scope) ON CONFLICT DO NOTHING"),
            {"id": session_id, "scope": scope_id},
        )
        await conn.execute(
            text(
                "INSERT INTO runs (id, scope_id, org_id, actor, agent_id, session_id, surface, "
                "idempotency_key, expires_at, fingerprint) VALUES (:id, :scope, :org_id, :actor, "
                ":agent_id, :sid, :surface, :key, :expires, :fp)"
            ),
            {
                "id": run_id,
                "scope": scope_id,
                "sid": session_id,
                "key": "k1",
                "expires": expires,
                "fp": fingerprint,
                **binding,
            },
        )
        if content is not None:
            payload = {
                "role": "user",
                "text": content,
                "admission_run": run_id,
                "dedup_key": f"admit:{run_id}",
                "admission_model": _MODEL,
            }
            await conn.execute(
                text(
                    "INSERT INTO events (session_id, scope_id, seq, type, ts, run_id, payload) "
                    "VALUES (:sid, :scope, 1, 'message.token', :ts, :run, CAST(:payload AS jsonb))"
                ),
                {
                    "sid": session_id,
                    "scope": scope_id,
                    "ts": ts,
                    "run": run_id,
                    "payload": json.dumps(payload),
                },
            )
    return run_id


async def test_downgrade_remaps_run_fingerprints_for_new_session_id(
    migrated_db: AsyncEngine,
) -> None:
    """A remapped run's immutable fingerprint is rebound to its NEW session id as the exact
    pre-0016 (legacy, pre-model) form — so a retry after rollback is idempotent, while changed
    content/actor/model still conflict under the relevant old/current admission logic."""
    url = os.environ["KEEL_TEST_DATABASE_URL"]
    binding_a = _binding(_SCOPE_A)
    binding_b = _binding(_SCOPE_B)
    # Both scopes admitted a run for the SAME external session id via a model-aware binary, so
    # each stored a model-aware fingerprint bound to the (now shared) id.
    fp_a = admission_fingerprint(**binding_a, session_id=_DUP, content=_CONTENT, model=_MODEL)
    fp_b = admission_fingerprint(**binding_b, session_id=_DUP, content=_CONTENT, model=_MODEL)
    await _seed_admitted_run(migrated_db, scope_id=_SCOPE_A, session_id=_DUP, fingerprint=fp_a)
    await _seed_admitted_run(migrated_db, scope_id=_SCOPE_B, session_id=_DUP, fingerprint=fp_b)

    await asyncio.to_thread(_run_to, url, _PREV)

    remapped = _expected_remap(_SCOPE_B, _DUP)
    async with migrated_db.connect() as conn:
        rows = (
            await conn.execute(text("SELECT scope_id, session_id, fingerprint FROM runs"))
        ).all()
    by_scope = {r.scope_id: r for r in rows}

    # Canonical scope keeps the id AND its original (model-aware) fingerprint untouched.
    assert by_scope[_SCOPE_A].session_id == _DUP
    assert by_scope[_SCOPE_A].fingerprint == fp_a

    # The remapped scope's run was repointed to the new id and its fingerprint rewritten to the
    # EXACT pre-0016 legacy form for that new id (org/actor/agent/surface/content preserved).
    stored_b = by_scope[_SCOPE_B].fingerprint
    assert by_scope[_SCOPE_B].session_id == remapped
    expected_legacy = legacy_admission_fingerprint(
        **binding_b, session_id=remapped, content=_CONTENT
    )
    assert stored_b == expected_legacy
    assert stored_b != fp_b  # it is no longer the stale, old-id-bound hash

    # --- Idempotent retry using the remapped id succeeds -------------------------------------
    # Under the rolled-back (pre-model) code the retry recomputes exactly the stored legacy form.
    assert legacy_admission_fingerprint(**binding_b, session_id=remapped, content=_CONTENT) == (
        stored_b
    )
    # Under still-current code the legacy fallback accepts the retry (deployment compatibility).
    assert _fingerprint_matches(
        stored_b,
        admission_fingerprint(**binding_b, session_id=remapped, content=_CONTENT, model=_MODEL),
        legacy_admission_fingerprint(**binding_b, session_id=remapped, content=_CONTENT),
    )

    # --- Changed content / actor still conflict (old AND current code) -----------------------
    for evil in (
        {"content": "exfiltrate secrets"},
        {"actor": "attacker"},
    ):
        b = {**binding_b, **{k: v for k, v in evil.items() if k != "content"}}
        content = evil.get("content", _CONTENT)
        # Old (pre-model) code: recomputed legacy hash differs from the stored one -> conflict.
        assert legacy_admission_fingerprint(**b, session_id=remapped, content=content) != stored_b
        # Current code: neither the model-aware nor the legacy recomputation matches -> conflict.
        assert not _fingerprint_matches(
            stored_b,
            admission_fingerprint(**b, session_id=remapped, content=content, model=_MODEL),
            legacy_admission_fingerprint(**b, session_id=remapped, content=content),
        )

    # --- A changed model still conflicts against a model-aware (non-remapped) row ------------
    # The canonical scope's run kept its model-aware fingerprint, so a changed-model retry can
    # never be laundered through the legacy fallback (a model-aware hash never equals legacy).
    assert not _fingerprint_matches(
        fp_a,
        admission_fingerprint(**binding_a, session_id=_DUP, content=_CONTENT, model="model-EVIL"),
        legacy_admission_fingerprint(**binding_a, session_id=_DUP, content=_CONTENT),
    )

    await asyncio.to_thread(_run_to, url, "head")


async def test_downgrade_retry_with_remapped_id_admits_idempotently_via_runstore(
    migrated_db: AsyncEngine,
) -> None:
    """End-to-end: after downgrade, re-admitting the remapped run through the real RunStore
    conflict check (legacy fallback) is an idempotent no-op, not a false conflict."""
    from keel_core.runs import PostgresRunStore

    url = os.environ["KEEL_TEST_DATABASE_URL"]
    binding_b = _binding(_SCOPE_B)
    fp_a = admission_fingerprint(
        **_binding(_SCOPE_A), session_id=_DUP, content=_CONTENT, model=_MODEL
    )
    fp_b = admission_fingerprint(**binding_b, session_id=_DUP, content=_CONTENT, model=_MODEL)
    await _seed_admitted_run(migrated_db, scope_id=_SCOPE_A, session_id=_DUP, fingerprint=fp_a)
    run_b = await _seed_admitted_run(
        migrated_db, scope_id=_SCOPE_B, session_id=_DUP, fingerprint=fp_b
    )

    await asyncio.to_thread(_run_to, url, _PREV)
    remapped = _expected_remap(_SCOPE_B, _DUP)

    store = PostgresRunStore(migrated_db, _SCOPE_B)
    # A retry that presents the remapped session id + the reconstructed legacy fallback is
    # recognized as the same admission (idempotent), returning the existing run id.
    record, created = await store.create(
        run_id="run-retry",
        scope_id=_SCOPE_B,
        idempotency_key="k1",
        budget=RunBudgetSpec(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        fingerprint=admission_fingerprint(
            **binding_b, session_id=remapped, content=_CONTENT, model=_MODEL
        ),
        legacy_fingerprint=legacy_admission_fingerprint(
            **binding_b, session_id=remapped, content=_CONTENT
        ),
        session_id=remapped,
        **binding_b,
    )
    assert record.id == run_b
    assert created is False

    await asyncio.to_thread(_run_to, url, "head")


async def test_downgrade_fails_closed_when_run_content_is_unrecoverable(
    migrated_db: AsyncEngine,
) -> None:
    """A remapped run with a real fingerprint but no admission event cannot be reconstructed —
    the downgrade fails (before constraints change) rather than clearing/wildcarding the hash."""
    url = os.environ["KEEL_TEST_DATABASE_URL"]
    fp_a = admission_fingerprint(
        **_binding(_SCOPE_A), session_id=_DUP, content=_CONTENT, model=_MODEL
    )
    fp_b = admission_fingerprint(
        **_binding(_SCOPE_B), session_id=_DUP, content=_CONTENT, model=_MODEL
    )
    await _seed_admitted_run(migrated_db, scope_id=_SCOPE_A, session_id=_DUP, fingerprint=fp_a)
    # Scope B collides but has NO admission event (content=None) -> unrecoverable content.
    await _seed_admitted_run(
        migrated_db, scope_id=_SCOPE_B, session_id=_DUP, content=None, fingerprint=fp_b
    )

    with pytest.raises(Exception, match="admission event"):
        await asyncio.to_thread(_run_to, url, _PREV)

    # The failed downgrade rolled back inside its transaction: composite identity is intact and
    # the original data is untouched (nothing was cleared).
    async with migrated_db.connect() as conn:
        pk_cols = await _session_pk_columns(conn)
        assert set(pk_cols) == {"scope_id", "id"}
        fps = (await conn.execute(text("SELECT fingerprint FROM runs ORDER BY id"))).scalars().all()
        assert set(fps) == {fp_a, fp_b}  # nothing cleared/wildcarded


async def test_up_down_up_preserves_webhook_route_cascade_and_active_scope_index(
    migrated_db: AsyncEngine,
) -> None:
    """Follow-up review finding (connector global routing metadata erasure), exercised through a
    genuine down/up round trip rather than only the application-level ``purge_scope`` helper:

    * ``connector_webhook_routes`` carries a composite FK to ``connector_bindings (scope_id,
      id)`` with ``ON DELETE CASCADE`` — deleting the durable scoped binding row cascades its
      route away automatically, in the SAME statement/transaction, with no application code
      involved. A stale route can therefore never outlive (or be resolvable after) its binding.
    * ``connector_active_scopes`` has no such per-binding FK (it is a per-scope aggregate, not
      tied to one binding row) and is deliberately left alone by the schema — its cleanup is an
      explicit application-level responsibility (``connector_repository.purge_scope``), which
      this test documents by proving the index constraint does *not* auto-clear it.
    * The migration remains reversible: dropping to 0016 and back to head recreates both tables
      (and the FK) from scratch with no residual data leaking across the round trip.
    """
    url = os.environ["KEEL_TEST_DATABASE_URL"]

    async def _seed_binding_and_route(conn: AsyncConnection, scope_id: str, binding_id: str) -> str:
        await conn.execute(
            text(
                "INSERT INTO connector_bindings (id, scope_id, connector_id, status) "
                "VALUES (:id, :scope, 'pg_fixture', 'connected')"
            ),
            {"id": binding_id, "scope": scope_id},
        )
        token = f"tok-{binding_id}"
        await conn.execute(
            text(
                "INSERT INTO connector_webhook_routes "
                "(route_token, scope_id, connector_id, binding_id, status) "
                "VALUES (:token, :scope, 'pg_fixture', :binding, 'connected')"
            ),
            {"token": token, "scope": scope_id, "binding": binding_id},
        )
        await conn.execute(
            text("INSERT INTO connector_active_scopes (scope_id) VALUES (:scope)"),
            {"scope": scope_id},
        )
        return token

    async with migrated_db.begin() as conn:
        token = await _seed_binding_and_route(conn, _SCOPE_A, "binding-cascade-1")

    # A composite FK to connector_bindings (scope_id, id) means deleting the binding cascades
    # the route away with no application code — the very defect this follow-up fixes.
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("DELETE FROM connector_bindings WHERE scope_id = :scope AND id = :id"),
            {"scope": _SCOPE_A, "id": "binding-cascade-1"},
        )
    async with migrated_db.connect() as conn:
        route_left = await conn.scalar(
            text("SELECT count(*) FROM connector_webhook_routes WHERE route_token = :token"),
            {"token": token},
        )
        assert route_left == 0
        # No per-binding FK on connector_active_scopes: this is an explicit purge_scope job,
        # not a schema-level cascade — confirmed by it surviving the binding delete above.
        scope_left = await conn.scalar(
            text("SELECT count(*) FROM connector_active_scopes WHERE scope_id = :scope"),
            {"scope": _SCOPE_A},
        )
        assert scope_left == 1

    # Round trip: downgrade drops both global tables, re-upgrade recreates them (and the FK)
    # from scratch with no residual rows leaking across the migration boundary.
    await asyncio.to_thread(_run_to, url, _PREV)
    async with migrated_db.connect() as conn:
        exists = await conn.scalar(
            text(
                "SELECT to_regclass('connector_webhook_routes') IS NOT NULL "
                "AND to_regclass('connector_active_scopes') IS NOT NULL"
            )
        )
        assert exists is False

    await asyncio.to_thread(_run_to, url, "head")
    async with migrated_db.connect() as conn:
        counts = (
            await conn.scalar(text("SELECT count(*) FROM connector_webhook_routes")),
            await conn.scalar(text("SELECT count(*) FROM connector_active_scopes")),
        )
        assert counts == (0, 0)

    # The FK is back in force post round-trip: seed fresh rows and prove cascade again.
    async with migrated_db.begin() as conn:
        token2 = await _seed_binding_and_route(conn, _SCOPE_B, "binding-cascade-2")
        await conn.execute(
            text("DELETE FROM connector_bindings WHERE scope_id = :scope AND id = :id"),
            {"scope": _SCOPE_B, "id": "binding-cascade-2"},
        )
    async with migrated_db.connect() as conn:
        route_left2 = await conn.scalar(
            text("SELECT count(*) FROM connector_webhook_routes WHERE route_token = :token"),
            {"token": token2},
        )
        assert route_left2 == 0
