"""Unit tests: session ownership/visibility composition (R1B), independent of Agent access.

Pure-function / in-memory-store coverage for :mod:`keel_core.session_visibility` — the
Postgres-backed atomicity/idempotency of ``ensure_session_identity`` is covered by the
integration adversarial suite (``tests/integration/test_r1b_agent_access_session_visibility.py``),
which requires a live Postgres substrate.
"""

from __future__ import annotations

from keel_core.session_visibility import (
    InMemorySessionAccessStore,
    SessionIdentity,
    SessionVisibility,
    can_manage_session_visibility,
    can_view_session,
)

ALICE = "usr_alice"
BOB = "usr_bob"
CAROL = "usr_carol"


def _identity(
    *,
    owner: str | None = None,
    org_id: str | None = "org_a",
    channel_provider: str | None = None,
    visibility: SessionVisibility = SessionVisibility.private,
) -> SessionIdentity:
    return SessionIdentity(
        scope_id="agent:org_a/agt_1",
        session_id="s1",
        org_id=org_id,
        owner_user_id=owner,
        channel_provider=channel_provider,
        channel_external_id="chat-1" if channel_provider else None,
        visibility=visibility,
    )


def test_owner_always_sees_their_own_session() -> None:
    identity = _identity(owner=ALICE, visibility=SessionVisibility.private)
    assert can_view_session(
        identity, actor_user_id=ALICE, has_active_agent_access=True, has_explicit_share=False
    )
    assert can_view_session(
        identity, actor_user_id=ALICE, has_active_agent_access=False, has_explicit_share=False
    )


def test_private_session_denies_non_owner_even_with_agent_access() -> None:
    """Using a team Agent never by itself grants reading another user's private session."""
    identity = _identity(owner=ALICE, visibility=SessionVisibility.private)
    assert not can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )


def test_agent_members_visibility_requires_active_agent_access() -> None:
    """agent_members visibility works only for principals holding active Agent Access."""
    identity = _identity(owner=ALICE, visibility=SessionVisibility.agent_members)
    assert can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )
    assert not can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=False, has_explicit_share=False
    )


def test_ownerless_machine_session_is_agent_members_visible() -> None:
    identity = _identity(owner=None, org_id="org_a", visibility=SessionVisibility.agent_members)
    assert not identity.is_legacy
    assert can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )


def test_explicit_visibility_requires_share_not_agent_access() -> None:
    identity = _identity(owner=ALICE, visibility=SessionVisibility.explicit)
    # Agent access alone is not enough for 'explicit' visibility.
    assert not can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )
    assert can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=True
    )


def test_legacy_session_falls_back_to_agent_scope_gate() -> None:
    """A row with no owner and no channel identity predates R1B (or an untouched surface)."""
    identity = _identity(owner=None, org_id=None, visibility=SessionVisibility.agent_members)
    assert identity.is_legacy
    assert can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )
    assert not can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=False, has_explicit_share=False
    )


def test_post_migration_unwired_session_with_private_default_is_still_legacy() -> None:
    identity = _identity(owner=None, org_id=None, visibility=SessionVisibility.private)
    assert identity.is_legacy
    assert can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )


def test_erased_owner_does_not_turn_private_session_into_legacy_shared_session() -> None:
    identity = _identity(owner=None, org_id="org_a", visibility=SessionVisibility.private)
    assert not identity.is_legacy
    assert not can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )


def test_private_channel_session_is_owned_by_run_as_user() -> None:
    """A private IM chat's session is owned by the run-as user; other users are denied."""
    identity = _identity(
        owner=ALICE, channel_provider="telegram", visibility=SessionVisibility.private
    )
    assert not identity.is_legacy
    assert can_view_session(
        identity, actor_user_id=ALICE, has_active_agent_access=True, has_explicit_share=False
    )
    assert not can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )


def test_group_channel_session_is_agent_members_visible() -> None:
    """A group IM channel session has no single owner; agent_members governs its readers."""
    identity = _identity(
        owner=None, channel_provider="telegram", visibility=SessionVisibility.agent_members
    )
    assert not identity.is_legacy  # channel identity present -> not the legacy fallback
    assert can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=True, has_explicit_share=False
    )
    assert not can_view_session(
        identity, actor_user_id=BOB, has_active_agent_access=False, has_explicit_share=False
    )


def test_can_manage_session_visibility_requires_owner_or_agent_manage() -> None:
    identity = _identity(owner=ALICE, visibility=SessionVisibility.private)
    assert can_manage_session_visibility(
        identity, actor_user_id=ALICE, actor_can_manage_agent=False
    )
    assert not can_manage_session_visibility(
        identity, actor_user_id=BOB, actor_can_manage_agent=False
    )
    assert can_manage_session_visibility(identity, actor_user_id=BOB, actor_can_manage_agent=True)


async def test_in_memory_share_store_grant_list_revoke_roundtrip() -> None:
    store = InMemorySessionAccessStore()
    scope, session_id = "agent:org_a/agt_1", "s1"
    share = await store.create_share(scope, session_id, BOB, granted_by_user_id=ALICE)
    assert share.user_id == BOB
    assert await store.has_active_share(scope, session_id, BOB)
    assert not await store.has_active_share(scope, session_id, CAROL)

    listed = await store.list_shares(scope, session_id)
    assert [s.user_id for s in listed] == [BOB]

    revoked = await store.revoke_share(scope, session_id, BOB)
    assert revoked is not None and revoked.status.value == "revoked"
    assert not await store.has_active_share(scope, session_id, BOB)

    # Re-granting reactivates the same row rather than creating a duplicate.
    regranted = await store.create_share(scope, session_id, BOB, granted_by_user_id=ALICE)
    assert regranted.id == share.id
    assert await store.has_active_share(scope, session_id, BOB)
