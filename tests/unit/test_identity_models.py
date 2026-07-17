"""Identity domain model validation + capability mapping (M3.6)."""

from __future__ import annotations

import pytest

from keel_core.identity import (
    Capability,
    IdentityValidationError,
    MembershipRole,
    capabilities_for_role,
    new_agent_id,
    new_grant_id,
    new_org_id,
    new_user_id,
    normalize_email,
    validate_agent_name,
    validate_display_name,
    validate_org_slug,
)


def test_id_prefixes_are_stable_and_distinct() -> None:
    assert new_user_id().startswith("usr_")
    assert new_org_id().startswith("org_")
    assert new_agent_id().startswith("agt_")
    assert new_grant_id().startswith("grt_")
    assert len({new_user_id(), new_user_id(), new_user_id()}) == 3


@pytest.mark.parametrize("good", ["acme", "acme-corp", "a1b2", "team-42"])
def test_valid_org_slugs(good: str) -> None:
    assert validate_org_slug(good.upper()) == good


@pytest.mark.parametrize("bad", ["ab", "-acme", "acme-", "a" * 41, "spaces here", "UP!"])
def test_invalid_org_slugs(bad: str) -> None:
    with pytest.raises(IdentityValidationError):
        validate_org_slug(bad)


@pytest.mark.parametrize("bad", ["a", "", "x" * 65, " "])
def test_invalid_agent_names(bad: str) -> None:
    with pytest.raises(IdentityValidationError):
        validate_agent_name(bad)


def test_agent_name_trims_and_accepts() -> None:
    assert validate_agent_name("  Support Bot ") == "Support Bot"


def test_display_name_bounds() -> None:
    assert validate_display_name(" Alice ") == "Alice"
    with pytest.raises(IdentityValidationError):
        validate_display_name("")
    with pytest.raises(IdentityValidationError):
        validate_display_name("x" * 201)


def test_email_normalization() -> None:
    assert normalize_email("  Alice@Example.COM ") == "alice@example.com"
    assert normalize_email(None) is None
    assert normalize_email("  ") is None
    with pytest.raises(IdentityValidationError):
        normalize_email("not-an-email")


def test_role_capabilities_are_monotonic() -> None:
    viewer = capabilities_for_role(MembershipRole.viewer)
    member = capabilities_for_role(MembershipRole.member)
    admin = capabilities_for_role(MembershipRole.admin)
    owner = capabilities_for_role(MembershipRole.owner)
    assert viewer == {Capability.read}
    assert viewer < member < admin
    assert admin == owner
    assert Capability.manage in admin and Capability.manage not in member
