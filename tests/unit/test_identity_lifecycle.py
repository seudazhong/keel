"""The identity data-map entries are honest: erasable, retention-classified, purge-backed."""

from __future__ import annotations

from dataclasses import fields

from keel_core.identity.purge import OrganizationErasureResult, UserErasureResult
from keel_core.lifecycle.datamap import DATA_MAP_BY_NAME, ErasureTreatment
from keel_core.lifecycle.policies import DEFAULT_RETENTION, IDENTITY, RetentionClass

_ORG_SCOPED = {"organizations", "memberships", "agents", "resource_grants"}
_IDENTITY_GLOBAL = {"users", "oidc_identities"}


def test_identity_tables_are_registered_in_the_data_map() -> None:
    for name in _ORG_SCOPED | _IDENTITY_GLOBAL:
        assert name in DATA_MAP_BY_NAME, name
        assert DATA_MAP_BY_NAME[name].resource_class == IDENTITY


def test_identity_retention_is_permanent() -> None:
    policy = DEFAULT_RETENTION[IDENTITY]
    assert policy.retention_class is RetentionClass.permanent
    assert policy.is_permanent  # removed only by explicit erasure, never on a timer


def test_org_scoped_entries_map_to_org_purge_result() -> None:
    org_fields = {f.name for f in fields(OrganizationErasureResult)}
    for name in _ORG_SCOPED:
        entry = DATA_MAP_BY_NAME[name]
        assert entry.treatment is ErasureTreatment.org_scoped
        # Each org-scoped table has a matching count field on the org-erasure result
        # (organizations -> organization).
        expected = "organization" if name == "organizations" else name
        assert expected in org_fields, name


def test_identity_global_entries_map_to_user_purge_result() -> None:
    user_fields = {f.name for f in fields(UserErasureResult)}
    for name in _IDENTITY_GLOBAL:
        entry = DATA_MAP_BY_NAME[name]
        assert entry.treatment is ErasureTreatment.identity_global
        expected = "user" if name == "users" else name
        assert expected in user_fields, name


def test_identity_entries_are_not_scope_erasable() -> None:
    # Identity is org-partitioned, not scope-bound: scope erasure must not claim them.
    scope_erasable = {
        ErasureTreatment.scope_bound,
        ErasureTreatment.session_scoped,
        ErasureTreatment.project_scoped,
    }
    for name in _ORG_SCOPED | _IDENTITY_GLOBAL:
        assert DATA_MAP_BY_NAME[name].treatment not in scope_erasable
