"""Pure-domain tests for patch models: state machine, bounds, digests, path/branch policy."""

from __future__ import annotations

import pytest

from keel_core.patch.errors import (
    PatchBoundsExceeded,
    PatchPolicyViolation,
    PatchStateError,
    PatchValidationError,
)
from keel_core.patch.models import (
    LEGAL_TRANSITIONS,
    TERMINAL_STATUSES,
    ChangedFile,
    ChangeKind,
    PatchProposalRequest,
    PatchStatus,
    can_transition,
    changed_path_digest,
    ensure_transition,
    is_forbidden_path,
    run_branch_for,
    validate_run_branch,
)


def test_state_machine_has_no_edges_out_of_terminal_states() -> None:
    for status in TERMINAL_STATUSES:
        assert LEGAL_TRANSITIONS[status] == frozenset()
        for other in PatchStatus:
            if other != status:
                assert not can_transition(status, other)


def test_illegal_transition_fails_closed() -> None:
    with pytest.raises(PatchStateError):
        ensure_transition(PatchStatus.generating, PatchStatus.draft_pr_created)
    # Same-status is a no-op (idempotent), never an error.
    ensure_transition(PatchStatus.ready, PatchStatus.ready)


def test_happy_path_transitions_are_legal() -> None:
    chain = [
        PatchStatus.generating,
        PatchStatus.ready,
        PatchStatus.approval_pending,
        PatchStatus.approved,
        PatchStatus.writing,
        PatchStatus.draft_pr_created,
    ]
    for a, b in zip(chain, chain[1:], strict=False):
        assert can_transition(a, b)


def test_stale_reachable_from_every_non_terminal_state() -> None:
    for status in PatchStatus:
        if status in TERMINAL_STATUSES:
            continue
        assert can_transition(status, PatchStatus.stale)


@pytest.mark.parametrize(
    "path",
    [
        ".git/config",
        "a/.git/x",
        ".env",
        "svc/.env",
        "deploy/secrets.yaml",
        "id_rsa",
        "keys/server.pem",
    ],
)
def test_forbidden_paths_rejected(path: str) -> None:
    assert is_forbidden_path(path)


@pytest.mark.parametrize("path", ["src/app.py", "README.md", "a/b/c.ts", "environment.py"])
def test_ordinary_paths_allowed(path: str) -> None:
    assert not is_forbidden_path(path)


def test_changed_file_rejects_forbidden_and_traversal() -> None:
    with pytest.raises(PatchPolicyViolation):
        ChangedFile(path=".env", change_kind=ChangeKind.added, blob_sha="a" * 40, size_bytes=1)
    with pytest.raises(PatchValidationError):
        ChangedFile(path="../escape", change_kind=ChangeKind.added, blob_sha="a" * 40, size_bytes=1)


def test_changed_file_oversize_rejected() -> None:
    with pytest.raises(PatchBoundsExceeded):
        ChangedFile(
            path="big.bin", change_kind=ChangeKind.added, blob_sha="a" * 40, size_bytes=5_000_000
        )


def test_changed_path_digest_is_order_independent_and_content_sensitive() -> None:
    a = ChangedFile(path="a.py", change_kind=ChangeKind.modified, blob_sha="1" * 40, size_bytes=3)
    b = ChangedFile(path="b.py", change_kind=ChangeKind.added, blob_sha="2" * 40, size_bytes=4)
    assert changed_path_digest([a, b]) == changed_path_digest([b, a])
    b2 = ChangedFile(path="b.py", change_kind=ChangeKind.added, blob_sha="3" * 40, size_bytes=4)
    assert changed_path_digest([a, b]) != changed_path_digest([a, b2])


def test_run_branch_validation() -> None:
    branch = run_branch_for("pp_abc123")
    assert branch == "keel/patch/pp_abc123"
    validate_run_branch(branch, default_branch="main")
    with pytest.raises(PatchValidationError):
        validate_run_branch("main", default_branch="main")
    with pytest.raises(PatchValidationError):
        validate_run_branch("feature/x", default_branch="main")
    with pytest.raises(PatchValidationError):
        validate_run_branch("keel/patch/../etc", default_branch="main")


def test_request_validation_bounds() -> None:
    with pytest.raises(PatchValidationError):
        PatchProposalRequest(
            org_id="o",
            project_id="p",
            actor="u",
            task="",
            base_ref="main",
            model="m",
            idempotency_key="k",
        )
    with pytest.raises(PatchValidationError):
        PatchProposalRequest(
            org_id="o",
            project_id="p",
            actor="u",
            task="t",
            base_ref="main",
            model="m",
            idempotency_key="k",
            cost_ceiling_usd=999.0,
        )
