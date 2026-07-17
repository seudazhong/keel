"""Unit tests for the operator identity-erasure CLI (no live database).

Covers: fail-closed on a missing maintenance URL, non-destructive preflight/dry-run, explicit
confirmation (non-interactive requires ``--yes``; interactive requires typing the id back), the
blocked-owner path, structured output, and the guarantee that no credential/URL is ever
printed.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from keel_core.config import Settings
from keel_core.identity.erase_cli import (
    EXIT_ABORTED,
    EXIT_BLOCKED,
    EXIT_CONFIG,
    EXIT_OK,
    build_parser,
    run_cli,
)
from keel_core.identity.purge import (
    OrganizationErasureResult,
    UserErasureBlockedError,
    UserErasureResult,
)

_SECRET_URL = "postgresql+psycopg://maint:s3cr3t-pw@db.internal/keel"


@dataclass
class FakeRepo:
    """Records calls and returns canned erasure results (no database)."""

    user_result: UserErasureResult | None = None
    org_result: OrganizationErasureResult | None = None
    blocked_ids: tuple[str, ...] | None = None
    principal: str = "keel_erase_login_test"
    calls: list[tuple[str, str, bool]] = field(default_factory=list)
    closed: bool = False

    async def verify_principal(self) -> str:
        return self.principal

    async def erase_user(self, user_id: str, *, dry_run: bool = False) -> UserErasureResult:
        self.calls.append(("user", user_id, dry_run))
        if self.blocked_ids:
            raise UserErasureBlockedError(self.blocked_ids)
        assert self.user_result is not None
        return self.user_result

    async def erase_organization(
        self, org_id: str, *, dry_run: bool = False
    ) -> OrganizationErasureResult:
        self.calls.append(("organization", org_id, dry_run))
        assert self.org_result is not None
        return self.org_result

    async def aclose(self) -> None:
        self.closed = True


def _args(*argv: str):
    return build_parser().parse_args(list(argv))


def _factory(repo: FakeRepo):
    async def make(_settings: Settings) -> FakeRepo:
        return repo

    return make


def _run(args, repo: FakeRepo, **kw) -> int:
    settings = Settings(maintenance_database_url=_SECRET_URL)
    return asyncio.run(run_cli(args, settings=settings, repo_factory=_factory(repo), **kw))


def test_fails_closed_when_maintenance_url_missing(capsys: pytest.CaptureFixture[str]) -> None:
    # Default factory -> create_identity_purge_repository -> require_maintenance_database_url.
    rc = asyncio.run(
        run_cli(_args("user", "u-1", "--yes"), settings=Settings(maintenance_database_url=""))
    )
    assert rc == EXIT_CONFIG
    assert "fails closed" in capsys.readouterr().err


def test_dry_run_previews_without_erasing(capsys: pytest.CaptureFixture[str]) -> None:
    repo = FakeRepo(
        user_result=UserErasureResult(
            oidc_identities=1, agents=0, memberships=1, resource_grants=0, user=1
        )
    )
    rc = _run(_args("user", "u-1", "--dry-run", "--json"), repo)
    assert rc == EXIT_OK
    # Only the preflight (dry_run=True) ran; no real erasure.
    assert repo.calls == [("user", "u-1", True)]
    assert '"status": "preview"' in capsys.readouterr().out
    assert repo.closed is True


def test_non_interactive_without_yes_aborts(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    repo = FakeRepo(user_result=UserErasureResult(0, 0, 1, 0, 1))
    rc = _run(_args("user", "u-1"), repo)
    assert rc == EXIT_ABORTED
    # Preflight ran, but no destructive call was made.
    assert repo.calls == [("user", "u-1", True)]
    assert "not confirmed" in capsys.readouterr().err


def test_interactive_confirmation_mismatch_aborts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    repo = FakeRepo(user_result=UserErasureResult(0, 0, 1, 0, 1))
    rc = _run(_args("user", "u-1"), repo, input_fn=lambda _p: "wrong-id")
    assert rc == EXIT_ABORTED
    assert repo.calls == [("user", "u-1", True)]  # only preflight


def test_interactive_confirmation_match_erases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    repo = FakeRepo(user_result=UserErasureResult(1, 1, 1, 1, 1))
    rc = _run(_args("user", "u-1"), repo, input_fn=lambda _p: "u-1")
    assert rc == EXIT_OK
    # Preflight then the real erasure.
    assert repo.calls == [("user", "u-1", True), ("user", "u-1", False)]


def test_yes_flag_erases_user(capsys: pytest.CaptureFixture[str]) -> None:
    repo = FakeRepo(user_result=UserErasureResult(1, 2, 3, 4, 1, archived_organizations=1))
    rc = _run(_args("user", "u-1", "--yes", "--json"), repo)
    assert rc == EXIT_OK
    assert repo.calls == [("user", "u-1", True), ("user", "u-1", False)]
    out = capsys.readouterr().out
    assert '"status": "erased"' in out


def test_yes_flag_erases_organization(capsys: pytest.CaptureFixture[str]) -> None:
    repo = FakeRepo(org_result=OrganizationErasureResult(2, 3, 4, 1))
    rc = _run(_args("organization", "org-9", "--yes", "--json"), repo)
    assert rc == EXIT_OK
    assert repo.calls == [
        ("organization", "org-9", True),
        ("organization", "org-9", False),
    ]
    assert '"status": "erased"' in capsys.readouterr().out


def test_blocked_owner_reported_and_not_erased(capsys: pytest.CaptureFixture[str]) -> None:
    repo = FakeRepo(blocked_ids=("org-a", "org-b"))
    rc = _run(_args("user", "u-1", "--yes", "--json"), repo)
    assert rc == EXIT_BLOCKED
    out = capsys.readouterr().out
    assert '"status": "blocked"' in out
    assert "org-a" in out and "org-b" in out
    # The block is discovered at preflight, so no destructive erase is attempted.
    assert repo.calls == [("user", "u-1", True)]


def test_never_leaks_credentials(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    repo = FakeRepo(user_result=UserErasureResult(1, 1, 1, 1, 1))
    _run(_args("user", "u-1", "--json"), repo)
    captured = capsys.readouterr()
    assert "s3cr3t-pw" not in captured.out
    assert "s3cr3t-pw" not in captured.err
    assert _SECRET_URL not in captured.out
    assert _SECRET_URL not in captured.err
