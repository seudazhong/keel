"""No-follow / exclusive workspace-namespace isolation (M3.6 finding 5).

The namespace root is the isolation boundary, so :class:`DirectoryWorkspaceProvider` must
provision it exclusively and reject a root that is a symlink / junction / reparse point or an
alias to a different directory — even one whose target stays inside the base — and it must
re-validate on every ``resolve`` so a swap between validation and use is caught.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from keel_sandbox.service import DirectoryWorkspaceProvider

_NS_A = "ws_" + ("a" * 32)
_NS_B = "ws_" + ("b" * 32)


class _StubEnv:
    def __init__(self, root: Path) -> None:
        self.root = root

    async def aclose(self) -> None:  # pragma: no cover - trivial
        return None


def _provider(base: Path) -> DirectoryWorkspaceProvider:
    return DirectoryWorkspaceProvider(base, lambda root: _StubEnv(root))  # type: ignore[arg-type]


def _can_symlink(tmp_path: Path) -> bool:
    probe = tmp_path / "_probe_link"
    target = tmp_path / "_probe_target"
    target.mkdir()
    try:
        os.symlink(target, probe, target_is_directory=True)
    except (OSError, NotImplementedError):
        return False
    finally:
        if probe.exists() or probe.is_symlink():
            probe.unlink()
    return True


def test_resolve_provisions_a_real_namespace_directory(tmp_path: Path) -> None:
    provider = _provider(tmp_path / "ns")
    env = provider.resolve(_NS_A)
    assert env is not None
    root = tmp_path / "ns" / _NS_A
    assert root.is_dir() and not root.is_symlink()
    # Cached and stable across calls.
    assert provider.resolve(_NS_A) is env


def test_invalid_namespace_token_is_rejected(tmp_path: Path) -> None:
    provider = _provider(tmp_path / "ns")
    assert provider.resolve("../escape") is None
    assert provider.resolve("ws_../x") is None
    assert provider.resolve("notns") is None


def test_regular_file_in_place_of_namespace_root_fails_closed(tmp_path: Path) -> None:
    base = tmp_path / "ns"
    base.mkdir()
    # Plant a regular file where the namespace directory would live.
    (base / _NS_A).write_text("not a directory")
    provider = _provider(base)
    assert provider.resolve(_NS_A) is None


def test_symlink_alias_inside_base_is_rejected(tmp_path: Path) -> None:
    if not _can_symlink(tmp_path):
        pytest.skip("symlink creation not permitted on this platform/user")
    base = tmp_path / "ns"
    base.mkdir()
    victim = base / _NS_B
    victim.mkdir()
    # Plant NS_A as a symlink to NS_B: the alias target stays *inside* the base, but must still
    # be rejected so NS_A can never share NS_B's tree.
    os.symlink(victim, base / _NS_A, target_is_directory=True)
    provider = _provider(base)
    assert provider.resolve(_NS_A) is None


def test_revalidates_each_request_defeating_swap(tmp_path: Path) -> None:
    if not _can_symlink(tmp_path):
        pytest.skip("symlink creation not permitted on this platform/user")
    base = tmp_path / "ns"
    provider = _provider(base)
    # First resolve provisions a genuine directory.
    assert provider.resolve(_NS_A) is not None
    # Swap the validated root for a symlink alias after first use.
    root = base / _NS_A
    for child in list(root.iterdir()):
        child.unlink()
    root.rmdir()
    victim = base / _NS_B
    victim.mkdir()
    os.symlink(victim, root, target_is_directory=True)
    # The next request re-validates and fails closed rather than trusting the cache.
    assert provider.resolve(_NS_A) is None
