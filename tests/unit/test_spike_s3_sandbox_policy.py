"""Spike S3 acceptance: sandbox path allow-list + egress deny."""

from __future__ import annotations

from keel_sandbox.policy import EgressPolicy, PathPolicy


def test_paths_within_workspace_allowed() -> None:
    policy = PathPolicy("/work")
    assert policy.is_allowed("/work/src/app.py")
    assert policy.is_allowed("src/app.py")  # relative -> joined under workspace
    assert policy.is_allowed("/work")


def test_path_escape_denied() -> None:
    policy = PathPolicy("/work")
    assert not policy.is_allowed("/etc/passwd")
    assert not policy.is_allowed("/work/../etc/passwd")
    assert not policy.is_allowed("../secret")


def test_sensitive_names_denied() -> None:
    policy = PathPolicy("/work")
    assert not policy.is_allowed("/work/.git/config")
    assert not policy.is_allowed("/work/.env")
    assert not policy.is_allowed("sub/.git/HEAD")


def test_egress_off_by_default() -> None:
    assert not EgressPolicy().is_allowed("example.com")


def test_egress_blocks_loopback_and_ssrf_even_when_enabled() -> None:
    policy = EgressPolicy(
        allow_hosts=frozenset({"example.com", "127.0.0.1", "169.254.169.254"}),
        network_enabled=True,
    )
    assert policy.is_allowed("example.com")
    assert not policy.is_allowed("127.0.0.1")  # loopback
    assert not policy.is_allowed("169.254.169.254")  # cloud metadata (link-local)
    assert not policy.is_allowed("10.0.0.5")  # private
    assert not policy.is_allowed("localhost")
    assert not policy.is_allowed("unlisted.example.org")  # not in allow-list
