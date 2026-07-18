"""Contract tests for ``deploy/docker/sandbox.Dockerfile`` (M3B hardening).

Deterministic, Docker-free assertions that the sandbox image builds a *minimal, sandbox-only
closure*: the workspace dev group (pytest/ruff/mypy) is excluded and no control-plane/SDK package
source ships in the image. The live image build (see docs/OPERATIONS.md) additionally proves the
absence at runtime by inspecting ``/app/packages`` and the installed site-packages.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "deploy" / "docker" / "sandbox.Dockerfile"

# The only members that belong in the sandbox runtime closure.
_CLOSURE = ("keel-core", "keel-sandbox")
# Workspace packages that must NOT ship in the sandbox image.
_NON_CLOSURE = ("keel-server", "keel-worker", "keel-cli", "keel-sdk", "keel-scheduler")


def _dockerfile_text() -> str:
    assert DOCKERFILE.exists(), "deploy/docker/sandbox.Dockerfile is missing"
    return DOCKERFILE.read_text(encoding="utf-8")


def test_uv_sync_excludes_dev_group() -> None:
    text = _dockerfile_text()
    sync_lines = [
        ln for ln in text.splitlines() if "uv sync" in ln and not ln.strip().startswith("#")
    ]
    assert sync_lines, "sandbox image must run `uv sync`"
    for line in sync_lines:
        assert "--no-dev" in line, f"uv sync must drop the dev group (pytest/ruff/mypy): {line!r}"


def test_installs_only_the_sandbox_member() -> None:
    text = _dockerfile_text()
    assert "--package keel-sandbox" in text, "must target only the keel-sandbox workspace member"


def test_prunes_every_non_closure_package_source() -> None:
    text = _dockerfile_text()
    # The build must delete package trees, keeping only the runtime closure.
    assert "rm -rf" in text, "non-closure package sources must be pruned from the image"
    for keep in _CLOSURE:
        assert f"packages/{keep}/" in text, f"the prune must explicitly preserve {keep}"
    # No non-closure package may be special-cased into the keep list.
    for pkg in _NON_CLOSURE:
        assert f"packages/{pkg}/)" not in text, f"{pkg} must not be preserved by the prune"


def test_runs_as_non_root_and_bakes_no_secret() -> None:
    text = _dockerfile_text()
    assert "USER 10100:10100" in text, "sandbox image must run as the non-root uid/gid 10100"
    # The image must not GENERATE or bake a secret; it is mounted read-only at runtime and only
    # referenced by the KEEL_SANDBOX_RPC_SECRET_FILE env-var name.
    assert "token_urlsafe" not in text, "no secret may be generated/baked into the sandbox image"
