"""Integration proof: SandboxedLoopPatchAuthor against the *real* sandbox transfer service.

Drives the author with a real :class:`SandboxTransferClient` over the real :func:`create_app`
transfer routes (in-process ASGI — the integration event loop is a Windows selector loop that
cannot spawn subprocesses) and a real agent loop whose file tools operate on the same on-disk
namespace the transfer service manages. Proves the upload -> loop -> export -> apply -> delete
round trip end to end. No Postgres, Redis, or public network is touched.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from keel_core.patch.author import SandboxedLoopPatchAuthor, patch_run_namespace
from keel_core.patch.models import PatchProposalRequest
from keel_core.patch.transfer_client import SandboxTransferClient, SandboxTransferNotFound
from keel_core.protocols import ProviderChunk, ToolCall, Usage
from keel_core.testing import ScriptedProviderGateway
from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_core.tools.environment import ExecutionEnvironment
from keel_core.types import FinishReason
from keel_sandbox.service import DirectoryWorkspaceProvider, create_app
from keel_sandbox.transfer import SandboxTransferService

pytestmark = pytest.mark.integration

_SECRET = "test-sandbox-rpc-secret-" + ("x" * 32)
_CODING_RUN_ID = "coding-run-integration"


def _make_transfer(tmp_path: Path) -> tuple[SandboxTransferClient, Path]:
    namespaces_root = tmp_path / "namespaces"
    provider = DirectoryWorkspaceProvider(
        namespaces_root,
        lambda root: UnsafeLocalDevExecutionEnvironment(root),
    )
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path / "default"),
        isolation_verified=True,
        shared_secret=_SECRET,
        workspace_provider=provider,
        transfer_service=SandboxTransferService(provider),
    )
    inner = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://sandbox")
    client = SandboxTransferClient("http://sandbox", shared_secret=_SECRET, client=inner)
    return client, namespaces_root


def _request() -> PatchProposalRequest:
    return PatchProposalRequest(
        org_id="org-1",
        project_id="proj-1",
        actor="user-1",
        task="Rename the greeting and drop the stale file.",
        base_ref="main",
        model="test/model",
        idempotency_key="idem-int",
    )


async def test_author_round_trip_against_real_transfer_service(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / "app.py").write_bytes(b"print('old')\n")
    (worktree / "notes.txt").write_bytes(b"keep\r\n")
    (worktree / "stale.txt").write_bytes(b"remove me\n")
    (worktree / "logo.bin").write_bytes(b"\x00\x01\x02BIN")

    client, namespaces_root = _make_transfer(tmp_path)

    def environment_factory(namespace: str) -> ExecutionEnvironment:
        return UnsafeLocalDevExecutionEnvironment(namespaces_root / namespace)

    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c1",
                        name="edit",
                        arguments={
                            "path": "app.py",
                            "old": "old",
                            "new": "new",
                        },
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c2", name="delete", arguments={"path": "stale.txt"}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [
                ProviderChunk(
                    delta="Done.\nDONE",
                    finish_reason=FinishReason.end_turn,
                    usage=Usage(prompt_tokens=50, completion_tokens=10, cost_usd=0.1),
                )
            ],
        ]
    )
    author = SandboxedLoopPatchAuthor(
        provider=provider,
        transfer=client,
        environment_factory=environment_factory,
    )

    try:
        result = await author.author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )
    finally:
        await client.aclose()

    assert result.iterations == 2
    assert (worktree / "app.py").read_bytes() == b"print('new')\n"
    assert not (worktree / "stale.txt").exists()
    assert (worktree / "notes.txt").read_bytes() == b"keep\r\n"  # CRLF preserved
    assert (worktree / "logo.bin").read_bytes() == b"\x00\x01\x02BIN"  # binary untouched

    # The real delete route retired the namespace: a later export fails closed as not found.
    namespace = patch_run_namespace(_CODING_RUN_ID)
    assert not (namespaces_root / namespace).exists()
    client2, _ = _make_transfer(tmp_path)
    try:
        with pytest.raises(SandboxTransferNotFound):
            await client2.export_snapshot(namespace)
    finally:
        await client2.aclose()
