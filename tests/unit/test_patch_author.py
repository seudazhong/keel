"""SandboxedLoopPatchAuthor: real agent-loop generation against a fake sandbox (M4 P3a-2).

The fake sandbox reuses the *real* P3a-1 snapshot codec (``parse_snapshot_archive`` on upload,
``build_snapshot_from_directory`` on export) and hands the author a real
``UnsafeLocalDevExecutionEnvironment`` rooted at the namespace directory, so every test exercises
the real file tools, the real loop, and the real ``apply_export_to_worktree`` writeback.
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from keel_core.patch.author import SandboxedLoopPatchAuthor, patch_run_namespace
from keel_core.patch.errors import (
    PatchLeaseLost,
    PatchProviderError,
    PatchProviderUnavailable,
)
from keel_core.patch.models import PatchProposalRequest
from keel_core.patch.transfer import build_snapshot_from_directory, parse_snapshot_archive
from keel_core.patch.transfer_client import (
    DeleteAck,
    SandboxTransferNotFound,
    SandboxTransferRejected,
    SandboxTransferUnavailable,
    UploadAck,
)
from keel_core.protocols import ProviderChunk, ProviderRequest, ToolCall, Usage
from keel_core.testing import ScriptedProviderGateway
from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_core.tools.environment import ExecutionEnvironment
from keel_core.types import FinishReason

_CODING_RUN_ID = "coding-run-42"


# --- helpers -------------------------------------------------------------------------


def _request(**overrides: object) -> PatchProposalRequest:
    base: dict[str, object] = {
        "org_id": "org-1",
        "project_id": "proj-1",
        "actor": "user-1",
        "task": "Rename the greeting from old to new.",
        "base_ref": "main",
        "model": "test/model",
        "idempotency_key": "idem-1",
    }
    base.update(overrides)
    return PatchProposalRequest(**base)  # type: ignore[arg-type]


def _seed_worktree(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "app.py").write_bytes(b"print('old')\n")
    (root / "notes.txt").write_bytes(b"keep me\r\n")  # CRLF must survive the roundtrip
    (root / "remove_me.txt").write_bytes(b"delete this\n")
    (root / "logo.bin").write_bytes(b"\x00\x01\x02BINARY")  # binary must stay byte-identical


def _tool_turn(call_id: str, name: str, **arguments: object) -> list[ProviderChunk]:
    return [
        ProviderChunk(
            tool_call=ToolCall(id=call_id, name=name, arguments=arguments),
            finish_reason=FinishReason.tool_use,
        )
    ]


def _done_turn(
    *, text: str = "All changes complete.\nDONE", usage: Usage | None = None
) -> list[ProviderChunk]:
    return [
        ProviderChunk(
            delta=text,
            finish_reason=FinishReason.end_turn,
            usage=usage if usage is not None else Usage(prompt_tokens=100, completion_tokens=20),
        )
    ]


def _happy_turns() -> list[list[ProviderChunk]]:
    return [
        _tool_turn("c1", "read", path="app.py"),
        _tool_turn("c2", "edit", path="app.py", old="old", new="new"),
        _tool_turn("c3", "write", path="pkg/new_file.py", content="created\n"),
        _tool_turn("c4", "delete", path="remove_me.txt"),
        _done_turn(usage=Usage(prompt_tokens=100, completion_tokens=20, cost_usd=0.3)),
    ]


class _FakeSandbox:
    """A namespace-keyed sandbox backed by real snapshot codec + a real local environment."""

    def __init__(
        self,
        root: Path,
        *,
        cleanup_pending: bool = False,
        fail_delete: bool = False,
        fail_upload: Exception | None = None,
        fail_export: Exception | None = None,
    ) -> None:
        self._root = root
        self._cleanup_pending = cleanup_pending
        self._fail_delete = fail_delete
        self._fail_upload = fail_upload
        self._fail_export = fail_export
        self.uploaded: list[str] = []
        self.exported: list[str] = []
        self.deleted: list[str] = []

    def _ns(self, namespace: str) -> Path:
        return self._root / namespace

    async def upload_snapshot(self, namespace: str, archive: bytes) -> UploadAck:
        if self._fail_upload is not None:
            raise self._fail_upload
        target = self._ns(namespace)
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        parsed = parse_snapshot_archive(archive)
        total = 0
        for snapshot_file in parsed.files.values():
            dest = target / snapshot_file.path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(snapshot_file.data)
            total += snapshot_file.size
        self.uploaded.append(namespace)
        return UploadAck(
            namespace=namespace,
            files=len(parsed.files),
            total_bytes=total,
            cleanup_pending=self._cleanup_pending,
        )

    async def export_snapshot(self, namespace: str) -> bytes:
        if self._fail_export is not None:
            raise self._fail_export
        self.exported.append(namespace)
        archive, _manifest = build_snapshot_from_directory(self._ns(namespace))
        return archive

    async def delete_namespace(self, namespace: str) -> DeleteAck:
        self.deleted.append(namespace)
        if self._fail_delete:
            raise SandboxTransferUnavailable("sandbox delete failed")
        target = self._ns(namespace)
        if target.exists():
            shutil.rmtree(target)
        return DeleteAck(namespace=namespace, deleted=True)

    def environment_factory(self, namespace: str) -> ExecutionEnvironment:
        return UnsafeLocalDevExecutionEnvironment(self._ns(namespace))


def _author(sandbox: _FakeSandbox, provider: object) -> SandboxedLoopPatchAuthor:
    return SandboxedLoopPatchAuthor(
        provider=provider,  # type: ignore[arg-type]
        transfer=sandbox,
        environment_factory=sandbox.environment_factory,
    )


class _RaisingProvider:
    """Yield a partial-usage chunk, then raise a transport error mid-turn (fail closed)."""

    def __init__(self, *, before: list[ProviderChunk], exc: Exception) -> None:
        self._before = before
        self._exc = exc

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        return self._gen()

    async def _gen(self) -> AsyncIterator[ProviderChunk]:
        for chunk in self._before:
            yield chunk
        raise self._exc


class _CapturingProvider:
    """Record the ``max_output_tokens`` each request carries, then complete with DONE."""

    def __init__(self) -> None:
        self.seen_max_output_tokens: list[int | None] = []

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        self.seen_max_output_tokens.append(request.max_output_tokens)
        return self._gen()

    async def _gen(self) -> AsyncIterator[ProviderChunk]:
        yield ProviderChunk(delta="done\nDONE", finish_reason=FinishReason.end_turn)


class _TransientBoom(Exception):
    status_code = 503


class _PermanentBoom(Exception):
    status_code = 400


class _TrippingInterrupt:
    """Return ``True`` once it has been consulted ``trip_on`` times (1-based)."""

    def __init__(self, trip_on: int) -> None:
        self._trip_on = trip_on
        self.calls = 0

    def __call__(self) -> bool:
        self.calls += 1
        return self.calls >= self._trip_on


# --- namespace -----------------------------------------------------------------------


def test_patch_run_namespace_is_deterministic_ws_hex() -> None:
    namespace = patch_run_namespace(_CODING_RUN_ID)
    assert namespace.startswith("ws_")
    hex_part = namespace.removeprefix("ws_")
    assert len(hex_part) == 64
    assert all(char in "0123456789abcdef" for char in hex_part)
    assert namespace == patch_run_namespace(_CODING_RUN_ID)  # stable
    assert namespace != patch_run_namespace("other-run")


# --- happy path ----------------------------------------------------------------------


async def test_author_applies_edits_deletes_and_preserves_bytes(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    provider = ScriptedProviderGateway(_happy_turns())

    result = await _author(sandbox, provider).author(
        worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
    )

    # The disposable worktree now holds exactly the model's changes, byte-for-byte.
    assert (worktree / "app.py").read_bytes() == b"print('new')\n"
    assert (worktree / "pkg" / "new_file.py").read_bytes() == b"created\n"
    assert not (worktree / "remove_me.txt").exists()  # omitted included text is deleted
    assert (worktree / "notes.txt").read_bytes() == b"keep me\r\n"  # CRLF preserved untouched
    assert (worktree / "logo.bin").read_bytes() == b"\x00\x01\x02BINARY"  # binary unchanged

    # Cumulative usage/iterations flow back; the namespace was exported once then cleaned up.
    assert result.usage.cost_usd == pytest.approx(0.3)
    assert result.iterations == 4
    namespace = patch_run_namespace(_CODING_RUN_ID)
    assert sandbox.uploaded == [namespace]
    assert sandbox.exported == [namespace]
    assert sandbox.deleted == [namespace]
    assert not (tmp_path / "sandbox" / namespace).exists()


async def test_author_binary_delete_is_refused_but_run_still_completes(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    # The model tries to delete the binary (the tool refuses), then makes a legit edit + DONE.
    provider = ScriptedProviderGateway(
        [
            _tool_turn("c1", "delete", path="logo.bin"),
            _tool_turn("c2", "edit", path="app.py", old="old", new="new"),
            _done_turn(),
        ]
    )

    result = await _author(sandbox, provider).author(
        worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
    )

    assert result.iterations == 2
    assert (worktree / "logo.bin").read_bytes() == b"\x00\x01\x02BINARY"  # binary survived
    assert (worktree / "app.py").read_bytes() == b"print('new')\n"


async def test_author_injects_output_max_tokens(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    provider = _CapturingProvider()

    await _author(sandbox, provider).author(
        worktree_path=worktree,
        request=_request(output_max_tokens=12345),
        coding_run_id=_CODING_RUN_ID,
    )

    assert provider.seen_max_output_tokens == [12345]


# --- interrupt (lost lease) ----------------------------------------------------------


async def test_interrupt_before_upload_never_transfers(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    provider = ScriptedProviderGateway(_happy_turns())

    with pytest.raises(PatchLeaseLost):
        await _author(sandbox, provider).author(
            worktree_path=worktree,
            request=_request(),
            coding_run_id=_CODING_RUN_ID,
            interrupt=lambda: True,
        )

    assert sandbox.uploaded == []
    assert sandbox.exported == []
    assert sandbox.deleted == []


async def test_interrupt_after_upload_cleans_up_without_export(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    provider = ScriptedProviderGateway(_happy_turns())
    interrupt = _TrippingInterrupt(trip_on=2)  # False before upload, True right after

    with pytest.raises(PatchLeaseLost):
        await _author(sandbox, provider).author(
            worktree_path=worktree,
            request=_request(),
            coding_run_id=_CODING_RUN_ID,
            interrupt=interrupt,
        )

    namespace = patch_run_namespace(_CODING_RUN_ID)
    assert sandbox.uploaded == [namespace]
    assert sandbox.exported == []  # no writeback under a lost lease
    assert sandbox.deleted == [namespace]  # namespace still cleaned up


async def test_interrupt_during_loop_is_lease_lost_without_export(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    provider = ScriptedProviderGateway(_happy_turns())
    interrupt = _TrippingInterrupt(trip_on=3)  # trips at the loop's first iteration check

    with pytest.raises(PatchLeaseLost):
        await _author(sandbox, provider).author(
            worktree_path=worktree,
            request=_request(),
            coding_run_id=_CODING_RUN_ID,
            interrupt=interrupt,
        )

    assert sandbox.exported == []
    assert sandbox.deleted == [patch_run_namespace(_CODING_RUN_ID)]
    assert (worktree / "app.py").read_bytes() == b"print('old')\n"  # untouched


# --- cost ceiling --------------------------------------------------------------------


async def test_cost_ceiling_fails_without_export(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    # The first (tool) turn already overshoots the ceiling; the loop must stop before writeback.
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="edit", arguments={"path": "app.py"}),
                    finish_reason=FinishReason.tool_use,
                    usage=Usage(prompt_tokens=10, completion_tokens=10, cost_usd=1.0),
                )
            ],
            _done_turn(),
        ]
    )

    with pytest.raises(PatchProviderError) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree,
            request=_request(cost_ceiling_usd=0.5),
            coding_run_id=_CODING_RUN_ID,
        )

    # The ceiling failure carries the usage already consumed so the coordinator can charge it.
    assert excinfo.value.usage is not None
    assert excinfo.value.usage.cost_usd == pytest.approx(1.0)
    assert sandbox.exported == []
    assert sandbox.deleted == [patch_run_namespace(_CODING_RUN_ID)]


async def test_single_completed_call_overshoot_still_fails(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    # A single call that reaches DONE but overshoots the ceiling must not export.
    provider = ScriptedProviderGateway(
        [_done_turn(usage=Usage(prompt_tokens=10, completion_tokens=10, cost_usd=9.0))]
    )

    with pytest.raises(PatchProviderError) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree,
            request=_request(cost_ceiling_usd=1.0),
            coding_run_id=_CODING_RUN_ID,
        )

    assert excinfo.value.usage is not None
    assert excinfo.value.usage.cost_usd == pytest.approx(9.0)
    assert sandbox.exported == []


# --- non-completion / provider failures ----------------------------------------------


async def test_completion_without_done_signal_fails(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="I could not do it.", finish_reason=FinishReason.end_turn)]]
    )

    with pytest.raises(PatchProviderError):
        await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    assert sandbox.exported == []
    assert sandbox.deleted == [patch_run_namespace(_CODING_RUN_ID)]


async def test_max_iterations_without_done_fails(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    # The model never signals DONE; the loop halts at the iteration bound -> permanent error.
    provider = ScriptedProviderGateway([_tool_turn("c1", "read", path="app.py")])

    with pytest.raises(PatchProviderError) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree,
            request=_request(max_iterations=1),
            coding_run_id=_CODING_RUN_ID,
        )

    assert excinfo.value.usage is not None  # a non-completion still reports the usage consumed
    assert sandbox.exported == []


async def test_transient_provider_failure_is_unavailable_with_partial_usage(
    tmp_path: Path,
) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    provider = _RaisingProvider(
        before=[ProviderChunk(delta="working", usage=Usage(completion_tokens=5, cost_usd=0.2))],
        exc=_TransientBoom("upstream 503"),
    )

    with pytest.raises(PatchProviderUnavailable) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    assert excinfo.value.usage is not None
    assert excinfo.value.usage.cost_usd == pytest.approx(0.2)
    assert sandbox.exported == []
    assert sandbox.deleted == [patch_run_namespace(_CODING_RUN_ID)]


async def test_permanent_provider_failure_is_provider_error(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox")
    # A partial-usage chunk lands before the permanent failure: the error must still carry it so the
    # coordinator charges the tokens spent even on a terminal failure.
    provider = _RaisingProvider(
        before=[ProviderChunk(delta="partial", usage=Usage(completion_tokens=3, cost_usd=0.05))],
        exc=_PermanentBoom("bad request"),
    )

    with pytest.raises(PatchProviderError) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    assert excinfo.value.usage is not None
    assert excinfo.value.usage.cost_usd == pytest.approx(0.05)
    assert sandbox.exported == []


# --- cleanup semantics ---------------------------------------------------------------


async def test_cleanup_failure_after_success_still_returns_result(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox", fail_delete=True)
    provider = ScriptedProviderGateway(_happy_turns())

    # The worktree is already mutated by a successful apply, so a failed *cleanup* must not raise a
    # retryable error (re-running would re-apply the patch). It returns success + logs a warning.
    with caplog.at_level(logging.WARNING, logger="keel_core.patch.author"):
        result = await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    namespace = patch_run_namespace(_CODING_RUN_ID)
    assert result.iterations == 4  # the successful generation result is preserved
    assert result.usage.cost_usd == pytest.approx(0.3)
    # The writeback happened exactly once (single export + apply, single delete attempt).
    assert (worktree / "app.py").read_bytes() == b"print('new')\n"
    assert sandbox.exported == [namespace]
    assert sandbox.deleted == [namespace]
    assert any(
        "cleanup failed" in record.message and "successful generation" in record.message
        for record in caplog.records
    )


# --- transfer failure mapping (never a raw SandboxTransferClientError) ----------------


async def test_upload_transient_failure_maps_to_unavailable_zero_usage(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(
        tmp_path / "sandbox", fail_upload=SandboxTransferUnavailable("sandbox not ready")
    )
    provider = ScriptedProviderGateway(_happy_turns())

    with pytest.raises(PatchProviderUnavailable) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    # No provider call happened before the upload, so the charge is zero; nothing was exported or
    # cleaned up (the upload never committed).
    assert excinfo.value.usage is not None
    assert excinfo.value.usage.cost_usd == pytest.approx(0.0)
    assert sandbox.exported == []
    assert sandbox.deleted == []
    assert (worktree / "app.py").read_bytes() == b"print('old')\n"  # worktree untouched


async def test_upload_permanent_failure_maps_to_provider_error(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(
        tmp_path / "sandbox",
        fail_upload=SandboxTransferRejected("archive too large", status_code=413),
    )
    provider = ScriptedProviderGateway(_happy_turns())

    with pytest.raises(PatchProviderError) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    assert excinfo.value.usage is not None
    assert excinfo.value.usage.cost_usd == pytest.approx(0.0)
    assert sandbox.exported == []
    assert sandbox.deleted == []


async def test_export_transient_failure_maps_to_unavailable_with_full_usage(
    tmp_path: Path,
) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(
        tmp_path / "sandbox", fail_export=SandboxTransferNotFound("namespace vanished")
    )
    # A clean DONE (no file mutations) so the run completes and we reach the export step.
    provider = ScriptedProviderGateway(
        [_done_turn(usage=Usage(prompt_tokens=50, completion_tokens=10, cost_usd=0.25))]
    )

    with pytest.raises(PatchProviderUnavailable) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    # Generation completed, so the failure carries the full run usage; nothing was applied and the
    # namespace was still cleaned up.
    assert excinfo.value.usage is not None
    assert excinfo.value.usage.cost_usd == pytest.approx(0.25)
    assert sandbox.deleted == [patch_run_namespace(_CODING_RUN_ID)]
    assert (worktree / "app.py").read_bytes() == b"print('old')\n"  # apply never ran


async def test_export_permanent_failure_maps_to_provider_error_with_full_usage(
    tmp_path: Path,
) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(
        tmp_path / "sandbox", fail_export=SandboxTransferRejected("bad export", status_code=422)
    )
    provider = ScriptedProviderGateway(
        [_done_turn(usage=Usage(prompt_tokens=50, completion_tokens=10, cost_usd=0.25))]
    )

    with pytest.raises(PatchProviderError) as excinfo:
        await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    assert excinfo.value.usage is not None
    assert excinfo.value.usage.cost_usd == pytest.approx(0.25)
    assert sandbox.deleted == [patch_run_namespace(_CODING_RUN_ID)]


async def test_cleanup_failure_during_primary_error_preserves_primary(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox", fail_delete=True)
    provider = _RaisingProvider(before=[], exc=_PermanentBoom("bad request"))

    with caplog.at_level(logging.WARNING, logger="keel_core.patch.author"):
        with pytest.raises(PatchProviderError):  # primary error, NOT the cleanup error
            await _author(sandbox, provider).author(
                worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
            )

    assert sandbox.deleted == [patch_run_namespace(_CODING_RUN_ID)]
    assert any("cleanup failed" in record.message for record in caplog.records)


async def test_upload_cleanup_pending_is_observed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    worktree = tmp_path / "worktree"
    _seed_worktree(worktree)
    sandbox = _FakeSandbox(tmp_path / "sandbox", cleanup_pending=True)
    provider = ScriptedProviderGateway(_happy_turns())

    with caplog.at_level(logging.WARNING, logger="keel_core.patch.author"):
        result = await _author(sandbox, provider).author(
            worktree_path=worktree, request=_request(), coding_run_id=_CODING_RUN_ID
        )

    assert result.iterations == 4  # generation still succeeds
    assert any("deferred sandbox cleanup" in record.message for record in caplog.records)
