"""Typed execution boundary for commands and workspace file operations.

The runtime only talks to this interface.  The local implementation is deliberately
named unsafe and must be selected explicitly by trusted developer-facing callers.
Server and worker wiring use the sandbox RPC implementation by default.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from keel_core.tools.bounding import MAX_BYTES, MAX_LINES, bound_output

MAX_TIMEOUT_SECONDS = 300.0


class ExecutionErrorCode(StrEnum):
    """Stable failure categories returned across the execution boundary."""

    denied = "denied"
    not_found = "not_found"
    invalid = "invalid"
    timed_out = "timed_out"
    cancelled = "cancelled"
    unavailable = "unavailable"
    failed = "failed"


@dataclass(frozen=True)
class ExecutionError:
    code: ExecutionErrorCode
    message: str


@dataclass(frozen=True)
class ExecutionLimits:
    """Per-operation deadline and model-facing output bounds."""

    timeout_seconds: float = 30.0
    max_lines: int = MAX_LINES
    max_bytes: int = MAX_BYTES

    def __post_init__(self) -> None:
        if not 0 < self.timeout_seconds <= MAX_TIMEOUT_SECONDS:
            raise ValueError(f"timeout_seconds must be in (0, {MAX_TIMEOUT_SECONDS}]")
        if not 0 < self.max_lines <= MAX_LINES:
            raise ValueError(f"max_lines must be in (0, {MAX_LINES}]")
        if not 0 < self.max_bytes <= MAX_BYTES:
            raise ValueError(f"max_bytes must be in (0, {MAX_BYTES}]")


@dataclass
class CancellationToken:
    """Cooperative cancellation signal usable across local and RPC implementations."""

    _event: asyncio.Event = field(default_factory=asyncio.Event)

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()


@dataclass(frozen=True)
class OperationOptions:
    limits: ExecutionLimits = field(default_factory=ExecutionLimits)
    cancellation: CancellationToken | None = None


@dataclass(frozen=True)
class CommandRequest:
    command: str
    options: OperationOptions = field(default_factory=OperationOptions)
    requested_egress_hosts: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class ReadRequest:
    path: str
    options: OperationOptions = field(default_factory=OperationOptions)


@dataclass(frozen=True)
class WriteRequest:
    path: str
    content: str
    options: OperationOptions = field(default_factory=OperationOptions)


@dataclass(frozen=True)
class EditRequest:
    path: str
    old: str
    new: str
    options: OperationOptions = field(default_factory=OperationOptions)


@dataclass(frozen=True)
class ListRequest:
    path: str = "."
    options: OperationOptions = field(default_factory=OperationOptions)


@dataclass(frozen=True)
class GlobRequest:
    pattern: str
    options: OperationOptions = field(default_factory=OperationOptions)


@dataclass(frozen=True)
class GrepRequest:
    pattern: str
    glob: str = "**/*"
    options: OperationOptions = field(default_factory=OperationOptions)


@dataclass(frozen=True)
class ExecutionResult:
    """A bounded, transport-safe operation result."""

    ok: bool
    output: str = ""
    error: ExecutionError | None = None
    exit_code: int | None = None
    truncated: bool = False
    spill_path: str | None = None


class ExecutionEnvironment(Protocol):
    """The complete command/file capability exposed to built-in tools."""

    async def execute(self, request: CommandRequest) -> ExecutionResult: ...

    async def read(self, request: ReadRequest) -> ExecutionResult: ...

    async def write(self, request: WriteRequest) -> ExecutionResult: ...

    async def edit(self, request: EditRequest) -> ExecutionResult: ...

    async def list(self, request: ListRequest) -> ExecutionResult: ...

    async def glob(self, request: GlobRequest) -> ExecutionResult: ...

    async def grep(self, request: GrepRequest) -> ExecutionResult: ...

    async def aclose(self) -> None: ...


class WorkspacePathPolicy:
    """Resolve relative paths beneath one workspace and deny sensitive names."""

    def __init__(
        self,
        workspace: Path | str,
        *,
        deny_names: frozenset[str] = frozenset({".git", ".env"}),
    ) -> None:
        self.root = Path(workspace).resolve()
        self.deny_names = deny_names

    def resolve(self, relative_path: str) -> Path | None:
        if not relative_path or Path(relative_path).is_absolute():
            return None
        candidate = (self.root / relative_path).resolve()
        if candidate != self.root and not candidate.is_relative_to(self.root):
            return None
        relative = candidate.relative_to(self.root)
        if any(part in self.deny_names for part in relative.parts):
            return None
        return candidate

    def allows_pattern(self, pattern: str) -> bool:
        path = Path(pattern)
        return (
            bool(pattern)
            and not path.is_absolute()
            and not any(part == ".." or part in self.deny_names for part in path.parts)
        )

    def relative(self, path: Path) -> Path | None:
        resolved = path.resolve()
        if not resolved.is_relative_to(self.root):
            return None
        relative = resolved.relative_to(self.root)
        if any(part in self.deny_names for part in relative.parts):
            return None
        return relative

    def is_sanitized_for_shell(self) -> bool:
        """Verify denied names are absent from the workspace tree.

        This validates the local workspace contents. Container deployments must also
        mount only this sanitized tree so shell processes cannot reach host parents.
        """
        if not self.root.is_dir() or self.root.name in self.deny_names:
            return False

        def raise_walk_error(error: OSError) -> None:
            raise error

        for current, directories, files in os.walk(
            self.root,
            followlinks=False,
            onerror=raise_walk_error,
        ):
            if any(name in self.deny_names for name in (*directories, *files)):
                return False
            for name in (*directories, *files):
                candidate = Path(current, name)
                if candidate.is_symlink() or candidate.is_junction() or candidate.is_mount():
                    return False
        return True


async def _with_controls[T](
    operation: Awaitable[T],
    options: OperationOptions,
    *,
    on_cancel: Callable[[], Awaitable[None]] | None = None,
) -> T | ExecutionResult:
    """Wait for an operation, deadline, or cooperative cancellation."""

    task = asyncio.ensure_future(operation)
    cancellation_task: asyncio.Task[None] | None = None
    if options.cancellation is not None:
        if options.cancellation.cancelled:
            task.cancel()
            return _error(ExecutionErrorCode.cancelled, "operation cancelled")
        cancellation_task = asyncio.create_task(options.cancellation.wait())
    try:
        waiters: set[asyncio.Future[Any]] = {task}
        if cancellation_task is not None:
            waiters.add(cancellation_task)
        done, _ = await asyncio.wait(
            waiters,
            timeout=options.limits.timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            return await task
        task.cancel()
        if on_cancel is not None:
            await on_cancel()
        await asyncio.gather(task, return_exceptions=True)
        if cancellation_task is not None and cancellation_task in done:
            return _error(ExecutionErrorCode.cancelled, "operation cancelled")
        return _error(
            ExecutionErrorCode.timed_out,
            f"operation timed out after {options.limits.timeout_seconds}s",
        )
    except asyncio.CancelledError:
        task.cancel()
        if on_cancel is not None:
            await on_cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise
    finally:
        if cancellation_task is not None:
            cancellation_task.cancel()


def _error(code: ExecutionErrorCode, message: str) -> ExecutionResult:
    return ExecutionResult(ok=False, output=message, error=ExecutionError(code, message))


class UnavailableExecutionEnvironment:
    """Fail-closed environment used when no execution infrastructure was wired."""

    async def _unavailable(self) -> ExecutionResult:
        return _error(ExecutionErrorCode.unavailable, "execution environment unavailable")

    async def execute(self, request: CommandRequest) -> ExecutionResult:
        return await self._unavailable()

    async def read(self, request: ReadRequest) -> ExecutionResult:
        return await self._unavailable()

    async def write(self, request: WriteRequest) -> ExecutionResult:
        return await self._unavailable()

    async def edit(self, request: EditRequest) -> ExecutionResult:
        return await self._unavailable()

    async def list(self, request: ListRequest) -> ExecutionResult:
        return await self._unavailable()

    async def glob(self, request: GlobRequest) -> ExecutionResult:
        return await self._unavailable()

    async def grep(self, request: GrepRequest) -> ExecutionResult:
        return await self._unavailable()

    async def aclose(self) -> None:
        return None


class UnsafeLocalDevExecutionEnvironment:
    """Opt-in in-process backend for trusted local development only.

    It enforces workspace path policy but cannot provide container-grade process or
    network isolation. Shell execution additionally requires an explicit sanitized
    workspace assertion and revalidates that denied paths are absent before every
    command. Production service wiring rejects this backend by default.
    """

    def __init__(
        self,
        workspace: Path | str,
        *,
        spill_dir: Path | None = None,
        shell_workspace_provisioned: bool = False,
    ) -> None:
        self._policy = WorkspacePathPolicy(workspace)
        self._spill_dir = spill_dir
        self._shell_workspace_provisioned = shell_workspace_provisioned

    def _bounded(
        self,
        text: str,
        options: OperationOptions,
        *,
        ok: bool = True,
        exit_code: int | None = None,
    ) -> ExecutionResult:
        bounded = bound_output(
            text,
            max_lines=options.limits.max_lines,
            max_bytes=options.limits.max_bytes,
            spill_dir=self._spill_dir,
        )
        return ExecutionResult(
            ok=ok,
            output=bounded.text,
            exit_code=exit_code,
            truncated=bounded.truncated,
            spill_path=bounded.spill_path,
        )

    async def execute(self, request: CommandRequest) -> ExecutionResult:
        if not request.command:
            return _error(ExecutionErrorCode.invalid, "empty command")
        if not self._shell_workspace_provisioned:
            return _error(
                ExecutionErrorCode.denied,
                "shell workspace was not explicitly provisioned as sanitized",
            )
        try:
            sanitized = await asyncio.to_thread(self._policy.is_sanitized_for_shell)
        except OSError:
            return _error(
                ExecutionErrorCode.denied,
                "shell workspace sanitization could not be verified",
            )
        if not sanitized:
            return _error(
                ExecutionErrorCode.denied,
                "shell workspace contains denied paths",
            )
        if request.requested_egress_hosts:
            return _error(
                ExecutionErrorCode.denied,
                "unsafe local execution cannot enforce requested network policy",
            )
        process = await asyncio.create_subprocess_shell(
            request.command,
            cwd=str(self._policy.root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        async def terminate() -> None:
            if process.returncode is None:
                process.kill()
                await process.wait()

        controlled = await _with_controls(
            process.communicate(),
            request.options,
            on_cancel=terminate,
        )
        if isinstance(controlled, ExecutionResult):
            return controlled
        stdout, _ = controlled
        text = stdout.decode("utf-8", errors="replace")
        output = text or f"(exit {process.returncode})"
        return self._bounded(
            output,
            request.options,
            ok=process.returncode == 0,
            exit_code=process.returncode,
        )

    async def _blocking(
        self,
        operation: Callable[[], ExecutionResult],
        options: OperationOptions,
    ) -> ExecutionResult:
        controlled = await _with_controls(asyncio.to_thread(operation), options)
        if isinstance(controlled, ExecutionResult):
            return controlled
        return controlled

    async def read(self, request: ReadRequest) -> ExecutionResult:
        def operation() -> ExecutionResult:
            path = self._policy.resolve(request.path)
            if path is None:
                return _error(ExecutionErrorCode.denied, "path denied or outside workspace")
            if not path.is_file():
                return _error(ExecutionErrorCode.not_found, "file not found")
            return self._bounded(
                path.read_text(encoding="utf-8", errors="replace"),
                request.options,
            )

        return await self._blocking(operation, request.options)

    async def write(self, request: WriteRequest) -> ExecutionResult:
        def operation() -> ExecutionResult:
            path = self._policy.resolve(request.path)
            if path is None:
                return _error(ExecutionErrorCode.denied, "path denied or outside workspace")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(request.content, encoding="utf-8")
            return ExecutionResult(ok=True, output=f"wrote {len(request.content)} bytes")

        return await self._blocking(operation, request.options)

    async def edit(self, request: EditRequest) -> ExecutionResult:
        def operation() -> ExecutionResult:
            path = self._policy.resolve(request.path)
            if path is None or not path.is_file():
                return _error(
                    ExecutionErrorCode.denied,
                    "path denied or file not found",
                )
            text = path.read_text(encoding="utf-8")
            occurrences = text.count(request.old)
            if occurrences == 0:
                return _error(ExecutionErrorCode.not_found, "`old` string not found")
            if occurrences > 1:
                return _error(ExecutionErrorCode.invalid, "`old` string is not unique")
            path.write_text(text.replace(request.old, request.new, 1), encoding="utf-8")
            return ExecutionResult(ok=True, output="edited 1 occurrence")

        return await self._blocking(operation, request.options)

    async def list(self, request: ListRequest) -> ExecutionResult:
        def operation() -> ExecutionResult:
            path = self._policy.resolve(request.path)
            if path is None or not path.is_dir():
                return _error(
                    ExecutionErrorCode.denied,
                    "path denied or not a directory",
                )
            entries = sorted(
                f"{item.name}/" if item.is_dir() else item.name for item in path.iterdir()
            )
            return self._bounded("\n".join(entries), request.options)

        return await self._blocking(operation, request.options)

    async def glob(self, request: GlobRequest) -> ExecutionResult:
        def operation() -> ExecutionResult:
            if not self._policy.allows_pattern(request.pattern):
                return _error(ExecutionErrorCode.denied, "glob pattern denied")
            try:
                candidates = list(self._policy.root.glob(request.pattern))
            except (ValueError, NotImplementedError) as exc:
                return _error(ExecutionErrorCode.invalid, f"invalid glob pattern: {exc}")
            matches = [
                str(relative)
                for match in candidates
                if (relative := self._policy.relative(match)) is not None
            ]
            return self._bounded("\n".join(sorted(matches)), request.options)

        return await self._blocking(operation, request.options)

    async def grep(self, request: GrepRequest) -> ExecutionResult:
        def operation() -> ExecutionResult:
            if not self._policy.allows_pattern(request.glob):
                return _error(ExecutionErrorCode.denied, "glob pattern denied")
            try:
                regex = re.compile(request.pattern)
            except re.error as exc:
                return _error(ExecutionErrorCode.invalid, f"invalid regex: {exc}")
            try:
                candidates = sorted(self._policy.root.glob(request.glob))
            except (ValueError, NotImplementedError) as exc:
                return _error(ExecutionErrorCode.invalid, f"invalid glob pattern: {exc}")
            hits: list[str] = []
            for match in candidates:
                relative = self._policy.relative(match)
                if relative is None or not match.is_file():
                    continue
                for lineno, line in enumerate(
                    match.read_text(encoding="utf-8", errors="replace").splitlines(),
                    start=1,
                ):
                    if regex.search(line):
                        hits.append(f"{relative}:{lineno}:{line}")
            return self._bounded("\n".join(hits), request.options)

        return await self._blocking(operation, request.options)

    async def aclose(self) -> None:
        return None
