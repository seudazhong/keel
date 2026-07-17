"""Sandbox execution RPC contract and fail-closed HTTP client."""

from __future__ import annotations

import asyncio
from enum import StrEnum
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

from keel_core.tools.bounding import bound_output
from keel_core.tools.environment import (
    CommandRequest,
    EditRequest,
    ExecutionEnvironment,
    ExecutionError,
    ExecutionErrorCode,
    ExecutionLimits,
    ExecutionResult,
    GlobRequest,
    GrepRequest,
    ListRequest,
    OperationOptions,
    ReadRequest,
    WriteRequest,
)
from keel_core.tools.rpc_auth import RpcRequestSigner


class ExecutionOperation(StrEnum):
    command = "command"
    read = "read"
    write = "write"
    edit = "edit"
    list = "list"
    glob = "glob"
    grep = "grep"


class RpcLimits(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timeout_seconds: float = Field(default=30.0, gt=0, le=300.0)
    max_lines: int = Field(default=2000, gt=0, le=2000)
    max_bytes: int = Field(default=50_000, gt=0, le=50_000)


class ExecutionRpcRequest(BaseModel):
    """Versioned operation envelope accepted by ``keel-sandbox``."""

    model_config = ConfigDict(extra="forbid")

    version: int = Field(default=1, ge=1, le=1)
    operation: ExecutionOperation
    path: str | None = None
    command: str | None = None
    content: str | None = None
    old: str | None = None
    new: str | None = None
    pattern: str | None = None
    glob: str | None = None
    requested_egress_hosts: list[str] = Field(default_factory=list)
    limits: RpcLimits = Field(default_factory=RpcLimits)
    # Opaque per-scope workspace namespace (``ws_<hex>``) requested by the caller so the
    # executor confines file/shell operations to that scope's isolated workspace under the
    # sandbox's configured root — never a shared writable workspace across scopes (M3.6 item
    # 3). ``None`` means the caller did not request scoped isolation (single-workspace/local
    # preview). A malformed value, or a scoped request a sandbox cannot provision, fails closed.
    workspace: str | None = Field(default=None, max_length=64)


class RpcError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: ExecutionErrorCode
    message: str


class ExecutionRpcResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(default=1, ge=1, le=1)
    ok: bool
    output: str = ""
    error: RpcError | None = None
    exit_code: int | None = None
    truncated: bool = False
    spill_path: str | None = None

    @classmethod
    def from_result(cls, result: ExecutionResult) -> ExecutionRpcResponse:
        error = (
            None
            if result.error is None
            else RpcError(code=result.error.code, message=result.error.message)
        )
        return cls(
            ok=result.ok,
            output=result.output,
            error=error,
            exit_code=result.exit_code,
            truncated=result.truncated,
            spill_path=result.spill_path,
        )

    def to_result(self) -> ExecutionResult:
        error = (
            None
            if self.error is None
            else ExecutionError(code=self.error.code, message=self.error.message)
        )
        return ExecutionResult(
            ok=self.ok,
            output=self.output,
            error=error,
            exit_code=self.exit_code,
            truncated=self.truncated,
            spill_path=self.spill_path,
        )


def _rpc_limits(options: OperationOptions) -> RpcLimits:
    limits = options.limits
    return RpcLimits(
        timeout_seconds=limits.timeout_seconds,
        max_lines=limits.max_lines,
        max_bytes=limits.max_bytes,
    )


def request_options(request: ExecutionRpcRequest) -> OperationOptions:
    return OperationOptions(
        limits=ExecutionLimits(
            timeout_seconds=request.limits.timeout_seconds,
            max_lines=request.limits.max_lines,
            max_bytes=request.limits.max_bytes,
        )
    )


def _rpc_failure(code: ExecutionErrorCode, message: str) -> ExecutionResult:
    return ExecutionResult(
        ok=False,
        output=message,
        error=ExecutionError(code=code, message=message),
    )


class SandboxExecutionEnvironment(ExecutionEnvironment):
    """HTTP client for the isolated ``keel-sandbox`` executor service."""

    def __init__(
        self,
        base_url: str,
        *,
        shared_secret: str | None = None,
        allow_unauthenticated_local_test: bool = False,
        workspace: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not base_url:
            raise ValueError("sandbox base URL is required")
        self._signer = RpcRequestSigner(
            shared_secret,
            allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        )
        self._workspace = workspace or None
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(base_url=base_url.rstrip("/"))

    def _envelope(self, **fields: Any) -> ExecutionRpcRequest:
        """Build an RPC request, stamping the per-scope workspace namespace on every call."""
        return ExecutionRpcRequest(workspace=self._workspace, **fields)

    async def _send(
        self,
        rpc_request: ExecutionRpcRequest,
        options: OperationOptions,
    ) -> ExecutionResult:
        if options.cancellation is not None and options.cancellation.cancelled:
            return _rpc_failure(ExecutionErrorCode.cancelled, "operation cancelled")
        body = rpc_request.model_dump_json().encode("utf-8")
        headers = self._signer.headers(body)
        headers["Content-Type"] = "application/json"
        request_task = asyncio.create_task(
            self._client.post(
                "/v1/execute",
                content=body,
                headers=headers,
                timeout=options.limits.timeout_seconds,
            )
        )
        cancellation_task: asyncio.Task[None] | None = None
        if options.cancellation is not None:
            cancellation_task = asyncio.create_task(options.cancellation.wait())
        try:
            waiters: set[asyncio.Task[Any]] = {request_task}
            if cancellation_task is not None:
                waiters.add(cancellation_task)
            done, _ = await asyncio.wait(
                waiters,
                timeout=options.limits.timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if request_task not in done:
                request_task.cancel()
                await asyncio.gather(request_task, return_exceptions=True)
                if cancellation_task is not None and cancellation_task in done:
                    return _rpc_failure(ExecutionErrorCode.cancelled, "operation cancelled")
                return _rpc_failure(
                    ExecutionErrorCode.timed_out,
                    f"operation timed out after {options.limits.timeout_seconds}s",
                )
            response = await request_task
            if response.status_code != 200:
                return _rpc_failure(
                    ExecutionErrorCode.unavailable,
                    f"sandbox RPC unavailable ({response.status_code})",
                )
            try:
                parsed = ExecutionRpcResponse.model_validate(response.json())
            except (ValueError, TypeError):
                return _rpc_failure(
                    ExecutionErrorCode.unavailable,
                    "sandbox RPC returned an invalid response",
                )
            result = parsed.to_result()
            bounded = bound_output(
                result.output,
                max_lines=options.limits.max_lines,
                max_bytes=options.limits.max_bytes,
            )
            return ExecutionResult(
                ok=result.ok,
                output=bounded.text,
                error=result.error,
                exit_code=result.exit_code,
                truncated=result.truncated or bounded.truncated,
                spill_path=result.spill_path,
            )
        except httpx.TimeoutException:
            return _rpc_failure(
                ExecutionErrorCode.timed_out,
                f"operation timed out after {options.limits.timeout_seconds}s",
            )
        except httpx.HTTPError:
            return _rpc_failure(ExecutionErrorCode.unavailable, "sandbox RPC unavailable")
        except asyncio.CancelledError:
            request_task.cancel()
            await asyncio.gather(request_task, return_exceptions=True)
            raise
        finally:
            if cancellation_task is not None:
                cancellation_task.cancel()

    async def execute(self, request: CommandRequest) -> ExecutionResult:
        return await self._send(
            self._envelope(
                operation=ExecutionOperation.command,
                command=request.command,
                requested_egress_hosts=sorted(request.requested_egress_hosts),
                limits=_rpc_limits(request.options),
            ),
            request.options,
        )

    async def read(self, request: ReadRequest) -> ExecutionResult:
        return await self._send(
            self._envelope(
                operation=ExecutionOperation.read,
                path=request.path,
                limits=_rpc_limits(request.options),
            ),
            request.options,
        )

    async def write(self, request: WriteRequest) -> ExecutionResult:
        return await self._send(
            self._envelope(
                operation=ExecutionOperation.write,
                path=request.path,
                content=request.content,
                limits=_rpc_limits(request.options),
            ),
            request.options,
        )

    async def edit(self, request: EditRequest) -> ExecutionResult:
        return await self._send(
            self._envelope(
                operation=ExecutionOperation.edit,
                path=request.path,
                old=request.old,
                new=request.new,
                limits=_rpc_limits(request.options),
            ),
            request.options,
        )

    async def list(self, request: ListRequest) -> ExecutionResult:
        return await self._send(
            self._envelope(
                operation=ExecutionOperation.list,
                path=request.path,
                limits=_rpc_limits(request.options),
            ),
            request.options,
        )

    async def glob(self, request: GlobRequest) -> ExecutionResult:
        return await self._send(
            self._envelope(
                operation=ExecutionOperation.glob,
                pattern=request.pattern,
                limits=_rpc_limits(request.options),
            ),
            request.options,
        )

    async def grep(self, request: GrepRequest) -> ExecutionResult:
        return await self._send(
            self._envelope(
                operation=ExecutionOperation.grep,
                pattern=request.pattern,
                glob=request.glob,
                limits=_rpc_limits(request.options),
            ),
            request.options,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
