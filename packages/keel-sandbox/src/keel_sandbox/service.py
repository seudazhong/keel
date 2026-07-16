"""HTTP executor boundary intended to run inside an externally isolated sandbox."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from keel_core.tools.environment import (
    CommandRequest,
    EditRequest,
    ExecutionEnvironment,
    ExecutionError,
    ExecutionErrorCode,
    ExecutionResult,
    GlobRequest,
    GrepRequest,
    ListRequest,
    ReadRequest,
    WriteRequest,
)
from keel_core.tools.rpc import (
    ExecutionOperation,
    ExecutionRpcRequest,
    ExecutionRpcResponse,
    request_options,
)
from keel_sandbox.policy import EgressPolicy, PathPolicy

_DENIED_COMMAND_PATH = re.compile(r"(^|[/\\\s'\"=])(?:\.\.|\.git|\.env)(?=$|[/\\\s'\"=])")


def _failure(code: ExecutionErrorCode, message: str) -> ExecutionRpcResponse:
    return ExecutionRpcResponse.from_result(
        ExecutionResult(
            ok=False,
            output=message,
            error=ExecutionError(code=code, message=message),
        )
    )


class ExecutorAdmissionPolicy:
    """Default-deny admission before any executor method is invoked."""

    def __init__(
        self,
        workspace: str = "/workspace",
        *,
        egress: EgressPolicy | None = None,
    ) -> None:
        self.paths = PathPolicy(workspace)
        self.egress = egress or EgressPolicy()

    def admit(self, request: ExecutionRpcRequest) -> ExecutionRpcResponse | None:
        if any(not self.egress.is_allowed(host) for host in request.requested_egress_hosts):
            return _failure(ExecutionErrorCode.denied, "network egress denied")
        path = request.path
        if path is not None and not self.paths.is_allowed(path):
            return _failure(ExecutionErrorCode.denied, "path denied or outside workspace")
        pattern = request.pattern
        if pattern is not None and not self.paths.is_allowed_pattern(pattern):
            return _failure(ExecutionErrorCode.denied, "pattern denied")
        glob = request.glob
        if glob is not None and not self.paths.is_allowed_pattern(glob):
            return _failure(ExecutionErrorCode.denied, "pattern denied")
        if request.operation is ExecutionOperation.command and not request.command:
            return _failure(ExecutionErrorCode.invalid, "empty command")
        if request.command is not None and _DENIED_COMMAND_PATH.search(request.command):
            return _failure(ExecutionErrorCode.denied, "command references a denied path")
        return None


async def _dispatch(
    environment: ExecutionEnvironment,
    request: ExecutionRpcRequest,
) -> ExecutionResult:
    options = request_options(request)
    if request.operation is ExecutionOperation.command:
        return await environment.execute(
            CommandRequest(
                request.command or "",
                options,
                frozenset(request.requested_egress_hosts),
            )
        )
    if request.operation is ExecutionOperation.read:
        return await environment.read(ReadRequest(request.path or "", options))
    if request.operation is ExecutionOperation.write:
        return await environment.write(
            WriteRequest(request.path or "", request.content or "", options)
        )
    if request.operation is ExecutionOperation.edit:
        return await environment.edit(
            EditRequest(
                request.path or "",
                request.old or "",
                request.new or "",
                options,
            )
        )
    if request.operation is ExecutionOperation.list:
        return await environment.list(ListRequest(request.path or ".", options))
    if request.operation is ExecutionOperation.glob:
        return await environment.glob(GlobRequest(request.pattern or "", options))
    return await environment.grep(
        GrepRequest(request.pattern or "", request.glob or "**/*", options)
    )


def create_app(
    environment: ExecutionEnvironment,
    *,
    isolation_verified: bool = False,
    admission: ExecutorAdmissionPolicy | None = None,
) -> FastAPI:
    """Create the service; unverified runtime isolation fails every request closed."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await environment.aclose()

    app = FastAPI(title="keel-sandbox", version="1", lifespan=lifespan)
    policy = admission or ExecutorAdmissionPolicy()

    @app.post("/v1/execute", response_model=ExecutionRpcResponse)
    async def execute(request: ExecutionRpcRequest) -> ExecutionRpcResponse | JSONResponse:
        if not isolation_verified:
            body = _failure(
                ExecutionErrorCode.unavailable,
                "sandbox isolation is not verified",
            )
            return JSONResponse(body.model_dump(mode="json"), status_code=503)
        denial = policy.admit(request)
        if denial is not None:
            return denial
        try:
            return ExecutionRpcResponse.from_result(await _dispatch(environment, request))
        except Exception:
            body = _failure(ExecutionErrorCode.failed, "sandbox executor failure")
            return JSONResponse(body.model_dump(mode="json"), status_code=500)

    return app
