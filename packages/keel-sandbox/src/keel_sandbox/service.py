"""HTTP executor boundary intended to run inside an externally isolated sandbox."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Protocol

from fastapi import FastAPI, Request
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
from keel_core.tools.rpc_auth import (
    DEFAULT_REPLAY_WINDOW_SECONDS,
    RpcRequestVerifier,
)
from keel_sandbox.policy import EgressPolicy, PathPolicy

# Defense in depth only. The executor's sanitized-workspace validation and the
# container mount contract are the security boundary for shell path confinement.
_DENIED_COMMAND_PATH = re.compile(r"(^|[/\\\s'\"=])(?:\.\.|\.git|\.env)(?=$|[/\\\s'\"=])")

# An opaque per-scope workspace namespace produced by ``keel_core.scoping.workspace_namespace``.
_WORKSPACE_NAMESPACE = re.compile(r"^ws_[0-9a-f]{1,64}$")


def _failure(code: ExecutionErrorCode, message: str) -> ExecutionRpcResponse:
    return ExecutionRpcResponse.from_result(
        ExecutionResult(
            ok=False,
            output=message,
            error=ExecutionError(code=code, message=message),
        )
    )


class WorkspaceProvider(Protocol):
    """Resolve a validated ``ws_<hash>`` namespace to its own confined environment.

    The executor must **never** ignore the namespace on the RPC: two scopes that operate on
    the same relative path must resolve to two distinct, isolated workspaces (M3.6, item 6).
    ``resolve(None)`` returns the explicit default/unscoped environment; ``resolve(namespace)``
    returns a distinct per-namespace environment, or ``None`` when a scoped workspace cannot be
    provisioned (the request then fails closed rather than sharing a workspace).
    """

    def resolve(self, namespace: str | None) -> ExecutionEnvironment | None: ...

    async def aclose(self) -> None: ...


class DirectoryWorkspaceProvider:
    """A :class:`WorkspaceProvider` that roots each namespace at ``<base>/<namespace>``.

    Every namespace maps one-to-one to a distinct child directory of ``base_root`` and is
    served by an environment the factory builds *confined to that directory*, so a path in one
    namespace can never resolve into another's tree (the environment's workspace path policy
    resolves symlinks/junctions and rejects anything outside its own root). Because the
    namespace is an opaque, validated ``ws_<hex>`` token it can never contain a separator or
    ``..``; we still re-check containment defensively. Environments are cached per namespace so
    repeated operations reuse one confined root, and the directory is created on first use
    (explicit provisioning). A namespace that cannot be provisioned resolves to ``None``.
    """

    def __init__(
        self,
        base_root: Path | str,
        factory: Callable[[Path], ExecutionEnvironment],
        *,
        default_environment: ExecutionEnvironment | None = None,
    ) -> None:
        self._base = Path(base_root).resolve()
        self._factory = factory
        self._default = default_environment
        self._cache: dict[str, ExecutionEnvironment] = {}

    def resolve(self, namespace: str | None) -> ExecutionEnvironment | None:
        if namespace is None:
            return self._default
        if not _WORKSPACE_NAMESPACE.match(namespace):
            return None
        root = (self._base / namespace).resolve()
        if root != self._base and not root.is_relative_to(self._base):
            # Defense in depth: an opaque validated namespace can never escape, but never
            # serve a root outside the provider's base even if that invariant regresses.
            return None
        environment = self._cache.get(namespace)
        if environment is None:
            try:
                root.mkdir(parents=True, exist_ok=True)
            except OSError:
                return None
            environment = self._factory(root)
            self._cache[namespace] = environment
        return environment

    async def aclose(self) -> None:
        for environment in self._cache.values():
            await environment.aclose()
        self._cache.clear()
        if self._default is not None:
            await self._default.aclose()


class ExecutorAdmissionPolicy:
    """Default-deny admission before any executor method is invoked."""

    def __init__(
        self,
        workspace: str = "/workspace",
        *,
        egress: EgressPolicy | None = None,
        scoped_workspaces_supported: bool = False,
    ) -> None:
        self.paths = PathPolicy(workspace)
        self.egress = egress or EgressPolicy()
        # Whether this executor can provision an isolated per-scope workspace. When False, a
        # request that asks for a scoped ``workspace`` namespace is denied (fail closed) rather
        # than silently served from the single shared workspace (M3.6, item 3).
        self.scoped_workspaces_supported = scoped_workspaces_supported

    def admit(self, request: ExecutionRpcRequest) -> ExecutionRpcResponse | None:
        namespace = request.workspace
        if namespace is not None:
            if not _WORKSPACE_NAMESPACE.match(namespace):
                return _failure(ExecutionErrorCode.denied, "invalid workspace namespace")
            if not self.scoped_workspaces_supported:
                # A scoped workspace was requested but this executor cannot provision one:
                # fail closed rather than share a single writable workspace across scopes.
                return _failure(
                    ExecutionErrorCode.unavailable,
                    "scoped workspace is not provisioned by this sandbox",
                )
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
    shared_secret: str | None = None,
    allow_unauthenticated_local_test: bool = False,
    replay_window_seconds: int = DEFAULT_REPLAY_WINDOW_SECONDS,
    workspace_provider: WorkspaceProvider | None = None,
) -> FastAPI:
    """Create the authenticated service.

    Production requires both a strong shared secret and externally verified container
    isolation. Tests may explicitly opt into unauthenticated local mode.

    When a ``workspace_provider`` is supplied the executor resolves each request's validated
    ``ws_<hash>`` namespace to its own confined environment (never the shared ``environment``),
    and a scoped namespace that cannot be provisioned fails closed. Without a provider the
    executor serves the single ``environment`` and denies any scoped namespace at admission
    unless the admission policy explicitly declares scoped workspaces supported.
    """

    verifier = RpcRequestVerifier(
        shared_secret,
        allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        replay_window_seconds=replay_window_seconds,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        if workspace_provider is not None:
            await workspace_provider.aclose()
        else:
            await environment.aclose()

    app = FastAPI(title="keel-sandbox", version="1", lifespan=lifespan)
    # A provider can provision a distinct workspace per namespace, so scoped namespaces are
    # admitted (then resolved per-namespace below). Without a provider the default policy keeps
    # its fail-closed posture toward scoped namespaces.
    policy = admission or ExecutorAdmissionPolicy(
        scoped_workspaces_supported=workspace_provider is not None
    )

    def _resolve_environment(request: ExecutionRpcRequest) -> ExecutionEnvironment | None:
        if workspace_provider is None:
            # Legacy single-workspace mode: the admission policy has already rejected any
            # scoped namespace it cannot serve, so the shared environment is correct here.
            return environment
        return workspace_provider.resolve(request.workspace)

    @app.post("/v1/execute", response_model=ExecutionRpcResponse)
    async def execute(raw_request: Request) -> ExecutionRpcResponse | JSONResponse:
        body_bytes = await raw_request.body()
        if not verifier.verify(
            raw_request.headers,
            body_bytes,
            method=raw_request.method,
            path=raw_request.url.path,
        ):
            return JSONResponse({"detail": "authentication failed"}, status_code=401)
        if not isolation_verified:
            body = _failure(
                ExecutionErrorCode.unavailable,
                "sandbox isolation is not verified",
            )
            return JSONResponse(body.model_dump(mode="json"), status_code=503)
        try:
            request = ExecutionRpcRequest.model_validate_json(body_bytes)
        except ValueError:
            return JSONResponse({"detail": "invalid request"}, status_code=400)
        denial = policy.admit(request)
        if denial is not None:
            return denial
        scoped_environment = _resolve_environment(request)
        if scoped_environment is None:
            # The namespace was admitted but a distinct confined workspace could not be
            # provisioned: fail closed rather than fall back to a shared/default workspace.
            return _failure(
                ExecutionErrorCode.unavailable,
                "scoped workspace could not be provisioned",
            )
        try:
            return ExecutionRpcResponse.from_result(await _dispatch(scoped_environment, request))
        except Exception:
            body = _failure(ExecutionErrorCode.failed, "sandbox executor failure")
            return JSONResponse(body.model_dump(mode="json"), status_code=500)

    return app
