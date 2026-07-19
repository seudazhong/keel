"""HTTP executor boundary intended to run inside an externally isolated sandbox."""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Protocol

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from keel_core.patch.errors import (
    PatchBoundsExceeded,
    PatchPolicyViolation,
    PatchValidationError,
)
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
    RPC_NONCE_HEADER,
    RpcRequestVerifier,
    RpcResponseSigner,
)
from keel_sandbox.policy import EgressPolicy, PathPolicy
from keel_sandbox.transfer import (
    SandboxTransferService,
    TransferIOError,
    TransferNamespaceError,
)

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


async def _read_bounded_body(request: Request, max_bytes: int) -> bytes | None:
    """Read a request body, rejecting anything over ``max_bytes`` before buffering it whole.

    A declared ``Content-Length`` over the ceiling is refused up front, and a chunked/streamed
    body is capped as it arrives (``max_bytes + 1`` stop), so an oversized or unbounded upload
    is rejected without allocating the full payload. Returns ``None`` when the body is too large
    or the declared length is not a valid non-negative integer.
    """

    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            length = int(declared)
        except ValueError:
            return None
        if length < 0 or length > max_bytes:
            return None
    buffer = bytearray()
    async for chunk in request.stream():
        buffer.extend(chunk)
        if len(buffer) > max_bytes:
            return None
    return bytes(buffer)


class WorkspaceProvider(Protocol):
    """Resolve a validated ``ws_<hash>`` namespace to its own confined environment.

    The executor must **never** ignore the namespace on the RPC: two scopes that operate on
    the same relative path must resolve to two distinct, isolated workspaces (M3.6, item 6).
    ``resolve(None)`` returns the explicit default/unscoped environment; ``resolve(namespace)``
    returns a distinct per-namespace environment, or ``None`` when a scoped workspace cannot be
    provisioned (the request then fails closed rather than sharing a workspace).
    """

    def resolve(self, namespace: str | None) -> ExecutionEnvironment | None: ...

    def namespaced_shell_isolated(self) -> bool:
        """Whether ``command``/shell execution against a *namespaced* workspace is confined by a
        real OS/container/microVM boundary that exposes **only** that namespace root.

        Path policy confines *file* operations, but it cannot confine a shell subprocess: a
        command can reference absolute paths, expand globs, or derive a sibling namespace path
        and read/write outside its namespace root. A namespaced workspace that is merely a child
        directory under a shared parent is therefore **not** a shell sandbox. This capability
        must default to ``False`` and may be asserted ``True`` only by a backend that proves an
        OS-level mount boundary; when it is ``False`` the service denies every namespaced
        ``command`` before it can reach the environment.
        """
        ...

    async def aclose(self) -> None: ...


class DirectoryWorkspaceProvider:
    """A :class:`WorkspaceProvider` that roots each namespace at ``<base>/<namespace>``.

    Every namespace maps one-to-one to a distinct child directory of ``base_root`` and is
    served by an environment the factory builds *confined to that directory*, so a path in one
    namespace can never resolve into another's tree (the environment's workspace path policy
    resolves symlinks/junctions and rejects anything outside its own root). Because the
    namespace is an opaque, validated ``ws_<hex>`` token it can never contain a separator or
    ``..``.

    The namespace **root itself** is the isolation boundary, so it is provisioned and validated
    with no-follow / exclusive semantics (M3.6 finding 5): the child is created with an exclusive
    ``mkdir`` (which never follows a final-component symlink and fails closed if the name already
    exists), and every ``resolve`` — cache hit or miss — re-validates the root via a *no-follow*
    ``lstat`` and a real-path **identity** check. This rejects a namespace root that is a symlink,
    a Windows junction / reparse point, or an alias to a different directory **even when its
    target stays inside the base** (a same-base alias would otherwise silently share another
    namespace's tree). Re-validating on every request (rather than trusting a cached mapping)
    defeats a swap between validation and use.

    Directory confinement bounds *file* operations only. A namespace root that is just a child
    directory under a shared parent is **not** a shell sandbox: a ``command`` subprocess can
    reference an absolute path, expand a glob, or derive a sibling ``ws_<hex>`` path and reach
    outside its own root. ``shell_isolated`` (default ``False``) therefore gates namespaced
    ``command`` execution: it must be asserted ``True`` only by a deployment that wraps each
    namespace root in a real OS/container/microVM mount boundary exposing only that root.
    While it is ``False`` the service denies every namespaced ``command`` before it reaches an
    environment; file operations remain available (confined by path policy).
    """

    def __init__(
        self,
        base_root: Path | str,
        factory: Callable[[Path], ExecutionEnvironment],
        *,
        default_environment: ExecutionEnvironment | None = None,
        shell_isolated: bool = False,
    ) -> None:
        self._base = Path(base_root).resolve()
        self._base_real = Path(os.path.realpath(self._base))
        self._factory = factory
        self._default = default_environment
        self._shell_isolated = shell_isolated
        self._cache: dict[str, ExecutionEnvironment] = {}

    def namespaced_shell_isolated(self) -> bool:
        return self._shell_isolated

    @property
    def namespace_base(self) -> Path:
        """The validated real base directory that confines every namespace root.

        Exposed as a public accessor (never a private ``_base`` reflection) so the transfer
        service can create same-filesystem staging/backup siblings under the trusted base.
        """
        return self._base_real

    def _is_own_directory(self, child: Path) -> bool:
        """Whether ``child`` is a real, non-aliased directory owned by this base (no-follow).

        Rejects a missing path, a symlink, a Windows junction / reparse point, a non-directory,
        or any path whose real target is not exactly ``<base>/<name>`` — i.e. an alias to a
        different directory, even one that still lives inside the base.
        """
        try:
            info = child.lstat()  # no-follow: describe the link itself, never its target
        except OSError:
            return False
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            return False
        # Windows junctions / reparse points are not S_ISLNK; reject them explicitly.
        reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        if os.name == "nt" and bool(getattr(info, "st_file_attributes", 0) & reparse):
            return False
        # Identity + containment: the resolved target must be exactly base_real/<name> — no
        # alias to another name, and never outside the base tree.
        real = Path(os.path.realpath(child))
        expected = self._base_real / child.name
        if real != expected:
            return False
        return real == self._base_real or real.is_relative_to(self._base_real)

    def resolve(self, namespace: str | None) -> ExecutionEnvironment | None:
        if namespace is None:
            return self._default
        child = self._provision_namespace(namespace)
        if child is None:
            return None
        environment = self._cache.get(namespace)
        if environment is None:
            environment = self._factory(child)
            self._cache[namespace] = environment
        return environment

    def _provision_namespace(self, namespace: str) -> Path | None:
        """Exclusively provision + no-follow validate a namespace root, returning its path.

        Shared by :meth:`resolve` and :meth:`namespace_directory` so the file executor and the
        transfer service provision and re-validate a namespace root with identical semantics.
        """
        if not _WORKSPACE_NAMESPACE.match(namespace):
            return None
        # The direct, un-followed child path (the namespace is a flat ``ws_<hex>`` token, so it
        # can never contain a separator or ``..``).
        child = self._base / namespace
        # Ensure the (trusted) base tree exists, then provision the namespace root exclusively
        # when absent: ``mkdir`` fails closed if the name already exists and never follows a
        # planted final-component symlink.
        try:
            self._base.mkdir(parents=True, exist_ok=True)
        except OSError:
            return None
        try:
            os.mkdir(child)
        except FileExistsError:
            pass  # already present — validate below (it could be a planted alias/reparse point)
        except OSError:
            return None
        # Re-validate on EVERY call (cache hit or miss) so a swap between validation and use
        # is caught: the root must be our own real directory, not a symlink/junction/alias.
        if not self._is_own_directory(child):
            return None
        return child

    def namespace_directory(self, namespace: str) -> Path | None:
        """Provision + no-follow validate a namespace root and return its confined path.

        Returns ``None`` when the namespace is malformed or its root cannot be provisioned as
        our own real directory (fail closed). Used by the transfer service to extract into /
        export from the same confined root the file executor serves.
        """
        return self._provision_namespace(namespace)

    def is_own_namespace_directory(self, namespace: str) -> bool:
        """Whether an **existing** namespace root is our own real directory (no provisioning).

        Unlike :meth:`namespace_directory` this never creates the directory, so the transfer
        service can distinguish an absent namespace (idempotent delete / unknown export) from a
        planted symlink/junction/alias that must fail closed.
        """
        if not _WORKSPACE_NAMESPACE.match(namespace):
            return False
        return self._is_own_directory(self._base / namespace)

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
    transfer_service: SandboxTransferService | None = None,
) -> FastAPI:
    """Create the authenticated service.

    Production requires both a strong shared secret and externally verified container
    isolation. Tests may explicitly opt into unauthenticated local mode.

    When a ``workspace_provider`` is supplied the executor resolves each request's validated
    ``ws_<hash>`` namespace to its own confined environment (never the shared ``environment``),
    and a scoped namespace that cannot be provisioned fails closed. Without a provider the
    executor serves the single ``environment`` and denies any scoped namespace at admission
    unless the admission policy explicitly declares scoped workspaces supported.

    When a ``transfer_service`` is supplied the ``/v1/transfer/{upload,export,delete}/{namespace}``
    routes are registered: each verifies the request HMAC (over the bounded body) before any
    gzip decompression, and returns a keyed response signature bound to the caller's request.
    """

    verifier = RpcRequestVerifier(
        shared_secret,
        allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        replay_window_seconds=replay_window_seconds,
    )
    response_signer = RpcResponseSigner(
        shared_secret,
        allow_unauthenticated_local_test=allow_unauthenticated_local_test,
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

    @app.get("/health")
    async def health() -> JSONResponse:
        """Unauthenticated liveness: the process is up and serving.

        Deliberately reveals nothing about the shared secret, the admitted egress policy, the
        workspace contents, or whether isolation was asserted — a container healthcheck must be
        callable without credentials, so it must never leak security-relevant state. Auth-
        contract and isolation readiness are proven separately by the authenticated ``/v1/ping``
        below (callers that hold the shared secret), never by this endpoint.
        """
        return JSONResponse({"status": "ok"})

    @app.post("/v1/ping")
    async def ping(raw_request: Request) -> JSONResponse:
        """Authenticated readiness probe for control-plane wiring.

        Proves, without executing any tool or exposing workspace state, that (1) the caller and
        the executor share the RPC secret and the HMAC request-binding contract holds (else
        401), and (2) the operator asserted an isolation boundary so real operations will be
        served rather than fail closed (else 503). ``keel-server``/``keel-worker`` call this at
        readiness/startup so they surface an unreachable or misauthenticated sandbox instead of
        silently degrading. The empty body still participates in the signature (its digest is
        bound), so a replayed or unsigned probe is rejected exactly like a real operation.
        """
        body_bytes = await raw_request.body()
        if not verifier.verify(
            raw_request.headers,
            body_bytes,
            method=raw_request.method,
            path=raw_request.url.path,
        ):
            return JSONResponse(
                {"ready": False, "detail": "authentication failed"}, status_code=401
            )
        if not isolation_verified:
            return JSONResponse(
                {"ready": False, "detail": "sandbox isolation is not verified"},
                status_code=503,
            )
        return JSONResponse({"ready": True})

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
        if (
            request.operation is ExecutionOperation.command
            and request.workspace is not None
            and workspace_provider is not None
            and not workspace_provider.namespaced_shell_isolated()
        ):
            # A namespaced workspace is confined for *file* operations by path policy, but a
            # shell subprocess is not: it can reference an absolute path, expand a glob, or
            # derive a sibling namespace path and escape its root. Unless the provider proves a
            # real OS/container/microVM mount boundary exposing only this namespace root, deny
            # the command here — before it can reach the environment. (The unscoped/default
            # workspace path, ``request.workspace is None``, is unaffected.)
            return _failure(
                ExecutionErrorCode.denied,
                "shell/command execution is disabled for a namespaced workspace without a "
                "proven OS isolation boundary",
            )
        try:
            return ExecutionRpcResponse.from_result(await _dispatch(scoped_environment, request))
        except Exception:
            body = _failure(ExecutionErrorCode.failed, "sandbox executor failure")
            return JSONResponse(body.model_dump(mode="json"), status_code=500)

    if transfer_service is not None:
        _register_transfer_routes(
            app,
            transfer_service=transfer_service,
            verifier=verifier,
            response_signer=response_signer,
            isolation_verified=isolation_verified,
        )

    return app


def _sign_response(
    response_signer: RpcResponseSigner,
    request: Request,
    *,
    request_body: bytes,
    response_body: bytes,
) -> dict[str, str]:
    return response_signer.headers(
        response_body,
        request_nonce=request.headers.get(RPC_NONCE_HEADER, ""),
        request_body=request_body,
        method=request.method,
        path=request.url.path,
    )


def _register_transfer_routes(
    app: FastAPI,
    *,
    transfer_service: SandboxTransferService,
    verifier: RpcRequestVerifier,
    response_signer: RpcResponseSigner,
    isolation_verified: bool,
) -> None:
    """Register the authenticated snapshot upload/export/delete routes.

    Every route reads a bounded body (rejecting oversized/unbounded uploads before buffering),
    verifies the request HMAC over that exact body *before* any gzip decompression, requires
    verified isolation, maps typed transfer failures to fail-closed status codes, and signs the
    response body with a key bound to the caller's request nonce.
    """

    max_archive = transfer_service.bounds.max_archive_bytes
    # Export/delete carry only a tiny (possibly empty) request body; keep it strictly bounded.
    max_control_body = 4096

    def _authenticate(request: Request, body: bytes) -> JSONResponse | None:
        if not verifier.verify(request.headers, body, method=request.method, path=request.url.path):
            return JSONResponse({"detail": "authentication failed"}, status_code=401)
        if not isolation_verified:
            return JSONResponse({"detail": "sandbox isolation is not verified"}, status_code=503)
        return None

    @app.post("/v1/transfer/upload/{namespace}")
    async def transfer_upload(namespace: str, raw_request: Request) -> Response:
        body = await _read_bounded_body(raw_request, max_archive)
        if body is None:
            return JSONResponse({"detail": "request body too large"}, status_code=413)
        denied = _authenticate(raw_request, body)
        if denied is not None:
            return denied
        try:
            result = await transfer_service.upload(namespace, body)
        except TransferNamespaceError:
            return JSONResponse({"detail": "invalid namespace"}, status_code=400)
        except PatchBoundsExceeded:
            return JSONResponse({"detail": "snapshot exceeds bounds"}, status_code=413)
        except (PatchPolicyViolation, PatchValidationError):
            return JSONResponse({"detail": "snapshot rejected"}, status_code=422)
        except TransferIOError:
            return JSONResponse({"detail": "sandbox transfer failure"}, status_code=500)
        payload = _json_bytes(
            {
                "namespace": result.namespace,
                "files": result.manifest.file_count,
                "total_bytes": result.manifest.total_bytes,
            }
        )
        headers = _sign_response(
            response_signer, raw_request, request_body=body, response_body=payload
        )
        return Response(content=payload, media_type="application/json", headers=headers)

    @app.post("/v1/transfer/export/{namespace}")
    async def transfer_export(namespace: str, raw_request: Request) -> Response:
        body = await _read_bounded_body(raw_request, max_control_body)
        if body is None:
            return JSONResponse({"detail": "request body too large"}, status_code=413)
        denied = _authenticate(raw_request, body)
        if denied is not None:
            return denied
        try:
            result = await transfer_service.export(namespace)
        except TransferNamespaceError:
            return JSONResponse({"detail": "namespace not found"}, status_code=404)
        except PatchBoundsExceeded:
            return JSONResponse({"detail": "snapshot exceeds bounds"}, status_code=413)
        except (PatchPolicyViolation, PatchValidationError):
            return JSONResponse({"detail": "snapshot rejected"}, status_code=422)
        except TransferIOError:
            return JSONResponse({"detail": "sandbox transfer failure"}, status_code=500)
        headers = _sign_response(
            response_signer, raw_request, request_body=body, response_body=result.archive
        )
        return Response(content=result.archive, media_type="application/gzip", headers=headers)

    @app.post("/v1/transfer/delete/{namespace}")
    async def transfer_delete(namespace: str, raw_request: Request) -> Response:
        body = await _read_bounded_body(raw_request, max_control_body)
        if body is None:
            return JSONResponse({"detail": "request body too large"}, status_code=413)
        denied = _authenticate(raw_request, body)
        if denied is not None:
            return denied
        try:
            result = await transfer_service.delete(namespace)
        except TransferNamespaceError:
            return JSONResponse({"detail": "invalid namespace"}, status_code=400)
        except TransferIOError:
            return JSONResponse({"detail": "sandbox transfer failure"}, status_code=500)
        payload = _json_bytes({"namespace": result.namespace, "deleted": result.deleted})
        headers = _sign_response(
            response_signer, raw_request, request_body=body, response_body=payload
        )
        return Response(content=payload, media_type="application/json", headers=headers)


def _json_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
