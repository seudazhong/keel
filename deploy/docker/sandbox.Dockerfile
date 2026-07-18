# syntax=docker/dockerfile:1
# Dedicated image for the keel-sandbox executor RPC boundary.
#
# This is the ONLY container that runs model-chosen file/shell tool operations. It is built
# and run distinctly from the app image so it can be hardened independently and carries none
# of the control-plane's dependencies or credentials:
#   * only the `keel-sandbox` workspace member (+ its transitive deps) is installed via
#     `uv sync --no-dev --package keel-sandbox` — never keel-server/keel-worker/keel-cli/
#     keel-sdk/keel-scheduler or their extra deps (arq, etc.), and never the dev group
#     (pytest/ruff/mypy); it holds no alembic/migrations either. After install, every
#     non-closure package source is pruned so /app/packages contains only keel-core +
#     keel-sandbox;
#   * it runs as a non-root user (uid/gid 10100, matching deploy/k8s/base/sandbox), which
#     owns ONLY the sandbox workspace + per-scope namespaces roots;
#   * everything else is meant to run on a read-only root filesystem with tmpfs scratch and
#     all Linux capabilities dropped (enforced by docker-compose.yml / the K8s Job template),
#     so this image intentionally writes nothing outside /sandbox at runtime.
# The image itself contains NO secret: the RPC shared secret is mounted read-only at runtime
# and read from KEEL_SANDBOX_RPC_SECRET_FILE.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /app

# Manifests + sources first so the uv workspace resolves against the frozen lock. All members
# are COPYied so resolution succeeds, but only the sandbox member's runtime closure is installed
# and every non-closure source is then pruned (see below).
COPY pyproject.toml uv.lock ./
COPY packages/ ./packages/
# --no-dev drops the workspace dev group (pytest/ruff/mypy); --package keel-sandbox installs just
# keel-sandbox -> keel-core (+ fastapi/uvicorn), never keel-server/keel-worker/keel-cli/keel-sdk/
# keel-scheduler or their extra deps (arq, etc.). Then prune every non-closure package source so
# /app/packages holds ONLY keel-core + keel-sandbox — the two editable members that must keep
# their sources at runtime. Contract: tests/unit/test_sandbox_dockerfile.py.
RUN uv sync --frozen --no-dev --package keel-sandbox \
    && for pkg in packages/*/; do \
         case "$pkg" in \
           packages/keel-core/ | packages/keel-sandbox/) ;; \
           *) rm -rf "$pkg" ;; \
         esac; \
       done

# Put the workspace venv on PATH so the `keel-sandbox` console script resolves.
ENV PATH="/app/.venv/bin:$PATH"

# Non-root runtime user that owns only the sandbox's own writable trees. The default
# workspace and the per-scope namespaces root live under /sandbox; nothing else is writable
# once the container runs with a read-only root filesystem + tmpfs (see docker-compose.yml).
RUN groupadd --gid 10100 keelsandbox \
    && useradd --uid 10100 --gid 10100 --home-dir /sandbox --shell /usr/sbin/nologin keelsandbox \
    && mkdir -p /sandbox/workspace /sandbox/namespaces \
    && chown -R 10100:10100 /sandbox

ENV KEEL_SANDBOX_HOST=0.0.0.0 \
    KEEL_SANDBOX_PORT=8090 \
    KEEL_SANDBOX_WORKSPACE=/sandbox/workspace \
    KEEL_SANDBOX_NAMESPACES_ROOT=/sandbox/namespaces

USER 10100:10100
EXPOSE 8090

# Unauthenticated liveness only (never leaks the secret / egress policy / workspace state);
# authenticated readiness is proven separately by keel-server/keel-worker via /v1/ping.
HEALTHCHECK --interval=10s --timeout=5s --retries=12 --start-period=10s \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8090/health', timeout=4).status==200 else 1)"]

CMD ["keel-sandbox"]
