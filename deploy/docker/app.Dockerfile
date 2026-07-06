# syntax=docker/dockerfile:1
# Single image shared by keel-server / keel-worker / keel-migrate.
# Behaviour is selected by the compose `command:` per service.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /app

# Manifests + sources first (workspace members are installed editable).
COPY pyproject.toml uv.lock ./
COPY packages/ ./packages/
RUN uv sync --frozen

# Migrations + alembic config for the keel-migrate service.
COPY alembic.ini ./
COPY migrations/ ./migrations/

# Put the workspace venv on PATH so console scripts resolve
# (keel-server, arq, alembic, keel).
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000
CMD ["keel-server"]
