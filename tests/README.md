# Tests

- **unit/** — pure `keel-core` and package smoke tests (no external services).
- **integration/** — services + Postgres/Redis via testcontainers (M1).
- **e2e/** — compose task-suite (M1).
- **eval/** — Langfuse dataset eval harness (M1+).

Run: `uv run pytest`
