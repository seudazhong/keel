# Keel tests and evaluations

The root `pyproject.toml` configures pytest with `tests/` as its test path.

## Layout

- `unit/` and top-level focused files: pure runtime/package behavior and contract tests.
- `integration/`: live Postgres and/or Redis/arq behavior, including RLS and recovery.
- `e2e/`: cross-component flows.
- `eval/`: deterministic Memory and Knowledge datasets, cassettes, scoring, and reports.

## Fast local checks

```powershell
uv sync --frozen
uv run pytest -m "not integration"
```

Target a file or expression while iterating:

```powershell
uv run pytest tests\unit\test_example.py
uv run pytest -k "knowledge and not integration"
```

## Integration safety

Integration/eval fixtures are destructive. They require an explicit isolated database named
`keel_test` or `keel_eval` and reject the normal `keel` database.

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_EVAL_DATABASE_URL = $env:KEEL_TEST_DATABASE_URL
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/0"
uv run pytest -m integration
```

The committed CI uses pgvector/Postgres 16 and Redis 7. Do not point these variables at a
development or production-like database.

## Evals

Use the committed runners for Memory and Knowledge:

```powershell
uv run python scripts\run_memory_evals.py --help
uv run python scripts\run_knowledge_evals.py --help
```

Replay mode must fail closed rather than call a live provider/embedding service. Live/record
mode can incur cost and mutate evaluation data; inspect `--help` and use only isolated
credentials/databases.

See [Development](../docs/DEVELOPMENT.md) for CI commands and
[Status](../docs/STATUS.md) for current capability maturity.
