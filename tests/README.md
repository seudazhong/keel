# Tests

- **unit/** — pure `keel-core` and package smoke tests (no external services).
- **integration/** — tests against live Postgres/Redis services.
- **e2e/** — compose task-suite (M1).
- **eval/** — Langfuse dataset eval harness (M1+).

Postgres integration tests are destructive and require an explicit isolated database
named `keel_test`; the fixtures refuse missing URLs and the live `keel` database.

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
uv run pytest
```
