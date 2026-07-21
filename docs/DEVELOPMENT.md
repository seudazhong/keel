# Developing Keel

## Prerequisites and setup

Use Python 3.12+, [uv](https://docs.astral.sh/uv/), Node/npm for `web/`, Docker Desktop for
integration services, and PowerShell on Windows.

```powershell
uv sync --frozen
uv run keel version
```

## Backend checks

These match the committed CI workflow:

```powershell
uv run ruff check .
uv run ruff format --check .
uv run mypy packages
uv run pytest -m "not integration"
```

Integration tests are destructive and refuse the normal `keel` database:

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_EVAL_DATABASE_URL = $env:KEEL_TEST_DATABASE_URL
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/0"
uv run pytest -m integration
```

See [`tests/README.md`](../tests/README.md) for targeted suites and eval caveats.

## React checks

```powershell
Set-Location web
npm ci
npm run lint
npm run test
npm run build
```

Run `npm run dev` for the actual React UI against an already-running `:8000` API, or use the
built Compose `keel-web` bundle directly (see [Demo guide](./DEMO.md)).

### Playwright demo smoke

A read-only Playwright smoke covers the documented walkthrough (see
[Demo guide](./DEMO.md)). It targets the Compose React
surface on `http://127.0.0.1:3000` by default and requires an already-running stack — it does
not start/stop Compose or delete volumes:

```powershell
docker compose -f docker-compose.yml --profile dev up -d
Set-Location web
npm run test:e2e:install   # first run only: installs the Chromium browser
npm run test:e2e
```

Override the target with `$env:SMOKE_BASE_URL` (e.g. a Vite dev server on `:5173`). The suite
fails fast with an actionable error if the target is unreachable or is a stale build
(missing current-main routes like `/v1/jobs`) rather than silently skipping current routes.

## Local services

Start and interpret the trusted Compose profile through
[Operations](./OPERATIONS.md#1-supported-current-profile). Development commonly needs:

```powershell
docker compose -f docker-compose.yml --profile dev up -d --build
Invoke-RestMethod http://localhost:8000/readiness
docker compose logs --tail 100 keel-server keel-worker
```

On Windows, host-run server/CLI Postgres durability uses psycopg's selector loop; shell
subprocess behavior is better exercised in Linux containers.

## Documentation changes

- Current truth belongs only in `STATUS.md`; future sequencing belongs only in `ROADMAP.md`.
- Architecture must label current implementation and target contracts explicitly.
- PRD defines product requirements, not implementation status.
- A changed decision gets a new ADR; superseded implementation detail is retrieved through
  [`HISTORY.md`](./HISTORY.md), not kept as a parallel documentation tree.
- Check local paths, heading anchors, and whitespace before committing:

```powershell
uv run python scripts/check_markdown_links.py
git diff --check
```

The script checks tracked and newly created non-ignored Markdown files, while excluding
`node_modules/`, `.venv/`, `.worktrees/`, and other ignored/vendor trees.
