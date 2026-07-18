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

A read-only Playwright smoke covers the documented 10-15 minute demo (see
[Demo guide](./DEMO.md#6-automated-browser-smoke-23-minutes)). It targets the Compose React
surface on `http://127.0.0.1:3000` by default and requires an already-running stack — it does
not start/stop Compose or delete volumes:

```powershell
docker compose -f docker-compose.yml --profile dev up -d
Set-Location web
npm run test:e2e:install   # first run only: installs the Chromium browser
npm run test:e2e
```

Override the target with `$env:SMOKE_BASE_URL` (e.g. a Vite dev server on `:5173`). The suite
fails fast with an actionable error if the target is unreachable or is a stale pre-M3.1 build
(missing current-main routes like `/v1/jobs`) rather than silently skipping current routes.

## Local services

The Compose `dev` profile runs a **real, authenticated sandbox execution boundary**: a
one-shot `keel-secret-init` generates a random ≥32-byte RPC secret into a dedicated volume
(never in source/YAML/logs), and server/worker use the fail-closed `sandbox` backend to send
every file/shell tool call over an internal-only RPC network to the hardened `keel-sandbox`
executor (non-root, read-only rootfs, dropped caps, no egress, no credentials). Shell stays
disabled (file tools work, isolated per scope); it is a trusted single-org dev deployment, not
a microVM/multi-tenant boundary. Pass `-f docker-compose.yml` so a local override cannot change
that contract.

```powershell
docker compose -f docker-compose.yml --profile dev up -d --build
Invoke-RestMethod http://localhost:8000/readiness   # checks.sandbox == "ok" (probed RPC)
docker compose logs --tail 100 keel-server keel-worker
```

On Windows, host-run server/CLI Postgres durability uses psycopg's selector loop; shell
subprocess behavior is better exercised in Linux containers.

## Documentation changes

- Current truth belongs in `STATUS.md`; future sequencing belongs only in `ROADMAP.md`.
- PRD and Architecture may describe targets, but must link to current fidelity.
- Dated `designs/` and `plans/` are historical snapshots.
- Check links and whitespace before committing:

```powershell
@'
import re
import subprocess
from pathlib import Path
from urllib.parse import unquote

bad = []
md_files = subprocess.run(
    ["git", "ls-files", "*.md"], capture_output=True, text=True, check=True
).stdout.splitlines()
for rel in md_files:
    path = Path(rel)
    text = path.read_text(encoding="utf-8")
    for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", text):
        target = target.split("#", 1)[0].strip()
        if not target or "://" in target or target.startswith("mailto:"):
            continue
        resolved = (path.parent / unquote(target)).resolve()
        if not resolved.exists():
            bad.append(f"{path}: {target}")
if bad:
    raise SystemExit("\n".join(bad))
print("relative Markdown links: OK")
'@ | python -
git diff --check
```

Using `git ls-files` (rather than an unfiltered filesystem walk) keeps the check scoped to
tracked documentation and avoids false failures from `node_modules/`, `.venv/`, `.worktrees/`,
and similar untracked/vendored directories that also contain Markdown files.
