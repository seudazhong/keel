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

Run `npm run dev` for the actual React UI. Compose `keel-web` is only a static stub.

## Local services

```powershell
docker compose --profile dev up -d --build
Invoke-RestMethod http://localhost:8000/readiness
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
from pathlib import Path
import re
from urllib.parse import unquote

bad = []
for path in Path(".").rglob("*.md"):
    if ".git" in path.parts:
        continue
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
