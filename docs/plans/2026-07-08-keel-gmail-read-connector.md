# Gmail Read Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the digest agent's fake `inbox_list` with a real read-only Gmail fetch behind the existing `ConnectorTool`/`ActionFn` seam, gated by a feature flag so the fake stays the default/test path.

**Architecture:** A new `keel_core.gmail` module loads an OAuth refresh token from the existing `PostgresTokenStore` (scope `web:local`, connector `"gmail"`), calls the Gmail API read-only, and formats messages exactly like the fake `_inbox_text()`. The digest registry gains an optional `inbox_action` override; the worker injects the real action when `KEEL_GMAIL_ENABLED=1`. A one-shot host script runs `InstalledAppFlow.run_local_server` (browser consent once) and stores `Credentials.to_json()` via the token store. `email_send` stays mocked + approval-gated (read-only scope only).

**Tech Stack:** google-auth, google-auth-oauthlib, google-api-python-client; existing EnvelopeCipher/PostgresTokenStore; arq worker; Docker Compose.

## Global Constraints

- Python >= 3.12; ruff (`E,F,I,UP,B,ASYNC`, line-length 100); mypy strict over `packages` (`ignore_missing_imports = true`).
- LLM tool/function names match `^[a-zA-Z0-9_-]{1,64}$` — no dots (tools stay `inbox_list`/`email_send`).
- Inbound connector output is tainted (G17); `ConnectorTool(outbound=False)` already tags taint — the Gmail action returns plain text, the tool taints it.
- Cipher is deterministic from `KEEL_SECRET_KEY` (SHA256→Fernet); host authorize script and Docker worker MUST share the same `KEEL_SECRET_KEY`.
- Every commit ends with `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`.
- The fake connector stays the default: existing tests and the disabled path must be byte-for-byte unchanged in behavior.

---

## File Structure

- Create `packages/keel-core/src/keel_core/gmail.py` — Gmail scopes/const, message formatting, sync fetch, `make_gmail_inbox_action` factory, `TokenStore` Protocol.
- Modify `packages/keel-core/src/keel_core/digest.py` — `digest_registry(sent=None, *, inbox_action=None)`.
- Modify `packages/keel-core/src/keel_core/config.py` — `gmail_enabled`, `gmail_client_secrets_path`, `gmail_max_messages` settings.
- Modify `packages/keel-core/pyproject.toml` — add the 3 Google deps.
- Modify `packages/keel-worker/src/keel_worker/main.py` — `_digest_registry(ctx, settings, scope_id)` helper used by both `run_agent` and `resume_run`.
- Create `scripts/gmail_authorize.py` — one-shot OAuth + token store.
- Modify `.env` + `docker-compose.override.yml` — `KEEL_SECRET_KEY`, `KEEL_GMAIL_ENABLED`.
- Create `tests/unit/test_gmail.py` — formatting + action factory (hermetic; fetch monkeypatched).
- Modify `tests/unit/test_digest.py` (or create) — registry honors `inbox_action`.

---

### Task 1: Deps + config

**Files:**
- Modify: `packages/keel-core/pyproject.toml`
- Modify: `packages/keel-core/src/keel_core/config.py`

**Interfaces:**
- Produces: `Settings.gmail_enabled: bool`, `Settings.gmail_client_secrets_path: str`, `Settings.gmail_max_messages: int`.

- [ ] Add to `keel-core` deps: `"google-auth>=2.30"`, `"google-auth-oauthlib>=1.2"`, `"google-api-python-client>=2.130"`.
- [ ] Add settings fields (defaults: `False`, `".secrets/gmail_client.json"`, `5`).
- [ ] Run `python -m uv lock`; then `python -m uv sync --frozen`.
- [ ] Commit.

### Task 2: `keel_core.gmail`

**Files:**
- Create: `packages/keel-core/src/keel_core/gmail.py`
- Test: `tests/unit/test_gmail.py`

**Interfaces:**
- Produces:
  - `GMAIL_SCOPES: tuple[str, ...]` = `("https://www.googleapis.com/auth/gmail.readonly",)`
  - `GMAIL_CONNECTOR_ID = "gmail"`
  - `format_inbox(messages: list[dict[str, str]]) -> str` — `[i] from — subject: snippet` per line.
  - `_fetch_inbox_sync(creds_json: str, max_messages: int) -> tuple[str, str]` — returns `(formatted_text, latest_creds_json)`.
  - `make_gmail_inbox_action(store: TokenStore, max_messages: int = 5) -> ActionFn`.
  - `TokenStore` Protocol: `async get(connector_id) -> str | None`, `async put(connector_id, secret) -> None`.

- [ ] Write failing tests: `format_inbox` output shape; `make_gmail_inbox_action` (a) raises clear error when `store.get` returns None, (b) returns fetched text and persists rotated creds when `_fetch_inbox_sync` (monkeypatched) returns a new json.
- [ ] Run tests → fail.
- [ ] Implement module (lazy-import Google libs inside `_fetch_inbox_sync`; action uses `asyncio.to_thread`).
- [ ] Run tests → pass. ruff + mypy.
- [ ] Commit.

### Task 3: Wire `digest_registry`

**Files:**
- Modify: `packages/keel-core/src/keel_core/digest.py`
- Test: `tests/unit/test_digest.py`

**Interfaces:**
- Consumes: `ActionFn` from `keel_core.connectors`.
- Produces: `digest_registry(sent=None, *, inbox_action: ActionFn | None = None) -> ToolRegistry`.

- [ ] Failing test: injected `inbox_action` returning `"REAL"` makes the `inbox_list` tool output `"REAL"` and taint tainted; default (no arg) still returns `_inbox_text()`.
- [ ] Run → fail.
- [ ] Implement: use `inbox_action or inbox_list` for the inbound tool's `action`.
- [ ] Run → pass. ruff + mypy.
- [ ] Commit.

### Task 4: Worker wiring

**Files:**
- Modify: `packages/keel-worker/src/keel_worker/main.py`

**Interfaces:**
- Produces: `_digest_registry(ctx, settings, scope_id) -> ToolRegistry` used by `run_agent` + `resume_run`.

- [ ] Add helper: when `settings.gmail_enabled`, build `PostgresTokenStore(ctx["engine"], scope_id, cipher_from_settings(settings))` + `make_gmail_inbox_action(store, settings.gmail_max_messages)`; else `None`. Return `digest_registry(ctx.get("sent"), inbox_action=action)`.
- [ ] Replace the two `digest_registry(ctx.get("sent"))` calls with the helper.
- [ ] `python -m pytest -m "not integration" -q` (worker + core unaffected). ruff + mypy.
- [ ] Commit.

### Task 5: Authorize script

**Files:**
- Create: `scripts/gmail_authorize.py`

- [ ] `load_env_file()` + `get_settings()`; `InstalledAppFlow.from_client_secrets_file(settings.gmail_client_secrets_path, list(GMAIL_SCOPES)).run_local_server(port=0)`.
- [ ] Store `creds.to_json()` via `PostgresTokenStore(create_async_engine(settings.database_url), "web:local", cipher_from_settings(settings))` inside `asyncio.run(...)`; set `WindowsSelectorEventLoopPolicy` on Windows first.
- [ ] Print the stored account + connector; `python -m compileall scripts/gmail_authorize.py`. ruff.
- [ ] Commit.

### Task 6: Env/compose

**Files:**
- Modify: `.env`
- Modify: `docker-compose.override.yml`

- [ ] `.env`: add `KEEL_SECRET_KEY=<dev value>` (host script + reference).
- [ ] Override: add `KEEL_SECRET_KEY` (same) + `KEEL_GMAIL_ENABLED: "1"` to `keel-worker` (and `keel-server`) environment.
- [ ] Commit `.env` only if not gitignored (override is gitignored — no commit).

### Task 7: Live verification (Docker)

- [ ] `docker compose build keel-worker` (new deps) then `docker compose --profile dev up -d`.
- [ ] Host: `python scripts/gmail_authorize.py` → consent once in browser → token stored.
- [ ] `python scripts/seed_digest_schedule.py --scope web:local` (re-seed if needed).
- [ ] Wait for cron tick → digest reads the real inbox → drafts a reply → suspends on tainted `email_send`.
- [ ] Approve in React `/approvals` (or REST) → `resume_run ● completed`.
- [ ] Verify worker logs show a real Gmail read (subjects from the user's actual inbox).
