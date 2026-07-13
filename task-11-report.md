# Task 11 Report — Optional LLM judge (advisory, fail-open, never a gate)

## Outcome: ✅ COMPLETE

**Commit:** `b6ff9e9` — `feat(evals): optional fail-open LLM judge (advisory, never a gate)`
**Range:** `b0d7bb5..b6ff9e9`

**Files touched**
- `packages/keel-worker/src/keel_worker/evals/providers.py` (+42 / −0)
  - Added `from keel_worker.evals.models import JudgeResult as JudgeResult` (explicit re-export so `from ...providers import JudgeResult` works under `mypy --strict`).
  - Added `parse_judge_response(text: str) -> JudgeResult` — grabs the first `{…}` span, parses via `json.loads`, returns a fail-open `JudgeResult` on any error.
  - Added `class LiteLLMMemoryJudge` — `__init__(model, *, gateway=None)` lazy-constructs `LiteLLMGateway` only when no stub is injected; `async judge(prompt) -> JudgeResult` streams the provider, concatenates `chunk.delta`, parses via `parse_judge_response`; any exception is swallowed and returned as `JudgeResult(error=…)` (fail-open, `passed=True`).
- `tests/unit/test_eval_judge.py` (new, 58 lines, 4 tests).

---

## Requirements checklist

| Requirement (task-11 brief + plan §Task 11) | Status | Where |
|---|---|---|
| `JudgeResult` already defined in `models.py` (Task 1) | ✅ | `models.py:197–204` — no change needed |
| `JudgeResult` re-exported from `providers.py` so `from ...providers import JudgeResult` resolves | ✅ | `providers.py:27` — `import JudgeResult as JudgeResult` (explicit re-export) |
| `parse_judge_response(text: str) -> JudgeResult` | ✅ | `providers.py:260–273` |
| Fail-open on garbage input (`no json object in judge output`) | ✅ | `providers.py:263–264` — `start == -1 or end == -1 or end < start` branch |
| Fail-open on bad JSON (`judge parse error`) | ✅ | `providers.py:272–273` — `except (ValueError, TypeError)` |
| `class LiteLLMMemoryJudge.__init__(model, *, gateway=None)` | ✅ | `providers.py:276–283` — lazy `LiteLLMGateway` import if `gateway is None` |
| `async judge(prompt) -> JudgeResult` streams provider, concatenates deltas, parses | ✅ | `providers.py:285–294` |
| Any provider exception is fail-open (`passed=True`, `error` set) | ✅ | `providers.py:293–294` — `except Exception as exc: # noqa: BLE001` |
| Never a gate — advisory only | ✅ | No gate path; `JudgeResult.passed` is ignored by scorers and hard gates |
| TDD RED → GREEN | ✅ | RED: `ImportError: cannot import name 'JudgeResult' from 'keel_worker.evals.providers'`; GREEN: **4 passed** |
| `ruff check` clean (whole `evals/` + new test) | ✅ | `All checks passed!` |
| `mypy --strict` clean (2 files + whole `evals/`) | ✅ | `Success: no issues found in 2 source files` / `9 source files` |
| Regression: full unit suite | ✅ | **384 passed** (4 new + 380 pre-existing) |
| Commit trailers (`Co-authored-by: Copilot`, `Copilot-Session: e6e934ad-…`) | ✅ | `git log -1 --format="%(trailers:only=true)"` — byte-identical to tasks 4–10 |

---

## Verification against the real codebase (deltas from the plan skeleton)

The plan gives a complete skeleton; two small quality-driven adjustments were made:

1. **Explicit re-export `import JudgeResult as JudgeResult` instead of bare `import JudgeResult`.**
   The plan shows `from keel_worker.evals.models import JudgeResult` (bare import). Under
   `mypy --strict`, a bare import is not an explicit re-export so `from ...providers import
   JudgeResult` in the test raises `attr-defined`. Using the `as Name` alias pattern (PEP 484
   / mypy convention) signals an explicit public re-export without altering the runtime object
   or name binding. The test `import JudgeResult` still resolves to the same class.

2. **`# noqa: F401` on the test's `JudgeResult` import line.**
   The test imports `JudgeResult` to verify the re-export path works, but never references
   the name by itself (all assertions operate on the `.score`, `.passed`, `.error` attributes
   of a `JudgeResult` instance returned by `parse_judge_response` / `judge`). Ruff correctly
   flags this as `F401 imported but unused`; a `# noqa: F401` comment suppresses it while
   keeping the re-export coverage intent visible. Ruff auto-sorted the import block (`I001`)
   during the `--fix` pass, which is why the `keel_worker.evals.providers` import line now
   appears after the stdlib/core imports.

Neither change alters the public contract, the runtime behaviour, the fail-open semantics, or
the `JudgeResult` field population.

---

## Test coverage (4 unit tests)

- **`test_parse_extracts_json_block`** — `parse_judge_response` finds the first `{…}` span in
  a string that has prefix noise and suffix noise; asserts `score == 0.9`, `passed is True`,
  `error is None`. Proves the substring-extraction logic and the happy-path JSON parse.

- **`test_parse_is_fail_open_on_garbage`** — passes `"not json at all"` (no `{` or `}`);
  asserts `passed is True` and `error is not None`. Proves the fail-open contract on
  completely malformed input.

- **`test_judge_reads_stream`** — uses `_Gateway` (an in-process stub that yields one delta
  chunk + one `finish_reason` chunk). Asserts `score == 0.5` and `passed is False`. Proves
  the streaming accumulation and the parse round-trip with `passed=false` (the `False`
  boolean path).

- **`test_judge_fail_open_on_provider_error`** — uses `_BoomGateway` (an async generator that
  raises `RuntimeError("provider down")` on first `__anext__`). Asserts `passed is True` and
  `"provider down" in result.error`. Proves the `except Exception` catch-all and the
  fail-open error string embedding.

---

## Quality gates

| Check | Command | Result |
|---|---|---|
| RED | `pytest tests/unit/test_eval_judge.py -q` | `ImportError: cannot import name 'JudgeResult' from 'keel_worker.evals.providers'` |
| GREEN | same after impl | **4 passed** |
| Regression (unit suite) | `pytest tests/unit --tb=no` | **384 passed** (4 new + 380 pre-existing) |
| Lint (file-scoped) | `ruff check providers.py test_eval_judge.py` | **All checks passed** |
| Lint (whole evals pkg + test) | `ruff check evals/ tests/unit/test_eval_judge.py` | **All checks passed** |
| Types (2 files) | `mypy providers.py test_eval_judge.py --strict` | `Success: no issues found in 2 source files` |
| Types (whole evals pkg) | `mypy evals/ --strict` | `Success: no issues found in 9 source files` |
| Trailers | `git log -1 --format="%(trailers:only=true)"` | `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>` + `Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49` (byte-identical to tasks 4–10) |
