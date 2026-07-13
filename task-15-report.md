# Task 15 Report — CLI + entry script

## Summary

Implemented the argparse CLI (`cli.py`) and thin repo-root wrapper (`run_memory_evals.py`) for the memory eval suite, with full unit test coverage.

## Files created

| File | Purpose |
|------|---------|
| `packages/keel-worker/src/keel_worker/evals/cli.py` | `build_parser`, `resolve_enforce`, `resolve_suites`, `validate_record`, `validate_paths`, `run_cli`, `main` |
| `scripts/run_memory_evals.py` | Thin repo-root entry that delegates to `cli.main()` |
| `tests/unit/test_eval_cli.py` | 7 unit tests covering all public functions |

## TDD cycle

- **RED**: `test_eval_cli.py` imported before `cli.py` existed → `ModuleNotFoundError` (exit 2).
- **GREEN**: All 8 tests pass after implementation.

## Quality

- `ruff check`: All checks passed.
- `mypy`: No issues found in 1 source file.

## Behaviour summary

| Flag | Default (replay) | Default (live) |
|------|-----------------|---------------|
| `--enforce` | `True` (CI gates on) | `False` |
| `--record` | ❌ (requires `--mode live`) | optional |
| `--judge` | off | off |
| `--langfuse` | off | off |

Exit codes mirror `EvalRunReport.exit_code`: `0` pass, `1` gate failure (enforced), `2` infra/validation error.

## Commit

```
feat(evals): CLI and repo-root entry script
```
