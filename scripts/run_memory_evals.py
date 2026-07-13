#!/usr/bin/env python
"""Repo-root entry point for the memory eval suite.

Usage (Windows):
  $env:KEEL_EVAL_DATABASE_URL = "******localhost:5432/keel_test"
  .\\.venv\\Scripts\\python.exe scripts\\run_memory_evals.py --mode replay --suite all --enforce
"""

from __future__ import annotations

from keel_worker.evals.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
