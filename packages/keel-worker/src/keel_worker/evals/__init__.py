"""Keel memory quality-eval harness (offline-replayable + live-runnable).

Placed under ``keel_worker`` so the consolidation executor can import the real
production chain (``keel_worker.main.consolidate_memory``) without inverting the
``keel-core`` dependency. See docs/superpowers/specs/2026-07-12-memory-evals-design.md.
"""

from __future__ import annotations
