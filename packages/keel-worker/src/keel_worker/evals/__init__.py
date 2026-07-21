"""Keel memory quality-eval harness (offline-replayable + live-runnable).

Placed under ``keel_worker`` so the consolidation executor can import the real
production chain (``keel_worker.main.consolidate_memory``) without inverting the
``keel-core`` dependency. See docs/MEMORY.md.
"""

from __future__ import annotations
