"""Scheduler entry point (stub).

M0 is a no-op that logs and exits. The leader-elected, at-most-once cron
(ADR-0006, spike S2) is implemented in M2.
"""

from __future__ import annotations

import asyncio
import logging

from keel_core import __version__

logger = logging.getLogger("keel.scheduler")


async def run() -> None:
    """Stub run: log and return (no behaviour in M0)."""
    logger.info("keel-scheduler %s — stub; leader election + cron land in M2", __version__)


def main() -> None:
    """Run the scheduler stub."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
