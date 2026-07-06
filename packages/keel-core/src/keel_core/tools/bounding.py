"""Output bounding + spill (FR-T5).

Cap model-facing output at a line/byte budget; when exceeded, optionally spill the
full text to a retained file and return a truncated view plus the path.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

MAX_LINES = 2000
MAX_BYTES = 50_000


@dataclass
class BoundedOutput:
    """A bounded view of a larger output."""

    text: str
    truncated: bool
    spill_path: str | None = None


def bound_output(
    text: str,
    *,
    max_lines: int = MAX_LINES,
    max_bytes: int = MAX_BYTES,
    spill_dir: Path | None = None,
) -> BoundedOutput:
    """Return ``text`` unchanged if within budget, else a truncated view (+ spill)."""
    encoded = text.encode("utf-8")
    lines = text.splitlines()
    if len(lines) <= max_lines and len(encoded) <= max_bytes:
        return BoundedOutput(text=text, truncated=False)

    spill_path: str | None = None
    if spill_dir is not None:
        spill_dir.mkdir(parents=True, exist_ok=True)
        target = spill_dir / f"output-{uuid4().hex}.txt"
        target.write_text(text, encoding="utf-8")
        spill_path = str(target)

    clipped = "\n".join(lines[:max_lines]).encode("utf-8")[:max_bytes]
    view = clipped.decode("utf-8", errors="ignore")
    note = f"\n… output truncated ({len(lines)} lines, {len(encoded)} bytes)"
    if spill_path is not None:
        note += f"; full output at {spill_path}"
    return BoundedOutput(text=view + note, truncated=True, spill_path=spill_path)
