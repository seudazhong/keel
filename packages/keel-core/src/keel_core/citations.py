"""Provider-neutral citation contracts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Citation(BaseModel):
    """Structured provenance attached to a tool result."""

    id: str
    label: str
    source: str
    metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = ["Citation"]
