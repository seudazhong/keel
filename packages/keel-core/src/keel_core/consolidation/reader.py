"""Bounded, scope-global batch reader for consolidation input (spec §6).

Reads the next window of durable ``message.token`` events after a cursor across ALL of
a scope's sessions (excluding the digest's and consolidation's own sessions), in id
order. Each message is truncated to ``message_max_chars`` and the batch stops before it
would exceed ``input_max_chars`` (always yielding at least one message). ``user_event_ids``
and ``max_event_id`` feed the user-evidence rule and the cursor advance. Scope-bound + RLS.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")

_SELECT_BATCH = text(
    "SELECT id, session_id, payload->>'role' AS role, payload->>'text' AS text "
    "FROM events "
    "WHERE scope_id = :scope "
    "AND type = 'message.token' "
    "AND id > :after "
    "AND payload->>'role' IN ('user', 'assistant') "
    "AND COALESCE((payload->>'partial')::boolean, false) = false "
    "AND length(COALESCE(payload->>'text', '')) > 0 "
    "AND session_id NOT LIKE 'digest:%' "
    "AND session_id NOT LIKE 'consolidation:%' "
    "ORDER BY id ASC "
    "LIMIT :limit"
)


@dataclass(frozen=True)
class ConsolidationMessage:
    """One conversation message admitted into a consolidation batch."""

    event_id: int
    session_id: str
    role: Literal["user", "assistant"]
    content: str


@dataclass(frozen=True)
class ConsolidationBatch:
    """A bounded window of messages plus the metadata the run needs."""

    messages: tuple[ConsolidationMessage, ...]
    user_event_ids: frozenset[int]
    max_event_id: int
    eligible_count: int


class ConsolidationBatchReader:
    """Scope-bound reader of the next consolidation window after a cursor."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def read(
        self,
        after_event_id: int,
        *,
        limit: int = 50,
        message_max_chars: int = 4000,
        input_max_chars: int = 20000,
    ) -> ConsolidationBatch:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    _SELECT_BATCH,
                    {"scope": self._scope_id, "after": after_event_id, "limit": limit},
                )
            ).all()

        messages: list[ConsolidationMessage] = []
        user_ids: set[int] = set()
        total = 0
        max_event_id = after_event_id
        for row in rows:
            content = str(row.text)[:message_max_chars]
            if messages and total + len(content) > input_max_chars:
                break
            role = cast(Literal["user", "assistant"], str(row.role))
            event_id = int(row.id)
            messages.append(
                ConsolidationMessage(
                    event_id=event_id,
                    session_id=str(row.session_id),
                    role=role,
                    content=content,
                )
            )
            if role == "user":
                user_ids.add(event_id)
            total += len(content)
            max_event_id = event_id
        return ConsolidationBatch(
            messages=tuple(messages),
            user_event_ids=frozenset(user_ids),
            max_event_id=max_event_id,
            eligible_count=len(rows),
        )
