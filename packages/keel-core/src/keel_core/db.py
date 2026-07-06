"""Shared datastore factories (Postgres via SQLAlchemy async, Redis).

Thin helpers so services share one connection convention. No schema or
behaviour here — projections and the event store land in M1 (WS-D).
"""

from __future__ import annotations

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .config import Settings


def make_async_engine(settings: Settings) -> AsyncEngine:
    """Create the application's async SQLAlchemy engine."""
    return create_async_engine(settings.database_url, pool_pre_ping=True, future=True)


def make_redis(settings: Settings) -> redis.Redis:
    """Create an async Redis client."""
    client: redis.Redis = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    return client
