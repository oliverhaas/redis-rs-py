"""Async sentinel client — drop-in replacement for redis.asyncio.sentinel.Sentinel."""

import sys

import redis_rs_py._driver  # noqa: F401
from redis_rs_py._driver.asyncio.sentinel import AsyncSentinel  # ty: ignore[unresolved-import]

# Redis-py compatibility alias.
Sentinel = AsyncSentinel

# Register the submodule in sys.modules so dotted imports work.
sys.modules.setdefault("redis_rs_py.asyncio.sentinel", sys.modules[__name__])

__all__ = ["AsyncSentinel", "Sentinel"]
