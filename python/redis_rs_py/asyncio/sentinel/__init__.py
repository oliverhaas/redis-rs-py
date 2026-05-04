"""Async sentinel client — drop-in replacement for redis.asyncio.sentinel.Sentinel."""

from __future__ import annotations

import sys

from redis_rs_py._driver.asyncio.sentinel import AsyncSentinel  # type: ignore[attr-defined]

# Redis-py compatibility alias.
Sentinel = AsyncSentinel

# Register the submodule in sys.modules so dotted imports work.
sys.modules.setdefault("redis_rs_py.asyncio.sentinel", sys.modules[__name__])

__all__ = ["AsyncSentinel", "Sentinel"]
