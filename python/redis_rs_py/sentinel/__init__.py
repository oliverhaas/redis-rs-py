"""Sentinel client — drop-in replacement for redis.sentinel.Sentinel."""

import sys

import redis_rs_py._driver  # noqa: F401
from redis_rs_py._driver.sentinel import Sentinel  # ty: ignore[unresolved-import]

# Register the submodule in sys.modules so dotted imports work.
sys.modules.setdefault("redis_rs_py.sentinel", sys.modules[__name__])

__all__ = ["Sentinel"]
