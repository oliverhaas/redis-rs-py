"""Sentinel client — drop-in replacement for redis.sentinel.Sentinel."""

from __future__ import annotations

import sys

from redis_rs_py._driver.sentinel import Sentinel  # type: ignore[attr-defined]

# Register the submodule in sys.modules so dotted imports work.
sys.modules.setdefault("redis_rs_py.sentinel", sys.modules[__name__])

__all__ = ["Sentinel"]
