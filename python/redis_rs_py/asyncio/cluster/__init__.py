"""Async cluster client — drop-in replacement for redis.asyncio.cluster.

Re-exports the Rust pyclasses registered on `_driver.asyncio.cluster`.
"""

import redis_rs_py._driver  # noqa: F401
from redis_rs_py._driver.asyncio.cluster import AsyncRedisCluster  # ty: ignore[unresolved-import]

__all__ = ["AsyncRedisCluster"]
