"""redis-rs-py — high-performance, drop-in replacement for redis-py.

The public surface is added by plan 10 (sync facade) and plan 11
(asyncio facade). For now, only the low-level driver is exposed.
"""

from redis_rs_py._driver import RedisRsAwaitable, RedisRsDriver, __version__

__all__ = ["RedisRsAwaitable", "RedisRsDriver", "__version__"]
