"""Asyncio façade — `redis_rs_py.asyncio.Redis`.

Mirrors `redis.asyncio` from upstream redis-py: same constructor
surface as the sync façade (`redis_rs_py.Redis`), every method returns
an awaitable.
"""

# Force the parent _driver to load — this is what registers the
# `redis_rs_py._driver.asyncio` submodule in `sys.modules`.
import redis_rs_py._driver  # noqa: F401
from redis_rs_py._driver import AsyncPipeline as Pipeline
from redis_rs_py._driver.asyncio import Redis  # ty: ignore[unresolved-import]
from redis_rs_py.asyncio._scan_iter import scan_iter_async as _scan_iter_async

# Attach the async scan_iter helper to the async Redis class.
# ``scan_iter`` is the idiomatic redis-py name for async iteration;
# ``scan_iter_async`` is the legacy alias used by test code via the
# _DriverCompat shim (``driver.scan_iter_async(...)``).
Redis.scan_iter = _scan_iter_async  # type: ignore[attr-defined]
Redis.scan_iter_async = _scan_iter_async  # type: ignore[attr-defined]

__all__ = ["Pipeline", "Redis"]
