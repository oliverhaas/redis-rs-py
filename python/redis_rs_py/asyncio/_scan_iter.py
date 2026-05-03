"""Async generator wrapper around AsyncRedis.scan(cursor=).

Same rationale as the sync version: an async-generator function on a
PyO3 pyclass isn't expressible directly. Attached to Redis (asyncio)
via monkey-patch in redis_rs_py.asyncio.__init__.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis_rs_py._driver.asyncio import Redis  # ty: ignore[unresolved-import]


async def scan_iter_async(
    self: Redis,
    *,
    match: str | None = None,
    count: int | None = None,
    type: str | None = None,
) -> AsyncIterator[bytes]:
    """Asynchronously yield every key, paginated via SCAN."""
    cursor = 0
    while True:
        cursor, keys = await self.scan(cursor=cursor, match=match, count=count, type=type)
        for k in keys:
            yield k
        if cursor == 0:
            return
