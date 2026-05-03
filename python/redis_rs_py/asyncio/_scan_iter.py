"""Async generator wrapper around RedisRsDriver.ascan(cursor=).

Same rationale as the sync version: an async-generator function on a
PyO3 pyclass isn't expressible directly. Attached to RedisRsDriver as
`scan_iter_async` via __init__.py monkey-patch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis_rs_py._driver import RedisRsDriver


async def scan_iter_async(
    self: RedisRsDriver,
    *,
    match: str | None = None,
    count: int | None = None,
    type: str | None = None,
) -> AsyncIterator[bytes]:
    """Asynchronously yield every key, paginated via SCAN."""
    cursor = 0
    while True:
        cursor, keys = await self.ascan(cursor=cursor, match=match, count=count, type=type)
        for k in keys:
            yield k
        if cursor == 0:
            return
