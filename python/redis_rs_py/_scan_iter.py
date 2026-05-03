"""Python-side generator wrapper around RedisRsDriver.scan(cursor=).

This is the one place where shipping Python code is unavoidable: PyO3
pyclasses can implement __iter__/__next__ but not be a true Python
generator (and crucially can't be an async-generator on the asyncio
side). Documented as the explicit Rust-by-default escape hatch in
PLAN.md lines 60-63.

Both helpers are attached to RedisRsDriver via __init__.py monkey-patch
at import time so users can call `driver.scan_iter(...)` directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from redis_rs_py._driver import RedisRsDriver


def scan_iter(
    self: RedisRsDriver,
    *,
    match: str | None = None,
    count: int | None = None,
    type: str | None = None,
) -> Iterator[bytes]:
    """Yield every key in the database, paginated via SCAN under the hood.

    Honors the same `match`/`count`/`type` filters as `scan(cursor=, ...)`.
    Resumes from the cursor returned by the previous SCAN call until the
    server returns cursor 0.
    """
    cursor = 0
    while True:
        cursor, keys = self.scan(cursor=cursor, match=match, count=count, type=type)
        yield from keys
        if cursor == 0:
            return
