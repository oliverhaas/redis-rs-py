"""Async lifecycle: aclose + __aenter__/__aexit__."""

from __future__ import annotations

import pytest
from redis_rs_py.asyncio import Redis


@pytest.mark.asyncio
async def test_aclose_drops_driver(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    assert await r.ping() is True
    await r.aclose()
    with pytest.raises(ValueError, match="closed"):
        await r.ping()


@pytest.mark.asyncio
async def test_async_context_manager(valkey_url: str) -> None:
    async with Redis.from_url(valkey_url) as r:
        assert await r.ping() is True
    with pytest.raises(ValueError, match="closed"):
        await r.ping()


@pytest.mark.asyncio
async def test_double_aclose_is_idempotent(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    await r.aclose()
    await r.aclose()
