"""asyncio.Redis.from_url URL parsing."""

from __future__ import annotations

import pytest
from redis_rs_py.asyncio import Redis
from redis_rs_py.exceptions import ConnectionError as RedisConnectionError


@pytest.mark.asyncio
async def test_from_url_basic(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    assert await r.ping() is True
    await r.aclose()


@pytest.mark.asyncio
async def test_from_url_with_db_in_query(valkey_url: str) -> None:
    base = valkey_url.split("?", 1)[0]
    r = Redis.from_url(f"{base}?db=2")
    assert await r.ping() is True
    await r.aclose()


@pytest.mark.asyncio
async def test_from_url_with_userinfo() -> None:
    with pytest.raises(RedisConnectionError):
        Redis.from_url("redis://default:secret@127.0.0.1:1/0")


@pytest.mark.asyncio
async def test_from_url_invalid_scheme_raises_value_error() -> None:
    with pytest.raises(ValueError, match="scheme"):
        Redis.from_url("http://127.0.0.1:6379/0")


@pytest.mark.asyncio
async def test_from_url_kwargs_lower_precedence(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url, host="impossible.invalid", port=1)
    assert await r.ping() is True
    await r.aclose()
