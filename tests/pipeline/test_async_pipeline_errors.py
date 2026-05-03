"""Error-path tests for AsyncPipeline."""

from __future__ import annotations

import pytest
from redis_rs_py.exceptions import RedisError


@pytest.mark.asyncio
async def test_aexecute_after_close_raises(aclient) -> None:
    pipe = aclient.pipeline(transaction=False)
    await pipe.aclose()
    with pytest.raises(RedisError):
        await pipe.aexecute()


@pytest.mark.asyncio
async def test_awatch_after_multi_raises(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.multi()
        with pytest.raises(RedisError):
            await pipe.awatch("k")


@pytest.mark.asyncio
async def test_multi_twice_raises(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.multi()
        with pytest.raises(RedisError):
            pipe.multi()
