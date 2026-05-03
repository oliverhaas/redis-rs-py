"""adiscard() / reset() / aclose() tests for AsyncPipeline."""

from __future__ import annotations

import pytest
from redis_rs_py.exceptions import RedisError


@pytest.mark.asyncio
async def test_adiscard_clears_buffer(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.set("k", b"v")
        assert len(pipe) == 1
        await pipe.adiscard()
        assert len(pipe) == 0
        result = await pipe.aexecute()
    assert result == []


@pytest.mark.asyncio
async def test_context_manager_exit_does_not_execute(aclient) -> None:
    """__aexit__ resets state; key should NOT be set."""
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.set("x", b"1")
    assert await aclient.get("x") is None


@pytest.mark.asyncio
async def test_aclose_prevents_further_use(aclient) -> None:
    pipe = aclient.pipeline(transaction=False)
    await pipe.aclose()
    with pytest.raises(RedisError):
        await pipe.aexecute()


@pytest.mark.asyncio
async def test_aclose_before_execute_does_not_write(aclient) -> None:
    pipe = aclient.pipeline(transaction=False)
    pipe.set("k", b"v")
    await pipe.aclose()
    assert await aclient.get("k") is None


@pytest.mark.asyncio
async def test_reset_after_awatch_releases_reserved(aclient) -> None:
    """reset() after awatch() unregisters the WATCH."""
    await aclient.set("r", b"0")
    async with aclient.pipeline(transaction=True) as pipe:
        await pipe.awatch("r")
        await pipe.reset()
    await aclient.set("r", b"1")
    assert await aclient.get("r") == b"1"
