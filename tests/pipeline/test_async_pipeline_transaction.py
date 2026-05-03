"""Async pipeline MULTI/EXEC atomic block tests."""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_async_pipeline_transaction_true_is_atomic(aclient) -> None:
    async with aclient.pipeline(transaction=True) as pipe:
        pipe.set("x", b"10")
        pipe.incr("x")
        pipe.get("x")
        result = await pipe.aexecute()
    assert result == [True, 11, b"11"]


@pytest.mark.asyncio
async def test_async_pipeline_transaction_false_not_wrapped(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.set("a", b"1")
        pipe.set("b", b"2")
        result = await pipe.aexecute()
    assert result == [True, True]


@pytest.mark.asyncio
async def test_async_pipeline_multi_then_execute(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.multi()
        pipe.set("k", b"hello")
        pipe.get("k")
        result = await pipe.aexecute()
    assert result == [True, b"hello"]


@pytest.mark.asyncio
async def test_async_pipeline_nested_multi_raises(aclient) -> None:
    from redis_rs_py.exceptions import RedisError

    async with aclient.pipeline(transaction=False) as pipe:
        pipe.multi()
        with pytest.raises(RedisError):
            pipe.multi()


@pytest.mark.asyncio
async def test_async_pipeline_transaction_returns_list_of_replies(aclient) -> None:
    async with aclient.pipeline(transaction=True) as pipe:
        for i in range(5):
            pipe.set(f"k{i}", str(i).encode())
        result = await pipe.aexecute()
    assert len(result) == 5
    assert all(r is True for r in result)
