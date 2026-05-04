"""Async pipeline: buffered commands, aexecute() returns list."""

import pytest


@pytest.mark.asyncio
async def test_async_pipeline_set_get_returns_list(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.set("a", b"1")
        pipe.set("b", b"2")
        pipe.get("a")
        pipe.get("b")
        result = await pipe.aexecute()
    assert result == [True, True, b"1", b"2"]


@pytest.mark.asyncio
async def test_async_pipeline_chained_returns_pipe(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        ret = pipe.set("k", b"v")
        assert ret is pipe
        ret2 = pipe.get("k")
        assert ret2 is pipe


@pytest.mark.asyncio
async def test_async_pipeline_empty_execute_returns_empty_list(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        result = await pipe.aexecute()
    assert result == []


@pytest.mark.asyncio
async def test_async_pipeline_len(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        assert len(pipe) == 0
        pipe.set("x", b"1")
        assert len(pipe) == 1
        pipe.get("x")
        assert len(pipe) == 2
        await pipe.aexecute()
        assert len(pipe) == 0


@pytest.mark.asyncio
async def test_async_pipeline_incr(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.incr("counter")
        pipe.incr("counter")
        pipe.incr("counter")
        result = await pipe.aexecute()
    assert result == [1, 2, 3]


@pytest.mark.asyncio
async def test_async_pipeline_hash_commands(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.hset("h", "f1", b"v1")
        pipe.hget("h", "f1")
        pipe.hlen("h")
        result = await pipe.aexecute()
    assert result[0] == 1
    assert result[1] == b"v1"
    assert result[2] == 1


@pytest.mark.asyncio
async def test_async_pipeline_execute_clears_buffer(aclient) -> None:
    async with aclient.pipeline(transaction=False) as pipe:
        pipe.set("x", b"1")
        await pipe.aexecute()
        assert len(pipe) == 0
        pipe.set("y", b"2")
        result = await pipe.aexecute()
    assert result == [True]
