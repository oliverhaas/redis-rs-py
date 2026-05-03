"""ar.atransaction() retry helper tests."""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_atransaction_basic(aclient) -> None:
    """atransaction() runs the function and returns aexecute() result."""
    await aclient.set("tc", b"0")

    async def func(pipe) -> None:
        pipe.multi()
        pipe.incr("tc")
        pipe.incr("tc")

    result = await aclient.atransaction(func, "tc")
    assert result == [1, 2]


@pytest.mark.asyncio
async def test_atransaction_value_from_callable(aclient) -> None:
    """value_from_callable=True returns the callable's return value."""
    await aclient.set("vfc", b"hello")

    async def func(pipe):
        val = await pipe.aget_immediate("vfc")
        pipe.multi()
        pipe.set("vfc", b"world")
        return val

    result = await aclient.atransaction(func, "vfc", value_from_callable=True)
    assert result == b"hello"
    assert await aclient.get("vfc") == b"world"


@pytest.mark.asyncio
async def test_atransaction_retries_on_watch_error(aclient) -> None:
    """atransaction() retries the function when a WatchError is raised."""
    await aclient.set("retry_key", b"0")
    attempts: list[int] = [0]

    async def func(pipe) -> None:
        attempts[0] += 1
        if attempts[0] == 1:
            # Dirty the key between WATCH and MULTI to trigger WatchError.
            await aclient.set("retry_key", b"0")
        pipe.multi()
        pipe.set("retry_key", b"done")  # use SET to avoid integer constraints

    await aclient.atransaction(func, "retry_key")
    assert attempts[0] == 2
    assert await aclient.get("retry_key") == b"done"


@pytest.mark.asyncio
async def test_atransaction_propagates_non_watch_error(aclient) -> None:
    """Non-WatchError exceptions bubble up from atransaction()."""

    async def func(pipe) -> None:
        pipe.multi()
        raise ValueError("deliberate")

    with pytest.raises(ValueError, match="deliberate"):
        await aclient.atransaction(func)


@pytest.mark.asyncio
async def test_atransaction_no_watches(aclient) -> None:
    """atransaction() without watch keys works as an unwatched MULTI/EXEC."""
    await aclient.set("nw", b"0")

    async def func(pipe) -> None:
        pipe.multi()
        pipe.incr("nw")

    result = await aclient.atransaction(func)
    assert result == [1]


@pytest.mark.asyncio
async def test_atransaction_concurrent_tasks_retry(aclient) -> None:
    """Two concurrent coroutines: one retries but both eventually succeed."""
    await aclient.set("race", b"0")

    async def worker():
        async def func(pipe) -> None:
            pipe.multi()
            pipe.incr("race")

        await aclient.atransaction(func, "race")

    await asyncio.gather(worker(), worker())

    final = int(await aclient.get("race"))
    assert final == 2
