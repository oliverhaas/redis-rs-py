"""WATCH / WatchError tests for the async AsyncPipeline."""

import asyncio

import pytest
from redis_rs_py.exceptions import WatchError


@pytest.mark.asyncio
async def test_awatch_no_conflict_succeeds(aclient) -> None:
    """awatch() with no concurrent modification: aexecute() succeeds."""
    await aclient.set("watched_key", b"initial")
    async with aclient.pipeline(transaction=True) as pipe:
        await pipe.awatch("watched_key")
        pipe.multi()
        pipe.set("watched_key", b"new_value")
        result = await pipe.aexecute()
    assert result == [True]
    assert await aclient.get("watched_key") == b"new_value"


@pytest.mark.asyncio
async def test_awatch_with_conflict_raises_watch_error(aclient) -> None:
    """awatch() followed by concurrent SET raises WatchError on aexecute()."""
    await aclient.set("racekey", b"0")

    async with aclient.pipeline(transaction=True) as pipe:
        await pipe.awatch("racekey")
        # Simulate a concurrent write.
        await aclient.set("racekey", b"dirty")
        pipe.multi()
        pipe.set("racekey", b"mine")
        with pytest.raises(WatchError):
            await pipe.aexecute()


@pytest.mark.asyncio
async def test_awatch_immediate_mode_get(aclient) -> None:
    """After awatch(), immediate-mode commands use the reserved connection."""
    await aclient.set("imm", b"hello")
    async with aclient.pipeline(transaction=True) as pipe:
        await pipe.awatch("imm")
        val = await pipe.aget_immediate("imm")
        assert val == b"hello"
        pipe.multi()
        pipe.set("imm", b"world")
        result = await pipe.aexecute()
    assert result == [True]
    assert await aclient.get("imm") == b"world"


@pytest.mark.asyncio
async def test_aunwatch_resets_state(aclient) -> None:
    """aunwatch() clears watched keys; subsequent aexecute() is clean."""
    await aclient.set("uk", b"0")
    async with aclient.pipeline(transaction=True) as pipe:
        await pipe.awatch("uk")
        await pipe.aunwatch()
        pipe.set("uk", b"updated")
        result = await pipe.aexecute()
    assert result == [True]


@pytest.mark.asyncio
async def test_awatch_concurrent_task_triggers_watch_error(aclient) -> None:
    """A concurrent asyncio task modifying the watched key raises WatchError."""
    await aclient.set("conc", b"0")

    watch_done = asyncio.Event()
    dirty_done = asyncio.Event()

    async def watcher():
        async with aclient.pipeline(transaction=True) as pipe:
            await pipe.awatch("conc")
            watch_done.set()
            await dirty_done.wait()
            pipe.multi()
            pipe.incr("conc")
            with pytest.raises(WatchError):
                await pipe.aexecute()

    async def modifier():
        await watch_done.wait()
        await aclient.set("conc", b"99")
        dirty_done.set()

    await asyncio.gather(watcher(), modifier())
