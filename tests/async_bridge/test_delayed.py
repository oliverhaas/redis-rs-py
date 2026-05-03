"""Delayed-resolution awaitable — forces 6+ poll misses → callback mode."""

import asyncio

import pytest
from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_delayed_resolves() -> None:
    assert await _driver._test_delayed_bytes(b"slow", 50) == b"slow"  # noqa: SLF001


@pytest.mark.asyncio
async def test_delayed_zero_ms_still_works() -> None:
    """A zero-delay sleep still defers across at least one event-loop tick."""
    assert await _driver._test_delayed_bytes(b"fast", 0) == b"fast"  # noqa: SLF001


@pytest.mark.asyncio
async def test_delayed_marks_callback_state() -> None:
    """After we await past 5 polls, the awaitable should have a _loop."""
    awaitable = _driver._test_delayed_bytes(b"x", 100)  # noqa: SLF001
    task = asyncio.create_task(_consume(awaitable))
    await asyncio.sleep(0.02)
    # By now we've polled at least 6 times and entered callback mode.
    assert awaitable._loop is not None  # noqa: SLF001
    await task


async def _consume(aw):
    return await aw
