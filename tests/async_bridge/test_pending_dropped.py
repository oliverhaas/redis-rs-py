"""Pending (never resolves) and dropped (sender drops) awaitables."""

import asyncio

import pytest
from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_pending_never_resolves_within_window() -> None:
    aw = _driver._test_pending()  # noqa: SLF001
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(aw, timeout=0.1)


@pytest.mark.asyncio
async def test_dropped_raises_runtime_error() -> None:
    aw = _driver._test_dropped()  # noqa: SLF001
    with pytest.raises(RuntimeError, match="dropped"):
        await aw


@pytest.mark.asyncio
async def test_pending_can_be_cancelled() -> None:
    aw = _driver._test_pending()  # noqa: SLF001
    task = asyncio.create_task(_consume(aw))
    await asyncio.sleep(0.02)  # let the task enter callback mode
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert aw.cancelled() is True
    assert aw.done() is True


async def _consume(aw):
    return await aw
