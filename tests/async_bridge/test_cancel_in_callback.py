"""Cancel after callback-mode initialisation must wake pending callbacks."""

import asyncio

import pytest
from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_cancel_wakes_pending_done_callback() -> None:
    aw = _driver._test_pending()  # noqa: SLF001
    fired = asyncio.Event()

    task = asyncio.create_task(_consume(aw))
    await asyncio.sleep(0.02)  # callback mode entered

    def on_done(_fut):
        fired.set()

    aw.add_done_callback(on_done)

    assert aw.cancel() is True
    # Cancellation must schedule the callback (loop.call_soon).
    await asyncio.wait_for(fired.wait(), timeout=0.5)

    with pytest.raises(asyncio.CancelledError):
        await task

    assert aw.cancel() is False  # already cancelled


async def _consume(aw):
    return await aw
