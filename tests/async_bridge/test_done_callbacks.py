"""add_done_callback with and without contextvars.Context."""

import asyncio
import contextvars

import pytest
from redis_rs_py import _driver

VAR: contextvars.ContextVar[str] = contextvars.ContextVar("VAR", default="default")


@pytest.mark.asyncio
async def test_done_callback_without_context() -> None:
    aw = _driver._test_delayed_bytes(b"x", 50)  # noqa: SLF001
    seen: list[object] = []

    # Trigger entry into callback mode by yielding past the busy-yield window.
    task = asyncio.create_task(_consume(aw))
    await asyncio.sleep(0.02)

    def on_done(fut):
        seen.append(fut)

    aw.add_done_callback(on_done)
    await task
    # _wake fires the callback synchronously after StopIteration delivery.
    assert len(seen) == 1
    assert seen[0] is aw


@pytest.mark.asyncio
async def test_done_callback_runs_in_provided_context() -> None:
    aw = _driver._test_delayed_bytes(b"x", 50)  # noqa: SLF001
    captured: list[str] = []

    ctx = contextvars.copy_context()
    ctx.run(VAR.set, "from-context")

    task = asyncio.create_task(_consume(aw))
    await asyncio.sleep(0.02)

    def on_done(_fut):
        captured.append(VAR.get())

    aw.add_done_callback(on_done, context=ctx)
    await task

    # The callback ran inside `ctx`, so it must observe the value set there
    # rather than the default.
    assert captured == ["from-context"]


async def _consume(aw):
    return await aw
