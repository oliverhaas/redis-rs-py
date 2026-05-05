"""Scenarios 5-6 — async single-task and 100-task concurrent GETs.

Single-task: one coroutine awaits ``client.get()`` per timed call. This
isolates per-call interpreter + bridge overhead.

100-task: 100 coroutines each await one GET, gathered. This exercises
the connection multiplexer; the Rust core's tokio-driven concurrency is
where it should pull ahead of redis-py (which serialises through a
Python event loop) and match valkey-glide (which has the same async
backbone we do).

Async calls are wrapped in coroutine factories so that ``client.get(...)``
is evaluated *inside* the running loop. ``RedisRsAwaitable`` captures the
event loop at construction time, so building it outside a running loop
binds it to the wrong one and ``run_until_complete`` raises
``ValueError: The future belongs to a different loop``.
"""

import asyncio

import pytest

from benchmarks._helpers import (
    HOT_KEY,
    glide_async_client,
    py_async_client,
    rs_async_client,
)

CONCURRENT_TASKS = 100


# ---------------------------------------------------------------------------
# Scenario 5: async single task
# ---------------------------------------------------------------------------


async def _single_get(client) -> object:
    return await client.get(HOT_KEY)


@pytest.mark.benchmark(group="async-single")
def test_async_single_redis_rs_py(benchmark, hot_key, event_loop) -> None:
    client = rs_async_client(hot_key)
    try:
        benchmark(lambda: event_loop.run_until_complete(_single_get(client)))
    finally:
        event_loop.run_until_complete(client.aclose())


@pytest.mark.benchmark(group="async-single")
def test_async_single_redis_py_hiredis(benchmark, hot_key, event_loop) -> None:
    client = py_async_client(hot_key)
    try:
        benchmark(lambda: event_loop.run_until_complete(_single_get(client)))
    finally:
        event_loop.run_until_complete(client.aclose())


@pytest.mark.benchmark(group="async-single")
def test_async_single_valkey_glide(benchmark, hot_key, event_loop) -> None:
    client = event_loop.run_until_complete(glide_async_client(hot_key))
    try:
        benchmark(lambda: event_loop.run_until_complete(_single_get(client)))
    finally:
        event_loop.run_until_complete(client.close())


# ---------------------------------------------------------------------------
# Scenario 6: 100 concurrent tasks per iteration
# ---------------------------------------------------------------------------


async def _gather_redis_rs_py(client) -> None:
    # Wrap each awaitable in a coroutine so asyncio.gather treats them as
    # tasks rather than pre-bound futures (RedisRsAwaitable is not a plain
    # coroutine and triggers a loop-mismatch error if passed to gather
    # directly).
    async def _one() -> object:
        return await client.get(HOT_KEY)

    await asyncio.gather(*[_one() for _ in range(CONCURRENT_TASKS)])


async def _gather_plain(client) -> None:
    await asyncio.gather(*[client.get(HOT_KEY) for _ in range(CONCURRENT_TASKS)])


@pytest.mark.benchmark(group="async-100")
def test_async_100_redis_rs_py(benchmark, hot_key, event_loop) -> None:
    client = rs_async_client(hot_key)
    try:
        benchmark(lambda: event_loop.run_until_complete(_gather_redis_rs_py(client)))
    finally:
        event_loop.run_until_complete(client.aclose())


@pytest.mark.benchmark(group="async-100")
def test_async_100_redis_py_hiredis(benchmark, hot_key, event_loop) -> None:
    client = py_async_client(hot_key)
    try:
        benchmark(lambda: event_loop.run_until_complete(_gather_plain(client)))
    finally:
        event_loop.run_until_complete(client.aclose())


@pytest.mark.benchmark(group="async-100")
def test_async_100_valkey_glide(benchmark, hot_key, event_loop) -> None:
    client = event_loop.run_until_complete(glide_async_client(hot_key))
    try:
        benchmark(lambda: event_loop.run_until_complete(_gather_plain(client)))
    finally:
        event_loop.run_until_complete(client.close())
