"""Scenarios 5-6 — async single-task and 100-task concurrent GETs.

Single-task: a single coroutine awaits ``client.get()`` in a loop. This
isolates per-call interpreter + bridge overhead.

100-task: 100 coroutines each await one GET, gathered. This exercises
the connection multiplexer; the Rust core's tokio-driven concurrency is
where it should pull ahead of redis-py (which serialises through a
Python event loop) and match valkey-glide (which has the same async
backbone we do).

Run via pyperf directly:

    BENCH_VALKEY_URL=redis://127.0.0.1:6379/0 \\
        uv run --group bench python benchmarks/bench_async_throughput.py \\
        -o benchmarks/results/async_throughput.json

Or via the orchestrator:

    uv run --group bench python benchmarks/run_all.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Ensure the repo root is on sys.path so pyperf worker subprocesses
# can resolve ``benchmarks._helpers`` regardless of cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pyperf

from benchmarks._helpers import (
    HOT_KEY,
    ensure_bench_env_inherited,
    get_valkey_url,
    glide_async_client,
    py_async_client,
    rs_async_client,
    seed_hot_key,
)

CONCURRENT_TASKS = 100


# ---------------------------------------------------------------------------
# Scenario 5: async single task
# ---------------------------------------------------------------------------


def _bench_async_single_rs(loops: int, url: str) -> float:
    async def _go() -> float:
        client = rs_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.get(HOT_KEY)
        dt = pyperf.perf_counter() - t0
        await client.aclose()
        return dt

    return asyncio.run(_go())


def _bench_async_single_py(loops: int, url: str) -> float:
    async def _go() -> float:
        client = py_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.get(HOT_KEY)
        dt = pyperf.perf_counter() - t0
        await client.aclose()
        return dt

    return asyncio.run(_go())


def _bench_async_single_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.get(HOT_KEY)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# Scenario 6: 100 concurrent tasks per iteration
# ---------------------------------------------------------------------------


def _bench_async_100_rs(loops: int, url: str) -> float:
    async def _one_get(client: object, key: str) -> object:
        return await client.get(key)  # type: ignore[union-attr]

    async def _go() -> float:
        client = rs_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            # Wrap each awaitable in a coroutine so asyncio.gather treats
            # them as tasks rather than pre-bound futures (RedisRsAwaitable
            # is not a plain coroutine and triggers a loop-mismatch error if
            # passed to gather directly).
            await asyncio.gather(*[_one_get(client, HOT_KEY) for _ in range(CONCURRENT_TASKS)])
        dt = pyperf.perf_counter() - t0
        await client.aclose()
        return dt

    return asyncio.run(_go())


def _bench_async_100_py(loops: int, url: str) -> float:
    async def _go() -> float:
        client = py_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await asyncio.gather(*[client.get(HOT_KEY) for _ in range(CONCURRENT_TASKS)])
        dt = pyperf.perf_counter() - t0
        await client.aclose()
        return dt

    return asyncio.run(_go())


def _bench_async_100_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await asyncio.gather(*[client.get(HOT_KEY) for _ in range(CONCURRENT_TASKS)])
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


def main() -> None:
    ensure_bench_env_inherited()
    runner = pyperf.Runner()
    url = get_valkey_url()
    seed_hot_key(url)

    runner.bench_time_func("async-single/redis-rs-py", _bench_async_single_rs, url)
    runner.bench_time_func("async-single/redis-py[hiredis]", _bench_async_single_py, url)
    runner.bench_time_func("async-single/valkey-glide", _bench_async_single_glide, url)

    runner.bench_time_func("async-100/redis-rs-py", _bench_async_100_rs, url)
    runner.bench_time_func("async-100/redis-py[hiredis]", _bench_async_100_py, url)
    runner.bench_time_func("async-100/valkey-glide", _bench_async_100_glide, url)


if __name__ == "__main__":
    main()
