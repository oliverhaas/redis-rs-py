"""Scenarios 1-3 — sync GET, SET, MGET.

Each scenario runs all three clients (redis-rs-py, redis-py[hiredis],
valkey-glide) and records ops/sec via pyperf. valkey-glide has no sync
API, so its scenarios run via ``asyncio.run(coro)`` per iteration with
a one-time client cached at module import (the
``asyncio.run`` overhead is shared by ``run_all.py`` for all clients
in the async-throughput scenarios; here it's a deliberate fairness
disclosure noted in the RESULTS.md).

Run via pyperf directly:

    BENCH_VALKEY_URL=redis://127.0.0.1:6379/0 \\
        uv run --group bench python benchmarks/bench_get_set.py \\
        -o benchmarks/results/get_set.json

Or via the orchestrator:

    uv run --group bench python benchmarks/run_all.py
"""

import asyncio
import sys
from pathlib import Path

# Ensure the repo root is on sys.path so pyperf worker subprocesses
# can resolve ``benchmarks._helpers`` regardless of cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pyperf

from benchmarks._helpers import (
    HOT_KEY,
    MGET_KEYS,
    SMALL_VALUE,
    ensure_bench_env_inherited,
    get_valkey_url,
    glide_async_client,
    py_client,
    rs_client,
    seed_hot_key,
    seed_mget_keys,
)

# ---------------------------------------------------------------------------
# Scenario 1: hot-key GET
# ---------------------------------------------------------------------------


def _bench_get_rs(loops: int, url: str) -> float:
    client = rs_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        client.get(HOT_KEY)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_get_py(loops: int, url: str) -> float:
    client = py_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        client.get(HOT_KEY)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_get_glide(loops: int, url: str) -> float:
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
# Scenario 2: SET small value
# ---------------------------------------------------------------------------


def _bench_set_rs(loops: int, url: str) -> float:
    client = rs_client(url)
    t0 = pyperf.perf_counter()
    for i in range(loops):
        client.set(f"bench:set:{i}", SMALL_VALUE)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_set_py(loops: int, url: str) -> float:
    client = py_client(url)
    t0 = pyperf.perf_counter()
    for i in range(loops):
        client.set(f"bench:set:{i}", SMALL_VALUE)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_set_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for i in range(loops):
            await client.set(f"bench:set:{i}", SMALL_VALUE)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# Scenario 3: MGET 100 keys
# ---------------------------------------------------------------------------


def _bench_mget_rs(loops: int, url: str) -> float:
    client = rs_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        client.mget(MGET_KEYS)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_mget_py(loops: int, url: str) -> float:
    client = py_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        client.mget(MGET_KEYS)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_mget_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.mget(MGET_KEYS)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


def main() -> None:
    ensure_bench_env_inherited()
    runner = pyperf.Runner()
    url = get_valkey_url()

    seed_hot_key(url)
    seed_mget_keys(url)

    runner.bench_time_func("get/redis-rs-py", _bench_get_rs, url)
    runner.bench_time_func("get/redis-py[hiredis]", _bench_get_py, url)
    runner.bench_time_func("get/valkey-glide", _bench_get_glide, url)

    runner.bench_time_func("set/redis-rs-py", _bench_set_rs, url)
    runner.bench_time_func("set/redis-py[hiredis]", _bench_set_py, url)
    runner.bench_time_func("set/valkey-glide", _bench_set_glide, url)

    runner.bench_time_func("mget/redis-rs-py", _bench_mget_rs, url)
    runner.bench_time_func("mget/redis-py[hiredis]", _bench_mget_py, url)
    runner.bench_time_func("mget/valkey-glide", _bench_mget_glide, url)


if __name__ == "__main__":
    main()
