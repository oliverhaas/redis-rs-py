"""Scenario 8 — construct + first-PING latency.

Short-lived processes pay this once per cold start; serverless and CLI
tools care a lot about it. We measure the full end-to-end:
``from_url`` -> first command response.

Run via pyperf directly:

    BENCH_VALKEY_URL=redis://127.0.0.1:6379/0 \\
        uv run --group bench python benchmarks/bench_connect.py \\
        -o benchmarks/results/connect.json

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
    ensure_bench_env_inherited,
    get_valkey_url,
    glide_async_client,
    py_client,
    rs_client,
)


def _bench_connect_rs(loops: int, url: str) -> float:
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        c = rs_client(url)
        c.ping()
        c.close()
    return pyperf.perf_counter() - t0


def _bench_connect_py(loops: int, url: str) -> float:
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        c = py_client(url)
        c.ping()
        c.close()
    return pyperf.perf_counter() - t0


def _bench_connect_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            c = await glide_async_client(url)
            await c.ping()
            await c.close()
        return pyperf.perf_counter() - t0

    return asyncio.run(_go())


def main() -> None:
    ensure_bench_env_inherited()
    runner = pyperf.Runner()
    url = get_valkey_url()

    runner.bench_time_func("connect/redis-rs-py", _bench_connect_rs, url)
    runner.bench_time_func("connect/redis-py[hiredis]", _bench_connect_py, url)
    runner.bench_time_func("connect/valkey-glide", _bench_connect_glide, url)


if __name__ == "__main__":
    main()
