"""Scenario 4 — pipelined GET throughput (1000 commands per pipeline).

A "pipeline" here is a single round trip carrying 1000 GETs. Pipelines
are the canonical "how fast can you push commands at the wire" test;
removing per-command Python frame overhead is exactly the value
proposition we're measuring.

valkey-glide does not expose a named pipeline() builder but provides
a ``Batch`` object (is_atomic=False for non-transactional). We use that
as the structural equivalent.

Run via pyperf directly:

    BENCH_VALKEY_URL=redis://127.0.0.1:6379/0 \\
        uv run --group bench python benchmarks/bench_pipeline.py \\
        -o benchmarks/results/pipeline.json

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
    PIPELINE_KEYS,
    ensure_bench_env_inherited,
    get_valkey_url,
    glide_async_client,
    py_client,
    rs_client,
    seed_pipeline_keys,
)


def _bench_pipeline_rs(loops: int, url: str) -> float:
    client = rs_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        pipe = client.pipeline(transaction=False)
        for k in PIPELINE_KEYS:
            pipe.get(k)
        pipe.execute()
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_pipeline_py(loops: int, url: str) -> float:
    client = py_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        pipe = client.pipeline(transaction=False)
        for k in PIPELINE_KEYS:
            pipe.get(k)
        pipe.execute()
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_pipeline_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        from glide import Batch

        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            batch = Batch(is_atomic=False)
            for k in PIPELINE_KEYS:
                batch.get(k)
            await client.exec(batch, raise_on_error=True)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


def main() -> None:
    ensure_bench_env_inherited()
    runner = pyperf.Runner()
    url = get_valkey_url()
    seed_pipeline_keys(url)

    runner.bench_time_func("pipeline-1000/redis-rs-py", _bench_pipeline_rs, url)
    runner.bench_time_func("pipeline-1000/redis-py[hiredis]", _bench_pipeline_py, url)
    runner.bench_time_func("pipeline-1000/valkey-glide", _bench_pipeline_glide, url)


if __name__ == "__main__":
    main()
