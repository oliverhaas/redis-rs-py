"""Scenario 4 — pipelined GET throughput (1000 commands per pipeline).

A "pipeline" here is a single round trip carrying 1000 GETs. Pipelines
are the canonical "how fast can you push commands at the wire" test;
removing per-command Python frame overhead is exactly the value
proposition we're measuring.

valkey-glide does not expose a named ``pipeline()`` builder but provides
a ``Batch`` object (``is_atomic=False`` for non-transactional). We use
that as the structural equivalent.
"""

import pytest

from benchmarks._helpers import (
    PIPELINE_KEYS,
    glide_async_client,
    py_client,
    rs_client,
)


def _pipeline_get_sync(client) -> None:
    pipe = client.pipeline(transaction=False)
    for k in PIPELINE_KEYS:
        pipe.get(k)
    pipe.execute()


async def _pipeline_get_glide(client) -> None:
    from glide import Batch

    batch = Batch(is_atomic=False)
    for k in PIPELINE_KEYS:
        batch.get(k)
    await client.exec(batch, raise_on_error=True)


@pytest.mark.benchmark(group="pipeline-1000")
def test_pipeline_redis_rs_py(benchmark, pipeline_keys) -> None:
    client = rs_client(pipeline_keys)
    try:
        benchmark(_pipeline_get_sync, client)
    finally:
        client.close()


@pytest.mark.benchmark(group="pipeline-1000")
def test_pipeline_redis_py_hiredis(benchmark, pipeline_keys) -> None:
    client = py_client(pipeline_keys)
    try:
        benchmark(_pipeline_get_sync, client)
    finally:
        client.close()


@pytest.mark.benchmark(group="pipeline-1000")
def test_pipeline_valkey_glide(benchmark, pipeline_keys, event_loop) -> None:
    client = event_loop.run_until_complete(glide_async_client(pipeline_keys))
    try:
        benchmark(lambda: event_loop.run_until_complete(_pipeline_get_glide(client)))
    finally:
        event_loop.run_until_complete(client.close())
