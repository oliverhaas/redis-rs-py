"""Scenario 8 — construct + first-PING latency.

Short-lived processes pay this once per cold start; serverless and CLI
tools care a lot about it. We measure the full end-to-end:
``from_url`` → first command response.
"""

import pytest

from benchmarks._helpers import (
    glide_async_client,
    py_client,
    rs_client,
)


@pytest.mark.benchmark(group="connect")
def test_connect_redis_rs_py(benchmark, valkey_url) -> None:
    def _connect_and_ping() -> None:
        c = rs_client(valkey_url)
        c.ping()
        c.close()

    benchmark(_connect_and_ping)


@pytest.mark.benchmark(group="connect")
def test_connect_redis_py_hiredis(benchmark, valkey_url) -> None:
    def _connect_and_ping() -> None:
        c = py_client(valkey_url)
        c.ping()
        c.close()

    benchmark(_connect_and_ping)


@pytest.mark.benchmark(group="connect")
def test_connect_valkey_glide(benchmark, valkey_url, event_loop) -> None:
    async def _connect_and_ping() -> None:
        c = await glide_async_client(valkey_url)
        await c.ping()
        await c.close()

    benchmark(lambda: event_loop.run_until_complete(_connect_and_ping()))
