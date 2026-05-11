"""Bench-suite fixtures: Valkey container, seeding, async event loop.

Pinning the image tag is part of the reproducibility contract — if your
local results disagree with the committed numbers, check that
``BENCH_VALKEY_IMAGE`` matches the value listed in
``benchmarks/RESULTS.md``'s reference-machine block.
"""

import asyncio
import os
from collections.abc import Iterator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from benchmarks._helpers import (
    flush,
    seed_hot_key,
    seed_mget_keys,
    seed_pipeline_keys,
)

VALKEY_IMAGE = os.environ.get("BENCH_VALKEY_IMAGE", "valkey/valkey:8.0")


@pytest.fixture(scope="session")
def valkey_url() -> Iterator[str]:
    """Spin up a single Valkey container for the whole bench session.

    Honors ``BENCH_VALKEY_URL`` if already set (e.g. by ``run_all.py``,
    which spawns the container itself so the URL is shared across the
    pytest invocation and the renderer). Otherwise, spawn a container
    here and tear it down at session end.
    """
    preset_url = os.environ.get("BENCH_VALKEY_URL")
    if preset_url:
        yield preset_url
        return

    container = DockerContainer(VALKEY_IMAGE).with_exposed_ports(6379)
    container.start()
    try:
        wait_for_logs(container, "Ready to accept connections", timeout=30)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        url = f"redis://{host}:{port}/0"
        os.environ["BENCH_VALKEY_URL"] = url
        yield url
    finally:
        container.stop()
        os.environ.pop("BENCH_VALKEY_URL", None)


@pytest.fixture
def flushed_db(valkey_url: str) -> str:
    """FLUSHDB before the test, so each scenario starts clean."""
    flush(valkey_url)
    return valkey_url


@pytest.fixture
def hot_key(valkey_url: str) -> str:
    """Seed the single hot key used by GET-style benchmarks."""
    seed_hot_key(valkey_url)
    return valkey_url


@pytest.fixture
def mget_keys(valkey_url: str) -> str:
    """Seed the 100 MGET keys."""
    seed_mget_keys(valkey_url)
    return valkey_url


@pytest.fixture
def pipeline_keys(valkey_url: str) -> str:
    """Seed the 1000 keys for the pipeline benchmark."""
    seed_pipeline_keys(valkey_url)
    return valkey_url


@pytest.fixture(scope="module")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """Module-scoped event loop reused across async benchmarks.

    pytest-codspeed expects sync callables, so async benchmarks call
    ``loop.run_until_complete(coro)`` per inner iteration. Reusing one
    loop avoids the ~50-100 us ``asyncio.new_event_loop()`` cost
    dominating every fast scenario.

    ``set_event_loop`` is required because ``RedisRsAwaitable`` captures
    the running loop at construction time via ``get_event_loop()``;
    without setting our loop as the current one, awaitables built outside
    ``run_until_complete`` would be bound to a stale/different loop and
    raise ``ValueError: The future belongs to a different loop``.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        asyncio.set_event_loop(None)
        loop.close()
