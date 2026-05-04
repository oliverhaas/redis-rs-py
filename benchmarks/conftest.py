"""Shared Valkey fixture for the bench suite.

Mirrors ``tests/conftest.py``'s ``valkey_url`` but with bench-only
defaults: session-scope, env-var publication so ``pyperf``-driven
direct invocations can find the URL too.

Pinning the image tag is part of the reproducibility contract — if
your local results disagree with the committed numbers, check that
``BENCH_VALKEY_IMAGE`` matches the value listed in
``benchmarks/RESULTS.md``'s reference-machine block.
"""

import os
from collections.abc import Iterator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

VALKEY_IMAGE = os.environ.get("BENCH_VALKEY_IMAGE", "valkey/valkey:8.0")


@pytest.fixture(scope="session")
def valkey_url() -> Iterator[str]:
    """Spin up a single Valkey container for the whole bench session."""
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
