"""Live-Valkey fixtures for the driver and façade test suites.

We use testcontainers to bring up a single shared Valkey instance per
pytest session. The `valkey_url` fixture is xdist-safe: the worker that
wins the race owns the container; other workers wait on a sidecar file.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from filelock import FileLock
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

if TYPE_CHECKING:
    from collections.abc import Iterator

VALKEY_IMAGE = os.environ.get("REDIS_RS_PY_VALKEY_IMAGE", "valkey/valkey:8.0")


def _spawn_valkey() -> tuple[DockerContainer, str]:
    container = DockerContainer(VALKEY_IMAGE).with_exposed_ports(6379)
    container.start()
    wait_for_logs(container, "Ready to accept connections", timeout=30)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)
    return container, f"redis://{host}:{port}/0"


@pytest.fixture(scope="session")
def valkey_url(tmp_path_factory: pytest.TempPathFactory, worker_id: str) -> Iterator[str]:
    if worker_id == "master":
        container, url = _spawn_valkey()
        try:
            yield url
        finally:
            container.stop()
        return

    root = tmp_path_factory.getbasetemp().parent
    lockfile = root / "valkey.lock"
    urlfile = root / "valkey.url"

    with FileLock(str(lockfile)):
        if urlfile.exists():
            container = None
            url = urlfile.read_text().strip()
        else:
            container, url = _spawn_valkey()
            urlfile.write_text(url)

    try:
        yield url
    finally:
        if container is not None:
            container.stop()
            urlfile.unlink(missing_ok=True)


@pytest.fixture
def driver(valkey_url: str):
    from redis_rs_py._driver import RedisRsDriver  # noqa: PLC0415

    drv = RedisRsDriver.connect_standard(valkey_url)
    # FLUSHDB so each test starts clean. We call sync `flushdb` once it lands;
    # for now use the upstream redis-py client.
    import redis  # noqa: PLC0415

    rp = redis.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    return drv
