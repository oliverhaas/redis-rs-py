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

# Module-global pin so spawned containers survive past their fixture's teardown.
# Under xdist, workers' session-scope teardowns run out-of-order; if the
# container reference were held only in the fixture and dropped on first
# worker's session end, other workers still using the URL would fail with
# ConnectionError. Keeping a module-level pin lets Ryuk reap on process exit.
_PINNED_CONTAINERS: list[DockerContainer] = []


def _spawn_valkey() -> tuple[DockerContainer, str]:
    container = DockerContainer(VALKEY_IMAGE).with_exposed_ports(6379)
    container.start()
    wait_for_logs(container, "Ready to accept connections", timeout=30)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)
    return container, f"redis://{host}:{port}/0"


def _worker_db(worker_id: str) -> int:
    """Map an xdist worker_id to a Valkey DB index in the range [0, 15].

    Without per-worker isolation, parallel workers stomp on each other's keys
    (worker A SETs "a", worker B FLUSHDBs, worker A's count assertion fails).
    Valkey ships 16 numbered DBs by default — one per worker keeps fixtures
    independent.
    """
    if worker_id == "master":
        return 0
    # worker_id is "gw0", "gw1", ... under xdist. Strip "gw" and mod 16.
    digits = worker_id.removeprefix("gw")
    if digits.isdigit():
        return int(digits) % 16
    # Defensive fallback: hash the id deterministically.
    return abs(hash(worker_id)) % 16


def _with_db(url: str, db: int) -> str:
    """Return `url` with its trailing /<n> path segment replaced by /<db>."""
    base, _, _ = url.rpartition("/")
    return f"{base}/{db}"


@pytest.fixture(scope="session")
def valkey_url(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
) -> Iterator[str]:
    """Per-worker DB-isolated Valkey URL.

    Workers share one container (cheap) but each gets its own DB index so
    test fixtures running in parallel don't race each other's keys.
    """
    db = _worker_db(worker_id)
    if worker_id == "master":
        container, url = _spawn_valkey()
        try:
            yield _with_db(url, db)
        finally:
            container.stop()
        return

    root = tmp_path_factory.getbasetemp().parent
    lockfile = root / "valkey.lock"
    urlfile = root / "valkey.url"

    with FileLock(str(lockfile)):
        if urlfile.exists():
            base_url = urlfile.read_text().strip()
        else:
            container, base_url = _spawn_valkey()
            urlfile.write_text(base_url)
            # Pin the container at module level — see _PINNED_CONTAINERS comment.
            _PINNED_CONTAINERS.append(container)

    yield _with_db(base_url, db)


@pytest.fixture
def driver(valkey_url: str):
    from redis_rs_py._driver import RedisRsDriver  # noqa: PLC0415

    drv = RedisRsDriver.connect_standard(valkey_url)
    # FLUSHDB the per-worker DB so each test starts clean. (We call sync
    # `flushdb` once it lands; for now use the upstream redis-py client.)
    import redis  # noqa: PLC0415

    rp = redis.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    return drv
