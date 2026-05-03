"""Live-Valkey fixtures for the driver and façade test suites.

We use testcontainers to bring up a single shared Valkey instance per
pytest session. The `valkey_url` fixture is xdist-safe: the worker that
wins the race owns the container; other workers wait on a sidecar file.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

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


class _DriverCompat:
    """Thin compatibility shim that exposes both sync (Redis) and async
    (AsyncRedis) APIs on a single object, preserving the old
    ``RedisRsDriver`` call surface so existing tests need no changes.

    Sync method calls are forwarded to ``_sync``.
    Calls with an ``a`` prefix (e.g. ``driver.aset(...)``) are forwarded
    to the matching un-prefixed method on ``_async``.
    """

    def __init__(self, url: str) -> None:
        from redis_rs_py import Redis
        from redis_rs_py.asyncio import Redis as AsyncRedis

        self._sync = Redis.from_url(url)
        self._async = AsyncRedis.from_url(url)

    # Expose sync attrs directly (connection_url, cache_statistics, etc.)
    def __getattr__(self, name: str) -> Any:
        # Route ``a``-prefixed names and ``await_`` to the async object.
        #
        # ``await_`` is the Python-keyword-safe alias for ``wait`` on the
        # async Redis class. We check it before the general "a"-prefix
        # stripping so it isn't mangled to "wait_".
        if name == "await_":
            return self._async.await_
        # Names containing "async" (e.g. ``scan_iter_async``) are bound to
        # the async object so that ``self.scan(...)`` inside them awaits
        # properly — the sync Redis.scan_iter_async is the same function but
        # bound to a sync object, causing a "tuple can't be awaited" error.
        if "async" in name:
            async_attr = getattr(self._async, name, None)
            if async_attr is not None:
                return async_attr
        if name.startswith("a") and len(name) > 1 and not name.startswith("async"):
            unprefixed = name[1:]
            async_attr = getattr(self._async, unprefixed, None)
            if async_attr is not None:
                return async_attr
        # Default: forward to sync object.
        return getattr(self._sync, name)


@pytest.fixture
def driver(valkey_url: str) -> _DriverCompat:
    compat = _DriverCompat(valkey_url)
    # FLUSHDB the per-worker DB so each test starts clean.
    import redis

    rp = redis.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    return compat


@pytest.fixture
def redis_client(valkey_url: str):
    """Sync Redis client fixture (new API name)."""
    import redis
    from redis_rs_py import Redis

    rp = redis.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    return Redis.from_url(valkey_url)


@pytest.fixture
def async_redis_client(valkey_url: str):
    """Async Redis client fixture."""
    from redis_rs_py.asyncio import Redis as AsyncRedis

    return AsyncRedis.from_url(valkey_url)


@pytest.fixture
def redis_py_client(valkey_url: str):
    """Upstream redis-py client against the same Valkey instance.

    Used by parity tests in plans 08+ to compare reply shapes between
    redis-rs-py and redis-py — for stream commands especially, the
    bytes-vs-tuple-vs-dict shape contract is non-trivial and must
    match exactly.
    """
    import redis

    rp = redis.Redis.from_url(valkey_url, decode_responses=False)
    yield rp
    rp.close()
