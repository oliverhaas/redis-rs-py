"""Live-Valkey fixtures for the driver and façade test suites.

We use testcontainers to bring up a single shared Valkey instance per
pytest session. The `valkey_url` fixture is xdist-safe: the worker that
wins the race owns the container; other workers wait on a sidecar file.

Cluster fixtures (Plan 15) spawn a 3-master/3-replica Valkey cluster using
six standalone containers wired with CLUSTER MEET/ADDSLOTS/REPLICATE.
They are gated behind REDIS_RS_PY_CLUSTER_TESTS=1 to avoid slowing down
the regular CI loop.
"""

import os
import time
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
    ``Redis``/``AsyncRedis`` call surface so existing tests need no changes.

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


# =============================================================================
# Cluster fixtures (Plan 15) — gated behind REDIS_RS_PY_CLUSTER_TESTS=1
# =============================================================================
#
# Spins six standalone Valkey containers on a shared docker network, wires
# them into a 3-master/3-replica cluster, and yields the three master URLs.
# The setup can take 30-90 seconds on first run; subsequent pytest-xdist
# workers share a single cluster via the file-lock + sidecar pattern.
#
# Set REDIS_RS_PY_CLUSTER_TESTS=1 to enable. Without it all cluster tests
# are skipped automatically.

_CLUSTER_ENABLED = os.environ.get("REDIS_RS_PY_CLUSTER_TESTS", "0") == "1"
_CLUSTER_NODE_COUNT = 6  # 3 masters + 3 replicas
_CLUSTER_SLOT_TOTAL = 16384
_CLUSTER_PINNED_CONTAINERS: list[DockerContainer] = []


def _cluster_command(container: DockerContainer, *args: str) -> str:
    """Run valkey-cli inside a container and return trimmed stdout."""
    cmd = ["valkey-cli", "-p", "6379", *args]
    rc, out = container.exec(cmd)
    if rc != 0:
        raise RuntimeError(f"valkey-cli {args} failed (rc={rc}): {out!r}")
    return out.decode().strip() if isinstance(out, (bytes, bytearray)) else out.strip()


def _container_ip(container: DockerContainer, network_name: str) -> str:
    """Return the container's IP on the given network."""
    import docker as docker_sdk

    client = docker_sdk.from_env()
    container_id = container._container.id  # noqa: SLF001
    info = client.containers.get(container_id)
    nets = info.attrs["NetworkSettings"]["Networks"]
    # Try exact name first, then any network
    for name, data in nets.items():
        if name == network_name or network_name in name:
            return data["IPAddress"]
    # fallback: first network
    for data in nets.values():
        if data.get("IPAddress"):
            return data["IPAddress"]
    raise RuntimeError(f"Could not find IP for container {container_id!r}")


def _spawn_cluster() -> tuple[object, list[DockerContainer], list[str]]:  # noqa: C901, PLR0912
    """Bring up a 3-master/3-replica cluster. Returns (network, containers, master_urls)."""
    from testcontainers.core.network import Network

    network = Network()
    network.create()
    network_name: str = network._network.name  # type: ignore[attr-defined]  # noqa: SLF001

    containers: list[DockerContainer] = []
    for idx in range(_CLUSTER_NODE_COUNT):
        name = f"valkey-cluster-node-{idx}"
        c = (
            DockerContainer(VALKEY_IMAGE)
            .with_network(network)
            .with_name(name)
            .with_exposed_ports(6379)
            .with_command(
                "valkey-server "
                "--cluster-enabled yes "
                "--cluster-node-timeout 5000 "
                "--appendonly yes "
                '--save "" '
                "--protected-mode no "
                "--port 6379",
            )
        )
        c.start()
        wait_for_logs(c, "Ready to accept connections", timeout=30)
        containers.append(c)

    # Collect internal IPs — Valkey CLUSTER MEET requires IP, not hostname.
    container_ips = [_container_ip(c, network_name) for c in containers]

    # Step A: CLUSTER MEET — point node 0 at every other node via IP.
    for idx in range(1, _CLUSTER_NODE_COUNT):
        _cluster_command(containers[0], "CLUSTER", "MEET", container_ips[idx], "6379")

    # Wait for topology propagation.
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        info = _cluster_command(containers[0], "CLUSTER", "INFO")
        nodes = _cluster_command(containers[0], "CLUSTER", "NODES").splitlines()
        if "cluster_known_nodes:6" in info and len(nodes) == _CLUSTER_NODE_COUNT:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("Cluster topology did not propagate within 20s")

    # Step B: ADDSLOTS — partition 16384 slots across the 3 masters.
    masters = containers[:3]
    replicas = containers[3:]
    slots_per_master = _CLUSTER_SLOT_TOTAL // 3
    cursor = 0
    for m_idx, master in enumerate(masters):
        end = cursor + slots_per_master + (1 if m_idx == len(masters) - 1 else 0)
        _cluster_command(master, "CLUSTER", "ADDSLOTSRANGE", str(cursor), str(end - 1))
        cursor = end

    # Step C: CLUSTER REPLICATE — attach each replica to a master.
    master_ids = [_cluster_command(m, "CLUSTER", "MYID") for m in masters]

    for r_idx, replica in enumerate(replicas):
        master_id = master_ids[r_idx]
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            nodes = _cluster_command(replica, "CLUSTER", "NODES")
            if master_id in nodes:
                break
            time.sleep(0.3)
        else:
            raise RuntimeError(f"Replica {r_idx} could not see master {master_id} within 10s")
        _cluster_command(replica, "CLUSTER", "REPLICATE", master_id)

    # Step D: wait until cluster_state:ok.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        info = _cluster_command(containers[0], "CLUSTER", "INFO")
        if "cluster_state:ok" in info:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("cluster_state never reached ok")

    # Step E: collect host-mapped URLs for the three masters.
    master_urls: list[str] = []
    for master in masters:
        host = master.get_container_host_ip()
        port = master.get_exposed_port(6379)
        master_urls.append(f"redis://{host}:{port}")

    return network, containers, master_urls


@pytest.fixture(scope="session")
def cluster_urls(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
) -> Iterator[list[str]]:
    """List of redis://host:port URLs for the 3 cluster masters.

    Skipped if REDIS_RS_PY_CLUSTER_TESTS != 1.
    """
    if not _CLUSTER_ENABLED:
        pytest.skip("Cluster tests disabled. Set REDIS_RS_PY_CLUSTER_TESTS=1 to enable.")

    if worker_id == "master":
        network, containers, urls = _spawn_cluster()
        try:
            yield urls
        finally:
            for c in containers:
                c.stop()
            network.remove()
        return

    root = tmp_path_factory.getbasetemp().parent
    lockfile = root / "valkey_cluster.lock"
    urlsfile = root / "valkey_cluster.urls"

    network = None
    containers_owned: list[DockerContainer] = []

    with FileLock(str(lockfile)):
        if urlsfile.exists():
            urls = urlsfile.read_text().strip().splitlines()
        else:
            network, containers_owned, urls = _spawn_cluster()
            urlsfile.write_text("\n".join(urls))
            _CLUSTER_PINNED_CONTAINERS.extend(containers_owned)

    try:
        yield urls
    finally:
        if containers_owned:
            for c in containers_owned:
                c.stop()
            if network is not None:
                network.remove()
            urlsfile.unlink(missing_ok=True)


@pytest.fixture
def cluster_client_sync(cluster_urls: list[str]):
    """Upstream redis-py cluster client used to FLUSHALL between tests."""
    import redis as upstream_redis

    nodes = []
    for url in cluster_urls:
        host, port = url.removeprefix("redis://").split(":", 1)
        nodes.append(upstream_redis.cluster.ClusterNode(host=host, port=int(port)))
    rc = upstream_redis.cluster.RedisCluster(startup_nodes=nodes)
    rc.flushall()
    yield rc
    rc.close()


# =============================================================================
# Sentinel fixtures (Plan 16) — gated behind REDIS_RS_PY_SENTINEL_TESTS=1
# =============================================================================
#
# Spins 1 master + 1 replica + 3 sentinels via testcontainers on a shared
# docker network. Each sentinel gets a config that monitors the master under
# SERVICE_NAME with quorum 2. Tests are gated behind
# REDIS_RS_PY_SENTINEL_TESTS=1 to avoid slowing down the regular CI loop.

_SENTINEL_ENABLED = os.environ.get("REDIS_RS_PY_SENTINEL_TESTS", "0") == "1"

SERVICE_NAME = "redis-rs-py-test-master"
SENTINEL_PORT = 26379


def _spawn_sentinel_topology() -> tuple[Any, list[DockerContainer], list[str]]:
    """Bring up 1 master + 1 replica + 3 sentinels.

    Returns (network, containers, sentinel_urls). All 5 containers share
    a docker network so the sentinels can reach the master/replica by
    name; the 3 sentinel ports are mapped back to host so the Python
    client can connect from outside.
    """
    from testcontainers.core.network import Network

    network = Network()
    network.create()

    containers: list[DockerContainer] = []
    master_name = "valkey-sentinel-master"
    replica_name = "valkey-sentinel-replica"

    # Master.
    master = (
        DockerContainer(VALKEY_IMAGE)
        .with_network(network)
        .with_name(master_name)
        .with_exposed_ports(6379)
        .with_command(
            'valkey-server --port 6379 --protected-mode no --appendonly no --save ""',
        )
    )
    master.start()
    wait_for_logs(master, "Ready to accept connections", timeout=30)
    containers.append(master)

    # Replica.
    replica = (
        DockerContainer(VALKEY_IMAGE)
        .with_network(network)
        .with_name(replica_name)
        .with_exposed_ports(6379)
        .with_command(
            f'valkey-server --port 6379 --protected-mode no --appendonly no --save "" --replicaof {master_name} 6379',
        )
    )
    replica.start()
    wait_for_logs(replica, "Ready to accept connections", timeout=30)
    containers.append(replica)

    # 3 sentinels — each gets its own config written at startup.
    sentinel_urls: list[str] = []
    for idx in range(3):
        name = f"valkey-sentinel-{idx}"
        cfg = (
            f"port {SENTINEL_PORT}\n"
            f"sentinel monitor {SERVICE_NAME} {master_name} 6379 2\n"
            f"sentinel down-after-milliseconds {SERVICE_NAME} 2000\n"
            f"sentinel parallel-syncs {SERVICE_NAME} 1\n"
            f"sentinel failover-timeout {SERVICE_NAME} 10000\n"
            f"sentinel resolve-hostnames yes\n"
            f"protected-mode no\n"
        )
        # Write the config inside the container at startup via shell.
        sentinel = (
            DockerContainer(VALKEY_IMAGE)
            .with_network(network)
            .with_name(name)
            .with_exposed_ports(SENTINEL_PORT)
            .with_command(
                "sh -c 'printf \""
                + cfg.replace('"', '\\"').replace("\n", "\\n")
                + '" | sed "s/\\\\n/\\n/g" > /tmp/sentinel.conf && '
                + "valkey-sentinel /tmp/sentinel.conf'",
            )
        )
        sentinel.start()
        wait_for_logs(sentinel, r"\+monitor", timeout=30)
        containers.append(sentinel)
        host = sentinel.get_container_host_ip()
        port = sentinel.get_exposed_port(SENTINEL_PORT)
        sentinel_urls.append(f"redis://{host}:{port}")

    return network, containers, sentinel_urls


@pytest.fixture(scope="session")
def _sentinel_topology(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
) -> Iterator[tuple[list[str], list[DockerContainer]]]:
    """Internal shared sentinel topology fixture."""
    if not _SENTINEL_ENABLED:
        pytest.skip("Sentinel tests disabled. Set REDIS_RS_PY_SENTINEL_TESTS=1 to enable.")

    if worker_id == "master":
        network, containers, urls = _spawn_sentinel_topology()
        try:
            yield urls, containers
        finally:
            for c in containers:
                c.stop()
            network.remove()
        return

    root = tmp_path_factory.getbasetemp().parent
    lockfile = root / "valkey_sentinel.lock"
    urlsfile = root / "valkey_sentinel.urls"

    network = None
    containers: list[DockerContainer] = []

    with FileLock(str(lockfile)):
        if urlsfile.exists():
            urls = urlsfile.read_text().strip().splitlines()
        else:
            network, containers, urls = _spawn_sentinel_topology()
            urlsfile.write_text("\n".join(urls))

    try:
        yield urls, containers
    finally:
        if containers:
            for c in containers:
                c.stop()
            if network is not None:
                network.remove()
            urlsfile.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def sentinel_urls(_sentinel_topology: tuple[list[str], list[DockerContainer]]) -> list[str]:
    """List of redis://host:port URLs for the 3 sentinels."""
    urls, _ = _sentinel_topology
    return urls


@pytest.fixture(scope="session")
def sentinel_service_name() -> str:
    """The service name registered with the sentinels."""
    return SERVICE_NAME


@pytest.fixture(scope="session")
def sentinel_containers(
    _sentinel_topology: tuple[list[str], list[DockerContainer]],
) -> list[DockerContainer]:
    """Container handles for the sentinel topology — used by the failover test."""
    _, containers = _sentinel_topology
    return containers
