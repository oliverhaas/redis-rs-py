"""Critical correctness test: transparent failover.

This test is intentionally heavyweight (~30-60 s wall clock):
  * Use `master_for` to grab a writable Redis pointed at the master.
  * SET a key (verifies the connection is healthy).
  * Stop the master container — sentinels detect within ~2 s
    (down-after-milliseconds), trigger a vote, elect the replica.
  * SET again on the same Redis handle — the in-flight call hits a
    closed connection, sentinel_retry! triggers, rediscover() picks up
    the new master, the retry SET completes against the new master.

Gated behind REDIS_RS_PY_SENTINEL_TESTS=1.
"""

import time
from typing import TYPE_CHECKING

import pytest
from redis_rs_py.sentinel import Sentinel

if TYPE_CHECKING:
    from testcontainers.core.container import DockerContainer


def test_failover_is_transparent_to_master_for_caller(  # noqa: C901
    sentinel_urls: list[str],
    sentinel_service_name: str,
    sentinel_containers: list[DockerContainer],
) -> None:
    if not sentinel_containers:
        pytest.skip("Failover test runs only on the xdist master worker.")

    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls
    ]
    s = Sentinel(nodes)
    master = s.master_for(sentinel_service_name)

    # Sanity: write succeeds against the original master.
    master.set("pre-failover", b"ok")
    assert master.get("pre-failover") == b"ok"

    # Find the container running on the master's host:port and stop it.
    addr = s.discover_master(sentinel_service_name)
    host, port = addr
    target = None
    for c in sentinel_containers:
        try:
            ch_host = c.get_container_host_ip()
            ch_port = int(c.get_exposed_port(6379))
        except Exception:  # noqa: S112
            continue
        if ch_host == host and ch_port == port:
            target = c
            break

    if target is None:
        pytest.skip(f"no container matches master {host}:{port} — cannot stop it")
    target.stop()

    # Wait for sentinels to elect a new master. With down-after-milliseconds
    # = 2000 + failover-timeout = 10000, this should be < 20 s.
    deadline = time.monotonic() + 30
    new_addr: tuple[str, int] | None = None
    while time.monotonic() < deadline:
        try:
            candidate = s.discover_master(sentinel_service_name)
            if candidate != addr:
                new_addr = candidate
                break
        except Exception:
            pass
        time.sleep(0.5)

    assert new_addr is not None, "no failover within 30s"
    assert new_addr != addr

    # Critical: this SET must succeed. The handle is still the SAME
    # `master` object created before the failover. The first attempt may
    # hit the dead connection; sentinel_retry! triggers rediscover, the
    # retry hits the new master.
    deadline = time.monotonic() + 20
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            master.set("post-failover", b"ok")
            assert master.get("post-failover") == b"ok"
            break
        except Exception as e:
            last_err = e
            time.sleep(0.3)
    else:
        raise AssertionError(
            f"post-failover SET never succeeded; last error: {last_err}",
        )
