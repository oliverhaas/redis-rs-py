"""ValkeyConn / connect_sentinel end-to-end smoke tests.

Gated behind REDIS_RS_PY_SENTINEL_TESTS=1.
"""

from __future__ import annotations

import pytest


def test_sentinel_fixture_brings_up_three_sentinels(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    assert len(sentinel_urls) == 3
    assert sentinel_service_name == "redis-rs-py-test-master"
    for u in sentinel_urls:
        assert u.startswith("redis://")


def test_sentinel_quorum_reports_master(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    """Use the upstream redis-py Sentinel to confirm the topology is healthy."""
    import redis.sentinel as upstream

    nodes = []
    for url in sentinel_urls:
        host, port = url.removeprefix("redis://").split(":", 1)
        nodes.append((host, int(port)))
    s = upstream.Sentinel(nodes, socket_timeout=2.0)
    addr = s.discover_master(sentinel_service_name)
    assert addr is not None
    host, port = addr
    assert isinstance(host, str)
    assert isinstance(port, int) and port > 0


def test_connect_sentinel_master_smoke(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    from redis_rs_py.sentinel import Sentinel

    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls
    ]
    s = Sentinel(nodes)
    master = s.master_for(sentinel_service_name)
    master.set("smoke", b"ok")
    assert master.get("smoke") == b"ok"
    master.delete("smoke")


def test_connect_sentinel_slave_can_read(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    import time

    from redis_rs_py.sentinel import Sentinel

    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls
    ]
    s = Sentinel(nodes)
    master = s.master_for(sentinel_service_name)
    master.set("slave-read", b"yes")

    # Wait briefly for replication.
    time.sleep(1.5)

    slave = s.slave_for(sentinel_service_name)
    assert slave.get("slave-read") == b"yes"


@pytest.mark.asyncio
async def test_aconnect_sentinel_master(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    from redis_rs_py.asyncio.sentinel import AsyncSentinel

    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls
    ]
    s = AsyncSentinel(nodes)
    master = s.master_for(sentinel_service_name)
    await master.set("async-smoke", b"ok")
    assert await master.get("async-smoke") == b"ok"


def test_connect_sentinel_unknown_service_raises(
    sentinel_urls: list[str],
) -> None:
    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError
    from redis_rs_py.sentinel import Sentinel

    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls
    ]
    s = Sentinel(nodes)
    with pytest.raises(RedisConnectionError):
        s.master_for("no-such-master")


def test_connect_sentinel_no_sentinels_raises() -> None:
    from redis_rs_py.exceptions import DataError
    from redis_rs_py.sentinel import Sentinel

    with pytest.raises(DataError):
        Sentinel([])
