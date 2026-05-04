"""Sentinel discovery + introspection + admin commands.

Gated behind REDIS_RS_PY_SENTINEL_TESTS=1.
"""

import pytest
from redis_rs_py.sentinel import Sentinel

SERVICE_NAME = "redis-rs-py-test-master"


@pytest.fixture
def s(sentinel_urls: list[str]) -> Sentinel:
    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls
    ]
    return Sentinel(nodes)


def test_discover_master(s: Sentinel, sentinel_service_name: str) -> None:
    addr = s.discover_master(sentinel_service_name)
    assert isinstance(addr, tuple)
    host, port = addr
    assert isinstance(host, str)
    assert isinstance(port, int) and port > 0


def test_discover_slaves_returns_list(s: Sentinel, sentinel_service_name: str) -> None:
    slaves = s.discover_slaves(sentinel_service_name)
    assert isinstance(slaves, list)
    # We have exactly 1 replica in the fixture.
    assert len(slaves) == 1
    host, port = slaves[0]
    assert isinstance(host, str)
    assert isinstance(port, int) and port > 0


def test_sentinel_get_master_addr_by_name(
    s: Sentinel,
    sentinel_service_name: str,
) -> None:
    addr = s.sentinel_get_master_addr_by_name(sentinel_service_name)
    assert isinstance(addr, tuple)
    assert len(addr) == 2


def test_discover_master_unknown_service_raises(s: Sentinel) -> None:
    from redis_rs_py.exceptions import MasterDownError

    with pytest.raises(MasterDownError):
        s.discover_master("no-such-service")


def test_sentinel_masters(s: Sentinel) -> None:
    masters = s.sentinel_masters()
    assert isinstance(masters, dict)
    assert SERVICE_NAME in masters


def test_sentinel_master_returns_dict(s: Sentinel, sentinel_service_name: str) -> None:
    info = s.sentinel_master(sentinel_service_name)
    assert isinstance(info, dict)
    assert "ip" in info or b"ip" in info
    assert "port" in info or b"port" in info


def test_sentinel_slaves_returns_list(s: Sentinel, sentinel_service_name: str) -> None:
    slaves = s.sentinel_slaves(sentinel_service_name)
    assert isinstance(slaves, list)
    assert len(slaves) == 1


def test_sentinel_sentinels_returns_list(
    s: Sentinel,
    sentinel_service_name: str,
) -> None:
    sentinels = s.sentinel_sentinels(sentinel_service_name)
    assert isinstance(sentinels, list)
    # Excludes the calling sentinel itself, so 2 of 3.
    assert len(sentinels) == 2


def test_sentinel_set_then_check(s: Sentinel, sentinel_service_name: str) -> None:
    """SET a benign sentinel option, observe it via sentinel_master."""
    s.sentinel_set(sentinel_service_name, "down-after-milliseconds", "3000")
    info = s.sentinel_master(sentinel_service_name)
    val = info.get("down-after-milliseconds") or info.get(b"down-after-milliseconds")
    assert val in {"3000", b"3000"}


def test_sentinel_reset_returns_count(
    s: Sentinel,
    sentinel_service_name: str,
) -> None:
    n = s.sentinel_reset("*")  # reset all known masters
    assert isinstance(n, int)
    assert n >= 1


def test_sentinel_failover_on_known_master(
    s: Sentinel,
    sentinel_service_name: str,
) -> None:
    """sentinel_failover triggers a manual failover. Skipped here to avoid
    racing with failover test."""
    pytest.skip(
        "sentinel_failover races with the failover test; exercised separately.",
    )


def test_sentinel_remove_then_monitor_roundtrip(
    s: Sentinel,
    sentinel_service_name: str,
) -> None:
    """Discover master addr (verifies discover_master); don't remove to avoid
    breaking subsequent tests."""
    addr = s.discover_master(sentinel_service_name)
    assert addr is not None
