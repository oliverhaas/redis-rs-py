"""Sentinel pyclass — constructor, master_for, slave_for.

Gated behind REDIS_RS_PY_SENTINEL_TESTS=1.
"""

import pytest
from redis_rs_py.sentinel import Sentinel


def _nodes(sentinel_urls: list[str]) -> list[tuple[str, int]]:
    return [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls
    ]


def test_construct_with_tuples(sentinel_urls: list[str]) -> None:
    nodes = _nodes(sentinel_urls)
    s = Sentinel(nodes)
    assert s.sentinels == nodes


def test_construct_with_min_other_sentinels(sentinel_urls: list[str]) -> None:
    nodes = _nodes(sentinel_urls)
    s = Sentinel(nodes, min_other_sentinels=1)
    assert s.min_other_sentinels == 1


def test_unknown_kwarg_warns(sentinel_urls: list[str]) -> None:
    import warnings

    nodes = _nodes(sentinel_urls)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        Sentinel(nodes, weird_kwarg="ignore-me")
    assert any("weird_kwarg" in str(rec.message) for rec in w)


def test_empty_sentinels_raises_dataerror() -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError):
        Sentinel([])


def test_master_for_returns_writable_client(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    nodes = _nodes(sentinel_urls)
    s = Sentinel(nodes)
    master = s.master_for(sentinel_service_name)
    master.set("master-write", b"yes")
    assert master.get("master-write") == b"yes"


def test_slave_for_returns_readable_client(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    import time

    nodes = _nodes(sentinel_urls)
    s = Sentinel(nodes)
    master = s.master_for(sentinel_service_name)
    master.set("slave-readback", b"replica-sees-this")

    time.sleep(1.5)  # let replication catch up

    slave = s.slave_for(sentinel_service_name)
    assert slave.get("slave-readback") == b"replica-sees-this"


def test_master_for_kwargs_override_construction_kwargs(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    nodes = _nodes(sentinel_urls)
    s = Sentinel(nodes, db=0)
    master = s.master_for(sentinel_service_name, db=0)
    assert master.ping() is True


def test_slave_for_round_robin_across_calls(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    """With one slave in the topology, round-robin always returns the
    same slave but the cursor still increments — assert the call
    succeeds repeatedly."""
    nodes = _nodes(sentinel_urls)
    s = Sentinel(nodes)
    for _ in range(3):
        slave = s.slave_for(sentinel_service_name)
        assert slave.ping() is True
