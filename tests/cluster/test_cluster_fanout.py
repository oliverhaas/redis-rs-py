"""Cross-slot fan-out for MGET, MSET, DEL, EXISTS, UNLINK."""

from __future__ import annotations

import pytest
from redis_rs_py.cluster import RedisCluster


@pytest.fixture
def rc(cluster_urls: list[str], cluster_client_sync) -> RedisCluster:
    client = RedisCluster.from_url(cluster_urls[0])
    yield client
    client.close()


def test_mset_mget_across_slots(rc: RedisCluster) -> None:
    # Keys designed to straddle multiple slots.
    pairs = {f"k{i}": f"v{i}".encode() for i in range(5)}
    rc.mset(pairs)
    out = rc.mget(list(pairs))
    assert out == list(pairs.values())


def test_mget_with_missing_keys_returns_none(rc: RedisCluster) -> None:
    rc.set("present", b"yes")
    out = rc.mget(["present", "missing-1", "missing-2"])
    assert out[0] == b"yes"
    assert out[1] is None
    assert out[2] is None


def test_del_across_slots_returns_count(rc: RedisCluster) -> None:
    rc.mset({f"d{i}": b"x" for i in range(4)})
    n = rc.delete(*[f"d{i}" for i in range(4)], "missing")
    assert n == 4


def test_exists_across_slots_counts_matches(rc: RedisCluster) -> None:
    rc.mset({"e1": b"x", "e2": b"x", "e3": b"x"})
    assert rc.exists("e1", "e2", "e3", "nope") == 3


def test_unlink_across_slots_returns_count(rc: RedisCluster) -> None:
    rc.mset({f"u{i}": b"x" for i in range(3)})
    assert rc.unlink(*[f"u{i}" for i in range(3)], "absent") == 3


def test_mset_empty_is_noop(rc: RedisCluster) -> None:
    rc.mset({})  # must not raise
    assert rc.mget([]) == []


def test_fanout_error_propagation_documented() -> None:
    """Edge-case error propagation is tested at unit level in Rust; here we
    document the behaviour contract and skip the Python-layer test."""
    pytest.skip("Edge-case error propagation covered by unit-level tests in Rust.")
