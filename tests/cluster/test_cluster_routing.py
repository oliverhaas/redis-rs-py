"""Single-key routing — verify SET lands in the right slot."""

from __future__ import annotations

import pytest
from redis_rs_py.cluster import RedisCluster


@pytest.fixture
def rc(cluster_urls: list[str], cluster_client_sync) -> RedisCluster:
    client = RedisCluster.from_url(cluster_urls[0])
    yield client
    client.close()


def test_set_then_get_through_moved_redirect(rc: RedisCluster) -> None:
    """redis-rs cluster_async follows MOVED transparently."""
    for i in range(20):
        rc.set(f"k:{i}", str(i).encode())
        assert rc.get(f"k:{i}") == str(i).encode()


def test_keyslot_consistency(rc: RedisCluster, cluster_client_sync) -> None:
    """The slot we compute must match the slot upstream redis-py computes."""
    for k in ("a", "foo", "bar", "{tagged}.x", "{tagged}.y"):
        ours = rc.cluster_keyslot(k)
        # redis-py RedisCluster exposes keyslot()
        theirs = cluster_client_sync.keyslot(k)
        assert ours == theirs, f"slot mismatch for {k!r}: {ours} vs {theirs}"


def test_hashtag_co_locates(rc: RedisCluster) -> None:
    a = rc.cluster_keyslot("{user:42}.profile")
    b = rc.cluster_keyslot("{user:42}.session")
    assert a == b


def test_keys_cluster_caveat(cluster_urls: list[str]) -> None:
    """KEYS in cluster mode only returns keys on the queried master shard — document this."""
    rc = RedisCluster.from_url(cluster_urls[0])
    rc.set("a", b"1")
    rc.set("b", b"2")
    keys = rc.keys("*")
    assert isinstance(keys, list)
    # We don't assert full membership — cluster KEYS only covers one shard.
    rc.close()
