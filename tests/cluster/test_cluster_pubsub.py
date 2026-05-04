"""ClusterPubSub stub — minimal API surface tests."""

import pytest
from redis_rs_py.cluster import RedisCluster


@pytest.fixture
def rc(cluster_urls: list[str]) -> RedisCluster:
    client = RedisCluster.from_url(cluster_urls[0])
    yield client
    client.close()


def test_cluster_pubsub_handle_exists(rc: RedisCluster) -> None:
    """pubsub() returns a ClusterPubSub object (stub)."""
    ps = rc.pubsub()
    assert ps is not None


def test_cluster_pubsub_subscribe_raises_not_implemented(rc: RedisCluster) -> None:
    ps = rc.pubsub()
    with pytest.raises(NotImplementedError):
        ps.subscribe(["ch"])


def test_cluster_pubsub_close_is_noop(rc: RedisCluster) -> None:
    ps = rc.pubsub()
    ps.close()  # Should not raise
