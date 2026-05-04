"""RedisCluster constructor + smoke commands."""

import pytest
from redis_rs_py.cluster import ClusterNode, RedisCluster


def test_construct_from_startup_nodes(cluster_urls: list[str]) -> None:
    nodes = []
    for url in cluster_urls:
        host, port = url.removeprefix("redis://").split(":", 1)
        nodes.append(ClusterNode(host=host, port=int(port)))
    rc = RedisCluster(startup_nodes=nodes)
    assert rc.ping() is True
    rc.close()


def test_construct_from_host_port(cluster_urls: list[str]) -> None:
    host, port = cluster_urls[0].removeprefix("redis://").split(":", 1)
    rc = RedisCluster(host=host, port=int(port))
    assert rc.ping() is True
    rc.close()


def test_construct_from_url(cluster_urls: list[str]) -> None:
    rc = RedisCluster.from_url(cluster_urls[0])
    assert rc.ping() is True
    rc.close()


def test_set_get_via_cluster(cluster_urls: list[str], cluster_client_sync) -> None:
    rc = RedisCluster.from_url(cluster_urls[0])
    rc.set("hello", b"world")
    assert rc.get("hello") == b"world"
    rc.close()


def test_set_get_delete_exists_single_key(
    cluster_urls: list[str],
    cluster_client_sync,
) -> None:
    rc = RedisCluster.from_url(cluster_urls[0])
    rc.set("k", b"v")
    assert rc.get("k") == b"v"
    assert rc.exists("k") == 1
    assert rc.delete("k") == 1
    assert rc.get("k") is None
    rc.close()


def test_incr_decr(cluster_urls: list[str], cluster_client_sync) -> None:
    rc = RedisCluster.from_url(cluster_urls[0])
    assert rc.incr("counter") == 1
    assert rc.incrby("counter", 5) == 6
    assert rc.decr("counter") == 5
    rc.close()


def test_hash_commands(cluster_urls: list[str], cluster_client_sync) -> None:
    rc = RedisCluster.from_url(cluster_urls[0])
    rc.hset("h", "f", b"v")
    assert rc.hget("h", "f") == b"v"
    assert rc.hgetall("h") == {b"f": b"v"}
    rc.close()


def test_list_commands(cluster_urls: list[str], cluster_client_sync) -> None:
    rc = RedisCluster.from_url(cluster_urls[0])
    rc.rpush("l", b"a", b"b", b"c")
    assert rc.lrange("l", 0, -1) == [b"a", b"b", b"c"]
    rc.close()


def test_zset_commands(cluster_urls: list[str], cluster_client_sync) -> None:
    rc = RedisCluster.from_url(cluster_urls[0])
    rc.zadd("z", {b"x": 1.0, b"y": 2.0})
    assert rc.zrange("z", 0, -1) == [b"x", b"y"]
    rc.close()


def test_context_manager(cluster_urls: list[str]) -> None:
    with RedisCluster.from_url(cluster_urls[0]) as rc:
        rc.set("ctx", b"ok")
        assert rc.get("ctx") == b"ok"


def test_unknown_kwarg_warns_once_per_process(cluster_urls: list[str]) -> None:
    import warnings

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        rc = RedisCluster.from_url(cluster_urls[0], some_unknown_kwarg=42)
        rc.close()
    assert any("some_unknown_kwarg" in str(rec.message) for rec in w)


def test_cache_kwargs_warn_on_cluster(cluster_urls: list[str]) -> None:
    """Client-side caching is not supported on cluster — warn the user."""
    import warnings

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        rc = RedisCluster.from_url(cluster_urls[0], cache_max_size=100, cache_ttl_secs=60)
        rc.close()
    msgs = [str(rec.message) for rec in w]
    assert any("cluster" in m.lower() for m in msgs)


def test_no_host_no_url_no_startup_nodes_raises() -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError):
        RedisCluster()
