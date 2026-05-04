"""Cluster admin commands."""

from __future__ import annotations

import pytest
from redis_rs_py.cluster import RedisCluster


@pytest.fixture
def rc(cluster_urls: list[str]) -> RedisCluster:
    client = RedisCluster.from_url(cluster_urls[0])
    yield client
    client.close()


def test_cluster_info(rc: RedisCluster) -> None:
    out = rc.cluster_info()
    assert isinstance(out, (bytes, str))
    out_s = out.decode() if isinstance(out, bytes) else out
    assert "cluster_state:ok" in out_s
    assert "cluster_slots_assigned:16384" in out_s


def test_cluster_nodes(rc: RedisCluster) -> None:
    out = rc.cluster_nodes()
    assert isinstance(out, (bytes, str))
    out_s = out.decode() if isinstance(out, bytes) else out
    lines = [line for line in out_s.splitlines() if line.strip()]
    assert len(lines) == 6  # 3 masters + 3 replicas


def test_cluster_slots(rc: RedisCluster) -> None:
    out = rc.cluster_slots()
    assert isinstance(out, list)
    assert len(out) == 3  # 3 master slot ranges


def test_cluster_shards(rc: RedisCluster) -> None:
    out = rc.cluster_shards()
    assert isinstance(out, list)


def test_cluster_myid(rc: RedisCluster) -> None:
    out = rc.cluster_myid()
    assert isinstance(out, (bytes, str))
    out_s = out.decode() if isinstance(out, bytes) else out
    assert len(out_s) == 40


def test_cluster_keyslot(rc: RedisCluster) -> None:
    slot = rc.cluster_keyslot("foo")
    assert 0 <= slot < 16384


def test_cluster_countkeysinslot(rc: RedisCluster) -> None:
    rc.set("ck", b"v")
    slot = rc.cluster_keyslot("ck")
    n = rc.cluster_countkeysinslot(slot)
    assert n >= 1


def test_cluster_getkeysinslot(rc: RedisCluster) -> None:
    rc.set("findme", b"v")
    slot = rc.cluster_keyslot("findme")
    keys = rc.cluster_getkeysinslot(slot, 10)
    assert isinstance(keys, list)
    assert b"findme" in keys or "findme" in keys


def test_cluster_links(rc: RedisCluster) -> None:
    out = rc.cluster_links()
    assert isinstance(out, list)


def test_cluster_replicas_returns_list(rc: RedisCluster) -> None:
    out = rc.cluster_nodes()
    out_s = out.decode() if isinstance(out, bytes) else out
    master_id = next(line.split()[0] for line in out_s.splitlines() if "master" in line and "slave" not in line)
    replicas = rc.cluster_replicas(master_id)
    assert isinstance(replicas, list)


def test_cluster_failover_admin_command_exists(rc: RedisCluster) -> None:
    assert callable(rc.cluster_failover)
