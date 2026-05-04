"""ValkeyConnInner::Cluster and connect_cluster end-to-end smoke tests."""

from __future__ import annotations


def test_cluster_fixture_brings_up_three_masters(cluster_urls: list[str]) -> None:
    assert len(cluster_urls) == 3
    for url in cluster_urls:
        assert url.startswith("redis://")


def test_cluster_fixture_is_reachable(cluster_client_sync) -> None:
    cluster_client_sync.set("smoke", b"ok")
    assert cluster_client_sync.get("smoke") == b"ok"
