"""AsyncRedisCluster — async sibling of RedisCluster."""

import pytest
from redis_rs_py.asyncio.cluster import AsyncRedisCluster


@pytest.mark.asyncio
async def test_async_set_get(cluster_urls: list[str], cluster_client_sync) -> None:
    rc = AsyncRedisCluster.from_url(cluster_urls[0])
    await rc.set("ak", b"av")
    assert await rc.get("ak") == b"av"
    await rc.aclose()


@pytest.mark.asyncio
async def test_async_mget_fanout(cluster_urls: list[str], cluster_client_sync) -> None:
    rc = AsyncRedisCluster.from_url(cluster_urls[0])
    await rc.mset({f"a{i}": str(i).encode() for i in range(5)})
    out = await rc.mget([f"a{i}" for i in range(5)])
    assert out == [str(i).encode() for i in range(5)]
    await rc.aclose()


@pytest.mark.asyncio
async def test_async_delete_returns_count(cluster_urls: list[str], cluster_client_sync) -> None:
    rc = AsyncRedisCluster.from_url(cluster_urls[0])
    await rc.mset({"x": b"1", "y": b"2"})
    assert await rc.delete("x", "y", "z") == 2
    await rc.aclose()


@pytest.mark.asyncio
async def test_async_cluster_info(cluster_urls: list[str]) -> None:
    rc = AsyncRedisCluster.from_url(cluster_urls[0])
    info = await rc.cluster_info()
    assert b"cluster_state:ok" in info if isinstance(info, bytes) else "cluster_state:ok" in info
    await rc.aclose()


@pytest.mark.asyncio
async def test_async_context_manager(cluster_urls: list[str], cluster_client_sync) -> None:
    async with AsyncRedisCluster.from_url(cluster_urls[0]) as rc:
        await rc.set("ctx", b"val")
        assert await rc.get("ctx") == b"val"


@pytest.mark.asyncio
async def test_async_ping(cluster_urls: list[str]) -> None:
    rc = AsyncRedisCluster.from_url(cluster_urls[0])
    assert await rc.ping() is True
    await rc.aclose()
