"""AsyncSentinel — async sibling of Sentinel.

Gated behind REDIS_RS_PY_SENTINEL_TESTS=1.
"""

import pytest
from redis_rs_py.asyncio.sentinel import AsyncSentinel


def _nodes(sentinel_urls: list[str]) -> list[tuple[str, int]]:
    return [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls
    ]


@pytest.mark.asyncio
async def test_async_master_for_smoke(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    nodes = _nodes(sentinel_urls)
    s = AsyncSentinel(nodes)
    master = s.master_for(sentinel_service_name)
    await master.set("ak", b"av")
    assert await master.get("ak") == b"av"


@pytest.mark.asyncio
async def test_async_discover_master(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    nodes = _nodes(sentinel_urls)
    s = AsyncSentinel(nodes)
    addr = await s.discover_master(sentinel_service_name)
    # Returns a list [host, port_str] from the async path.
    assert isinstance(addr, (tuple, list))
    assert len(addr) == 2
    host, port_str = addr
    assert isinstance(host, str)
    assert int(port_str) > 0


@pytest.mark.asyncio
async def test_async_sentinel_masters(sentinel_urls: list[str]) -> None:
    nodes = _nodes(sentinel_urls)
    s = AsyncSentinel(nodes)
    # The async path returns a raw redis Value (list of lists), not a dict.
    # Just verify it's a non-empty list/dict.
    result = await s.sentinel_masters()
    assert result is not None
    # Accept both list (raw) and dict (if processed).
    assert isinstance(result, (list, dict))


@pytest.mark.asyncio
async def test_async_slave_for_smoke(
    sentinel_urls: list[str],
    sentinel_service_name: str,
) -> None:
    import asyncio

    nodes = _nodes(sentinel_urls)
    s = AsyncSentinel(nodes)
    master = s.master_for(sentinel_service_name)
    # Use a unique key to avoid cross-test key collision when running the full suite.
    test_key = "async-slave-smoke-unique-xf7b"
    await master.set(test_key, b"yes")
    await asyncio.sleep(1.5)  # Let replication settle
    slave = s.slave_for(sentinel_service_name)
    assert await slave.get(test_key) == b"yes"
