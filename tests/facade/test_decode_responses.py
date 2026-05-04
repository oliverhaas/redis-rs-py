"""Integration tests for decode_responses=True mode.

Each test verifies that, when the facade is constructed with
``decode_responses=True``, bytes values coming back from the server are
transparently returned as str instead.

Tests use the live Valkey fixture.  Both the sync (Redis) and async
(asyncio.Redis) facades are exercised.
"""

import asyncio

import pytest


@pytest.fixture
def dr(valkey_url: str):
    """Sync Redis with decode_responses=True, flushed before each test."""
    from redis_rs_py import Redis

    client = Redis.from_url(valkey_url, decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def adr(valkey_url: str):
    """Async Redis with decode_responses=True, flushed before each test."""
    from redis_rs_py.asyncio import Redis as AsyncRedis

    return AsyncRedis.from_url(valkey_url, decode_responses=True)


# ---------------------------------------------------------------------------
# Helper: run a coroutine synchronously
# ---------------------------------------------------------------------------


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Sync: String commands
# ---------------------------------------------------------------------------


def test_sync_get_returns_str(dr):
    dr.set("k", b"hello")
    assert dr.get("k") == "hello"
    assert isinstance(dr.get("k"), str)


def test_sync_mget_returns_str_list(dr):
    dr.set("a", b"one")
    dr.set("b", b"two")
    result = dr.mget("a", "b")
    assert result == ["one", "two"]
    assert all(isinstance(v, str) for v in result)


def test_sync_getrange_returns_str(dr):
    dr.set("k", b"abcdef")
    result = dr.getrange("k", 0, 2)
    assert result == "abc"
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Sync: Hash commands
# ---------------------------------------------------------------------------


def test_sync_hget_returns_str(dr):
    dr.hset("h", mapping={"field": b"value"})
    result = dr.hget("h", "field")
    assert result == "value"
    assert isinstance(result, str)


def test_sync_hgetall_returns_str_dict(dr):
    dr.hset("h", mapping={"f1": b"v1", "f2": b"v2"})
    result = dr.hgetall("h")
    assert result == {"f1": "v1", "f2": "v2"}
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in result.items())


def test_sync_hkeys_hvals_return_str(dr):
    dr.hset("h", mapping={"f": b"v"})
    assert all(isinstance(k, str) for k in dr.hkeys("h"))
    assert all(isinstance(v, str) for v in dr.hvals("h"))


# ---------------------------------------------------------------------------
# Sync: List commands
# ---------------------------------------------------------------------------


def test_sync_lpop_returns_str(dr):
    dr.rpush("l", b"one", b"two")
    result = dr.lpop("l")
    assert result == "one"
    assert isinstance(result, str)


def test_sync_lrange_returns_str_list(dr):
    dr.rpush("l", b"a", b"b", b"c")
    result = dr.lrange("l", 0, -1)
    assert result == ["a", "b", "c"]
    assert all(isinstance(v, str) for v in result)


# ---------------------------------------------------------------------------
# Sync: Set commands
# ---------------------------------------------------------------------------


def test_sync_smembers_returns_str_set(dr):
    dr.sadd("s", b"x", b"y", b"z")
    result = dr.smembers("s")
    assert result == {"x", "y", "z"}
    assert all(isinstance(m, str) for m in result)


# ---------------------------------------------------------------------------
# Sync: Sorted set commands
# ---------------------------------------------------------------------------


def test_sync_zrange_returns_str_list(dr):
    dr.zadd("z", {b"a": 1, b"b": 2, b"c": 3})
    result = dr.zrange("z", 0, -1)
    assert result == ["a", "b", "c"]
    assert all(isinstance(v, str) for v in result)


def test_sync_zrange_withscores_member_is_str(dr):
    dr.zadd("z", {b"a": 1.0})
    result = dr.zrange("z", 0, -1, withscores=True)
    # Returns list of (member, score) tuples
    assert len(result) == 1
    member, score = result[0]
    assert member == "a"
    assert isinstance(member, str)
    assert isinstance(score, float)


# ---------------------------------------------------------------------------
# Sync: Admin commands
# ---------------------------------------------------------------------------


def test_sync_keys_returns_str_list(dr):
    import warnings

    dr.set("foo", b"bar")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        keys = dr.keys("*")
    assert all(isinstance(k, str) for k in keys)


def test_sync_randomkey_returns_str(dr):
    dr.set("k", b"v")
    rk = dr.randomkey()
    if rk is not None:
        assert isinstance(rk, str)


# ---------------------------------------------------------------------------
# Async: Basic decode check
# ---------------------------------------------------------------------------


def test_async_get_returns_str(adr, valkey_url):
    from redis_rs_py import Redis

    # Seed the key via sync client
    seed = Redis.from_url(valkey_url)
    seed.flushdb()
    seed.set("k", b"hello")
    seed.close()

    async def _run():
        return await adr.get("k")

    result = run(_run())
    assert result == "hello"
    assert isinstance(result, str)


def test_async_hgetall_returns_str_dict(adr, valkey_url):
    from redis_rs_py import Redis

    seed = Redis.from_url(valkey_url)
    seed.hset("h", mapping={"f1": b"v1", "f2": b"v2"})
    seed.close()

    async def _run():
        return await adr.hgetall("h")

    result = run(_run())
    assert result == {"f1": "v1", "f2": "v2"}
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in result.items())


def test_async_lrange_returns_str_list(adr, valkey_url):
    from redis_rs_py import Redis

    seed = Redis.from_url(valkey_url)
    seed.rpush("l", b"a", b"b")
    seed.close()

    async def _run():
        return await adr.lrange("l", 0, -1)

    result = run(_run())
    assert result == ["a", "b"]
    assert all(isinstance(v, str) for v in result)
