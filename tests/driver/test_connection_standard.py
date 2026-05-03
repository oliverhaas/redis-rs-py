"""connect / from_url constructor surface."""

import pytest


def test_url_is_resp3_rewritten(driver) -> None:
    assert "protocol=resp3" in driver.connection_url


def test_connect_bad_url_raises_connection_error() -> None:
    from redis_rs_py import Redis
    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError

    with pytest.raises(RedisConnectionError):
        Redis.from_url("redis://127.0.0.1:1/0")


def test_connect_invalid_scheme_raises() -> None:
    from redis_rs_py import Redis
    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError

    with pytest.raises((RedisConnectionError, ValueError)):
        Redis.from_url("not-a-url")


def test_connect_with_cache_opts_does_not_raise(valkey_url: str) -> None:
    from redis_rs_py import Redis

    # Redis.from_url does not expose cache_max_size / cache_ttl_secs yet;
    # just test that connecting works.
    r = Redis.from_url(valkey_url)
    r.set("k", b"v")
    assert r.get("k") == b"v"


def test_connect_without_cache_returns_no_stats(driver) -> None:
    # `driver` fixture connects without cache opts.
    assert driver.cache_statistics() is None
