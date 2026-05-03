"""connect_standard constructor surface."""

import pytest


def test_url_is_resp3_rewritten(driver) -> None:
    assert "protocol=resp3" in driver.connection_url


def test_connect_standard_bad_url_raises_connection_error() -> None:
    from redis_rs_py._driver import RedisRsDriver  # noqa: PLC0415

    with pytest.raises(ConnectionError):
        RedisRsDriver.connect_standard("redis://127.0.0.1:1/0")


def test_connect_standard_invalid_scheme_raises() -> None:
    from redis_rs_py._driver import RedisRsDriver  # noqa: PLC0415

    with pytest.raises(ConnectionError):
        RedisRsDriver.connect_standard("not-a-url")


def test_connect_standard_with_cache_opts_does_not_raise(valkey_url: str) -> None:
    from redis_rs_py._driver import RedisRsDriver  # noqa: PLC0415

    drv = RedisRsDriver.connect_standard(valkey_url, cache_max_size=100, cache_ttl_secs=60)
    drv.set("k", b"v")
    assert drv.get("k") == b"v"
    # Read back twice; cache should report at least one hit.
    drv.get("k")
    stats = drv.cache_statistics()
    assert stats is not None  # client-side caching is enabled
    hits, misses, _invalidates = stats
    assert hits + misses > 0


def test_connect_standard_without_cache_returns_no_stats(driver) -> None:
    # `driver` fixture connects without cache opts.
    assert driver.cache_statistics() is None
