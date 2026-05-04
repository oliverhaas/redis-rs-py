"""Tests for Redis lifecycle: close(), context-manager, post-close behaviour."""

import pytest


def test_close_releases_driver(valkey_url: str):
    """After close(), commands raise (driver is gone)."""
    from redis_rs_py import Redis

    r = Redis.from_url(valkey_url)
    assert r.ping() is True
    r.close()
    with pytest.raises(Exception):
        r.ping()


def test_context_manager_releases_on_exit(valkey_url: str):
    """with Redis.from_url(...) as r: ping works; after the block it raises."""
    from redis_rs_py import Redis

    with Redis.from_url(valkey_url) as r:
        assert r.ping() is True

    with pytest.raises(Exception):
        r.ping()


def test_context_manager_releases_on_exception(valkey_url: str):
    """__exit__ is called even when the body raises; subsequent ping raises too."""
    from redis_rs_py import Redis

    r_ref = None
    with pytest.raises(RuntimeError), Redis.from_url(valkey_url) as r:
        r_ref = r
        raise RuntimeError("boom")

    assert r_ref is not None
    with pytest.raises(Exception):
        r_ref.ping()


def test_double_close_is_safe(valkey_url: str):
    """Calling close() twice does not raise."""
    from redis_rs_py import Redis

    r = Redis.from_url(valkey_url)
    r.close()
    r.close()  # should be silent


def test_pipeline_returns_pipeline(valkey_url: str):
    """pipeline() returns a Pipeline object (implemented in Plan 13)."""
    from redis_rs_py import Pipeline, Redis

    with Redis.from_url(valkey_url) as r:
        pipe = r.pipeline()
        assert isinstance(pipe, Pipeline)


def test_pubsub_returns_pubsub(valkey_url: str):
    """pubsub() returns a PubSub instance (Plan 14)."""
    from redis_rs_py import PubSub, Redis

    with Redis.from_url(valkey_url) as r:
        ps = r.pubsub()
        assert isinstance(ps, PubSub)
        ps.close()


def test_transaction_runs_callable(valkey_url: str):
    """transaction() runs the callable and returns the result (Plan 13)."""
    from redis_rs_py import Redis

    with Redis.from_url(valkey_url) as r:
        r.set("tc_key", b"0")
        called: list[bool] = []

        def func(pipe) -> None:
            called.append(True)
            pipe.multi()
            pipe.set("tc_key", b"1")

        result = r.transaction(func, "tc_key")
        assert called == [True]
        assert result == [True]
