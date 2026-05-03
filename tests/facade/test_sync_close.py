"""Tests for Redis lifecycle: close(), context-manager, post-close behaviour."""

from __future__ import annotations

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


def test_pipeline_stub_raises_not_implemented(valkey_url: str):
    """pipeline() raises NotImplementedError (not yet implemented)."""
    from redis_rs_py import Redis

    with Redis.from_url(valkey_url) as r, pytest.raises(NotImplementedError):
        r.pipeline()


def test_pubsub_stub_raises_not_implemented(valkey_url: str):
    """pubsub() raises NotImplementedError (not yet implemented)."""
    from redis_rs_py import Redis

    with Redis.from_url(valkey_url) as r, pytest.raises(NotImplementedError):
        r.pubsub()


def test_transaction_stub_raises_not_implemented(valkey_url: str):
    """transaction() raises NotImplementedError (not yet implemented)."""
    from redis_rs_py import Redis

    with Redis.from_url(valkey_url) as r, pytest.raises(NotImplementedError):
        r.transaction(lambda pipe: None)
