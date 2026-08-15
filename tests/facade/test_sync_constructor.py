"""Tests for Redis.__new__ / Redis.__init__ constructor behaviour."""

import pytest


def _assert_accepts_kwargs(**kwargs) -> None:
    """Assert `Redis(**kwargs)` accepts the kwargs; connection failure is tolerated."""
    from redis_rs_py import Redis

    try:
        r = Redis(**kwargs)
    except (TypeError, ValueError) as exc:
        pytest.fail(f"Constructor rejected kwargs {kwargs!r}: {exc!r}")
    except Exception:
        return
    assert r is not None
    r.close()


def test_default_construction():
    """Redis() with no args constructs without raising a kwarg error."""
    _assert_accepts_kwargs()


def test_explicit_host_port():
    """host= and port= kwargs are accepted; construction should not raise a TypeError."""
    _assert_accepts_kwargs(host="127.0.0.1", port=6380)


def test_db_kwarg_int(valkey_conn_kwargs):
    """db= as int is accepted."""
    from redis_rs_py import Redis

    r = Redis(**valkey_conn_kwargs, db=3)
    assert r.ping() is True
    r.close()


def test_db_kwarg_str(valkey_conn_kwargs):
    """db= as str is accepted (redis-py allows it)."""
    from redis_rs_py import Redis

    r = Redis(**valkey_conn_kwargs, db="3")
    assert r.ping() is True
    r.close()


def test_password_kwarg():
    """password= is accepted (the container has no auth, so AUTH itself fails)."""
    _assert_accepts_kwargs(password="secret")


def test_context_manager(valkey_url: str):
    """Redis can be used as a context manager."""
    from redis_rs_py import Redis

    with Redis.from_url(valkey_url) as r:
        result = r.ping()
        assert result is True


def test_close_idempotent(valkey_url: str):
    """close() can be called multiple times without raising."""
    from redis_rs_py import Redis

    r = Redis.from_url(valkey_url)
    r.close()
    r.close()


def test_ping_after_construction(valkey_url: str):
    """A freshly constructed Redis can ping the server."""
    from redis_rs_py import Redis

    r = Redis.from_url(valkey_url)
    try:
        assert r.ping() is True
    finally:
        r.close()
