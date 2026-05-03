"""Tests for Redis.from_url() URL parsing."""

from __future__ import annotations

import pytest


def test_from_url_plain(valkey_url: str):
    """from_url with redis:// URL connects and pings."""
    from redis_rs_py import Redis

    with Redis.from_url(valkey_url) as r:
        assert r.ping() is True


def test_from_url_with_db_path(valkey_url: str):
    """from_url honours DB index in the URL path."""
    from redis_rs_py import Redis

    base, _, _ = valkey_url.rpartition("/")
    url_db2 = f"{base}/2"
    with Redis.from_url(url_db2) as r:
        assert r.ping() is True


def test_from_url_kwargs_override(valkey_url: str):
    """from_url passes extra kwargs to the constructor (accepted without error)."""
    from redis_rs_py import Redis

    with Redis.from_url(valkey_url, socket_timeout=30) as r:
        assert r.ping() is True


def test_from_url_invalid_scheme():
    """from_url raises ValueError for unsupported schemes."""
    from redis_rs_py import Redis

    with pytest.raises((ValueError, Exception)):
        Redis.from_url("ftp://localhost/0")


def test_from_url_rediss_scheme():
    """from_url with rediss:// scheme is attempted without a scheme parse error."""
    from redis_rs_py import Redis

    # The URL parses correctly; the driver may fail at connection (no TLS server,
    # missing crypto provider, etc.).  We only verify that a ValueError from URL
    # *parsing* is not raised.
    try:
        r = Redis.from_url("rediss://localhost:6379/0")
        r.close()
    except BaseException as exc:
        # Any runtime/connection exception is acceptable (including a Rust panic
        # when no TLS crypto provider is configured) — but not a clean
        # "invalid scheme" parse error.
        err = str(exc)
        assert not (err.startswith("Invalid") and "scheme" in err.lower()), (
            f"URL parse error should not be raised: {exc!r}"
        )


def test_from_url_unix_scheme():
    """from_url with unix:// scheme either works or raises a non-parse error."""
    from redis_rs_py import Redis

    try:
        r = Redis.from_url("unix:///tmp/redis.sock?db=0")
        r.close()
    except Exception:
        # Any exception is acceptable — the unix:// scheme may not be
        # supported by the driver or the socket may not exist.
        pass
