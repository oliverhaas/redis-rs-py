"""Tests for Redis.__new__ / Redis.__init__ constructor behaviour."""


def test_default_construction():
    """Redis() with no args constructs without raising."""
    from redis_rs_py import Redis

    r = Redis()
    assert r is not None
    r.close()


def test_explicit_host_port():
    """host= and port= kwargs are accepted; construction should not raise a TypeError."""
    from redis_rs_py import Redis

    # Construction with an unreachable port should either succeed (lazy connect)
    # or raise a connection error — but never a TypeError / ValueError about
    # the kwargs themselves.
    try:
        r = Redis(host="127.0.0.1", port=6380)
        assert r is not None
        r.close()
    except Exception as exc:
        # Connection failure is acceptable; kwarg errors are not.
        assert not isinstance(exc, (TypeError, ValueError)), f"Constructor raised unexpected error: {exc!r}"


def test_db_kwarg_int():
    """db= as int is accepted."""
    from redis_rs_py import Redis

    r = Redis(db=3)
    assert r is not None
    r.close()


def test_db_kwarg_str():
    """db= as str is accepted (redis-py allows it)."""
    from redis_rs_py import Redis

    r = Redis(db="3")
    assert r is not None
    r.close()


def test_password_kwarg():
    """password= is accepted (connection may fail, but construction does not)."""
    from redis_rs_py import Redis

    r = Redis(password="secret")
    assert r is not None
    r.close()


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
