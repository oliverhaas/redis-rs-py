"""Each redis-rs error kind translates to the right exception class.

Run live commands against testcontainers Valkey to exercise the
classifier in production conditions.
"""

import pytest
from redis_rs_py.exceptions import (
    ConnectionError as RedisConnectionError,
)


def test_wrongtype_raises_response_error(driver, valkey_url: str) -> None:
    driver.set("k", b"v")
    # LPUSH on a string key → WRONGTYPE. Sanity check via upstream client.
    # Use valkey_url (not driver.connection_url) — redis-py rejects the
    # `protocol=resp3` query parameter.
    import redis

    rp = redis.Redis.from_url(valkey_url)
    with pytest.raises(redis.exceptions.ResponseError):  # sanity check upstream
        rp.lpush("k", b"x")
    rp.close()
    # Driver-level WRONGTYPE test requires a list/hash command (plan 04).
    pytest.skip("WRONGTYPE driver-side test deferred to plan 04 (lists)")


def test_noscript_raises_noscript_error(driver) -> None:
    """EVALSHA against an unknown digest must raise NoScriptError."""
    pytest.skip("EVAL/EVALSHA land in plan 09; revisit when those exist")


def test_oom_raises_outofmemoryerror() -> None:
    """Set Valkey maxmemory to a tiny value, fill it, then SET → OOM.
    Skipped in CI to avoid the per-test container reconfigure cost."""
    pytest.skip("OOM live test gated; covered by classifier unit tests in Task 4")


def test_busy_loading_raises_busyloadingerror() -> None:
    """LOADING is only emitted right after the server starts and before
    RDB/AOF replay completes. Hard to trigger in-test; covered by the
    classifier unit test in Task 4."""
    pytest.skip("LOADING live test gated; covered by classifier unit tests in Task 4")


def test_auth_failure_raises_authentication_error(valkey_url: str) -> None:
    """If we connect with a bogus password to a passwordless server,
    Valkey replies with `ERR Client sent AUTH, but no password is set`.
    classify_error should pick that up via prefix sniff.

    Note: Valkey 8.0 without `requirepass` silently ignores AUTH on the
    `default` user, so this test is skipped until a password-protected
    container is added to the test matrix (tracked for plan 10).
    """
    pytest.skip(
        "Valkey 8.0 ignores AUTH for passwordless default user; auth branch covered by classifier unit tests in Task 4",
    )


def test_connect_to_dead_port_raises_connection_error() -> None:
    from redis_rs_py import Redis

    with pytest.raises(RedisConnectionError):
        Redis.from_url("redis://127.0.0.1:1/0")


def test_short_timeout_raises_timeout_error(valkey_url: str) -> None:
    """We don't yet expose `socket_timeout` at the driver level (lands in
    plan 10). For now, exercise the timeout path by calling DEBUG SLEEP via
    the upstream client and then using our PING with a connection that has
    response_timeout=30s — the expected behaviour is a clean PING (no
    timeout). This test is a placeholder marker for plan 10."""
    pytest.skip("socket_timeout exposure lands in plan 10")
