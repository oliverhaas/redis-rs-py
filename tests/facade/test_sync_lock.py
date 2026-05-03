"""Tests for the Redis.lock() / Lock distributed lock helper."""

from __future__ import annotations

import pytest


@pytest.fixture
def r(valkey_url: str):
    from redis_rs_py import Redis

    client = Redis.from_url(valkey_url)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


def test_lock_returns_lock_object(r):
    """Redis.lock() returns a Lock instance."""
    from redis_rs_py import Lock

    lk = r.lock("mylock")
    assert isinstance(lk, Lock)


def test_lock_acquire_release(r):
    """Lock can be acquired and released."""
    lk = r.lock("mylock", timeout=10)
    acquired = lk.acquire()
    assert acquired is True
    assert lk.owned() is True
    lk.release()
    assert lk.owned() is False


def test_lock_locked(r):
    """locked() returns True when the key exists in Redis."""
    lk = r.lock("mylock", timeout=10)
    assert lk.locked() is False
    lk.acquire()
    assert lk.locked() is True
    lk.release()
    assert lk.locked() is False


def test_lock_context_manager(r):
    """Lock can be used as a context manager."""
    with r.lock("mylock", timeout=10) as lk:
        assert lk.owned() is True
    assert lk.owned() is False


def test_lock_context_manager_releases_on_exception(r):
    """Lock is released even when the body raises."""
    lk = None
    with pytest.raises(RuntimeError), r.lock("mylock", timeout=10) as lk:
        raise RuntimeError("boom")
    assert lk is not None
    assert lk.owned() is False


def test_lock_extend(r):
    """extend() increases the TTL on a held lock."""
    lk = r.lock("mylock", timeout=5)
    lk.acquire()
    result = lk.extend(10)
    assert result is True
    ttl = r.pttl("mylock")
    assert ttl > 5000  # extended by 10 s → well above original 5 s
    lk.release()


def test_lock_nonblocking_fails_when_held(r):
    """A second non-blocking acquire on the same key returns False."""
    lk1 = r.lock("mylock", timeout=10)
    lk2 = r.lock("mylock", timeout=10)
    lk1.acquire()
    try:
        result = lk2.acquire(blocking=False)
        assert result is False
    finally:
        lk1.release()


def test_lock_release_not_owned_raises(r):
    """Releasing a lock not owned raises LockNotOwnedError."""
    from redis_rs_py.exceptions import LockNotOwnedError

    lk = r.lock("mylock", timeout=10)
    with pytest.raises(LockNotOwnedError):
        lk.release()
