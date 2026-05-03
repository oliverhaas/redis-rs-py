"""Fixtures for the pipeline test family.

Reuses the session-wide `valkey_url` from the top-level `tests/conftest.py`.
Each test that mutates Valkey uses flushdb via its own client.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def client(valkey_url: str):
    """High-level sync facade — used by tests that go through r.pipeline()."""
    from redis_rs_py import Redis

    r = Redis.from_url(valkey_url)
    import redis as _redis

    rp = _redis.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    return r


@pytest.fixture
async def aclient(valkey_url: str):
    """High-level async facade — used by tests that go through ar.pipeline()."""
    import redis as _redis
    from redis_rs_py.asyncio import Redis

    rp = _redis.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()

    ar = Redis.from_url(valkey_url)
    yield ar
    await ar.aclose()
