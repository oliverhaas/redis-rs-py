"""Fixtures for the pubsub tests.

Spawns an upstream redis-py client to act as the publisher side, so we
can prove our PubSub receives messages without bootstrapping our own
publish path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import redis as upstream_redis

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def publisher(valkey_url: str) -> Iterator[upstream_redis.Redis]:
    """Upstream redis-py client used to PUBLISH messages to our subscribers."""
    client = upstream_redis.Redis.from_url(valkey_url)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def redis_facade(valkey_url: str):
    """A redis_rs_py.Redis instance bound to the test Valkey."""
    from redis_rs_py import Redis

    r = Redis.from_url(valkey_url)
    try:
        yield r
    finally:
        r.close()
