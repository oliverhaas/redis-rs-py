"""Paired-client fixtures for the parity test suite.

Two clients, same Valkey container, FLUSHDB before every test:

* ``rs_client`` — the redis-rs-py high-level facade
  (``redis_rs_py.Redis.from_url``).
* ``py_client`` — upstream redis-py
  (``redis.Redis.from_url``).

Both are constructed with ``decode_responses=False`` (the default and
the only mode the parity suite cares about — Plan 12 has its own
``decode_responses=True`` tests).

Why FLUSHDB before each test? Because the verifiers seed their own
fixtures and need a known-empty database. Doing it here, once, is
~10x faster than FLUSHDB-twice (once per client) per test.

Why xdist_group("redis_global_state")? Some commands (FLUSHALL, BGSAVE,
BGREWRITEAOF, SCRIPT FLUSH, FUNCTION FLUSH) operate on server-global state
(all databases, AOF file, Lua script cache). Merging with the existing
"redis_global_state" group used by test_commands_scripts.py and facade admin
tests ensures all global-state operations serialize onto one worker.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import redis as redis_py

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def py_client(valkey_url: str) -> Iterator[redis_py.Redis]:
    """Reference client (upstream redis-py)."""
    client = redis_py.Redis.from_url(valkey_url, decode_responses=False)
    client.flushdb()
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def rs_client(valkey_url: str) -> Iterator:
    """System under test (redis-rs-py facade)."""
    from redis_rs_py import Redis

    client = Redis.from_url(valkey_url, decode_responses=False)
    # Don't FLUSHDB again — py_client already did, and the two share a
    # database. Tests must request both fixtures (or the database is
    # whatever the previous test left behind, which is wrong).
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def paired_clients(rs_client, py_client) -> tuple:
    """Convenience tuple for verifiers: (rs, py)."""
    return rs_client, py_client
