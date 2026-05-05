"""Shared client constructors and payload helpers for the bench suite.

Three clients, configured identically:

* ``rs_client(url)``         — redis-rs-py sync client
* ``rs_async_client(url)``   — redis-rs-py asyncio client
* ``py_client(url)``         — redis-py sync client (with hiredis parser)
* ``py_async_client(url)``   — redis-py asyncio client (with hiredis parser)
* ``glide_async_client(url)``— valkey-glide async client (no sync API)

Notes on fairness:

* All clients use ``decode_responses=False`` so we measure raw RESP, not
  bytes->str conversion.
* All clients use the default ``socket_keepalive=True`` and a single
  managed connection (max_connections=1 for redis-py, default pool for
  the other two — they multiplex internally on one socket).
* All clients use the same database (db=0).

valkey-glide availability: if the ``BENCH_SKIP_GLIDE`` env var is set,
``glide_async_client`` raises ``RuntimeError`` with a clear skip message.
This allows CI to gate glide scenarios without failing the whole suite.
"""

import os
from typing import Any

# ---------------------------------------------------------------------------
# Payload helpers — identical strings/bytes across every benchmark.
# ---------------------------------------------------------------------------

SMALL_VALUE: bytes = b"x" * 100
LARGE_VALUE: bytes = b"y" * 10_000
HOT_KEY: str = "bench:hot"
MGET_KEYS: list[str] = [f"bench:mget:{i}" for i in range(100)]
PIPELINE_KEYS: list[str] = [f"bench:pipe:{i}" for i in range(1000)]

# Display order mirrors the table in RESULTS.md.
CLIENT_ORDER: list[str] = ["redis-rs-py", "redis-py[hiredis]", "valkey-glide"]


# ---------------------------------------------------------------------------
# redis-rs-py
# ---------------------------------------------------------------------------


def rs_client(url: str) -> Any:
    """Construct a redis-rs-py sync client."""
    from redis_rs_py import Redis

    return Redis.from_url(url, decode_responses=False)


def rs_async_client(url: str) -> Any:
    """Construct a redis-rs-py asyncio client."""
    from redis_rs_py.asyncio import Redis as AsyncRedis

    return AsyncRedis.from_url(url, decode_responses=False)


# ---------------------------------------------------------------------------
# redis-py with hiredis parser
# ---------------------------------------------------------------------------


def py_client(url: str) -> Any:
    """Construct a redis-py sync client (hiredis parser, single conn)."""
    import redis

    return redis.Redis.from_url(url, decode_responses=False, max_connections=1)


def py_async_client(url: str) -> Any:
    """Construct a redis-py asyncio client (hiredis parser)."""
    import redis.asyncio as redis_async

    return redis_async.Redis.from_url(url, decode_responses=False)


# ---------------------------------------------------------------------------
# valkey-glide (async-only API)
# ---------------------------------------------------------------------------

#: Set this env var to any non-empty value to skip all valkey-glide scenarios.
#: Useful when glide wheels are unavailable for the current Python interpreter.
BENCH_SKIP_GLIDE = os.environ.get("BENCH_SKIP_GLIDE", "")


async def glide_async_client(url: str) -> Any:
    """Construct a valkey-glide client.

    glide-core is async-only, so this is an async constructor.

    Raises ``RuntimeError`` if ``BENCH_SKIP_GLIDE`` is set, so the bench
    suite can be run without glide installed.
    """
    if BENCH_SKIP_GLIDE:
        raise RuntimeError(
            "BENCH_SKIP_GLIDE is set — skipping valkey-glide scenarios. "
            "Install valkey-glide and unset BENCH_SKIP_GLIDE to include them.",
        )

    from urllib.parse import urlparse

    from glide import GlideClient, GlideClientConfiguration, NodeAddress

    parsed = urlparse(url)
    config = GlideClientConfiguration(
        addresses=[NodeAddress(host=parsed.hostname or "127.0.0.1", port=parsed.port or 6379)],
        database_id=int(parsed.path.lstrip("/") or 0),
    )
    return await GlideClient.create(config)


# ---------------------------------------------------------------------------
# Lifecycle helpers — identical seed across clients.
# ---------------------------------------------------------------------------


def seed_hot_key(url: str) -> None:
    """Seed the single hot key used by GET benchmarks."""
    import redis

    client = redis.Redis.from_url(url)
    client.set(HOT_KEY, SMALL_VALUE)
    client.close()


def seed_mget_keys(url: str) -> None:
    """Seed the 100 MGET keys."""
    import redis

    client = redis.Redis.from_url(url)
    client.mset(dict.fromkeys(MGET_KEYS, SMALL_VALUE))
    client.close()


def seed_pipeline_keys(url: str) -> None:
    """Seed the 1000 keys read by the pipeline benchmark."""
    import redis

    client = redis.Redis.from_url(url)
    pipe = client.pipeline(transaction=False)
    for k in PIPELINE_KEYS:
        pipe.set(k, SMALL_VALUE)
    pipe.execute()
    client.close()


def flush(url: str) -> None:
    """FLUSHDB between scenarios."""
    import redis

    client = redis.Redis.from_url(url)
    client.flushdb()
    client.close()


__all__ = [
    "BENCH_SKIP_GLIDE",
    "CLIENT_ORDER",
    "HOT_KEY",
    "LARGE_VALUE",
    "MGET_KEYS",
    "PIPELINE_KEYS",
    "SMALL_VALUE",
    "flush",
    "glide_async_client",
    "py_async_client",
    "py_client",
    "rs_async_client",
    "rs_client",
    "seed_hot_key",
    "seed_mget_keys",
    "seed_pipeline_keys",
]
