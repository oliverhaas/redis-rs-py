"""Shared client constructors and payload helpers for the bench suite.

Three clients, configured identically:

* ``rs_client(url)``     — redis-rs-py sync client
* ``rs_async_client(url)`` — redis-rs-py asyncio client
* ``py_client(url)``     — redis-py sync client (with hiredis parser)
* ``py_async_client(url)`` — redis-py asyncio client (with hiredis parser)
* ``glide_async_client(url)`` — valkey-glide async client (no sync API)

Notes on fairness:

* All clients use ``decode_responses=False`` so we measure raw RESP, not
  bytes->str conversion.
* All clients use the default ``socket_keepalive=True`` and a single
  managed connection (max_connections=1 for redis-py, default pool for
  the other two — they multiplex internally on one socket).
* All clients use the same database (db=0).
* The bench helpers do NOT reuse a Redis instance across pyperf
  iterations — each ``bench_func`` invocation gets a fresh client and
  closes it on teardown to prevent connection-state warmup from
  benefiting one client more than another.

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

    glide-core is async-only, so this is an async constructor. The bench
    code awaits this once at the start of ``bench_async_func`` setup.

    Raises ``RuntimeError`` (and prints a skip message) if ``BENCH_SKIP_GLIDE``
    is set, so the bench suite can be run without glide installed.
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
# Lifecycle helpers — identical seed + teardown across clients.
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


def get_valkey_url() -> str:
    """Resolve the Valkey URL the bench suite is currently pointed at.

    Set by ``conftest.py`` (when run via pytest) or via the
    ``BENCH_VALKEY_URL`` env var (when run via ``pyperf``-style direct
    invocation, which is how ``run_all.py`` calls each script).
    """
    url = os.environ.get("BENCH_VALKEY_URL")
    if url is None:
        raise RuntimeError(
            "BENCH_VALKEY_URL is not set; either run via "
            "`uv run python benchmarks/run_all.py` or set the env var "
            "manually before invoking a bench script.",
        )
    return url


# ---------------------------------------------------------------------------
# Async runner shim — pyperf's bench_async_func wants a callable returning
# an awaitable, not a coroutine. This helper packages the loop creation.
# ---------------------------------------------------------------------------


def make_async_runner(coro_factory: Any) -> Any:
    """Wrap ``coro_factory`` so each pyperf iteration gets a fresh task.

    ``coro_factory`` is a 0-arg callable returning a coroutine. The
    returned function is what pyperf calls per iteration. We do NOT
    create a new event loop per iteration (that's millisecond-scale
    overhead that would dominate fast scenarios) — the loop is bound by
    pyperf's ``Runner.bench_async_func`` itself.
    """

    async def _run() -> None:
        await coro_factory()

    return _run


# ---------------------------------------------------------------------------
# pyperf environment-inheritance helper
# ---------------------------------------------------------------------------


def ensure_bench_env_inherited() -> None:
    """Inject ``--copy-env`` into ``sys.argv`` when not already a pyperf worker.

    pyperf worker subprocesses are spawned with a minimal environment (only
    ``PATH``, ``HOME``, ``PYTHONPATH``, etc.) — ``BENCH_VALKEY_URL`` and the
    other bench-specific env vars are not forwarded by default.  Calling this
    function from a bench script's ``main()`` ensures the env is copied in full
    to every worker subprocess, without requiring callers to pass
    ``--copy-env`` on the CLI.

    Safe to call multiple times (idempotent).
    """
    import sys

    if "--worker" not in sys.argv and "--copy-env" not in sys.argv:
        sys.argv.insert(1, "--copy-env")


__all__ = [
    "BENCH_SKIP_GLIDE",
    "HOT_KEY",
    "LARGE_VALUE",
    "MGET_KEYS",
    "PIPELINE_KEYS",
    "SMALL_VALUE",
    "ensure_bench_env_inherited",
    "flush",
    "get_valkey_url",
    "glide_async_client",
    "make_async_runner",
    "py_async_client",
    "py_client",
    "rs_async_client",
    "rs_client",
    "seed_hot_key",
    "seed_mget_keys",
    "seed_pipeline_keys",
]
