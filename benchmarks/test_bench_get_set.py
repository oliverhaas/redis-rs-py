"""Scenarios 1-3 — sync GET / SET / MGET across the three clients.

Each scenario is a parametrized test, one row per client. The scenario
name (the ``group=`` kwarg on the ``@pytest.mark.benchmark`` marker) is
what ``run_all.py`` aggregates by when rendering RESULTS.md.

valkey-glide has no sync API, so its sync scenarios run via
``loop.run_until_complete(coro)`` inside the timed callable. The
shared module-scoped ``event_loop`` fixture amortises loop construction.
"""

import pytest

from benchmarks._helpers import (
    HOT_KEY,
    MGET_KEYS,
    SMALL_VALUE,
    glide_async_client,
    py_client,
    rs_client,
)

# ---------------------------------------------------------------------------
# Scenario 1: hot-key GET
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="get")
def test_get_redis_rs_py(benchmark, hot_key) -> None:
    client = rs_client(hot_key)
    try:
        benchmark(client.get, HOT_KEY)
    finally:
        client.close()


@pytest.mark.benchmark(group="get")
def test_get_redis_py_hiredis(benchmark, hot_key) -> None:
    client = py_client(hot_key)
    try:
        benchmark(client.get, HOT_KEY)
    finally:
        client.close()


@pytest.mark.benchmark(group="get")
def test_get_valkey_glide(benchmark, hot_key, event_loop) -> None:
    client = event_loop.run_until_complete(glide_async_client(hot_key))
    try:
        benchmark(lambda: event_loop.run_until_complete(client.get(HOT_KEY)))
    finally:
        event_loop.run_until_complete(client.close())


# ---------------------------------------------------------------------------
# Scenario 2: SET small value
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="set")
def test_set_redis_rs_py(benchmark, flushed_db) -> None:
    client = rs_client(flushed_db)
    try:
        counter = iter(range(10**9))
        benchmark(lambda: client.set(f"bench:set:{next(counter)}", SMALL_VALUE))
    finally:
        client.close()


@pytest.mark.benchmark(group="set")
def test_set_redis_py_hiredis(benchmark, flushed_db) -> None:
    client = py_client(flushed_db)
    try:
        counter = iter(range(10**9))
        benchmark(lambda: client.set(f"bench:set:{next(counter)}", SMALL_VALUE))
    finally:
        client.close()


@pytest.mark.benchmark(group="set")
def test_set_valkey_glide(benchmark, flushed_db, event_loop) -> None:
    client = event_loop.run_until_complete(glide_async_client(flushed_db))
    try:
        counter = iter(range(10**9))
        benchmark(lambda: event_loop.run_until_complete(client.set(f"bench:set:{next(counter)}", SMALL_VALUE)))
    finally:
        event_loop.run_until_complete(client.close())


# ---------------------------------------------------------------------------
# Scenario 3: MGET 100 keys
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="mget")
def test_mget_redis_rs_py(benchmark, mget_keys) -> None:
    client = rs_client(mget_keys)
    try:
        benchmark(client.mget, MGET_KEYS)
    finally:
        client.close()


@pytest.mark.benchmark(group="mget")
def test_mget_redis_py_hiredis(benchmark, mget_keys) -> None:
    client = py_client(mget_keys)
    try:
        benchmark(client.mget, MGET_KEYS)
    finally:
        client.close()


@pytest.mark.benchmark(group="mget")
def test_mget_valkey_glide(benchmark, mget_keys, event_loop) -> None:
    client = event_loop.run_until_complete(glide_async_client(mget_keys))
    try:
        benchmark(lambda: event_loop.run_until_complete(client.mget(MGET_KEYS)))
    finally:
        event_loop.run_until_complete(client.close())
