"""Contract tests for the lazy blocking-connection split.

Why this exists:
  The regular ConnectionManager has a 30 s response timeout and is
  multiplexed — every command shares the same TCP pipeline. A 30 s BLPOP
  would freeze every other in-flight command. To prevent that, the driver
  lazily allocates a SECOND ConnectionManager (no response timeout) and
  routes BLPOP/BRPOP/BLMOVE/BLMPOP through it.

These tests pin that contract:
  1. The blocking conn does NOT exist before the first blocking call.
  2. The second blocking call reuses the SAME conn (no per-call init).
  3. A long BLPOP on one task does NOT block a concurrent GET on the same
     driver.
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest


def test_blocking_connection_not_initialised_before_use(driver) -> None:
    # Fresh driver — only regular commands have run.
    driver.set("k", b"v")
    driver.get("k")
    assert driver._blocking_initialised() is False  # noqa: SLF001


def test_blocking_connection_initialised_on_first_blpop(driver) -> None:
    assert driver._blocking_initialised() is False  # noqa: SLF001
    driver.rpush("k", b"a")
    driver.blpop(["k"], timeout=0.1)
    assert driver._blocking_initialised() is True  # noqa: SLF001


def test_blocking_connection_reused_across_calls(driver) -> None:
    driver.rpush("k", b"a")
    driver.blpop(["k"], timeout=0.1)
    assert driver._blocking_initialised() is True  # noqa: SLF001
    # Second call must not re-init — the OnceCell stays Some.
    driver.rpush("k", b"b")
    driver.blpop(["k"], timeout=0.1)
    assert driver._blocking_initialised() is True  # noqa: SLF001


def test_blocking_connection_initialised_on_first_brpop(driver) -> None:
    assert driver._blocking_initialised() is False  # noqa: SLF001
    driver.rpush("k", b"a")
    driver.brpop(["k"], timeout=0.1)
    assert driver._blocking_initialised() is True  # noqa: SLF001


def test_blocking_connection_initialised_on_first_blmove(driver) -> None:
    assert driver._blocking_initialised() is False  # noqa: SLF001
    driver.blmove("empty", "dst", "LEFT", "RIGHT", timeout=0.1)
    assert driver._blocking_initialised() is True  # noqa: SLF001


def test_blocking_connection_initialised_on_first_blmpop(driver) -> None:
    assert driver._blocking_initialised() is False  # noqa: SLF001
    driver.blmpop(timeout=0.1, keys=["empty"], direction="LEFT", count=1)
    assert driver._blocking_initialised() is True  # noqa: SLF001


async def _await_any(aw):
    """Wrap an awaitable (not a coroutine) so create_task accepts it."""
    return await aw


@pytest.mark.asyncio
async def test_long_blpop_does_not_block_concurrent_get(driver) -> None:
    """The big architectural payoff: a 1 s BLPOP on the blocking conn must
    NOT delay a GET on the regular conn. We measure wall-clock time on the
    GET to prove it."""
    # Start a BLPOP that will wait the full timeout (no key exists).
    blpop_task = asyncio.create_task(_await_any(driver.ablpop(["never_set"], timeout=1.0)))

    # Give the BLPOP a moment to enter the await.
    await asyncio.sleep(0.05)

    # Now race a GET. If the architectures share a pipeline, the GET will
    # wait for the BLPOP to finish (>=1.0 s). If they're properly split, the
    # GET completes in well under 200 ms.
    start = time.monotonic()
    await driver.aset("ping", b"pong")
    value = await driver.aget("ping")
    elapsed = time.monotonic() - start

    assert value == b"pong"
    assert elapsed < 0.5, (
        f"GET took {elapsed:.3f}s while BLPOP was in flight — head-of-line "
        f"blocking is back, the connection split is broken."
    )

    # Tidy up: cancel the still-pending BLPOP.
    result = await blpop_task
    assert result is None  # BLPOP timed out


def test_long_blpop_does_not_block_sync_get(driver) -> None:
    """Same contract, but sync <-> sync. Spawn the BLPOP via the runtime,
    then do a regular sync GET — must complete fast."""
    barrier = threading.Event()
    finished = threading.Event()

    def _runner():
        barrier.set()
        # Sync BLPOP that waits the full timeout.
        driver.blpop(["never_set"], timeout=1.0)
        finished.set()

    thread = threading.Thread(target=_runner)
    thread.start()
    barrier.wait()
    # Yield briefly so the BLPOP enters the await on the runtime.
    time.sleep(0.05)

    start = time.monotonic()
    driver.set("ping", b"pong")
    value = driver.get("ping")
    elapsed = time.monotonic() - start

    assert value == b"pong"
    assert elapsed < 0.5, (
        f"GET took {elapsed:.3f}s while BLPOP was in flight in another thread — head-of-line blocking is back."
    )

    thread.join()
    assert finished.is_set()
