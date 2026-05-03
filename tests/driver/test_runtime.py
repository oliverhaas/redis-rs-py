"""Tests that cover the process-global tokio runtime singleton.

The runtime itself is opaque from Python — these tests exercise it
indirectly by constructing the test-helper awaitables (which all use
``get_runtime().spawn(...)``) and asserting they resolve.
"""

import asyncio
import os

import pytest
from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_runtime_resolves_simple_value() -> None:
    awaitable = _driver._test_resolved_int(42)  # noqa: SLF001
    assert await awaitable == 42


@pytest.mark.asyncio
async def test_runtime_resolves_after_spawn_blocking() -> None:
    awaitable = _driver._test_delayed_bytes(b"ok", 50)  # noqa: SLF001
    assert await awaitable == b"ok"


def test_runtime_survives_fork() -> None:
    """After fork, the parent runtime's threads are dead in the child.
    The PID-checked rebuild should produce a fresh runtime that resolves."""
    pid = os.fork()
    if pid == 0:
        # Child: must rebuild the runtime and the next call must succeed.
        async def _go() -> int:
            return await _driver._test_resolved_int(7)  # noqa: SLF001

        try:
            result = asyncio.run(_go())
            os._exit(0 if result == 7 else 1)
        except Exception:  # noqa: BLE001
            os._exit(2)
    else:
        _, status = os.waitpid(pid, 0)
        assert os.WIFEXITED(status), "child crashed"
        assert os.WEXITSTATUS(status) == 0, f"child reported failure: {os.WEXITSTATUS(status)}"
