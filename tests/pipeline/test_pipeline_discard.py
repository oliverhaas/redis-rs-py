"""discard() / reset() / close() tests for the sync Pipeline."""

from __future__ import annotations

import pytest
from redis_rs_py.exceptions import RedisError


def test_discard_clears_buffer(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        pipe.set("k", b"v")
        assert len(pipe) == 1
        pipe.discard()
        assert len(pipe) == 0
        result = pipe.execute()
    assert result == []


def test_reset_via_context_manager(client) -> None:
    """__exit__ calls reset(), clearing state."""
    with client.pipeline(transaction=False) as pipe:
        pipe.set("x", b"1")
    # Key should NOT be set — execute() was never called.
    assert client.get("x") is None


def test_close_prevents_further_use(client) -> None:
    pipe = client.pipeline(transaction=False)
    pipe.close()
    with pytest.raises(RedisError):
        pipe.execute()


def test_close_before_execute_does_not_write(client) -> None:
    pipe = client.pipeline(transaction=False)
    pipe.set("k", b"v")
    pipe.close()
    assert client.get("k") is None


def test_reset_after_watch_releases_reserved(client) -> None:
    """reset() after watch() sends UNWATCH and releases the reserved connection."""
    client.set("r", b"0")
    with client.pipeline(transaction=True) as pipe:
        pipe.watch("r")
        pipe.reset()
    # A subsequent regular write should work fine.
    client.set("r", b"1")
    assert client.get("r") == b"1"
