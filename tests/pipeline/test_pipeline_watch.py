"""WATCH / WatchError tests for the sync Pipeline."""

import threading

import pytest
from redis_rs_py.exceptions import WatchError


def test_watch_no_conflict_succeeds(client) -> None:
    """WATCH with no concurrent modification: EXEC succeeds."""
    client.set("watched_key", b"initial")
    with client.pipeline(transaction=True) as pipe:
        pipe.watch("watched_key")
        pipe.multi()
        pipe.set("watched_key", b"new_value")
        result = pipe.execute()
    assert result == [True]
    assert client.get("watched_key") == b"new_value"


def test_watch_with_conflict_raises_watch_error(client) -> None:
    """WATCH followed by a concurrent SET raises WatchError on execute()."""
    client.set("racekey", b"0")

    with client.pipeline(transaction=True) as pipe:
        pipe.watch("racekey")
        # Simulate a concurrent write between WATCH and MULTI/EXEC.
        client.set("racekey", b"dirty")
        pipe.multi()
        pipe.set("racekey", b"mine")
        with pytest.raises(WatchError):
            pipe.execute()


def test_watch_immediate_mode_get(client) -> None:
    """After watch(), commands before multi() are executed immediately."""
    client.set("imm", b"hello")
    with client.pipeline(transaction=True) as pipe:
        pipe.watch("imm")
        # In immediate mode, get() returns the real value, not the pipe.
        val = pipe.get("imm")
        assert val == b"hello"
        pipe.multi()
        pipe.set("imm", b"world")
        result = pipe.execute()
    assert result == [True]
    assert client.get("imm") == b"world"


def test_watch_unwatch_resets_state(client) -> None:
    """unwatch() clears the watched keys; subsequent execute() is clean."""
    client.set("uk", b"0")
    with client.pipeline(transaction=True) as pipe:
        pipe.watch("uk")
        pipe.unwatch()
        # After unwatch, pipe is back in buffering mode.
        pipe.set("uk", b"updated")
        result = pipe.execute()
    # Should succeed without WatchError.
    assert result == [True]


def test_watch_concurrent_thread_triggers_watch_error(client) -> None:
    """A thread modifying the watched key concurrently causes WatchError."""
    client.set("conc", b"0")
    barrier = threading.Barrier(2, timeout=5)
    errors: list[Exception] = []

    def watcher() -> None:
        try:
            with client.pipeline(transaction=True) as pipe:
                pipe.watch("conc")
                # Signal the modifier that WATCH has been sent.
                barrier.wait()
                # Wait for the modifier to finish dirtying the key.
                barrier.wait()
                pipe.multi()
                pipe.incr("conc")
                pipe.execute()
        except WatchError:
            errors.append(WatchError("expected"))
        except Exception as e:
            errors.append(e)

    def modifier() -> None:
        barrier.wait()
        client.set("conc", b"99")
        barrier.wait()

    t1 = threading.Thread(target=watcher)
    t2 = threading.Thread(target=modifier)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert len(errors) == 1
    assert isinstance(errors[0], WatchError)
