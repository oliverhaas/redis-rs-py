"""r.transaction() retry helper tests."""

from __future__ import annotations

import threading

import pytest


def test_transaction_basic(client) -> None:
    """transaction() runs the function and returns the execute() result."""
    client.set("tc", b"0")

    def func(pipe) -> None:
        pipe.multi()
        pipe.incr("tc")
        pipe.incr("tc")

    result = client.transaction(func, "tc")
    assert result == [1, 2]


def test_transaction_value_from_callable(client) -> None:
    """value_from_callable=True returns the callable's return value."""
    client.set("vfc", b"hello")

    def func(pipe):
        current = pipe.get("vfc")
        pipe.multi()
        pipe.set("vfc", b"world")
        return current  # immediate GET result

    result = client.transaction(func, "vfc", value_from_callable=True)
    assert result == b"hello"
    assert client.get("vfc") == b"world"


def test_transaction_retries_on_watch_error(client) -> None:
    """transaction() retries the function when a WatchError is raised."""
    client.set("retry_key", b"0")
    attempts: list[int] = [0]

    def func(pipe) -> None:
        attempts[0] += 1
        # On the first attempt, dirty the key to trigger WatchError.
        if attempts[0] == 1:
            client.set("retry_key", b"0")  # reset to valid integer value
        pipe.multi()
        pipe.set("retry_key", b"done")  # use SET to avoid integer constraints

    client.transaction(func, "retry_key")
    assert attempts[0] == 2
    assert client.get("retry_key") == b"done"


def test_transaction_propagates_non_watch_error(client) -> None:
    """Non-WatchError exceptions bubble up from transaction()."""

    def func(pipe) -> None:
        pipe.multi()
        raise ValueError("deliberate")

    with pytest.raises(ValueError, match="deliberate"):
        client.transaction(func)


def test_transaction_no_watches(client) -> None:
    """transaction() with no watch keys works as an unwatched MULTI/EXEC."""
    client.set("nw", b"0")

    def func(pipe) -> None:
        pipe.multi()
        pipe.incr("nw")

    result = client.transaction(func)
    assert result == [1]


def test_transaction_concurrent_retries(client) -> None:
    """Race between two threads: one will retry but both eventually succeed."""
    client.set("race", b"0")
    results: list[list] = [[], []]

    def worker(idx: int) -> None:
        def func(pipe) -> None:
            pipe.multi()
            pipe.incr("race")

        results[idx] = client.transaction(func, "race")

    t1 = threading.Thread(target=worker, args=(0,))
    t2 = threading.Thread(target=worker, args=(1,))
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    final = int(client.get("race"))
    assert final == 2
