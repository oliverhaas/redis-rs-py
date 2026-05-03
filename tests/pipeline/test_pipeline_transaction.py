"""Pipeline MULTI/EXEC atomic block tests."""

from __future__ import annotations


def test_pipeline_transaction_true_is_atomic(client) -> None:
    """pipeline(transaction=True) wraps commands in MULTI/EXEC."""
    with client.pipeline(transaction=True) as pipe:
        pipe.set("x", b"10")
        pipe.incr("x")
        pipe.get("x")
        result = pipe.execute()
    assert result == [True, 11, b"11"]


def test_pipeline_transaction_false_not_wrapped(client) -> None:
    """pipeline(transaction=False) is a plain pipelined batch, no MULTI."""
    with client.pipeline(transaction=False) as pipe:
        pipe.set("a", b"1")
        pipe.set("b", b"2")
        result = pipe.execute()
    assert result == [True, True]


def test_pipeline_multi_then_execute(client) -> None:
    """Explicit multi() call followed by execute() uses MULTI/EXEC."""
    with client.pipeline(transaction=False) as pipe:
        pipe.multi()
        pipe.set("k", b"hello")
        pipe.get("k")
        result = pipe.execute()
    assert result == [True, b"hello"]


def test_pipeline_nested_multi_raises(client) -> None:
    """Calling multi() twice raises RedisError."""
    import pytest
    from redis_rs_py.exceptions import RedisError

    with client.pipeline(transaction=False) as pipe:
        pipe.multi()
        with pytest.raises(RedisError):
            pipe.multi()


def test_pipeline_multi_after_commands_without_watch_raises(client) -> None:
    """Buffering commands then calling multi() (without WATCH) raises."""
    import pytest
    from redis_rs_py.exceptions import RedisError

    with client.pipeline(transaction=False) as pipe:
        pipe.set("k", b"v")
        with pytest.raises(RedisError):
            pipe.multi()


def test_pipeline_transaction_returns_list_of_replies(client) -> None:
    """Each command's reply is an element in the returned list."""
    with client.pipeline(transaction=True) as pipe:
        for i in range(5):
            pipe.set(f"k{i}", str(i).encode())
        result = pipe.execute()
    assert len(result) == 5
    assert all(r is True for r in result)
