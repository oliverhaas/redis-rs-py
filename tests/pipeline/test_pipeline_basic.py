"""Buffered-then-flushed pipeline semantics: chained calls, execute() returns list."""


def test_pipeline_set_get_returns_list(client) -> None:
    """Basic buffered pipeline: SET + GET returns a list."""
    with client.pipeline(transaction=False) as pipe:
        pipe.set("a", b"1")
        pipe.set("b", b"2")
        pipe.get("a")
        pipe.get("b")
        result = pipe.execute()
    assert result == [True, True, b"1", b"2"]


def test_pipeline_chained_returns_pipe(client) -> None:
    """Buffering mode: method calls return the pipeline itself (chainable)."""
    with client.pipeline(transaction=False) as pipe:
        ret = pipe.set("k", b"v")
        assert ret is pipe
        ret2 = pipe.get("k")
        assert ret2 is pipe


def test_pipeline_empty_execute_returns_empty_list(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        result = pipe.execute()
    assert result == []


def test_pipeline_len(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        assert len(pipe) == 0
        pipe.set("x", b"1")
        assert len(pipe) == 1
        pipe.get("x")
        assert len(pipe) == 2
        pipe.execute()
        assert len(pipe) == 0


def test_pipeline_incr(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        pipe.incr("counter")
        pipe.incr("counter")
        pipe.incr("counter")
        result = pipe.execute()
    assert result == [1, 2, 3]


def test_pipeline_hash_commands(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        pipe.hset("h", "f1", b"v1")
        pipe.hget("h", "f1")
        pipe.hlen("h")
        result = pipe.execute()
    assert result[0] == 1
    assert result[1] == b"v1"
    assert result[2] == 1


def test_pipeline_list_commands(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        pipe.rpush("lst", b"a", b"b", b"c")
        pipe.llen("lst")
        pipe.lrange("lst", 0, -1)
        result = pipe.execute()
    assert result[0] == 3
    assert result[1] == 3
    assert result[2] == [b"a", b"b", b"c"]


def test_pipeline_execute_clears_buffer(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        pipe.set("x", b"1")
        pipe.execute()
        assert len(pipe) == 0
        pipe.set("y", b"2")
        result = pipe.execute()
    assert result == [True]
