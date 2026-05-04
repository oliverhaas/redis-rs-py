"""Error-path tests for the sync Pipeline."""

import pytest
from redis_rs_py.exceptions import RedisError


def test_execute_after_close_raises(client) -> None:
    pipe = client.pipeline(transaction=False)
    pipe.close()
    with pytest.raises(RedisError):
        pipe.execute()


def test_watch_after_multi_raises(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        pipe.multi()
        with pytest.raises(RedisError):
            pipe.watch("k")


def test_multi_twice_raises(client) -> None:
    with client.pipeline(transaction=False) as pipe:
        pipe.multi()
        with pytest.raises(RedisError):
            pipe.multi()
