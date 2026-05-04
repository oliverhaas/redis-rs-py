"""Pattern subscriptions deliver `pmessage` with the matching pattern in
the dict and the actual channel under `channel`."""

import time

import pytest


def test_psubscribe_then_publish_yields_pmessage(redis_facade, publisher) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.psubscribe("news.*")
        confirm = ps.get_message(timeout=2.0)
        assert confirm["type"] == "psubscribe"
        assert confirm["channel"] == b"news.*"

        time.sleep(0.05)
        publisher.publish("news.tech", b"announcement")

        msg = ps.get_message(timeout=2.0)
        assert msg == {
            "type": "pmessage",
            "pattern": b"news.*",
            "channel": b"news.tech",
            "data": b"announcement",
        }
    finally:
        ps.close()


def test_psubscribe_then_punsubscribe(redis_facade) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.psubscribe("a.*", "b.*")
        for _ in range(2):
            ps.get_message(timeout=2.0)
        ps.punsubscribe("a.*")
        confirm = ps.get_message(timeout=2.0)
        assert confirm["type"] == "punsubscribe"
        assert confirm["channel"] == b"a.*"
    finally:
        ps.close()


def test_psubscribe_no_args_raises_data_error(redis_facade) -> None:
    from redis_rs_py.exceptions import DataError

    ps = redis_facade.pubsub()
    try:
        with pytest.raises(DataError):
            ps.psubscribe()
    finally:
        ps.close()


def test_punsubscribe_all_when_empty(redis_facade) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.psubscribe("a.*", "b.*")
        for _ in range(2):
            ps.get_message(timeout=2.0)
        ps.punsubscribe()
        # Drain at least one confirmation.
        m = ps.get_message(timeout=2.0)
        assert m is not None
        assert m["type"] == "punsubscribe"
    finally:
        ps.close()
