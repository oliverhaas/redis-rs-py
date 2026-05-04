"""Sync pub/sub: subscribe → publish from upstream client → get_message."""

import time

import pytest

# Pub/sub semantics are server-global and timing-sensitive; serialise.
pytestmark = pytest.mark.xdist_group(name="pubsub_serial")


def test_pubsub_constructed_via_facade(redis_facade) -> None:
    ps = redis_facade.pubsub()
    assert ps is not None
    ps.close()


def test_subscribe_returns_none(redis_facade) -> None:
    ps = redis_facade.pubsub()
    try:
        assert ps.subscribe("ch1") is None
    finally:
        ps.close()


def test_subscribe_confirmation_arrives_first(redis_facade) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.subscribe("ch1")
        msg = ps.get_message(timeout=2.0)
        assert msg is not None
        assert msg["type"] == "subscribe"
        assert msg["channel"] == b"ch1"
        assert msg["pattern"] is None
        assert msg["data"] == 1
    finally:
        ps.close()


def test_publish_then_get_message(redis_facade, publisher) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.subscribe("ch1")
        # Drain the subscribe confirmation.
        confirm = ps.get_message(timeout=2.0)
        assert confirm["type"] == "subscribe"

        # Allow the subscribe to land server-side.
        time.sleep(0.05)
        n = publisher.publish("ch1", b"hello")
        assert n == 1

        msg = ps.get_message(timeout=2.0)
        assert msg == {
            "type": "message",
            "pattern": None,
            "channel": b"ch1",
            "data": b"hello",
        }
    finally:
        ps.close()


def test_get_message_returns_none_on_timeout(redis_facade) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.subscribe("ch-quiet")
        # Eat the confirmation.
        ps.get_message(timeout=2.0)
        # Now no publishers; should time out cleanly.
        assert ps.get_message(timeout=0.2) is None
    finally:
        ps.close()


def test_subscribe_to_no_channels_raises_data_error(redis_facade) -> None:
    from redis_rs_py.exceptions import DataError

    ps = redis_facade.pubsub()
    try:
        with pytest.raises(DataError):
            ps.subscribe()
    finally:
        ps.close()


def test_unsubscribe_all_when_empty(redis_facade) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.subscribe("a", "b")
        # Drain 2 subscribe confirmations.
        for _ in range(2):
            ps.get_message(timeout=2.0)

        ps.unsubscribe()
        # Should produce at least one unsubscribe confirmation.
        kinds = []
        for _ in range(2):
            m = ps.get_message(timeout=2.0)
            if m:
                kinds.append(m["type"])
        assert kinds.count("unsubscribe") >= 1
    finally:
        ps.close()


def test_ignore_subscribe_messages(redis_facade, publisher) -> None:
    ps = redis_facade.pubsub(ignore_subscribe_messages=True)
    try:
        ps.subscribe("chx")
        time.sleep(0.05)
        publisher.publish("chx", b"x")
        msg = ps.get_message(timeout=2.0)
        assert msg["type"] == "message"
        assert msg["data"] == b"x"
    finally:
        ps.close()
