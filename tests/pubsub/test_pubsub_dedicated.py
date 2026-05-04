"""Subscribing must not block the multiplexed pool.

A `pubsub()` call opens its own dedicated subscriber connection. The
same `Redis` instance must remain usable for normal commands AND for
blocking commands (which use the lazy second connection).
"""

import threading
import time


def test_normal_command_runs_with_active_subscription(
    redis_facade,
    publisher,
) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.subscribe("ch1")
        ps.get_message(timeout=2.0)  # confirm

        # Issue a regular GET/SET while the subscription is active.
        # If the subscription were sharing the connection, this would hang.
        redis_facade.set("k", b"v")
        assert redis_facade.get("k") == b"v"
    finally:
        ps.close()


def test_blpop_runs_with_active_subscription(redis_facade, publisher) -> None:
    """BLPOP uses the lazy second (blocking) connection. It must not be
    starved by the active pubsub subscription."""
    ps = redis_facade.pubsub()
    try:
        ps.subscribe("ch-side")
        ps.get_message(timeout=2.0)

        result_box: list = []

        def blocking_pop() -> None:
            v = redis_facade.blpop(["queue"], timeout=5)
            result_box.append(v)

        t = threading.Thread(target=blocking_pop, daemon=True)
        t.start()

        # Push to the queue from another connection.
        time.sleep(0.2)
        redis_facade.rpush("queue", b"hello")

        t.join(timeout=3.0)
        assert not t.is_alive(), "BLPOP starved by active subscription"
        # blpop returns (key, value); key may be str or bytes depending on decode_responses.
        key, val = result_box[0]
        assert key in (b"queue", "queue")
        assert val == b"hello"
    finally:
        ps.close()


def test_two_pubsubs_are_independent(redis_facade, publisher) -> None:
    """Two pubsub() calls return independent objects with independent
    underlying connections; messages on ps1 do not appear on ps2."""
    ps1 = redis_facade.pubsub()
    ps2 = redis_facade.pubsub()
    try:
        ps1.subscribe("c1")
        ps2.subscribe("c2")
        for ps in (ps1, ps2):
            ps.get_message(timeout=2.0)  # confirm

        time.sleep(0.05)
        publisher.publish("c1", b"x")

        msg = ps1.get_message(timeout=2.0)
        assert msg["channel"] == b"c1"
        assert msg["data"] == b"x"

        # ps2 must NOT have received c1.
        no_msg = ps2.get_message(timeout=0.2)
        assert no_msg is None
    finally:
        ps1.close()
        ps2.close()
