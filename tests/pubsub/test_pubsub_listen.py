"""listen() iterator + close() interaction."""

import threading
import time

import pytest

# Pub/sub semantics are server-global and timing-sensitive; serialise.
pytestmark = pytest.mark.xdist_group(name="pubsub_serial")


def test_listen_yields_messages(redis_facade, publisher) -> None:
    ps = redis_facade.pubsub(ignore_subscribe_messages=True)
    received: list[dict] = []

    def consume() -> None:
        for msg in ps.listen():
            received.append(msg)
            if len(received) >= 3:
                ps.close()

    t = threading.Thread(target=consume, daemon=True)
    t.start()

    # Give the subscribe time to land before publishing.
    ps.subscribe("evt")
    time.sleep(0.1)
    for i in range(3):
        publisher.publish("evt", f"m{i}".encode())

    t.join(timeout=5.0)
    assert not t.is_alive(), "listen() did not exit after close()"
    assert [m["data"] for m in received] == [b"m0", b"m1", b"m2"]


def test_listen_terminates_on_close(redis_facade) -> None:
    ps = redis_facade.pubsub()
    ps.subscribe("ch-empty")
    ps.get_message(timeout=2.0)  # drain confirm

    def closer() -> None:
        time.sleep(0.1)
        ps.close()

    threading.Thread(target=closer, daemon=True).start()

    seen = list(ps.listen())  # blocks until close() takes effect
    # close() empties the channel; we should not have hung forever.
    assert isinstance(seen, list)


def test_iter_is_self(redis_facade) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.subscribe("c")
        it = iter(ps)
        assert it is ps
    finally:
        ps.close()


def test_context_manager(redis_facade) -> None:
    with redis_facade.pubsub() as ps:
        ps.subscribe("c")
        msg = ps.get_message(timeout=2.0)
        assert msg["type"] == "subscribe"
    # Exiting the context manager should have closed the bridge.
    assert ps.subscribed is False
