"""Reconnect-after-disconnect: when the dedicated subscriber connection
dies, the supervisor task rebuilds it and re-subscribes. Subsequent
publishes must still reach the consumer."""

import time

import pytest

# Pub/sub semantics are server-global and timing-sensitive; serialise.
pytestmark = pytest.mark.xdist_group(name="pubsub_serial")


def test_reconnect_after_client_kill(redis_facade, publisher) -> None:
    ps = redis_facade.pubsub(ignore_subscribe_messages=True, health_check_interval=1.0)
    try:
        ps.subscribe("ch")
        time.sleep(0.1)
        publisher.publish("ch", b"first")
        msg = ps.get_message(timeout=2.0)
        assert msg["data"] == b"first"

        # Kill the pubsub client. CLIENT KILL TYPE pubsub closes every
        # client that's currently subscribed to anything.
        killed = publisher.execute_command("CLIENT", "KILL", "TYPE", "pubsub")
        assert int(killed) >= 1

        # Give the supervisor a moment to reconnect and replay.
        time.sleep(2.0)

        publisher.publish("ch", b"after-reconnect")
        msg = ps.get_message(timeout=5.0)
        assert msg is not None
        assert msg["data"] == b"after-reconnect"
    finally:
        ps.close()


@pytest.mark.skip(
    reason="hard to assert health-check ping arrived without redis-cli MONITOR",
)
def test_health_check_keeps_connection_alive() -> None:
    pass
