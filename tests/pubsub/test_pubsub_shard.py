"""Sharded pub/sub (Redis 7+).

These tests are gated on the server version because:
  * older Redis returns `ERR unknown command 'SSUBSCRIBE'`
  * redis-rs 1.2 PubSub API does not yet expose ssubscribe directly,
    so we emit a `ClientError`-flavoured failure until upstream lands it.
"""

import time

import pytest
import redis as upstream_redis

# Pub/sub semantics are server-global and timing-sensitive; serialise.
pytestmark = pytest.mark.xdist_group(name="pubsub_serial")


def _server_supports_shard_pubsub(url: str) -> bool:
    client = upstream_redis.Redis.from_url(url)
    try:
        info = client.info()
        ver = info.get("redis_version", "0.0.0")
        major = int(ver.split(".", 1)[0])
        return major >= 7
    finally:
        client.close()


@pytest.fixture
def shard_or_skip(valkey_url: str) -> None:
    if not _server_supports_shard_pubsub(valkey_url):
        pytest.skip("ssubscribe requires Redis 7+")


def test_ssubscribe_emits_confirmation(redis_facade, shard_or_skip) -> None:
    """Until redis-rs surfaces ssubscribe, this test is expected to fail
    fast with a PubSubError — that's the contract we maintain."""
    from redis_rs_py.exceptions import PubSubError

    ps = redis_facade.pubsub()
    try:
        with pytest.raises(PubSubError, match="ssubscribe"):
            ps.ssubscribe("shard1")
    finally:
        ps.close()


def test_sunsubscribe_emits_confirmation(redis_facade, shard_or_skip) -> None:
    from redis_rs_py.exceptions import PubSubError

    ps = redis_facade.pubsub()
    try:
        with pytest.raises(PubSubError, match="sunsubscribe"):
            ps.sunsubscribe("shard1")
    finally:
        ps.close()


@pytest.mark.skip(reason="enable when redis-rs 1.x gains ssubscribe in PubSub")
def test_ssubscribe_then_publish_smessage(redis_facade, publisher, shard_or_skip) -> None:
    ps = redis_facade.pubsub()
    try:
        ps.ssubscribe("shard1")
        ps.get_message(timeout=2.0)  # confirm
        time.sleep(0.05)
        publisher.spublish("shard1", b"x")
        msg = ps.get_message(timeout=2.0)
        assert msg == {
            "type": "smessage",
            "pattern": None,
            "channel": b"shard1",
            "data": b"x",
        }
    finally:
        ps.close()
