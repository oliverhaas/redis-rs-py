"""Scenario 7 — pubsub message rate.

One subscriber + one publisher in the same process. The publisher fires
N messages, the subscriber drains them. Each timed call publishes and
drains a full batch — we use ``benchmark.pedantic`` so each round runs
the batch exactly once (vs. the auto-calibrated inner-loop count, which
would multiply by the already-large MESSAGES_PER_BATCH).

Setup paths are wrapped in coroutine factories — ``RedisRsAwaitable``
binds to the event loop at construction time, so a bare
``run_until_complete(client.method(...))`` would build the awaitable
before the loop is running and trip a loop-mismatch error.

valkey-glide pubsub uses a push-model with callbacks. We use
``get_pubsub_message()`` polling in a tight loop as the nearest
structural equivalent to the redis-py ``get_message()`` pull model.
"""

import pytest

from benchmarks._helpers import (
    SMALL_VALUE,
    glide_async_client,
    py_async_client,
    py_client,
    rs_async_client,
)

CHANNEL = "bench:pubsub"
MESSAGES_PER_BATCH = 1000


# ---------------------------------------------------------------------------
# redis-rs-py
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="pubsub-1000")
def test_pubsub_redis_rs_py(benchmark, valkey_url, event_loop) -> None:
    # redis-rs-py does not yet expose publish() on the standard Redis client —
    # we use redis-py as the publisher. Fair: we are measuring the SUBSCRIBER
    # throughput of redis-rs-py; the publisher's client choice does not affect
    # the numbers.
    publisher = py_client(valkey_url)
    subscriber = rs_async_client(valkey_url)
    ps = subscriber.pubsub()

    async def _setup() -> None:
        await ps.subscribe(CHANNEL)
        # Drain the SUBSCRIBE confirmation message before timing.
        await ps.aget_message(timeout=1.0)

    async def _drain_batch() -> None:
        for _ in range(MESSAGES_PER_BATCH):
            publisher.publish(CHANNEL, SMALL_VALUE)
        received = 0
        while received < MESSAGES_PER_BATCH:
            msg = await ps.aget_message(timeout=5.0)
            if msg is not None and msg.get("type") == "message":
                received += 1

    async def _teardown() -> None:
        await ps.unsubscribe()
        await ps.aclose()
        await subscriber.aclose()

    event_loop.run_until_complete(_setup())
    try:
        benchmark.pedantic(
            lambda: event_loop.run_until_complete(_drain_batch()),
            iterations=1,
            rounds=10,
            warmup_rounds=1,
        )
    finally:
        event_loop.run_until_complete(_teardown())
        publisher.close()


# ---------------------------------------------------------------------------
# redis-py
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="pubsub-1000")
def test_pubsub_redis_py_hiredis(benchmark, valkey_url, event_loop) -> None:
    publisher = py_async_client(valkey_url)
    subscriber = py_async_client(valkey_url)
    ps = subscriber.pubsub()

    async def _setup() -> None:
        await ps.subscribe(CHANNEL)
        await ps.get_message(ignore_subscribe_messages=True, timeout=1.0)

    async def _drain_batch() -> None:
        for _ in range(MESSAGES_PER_BATCH):
            await publisher.publish(CHANNEL, SMALL_VALUE)
        received = 0
        while received < MESSAGES_PER_BATCH:
            msg = await ps.get_message(ignore_subscribe_messages=True, timeout=5.0)
            if msg is not None:
                received += 1

    async def _teardown() -> None:
        await ps.unsubscribe()
        await ps.aclose()
        await publisher.aclose()
        await subscriber.aclose()

    event_loop.run_until_complete(_setup())
    try:
        benchmark.pedantic(
            lambda: event_loop.run_until_complete(_drain_batch()),
            iterations=1,
            rounds=10,
            warmup_rounds=1,
        )
    finally:
        event_loop.run_until_complete(_teardown())


# ---------------------------------------------------------------------------
# valkey-glide
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="pubsub-1000")
def test_pubsub_valkey_glide(benchmark, valkey_url, event_loop) -> None:
    from urllib.parse import urlparse

    from glide import GlideClient, GlideClientConfiguration, NodeAddress

    pub_sub_channel_modes = GlideClientConfiguration.PubSubChannelModes
    pub_sub_subscriptions = GlideClientConfiguration.PubSubSubscriptions

    parsed = urlparse(valkey_url)
    sub_config = GlideClientConfiguration(
        addresses=[NodeAddress(host=parsed.hostname or "127.0.0.1", port=parsed.port or 6379)],
        pubsub_subscriptions=pub_sub_subscriptions(
            channels_and_patterns={pub_sub_channel_modes.Exact: {CHANNEL}},
            callback=None,
            context=None,
        ),
    )
    publisher = event_loop.run_until_complete(glide_async_client(valkey_url))
    subscriber = event_loop.run_until_complete(GlideClient.create(sub_config))

    async def _drain_batch() -> None:
        for _ in range(MESSAGES_PER_BATCH):
            await publisher.publish(SMALL_VALUE, CHANNEL)
        received = 0
        while received < MESSAGES_PER_BATCH:
            msg = await subscriber.get_pubsub_message()
            if msg is not None:
                received += 1

    try:
        benchmark.pedantic(
            lambda: event_loop.run_until_complete(_drain_batch()),
            iterations=1,
            rounds=10,
            warmup_rounds=1,
        )
    finally:
        event_loop.run_until_complete(publisher.close())
        event_loop.run_until_complete(subscriber.close())
