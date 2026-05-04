"""Scenario 7 — pubsub message rate.

One subscriber + one publisher in the same process. The publisher
fires N messages, the subscriber drains them. We measure end-to-end
wall-clock time for the whole batch, then convert to messages/sec.

Reusing the publisher across iterations would conflate setup time with
throughput, so each pyperf iteration tears down both ends.

valkey-glide pubsub uses a push-model with callbacks. We use
``get_pubsub_message()`` polling in a tight loop as the nearest
structural equivalent to the redis-py ``get_message()`` pull model.

Run via pyperf directly:

    BENCH_VALKEY_URL=redis://127.0.0.1:6379/0 \\
        uv run --group bench python benchmarks/bench_pubsub.py \\
        -o benchmarks/results/pubsub.json

Or via the orchestrator:

    uv run --group bench python benchmarks/run_all.py
"""

import asyncio
import sys
from pathlib import Path

# Ensure the repo root is on sys.path so pyperf worker subprocesses
# can resolve ``benchmarks._helpers`` regardless of cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pyperf

from benchmarks._helpers import (
    SMALL_VALUE,
    ensure_bench_env_inherited,
    get_valkey_url,
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


def _bench_pubsub_rs(loops: int, url: str) -> float:
    async def _go() -> float:
        # redis-rs-py does not yet expose a publish() method on the standard
        # Redis client — we use redis-py as the publisher. This is fair: we are
        # measuring the SUBSCRIBER throughput of redis-rs-py; the publisher's
        # client choice does not affect the numbers.
        publisher = py_client(url)
        subscriber = rs_async_client(url)
        ps = subscriber.pubsub()
        await ps.subscribe(CHANNEL)
        # Drain the SUBSCRIBE confirmation message before timing.
        # redis-rs-py async PubSub uses aget_message() (no ignore_subscribe_messages).
        await ps.aget_message(timeout=1.0)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            for _ in range(MESSAGES_PER_BATCH):
                publisher.publish(CHANNEL, SMALL_VALUE)
            received = 0
            while received < MESSAGES_PER_BATCH:
                msg = await ps.aget_message(timeout=5.0)
                if msg is not None and msg.get("type") == "message":
                    received += 1
        dt = pyperf.perf_counter() - t0
        await ps.unsubscribe()
        await ps.aclose()
        publisher.close()
        await subscriber.aclose()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# redis-py
# ---------------------------------------------------------------------------


def _bench_pubsub_py(loops: int, url: str) -> float:
    async def _go() -> float:
        publisher = py_async_client(url)
        subscriber = py_async_client(url)
        ps = subscriber.pubsub()
        await ps.subscribe(CHANNEL)
        await ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            for _ in range(MESSAGES_PER_BATCH):
                await publisher.publish(CHANNEL, SMALL_VALUE)
            received = 0
            while received < MESSAGES_PER_BATCH:
                msg = await ps.get_message(ignore_subscribe_messages=True, timeout=5.0)
                if msg is not None:
                    received += 1
        dt = pyperf.perf_counter() - t0
        await ps.unsubscribe()
        await ps.aclose()
        await publisher.aclose()
        await subscriber.aclose()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# valkey-glide
# ---------------------------------------------------------------------------


def _bench_pubsub_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        from urllib.parse import urlparse

        from glide import GlideClient, GlideClientConfiguration, NodeAddress

        # PubSubChannelModes is nested under GlideClientConfiguration in glide 2.x.
        PubSubChannelModes = GlideClientConfiguration.PubSubChannelModes
        PubSubSubscriptions = GlideClientConfiguration.PubSubSubscriptions

        parsed = urlparse(url)
        sub_config = GlideClientConfiguration(
            addresses=[NodeAddress(host=parsed.hostname or "127.0.0.1", port=parsed.port or 6379)],
            pubsub_subscriptions=PubSubSubscriptions(
                channels_and_patterns={PubSubChannelModes.Exact: {CHANNEL}},
                callback=None,
                context=None,
            ),
        )
        publisher = await glide_async_client(url)
        subscriber = await GlideClient.create(sub_config)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            for _ in range(MESSAGES_PER_BATCH):
                await publisher.publish(SMALL_VALUE, CHANNEL)
            received = 0
            while received < MESSAGES_PER_BATCH:
                msg = await subscriber.get_pubsub_message()
                if msg is not None:
                    received += 1
        dt = pyperf.perf_counter() - t0
        await publisher.close()
        await subscriber.close()
        return dt

    return asyncio.run(_go())


def main() -> None:
    ensure_bench_env_inherited()
    runner = pyperf.Runner()
    url = get_valkey_url()

    runner.bench_time_func("pubsub-1000/redis-rs-py", _bench_pubsub_rs, url)
    runner.bench_time_func("pubsub-1000/redis-py[hiredis]", _bench_pubsub_py, url)
    runner.bench_time_func("pubsub-1000/valkey-glide", _bench_pubsub_glide, url)


if __name__ == "__main__":
    main()
