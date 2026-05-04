"""Async pub/sub: same surface as sync, but every method returns an
awaitable / async iterator."""

import asyncio

import pytest


@pytest.fixture
async def async_redis(valkey_url: str):
    from redis_rs_py.asyncio import Redis

    r = Redis.from_url(valkey_url)
    try:
        yield r
    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_async_subscribe_and_get_message(async_redis, publisher) -> None:
    ps = async_redis.pubsub()
    try:
        await ps.subscribe("ach")
        confirm = await ps.aget_message(timeout=2.0)
        assert confirm["type"] == "subscribe"
        assert confirm["channel"] == b"ach"

        await asyncio.sleep(0.05)
        publisher.publish("ach", b"async-hello")

        msg = await ps.aget_message(timeout=2.0)
        assert msg == {
            "type": "message",
            "pattern": None,
            "channel": b"ach",
            "data": b"async-hello",
        }
    finally:
        await ps.aclose()


@pytest.mark.asyncio
async def test_async_listen_iterator(async_redis, publisher) -> None:
    ps = async_redis.pubsub(ignore_subscribe_messages=True)
    received: list[dict] = []

    async def consume() -> None:
        async for msg in ps.listen():
            received.append(msg)
            if len(received) >= 3:
                await ps.aclose()

    await ps.subscribe("evt")
    await asyncio.sleep(0.1)

    consumer = asyncio.create_task(consume())
    for i in range(3):
        publisher.publish("evt", f"m{i}".encode())

    await asyncio.wait_for(consumer, timeout=5.0)
    assert [m["data"] for m in received] == [b"m0", b"m1", b"m2"]


@pytest.mark.asyncio
async def test_aget_message_timeout_returns_none(async_redis) -> None:
    ps = async_redis.pubsub()
    try:
        await ps.subscribe("quiet")
        await ps.aget_message(timeout=2.0)
        assert await ps.aget_message(timeout=0.2) is None
    finally:
        await ps.aclose()


@pytest.mark.asyncio
async def test_listen_responds_to_task_cancel(async_redis) -> None:
    """A pending listen() with no messages must respond to task.cancel()."""
    ps = async_redis.pubsub()
    await ps.subscribe("nothing-incoming")
    await ps.aget_message(timeout=2.0)  # drain confirm

    async def waiter():
        async for _ in ps.listen():
            pass

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await ps.aclose()


@pytest.mark.asyncio
async def test_async_psubscribe(async_redis, publisher) -> None:
    ps = async_redis.pubsub(ignore_subscribe_messages=True)
    try:
        await ps.psubscribe("news.*")
        await asyncio.sleep(0.1)
        publisher.publish("news.tech", b"announcement")

        msg = await ps.aget_message(timeout=2.0)
        assert msg["type"] == "pmessage"
        assert msg["pattern"] == b"news.*"
        assert msg["channel"] == b"news.tech"
        assert msg["data"] == b"announcement"
    finally:
        await ps.aclose()


@pytest.mark.asyncio
async def test_async_subscribe_no_args_raises_data_error(async_redis) -> None:
    from redis_rs_py.exceptions import DataError

    ps = async_redis.pubsub()
    try:
        with pytest.raises(DataError):
            await ps.subscribe()
    finally:
        await ps.aclose()
