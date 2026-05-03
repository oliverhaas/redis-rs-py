"""End-to-end smoke tests for the canonical 4 commands."""

import pytest


def test_set_get_sync(driver) -> None:
    driver.set("key", b"value")
    assert driver.get("key") == b"value"


def test_get_missing_returns_none(driver) -> None:
    assert driver.get("missing") is None


def test_delete_returns_count(driver) -> None:
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.delete("a", "b", "c") == 2


def test_ping(driver) -> None:
    assert driver.ping() is True


@pytest.mark.asyncio
async def test_aset_aget_async(driver) -> None:
    await driver.aset("k", b"v")
    assert await driver.aget("k") == b"v"


@pytest.mark.asyncio
async def test_aget_missing_returns_none(driver) -> None:
    assert await driver.aget("missing") is None


@pytest.mark.asyncio
async def test_adelete_returns_count(driver) -> None:
    await driver.aset("a", b"1")
    await driver.aset("b", b"2")
    assert await driver.adelete("a", "b", "c") == 2


@pytest.mark.asyncio
async def test_aping(driver) -> None:
    assert await driver.aping() is True


def test_set_with_ttl(driver) -> None:
    driver.set("key", b"value", ex=60)
    assert 0 < driver.ttl("key") <= 60


def test_connect_standard_bad_url_raises_connection_error() -> None:
    from redis_rs_py._driver import RedisRsDriver  # noqa: PLC0415
    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError  # noqa: PLC0415

    with pytest.raises(RedisConnectionError):
        RedisRsDriver.connect_standard("redis://127.0.0.1:1/0")
