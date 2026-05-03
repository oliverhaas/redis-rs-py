"""Resolved-state RedisRsAwaitable helpers."""

import pytest
from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_resolved_bytes() -> None:
    assert await _driver._test_resolved_bytes(b"hello") == b"hello"  # noqa: SLF001


@pytest.mark.asyncio
async def test_resolved_none() -> None:
    assert await _driver._test_resolved_none() is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_resolved_int() -> None:
    assert await _driver._test_resolved_int(7) == 7  # noqa: SLF001


@pytest.mark.asyncio
async def test_resolved_int_negative() -> None:
    assert await _driver._test_resolved_int(-1) == -1  # noqa: SLF001
