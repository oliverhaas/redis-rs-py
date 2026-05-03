"""Error and ServerError variants."""

import pytest
from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_error_raises_connection_error() -> None:
    aw = _driver._test_error("could not connect")  # noqa: SLF001
    with pytest.raises(ConnectionError, match="could not connect"):
        await aw


@pytest.mark.asyncio
async def test_server_error_raises_runtime_error() -> None:
    aw = _driver._test_server_error("WRONGTYPE")  # noqa: SLF001
    with pytest.raises(RuntimeError, match="WRONGTYPE"):
        await aw


@pytest.mark.asyncio
async def test_resolved_then_result_returns_value() -> None:
    aw = _driver._test_resolved_int(99)  # noqa: SLF001
    assert await aw == 99
    assert aw.result() == 99
    assert aw.exception() is None


@pytest.mark.asyncio
async def test_errored_then_exception_returns_exc() -> None:
    aw = _driver._test_error("boom")  # noqa: SLF001
    with pytest.raises(ConnectionError):
        await aw
    exc = aw.exception()
    assert isinstance(exc, ConnectionError)
