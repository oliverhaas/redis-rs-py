"""List command tests — covers the full Plan 04 surface."""

from __future__ import annotations

import time

import pytest
from redis_rs_py.exceptions import DataError, ResponseError

# ---------- LPUSH / RPUSH / LPUSHX / RPUSHX ----------


def test_lpush_single(driver) -> None:
    assert driver.lpush("k", b"a") == 1
    assert driver.lpush("k", b"b") == 2
    assert driver.lrange("k", 0, -1) == [b"b", b"a"]


def test_lpush_variadic(driver) -> None:
    assert driver.lpush("k", b"a", b"b", b"c") == 3
    # LPUSH a b c → list is c, b, a (each pushed at head)
    assert driver.lrange("k", 0, -1) == [b"c", b"b", b"a"]


def test_rpush_variadic(driver) -> None:
    assert driver.rpush("k", b"a", b"b", b"c") == 3
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"c"]


def test_lpushx_when_missing_returns_zero(driver) -> None:
    assert driver.lpushx("missing", b"a") == 0
    assert driver.exists("missing") == 0


def test_lpushx_when_exists(driver) -> None:
    driver.lpush("k", b"a")
    assert driver.lpushx("k", b"b") == 2


def test_rpushx_when_missing_returns_zero(driver) -> None:
    assert driver.rpushx("missing", b"a") == 0


def test_rpushx_when_exists(driver) -> None:
    driver.rpush("k", b"a")
    assert driver.rpushx("k", b"b") == 2


def test_lpush_empty_args_raises_response_error(driver) -> None:
    # Redis itself rejects LPUSH with no values.
    with pytest.raises(ResponseError):
        driver.lpush("k")


@pytest.mark.asyncio
async def test_alpush_arpush_variadic(driver) -> None:
    assert await driver.alpush("k", b"a", b"b", b"c") == 3
    assert await driver.arpush("k", b"d") == 4
    assert await driver.alrange("k", 0, -1) == [b"c", b"b", b"a", b"d"]


@pytest.mark.asyncio
async def test_alpushx_arpushx(driver) -> None:
    assert await driver.alpushx("missing", b"a") == 0
    await driver.alpush("k", b"a")
    assert await driver.alpushx("k", b"b") == 2
    assert await driver.arpushx("k", b"c") == 3


# ---------- LPOP / RPOP / LRANGE / LLEN ----------


def test_lpop_single(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.lpop("k") == b"a"
    assert driver.lpop("k") == b"b"
    assert driver.lpop("k") == b"c"
    assert driver.lpop("k") is None


def test_lpop_with_count(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"d")
    assert driver.lpop("k", count=2) == [b"a", b"b"]
    assert driver.lpop("k", count=10) == [b"c", b"d"]
    assert driver.lpop("k", count=1) is None


def test_lpop_count_zero_returns_empty_list(driver) -> None:
    driver.rpush("k", b"a")
    assert driver.lpop("k", count=0) == []


def test_rpop_single(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.rpop("k") == b"c"
    assert driver.rpop("k") == b"b"


def test_rpop_with_count(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"d")
    assert driver.rpop("k", count=2) == [b"d", b"c"]


def test_lrange_full(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"c"]


def test_lrange_partial(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"d")
    assert driver.lrange("k", 1, 2) == [b"b", b"c"]


def test_lrange_missing_returns_empty(driver) -> None:
    assert driver.lrange("missing", 0, -1) == []


def test_llen(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.llen("k") == 3


def test_llen_missing_returns_zero(driver) -> None:
    assert driver.llen("missing") == 0


@pytest.mark.asyncio
async def test_alpop_arpop_with_count(driver) -> None:
    await driver.arpush("k", b"a", b"b", b"c", b"d")
    assert await driver.alpop("k") == b"a"
    assert await driver.alpop("k", count=2) == [b"b", b"c"]
    assert await driver.arpop("k") == b"d"
    assert await driver.alrange("k", 0, -1) == []
    assert await driver.allen("k") == 0


# ---------- LMOVE / LPOS ----------


def test_lmove_left_right(driver) -> None:
    driver.rpush("src", b"a", b"b", b"c")
    assert driver.lmove("src", "dst", "LEFT", "RIGHT") == b"a"
    assert driver.lrange("src", 0, -1) == [b"b", b"c"]
    assert driver.lrange("dst", 0, -1) == [b"a"]


def test_lmove_right_left(driver) -> None:
    driver.rpush("src", b"a", b"b", b"c")
    assert driver.lmove("src", "dst", "RIGHT", "LEFT") == b"c"
    assert driver.lrange("dst", 0, -1) == [b"c"]


def test_lmove_empty_source_returns_none(driver) -> None:
    assert driver.lmove("missing", "dst", "LEFT", "RIGHT") is None


def test_lpos_simple(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"b")
    assert driver.lpos("k", b"b") == 1
    assert driver.lpos("k", b"missing") is None


def test_lpos_with_rank(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"b")
    # RANK 2 = second match
    assert driver.lpos("k", b"b", rank=2) == 3


def test_lpos_with_count(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"b", b"b")
    # COUNT 0 = all matches
    assert driver.lpos("k", b"b", count=0) == [1, 3, 4]
    assert driver.lpos("k", b"b", count=2) == [1, 3]


def test_lpos_with_maxlen(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"b")
    # MAXLEN restricts the scan window from the head.
    assert driver.lpos("k", b"b", maxlen=2) == 1
    assert driver.lpos("k", b"c", maxlen=2) is None


@pytest.mark.asyncio
async def test_almove_alpos(driver) -> None:
    await driver.arpush("src", b"a", b"b", b"c")
    assert await driver.almove("src", "dst", "LEFT", "RIGHT") == b"a"
    assert await driver.alpos("dst", b"a") == 0
    assert await driver.alpos("src", b"missing") is None
    assert await driver.alpos("src", b"b", count=0) == [0]


# ---------- LREM / LINDEX / LSET / LINSERT / LTRIM ----------


def test_lrem_from_head(driver) -> None:
    driver.rpush("k", b"a", b"b", b"a", b"c", b"a")
    # count=2: remove first 2 from head
    assert driver.lrem("k", 2, b"a") == 2
    assert driver.lrange("k", 0, -1) == [b"b", b"c", b"a"]


def test_lrem_from_tail(driver) -> None:
    driver.rpush("k", b"a", b"b", b"a", b"c", b"a")
    # count=-1: remove first 1 from tail
    assert driver.lrem("k", -1, b"a") == 1
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"a", b"c"]


def test_lrem_all(driver) -> None:
    driver.rpush("k", b"a", b"b", b"a", b"c", b"a")
    assert driver.lrem("k", 0, b"a") == 3


def test_lindex(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.lindex("k", 0) == b"a"
    assert driver.lindex("k", -1) == b"c"
    assert driver.lindex("k", 99) is None


def test_lset(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    driver.lset("k", 1, b"B")
    assert driver.lrange("k", 0, -1) == [b"a", b"B", b"c"]


def test_lset_out_of_range_raises(driver) -> None:
    driver.rpush("k", b"a")
    with pytest.raises(ResponseError):
        driver.lset("k", 99, b"x")


def test_linsert_before(driver) -> None:
    driver.rpush("k", b"a", b"c")
    assert driver.linsert("k", "BEFORE", b"c", b"b") == 3
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"c"]


def test_linsert_after(driver) -> None:
    driver.rpush("k", b"a", b"c")
    assert driver.linsert("k", "AFTER", b"a", b"b") == 3
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"c"]


def test_linsert_pivot_missing_returns_minus_one(driver) -> None:
    driver.rpush("k", b"a")
    assert driver.linsert("k", "BEFORE", b"missing", b"x") == -1


def test_linsert_key_missing_returns_zero(driver) -> None:
    assert driver.linsert("missing", "BEFORE", b"a", b"x") == 0


def test_linsert_invalid_where_raises(driver) -> None:
    driver.rpush("k", b"a")
    with pytest.raises(DataError):
        driver.linsert("k", "MIDDLE", b"a", b"x")


def test_ltrim(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"d", b"e")
    driver.ltrim("k", 1, 3)
    assert driver.lrange("k", 0, -1) == [b"b", b"c", b"d"]


@pytest.mark.asyncio
async def test_alrem_alindex_alset_alinsert_altrim(driver) -> None:
    await driver.arpush("k", b"a", b"b", b"c")
    assert await driver.alindex("k", 0) == b"a"
    await driver.alset("k", 0, b"A")
    assert await driver.alindex("k", 0) == b"A"
    assert await driver.alinsert("k", "BEFORE", b"b", b"X") == 4
    assert await driver.alrem("k", 1, b"X") == 1
    await driver.altrim("k", 0, 1)
    assert await driver.alrange("k", 0, -1) == [b"A", b"b"]


# ---------- LMPOP ----------


def test_lmpop_first_non_empty(driver) -> None:
    driver.rpush("k1", b"a", b"b")
    driver.rpush("k2", b"c", b"d")
    assert driver.lmpop(["empty", "k1", "k2"], direction="LEFT") == ("k1", [b"a"])


def test_lmpop_with_count(driver) -> None:
    driver.rpush("k1", b"a", b"b", b"c", b"d")
    assert driver.lmpop(["k1"], direction="RIGHT", count=2) == ("k1", [b"d", b"c"])


def test_lmpop_all_empty_returns_none(driver) -> None:
    assert driver.lmpop(["empty1", "empty2"], direction="LEFT") is None


def test_lmpop_invalid_direction_raises(driver) -> None:
    driver.rpush("k", b"a")
    with pytest.raises(DataError):
        driver.lmpop(["k"], direction="MIDDLE")


@pytest.mark.asyncio
async def test_almpop(driver) -> None:
    await driver.arpush("k1", b"a", b"b", b"c")
    result = await driver.almpop(["empty", "k1"], direction="LEFT", count=2)
    assert result == ("k1", [b"a", b"b"])
    result = await driver.almpop(["empty"], direction="LEFT")
    assert result is None


# ---------- BLPOP / BRPOP / BLMOVE / BLMPOP ----------


def test_blpop_with_immediate_value(driver) -> None:
    driver.rpush("k", b"a")
    assert driver.blpop(["k"], timeout=0.1) == ("k", b"a")


def test_blpop_timeout_returns_none(driver) -> None:
    start = time.monotonic()
    assert driver.blpop(["empty"], timeout=0.2) is None
    assert time.monotonic() - start >= 0.15


def test_blpop_first_available_key(driver) -> None:
    driver.rpush("k2", b"x")
    assert driver.blpop(["k1", "k2"], timeout=0.1) == ("k2", b"x")


def test_brpop(driver) -> None:
    driver.rpush("k", b"a", b"b")
    assert driver.brpop(["k"], timeout=0.1) == ("k", b"b")


def test_blmove(driver) -> None:
    driver.rpush("src", b"a", b"b")
    assert driver.blmove("src", "dst", "LEFT", "RIGHT", timeout=0.1) == b"a"
    assert driver.lrange("dst", 0, -1) == [b"a"]


def test_blmove_timeout_returns_none(driver) -> None:
    assert driver.blmove("empty", "dst", "LEFT", "RIGHT", timeout=0.2) is None


def test_blmpop(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.blmpop(timeout=0.1, keys=["empty", "k"], direction="LEFT", count=2) == (
        "k",
        [b"a", b"b"],
    )


def test_blmpop_timeout_returns_none(driver) -> None:
    assert driver.blmpop(timeout=0.2, keys=["empty"], direction="LEFT", count=1) is None


@pytest.mark.asyncio
async def test_ablpop_abrpop(driver) -> None:
    await driver.arpush("k", b"a", b"b")
    assert await driver.ablpop(["k"], timeout=0.1) == (b"k", b"a")
    assert await driver.abrpop(["k"], timeout=0.1) == (b"k", b"b")
    assert await driver.ablpop(["empty"], timeout=0.1) is None


@pytest.mark.asyncio
async def test_ablmove_ablmpop(driver) -> None:
    await driver.arpush("src", b"a", b"b")
    assert await driver.ablmove("src", "dst", "LEFT", "RIGHT", timeout=0.1) == b"a"
    await driver.arpush("k", b"x", b"y", b"z")
    result = await driver.ablmpop(timeout=0.1, keys=["k"], direction="RIGHT", count=2)
    assert result == ("k", [b"z", b"y"])
