"""Set command coverage on Redis/AsyncRedis — Plan 06."""

from __future__ import annotations

import pytest

# --- SADD ----------------------------------------------------------------


def test_sadd_returns_added_count(driver) -> None:
    assert driver.sadd("s", b"a", b"b", b"c") == 3
    # Re-adding existing members → 0 newly added.
    assert driver.sadd("s", b"a", b"b") == 0
    # Mix of new and existing.
    assert driver.sadd("s", b"a", b"d") == 1


def test_sadd_empty_members_returns_zero(driver) -> None:
    assert driver.sadd("s") == 0


@pytest.mark.asyncio
async def test_asadd(driver) -> None:
    assert await driver.asadd("s", b"x", b"y") == 2


# --- SREM ----------------------------------------------------------------


def test_srem_returns_removed_count(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    assert driver.srem("s", b"a", b"missing") == 1
    assert driver.srem("s", b"b", b"c") == 2


def test_srem_missing_key_returns_zero(driver) -> None:
    assert driver.srem("missing", b"a") == 0


@pytest.mark.asyncio
async def test_asrem(driver) -> None:
    await driver.asadd("s", b"a", b"b")
    assert await driver.asrem("s", b"a") == 1


# --- SMEMBERS ------------------------------------------------------------


def test_smembers_returns_python_set(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    got = driver.smembers("s")
    assert isinstance(got, set)
    assert got == {b"a", b"b", b"c"}


def test_smembers_missing_returns_empty_set(driver) -> None:
    got = driver.smembers("missing")
    assert isinstance(got, set)
    assert got == set()


@pytest.mark.asyncio
async def test_asmembers(driver) -> None:
    await driver.asadd("s", b"x", b"y")
    got = await driver.asmembers("s")
    assert isinstance(got, set)
    assert got == {b"x", b"y"}


# --- SCARD ---------------------------------------------------------------


def test_scard(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    assert driver.scard("s") == 3


def test_scard_missing_is_zero(driver) -> None:
    assert driver.scard("missing") == 0


@pytest.mark.asyncio
async def test_ascard(driver) -> None:
    await driver.asadd("s", b"a")
    assert await driver.ascard("s") == 1


# --- SISMEMBER -----------------------------------------------------------


def test_sismember_present(driver) -> None:
    driver.sadd("s", b"a", b"b")
    assert driver.sismember("s", b"a") is True


def test_sismember_absent(driver) -> None:
    driver.sadd("s", b"a")
    assert driver.sismember("s", b"missing") is False
    assert driver.sismember("missing-key", b"a") is False


@pytest.mark.asyncio
async def test_asismember(driver) -> None:
    await driver.asadd("s", b"a")
    assert await driver.asismember("s", b"a") is True


# --- SMISMEMBER ----------------------------------------------------------


def test_smismember_returns_list_of_bools(driver) -> None:
    driver.sadd("s", b"a", b"c")
    got = driver.smismember("s", b"a", b"b", b"c")
    assert isinstance(got, list)
    assert got == [True, False, True]


def test_smismember_missing_key(driver) -> None:
    assert driver.smismember("missing", b"a", b"b") == [False, False]


def test_smismember_empty_members_returns_empty_list(driver) -> None:
    driver.sadd("s", b"a")
    assert driver.smismember("s") == []


@pytest.mark.asyncio
async def test_asmismember(driver) -> None:
    await driver.asadd("s", b"a")
    assert await driver.asmismember("s", b"a", b"b") == [True, False]


# --- SPOP ----------------------------------------------------------------


def test_spop_no_count_returns_single_bytes(driver) -> None:
    driver.sadd("s", b"only")
    got = driver.spop("s")
    assert got == b"only"
    assert driver.scard("s") == 0


def test_spop_no_count_missing_returns_none(driver) -> None:
    assert driver.spop("missing") is None


def test_spop_with_count_returns_set(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    got = driver.spop("s", count=2)
    assert isinstance(got, set)
    assert len(got) == 2
    assert got.issubset({b"a", b"b", b"c"})
    assert driver.scard("s") == 1


def test_spop_with_count_zero_returns_empty_set(driver) -> None:
    driver.sadd("s", b"a")
    got = driver.spop("s", count=0)
    assert isinstance(got, set)
    assert got == set()


def test_spop_with_count_more_than_size(driver) -> None:
    driver.sadd("s", b"a", b"b")
    got = driver.spop("s", count=10)
    assert isinstance(got, set)
    assert got == {b"a", b"b"}


@pytest.mark.asyncio
async def test_aspop(driver) -> None:
    await driver.asadd("s", b"x")
    assert await driver.aspop("s") == b"x"


@pytest.mark.asyncio
async def test_aspop_with_count(driver) -> None:
    await driver.asadd("s", b"a", b"b")
    got = await driver.aspop("s", count=1)
    assert isinstance(got, set) and len(got) == 1


# --- SRANDMEMBER ---------------------------------------------------------


def test_srandmember_no_count_returns_single_bytes(driver) -> None:
    driver.sadd("s", b"a", b"b")
    got = driver.srandmember("s")
    assert got in (b"a", b"b")
    assert driver.scard("s") == 2  # SRANDMEMBER does not pop


def test_srandmember_no_count_missing_returns_none(driver) -> None:
    assert driver.srandmember("missing") is None


def test_srandmember_with_positive_count_returns_distinct_set(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    got = driver.srandmember("s", count=2)
    assert isinstance(got, set)
    assert len(got) == 2  # distinct
    assert got.issubset({b"a", b"b", b"c"})


def test_srandmember_with_negative_count_returns_list_with_repeats(driver) -> None:
    driver.sadd("s", b"only")
    got = driver.srandmember("s", count=-3)
    assert isinstance(got, list)
    assert got == [b"only", b"only", b"only"]


@pytest.mark.asyncio
async def test_asrandmember(driver) -> None:
    await driver.asadd("s", b"a")
    assert await driver.asrandmember("s") == b"a"
    assert await driver.asrandmember("s", count=1) == {b"a"}
    assert await driver.asrandmember("s", count=-2) == [b"a", b"a"]


# --- SINTER / SUNION / SDIFF (read) -------------------------------------


def test_sinter(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"2", b"3", b"4")
    got = driver.sinter("a", "b")
    assert isinstance(got, set)
    assert got == {b"2", b"3"}


def test_sinter_with_missing_key_is_empty(driver) -> None:
    driver.sadd("a", b"1", b"2")
    assert driver.sinter("a", "missing") == set()


def test_sunion(driver) -> None:
    driver.sadd("a", b"1", b"2")
    driver.sadd("b", b"2", b"3")
    got = driver.sunion("a", "b")
    assert isinstance(got, set)
    assert got == {b"1", b"2", b"3"}


def test_sdiff(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"2")
    assert driver.sdiff("a", "b") == {b"1", b"3"}


@pytest.mark.asyncio
async def test_asinter(driver) -> None:
    await driver.asadd("a", b"1", b"2")
    await driver.asadd("b", b"2")
    assert await driver.asinter("a", "b") == {b"2"}


# --- SINTERSTORE / SUNIONSTORE / SDIFFSTORE -----------------------------


def test_sinterstore(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"2", b"3", b"4")
    n = driver.sinterstore("dest", "a", "b")
    assert n == 2
    assert driver.smembers("dest") == {b"2", b"3"}


def test_sunionstore(driver) -> None:
    driver.sadd("a", b"1")
    driver.sadd("b", b"2")
    n = driver.sunionstore("dest", "a", "b")
    assert n == 2
    assert driver.smembers("dest") == {b"1", b"2"}


def test_sdiffstore(driver) -> None:
    driver.sadd("a", b"1", b"2")
    driver.sadd("b", b"2")
    n = driver.sdiffstore("dest", "a", "b")
    assert n == 1
    assert driver.smembers("dest") == {b"1"}


@pytest.mark.asyncio
async def test_asinterstore(driver) -> None:
    await driver.asadd("a", b"1")
    assert await driver.asinterstore("dest", "a") == 1


# --- SINTERCARD ---------------------------------------------------------


def test_sintercard_no_limit(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"2", b"3", b"4")
    assert driver.sintercard("a", "b") == 2


def test_sintercard_with_limit(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3", b"4")
    driver.sadd("b", b"1", b"2", b"3", b"4")
    # Cap result at 2.
    assert driver.sintercard("a", "b", limit=2) == 2


def test_sintercard_limit_zero_means_unlimited(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"1", b"2", b"3")
    assert driver.sintercard("a", "b", limit=0) == 3


@pytest.mark.asyncio
async def test_asintercard(driver) -> None:
    await driver.asadd("a", b"1", b"2")
    await driver.asadd("b", b"2", b"3")
    assert await driver.asintercard("a", "b") == 1


# --- SMOVE ---------------------------------------------------------------


def test_smove_member_present(driver) -> None:
    driver.sadd("src", b"a", b"b")
    driver.sadd("dst", b"x")
    assert driver.smove("src", "dst", b"a") is True
    assert driver.smembers("src") == {b"b"}
    assert driver.smembers("dst") == {b"a", b"x"}


def test_smove_member_absent(driver) -> None:
    driver.sadd("src", b"a")
    assert driver.smove("src", "dst", b"missing") is False


def test_smove_already_in_destination(driver) -> None:
    driver.sadd("src", b"a")
    driver.sadd("dst", b"a")
    # Per Redis: removed from src, dst unchanged but the move "succeeded".
    assert driver.smove("src", "dst", b"a") is True
    assert driver.scard("src") == 0
    assert driver.smembers("dst") == {b"a"}


@pytest.mark.asyncio
async def test_asmove(driver) -> None:
    await driver.asadd("src", b"x")
    assert await driver.asmove("src", "dst", b"x") is True


# --- SSCAN ---------------------------------------------------------------


def test_sscan_full_iteration(driver) -> None:
    expected = {f"m{i}".encode() for i in range(20)}
    driver.sadd("s", *expected)

    seen: set[bytes] = set()
    cursor = 0
    while True:
        cursor, batch = driver.sscan("s", cursor=cursor)
        assert isinstance(batch, set)
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == expected


def test_sscan_with_match(driver) -> None:
    driver.sadd("s", b"foo:1", b"foo:2", b"bar:1")
    cursor = 0
    seen: set[bytes] = set()
    while True:
        cursor, batch = driver.sscan("s", cursor=cursor, match="foo:*")
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == {b"foo:1", b"foo:2"}


def test_sscan_with_count(driver) -> None:
    driver.sadd("s", *[f"k{i}".encode() for i in range(50)])
    cursor, batch = driver.sscan("s", cursor=0, count=10)
    assert isinstance(batch, set)
    seen: set[bytes] = set(batch)
    while cursor != 0:
        cursor, batch = driver.sscan("s", cursor=cursor, count=10)
        seen.update(batch)
    assert len(seen) == 50


@pytest.mark.asyncio
async def test_asscan(driver) -> None:
    await driver.asadd("s", b"a", b"b")
    _cursor, batch = await driver.asscan("s", cursor=0)
    assert isinstance(batch, set)
    assert batch.issubset({b"a", b"b"})
