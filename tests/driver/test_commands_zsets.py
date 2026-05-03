"""Sorted-set command coverage on Redis/AsyncRedis — Plan 07."""

from __future__ import annotations

import pytest
from redis_rs_py.exceptions import DataError

# --- ZADD basic ---------------------------------------------------------


def test_zadd_basic_returns_added_count(driver) -> None:
    assert driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0}) == 3


def test_zadd_existing_member_returns_zero(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    # Updating an existing member does not bump the count (without CH).
    assert driver.zadd("z", mapping={"a": 5.0}) == 0


def test_zadd_with_ch_returns_changed_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    # Both `a` (rescored) and `c` (added) → 2 changed.
    assert driver.zadd("z", mapping={"a": 10.0, "c": 3.0}, ch=True) == 2


# --- NX / XX -------------------------------------------------------------


def test_zadd_nx_only_inserts_new(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    n = driver.zadd("z", mapping={"a": 99.0, "b": 2.0}, nx=True)
    assert n == 1  # only `b` newly added
    assert driver.zscore("z", b"a") == 1.0  # not overwritten


def test_zadd_xx_only_updates_existing(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    n = driver.zadd("z", mapping={"a": 5.0, "b": 2.0}, xx=True)
    # `b` is rejected by XX; `a` updated but not new → 0 added.
    assert n == 0
    assert driver.zscore("z", b"a") == 5.0
    assert driver.zscore("z", b"b") is None


def test_zadd_nx_and_xx_together_raises_data_error(driver) -> None:

    with pytest.raises(DataError, match="NX and XX"):
        driver.zadd("z", mapping={"a": 1.0}, nx=True, xx=True)


# --- GT / LT -------------------------------------------------------------


def test_zadd_gt_only_updates_when_new_score_higher(driver) -> None:
    driver.zadd("z", mapping={"a": 5.0})
    driver.zadd("z", mapping={"a": 3.0}, gt=True)
    assert driver.zscore("z", b"a") == 5.0  # not lowered
    driver.zadd("z", mapping={"a": 10.0}, gt=True)
    assert driver.zscore("z", b"a") == 10.0


def test_zadd_lt_only_updates_when_new_score_lower(driver) -> None:
    driver.zadd("z", mapping={"a": 5.0})
    driver.zadd("z", mapping={"a": 10.0}, lt=True)
    assert driver.zscore("z", b"a") == 5.0
    driver.zadd("z", mapping={"a": 1.0}, lt=True)
    assert driver.zscore("z", b"a") == 1.0


def test_zadd_gt_and_lt_together_raises(driver) -> None:

    with pytest.raises(DataError, match="GT and LT"):
        driver.zadd("z", mapping={"a": 1.0}, gt=True, lt=True)


def test_zadd_nx_and_gt_together_raises(driver) -> None:

    with pytest.raises(DataError, match="NX"):
        driver.zadd("z", mapping={"a": 1.0}, nx=True, gt=True)


# --- INCR ---------------------------------------------------------------


def test_zadd_incr_returns_new_score(driver) -> None:
    got = driver.zadd("z", mapping={"a": 5.0}, incr=True)
    assert got == 5.0
    got2 = driver.zadd("z", mapping={"a": 3.0}, incr=True)
    assert got2 == 8.0


def test_zadd_incr_blocked_by_nx_returns_none(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    got = driver.zadd("z", mapping={"a": 5.0}, incr=True, nx=True)
    assert got is None


def test_zadd_incr_with_multiple_pairs_raises(driver) -> None:

    with pytest.raises(DataError, match=r"INCR.*single"):
        driver.zadd("z", mapping={"a": 1.0, "b": 2.0}, incr=True)


def test_zadd_empty_mapping_raises(driver) -> None:

    with pytest.raises(DataError, match="empty"):
        driver.zadd("z", mapping={})


# --- async --------------------------------------------------------------


@pytest.mark.asyncio
async def test_azadd_basic(driver) -> None:
    assert await driver.azadd("z", mapping={"a": 1.0, "b": 2.0}) == 2


@pytest.mark.asyncio
async def test_azadd_incr_returns_score(driver) -> None:
    assert await driver.azadd("z", mapping={"a": 3.5}, incr=True) == 3.5


@pytest.mark.asyncio
async def test_azadd_incr_nx_blocked_returns_none(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azadd("z", mapping={"a": 5.0}, incr=True, nx=True) is None


# --- ZREM ---------------------------------------------------------------


def test_zrem_returns_removed_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrem("z", b"a", b"missing", b"c") == 2
    assert driver.zcard("z") == 1


def test_zrem_missing_key_returns_zero(driver) -> None:
    assert driver.zrem("missing", b"a") == 0


def test_zrem_no_members_returns_zero(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zrem("z") == 0


@pytest.mark.asyncio
async def test_azrem(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azrem("z", b"a") == 1


# --- ZRANGE -------------------------------------------------------------


def test_zrange_basic(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrange("z", 0, -1) == [b"a", b"b", b"c"]


def test_zrange_with_scores(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    got = driver.zrange("z", 0, -1, withscores=True)
    assert got == [(b"a", 1.0), (b"b", 2.0)]


def test_zrange_desc(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrange("z", 0, -1, desc=True) == [b"c", b"b", b"a"]


def test_zrange_byscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert driver.zrange("z", "2", "3", byscore=True) == [b"b", b"c"]


def test_zrange_byscore_with_limit(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    got = driver.zrange("z", "1", "10", byscore=True, offset=1, num=2)
    assert got == [b"b", b"c"]


def test_zrange_bylex(driver) -> None:
    # All same score for BYLEX ordering.
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0})
    assert driver.zrange("z", "[a", "[b", bylex=True) == [b"a", b"b"]


def test_zrange_byscore_and_bylex_together_raises(driver) -> None:

    with pytest.raises(DataError, match="BYSCORE and BYLEX"):
        driver.zrange("z", "0", "10", byscore=True, bylex=True)


def test_zrange_limit_without_byscore_or_bylex_raises(driver) -> None:

    with pytest.raises(DataError, match="LIMIT"):
        driver.zrange("z", 0, -1, offset=0, num=5)


def test_zrange_withscores_and_bylex_raises(driver) -> None:

    with pytest.raises(DataError, match="WITHSCORES"):
        driver.zrange("z", "[a", "[z", bylex=True, withscores=True)


@pytest.mark.asyncio
async def test_azrange(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azrange("z", 0, -1) == [b"a", b"b"]


@pytest.mark.asyncio
async def test_azrange_withscores(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrange("z", 0, -1, withscores=True) == [(b"a", 1.0)]


# --- ZRANGESTORE --------------------------------------------------------


def test_zrangestore_basic(driver) -> None:
    driver.zadd("src", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    n = driver.zrangestore("dst", "src", 0, 1)
    assert n == 2
    assert driver.zrange("dst", 0, -1) == [b"a", b"b"]


def test_zrangestore_byscore(driver) -> None:
    driver.zadd("src", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    n = driver.zrangestore("dst", "src", "2", "3", byscore=True)
    assert n == 2


@pytest.mark.asyncio
async def test_azrangestore(driver) -> None:
    await driver.azadd("src", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azrangestore("dst", "src", 0, -1) == 2


# --- ZRANGEBYSCORE / ZREVRANGEBYSCORE -----------------------------------


def test_zrangebyscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrangebyscore("z", "-inf", "2") == [b"a", b"b"]


def test_zrangebyscore_with_scores(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    assert driver.zrangebyscore("z", "1", "2", withscores=True) == [
        (b"a", 1.0),
        (b"b", 2.0),
    ]


def test_zrangebyscore_with_limit(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    got = driver.zrangebyscore("z", "1", "10", offset=1, num=2)
    assert got == [b"b", b"c"]


def test_zrevrangebyscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    # Note: max comes BEFORE min for REV.
    assert driver.zrevrangebyscore("z", "3", "1") == [b"c", b"b", b"a"]


def test_zrevrangebyscore_with_limit(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrevrangebyscore("z", "3", "1", offset=0, num=2) == [b"c", b"b"]


@pytest.mark.asyncio
async def test_azrangebyscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrangebyscore("z", "-inf", "+inf") == [b"a"]


# --- ZRANGEBYLEX / ZREVRANGEBYLEX ---------------------------------------


def test_zrangebylex(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
    assert driver.zrangebylex("z", "[a", "[c") == [b"a", b"b", b"c"]


def test_zrangebylex_exclusive(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0})
    assert driver.zrangebylex("z", "(a", "(c") == [b"b"]


def test_zrangebylex_with_limit(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
    assert driver.zrangebylex("z", "-", "+", offset=1, num=2) == [b"b", b"c"]


def test_zrevrangebylex(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0})
    # Note: max comes BEFORE min for REV.
    assert driver.zrevrangebylex("z", "[c", "[a") == [b"c", b"b", b"a"]


@pytest.mark.asyncio
async def test_azrangebylex(driver) -> None:
    await driver.azadd("z", mapping={"a": 0.0, "b": 0.0})
    assert await driver.azrangebylex("z", "[a", "[b") == [b"a", b"b"]


# --- ZINCRBY ------------------------------------------------------------


def test_zincrby_creates_member_at_delta(driver) -> None:
    assert driver.zincrby("z", 5.5, b"a") == 5.5
    assert driver.zscore("z", b"a") == 5.5


def test_zincrby_increments_existing(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zincrby("z", 2.5, b"a") == 3.5
    assert driver.zincrby("z", -1.0, b"a") == 2.5


@pytest.mark.asyncio
async def test_azincrby(driver) -> None:
    assert await driver.azincrby("z", 1.0, b"a") == 1.0


# --- ZCARD --------------------------------------------------------------


def test_zcard(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zcard("z") == 3


def test_zcard_missing_is_zero(driver) -> None:
    assert driver.zcard("missing") == 0


@pytest.mark.asyncio
async def test_azcard(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azcard("z") == 1


# --- ZSCORE -------------------------------------------------------------


def test_zscore_present(driver) -> None:
    driver.zadd("z", mapping={"a": 3.5})
    assert driver.zscore("z", b"a") == 3.5


def test_zscore_absent_returns_none(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zscore("z", b"missing") is None
    assert driver.zscore("missing-key", b"a") is None


@pytest.mark.asyncio
async def test_azscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 2.0})
    assert await driver.azscore("z", b"a") == 2.0
    assert await driver.azscore("z", b"x") is None


# --- ZMSCORE ------------------------------------------------------------


def test_zmscore_preserves_order(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "c": 3.0})
    assert driver.zmscore("z", b"a", b"b", b"c") == [1.0, None, 3.0]


def test_zmscore_missing_key(driver) -> None:
    assert driver.zmscore("missing", b"a", b"b") == [None, None]


def test_zmscore_no_members_returns_empty(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zmscore("z") == []


@pytest.mark.asyncio
async def test_azmscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azmscore("z", b"a", b"b") == [1.0, None]


# --- ZRANK / ZREVRANK ---------------------------------------------------


def test_zrank(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrank("z", b"a") == 0
    assert driver.zrank("z", b"c") == 2
    assert driver.zrank("z", b"missing") is None


def test_zrevrank(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrevrank("z", b"c") == 0
    assert driver.zrevrank("z", b"a") == 2


def test_zrank_withscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.5})
    got = driver.zrank("z", b"b", withscore=True)
    assert got == (1, 2.5)


def test_zrank_withscore_missing_returns_none(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zrank("z", b"missing", withscore=True) is None


def test_zrevrank_withscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    assert driver.zrevrank("z", b"a", withscore=True) == (1, 1.0)


@pytest.mark.asyncio
async def test_azrank(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrank("z", b"a") == 0


@pytest.mark.asyncio
async def test_azrank_withscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrank("z", b"a", withscore=True) == (0, 1.0)


# --- ZREMRANGEBYRANK ----------------------------------------------------


def test_zremrangebyrank(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert driver.zremrangebyrank("z", 1, 2) == 2
    assert driver.zrange("z", 0, -1) == [b"a", b"d"]


def test_zremrangebyrank_missing_key(driver) -> None:
    assert driver.zremrangebyrank("missing", 0, -1) == 0


@pytest.mark.asyncio
async def test_azremrangebyrank(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azremrangebyrank("z", 0, 0) == 1


# --- ZREMRANGEBYSCORE ---------------------------------------------------


def test_zremrangebyscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert driver.zremrangebyscore("z", "2", "3") == 2
    assert driver.zrange("z", 0, -1) == [b"a", b"d"]


@pytest.mark.asyncio
async def test_azremrangebyscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azremrangebyscore("z", "1", "1") == 1


# --- ZREMRANGEBYLEX -----------------------------------------------------


def test_zremrangebylex(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
    assert driver.zremrangebylex("z", "[b", "[c") == 2
    assert driver.zrange("z", 0, -1) == [b"a", b"d"]


@pytest.mark.asyncio
async def test_azremrangebylex(driver) -> None:
    await driver.azadd("z", mapping={"a": 0.0, "b": 0.0})
    assert await driver.azremrangebylex("z", "[a", "[a") == 1


# --- ZCOUNT / ZLEXCOUNT -------------------------------------------------


def test_zcount(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zcount("z", "1", "2") == 2
    assert driver.zcount("z", "-inf", "+inf") == 3


def test_zcount_with_exclusive_bounds(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zcount("z", "(1", "(3") == 1


@pytest.mark.asyncio
async def test_azcount(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azcount("z", "0", "5") == 1


def test_zlexcount(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0})
    assert driver.zlexcount("z", "[a", "[c") == 3
    assert driver.zlexcount("z", "-", "+") == 3
    assert driver.zlexcount("z", "(a", "(c") == 1


@pytest.mark.asyncio
async def test_azlexcount(driver) -> None:
    await driver.azadd("z", mapping={"a": 0.0, "b": 0.0})
    assert await driver.azlexcount("z", "[a", "[b") == 2


# --- ZPOPMIN / ZPOPMAX --------------------------------------------------


def test_zpopmin(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zpopmin("z") == [(b"a", 1.0)]
    assert driver.zcard("z") == 2


def test_zpopmin_with_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zpopmin("z", count=2) == [(b"a", 1.0), (b"b", 2.0)]


def test_zpopmin_missing_returns_empty(driver) -> None:
    assert driver.zpopmin("missing") == []


def test_zpopmax(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zpopmax("z") == [(b"c", 3.0)]


def test_zpopmax_with_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zpopmax("z", count=2) == [(b"c", 3.0), (b"b", 2.0)]


@pytest.mark.asyncio
async def test_azpopmin(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azpopmin("z") == [(b"a", 1.0)]


# --- ZMPOP --------------------------------------------------------------


def test_zmpop_min(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    got = driver.zmpop("z", direction="MIN")
    assert got == ("z", [(b"a", 1.0)])


def test_zmpop_max_with_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    got = driver.zmpop("z", direction="MAX", count=2)
    assert got == ("z", [(b"c", 3.0), (b"b", 2.0)])


def test_zmpop_no_match_returns_none(driver) -> None:
    assert driver.zmpop("missing-1", "missing-2", direction="MIN") is None


def test_zmpop_picks_first_non_empty(driver) -> None:
    driver.zadd("b", mapping={"x": 1.0})
    got = driver.zmpop("a", "b", direction="MIN")
    assert got == ("b", [(b"x", 1.0)])


def test_zmpop_invalid_direction_raises(driver) -> None:

    with pytest.raises(DataError, match="MIN or MAX"):
        driver.zmpop("z", direction="UP")


@pytest.mark.asyncio
async def test_azmpop(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azmpop("z", direction="MIN") == ("z", [(b"a", 1.0)])


# --- BZPOPMIN / BZPOPMAX ------------------------------------------------


def test_bzpopmin_immediate(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    got = driver.bzpopmin("z", timeout=1.0)
    # Returns (key, member, score) tuple per redis docs.
    assert got == (b"z", b"a", 1.0)


def test_bzpopmin_timeout_returns_none(driver) -> None:
    assert driver.bzpopmin("missing", timeout=0.1) is None


def test_bzpopmax_immediate(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    got = driver.bzpopmax("z", timeout=1.0)
    assert got == (b"z", b"b", 2.0)


@pytest.mark.asyncio
async def test_abzpopmin(driver) -> None:
    await driver.azadd("z", mapping={"x": 5.0})
    got = await driver.abzpopmin("z", timeout=1.0)
    assert got == (b"z", b"x", 5.0)


# --- BZMPOP -------------------------------------------------------------


def test_bzmpop_immediate(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    got = driver.bzmpop("z", direction="MIN", timeout=1.0)
    assert got == ("z", [(b"a", 1.0)])


def test_bzmpop_timeout_returns_none(driver) -> None:
    assert driver.bzmpop("missing", direction="MIN", timeout=0.1) is None


@pytest.mark.asyncio
async def test_abzmpop(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    got = await driver.abzmpop("z", direction="MIN", timeout=1.0)
    assert got == ("z", [(b"a", 1.0)])


# --- ZRANDMEMBER --------------------------------------------------------


def test_zrandmember_no_count(driver) -> None:
    driver.zadd("z", mapping={"only": 1.0})
    assert driver.zrandmember("z") == b"only"


def test_zrandmember_missing_returns_none(driver) -> None:
    assert driver.zrandmember("missing") is None


def test_zrandmember_positive_count_distinct(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    got = driver.zrandmember("z", count=2)
    assert isinstance(got, list)
    assert len(got) == 2
    assert len(set(got)) == 2


def test_zrandmember_negative_count_with_repeats(driver) -> None:
    driver.zadd("z", mapping={"only": 1.0})
    got = driver.zrandmember("z", count=-3)
    assert got == [b"only", b"only", b"only"]


def test_zrandmember_withscores(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    got = driver.zrandmember("z", count=2, withscores=True)
    assert isinstance(got, list)
    assert all(isinstance(item, tuple) and len(item) == 2 for item in got)


@pytest.mark.asyncio
async def test_azrandmember(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrandmember("z") == b"a"
    assert await driver.azrandmember("z", count=1) == [b"a"]
    assert await driver.azrandmember("z", count=1, withscores=True) == [(b"a", 1.0)]


# --- ZSCAN --------------------------------------------------------------


def test_zscan_full_iteration(driver) -> None:
    expected = {f"m{i}".encode(): float(i) for i in range(20)}
    driver.zadd("z", mapping={k.decode(): v for k, v in expected.items()})

    seen: dict[bytes, float] = {}
    cursor = 0
    while True:
        cursor, batch = driver.zscan("z", cursor=cursor)
        assert isinstance(batch, list)
        assert all(isinstance(p, tuple) and len(p) == 2 for p in batch)
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == expected


def test_zscan_with_match(driver) -> None:
    driver.zadd("z", mapping={"foo:1": 1.0, "foo:2": 2.0, "bar:1": 3.0})
    cursor = 0
    seen: dict[bytes, float] = {}
    while True:
        cursor, batch = driver.zscan("z", cursor=cursor, match="foo:*")
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == {b"foo:1": 1.0, b"foo:2": 2.0}


def test_zscan_with_count(driver) -> None:
    driver.zadd("z", mapping={f"k{i}": float(i) for i in range(40)})
    cursor, batch = driver.zscan("z", cursor=0, count=10)
    seen: dict[bytes, float] = dict(batch)
    while cursor != 0:
        cursor, batch = driver.zscan("z", cursor=cursor, count=10)
        seen.update(batch)
    assert len(seen) == 40


@pytest.mark.asyncio
async def test_azscan(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    _cursor, batch = await driver.azscan("z", cursor=0)
    seen = dict(batch)
    assert seen == {b"a": 1.0, b"b": 2.0}


# --- ZUNION / ZINTER / ZDIFF (read) -------------------------------------


def test_zunion_basic(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 3.0, "z": 4.0})
    got = driver.zunion(keys=["a", "b"])
    assert sorted(got) == [b"x", b"y", b"z"]


def test_zunion_with_scores(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 3.0, "z": 4.0})
    got = driver.zunion(keys=["a", "b"], withscores=True)
    # Default aggregate is SUM: y=2+3=5
    as_dict = dict(got)
    assert as_dict[b"y"] == 5.0


def test_zunion_with_weights_and_aggregate_max(driver) -> None:
    driver.zadd("a", mapping={"y": 1.0})
    driver.zadd("b", mapping={"y": 2.0})
    got = driver.zunion(keys=["a", "b"], weights=[10.0, 1.0], aggregate="MAX", withscores=True)
    # weights → a:y=10, b:y=2; MAX → 10
    assert got == [(b"y", 10.0)]


def test_zinter_basic(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 3.0, "z": 4.0})
    assert driver.zinter(keys=["a", "b"]) == [b"y"]


def test_zinter_with_scores(driver) -> None:
    driver.zadd("a", mapping={"y": 2.0})
    driver.zadd("b", mapping={"y": 3.0})
    assert driver.zinter(keys=["a", "b"], withscores=True) == [(b"y", 5.0)]


def test_zdiff_basic(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 1.0})
    assert driver.zdiff(keys=["a", "b"]) == [b"x"]


def test_zdiff_with_scores(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0})
    driver.zadd("b", mapping={"y": 1.0})
    assert driver.zdiff(keys=["a", "b"], withscores=True) == [(b"x", 1.0)]


def test_zunion_weights_count_mismatch_raises(driver) -> None:

    with pytest.raises(DataError, match="weights"):
        driver.zunion(keys=["a", "b"], weights=[1.0])


def test_zunion_invalid_aggregate_raises(driver) -> None:

    with pytest.raises(DataError, match="AGGREGATE"):
        driver.zunion(keys=["a"], aggregate="AVERAGE")


@pytest.mark.asyncio
async def test_azunion(driver) -> None:
    await driver.azadd("a", mapping={"x": 1.0})
    await driver.azadd("b", mapping={"x": 2.0})
    got = await driver.azunion(keys=["a", "b"], withscores=True)
    assert got == [(b"x", 3.0)]


# --- ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE -----------------------------


def test_zunionstore(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 3.0, "z": 4.0})
    n = driver.zunionstore("dst", keys=["a", "b"])
    assert n == 3
    assert driver.zscore("dst", b"y") == 5.0


def test_zinterstore_with_weights(driver) -> None:
    driver.zadd("a", mapping={"y": 1.0})
    driver.zadd("b", mapping={"y": 2.0})
    n = driver.zinterstore("dst", keys=["a", "b"], weights=[2.0, 1.0])
    assert n == 1
    # 1*2 + 2*1 = 4
    assert driver.zscore("dst", b"y") == 4.0


def test_zdiffstore(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 1.0})
    n = driver.zdiffstore("dst", keys=["a", "b"])
    assert n == 1


@pytest.mark.asyncio
async def test_azunionstore(driver) -> None:
    await driver.azadd("a", mapping={"x": 1.0})
    assert await driver.azunionstore("dst", keys=["a"]) == 1


# --- ZINTERCARD ---------------------------------------------------------


def test_zintercard(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0, "z": 3.0})
    driver.zadd("b", mapping={"y": 1.0, "z": 1.0, "w": 1.0})
    assert driver.zintercard(keys=["a", "b"]) == 2


def test_zintercard_with_limit(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0, "z": 3.0})
    driver.zadd("b", mapping={"x": 1.0, "y": 1.0, "z": 1.0})
    assert driver.zintercard(keys=["a", "b"], limit=2) == 2


def test_zintercard_limit_zero_unlimited(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 1.0})
    driver.zadd("b", mapping={"x": 1.0, "y": 1.0})
    assert driver.zintercard(keys=["a", "b"], limit=0) == 2


@pytest.mark.asyncio
async def test_azintercard(driver) -> None:
    await driver.azadd("a", mapping={"x": 1.0})
    await driver.azadd("b", mapping={"x": 1.0})
    assert await driver.azintercard(keys=["a", "b"]) == 1
