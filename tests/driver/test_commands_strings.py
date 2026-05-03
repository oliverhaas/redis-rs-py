"""String command tests for RedisRsDriver — Plan 03."""

from __future__ import annotations

import time

import pytest
import redis as upstream
from redis_rs_py.exceptions import DataError, ResponseError


def _upstream_url(connection_url: str) -> str:
    """Strip resp3 protocol parameter so upstream redis-py can connect."""
    # driver.connection_url has ?protocol=resp3 appended by url_with_resp3();
    # upstream redis-py can't parse "resp3" as an integer protocol version.
    return connection_url.replace("?protocol=resp3", "").replace("&protocol=resp3", "")


# ==========================================================================
# Task 2: Full SET option matrix
# ==========================================================================


def test_set_basic(driver) -> None:
    assert driver.set("k", b"v") is True
    assert driver.get("k") == b"v"


def test_set_with_ex(driver) -> None:
    assert driver.set("k", b"v", ex=60) is True
    rp = upstream.Redis.from_url(_upstream_url(driver.connection_url))
    assert 0 < rp.ttl("k") <= 60
    rp.close()


def test_set_with_px(driver) -> None:
    assert driver.set("k", b"v", px=60_000) is True
    rp = upstream.Redis.from_url(_upstream_url(driver.connection_url))
    assert 0 < rp.pttl("k") <= 60_000
    rp.close()


def test_set_nx_when_missing(driver) -> None:
    assert driver.set("k", b"v", nx=True) is True
    assert driver.get("k") == b"v"


def test_set_nx_when_present_returns_none(driver) -> None:
    driver.set("k", b"old")
    assert driver.set("k", b"new", nx=True) is None
    assert driver.get("k") == b"old"


def test_set_xx_when_missing_returns_none(driver) -> None:
    assert driver.set("k", b"v", xx=True) is None
    assert driver.get("k") is None


def test_set_xx_when_present(driver) -> None:
    driver.set("k", b"old")
    assert driver.set("k", b"new", xx=True) is True
    assert driver.get("k") == b"new"


def test_set_get_true_with_previous(driver) -> None:
    driver.set("k", b"old")
    assert driver.set("k", b"new", get=True) == b"old"
    assert driver.get("k") == b"new"


def test_set_get_true_without_previous_returns_none(driver) -> None:
    assert driver.set("k", b"v", get=True) is None
    assert driver.get("k") == b"v"


def test_set_keepttl(driver) -> None:
    driver.set("k", b"v", ex=60)
    driver.set("k", b"v2", keepttl=True)
    rp = upstream.Redis.from_url(_upstream_url(driver.connection_url))
    assert 0 < rp.ttl("k") <= 60
    rp.close()


def test_set_exat(driver) -> None:
    deadline = int(time.time()) + 30
    assert driver.set("k", b"v", exat=deadline) is True
    rp = upstream.Redis.from_url(_upstream_url(driver.connection_url))
    assert 0 < rp.ttl("k") <= 30
    rp.close()


def test_set_pxat(driver) -> None:
    deadline_ms = int(time.time() * 1000) + 30_000
    assert driver.set("k", b"v", pxat=deadline_ms) is True


def test_set_nx_and_xx_raises(driver) -> None:
    with pytest.raises(DataError, match="nx and xx"):
        driver.set("k", b"v", nx=True, xx=True)


def test_set_ex_and_px_raises(driver) -> None:
    with pytest.raises(DataError, match=r"ex.*px|only one of"):
        driver.set("k", b"v", ex=10, px=10_000)


def test_set_keepttl_with_ex_raises(driver) -> None:
    with pytest.raises(DataError, match="keepttl"):
        driver.set("k", b"v", ex=10, keepttl=True)


@pytest.mark.asyncio
async def test_aset_full_matrix(driver) -> None:
    assert await driver.aset("k", b"v") is True
    assert await driver.aset("k", b"v2", xx=True) is True
    assert await driver.aset("k", b"v3", nx=True) is None
    assert await driver.aset("k", b"v4", get=True) == b"v2"


@pytest.mark.asyncio
async def test_aset_invalid_kwargs_raises(driver) -> None:
    with pytest.raises(DataError):
        await driver.aset("k", b"v", nx=True, xx=True)


# ==========================================================================
# Task 3: GET family
# ==========================================================================


def test_getex_with_ex(driver) -> None:
    driver.set("k", b"v")
    assert driver.getex("k", ex=60) == b"v"
    rp = upstream.Redis.from_url(_upstream_url(driver.connection_url))
    assert 0 < rp.ttl("k") <= 60
    rp.close()


def test_getex_with_persist(driver) -> None:
    driver.set("k", b"v", ex=60)
    assert driver.getex("k", persist=True) == b"v"
    rp = upstream.Redis.from_url(_upstream_url(driver.connection_url))
    assert rp.ttl("k") == -1  # no TTL
    rp.close()


def test_getex_missing_returns_none(driver) -> None:
    assert driver.getex("missing") is None


def test_getex_invalid_kwargs_raises(driver) -> None:
    with pytest.raises(DataError):
        driver.getex("k", ex=10, px=10_000)


def test_getdel(driver) -> None:
    driver.set("k", b"v")
    assert driver.getdel("k") == b"v"
    assert driver.get("k") is None


def test_getdel_missing_returns_none(driver) -> None:
    assert driver.getdel("missing") is None


def test_getrange(driver) -> None:
    driver.set("k", b"hello world")
    assert driver.getrange("k", 0, 4) == b"hello"
    assert driver.getrange("k", 6, 10) == b"world"
    assert driver.getrange("k", 0, -1) == b"hello world"


def test_getrange_missing_returns_empty(driver) -> None:
    assert driver.getrange("missing", 0, 5) == b""


def test_setrange(driver) -> None:
    driver.set("k", b"hello world")
    assert driver.setrange("k", 6, b"redis") == 11
    assert driver.get("k") == b"hello redis"


def test_setrange_extends_string(driver) -> None:
    assert driver.setrange("k", 5, b"world") == 10
    assert driver.get("k") == b"\x00\x00\x00\x00\x00world"


def test_strlen(driver) -> None:
    driver.set("k", b"hello")
    assert driver.strlen("k") == 5


def test_strlen_missing_returns_zero(driver) -> None:
    assert driver.strlen("missing") == 0


def test_append_creates_key(driver) -> None:
    assert driver.append("k", b"hello") == 5
    assert driver.get("k") == b"hello"


def test_append_extends(driver) -> None:
    driver.set("k", b"hello")
    assert driver.append("k", b" world") == 11
    assert driver.get("k") == b"hello world"


@pytest.mark.asyncio
async def test_aget_family(driver) -> None:
    await driver.aset("k", b"hello")
    assert await driver.agetex("k", ex=60) == b"hello"
    assert await driver.agetdel("k") == b"hello"
    assert await driver.aget("k") is None
    await driver.aset("k", b"hello world")
    assert await driver.agetrange("k", 0, 4) == b"hello"
    assert await driver.asetrange("k", 6, b"redis") == 11
    assert await driver.astrlen("k") == 11
    assert await driver.aappend("k", b"!") == 12


# ==========================================================================
# Task 4: MGET / MSET / MSETNX
# ==========================================================================


def test_mget(driver) -> None:
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.mget(["a", "b", "missing"]) == [b"1", b"2", None]


def test_mget_empty_returns_empty_list(driver) -> None:
    assert driver.mget([]) == []


def test_mset(driver) -> None:
    driver.mset({"a": b"1", "b": b"2"})
    assert driver.get("a") == b"1"
    assert driver.get("b") == b"2"


def test_msetnx_when_all_missing(driver) -> None:
    assert driver.msetnx({"a": b"1", "b": b"2"}) is True
    assert driver.get("a") == b"1"


def test_msetnx_when_any_exists_returns_false(driver) -> None:
    driver.set("a", b"old")
    assert driver.msetnx({"a": b"new", "b": b"2"}) is False
    assert driver.get("a") == b"old"
    assert driver.get("b") is None


@pytest.mark.asyncio
async def test_amget_amset_amsetnx(driver) -> None:
    await driver.amset({"a": b"1", "b": b"2"})
    assert await driver.amget(["a", "b", "x"]) == [b"1", b"2", None]
    assert await driver.amsetnx({"c": b"3", "a": b"X"}) is False
    assert await driver.aget("a") == b"1"
    assert await driver.aget("c") is None


# ==========================================================================
# Task 5: INCR / DECR family
# ==========================================================================


def test_incr_creates_key_at_one(driver) -> None:
    assert driver.incr("counter") == 1
    assert driver.incr("counter") == 2


def test_incrby(driver) -> None:
    assert driver.incrby("counter", 10) == 10
    assert driver.incrby("counter", 5) == 15
    assert driver.incrby("counter", -3) == 12


def test_incrbyfloat(driver) -> None:
    assert driver.incrbyfloat("counter", 1.5) == pytest.approx(1.5)
    assert driver.incrbyfloat("counter", 2.25) == pytest.approx(3.75)


def test_decr(driver) -> None:
    driver.set("counter", b"10")
    assert driver.decr("counter") == 9
    assert driver.decr("counter") == 8


def test_decrby(driver) -> None:
    driver.set("counter", b"100")
    assert driver.decrby("counter", 25) == 75


def test_incr_on_non_numeric_raises(driver) -> None:
    driver.set("k", b"not-a-number")
    with pytest.raises(ResponseError):
        driver.incr("k")


@pytest.mark.asyncio
async def test_aincr_family(driver) -> None:
    assert await driver.aincr("c") == 1
    assert await driver.aincrby("c", 5) == 6
    assert await driver.adecr("c") == 5
    assert await driver.adecrby("c", 2) == 3
    assert await driver.aincrbyfloat("c", 0.5) == pytest.approx(3.5)


# ==========================================================================
# Task 6: EXISTS (variadic) / UNLINK
# ==========================================================================


def test_exists_single(driver) -> None:
    driver.set("a", b"1")
    assert driver.exists("a") == 1
    assert driver.exists("missing") == 0


def test_exists_variadic(driver) -> None:
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.exists("a", "b", "missing") == 2


def test_exists_counts_duplicates(driver) -> None:
    driver.set("a", b"1")
    # EXISTS counts each occurrence even on duplicates.
    assert driver.exists("a", "a", "a") == 3


def test_exists_empty_returns_zero(driver) -> None:
    assert driver.exists() == 0


def test_unlink(driver) -> None:
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.unlink("a", "b", "missing") == 2
    assert driver.get("a") is None


def test_unlink_empty_returns_zero(driver) -> None:
    assert driver.unlink() == 0


@pytest.mark.asyncio
async def test_aexists_aunlink(driver) -> None:
    await driver.aset("a", b"1")
    await driver.aset("b", b"2")
    assert await driver.aexists("a", "b", "x") == 2
    assert await driver.aunlink("a", "b") == 2
    assert await driver.aexists("a") == 0


# ==========================================================================
# Task 7: EXPIRE family + TTL/PTTL/PERSIST/EXPIRETIME/PEXPIRETIME
# ==========================================================================


def test_expire_returns_true(driver) -> None:
    driver.set("k", b"v")
    assert driver.expire("k", 60) is True
    assert 0 < driver.ttl("k") <= 60


def test_expire_missing_returns_false(driver) -> None:
    assert driver.expire("missing", 60) is False


def test_pexpire(driver) -> None:
    driver.set("k", b"v")
    assert driver.pexpire("k", 60_000) is True
    assert 0 < driver.pttl("k") <= 60_000


def test_expireat(driver) -> None:
    driver.set("k", b"v")
    assert driver.expireat("k", int(time.time()) + 30) is True
    assert 0 < driver.ttl("k") <= 30


def test_pexpireat(driver) -> None:
    driver.set("k", b"v")
    assert driver.pexpireat("k", int(time.time() * 1000) + 30_000) is True


def test_expire_with_xx_when_no_ttl_returns_false(driver) -> None:
    driver.set("k", b"v")
    # XX = only set TTL if there's already a TTL. None exists yet -> False.
    assert driver.expire("k", 60, xx=True) is False


def test_expire_with_nx_when_no_ttl_returns_true(driver) -> None:
    driver.set("k", b"v")
    assert driver.expire("k", 60, nx=True) is True


def test_expire_with_gt(driver) -> None:
    driver.set("k", b"v", ex=100)
    # GT = only update if new TTL is greater than current.
    assert driver.expire("k", 50, gt=True) is False
    assert driver.expire("k", 200, gt=True) is True


def test_expire_with_lt(driver) -> None:
    driver.set("k", b"v", ex=100)
    assert driver.expire("k", 200, lt=True) is False
    assert driver.expire("k", 50, lt=True) is True


def test_ttl_no_expiry_returns_minus_one(driver) -> None:
    driver.set("k", b"v")
    assert driver.ttl("k") == -1


def test_ttl_missing_returns_minus_two(driver) -> None:
    assert driver.ttl("missing") == -2


def test_pttl_missing_returns_minus_two(driver) -> None:
    assert driver.pttl("missing") == -2


def test_expiretime(driver) -> None:
    deadline = int(time.time()) + 60
    driver.set("k", b"v", exat=deadline)
    assert driver.expiretime("k") == deadline


def test_expiretime_no_expiry_returns_minus_one(driver) -> None:
    driver.set("k", b"v")
    assert driver.expiretime("k") == -1


def test_expiretime_missing_returns_minus_two(driver) -> None:
    assert driver.expiretime("missing") == -2


def test_pexpiretime(driver) -> None:
    deadline_ms = int(time.time() * 1000) + 60_000
    driver.set("k", b"v", pxat=deadline_ms)
    assert driver.pexpiretime("k") == deadline_ms


def test_persist(driver) -> None:
    driver.set("k", b"v", ex=60)
    assert driver.persist("k") is True
    assert driver.ttl("k") == -1


def test_persist_no_ttl_returns_false(driver) -> None:
    driver.set("k", b"v")
    assert driver.persist("k") is False


@pytest.mark.asyncio
async def test_aexpire_family(driver) -> None:
    await driver.aset("k", b"v")
    assert await driver.aexpire("k", 60) is True
    assert 0 < await driver.attl("k") <= 60
    assert await driver.apexpire("k", 90_000) is True
    assert await driver.apersist("k") is True
    assert await driver.attl("k") == -1
    assert await driver.aexpireat("k", int(time.time()) + 60) is True
    assert await driver.apexpireat("k", int(time.time() * 1000) + 60_000) is True
    et = await driver.aexpiretime("k")
    assert et > 0
    pet = await driver.apexpiretime("k")
    assert pet > 0


# ==========================================================================
# Task 8: RENAME / RENAMENX / TYPE
# ==========================================================================


def test_rename(driver) -> None:
    driver.set("a", b"v")
    driver.rename("a", "b")
    assert driver.get("a") is None
    assert driver.get("b") == b"v"


def test_rename_missing_source_raises(driver) -> None:
    with pytest.raises(ResponseError):
        driver.rename("missing", "b")


def test_renamenx_when_dest_missing(driver) -> None:
    driver.set("a", b"v")
    assert driver.renamenx("a", "b") is True
    assert driver.get("b") == b"v"


def test_renamenx_when_dest_exists_returns_false(driver) -> None:
    driver.set("a", b"v")
    driver.set("b", b"existing")
    assert driver.renamenx("a", "b") is False
    assert driver.get("a") == b"v"
    assert driver.get("b") == b"existing"


def test_type_string(driver) -> None:
    driver.set("k", b"v")
    # `type` is a Python keyword; getattr is the portable form.
    assert driver.type("k") == "string"


def test_type_alias_key_type(driver) -> None:
    driver.set("k", b"v")
    assert driver.key_type("k") == "string"


def test_type_missing_returns_none_string(driver) -> None:
    assert driver.key_type("missing") == "none"


@pytest.mark.asyncio
async def test_arename_atype(driver) -> None:
    await driver.aset("a", b"v")
    await driver.arename("a", "b")
    assert await driver.aget("b") == b"v"
    assert await driver.akey_type("b") == "string"
    await driver.aset("c", b"x")
    assert await driver.arenamenx("c", "b") is False


# ==========================================================================
# Task 9: COPY
# ==========================================================================


def test_copy_basic(driver) -> None:
    driver.set("a", b"v")
    assert driver.copy("a", "b") is True
    assert driver.get("b") == b"v"


def test_copy_when_dest_exists_no_replace_returns_false(driver) -> None:
    driver.set("a", b"v")
    driver.set("b", b"existing")
    assert driver.copy("a", "b") is False
    assert driver.get("b") == b"existing"


def test_copy_with_replace(driver) -> None:
    driver.set("a", b"v")
    driver.set("b", b"existing")
    assert driver.copy("a", "b", replace=True) is True
    assert driver.get("b") == b"v"


def test_copy_missing_source_returns_false(driver) -> None:
    assert driver.copy("missing", "dst") is False


def test_copy_with_db_to_other_db(driver) -> None:
    import uuid  # noqa: PLC0415

    driver.set("a", b"v")
    # COPY can target a different db. We use DB 15 as the cross-db target.
    # Under xdist with many workers, gw15 also owns DB 15, so we use a UUID
    # key to avoid any conflict with other tests and replace=True so a
    # hypothetical stale key never causes a spurious False.
    base_url = _upstream_url(driver.connection_url)
    target_url = base_url.rsplit("/", 1)[0] + "/15"
    rp15 = upstream.Redis.from_url(target_url)
    dst_key = f"test_copy_dst_{uuid.uuid4().hex}"
    try:
        assert driver.copy("a", dst_key, db=15) is True
        assert rp15.get(dst_key) == b"v"
    finally:
        rp15.delete(dst_key)
        rp15.close()


@pytest.mark.asyncio
async def test_acopy(driver) -> None:
    await driver.aset("a", b"v")
    assert await driver.acopy("a", "b") is True
    assert await driver.acopy("a", "b") is False
    assert await driver.acopy("a", "b", replace=True) is True


# ==========================================================================
# Task 10: DUMP / RESTORE
# ==========================================================================


def test_dump_returns_bytes(driver) -> None:
    driver.set("k", b"v")
    payload = driver.dump("k")
    assert isinstance(payload, bytes)
    assert len(payload) > 0


def test_dump_missing_returns_none(driver) -> None:
    assert driver.dump("missing") is None


def test_dump_then_restore_round_trip(driver) -> None:
    driver.set("k", b"hello")
    payload = driver.dump("k")
    assert driver.restore("k2", 0, payload) is True
    assert driver.get("k2") == b"hello"


def test_restore_existing_key_without_replace_raises(driver) -> None:
    driver.set("k", b"v")
    payload = driver.dump("k")
    driver.set("dst", b"existing")
    with pytest.raises(ResponseError, match=r"(?i)busy"):
        driver.restore("dst", 0, payload)


def test_restore_with_replace(driver) -> None:
    driver.set("k", b"new")
    payload = driver.dump("k")
    driver.set("dst", b"old")
    assert driver.restore("dst", 0, payload, replace=True) is True
    assert driver.get("dst") == b"new"


def test_restore_with_idletime(driver) -> None:
    driver.set("k", b"v")
    payload = driver.dump("k")
    assert driver.restore("dst", 0, payload, idletime=10) is True


def test_restore_with_absttl(driver) -> None:
    driver.set("k", b"v")
    payload = driver.dump("k")
    deadline_ms = int(time.time() * 1000) + 30_000
    assert driver.restore("dst", deadline_ms, payload, absttl=True) is True
    assert 0 < driver.pttl("dst") <= 30_000


@pytest.mark.asyncio
async def test_adump_arestore(driver) -> None:
    await driver.aset("k", b"hello")
    payload = await driver.adump("k")
    assert isinstance(payload, bytes)
    assert await driver.arestore("k2", 0, payload) is True
    assert await driver.aget("k2") == b"hello"
