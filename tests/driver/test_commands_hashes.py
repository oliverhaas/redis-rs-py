"""Hash command coverage on Redis/AsyncRedis — Plans 05."""

import time
import warnings

import pytest
import redis as upstream_redis
from redis_rs_py.exceptions import DataError, ResponseError

# --- HGET / HSET ---------------------------------------------------------


def test_hset_single_field_returns_count(driver) -> None:
    # First insert: 1 new field
    assert driver.hset("h", "f", b"v") == 1
    # Update existing field: 0 new fields
    assert driver.hset("h", "f", b"v2") == 0
    assert driver.hget("h", "f") == b"v2"


def test_hget_missing_returns_none(driver) -> None:
    assert driver.hget("missing-key", "missing-field") is None
    driver.hset("h", "f", b"v")
    assert driver.hget("h", "missing-field") is None


def test_hset_variadic_positional_pairs(driver) -> None:
    n = driver.hset("h", "f1", b"v1", "f2", b"v2", "f3", b"v3")
    assert n == 3
    assert driver.hget("h", "f1") == b"v1"
    assert driver.hget("h", "f2") == b"v2"
    assert driver.hget("h", "f3") == b"v3"


def test_hset_with_mapping_kwarg(driver) -> None:
    n = driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    assert n == 3
    assert driver.hget("h", "a") == b"1"
    assert driver.hget("h", "b") == b"2"


def test_hset_mixes_positional_and_mapping(driver) -> None:
    n = driver.hset("h", "f", b"v", mapping={"m1": b"x", "m2": b"y"})
    assert n == 3
    assert driver.hget("h", "f") == b"v"
    assert driver.hget("h", "m1") == b"x"


def test_hset_empty_raises_data_error(driver) -> None:
    with pytest.raises(DataError, match="at least one"):
        driver.hset("h")


def test_hset_odd_positional_count_raises_data_error(driver) -> None:
    with pytest.raises(DataError, match="even"):
        driver.hset("h", "f1", b"v1", "lonely")


# --- HSETNX --------------------------------------------------------------


def test_hsetnx_inserts_when_absent(driver) -> None:
    assert driver.hsetnx("h", "f", b"v") is True
    assert driver.hget("h", "f") == b"v"


def test_hsetnx_skips_when_present(driver) -> None:
    driver.hset("h", "f", b"original")
    assert driver.hsetnx("h", "f", b"replacement") is False
    assert driver.hget("h", "f") == b"original"


# --- async pair ----------------------------------------------------------


@pytest.mark.asyncio
async def test_ahset_ahget_basic(driver) -> None:
    assert await driver.ahset("h", "f", b"v") == 1
    assert await driver.ahget("h", "f") == b"v"


@pytest.mark.asyncio
async def test_ahset_with_mapping(driver) -> None:
    n = await driver.ahset("h", mapping={"a": b"1", "b": b"2"})
    assert n == 2


@pytest.mark.asyncio
async def test_ahsetnx(driver) -> None:
    assert await driver.ahsetnx("h", "f", b"v") is True
    assert await driver.ahsetnx("h", "f", b"v2") is False


# --- HGETALL -------------------------------------------------------------


def test_hgetall_returns_dict_of_bytes(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    got = driver.hgetall("h")
    assert isinstance(got, dict)
    assert got == {b"a": b"1", b"b": b"2"}


def test_hgetall_missing_key_returns_empty_dict(driver) -> None:
    assert driver.hgetall("missing") == {}


@pytest.mark.asyncio
async def test_ahgetall(driver) -> None:
    await driver.ahset("h", mapping={"x": b"1"})
    assert await driver.ahgetall("h") == {b"x": b"1"}


# --- HMGET ---------------------------------------------------------------


def test_hmget_preserves_order_and_missing_fields(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "c": b"3"})
    got = driver.hmget("h", "a", "b", "c")
    assert got == [b"1", None, b"3"]


def test_hmget_empty_fields_returns_empty_list(driver) -> None:
    driver.hset("h", "f", b"v")
    assert driver.hmget("h") == []


def test_hmget_missing_key_returns_all_none(driver) -> None:
    assert driver.hmget("missing", "a", "b") == [None, None]


@pytest.mark.asyncio
async def test_ahmget(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1"})
    assert await driver.ahmget("h", "a", "b") == [b"1", None]


# --- HMSET (deprecated upstream) -----------------------------------------


def test_hmset_writes_all_fields(driver) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        driver.hmset("h", {"a": b"1", "b": b"2"})
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)
    assert driver.hgetall("h") == {b"a": b"1", b"b": b"2"}


def test_hmset_empty_mapping_raises_data_error(driver) -> None:
    with pytest.raises(DataError, match="empty"):
        driver.hmset("h", {})


@pytest.mark.asyncio
async def test_ahmset(driver) -> None:
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        await driver.ahmset("h", {"a": b"1"})
    assert await driver.ahgetall("h") == {b"a": b"1"}


# --- HDEL ----------------------------------------------------------------


def test_hdel_variadic_returns_count(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    assert driver.hdel("h", "a", "b", "missing") == 2
    assert driver.hgetall("h") == {b"c": b"3"}


def test_hdel_missing_key_returns_zero(driver) -> None:
    assert driver.hdel("missing", "a", "b") == 0


def test_hdel_no_fields_returns_zero(driver) -> None:
    driver.hset("h", "f", b"v")
    assert driver.hdel("h") == 0


@pytest.mark.asyncio
async def test_ahdel(driver) -> None:
    await driver.ahset("h", mapping={"x": b"1", "y": b"2"})
    assert await driver.ahdel("h", "x", "z") == 1


# --- HEXISTS -------------------------------------------------------------


def test_hexists_present(driver) -> None:
    driver.hset("h", "f", b"v")
    assert driver.hexists("h", "f") is True


def test_hexists_absent(driver) -> None:
    driver.hset("h", "f", b"v")
    assert driver.hexists("h", "missing") is False
    assert driver.hexists("missing-key", "f") is False


@pytest.mark.asyncio
async def test_ahexists(driver) -> None:
    await driver.ahset("h", "f", b"v")
    assert await driver.ahexists("h", "f") is True
    assert await driver.ahexists("h", "g") is False


# --- HLEN ----------------------------------------------------------------


def test_hlen(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    assert driver.hlen("h") == 3


def test_hlen_missing_key_is_zero(driver) -> None:
    assert driver.hlen("missing") == 0


@pytest.mark.asyncio
async def test_ahlen(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1"})
    assert await driver.ahlen("h") == 1


# --- HKEYS / HVALS -------------------------------------------------------


def test_hkeys_returns_list_of_bytes(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    keys = driver.hkeys("h")
    assert isinstance(keys, list)
    assert sorted(keys) == [b"a", b"b"]


def test_hkeys_missing_key_returns_empty(driver) -> None:
    assert driver.hkeys("missing") == []


def test_hvals_returns_list_of_bytes(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    vals = driver.hvals("h")
    assert isinstance(vals, list)
    assert sorted(vals) == [b"1", b"2"]


@pytest.mark.asyncio
async def test_ahkeys_ahvals(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1"})
    assert await driver.ahkeys("h") == [b"a"]
    assert await driver.ahvals("h") == [b"1"]


# --- HRANDFIELD ----------------------------------------------------------


def test_hrandfield_no_count_returns_single_bytes(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    got = driver.hrandfield("h")
    assert got in (b"a", b"b", b"c")


def test_hrandfield_missing_returns_none(driver) -> None:
    assert driver.hrandfield("missing") is None


def test_hrandfield_with_positive_count_returns_distinct_list(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    got = driver.hrandfield("h", count=2)
    assert isinstance(got, list)
    assert len(got) == 2
    assert len(set(got)) == 2  # distinct


def test_hrandfield_with_negative_count_allows_repeats(driver) -> None:
    driver.hset("h", "only", b"v")
    got = driver.hrandfield("h", count=-3)
    assert got == [b"only", b"only", b"only"]


def test_hrandfield_withvalues(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    got = driver.hrandfield("h", count=2, withvalues=True)
    assert isinstance(got, list)
    assert all(isinstance(item, tuple) and len(item) == 2 for item in got)
    keys = {pair[0] for pair in got}
    assert keys.issubset({b"a", b"b"})


@pytest.mark.asyncio
async def test_ahrandfield(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1"})
    assert await driver.ahrandfield("h") == b"a"
    assert await driver.ahrandfield("h", count=1) == [b"a"]


# --- HINCRBY -------------------------------------------------------------


def test_hincrby_creates_field_at_zero(driver) -> None:
    assert driver.hincrby("h", "counter", 5) == 5
    assert driver.hget("h", "counter") == b"5"


def test_hincrby_increments_existing(driver) -> None:
    driver.hset("h", "counter", b"10")
    assert driver.hincrby("h", "counter", 7) == 17
    assert driver.hincrby("h", "counter", -3) == 14


def test_hincrby_on_non_integer_raises_response_error(driver) -> None:
    driver.hset("h", "f", b"not-a-number")
    with pytest.raises(ResponseError):
        driver.hincrby("h", "f", 1)


@pytest.mark.asyncio
async def test_ahincrby(driver) -> None:
    assert await driver.ahincrby("h", "c", 5) == 5
    assert await driver.ahincrby("h", "c", 5) == 10


# --- HINCRBYFLOAT --------------------------------------------------------


def test_hincrbyfloat_creates_field(driver) -> None:
    assert driver.hincrbyfloat("h", "f", 1.5) == pytest.approx(1.5)


def test_hincrbyfloat_increments_existing(driver) -> None:
    driver.hset("h", "f", b"3.14")
    assert driver.hincrbyfloat("h", "f", 0.86) == pytest.approx(4.0)


def test_hincrbyfloat_on_non_float_raises(driver) -> None:
    driver.hset("h", "f", b"nope")
    with pytest.raises(ResponseError):
        driver.hincrbyfloat("h", "f", 1.0)


@pytest.mark.asyncio
async def test_ahincrbyfloat(driver) -> None:
    val = await driver.ahincrbyfloat("h", "f", 2.5)
    assert val == pytest.approx(2.5)


# --- HSCAN ---------------------------------------------------------------


def test_hscan_full_iteration(driver) -> None:
    expected = {f"f{i}".encode(): str(i).encode() for i in range(20)}
    driver.hset("h", mapping={k.decode(): v for k, v in expected.items()})

    seen: dict[bytes, bytes] = {}
    cursor = 0
    while True:
        cursor, batch = driver.hscan("h", cursor=cursor)
        assert isinstance(batch, dict)
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == expected


def test_hscan_with_match(driver) -> None:
    driver.hset(
        "h",
        mapping={"foo:1": b"a", "foo:2": b"b", "bar:1": b"c"},
    )
    cursor = 0
    seen: dict[bytes, bytes] = {}
    while True:
        cursor, batch = driver.hscan("h", cursor=cursor, match="foo:*")
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == {b"foo:1": b"a", b"foo:2": b"b"}


def test_hscan_count_is_a_hint(driver) -> None:
    driver.hset("h", mapping={f"k{i}": str(i).encode() for i in range(50)})
    cursor, batch = driver.hscan("h", cursor=0, count=10)
    # COUNT is just a hint — server may return more or fewer.
    assert isinstance(batch, dict)
    # Eventually consume the whole hash.
    seen: dict[bytes, bytes] = dict(batch)
    while cursor != 0:
        cursor, batch = driver.hscan("h", cursor=cursor, count=10)
        seen.update(batch)
    assert len(seen) == 50


def test_hscan_novalues_returns_field_list(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    cursor, batch = driver.hscan("h", cursor=0, novalues=True)
    assert isinstance(batch, list)
    while cursor != 0:
        cursor, more = driver.hscan("h", cursor=cursor, novalues=True)
        batch.extend(more)
    assert sorted(batch) == [b"a", b"b"]


@pytest.mark.asyncio
async def test_ahscan(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1", "b": b"2"})
    _cursor, batch = await driver.ahscan("h", cursor=0)
    assert b"a" in batch and b"b" in batch


# --- Hash-field TTL family (Redis 7.4+) ----------------------------------


def _server_supports_hexpire(driver) -> bool:
    """Version probe — HEXPIRE family requires Redis >= 7.4.

    Note: Valkey does not implement HEXPIRE as of Valkey 8.1.  The test suite
    only enables these tests when the server is plain Redis >= 7.4.
    """
    # Strip RESP3 query-string from the URL — upstream redis-py can't parse it.
    url = driver.connection_url.split("?")[0]
    rp = upstream_redis.Redis.from_url(url)
    try:
        info = rp.info("server")
        # If valkey_version is present this is a Valkey fork — skip.
        if info.get("valkey_version"):
            return False
        version = info.get("redis_version") or "0.0.0"
        major, minor, *_ = (int(x) for x in version.split("-")[0].split("."))
    finally:
        rp.close()
    return (major, minor) >= (7, 4)


@pytest.fixture
def hexpire_driver(driver):
    if not _server_supports_hexpire(driver):
        pytest.skip("hash-field TTL family requires Redis/Valkey >= 7.4")
    return driver


# --- HEXPIRE / HPEXPIRE --------------------------------------------------


def test_hexpire_basic(hexpire_driver) -> None:
    hexpire_driver.hset("h", mapping={"a": b"1", "b": b"2"})
    got = hexpire_driver.hexpire("h", ["a", "b", "missing"], 60)
    assert got == [1, 1, -2]


def test_hexpire_nx_only_sets_when_no_ttl(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    assert hexpire_driver.hexpire("h", ["f"], 60, nx=True) == [1]
    # Second call with NX must report condition not met.
    assert hexpire_driver.hexpire("h", ["f"], 120, nx=True) == [0]


def test_hexpire_xx_only_sets_when_ttl_present(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    assert hexpire_driver.hexpire("h", ["f"], 60, xx=True) == [0]
    hexpire_driver.hexpire("h", ["f"], 60)
    assert hexpire_driver.hexpire("h", ["f"], 120, xx=True) == [1]


def test_hexpire_gt_lt_modifiers(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    hexpire_driver.hexpire("h", ["f"], 60)
    # GT — only set if new > current
    assert hexpire_driver.hexpire("h", ["f"], 30, gt=True) == [0]
    assert hexpire_driver.hexpire("h", ["f"], 120, gt=True) == [1]
    # LT — only set if new < current (current is now 120)
    assert hexpire_driver.hexpire("h", ["f"], 200, lt=True) == [0]
    assert hexpire_driver.hexpire("h", ["f"], 30, lt=True) == [1]


def test_hpexpire_milliseconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    assert hexpire_driver.hpexpire("h", ["f"], 60_000) == [1]


@pytest.mark.asyncio
async def test_ahexpire(hexpire_driver) -> None:
    await hexpire_driver.ahset("h", mapping={"a": b"1"})
    assert await hexpire_driver.ahexpire("h", ["a"], 60) == [1]


# --- HEXPIREAT / HPEXPIREAT ----------------------------------------------


def test_hexpireat_with_unix_seconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    future = int(time.time()) + 600
    assert hexpire_driver.hexpireat("h", ["f"], future) == [1]


def test_hpexpireat_with_unix_milliseconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    future_ms = int(time.time() * 1000) + 600_000
    assert hexpire_driver.hpexpireat("h", ["f"], future_ms) == [1]


# --- HTTL / HPTTL --------------------------------------------------------


def test_httl_returns_seconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", mapping={"a": b"1", "b": b"2"})
    hexpire_driver.hexpire("h", ["a"], 100)
    got = hexpire_driver.httl("h", ["a", "b", "missing"])
    assert got[0] > 0
    assert got[0] <= 100
    assert got[1] == -1  # no TTL set
    assert got[2] == -2  # no such field


def test_hpttl_returns_milliseconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    hexpire_driver.hpexpire("h", ["f"], 50_000)
    got = hexpire_driver.hpttl("h", ["f"])
    assert 0 < got[0] <= 50_000


@pytest.mark.asyncio
async def test_ahttl(hexpire_driver) -> None:
    await hexpire_driver.ahset("h", "f", b"v")
    await hexpire_driver.ahexpire("h", ["f"], 60)
    got = await hexpire_driver.ahttl("h", ["f"])
    assert got[0] > 0


# --- HEXPIRETIME / HPEXPIRETIME ------------------------------------------


def test_hexpiretime_returns_unix_seconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    when = int(time.time()) + 100
    hexpire_driver.hexpireat("h", ["f"], when)
    got = hexpire_driver.hexpiretime("h", ["f", "missing"])
    assert got[0] == when
    assert got[1] == -2


def test_hpexpiretime_returns_unix_milliseconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    when_ms = int(time.time() * 1000) + 100_000
    hexpire_driver.hpexpireat("h", ["f"], when_ms)
    got = hexpire_driver.hpexpiretime("h", ["f"])
    assert abs(got[0] - when_ms) < 1000  # within 1s tolerance


# --- HPERSIST ------------------------------------------------------------


def test_hpersist_removes_ttl(hexpire_driver) -> None:
    hexpire_driver.hset("h", mapping={"a": b"1", "b": b"2"})
    hexpire_driver.hexpire("h", ["a"], 100)
    got = hexpire_driver.hpersist("h", ["a", "b", "missing"])
    assert got == [1, -1, -2]
    # And HTTL should now report -1 for `a`.
    assert hexpire_driver.httl("h", ["a"]) == [-1]


@pytest.mark.asyncio
async def test_ahpersist(hexpire_driver) -> None:
    await hexpire_driver.ahset("h", "f", b"v")
    await hexpire_driver.ahexpire("h", ["f"], 60)
    assert await hexpire_driver.ahpersist("h", ["f"]) == [1]
