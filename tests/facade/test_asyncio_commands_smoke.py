"""Smoke tests for every asyncio façade command method."""

import pytest
from redis_rs_py.asyncio import Redis


@pytest.fixture
async def r(valkey_url: str):
    import redis as upstream

    rp = upstream.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    client = Redis.from_url(valkey_url)
    yield client
    await client.aclose()


# --- strings --------------------------------------------------------------


@pytest.mark.asyncio
async def test_string_get_set(r: Redis) -> None:
    await r.set("k", b"v")
    assert await r.get("k") == b"v"


@pytest.mark.asyncio
async def test_string_get_set_with_ex(r: Redis) -> None:
    await r.set("k", b"v", ex=60)
    assert 0 < await r.ttl("k") <= 60


@pytest.mark.asyncio
async def test_string_getex_getdel(r: Redis) -> None:
    await r.set("k", b"v")
    assert await r.getex("k", ex=30) == b"v"
    assert await r.getdel("k") == b"v"
    assert await r.get("k") is None


@pytest.mark.asyncio
async def test_string_copy(r: Redis) -> None:
    await r.set("a", b"v")
    await r.copy("a", "b")
    assert await r.get("b") == b"v"


@pytest.mark.asyncio
async def test_string_incr_decr(r: Redis) -> None:
    assert await r.incr("c") == 1
    assert await r.incrby("c", 4) == 5
    assert await r.decr("c") == 4
    assert await r.decrby("c", 2) == 2


@pytest.mark.asyncio
async def test_string_incrbyfloat(r: Redis) -> None:
    assert await r.incrbyfloat("f", 1.5) == 1.5


@pytest.mark.asyncio
async def test_string_append_strlen(r: Redis) -> None:
    await r.set("k", b"hello")
    assert await r.append("k", b" world") == 11
    assert await r.strlen("k") == 11


@pytest.mark.asyncio
async def test_string_mget_mset_msetnx(r: Redis) -> None:
    await r.mset({"a": b"1", "b": b"2"})
    assert await r.mget("a", "b") == [b"1", b"2"]
    assert await r.msetnx({"x": b"1"}) in (True, 1)
    assert await r.msetnx({"x": b"2"}) in (False, 0)


@pytest.mark.asyncio
async def test_string_setrange_getrange(r: Redis) -> None:
    await r.set("k", b"hello world")
    await r.setrange("k", 6, b"REDIS")
    assert await r.getrange("k", 0, -1) == b"hello REDIS"


@pytest.mark.asyncio
async def test_string_exists_delete_unlink(r: Redis) -> None:
    await r.set("a", b"1")
    await r.set("b", b"2")
    assert await r.exists("a", "b", "c") == 2
    assert await r.delete("a") == 1
    assert await r.unlink("b") == 1


@pytest.mark.asyncio
async def test_string_expire_persist(r: Redis) -> None:
    await r.set("k", b"v")
    await r.expire("k", 100)
    assert 0 < await r.ttl("k") <= 100
    await r.persist("k")
    assert await r.ttl("k") in (-1, None)


@pytest.mark.asyncio
async def test_string_pexpire_pttl(r: Redis) -> None:
    await r.set("k", b"v")
    await r.pexpire("k", 100_000)
    assert 0 < await r.pttl("k") <= 100_000


@pytest.mark.asyncio
async def test_string_expireat_pexpireat(r: Redis) -> None:
    import time

    await r.set("k", b"v")
    await r.expireat("k", int(time.time()) + 100)
    await r.set("k2", b"v")
    await r.pexpireat("k2", int(time.time() * 1000) + 100_000)
    assert await r.ttl("k") <= 100
    assert await r.pttl("k2") <= 100_000


@pytest.mark.asyncio
async def test_string_expiretime_pexpiretime(r: Redis) -> None:
    await r.set("k", b"v")
    await r.expire("k", 100)
    assert await r.expiretime("k") > 0
    assert await r.pexpiretime("k") > 0


@pytest.mark.asyncio
async def test_string_rename_renamenx(r: Redis) -> None:
    await r.set("a", b"v")
    await r.rename("a", "b")
    assert await r.get("b") == b"v"
    await r.set("a", b"x")
    assert await r.renamenx("a", "b") in (False, 0)


@pytest.mark.asyncio
async def test_string_type(r: Redis) -> None:
    await r.set("k", b"v")
    assert await r.type("k") in (b"string", "string")


@pytest.mark.asyncio
async def test_string_dump_restore(r: Redis) -> None:
    await r.set("k", b"v")
    blob = await r.dump("k")
    assert blob is not None
    await r.delete("k")
    await r.restore("k", 0, blob)
    assert await r.get("k") == b"v"


@pytest.mark.asyncio
async def test_ping(r: Redis) -> None:
    assert await r.ping() is True


# --- lists ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_push_pop(r: Redis) -> None:
    await r.rpush("L", b"a", b"b", b"c")
    assert await r.llen("L") == 3
    assert await r.lpop("L") == b"a"
    assert await r.rpop("L") == b"c"


@pytest.mark.asyncio
async def test_list_lpushx_rpushx(r: Redis) -> None:
    await r.lpush("L", b"a")
    await r.lpushx("L", b"b")
    await r.rpushx("L", b"c")
    assert await r.lrange("L", 0, -1) == [b"b", b"a", b"c"]


@pytest.mark.asyncio
async def test_list_lmove_lpos_lrem(r: Redis) -> None:
    await r.rpush("S", b"a", b"b", b"a")
    await r.lmove("S", "D", "LEFT", "RIGHT")
    assert await r.lpos("S", b"a") == 1
    assert await r.lrem("S", 1, b"a") == 1


@pytest.mark.asyncio
async def test_list_lindex_lset_linsert_ltrim(r: Redis) -> None:
    await r.rpush("L", b"a", b"c")
    await r.linsert("L", "BEFORE", b"c", b"b")
    assert await r.lindex("L", 1) == b"b"
    await r.lset("L", 0, b"X")
    await r.ltrim("L", 0, 1)
    assert await r.llen("L") == 2


@pytest.mark.asyncio
async def test_list_blpop_brpop_immediate(r: Redis) -> None:
    await r.rpush("L", b"x", b"y")
    assert await r.blpop(["L"], timeout=1.0) == (b"L", b"x")
    assert await r.brpop(["L"], timeout=1.0) == (b"L", b"y")


@pytest.mark.asyncio
async def test_list_blmove_lmpop_blmpop(r: Redis) -> None:
    await r.rpush("S", b"a")
    await r.blmove("S", "D", "LEFT", "RIGHT", timeout=1.0)
    await r.rpush("L", b"a", b"b")
    assert await r.lmpop(["L"], direction="LEFT", count=2) is not None
    assert await r.blmpop(timeout=1.0, keys=["empty"], direction="LEFT", count=1) is None


# --- hashes ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_hash_set_get(r: Redis) -> None:
    await r.hset("H", "f", b"v")
    assert await r.hget("H", "f") == b"v"


@pytest.mark.asyncio
async def test_hash_hsetnx(r: Redis) -> None:
    assert await r.hsetnx("H", "f", b"v") in (True, 1)
    assert await r.hsetnx("H", "f", b"x") in (False, 0)


@pytest.mark.asyncio
async def test_hash_hmset_hmget_hgetall(r: Redis) -> None:
    await r.hmset("H", {"a": b"1", "b": b"2"})
    assert await r.hmget("H", "a", "b") == [b"1", b"2"]
    assert await r.hgetall("H") == {b"a": b"1", b"b": b"2"}


@pytest.mark.asyncio
async def test_hash_hdel_hexists_hlen_hkeys_hvals(r: Redis) -> None:
    await r.hmset("H", {"a": b"1", "b": b"2"})
    assert await r.hexists("H", "a") in (True, 1)
    assert await r.hdel("H", "a") == 1
    assert await r.hlen("H") == 1
    assert sorted(await r.hkeys("H")) == [b"b"]
    assert sorted(await r.hvals("H")) == [b"2"]


@pytest.mark.asyncio
async def test_hash_hincrby_hincrbyfloat(r: Redis) -> None:
    assert await r.hincrby("H", "n", 4) == 4
    assert await r.hincrbyfloat("H", "f", 1.5) == 1.5


@pytest.mark.asyncio
async def test_hash_hscan_hrandfield(r: Redis) -> None:
    await r.hmset("H", {"a": b"1", "b": b"2"})
    cursor, batch = await r.hscan("H")
    assert cursor == 0
    assert len(batch) == 2
    val = await r.hrandfield("H")
    assert val in (b"a", b"b")


# --- sets -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_add_card_members(r: Redis) -> None:
    assert await r.sadd("S", b"a", b"b", b"c") == 3
    assert await r.scard("S") == 3
    assert set(await r.smembers("S")) == {b"a", b"b", b"c"}


@pytest.mark.asyncio
async def test_set_ismember_smismember(r: Redis) -> None:
    await r.sadd("S", b"a")
    assert await r.sismember("S", b"a") in (True, 1)
    assert await r.smismember("S", b"a", b"x") in ([True, False], [1, 0])


@pytest.mark.asyncio
async def test_set_inter_union_diff_store(r: Redis) -> None:
    await r.sadd("A", b"a", b"b")
    await r.sadd("B", b"b", b"c")
    assert set(await r.sinter("A", "B")) == {b"b"}
    assert set(await r.sunion("A", "B")) == {b"a", b"b", b"c"}
    assert set(await r.sdiff("A", "B")) == {b"a"}
    assert await r.sinterstore("X", "A", "B") == 1
    assert await r.sunionstore("Y", "A", "B") == 3
    assert await r.sdiffstore("Z", "A", "B") == 1


@pytest.mark.asyncio
async def test_set_intercard_smove_spop_srandmember_sscan(r: Redis) -> None:
    await r.sadd("A", b"a", b"b")
    await r.sadd("B", b"a")
    assert await r.sintercard("A", "B") == 1
    await r.smove("A", "B", b"b")
    assert (await r.spop("B")) in (b"a", b"b")
    assert (await r.srandmember("B")) in (b"a", b"b", None)
    cursor, _batch = await r.sscan("B")
    assert cursor == 0


# --- zsets ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_zset_basic(r: Redis) -> None:
    await r.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert await r.zcard("Z") == 3
    assert await r.zscore("Z", b"b") == 2.0
    assert await r.zrange("Z", 0, -1) == [b"a", b"b", b"c"]
    assert await r.zrange("Z", 0, -1, withscores=True) == [
        (b"a", 1.0),
        (b"b", 2.0),
        (b"c", 3.0),
    ]


@pytest.mark.asyncio
async def test_zset_rev_zincrby_zrank(r: Redis) -> None:
    await r.zadd("Z", {"a": 1, "b": 2})
    assert await r.zrange("Z", 0, -1, desc=True) == [b"b", b"a"]
    assert await r.zincrby("Z", 3.0, b"a") == 4.0
    assert await r.zrank("Z", b"b") == 0


@pytest.mark.asyncio
async def test_zset_zrem_zpopmin_zpopmax(r: Redis) -> None:
    await r.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert await r.zrem("Z", b"b") == 1
    assert (await r.zpopmin("Z"))[0] == (b"a", 1.0)
    assert (await r.zpopmax("Z"))[0] == (b"c", 3.0)


@pytest.mark.asyncio
async def test_zset_zmscore_zcount_zscan(r: Redis) -> None:
    await r.zadd("Z", {"a": 1, "b": 2})
    assert await r.zmscore("Z", b"a", b"x") == [1.0, None]
    assert await r.zcount("Z", "1", "2") == 2
    cursor, _batch = await r.zscan("Z")
    assert cursor == 0


@pytest.mark.asyncio
async def test_zset_set_ops_store(r: Redis) -> None:
    await r.zadd("A", {"a": 1, "b": 2})
    await r.zadd("B", {"b": 3, "c": 4})
    assert await r.zunionstore("U", ["A", "B"]) == 3
    assert await r.zinterstore("I", ["A", "B"]) == 1
    assert await r.zdiffstore("D", ["A", "B"]) == 1


# --- streams --------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_xadd_xlen_xrange(r: Redis) -> None:
    id1 = await r.xadd("S", "*", [("f", b"1")])
    id2 = await r.xadd("S", "*", [("f", b"2")])
    assert await r.xlen("S") == 2
    rng = await r.xrange("S", "-", "+")
    assert len(rng) == 2
    # xadd returns str IDs; xrange/xrevrange return bytes IDs — compare via encode
    assert (await r.xrevrange("S", "+", "-"))[0][0] == id2.encode()
    assert (await r.xrange("S", id1, id1))[0][0] == id1.encode()


@pytest.mark.asyncio
async def test_stream_xread_xreadgroup_xack(r: Redis) -> None:
    await r.xadd("S", "*", [("f", b"v")])
    await r.xgroup_create("S", "G", id="0", mkstream=False)
    msgs = await r.xreadgroup("G", "C1", {"S": ">"})
    assert msgs
    first = (await r.xrange("S", "-", "+"))[0][0]  # bytes entry ID
    assert await r.xack("S", "G", first.decode()) == 1  # xack takes str IDs


@pytest.mark.asyncio
async def test_stream_xdel_xtrim(r: Redis) -> None:
    id1 = await r.xadd("S", "*", [("f", b"1")])
    await r.xadd("S", "*", [("f", b"2")])
    assert await r.xdel("S", id1) == 1
    await r.xtrim("S", maxlen=1, approximate=False)
    assert await r.xlen("S") <= 1


@pytest.mark.asyncio
async def test_stream_xinfo_xpending(r: Redis) -> None:
    await r.xadd("S", "*", [("f", b"v")])
    await r.xgroup_create("S", "G", id="0", mkstream=False)
    await r.xreadgroup("G", "C1", {"S": ">"})
    assert await r.xinfo_stream("S")
    assert await r.xinfo_groups("S")
    assert await r.xpending("S", "G")


@pytest.mark.asyncio
async def test_stream_xclaim_xautoclaim_xsetid(r: Redis) -> None:
    id1 = await r.xadd("S", "*", [("f", b"v")])
    await r.xgroup_create("S", "G", id="0", mkstream=False)
    await r.xreadgroup("G", "C1", {"S": ">"})
    assert await r.xclaim("S", "G", "C2", min_idle_time=0, message_ids=[id1])
    assert await r.xautoclaim("S", "G", "C3", min_idle_time=0) is not None
    # xsetid requires an ID >= the current last entry; use a far-future timestamp
    await r.xsetid("S", "99999999999999-0")


# --- scripts + admin ------------------------------------------------------


@pytest.mark.asyncio
async def test_scripts_eval_evalsha(r: Redis) -> None:
    sha = await r.script_load("return KEYS[1]")
    assert await r.evalsha(sha, ["hello"], []) == b"hello"
    assert await r.eval("return 1", [], []) == 1


@pytest.mark.asyncio
async def test_admin_scan_keys_dbsize(r: Redis) -> None:
    await r.set("k1", b"v")
    await r.set("k2", b"v")
    cursor, _batch = await r.scan()
    assert cursor == 0
    keys = await r.keys("k*")
    assert set(keys) >= {b"k1", b"k2"} or set(keys) >= {"k1", "k2"}
    assert await r.dbsize() >= 2


@pytest.mark.asyncio
async def test_admin_info_config(r: Redis) -> None:
    info = await r.info()
    assert info
    cfg = await r.config_get("maxmemory")
    assert cfg is not None
    await r.config_resetstat()


@pytest.mark.asyncio
async def test_admin_client_apis(r: Redis) -> None:
    await r.client_setname("test-client")
    assert await r.client_getname() in (b"test-client", "test-client")
    assert await r.client_id() > 0
    assert await r.client_list()


@pytest.mark.asyncio
async def test_admin_object_memory(r: Redis) -> None:
    await r.set("k", b"v")
    assert await r.object_encoding("k") is not None
    assert await r.memory_usage("k") > 0


# BGSAVE / BGREWRITEAOF are server-global — only one can run at a time, so
# parallel workers would race each other. Pin to a single worker.
@pytest.mark.xdist_group(name="redis_global_state")
@pytest.mark.asyncio
async def test_admin_basic(r: Redis) -> None:
    assert await r.echo(b"hi") == b"hi"
    t = await r.time()
    assert isinstance(t, (list, tuple)) and len(t) == 2
    await r.lastsave()
    await r.bgsave()
    await r.bgrewriteaof()
