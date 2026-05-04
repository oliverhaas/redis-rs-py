"""Smoke tests: every command family fires at least one live round-trip."""

import pytest


@pytest.fixture
def r(valkey_url: str):
    """A connected Redis façade that flushes the DB before and after each test."""
    from redis_rs_py import Redis

    client = Redis.from_url(valkey_url)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


# ---------------------------------------------------------------------------
# String / key commands
# ---------------------------------------------------------------------------


def test_set_get(r):
    assert r.set("k", b"v") is True
    assert r.get("k") == b"v"


def test_set_ex(r):
    r.set("k", b"v", ex=100)
    assert 0 < r.ttl("k") <= 100


def test_set_nx(r):
    assert r.set("k", b"a", nx=True) is True
    assert r.set("k", b"b", nx=True) is None or r.set("k", b"b", nx=True) is False


def test_set_xx(r):
    assert r.set("missing", b"v", xx=True) is None or r.set("missing", b"v", xx=True) is False
    r.set("k", b"a")
    r.set("k", b"b", xx=True)
    assert r.get("k") == b"b"


def test_getex(r):
    r.set("k", b"v")
    assert r.getex("k", ex=60) == b"v"
    assert r.ttl("k") > 0


def test_getdel(r):
    r.set("k", b"v")
    assert r.getdel("k") == b"v"
    assert r.get("k") is None


def test_incr_decr(r):
    r.set("n", b"10")
    assert r.incr("n") == 11
    assert r.incrby("n", 4) == 15
    assert r.decr("n") == 14
    assert r.decrby("n", 3) == 11


def test_incrbyfloat(r):
    r.set("f", b"1.5")
    val = r.incrbyfloat("f", 0.5)
    assert abs(val - 2.0) < 1e-9


def test_append_strlen(r):
    r.set("k", b"hello")
    r.append("k", b" world")
    assert r.strlen("k") == 11


def test_mget_mset(r):
    r.mset({"a": b"1", "b": b"2"})
    assert r.mget(["a", "b", "missing"]) == [b"1", b"2", None]


def test_exists(r):
    r.set("k", b"v")
    assert r.exists("k") == 1
    assert r.exists("missing") == 0


def test_delete(r):
    r.set("k", b"v")
    assert r.delete("k") == 1
    assert r.get("k") is None


def test_unlink(r):
    r.set("k", b"v")
    assert r.unlink("k") == 1


def test_expire_ttl(r):
    r.set("k", b"v")
    r.expire("k", 100)
    ttl = r.ttl("k")
    assert 0 < ttl <= 100


def test_persist(r):
    r.set("k", b"v")
    r.expire("k", 100)
    r.persist("k")
    assert r.ttl("k") == -1


def test_rename(r):
    r.set("src", b"v")
    r.rename("src", "dst")
    assert r.get("dst") == b"v"
    assert r.get("src") is None


def test_type(r):
    r.set("k", b"v")
    assert r.type("k") == "string"


def test_randomkey(r):
    r.set("k", b"v")
    key = r.randomkey()
    assert key is not None


def test_dbsize(r):
    r.set("a", b"1")
    r.set("b", b"2")
    assert r.dbsize() >= 2


def test_keys_pattern(r):
    r.set("prefix:1", b"a")
    r.set("prefix:2", b"b")
    ks = r.keys("prefix:*")
    assert len(ks) == 2


def test_scan(r):
    r.set("s1", b"v")
    r.set("s2", b"v")
    cursor, keys = r.scan(0)
    assert isinstance(cursor, int)
    assert isinstance(keys, list)


def test_copy(r):
    r.set("src", b"v")
    ok = r.copy("src", "dst")
    assert ok is True
    assert r.get("dst") == b"v"


# ---------------------------------------------------------------------------
# List commands
# ---------------------------------------------------------------------------


def test_lpush_rpush_lrange(r):
    r.rpush("lst", b"a", b"b", b"c")
    assert r.lrange("lst", 0, -1) == [b"a", b"b", b"c"]


def test_lpop_rpop(r):
    r.rpush("lst", b"a", b"b", b"c")
    assert r.lpop("lst") == b"a"
    assert r.rpop("lst") == b"c"


def test_lpop_count(r):
    r.rpush("lst", b"a", b"b", b"c")
    items = r.lpop("lst", 2)
    assert items == [b"a", b"b"]


def test_llen(r):
    r.rpush("lst", b"a", b"b")
    assert r.llen("lst") == 2


def test_lindex(r):
    r.rpush("lst", b"a", b"b", b"c")
    assert r.lindex("lst", 1) == b"b"


def test_lset(r):
    r.rpush("lst", b"a", b"b")
    r.lset("lst", 0, b"x")
    assert r.lindex("lst", 0) == b"x"


def test_linsert(r):
    r.rpush("lst", b"a", b"c")
    r.linsert("lst", "before", b"c", b"b")
    assert r.lrange("lst", 0, -1) == [b"a", b"b", b"c"]


def test_ltrim(r):
    r.rpush("lst", b"a", b"b", b"c")
    r.ltrim("lst", 1, 2)
    assert r.lrange("lst", 0, -1) == [b"b", b"c"]


def test_lrem(r):
    r.rpush("lst", b"a", b"a", b"b")
    removed = r.lrem("lst", 2, b"a")
    assert removed == 2


def test_lmove(r):
    r.rpush("src", b"a", b"b")
    val = r.lmove("src", "dst", "LEFT", "RIGHT")
    assert val == b"a"
    assert r.lrange("dst", 0, -1) == [b"a"]


# ---------------------------------------------------------------------------
# Hash commands
# ---------------------------------------------------------------------------


def test_hset_hget(r):
    r.hset("h", "f", b"v")
    assert r.hget("h", "f") == b"v"


def test_hset_mapping(r):
    r.hset("h", mapping={"a": b"1", "b": b"2"})
    assert r.hget("h", "a") == b"1"


def test_hgetall(r):
    r.hset("h", mapping={"a": b"1", "b": b"2"})
    d = r.hgetall("h")
    assert d == {b"a": b"1", b"b": b"2"}


def test_hdel(r):
    r.hset("h", mapping={"a": b"1", "b": b"2"})
    assert r.hdel("h", "a") == 1
    assert r.hget("h", "a") is None


def test_hincrby(r):
    r.hset("h", "n", b"10")
    assert r.hincrby("h", "n", 5) == 15


def test_hkeys_hvals_hlen(r):
    r.hset("h", mapping={"a": b"1", "b": b"2"})
    assert set(r.hkeys("h")) == {b"a", b"b"}
    assert set(r.hvals("h")) == {b"1", b"2"}
    assert r.hlen("h") == 2


def test_hexists(r):
    r.hset("h", "f", b"v")
    assert r.hexists("h", "f") is True
    assert r.hexists("h", "missing") is False


def test_hmget(r):
    r.hset("h", mapping={"a": b"1", "b": b"2"})
    assert r.hmget("h", ["a", "b", "missing"]) == [b"1", b"2", None]


def test_hscan(r):
    r.hset("h", mapping={"a": b"1", "b": b"2"})
    cursor, data = r.hscan("h")
    assert isinstance(cursor, int)
    assert b"a" in data


# ---------------------------------------------------------------------------
# Set commands
# ---------------------------------------------------------------------------


def test_sadd_smembers(r):
    r.sadd("s", b"a", b"b", b"c")
    assert r.smembers("s") == {b"a", b"b", b"c"}


def test_srem(r):
    r.sadd("s", b"a", b"b")
    assert r.srem("s", b"a") == 1
    assert r.sismember("s", b"a") is False


def test_scard(r):
    r.sadd("s", b"a", b"b", b"c")
    assert r.scard("s") == 3


def test_sunion_sinter_sdiff(r):
    r.sadd("s1", b"a", b"b", b"c")
    r.sadd("s2", b"b", b"c", b"d")
    assert r.sunion("s1", "s2") == {b"a", b"b", b"c", b"d"}
    assert r.sinter("s1", "s2") == {b"b", b"c"}
    assert r.sdiff("s1", "s2") == {b"a"}


def test_smove(r):
    r.sadd("src", b"a", b"b")
    r.smove("src", "dst", b"a")
    assert r.sismember("src", b"a") is False
    assert r.sismember("dst", b"a") is True


def test_spop(r):
    r.sadd("s", b"a", b"b", b"c")
    v = r.spop("s")
    assert v in {b"a", b"b", b"c"}


def test_srandmember(r):
    r.sadd("s", b"a", b"b", b"c")
    v = r.srandmember("s")
    assert v in {b"a", b"b", b"c"}


def test_sscan(r):
    r.sadd("s", b"a", b"b", b"c")
    cursor, members = r.sscan("s")
    assert isinstance(cursor, int)
    assert isinstance(members, (list, set))


# ---------------------------------------------------------------------------
# Sorted set commands
# ---------------------------------------------------------------------------


def test_zadd_zrange(r):
    r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
    members = r.zrange("z", 0, -1)
    assert members == [b"a", b"b", b"c"]


def test_zadd_withscores(r):
    r.zadd("z", {"a": 1.0, "b": 2.0})
    pairs = r.zrange("z", 0, -1, withscores=True)
    assert (b"a", 1.0) in pairs


def test_zrem_zcard(r):
    r.zadd("z", {"a": 1.0, "b": 2.0})
    r.zrem("z", b"a")
    assert r.zcard("z") == 1


def test_zincrby(r):
    r.zadd("z", {"a": 1.0})
    val = r.zincrby("z", 2.5, b"a")
    assert abs(val - 3.5) < 1e-9


def test_zscore(r):
    r.zadd("z", {"a": 1.5})
    assert abs(r.zscore("z", b"a") - 1.5) < 1e-9


def test_zrank_zrevrank(r):
    r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert r.zrank("z", b"a") == 0
    assert r.zrevrank("z", b"a") == 2


def test_zcount_zrangebyscore(r):
    r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert r.zcount("z", "1", "2") == 2
    members = r.zrangebyscore("z", "1", "2")
    assert set(members) == {b"a", b"b"}


def test_zpopmin_zpopmax(r):
    r.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert r.zpopmin("z") == [(b"a", 1.0)]
    assert r.zpopmax("z") == [(b"c", 3.0)]


def test_zscan(r):
    r.zadd("z", {"a": 1.0, "b": 2.0})
    cursor, pairs = r.zscan("z")
    assert isinstance(cursor, int)
    assert isinstance(pairs, list)


def test_zunionstore_zinterstore(r):
    r.zadd("z1", {"a": 1.0, "b": 2.0})
    r.zadd("z2", {"b": 3.0, "c": 4.0})
    assert r.zunionstore("out", ["z1", "z2"]) == 3
    assert r.zinterstore("out2", ["z1", "z2"]) == 1


# ---------------------------------------------------------------------------
# Stream commands
# ---------------------------------------------------------------------------


def test_xadd_xlen_xrange(r):
    entry_id = r.xadd("stream", {"field": b"value"})
    assert entry_id is not None
    assert r.xlen("stream") == 1
    entries = r.xrange("stream")
    assert len(entries) == 1


def test_xread(r):
    r.xadd("stream", {"k": b"v"})
    result = r.xread({"stream": "0"})
    assert result is not None
    assert len(result) == 1


def test_xgroup_create_destroy(r):
    r.xadd("stream", {"k": b"v"})
    r.xgroup_create("stream", "grp", "$")
    assert r.xgroup_destroy("stream", "grp") == 1


def test_xdel(r):
    eid = r.xadd("stream", {"k": b"v"})
    assert r.xdel("stream", eid) == 1


def test_xtrim(r):
    for i in range(5):
        r.xadd("stream", {"i": str(i).encode()})
    trimmed = r.xtrim("stream", maxlen=3, approximate=False)
    assert trimmed >= 0
    assert r.xlen("stream") <= 3


# ---------------------------------------------------------------------------
# Scripting / admin commands
# ---------------------------------------------------------------------------


def test_eval(r):
    result = r.eval("return 'hello'", [], [])
    assert result == b"hello"


def test_eval_keys_argv(r):
    r.set("k", b"42")
    result = r.eval("return redis.call('GET', KEYS[1])", ["k"], [])
    assert result == b"42"


def test_script_load_evalsha(r):
    sha = r.script_load("return 'loaded'")
    assert isinstance(sha, str) and len(sha) == 40
    result = r.evalsha(sha, [], [])
    assert result == b"loaded"


def test_script_exists(r):
    sha = r.script_load("return 1")
    sha_str = sha
    exists = r.script_exists(sha_str)
    assert exists == [True]


def test_script_flush(r):
    r.script_flush()


def test_ping_no_message(r):
    assert r.ping() is True


def test_ping_with_message(r):
    result = r.ping(message="hello")
    assert result == b"hello"


def test_echo(r):
    assert r.echo("hello") == b"hello"


def test_dbsize_flushdb(r):
    r.set("a", b"1")
    r.set("b", b"2")
    assert r.dbsize() >= 2
    r.flushdb()
    assert r.dbsize() == 0


def test_info(r):
    info = r.info()
    assert info is not None


def test_time(r):
    ts = r.time()
    assert isinstance(ts, tuple)
    assert len(ts) == 2
    assert ts[0] > 0
