"""Verifiers for the sorted-sets command family."""

from . import verifier


@verifier("ZADD")
def _verify_zadd(rs, py) -> None:
    assert rs.zadd("Z", {b"a": 1, b"b": 2}) == py.zadd("Z_py", {b"a": 1, b"b": 2}) == 2


@verifier("ZREM")
def _verify_zrem(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    assert rs.zrem("Z", b"a") == 1


@verifier("ZRANGE")
def _verify_zrange(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2, b"c": 3})
    assert rs.zrange("Z", 0, -1) == py.zrange("Z", 0, -1) == [b"a", b"b", b"c"]
    assert rs.zrange("Z", 0, -1, withscores=True) == py.zrange("Z", 0, -1, withscores=True)


@verifier("ZRANGEBYSCORE")
def _verify_zrangebyscore(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2, b"c": 3})
    # rs.zrangebyscore takes min/max as strings
    assert rs.zrangebyscore("Z", "1", "2") == py.zrangebyscore("Z", 1, 2) == [b"a", b"b"]


@verifier("ZREVRANGEBYSCORE")
def _verify_zrevrangebyscore(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    # rs.zrevrangebyscore takes max/min as strings
    assert rs.zrevrangebyscore("Z", "2", "1") == py.zrevrangebyscore("Z", 2, 1) == [b"b", b"a"]


@verifier("ZRANGEBYLEX")
def _verify_zrangebylex(rs, py) -> None:
    py.zadd("Z", {b"a": 0, b"b": 0, b"c": 0})
    assert rs.zrangebylex("Z", "[a", "[b") == py.zrangebylex("Z", "[a", "[b") == [b"a", b"b"]


@verifier("ZREVRANGEBYLEX")
def _verify_zrevrangebylex(rs, py) -> None:
    py.zadd("Z", {b"a": 0, b"b": 0, b"c": 0})
    assert rs.zrevrangebylex("Z", "[b", "[a") == py.zrevrangebylex("Z", "[b", "[a") == [b"b", b"a"]


@verifier("ZRANGESTORE")
def _verify_zrangestore(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2, b"c": 3})
    assert rs.zrangestore("DST", "Z", 0, -1) == 3


@verifier("ZINCRBY")
def _verify_zincrby(rs, py) -> None:
    py.zadd("Z", {b"a": 1})
    assert rs.zincrby("Z", 5, b"a") == 6.0
    assert py.zincrby("Z_py", 6, b"a") == 6.0


@verifier("ZCARD")
def _verify_zcard(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    assert rs.zcard("Z") == py.zcard("Z") == 2


@verifier("ZSCORE")
def _verify_zscore(rs, py) -> None:
    py.zadd("Z", {b"a": 1.5})
    assert rs.zscore("Z", b"a") == py.zscore("Z", b"a") == 1.5


@verifier("ZMSCORE")
def _verify_zmscore(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    # rs.zmscore uses (*members) variadic, not a list argument
    assert rs.zmscore("Z", b"a", b"b", b"missing") == [1.0, 2.0, None]
    assert py.zmscore("Z", [b"a", b"b", b"missing"]) == [1.0, 2.0, None]


@verifier("ZRANK")
def _verify_zrank(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    assert rs.zrank("Z", b"a") == py.zrank("Z", b"a") == 0


@verifier("ZREVRANK")
def _verify_zrevrank(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    assert rs.zrevrank("Z", b"a") == py.zrevrank("Z", b"a") == 1


@verifier("ZREMRANGEBYRANK")
def _verify_zremrangebyrank(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2, b"c": 3})
    assert rs.zremrangebyrank("Z", 0, 0) == 1


@verifier("ZREMRANGEBYSCORE")
def _verify_zremrangebyscore(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    # rs.zremrangebyscore takes min/max as strings
    assert rs.zremrangebyscore("Z", "1", "1") == 1


@verifier("ZREMRANGEBYLEX")
def _verify_zremrangebylex(rs, py) -> None:
    py.zadd("Z", {b"a": 0, b"b": 0})
    assert rs.zremrangebylex("Z", "[a", "[a") == 1


@verifier("ZCOUNT")
def _verify_zcount(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    # rs.zcount takes min/max as strings
    assert rs.zcount("Z", "1", "2") == py.zcount("Z", 1, 2) == 2


@verifier("ZLEXCOUNT")
def _verify_zlexcount(rs, py) -> None:
    py.zadd("Z", {b"a": 0, b"b": 0})
    assert rs.zlexcount("Z", "[a", "[b") == py.zlexcount("Z", "[a", "[b") == 2


@verifier("ZPOPMIN")
def _verify_zpopmin(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    assert rs.zpopmin("Z") == [(b"a", 1.0)]


@verifier("ZPOPMAX")
def _verify_zpopmax(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    assert rs.zpopmax("Z") == [(b"b", 2.0)]


@verifier("BZPOPMIN")
def _verify_bzpopmin(rs, py) -> None:
    py.zadd("Z", {b"a": 1})
    # rs.bzpopmin(*keys, timeout=)
    assert rs.bzpopmin("Z", timeout=1) == (b"Z", b"a", 1.0)


@verifier("BZPOPMAX")
def _verify_bzpopmax(rs, py) -> None:
    py.zadd("Z", {b"a": 1})
    assert rs.bzpopmax("Z", timeout=1) == (b"Z", b"a", 1.0)


@verifier("ZMPOP")
def _verify_zmpop(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    # rs.zmpop(*keys, direction=, count=)
    rs_out = rs.zmpop("Z", direction="MIN", count=1)
    assert rs_out is not None


@verifier("BZMPOP")
def _verify_bzmpop(rs, py) -> None:
    py.zadd("Z", {b"a": 1})
    # rs.bzmpop(*keys, direction=, timeout=, count=)
    rs_out = rs.bzmpop("Z", direction="MIN", timeout=0.1, count=1)
    assert rs_out is not None


@verifier("ZSCAN")
def _verify_zscan(rs, py) -> None:
    py.zadd("Z", {f"m{i}".encode(): float(i) for i in range(20)})
    _rs_cursor, rs_data = rs.zscan("Z", cursor=0, count=100)
    _py_cursor, py_data = py.zscan("Z", 0, count=100)
    assert sorted(rs_data) == sorted(py_data)


@verifier("ZUNIONSTORE")
def _verify_zunionstore(rs, py) -> None:
    py.zadd("A", {b"a": 1})
    py.zadd("B", {b"b": 2})
    assert rs.zunionstore("DST", ["A", "B"]) == 2


@verifier("ZINTERSTORE")
def _verify_zinterstore(rs, py) -> None:
    py.zadd("A", {b"a": 1, b"b": 2})
    py.zadd("B", {b"b": 3})
    assert rs.zinterstore("DST", ["A", "B"]) == 1


@verifier("ZDIFFSTORE")
def _verify_zdiffstore(rs, py) -> None:
    py.zadd("A", {b"a": 1, b"b": 2})
    py.zadd("B", {b"b": 3})
    assert rs.zdiffstore("DST", ["A", "B"]) == 1


@verifier("ZUNION")
def _verify_zunion(rs, py) -> None:
    py.zadd("A", {b"a": 1})
    py.zadd("B", {b"b": 2})
    # rs.zunion(keys=, ...)
    assert sorted(rs.zunion(keys=["A", "B"])) == sorted(py.zunion(["A", "B"]))


@verifier("ZINTER")
def _verify_zinter(rs, py) -> None:
    py.zadd("A", {b"a": 1, b"b": 2})
    py.zadd("B", {b"b": 3})
    # rs.zinter(keys=, ...)
    assert rs.zinter(keys=["A", "B"]) == py.zinter(["A", "B"]) == [b"b"]


@verifier("ZDIFF")
def _verify_zdiff(rs, py) -> None:
    py.zadd("A", {b"a": 1, b"b": 2})
    py.zadd("B", {b"b": 3})
    # rs.zdiff(keys=, ...)
    assert rs.zdiff(keys=["A", "B"]) == py.zdiff(["A", "B"]) == [b"a"]


# ---------------------------------------------------------------------------
# Partial-mode verifiers (zsets family)
# ---------------------------------------------------------------------------


@verifier("ZRANDMEMBER")
def _verify_zrandmember(rs, py) -> None:
    py.zadd("Z", {b"a": 1, b"b": 2})
    out = rs.zrandmember("Z")
    assert out in {b"a", b"b"}
