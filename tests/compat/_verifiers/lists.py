"""Verifiers for the lists command family."""

from __future__ import annotations

from . import verifier


@verifier("LPUSH")
def _verify_lpush(rs, py) -> None:
    assert rs.lpush("L", b"a", b"b") == py.lpush("L_py", b"a", b"b") == 2


@verifier("RPUSH")
def _verify_rpush(rs, py) -> None:
    assert rs.rpush("L", b"a", b"b") == py.rpush("L_py", b"a", b"b") == 2


@verifier("LPUSHX")
def _verify_lpushx(rs, py) -> None:
    # Use separate keys to avoid shared-list interference
    py.rpush("L_rs", b"a")
    py.rpush("L_py", b"a")
    assert rs.lpushx("L_rs", b"b") == py.lpushx("L_py", b"b") == 2


@verifier("RPUSHX")
def _verify_rpushx(rs, py) -> None:
    # Use separate keys to avoid shared-list interference
    py.rpush("L_rs", b"a")
    py.rpush("L_py", b"a")
    assert rs.rpushx("L_rs", b"b") == py.rpushx("L_py", b"b") == 2


@verifier("LPOP")
def _verify_lpop(rs, py) -> None:
    # Use separate keys — both clients share a DB, so a single LPOP per key
    py.rpush("L_rs", b"a", b"b")
    py.rpush("L_py", b"a", b"b")
    assert rs.lpop("L_rs") == b"a"
    assert py.lpop("L_py") == b"a"


@verifier("RPOP")
def _verify_rpop(rs, py) -> None:
    # Use separate keys — both clients share a DB
    py.rpush("L_rs", b"a", b"b", b"c")
    py.rpush("L_py", b"a", b"b", b"c")
    assert rs.rpop("L_rs") == b"c"
    assert py.rpop("L_py") == b"c"


@verifier("LMOVE")
def _verify_lmove(rs, py) -> None:
    py.rpush("src", b"a")
    assert rs.lmove("src", "dst", "LEFT", "RIGHT") == b"a"


@verifier("BLMOVE")
def _verify_blmove(rs, py) -> None:
    py.rpush("src", b"a")
    assert rs.blmove("src", "dst", "LEFT", "RIGHT", 1) == b"a"


@verifier("LMPOP")
def _verify_lmpop(rs, py) -> None:
    py.rpush("L", b"a", b"b")
    # rs.lmpop(keys, direction=, count=)
    rs_out = rs.lmpop(["L"], direction="LEFT", count=1)
    assert rs_out is not None
    # rs returns (key_str, [b"a"]), py returns [b"L", [b"b"]]
    assert rs_out[1] == [b"a"]


@verifier("BLMPOP")
def _verify_blmpop(rs, py) -> None:
    py.rpush("L", b"a")
    # rs.blmpop(timeout=, keys=, direction=, count=)
    rs_out = rs.blmpop(timeout=0.1, keys=["L"], direction="LEFT", count=1)
    assert rs_out is not None


@verifier("BLPOP")
def _verify_blpop(rs, py) -> None:
    py.rpush("L", b"a")
    rs_out = rs.blpop(["L"], timeout=1)
    assert rs_out is not None and rs_out[1] == b"a"


@verifier("BRPOP")
def _verify_brpop(rs, py) -> None:
    py.rpush("L", b"a")
    rs_out = rs.brpop(["L"], timeout=1)
    assert rs_out is not None and rs_out[1] == b"a"


@verifier("LPOS")
def _verify_lpos(rs, py) -> None:
    py.rpush("L", b"a", b"b", b"c", b"b")
    assert rs.lpos("L", b"b") == py.lpos("L", b"b") == 1


@verifier("LRANGE")
def _verify_lrange(rs, py) -> None:
    py.rpush("L", b"a", b"b", b"c")
    assert rs.lrange("L", 0, -1) == py.lrange("L", 0, -1) == [b"a", b"b", b"c"]


@verifier("LLEN")
def _verify_llen(rs, py) -> None:
    py.rpush("L", b"a", b"b", b"c")
    assert rs.llen("L") == py.llen("L") == 3


@verifier("LREM")
def _verify_lrem(rs, py) -> None:
    py.rpush("L", b"a", b"b", b"a")
    assert rs.lrem("L", 1, b"a") == 1


@verifier("LINDEX")
def _verify_lindex(rs, py) -> None:
    py.rpush("L", b"a", b"b")
    assert rs.lindex("L", 0) == py.lindex("L", 0) == b"a"


@verifier("LSET")
def _verify_lset(rs, py) -> None:
    py.rpush("L", b"a", b"b")
    result = rs.lset("L", 0, b"x")
    assert result in (True, None)
    assert py.lindex("L", 0) == b"x"


@verifier("LINSERT")
def _verify_linsert(rs, py) -> None:
    # Use separate keys to avoid rs-insert modifying the list before py-insert
    py.rpush("L_rs", b"a", b"c")
    py.rpush("L_py", b"a", b"c")
    assert rs.linsert("L_rs", "BEFORE", b"c", b"b") == py.linsert("L_py", "BEFORE", b"c", b"b") == 3


@verifier("LTRIM")
def _verify_ltrim(rs, py) -> None:
    py.rpush("L", b"a", b"b", b"c", b"d")
    result = rs.ltrim("L", 1, 2)
    assert result in (True, None)
    assert py.lrange("L", 0, -1) == [b"b", b"c"]
