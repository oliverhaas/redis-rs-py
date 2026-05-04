"""Verifiers for the sets command family.

Many set commands return server-defined-order results — the verifiers
normalise to ``set(...)`` before comparing.
"""

from __future__ import annotations

from . import verifier


@verifier("SADD")
def _verify_sadd(rs, py) -> None:
    assert rs.sadd("S", b"a", b"b") == py.sadd("S_py", b"a", b"b") == 2


@verifier("SREM")
def _verify_srem(rs, py) -> None:
    py.sadd("S", b"a", b"b")
    assert rs.srem("S", b"a") == 1


@verifier("SMEMBERS")
def _verify_smembers(rs, py) -> None:
    py.sadd("S", b"a", b"b", b"c")
    assert rs.smembers("S") == py.smembers("S") == {b"a", b"b", b"c"}


@verifier("SISMEMBER")
def _verify_sismember(rs, py) -> None:
    py.sadd("S", b"a")
    # rs returns bool True, py returns 1 (int); both are truthy
    assert rs.sismember("S", b"a") is True
    assert py.sismember("S", b"a")


@verifier("SMISMEMBER")
def _verify_smismember(rs, py) -> None:
    py.sadd("S", b"a", b"b")
    # rs.smismember uses *members variadic, not a list argument
    assert rs.smismember("S", b"a", b"b", b"missing") == [True, True, False]
    assert py.smismember("S", [b"a", b"b", b"missing"]) == [1, 1, 0]


@verifier("SCARD")
def _verify_scard(rs, py) -> None:
    py.sadd("S", b"a", b"b")
    assert rs.scard("S") == py.scard("S") == 2


@verifier("SINTER")
def _verify_sinter(rs, py) -> None:
    py.sadd("A", b"a", b"b")
    py.sadd("B", b"b", b"c")
    assert rs.sinter("A", "B") == py.sinter("A", "B") == {b"b"}


@verifier("SINTERSTORE")
def _verify_sinterstore(rs, py) -> None:
    py.sadd("A", b"a", b"b")
    py.sadd("B", b"b", b"c")
    # rs.sinterstore uses (destination, *keys) variadic
    assert rs.sinterstore("DST", "A", "B") == 1


@verifier("SINTERCARD")
def _verify_sintercard(rs, py) -> None:
    py.sadd("A", b"a", b"b")
    py.sadd("B", b"b", b"c")
    # rs.sintercard uses (*keys, limit=None) — no numkeys prefix
    assert rs.sintercard("A", "B") == py.sintercard(2, ["A", "B"]) == 1


@verifier("SUNION")
def _verify_sunion(rs, py) -> None:
    py.sadd("A", b"a", b"b")
    py.sadd("B", b"b", b"c")
    assert rs.sunion("A", "B") == py.sunion("A", "B") == {b"a", b"b", b"c"}


@verifier("SUNIONSTORE")
def _verify_sunionstore(rs, py) -> None:
    py.sadd("A", b"a", b"b")
    py.sadd("B", b"b", b"c")
    # rs.sunionstore uses (destination, *keys) variadic
    assert rs.sunionstore("DST", "A", "B") == 3


@verifier("SDIFF")
def _verify_sdiff(rs, py) -> None:
    py.sadd("A", b"a", b"b")
    py.sadd("B", b"b")
    assert rs.sdiff("A", "B") == py.sdiff("A", "B") == {b"a"}


@verifier("SDIFFSTORE")
def _verify_sdiffstore(rs, py) -> None:
    py.sadd("A", b"a", b"b")
    py.sadd("B", b"b")
    # rs.sdiffstore uses (destination, *keys) variadic
    assert rs.sdiffstore("DST", "A", "B") == 1


@verifier("SMOVE")
def _verify_smove(rs, py) -> None:
    py.sadd("A", b"a")
    assert rs.smove("A", "B", b"a") is True


@verifier("SSCAN")
def _verify_sscan(rs, py) -> None:
    py.sadd("S", *[f"m{i}".encode() for i in range(20)])
    # rs.sscan uses keyword-only cursor argument
    _rs_cursor, rs_members = rs.sscan("S", cursor=0, count=100)
    _py_cursor, py_members = py.sscan("S", 0, count=100)
    assert set(rs_members) == set(py_members)


# ---------------------------------------------------------------------------
# Partial-mode verifiers (sets family)
# ---------------------------------------------------------------------------


@verifier("SPOP")
def _verify_spop(rs, py) -> None:
    py.sadd("S", b"a", b"b", b"c")
    rs_out = rs.spop("S")
    # Order is server-defined; assert membership, not value.
    assert rs_out in {b"a", b"b", b"c"}


@verifier("SRANDMEMBER")
def _verify_srandmember(rs, py) -> None:
    py.sadd("S", b"a", b"b", b"c")
    out = rs.srandmember("S")
    assert out in {b"a", b"b", b"c"}
