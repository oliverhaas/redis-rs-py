"""Verifiers for the hashes command family."""

from __future__ import annotations

from . import verifier


def _ok(v) -> bool:
    return v in (True, None, b"OK")


@verifier("HGET")
def _verify_hget(rs, py) -> None:
    py.hset("H", "f", b"v")
    assert rs.hget("H", "f") == py.hget("H", "f") == b"v"


@verifier("HSET")
def _verify_hset(rs, py) -> None:
    assert rs.hset("H", "f", b"v") == py.hset("H_py", "f", b"v") == 1


@verifier("HSETNX")
def _verify_hsetnx(rs, py) -> None:
    assert rs.hsetnx("H", "f", b"v") == py.hsetnx("H_py", "f", b"v") == 1


@verifier("HGETALL")
def _verify_hgetall(rs, py) -> None:
    py.hset("H", mapping={"a": b"1", "b": b"2"})
    assert rs.hgetall("H") == py.hgetall("H") == {b"a": b"1", b"b": b"2"}


@verifier("HDEL")
def _verify_hdel(rs, py) -> None:
    py.hset("H", "f", b"v")
    assert rs.hdel("H", "f") == 1


@verifier("HINCRBY")
def _verify_hincrby(rs, py) -> None:
    assert rs.hincrby("H", "c", 5) == py.hincrby("H_py", "c", 5) == 5


@verifier("HINCRBYFLOAT")
def _verify_hincrbyfloat(rs, py) -> None:
    rs_v = rs.hincrbyfloat("H", "c", 1.5)
    py_v = py.hincrbyfloat("H_py", "c", 1.5)
    assert rs_v == py_v == 1.5


@verifier("HKEYS")
def _verify_hkeys(rs, py) -> None:
    py.hset("H", mapping={"a": b"1", "b": b"2"})
    assert sorted(rs.hkeys("H")) == sorted(py.hkeys("H")) == [b"a", b"b"]


@verifier("HVALS")
def _verify_hvals(rs, py) -> None:
    py.hset("H", mapping={"a": b"1", "b": b"2"})
    assert sorted(rs.hvals("H")) == sorted(py.hvals("H")) == [b"1", b"2"]


@verifier("HEXISTS")
def _verify_hexists(rs, py) -> None:
    py.hset("H", "f", b"v")
    assert rs.hexists("H", "f") == py.hexists("H", "f") is True


@verifier("HLEN")
def _verify_hlen(rs, py) -> None:
    py.hset("H", mapping={"a": b"1", "b": b"2"})
    assert rs.hlen("H") == py.hlen("H") == 2


@verifier("HMGET")
def _verify_hmget(rs, py) -> None:
    py.hset("H", mapping={"a": b"1", "b": b"2"})
    assert rs.hmget("H", ["a", "b", "missing"]) == py.hmget("H", ["a", "b", "missing"]) == [b"1", b"2", None]


@verifier("HSCAN")
def _verify_hscan(rs, py) -> None:
    py.hset("H", mapping={f"f{i}": str(i).encode() for i in range(20)})
    # rs.hscan uses keyword-only cursor argument
    rs_cursor, rs_data = rs.hscan("H", cursor=0, count=100)
    py_cursor, py_data = py.hscan("H", 0, count=100)
    assert rs_data == py_data
    assert isinstance(rs_cursor, int) and isinstance(py_cursor, int)


@verifier("HRANDFIELD")
def _verify_hrandfield(rs, py) -> None:
    py.hset("H", mapping={"a": b"1", "b": b"2", "c": b"3"})
    rs_v = rs.hrandfield("H")
    py_v = py.hrandfield("H")
    assert rs_v in {b"a", b"b", b"c"}
    assert py_v in {b"a", b"b", b"c"}


def _hexpire_skip_if_unsupported(exc: Exception) -> None:
    """Skip gracefully when the server does not support HEXPIRE family (pre-7.4)."""
    import pytest

    msg = str(exc).lower()
    # Match any HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT/HEXPIRETIME/HPEXPIRETIME/HTTL/HPTTL/HPERSIST
    if "unknown command" in msg and any(
        kw in msg for kw in ("hexpire", "hpexpire", "hexpireat", "hpexpireat", "httl", "hpttl", "hpersist")
    ):
        pytest.skip("HEXPIRE family not supported by this server (requires Redis 7.4+)")
    raise exc


@verifier("HEXPIRE")
def _verify_hexpire(rs, py) -> None:
    py.hset("H", "f", b"v")
    # rs.hexpire(key, fields, time, ...) — fields comes before time
    try:
        out = rs.hexpire("H", ["f"], 60)
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert out == [1]


@verifier("HPEXPIRE")
def _verify_hpexpire(rs, py) -> None:
    py.hset("H", "f", b"v")
    try:
        result = rs.hpexpire("H", ["f"], 60_000)
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert result == [1]


@verifier("HEXPIREAT")
def _verify_hexpireat(rs, py) -> None:
    import time

    py.hset("H", "f", b"v")
    try:
        result = rs.hexpireat("H", ["f"], int(time.time()) + 60)
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert result == [1]


@verifier("HPEXPIREAT")
def _verify_hpexpireat(rs, py) -> None:
    import time

    py.hset("H", "f", b"v")
    try:
        result = rs.hpexpireat("H", ["f"], int(time.time() * 1000) + 60_000)
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert result == [1]


@verifier("HEXPIRETIME")
def _verify_hexpiretime(rs, py) -> None:
    py.hset("H", "f", b"v")
    try:
        rs.hexpire("H", ["f"], 60)
        out = rs.hexpiretime("H", ["f"])
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert isinstance(out, list) and out[0] > 0


@verifier("HPEXPIRETIME")
def _verify_hpexpiretime(rs, py) -> None:
    py.hset("H", "f", b"v")
    try:
        rs.hexpire("H", ["f"], 60)
        out = rs.hpexpiretime("H", ["f"])
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert isinstance(out, list) and out[0] > 0


@verifier("HTTL")
def _verify_httl(rs, py) -> None:
    py.hset("H", "f", b"v")
    try:
        rs.hexpire("H", ["f"], 60)
        out = rs.httl("H", ["f"])
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert isinstance(out, list) and 0 < out[0] <= 60


@verifier("HPTTL")
def _verify_hpttl(rs, py) -> None:
    py.hset("H", "f", b"v")
    try:
        rs.hexpire("H", ["f"], 60)
        out = rs.hpttl("H", ["f"])
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert isinstance(out, list) and out[0] > 0


@verifier("HPERSIST")
def _verify_hpersist(rs, py) -> None:
    py.hset("H", "f", b"v")
    try:
        rs.hexpire("H", ["f"], 60)
        result = rs.hpersist("H", ["f"])
    except Exception as exc:
        _hexpire_skip_if_unsupported(exc)
    assert result == [1]
