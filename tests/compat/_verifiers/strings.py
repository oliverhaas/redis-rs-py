"""Verifiers for the strings command family."""

from . import verifier


@verifier("GET")
def _verify_get(rs, py) -> None:
    py.set("k", "v")
    assert rs.get("k") == py.get("k") == b"v"
    assert rs.get("missing") == py.get("missing") is None


@verifier("SET")
def _verify_set(rs, py) -> None:
    # rs returns None for OK, py returns True — both falsy-free
    rs_r = rs.set("k", b"v")
    py_r = py.set("k", b"v")
    assert rs_r in (True, None)
    assert py_r is True
    assert rs.get("k") == py.get("k") == b"v"
    # NX flag — second set must fail with NX
    assert rs.set("k", b"x", nx=True) is None
    assert py.set("k", b"x", nx=True) is None
    # XX flag on missing — both should return None
    assert rs.set("missing", b"v", xx=True) is None
    assert py.set("missing", b"v", xx=True) is None


@verifier("GETEX")
def _verify_getex(rs, py) -> None:
    py.set("k", b"v")
    assert rs.getex("k", ex=60) == py.getex("k", ex=60) == b"v"


@verifier("GETDEL")
def _verify_getdel(rs, py) -> None:
    py.set("k", b"v")
    assert rs.getdel("k") == b"v"
    assert py.get("k") is None


@verifier("COPY")
def _verify_copy(rs, py) -> None:
    py.set("src", b"v")
    assert rs.copy("src", "dst") in (True, 1)
    assert rs.get("dst") == b"v"


@verifier("INCR")
def _verify_incr(rs, py) -> None:
    assert rs.incr("rs_c") == py.incr("py_c") == 1


@verifier("INCRBY")
def _verify_incrby(rs, py) -> None:
    assert rs.incrby("rs_c", 5) == py.incrby("py_c", 5) == 5


@verifier("INCRBYFLOAT")
def _verify_incrbyfloat(rs, py) -> None:
    rs_val = rs.incrbyfloat("rs_c", 1.5)
    py_val = py.incrbyfloat("py_c", 1.5)
    assert rs_val == py_val == 1.5


@verifier("DECR")
def _verify_decr(rs, py) -> None:
    assert rs.decr("rs_c") == py.decr("py_c") == -1


@verifier("DECRBY")
def _verify_decrby(rs, py) -> None:
    assert rs.decrby("rs_c", 3) == py.decrby("py_c", 3) == -3


@verifier("APPEND")
def _verify_append(rs, py) -> None:
    py.set("k", b"abc")
    # rs reads the same key that py just set
    assert rs.append("k", b"def") == 6
    assert py.append("k", b"ghi") == 9


@verifier("STRLEN")
def _verify_strlen(rs, py) -> None:
    py.set("k", b"abc")
    assert rs.strlen("k") == py.strlen("k") == 3


@verifier("MGET")
def _verify_mget(rs, py) -> None:
    py.set("mget_a", b"1")
    py.set("mget_b", b"2")
    assert (
        rs.mget("mget_a", "mget_b", "mget_missing") == py.mget("mget_a", "mget_b", "mget_missing") == [b"1", b"2", None]
    )


@verifier("MSET")
def _verify_mset(rs, py) -> None:
    # rs.mset requires {str: bytes} mapping
    rs_r = rs.mset({"a": b"1", "b": b"2"})
    assert rs_r in (True, None)
    assert rs.get("a") == b"1"


@verifier("MSETNX")
def _verify_msetnx(rs, py) -> None:
    assert rs.msetnx({"a": b"1"}) == py.msetnx({"b": b"2"}) == 1
    assert rs.msetnx({"a": b"x"}) == py.msetnx({"b": b"x"}) == 0


@verifier("SETRANGE")
def _verify_setrange(rs, py) -> None:
    py.set("k", b"Hello World")
    # setrange with bytes value
    assert rs.setrange("k", 6, b"Redis") == py.setrange("k", 6, b"Redis") == 11


@verifier("GETRANGE")
def _verify_getrange(rs, py) -> None:
    py.set("k", b"Hello World")
    assert rs.getrange("k", 0, 4) == py.getrange("k", 0, 4) == b"Hello"


@verifier("EXISTS")
def _verify_exists(rs, py) -> None:
    py.set("k", b"v")
    assert rs.exists("k", "missing") == py.exists("k", "missing") == 1


@verifier("DEL")
def _verify_del(rs, py) -> None:
    py.set("k1", b"v")
    py.set("k2", b"v")
    assert rs.delete("k1", "k2", "missing") == 2
    assert py.delete("k1", "k2", "missing") == 0  # already gone


@verifier("UNLINK")
def _verify_unlink(rs, py) -> None:
    py.set("k", b"v")
    assert rs.unlink("k") == 1
    assert py.unlink("k") == 0
