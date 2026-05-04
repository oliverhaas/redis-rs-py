"""Verifiers for the streams command family.

Stream IDs are server-assigned timestamps; the verifiers seed via
``xadd`` then read what they wrote, comparing shapes.
"""

from __future__ import annotations

from . import verifier


def _ok(v) -> bool:
    return v in (True, None, b"OK")


@verifier("XADD")
def _verify_xadd(rs, py) -> None:
    rs_id = rs.xadd("S", {"k": b"v"})
    py_id = py.xadd("S_py", {"k": b"v"})
    # rs returns str, py returns bytes — both look like "<ms>-<seq>"
    assert isinstance(rs_id, (bytes, str)) and b"-" in (rs_id if isinstance(rs_id, bytes) else rs_id.encode())
    assert isinstance(py_id, bytes) and b"-" in py_id


@verifier("XLEN")
def _verify_xlen(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    assert rs.xlen("S") == py.xlen("S") == 1


@verifier("XRANGE")
def _verify_xrange(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    rs_out = rs.xrange("S")
    py_out = py.xrange("S")
    assert len(rs_out) == len(py_out) == 1
    # Each entry is (id, fields-dict). IDs may differ, but the fields must match.
    assert rs_out[0][1] == py_out[0][1] == {b"k": b"v"}


@verifier("XREVRANGE")
def _verify_xrevrange(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    rs_out = rs.xrevrange("S")
    assert rs_out[0][1] == {b"k": b"v"}


@verifier("XREAD")
def _verify_xread(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    rs_out = rs.xread({"S": "0"})
    py_out = py.xread({"S": "0"})
    # rs returns dict {b"S": [...]}, py returns list [[b"S", [...]]]
    assert py_out[0][0] == b"S"
    assert py_out[0][1][0][1] == {b"k": b"v"}
    # rs returns dict
    assert b"S" in rs_out
    assert rs_out[b"S"][0][1] == {b"k": b"v"}


@verifier("XREADGROUP")
def _verify_xreadgroup(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    out = rs.xreadgroup("g", "c1", {"S": ">"})
    # rs returns dict {b"S": [...]}
    assert b"S" in out


@verifier("XACK")
def _verify_xack(rs, py) -> None:
    msg_id = py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    # rs.xack expects string IDs (msg_id from py.xadd is bytes)
    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
    assert rs.xack("S", "g", msg_id_str) == 1


@verifier("XDEL")
def _verify_xdel(rs, py) -> None:
    msg_id = py.xadd("S", {"k": b"v"})
    # rs.xdel expects string IDs
    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
    assert rs.xdel("S", msg_id_str) == 1


@verifier("XGROUP CREATE")
def _verify_xgroup_create(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    assert _ok(rs.xgroup_create("S", "g", id="0"))


@verifier("XGROUP SETID")
def _verify_xgroup_setid(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    assert _ok(rs.xgroup_setid("S", "g", id="$"))


@verifier("XGROUP DESTROY")
def _verify_xgroup_destroy(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    assert rs.xgroup_destroy("S", "g") == 1


@verifier("XGROUP DELCONSUMER")
def _verify_xgroup_delconsumer(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    assert rs.xgroup_delconsumer("S", "g", "c1") == 1


@verifier("XGROUP CREATECONSUMER")
def _verify_xgroup_createconsumer(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    assert rs.xgroup_createconsumer("S", "g", "c1") == 1


@verifier("XINFO STREAM")
def _verify_xinfo_stream(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    rs_info = rs.xinfo_stream("S")
    py_info = py.xinfo_stream("S")
    # rs uses bytes keys, py uses str keys — compare normalised
    rs_keys = {k.decode() if isinstance(k, bytes) else k for k in rs_info}
    py_keys = set(py_info)
    assert rs_keys == py_keys


@verifier("XINFO GROUPS")
def _verify_xinfo_groups(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    rs_groups = rs.xinfo_groups("S")
    py_groups = py.xinfo_groups("S")
    assert len(rs_groups) == len(py_groups) == 1


@verifier("XINFO CONSUMERS")
def _verify_xinfo_consumers(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    rs_out = rs.xinfo_consumers("S", "g")
    assert len(rs_out) == 1


@verifier("XTRIM")
def _verify_xtrim(rs, py) -> None:
    for _ in range(5):
        py.xadd("S", {"k": b"v"})
    # approximate=False to get exact count; stream already has 5 entries
    trimmed = rs.xtrim("S", maxlen=2, approximate=False)
    assert trimmed == 3
    assert rs.xlen("S") == 2


@verifier("XPENDING")
def _verify_xpending(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    rs_p = rs.xpending("S", "g")
    py_p = py.xpending("S", "g")
    # rs returns tuple (pending, min_id, max_id, consumers)
    # py returns dict {'pending': 1, ...}
    rs_pending = rs_p[0] if isinstance(rs_p, (list, tuple)) else rs_p.get("pending", rs_p.get(b"pending"))
    py_pending = py_p["pending"]
    assert rs_pending == py_pending == 1


@verifier("XCLAIM")
def _verify_xclaim(rs, py) -> None:
    msg_id = py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    # rs.xclaim expects string message IDs
    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
    out = rs.xclaim("S", "g", "c2", min_idle_time=0, message_ids=[msg_id_str])
    assert len(out) == 1


@verifier("XAUTOCLAIM")
def _verify_xautoclaim(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    out = rs.xautoclaim("S", "g", "c2", min_idle_time=0)
    assert isinstance(out, (tuple, list))


@verifier("XSETID")
def _verify_xsetid(rs, py) -> None:
    py.xadd("S", {"k": b"v"})
    assert _ok(rs.xsetid("S", "9999999999999-0"))
