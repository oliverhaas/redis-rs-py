"""Verifiers for the admin / scan / keyspace family."""

from . import verifier


def _ok(v) -> bool:
    """Accept None or True or b'OK' as a valid OK response."""
    return v in (True, None, b"OK")


@verifier("PING")
def _verify_ping(rs, py) -> None:
    assert rs.ping() == py.ping() is True


@verifier("ECHO")
def _verify_echo(rs, py) -> None:
    assert rs.echo(b"hello") == py.echo(b"hello") == b"hello"


@verifier("DBSIZE")
def _verify_dbsize(rs, py) -> None:
    py.set("a", b"1")
    py.set("b", b"2")
    assert rs.dbsize() == py.dbsize() == 2


@verifier("FLUSHDB")
def _verify_flushdb(rs, py) -> None:
    py.set("k", b"v")
    assert _ok(rs.flushdb())
    assert py.dbsize() == 0


@verifier("FLUSHALL")
def _verify_flushall(rs, py) -> None:
    import os

    import pytest

    # FLUSHALL wipes every DB, so xdist_group is not enough: unrelated workers
    # keep running. Same guard as TestFlushall in driver/test_commands_admin.py.
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.skip("FLUSHALL would race other xdist workers' DBs")
    py.set("k", b"v")
    assert _ok(rs.flushall())
    assert py.dbsize() == 0


@verifier("KEYS")
def _verify_keys(rs, py) -> None:
    py.set("a", b"1")
    py.set("b", b"2")
    assert sorted(rs.keys("*")) == sorted(py.keys("*"))


@verifier("SCAN")
def _verify_scan(rs, py) -> None:
    py.mset({f"k{i}": str(i).encode() for i in range(50)})
    rs_cursor, rs_keys = rs.scan(0, match="k*", count=100)
    py_cursor, py_keys = py.scan(0, match="k*", count=100)
    assert sorted(rs_keys) == sorted(py_keys)
    # Cursors converge to 0 once the scan is exhausted; both clients
    # may return non-zero on the first page — assert only that both
    # return ints.
    assert isinstance(rs_cursor, int) and isinstance(py_cursor, int)


@verifier("SCAN_ITER")
def _verify_scan_iter(rs, py) -> None:
    py.mset({f"k{i}": str(i).encode() for i in range(50)})
    rs_items = sorted(rs.scan_iter(match="k*"))
    py_items = sorted(py.scan_iter(match="k*"))
    assert rs_items == py_items


@verifier("EXPIRE")
def _verify_expire(rs, py) -> None:
    py.set("k", b"v")
    result = rs.expire("k", 60)
    assert result in (True, 1)
    assert 0 < rs.ttl("k") <= 60


@verifier("PEXPIRE")
def _verify_pexpire(rs, py) -> None:
    py.set("k", b"v")
    assert rs.pexpire("k", 60000) in (True, 1)


@verifier("TTL")
def _verify_ttl(rs, py) -> None:
    py.set("k", b"v", ex=60)
    rs_ttl = rs.ttl("k")
    py_ttl = py.ttl("k")
    # Allow +-1s drift between the two reads.
    assert abs(rs_ttl - py_ttl) <= 1


@verifier("PTTL")
def _verify_pttl(rs, py) -> None:
    py.set("k", b"v", ex=60)
    assert rs.pttl("k") > 0
    assert py.pttl("k") > 0


@verifier("PERSIST")
def _verify_persist(rs, py) -> None:
    py.set("k", b"v", ex=60)
    assert rs.persist("k") in (True, 1)
    assert py.ttl("k") == -1


@verifier("EXPIREAT")
def _verify_expireat(rs, py) -> None:
    import time

    py.set("k", b"v")
    target = int(time.time()) + 60
    assert rs.expireat("k", target) in (True, 1)
    assert py.expireat("k", target) in (True, 1)


@verifier("PEXPIREAT")
def _verify_pexpireat(rs, py) -> None:
    import time

    py.set("k", b"v")
    target = int(time.time() * 1000) + 60_000
    assert rs.pexpireat("k", target) in (True, 1)
    assert py.pexpireat("k", target) in (True, 1)


@verifier("EXPIRETIME")
def _verify_expiretime(rs, py) -> None:
    import time

    py.set("k", b"v", ex=60)
    rs_t = rs.expiretime("k")
    py_t = py.expiretime("k")
    now = int(time.time())
    assert now <= rs_t <= now + 61
    assert now <= py_t <= now + 61


@verifier("PEXPIRETIME")
def _verify_pexpiretime(rs, py) -> None:
    py.set("k", b"v", ex=60)
    assert rs.pexpiretime("k") > 0
    assert py.pexpiretime("k") > 0


@verifier("RENAME")
def _verify_rename(rs, py) -> None:
    py.set("k", b"v")
    assert _ok(rs.rename("k", "k2"))
    assert rs.get("k2") == b"v"


@verifier("RENAMENX")
def _verify_renamenx(rs, py) -> None:
    py.set("k", b"v")
    assert rs.renamenx("k", "k2") in (True, 1)
    # py would fail since "k" is gone, just check rs succeeded
    assert rs.get("k2") == b"v"


@verifier("TYPE")
def _verify_type(rs, py) -> None:
    py.set("k", b"v")
    rs_type = rs.type("k")
    py_type = py.type("k")
    # rs returns str, py returns bytes
    assert rs_type in (b"string", "string")
    assert py_type == b"string"


@verifier("DUMP")
def _verify_dump(rs, py) -> None:
    py.set("k", b"v")
    rs_dump = rs.dump("k")
    py_dump = py.dump("k")
    assert rs_dump == py_dump


@verifier("RESTORE")
def _verify_restore(rs, py) -> None:
    py.set("k", b"v")
    payload = py.dump("k")
    assert _ok(rs.restore("k2", 0, payload))
    assert py.get("k2") == b"v"


@verifier("CONFIG GET")
def _verify_config_get(rs, py) -> None:
    rs_v = rs.config_get("maxmemory")
    py_v = py.config_get("maxmemory")
    # rs returns {bytes: bytes}, py returns {str: str} in some versions
    assert rs_v is not None and py_v is not None
    # Both should have a maxmemory entry (key may be bytes or str)
    rs_key = next(iter(rs_v))
    py_key = next(iter(py_v))
    assert rs_key in (b"maxmemory", "maxmemory")
    assert py_key in (b"maxmemory", "maxmemory")


@verifier("CONFIG SET")
def _verify_config_set(rs, py) -> None:
    assert _ok(rs.config_set("maxmemory-policy", "noeviction"))
    cfg = py.config_get("maxmemory-policy")
    # key may be bytes or str
    value = next(iter(cfg.values()))
    assert value in (b"noeviction", "noeviction")


@verifier("CONFIG RESETSTAT")
def _verify_config_resetstat(rs, py) -> None:
    assert _ok(rs.config_resetstat())


@verifier("CLIENT GETNAME")
def _verify_client_getname(rs, py) -> None:
    rs.client_setname("rs-name")
    name = rs.client_getname()
    assert name in (b"rs-name", "rs-name")


@verifier("CLIENT SETNAME")
def _verify_client_setname(rs, py) -> None:
    assert _ok(rs.client_setname("rs"))


@verifier("CLIENT KILL")
def _verify_client_kill(rs, py) -> None:
    # Use client_id keyword (rs uses client_id=, py uses id=)
    out = rs.client_kill(client_id=999_999_999)  # bogus id — kills nothing
    assert isinstance(out, int)


@verifier("CLIENT PAUSE")
def _verify_client_pause(rs, py) -> None:
    assert _ok(rs.client_pause(1))


@verifier("CLIENT UNPAUSE")
def _verify_client_unpause(rs, py) -> None:
    assert _ok(rs.client_unpause())


@verifier("CLIENT NO-EVICT")
def _verify_client_no_evict(rs, py) -> None:
    assert _ok(rs.client_no_evict(mode="ON"))


@verifier("CLIENT NO-TOUCH")
def _verify_client_no_touch(rs, py) -> None:
    import pytest as _pytest

    try:
        result = rs.client_no_touch(mode="ON")
        assert _ok(result)
    except Exception as exc:
        if "unknown subcommand" in str(exc).lower() or "unknown command" in str(exc).lower():
            _pytest.skip("CLIENT NO-TOUCH not supported by this server")
        raise


@verifier("OBJECT ENCODING")
def _verify_object_encoding(rs, py) -> None:
    py.set("k", b"v")
    rs_enc = rs.object_encoding("k")
    # rs has object_encoding directly; py uses object('encoding', key)
    py_enc = py.object("encoding", "k")
    assert rs_enc == py_enc


@verifier("OBJECT REFCOUNT")
def _verify_object_refcount(rs, py) -> None:
    py.set("k", b"v")
    assert isinstance(rs.object_refcount("k"), int)


@verifier("MEMORY USAGE")
def _verify_memory_usage(rs, py) -> None:
    py.set("k", b"v")
    rs_v = rs.memory_usage("k")
    py_v = py.memory_usage("k")
    assert rs_v is not None
    assert py_v is not None
    # Two clients on the same value get within 64 bytes of each other in practice.
    assert abs(rs_v - py_v) <= 64


@verifier("WAIT")
def _verify_wait(rs, py) -> None:
    # Standalone server: WAIT 0 0 returns 0.
    assert rs.wait(numreplicas=0, timeout=100) == py.wait(0, 100) == 0


@verifier("WAITAOF")
def _verify_waitaof(rs, py) -> None:
    import pytest as _pytest

    try:
        out = rs.waitaof(numlocal=0, numreplicas=0, timeout=100)
        assert isinstance(out, list) and len(out) == 2
    except Exception as exc:
        if "unknown command" in str(exc).lower() or "unknown subcommand" in str(exc).lower():
            _pytest.skip("WAITAOF not supported by this server")
        raise


_BGSAVE_RETRY = ("already in progress", "another child process is active")
_BGREWRITEAOF_RETRY = (*_BGSAVE_RETRY, "background append")


@verifier("BGSAVE")
def _verify_bgsave(rs, py) -> None:
    import time as _time

    import pytest

    # BGSAVE is a server-global operation. Retry when blocked by another in-progress save
    # or an active AOF rewrite (both are common in parallel test runs).
    deadline = _time.monotonic() + 15
    while _time.monotonic() < deadline:
        try:
            out = rs.bgsave()
        except Exception as exc:
            msg = str(exc).lower()
            if any(phrase in msg for phrase in _BGSAVE_RETRY):
                _time.sleep(0.5)
            else:
                raise
        else:
            assert out in (True, b"Background saving started", "Background saving started", None)
            return
    pytest.skip("BGSAVE: background save contention exceeded 15s (parallel test interference)")


@verifier("BGREWRITEAOF")
def _verify_bgrewriteaof(rs, py) -> None:
    import time as _time

    import pytest

    # BGREWRITEAOF is a server-global operation; retry if blocked by a save or prior rewrite.
    deadline = _time.monotonic() + 15
    while _time.monotonic() < deadline:
        try:
            out = rs.bgrewriteaof()
        except Exception as exc:
            msg = str(exc).lower()
            if any(phrase in msg for phrase in _BGREWRITEAOF_RETRY):
                _time.sleep(0.5)
            else:
                raise
        else:
            assert isinstance(out, (bytes, str, bool)) or out is None
            return
    pytest.skip("BGREWRITEAOF: background save contention exceeded 15s (parallel test interference)")


# ---------------------------------------------------------------------------
# Partial-mode verifiers (admin family)
# ---------------------------------------------------------------------------


@verifier("RANDOMKEY")
def _verify_randomkey(rs, py) -> None:
    py.set("k", b"v")
    out = rs.randomkey()
    assert out == b"k"


@verifier("INFO")
def _verify_info(rs, py) -> None:
    rs_info = rs.info()
    py_info = py.info()
    # rs returns raw bytes string; py returns a parsed dict
    # Parse rs bytes into a set of keys for comparison
    if isinstance(rs_info, (bytes, str)):
        raw = rs_info.decode() if isinstance(rs_info, bytes) else rs_info
        rs_keys = {line.split(":")[0] for line in raw.splitlines() if line and not line.startswith("#") and ":" in line}
    else:
        rs_keys = {k.decode() if isinstance(k, bytes) else k for k in rs_info}
    py_keys = set(py_info)
    assert rs_keys >= {"redis_version", "tcp_port"}
    assert py_keys >= {"redis_version", "tcp_port"}


@verifier("CLIENT ID")
def _verify_client_id(rs, py) -> None:
    assert isinstance(rs.client_id(), int)
    assert isinstance(py.client_id(), int)


@verifier("CLIENT INFO")
def _verify_client_info(rs, py) -> None:
    rs_info = rs.client_info()
    # rs returns raw bytes string; parse it to check presence of "id" field
    if isinstance(rs_info, (bytes, str)):
        raw = rs_info.decode() if isinstance(rs_info, bytes) else rs_info
        assert "id=" in raw
    else:
        assert isinstance(rs_info, dict)
        assert b"id" in rs_info or "id" in rs_info


@verifier("CLIENT LIST")
def _verify_client_list(rs, py) -> None:
    rs_list = rs.client_list()
    assert isinstance(rs_list, list) and rs_list
    # First entry should have 'id' key (str or bytes)
    first = rs_list[0]
    assert "id" in first or b"id" in first


@verifier("OBJECT IDLETIME")
def _verify_object_idletime(rs, py) -> None:
    py.set("k", b"v")
    out = rs.object_idletime("k")
    assert isinstance(out, int) and out >= 0


@verifier("OBJECT FREQ")
def _verify_object_freq(rs, py) -> None:
    import pytest as _pytest

    # Requires LFU policy; skip if we're on the default (allkeys-lru).
    policy = py.config_get("maxmemory-policy")
    val = next(iter(policy.values()), b"")
    if isinstance(val, str):
        val = val.encode()
    if not val.startswith(b"allkeys-lfu"):
        _pytest.skip("OBJECT FREQ requires LFU policy")


@verifier("TIME")
def _verify_time(rs, py) -> None:
    rs_t = rs.time()
    py_t = py.time()
    # (sec, usec) tuples; assert shape and that both are within 5s of each other.
    assert isinstance(rs_t, tuple) and len(rs_t) == 2
    assert abs(rs_t[0] - py_t[0]) <= 5


@verifier("LASTSAVE")
def _verify_lastsave(rs, py) -> None:
    assert isinstance(rs.lastsave(), int)
