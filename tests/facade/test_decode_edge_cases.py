"""Edge-case tests for decode_responses mode.

Covers:
- decode_responses=False still returns bytes (default behaviour unchanged)
- custom encoding= / encoding_errors= propagate correctly
- None / numeric responses pass through unchanged in both modes
- Nested containers (lists of tuples, dicts-of-lists) are decoded recursively
"""

import pytest


@pytest.fixture
def r_bytes(valkey_url: str):
    """Default Redis (decode_responses=False); returns bytes as before."""
    from redis_rs_py import Redis

    client = Redis.from_url(valkey_url)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def r_decode(valkey_url: str):
    """Redis with decode_responses=True, utf-8/strict."""
    from redis_rs_py import Redis

    client = Redis.from_url(valkey_url, decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def r_latin1(valkey_url: str):
    """Redis with decode_responses=True, latin-1 encoding."""
    from redis_rs_py import Redis

    client = Redis.from_url(
        valkey_url,
        decode_responses=True,
        encoding="latin-1",
        encoding_errors="strict",
    )
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


# ---------------------------------------------------------------------------
# Default mode still returns bytes
# ---------------------------------------------------------------------------


def test_default_mode_get_returns_bytes(r_bytes):
    r_bytes.set("k", b"hello")
    result = r_bytes.get("k")
    assert result == b"hello"
    assert isinstance(result, bytes)


def test_default_mode_mget_returns_bytes(r_bytes):
    r_bytes.set("a", b"one")
    result = r_bytes.mget("a")
    assert result == [b"one"]
    assert isinstance(result[0], bytes)


def test_default_mode_smembers_returns_bytes(r_bytes):
    r_bytes.sadd("s", b"member")
    result = r_bytes.smembers("s")
    assert b"member" in result


def test_default_mode_hgetall_returns_bytes(r_bytes):
    r_bytes.hset("h", mapping={"f": b"v"})
    result = r_bytes.hgetall("h")
    # keys and values are bytes
    assert b"f" in result
    assert result[b"f"] == b"v"


# ---------------------------------------------------------------------------
# decode_responses=True leaves numerics unchanged
# ---------------------------------------------------------------------------


def test_decode_incr_returns_int(r_decode):
    r_decode.set("n", b"0")
    result = r_decode.incr("n")
    assert result == 1
    assert isinstance(result, int)


def test_decode_zadd_returns_int(r_decode):
    result = r_decode.zadd("z", {"m": 1.0})
    assert isinstance(result, int)


def test_decode_hset_returns_int(r_decode):
    result = r_decode.hset("h", "f", b"v")
    assert isinstance(result, int)


# ---------------------------------------------------------------------------
# None pass-through (missing keys)
# ---------------------------------------------------------------------------


def test_decode_get_missing_returns_none(r_decode):
    assert r_decode.get("missing") is None


def test_decode_hget_missing_returns_none(r_decode):
    assert r_decode.hget("missing_key", "field") is None


# ---------------------------------------------------------------------------
# Custom encoding
# ---------------------------------------------------------------------------


def test_latin1_roundtrip(r_latin1):
    """A latin-1 byte (0xe9 == é) stored and retrieved as str."""
    # Store raw bytes via a separate bytes-mode client connected to same URL
    import redis_rs_py

    r_plain = redis_rs_py.Redis.from_url(
        r_latin1.connection_url,
        decode_responses=False,
    )
    try:
        r_plain.set("latin", b"\xe9")
    finally:
        r_plain.close()

    result = r_latin1.get("latin")
    assert result == "\xe9"
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# encoding_errors propagation
# ---------------------------------------------------------------------------


def test_encoding_errors_replace_no_raise(valkey_url: str):
    """UTF-8 decoding with encoding_errors='replace' handles bad bytes gracefully."""
    import redis_rs_py

    r_plain = redis_rs_py.Redis.from_url(valkey_url, decode_responses=False)
    r_plain.set("bad_utf8", b"\xff\xfe")
    r_plain.close()

    r_replace = redis_rs_py.Redis.from_url(
        valkey_url,
        decode_responses=True,
        encoding="utf-8",
        encoding_errors="replace",
    )
    try:
        result = r_replace.get("bad_utf8")
        assert isinstance(result, str)
        assert "�" in result  # U+FFFD replacement character
    finally:
        r_replace.close()


def test_encoding_errors_strict_raises(valkey_url: str):
    """UTF-8 strict decoding raises UnicodeDecodeError for invalid bytes."""
    import redis_rs_py

    r_plain = redis_rs_py.Redis.from_url(valkey_url, decode_responses=False)
    r_plain.set("bad_utf8", b"\xff")
    r_plain.close()

    r_strict = redis_rs_py.Redis.from_url(
        valkey_url,
        decode_responses=True,
        encoding="utf-8",
        encoding_errors="strict",
    )
    try:
        with pytest.raises(UnicodeDecodeError):
            r_strict.get("bad_utf8")
    finally:
        r_strict.close()


# ---------------------------------------------------------------------------
# Nested containers
# ---------------------------------------------------------------------------


def test_decode_scan_cursor_int_keys_str(r_decode):
    r_decode.set("scan_key", b"value")
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cursor, keys = r_decode.scan()
    assert isinstance(cursor, int)
    assert all(isinstance(k, str) for k in keys)


def test_decode_hmget_returns_str_list(r_decode):
    r_decode.hset("h", mapping={"f1": b"v1", "f2": b"v2"})
    result = r_decode.hmget("h", "f1", "f2")
    assert result == ["v1", "v2"]
    assert all(isinstance(v, str) for v in result)


def test_decode_hscan_returns_str_dict(r_decode):
    r_decode.hset("h", mapping={"f": b"v"})
    _cursor, pairs = r_decode.hscan("h")
    assert all(isinstance(k, str) for k in pairs)
    assert all(isinstance(v, str) for v in pairs.values())


def test_decode_sscan_returns_str_members(r_decode):
    r_decode.sadd("s", b"alpha", b"beta")
    _cursor, members = r_decode.sscan("s")
    assert all(isinstance(m, str) for m in members)


def test_decode_zscan_member_str_score_float(r_decode):
    r_decode.zadd("z", {b"member": 1.5})
    _cursor, pairs = r_decode.zscan("z")
    for member, score in pairs:
        assert isinstance(member, str)
        assert isinstance(score, float)
