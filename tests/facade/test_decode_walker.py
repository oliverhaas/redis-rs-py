"""Unit tests for the decode_walk C-extension helper (_facade_decode_walk).

These tests exercise the recursive walker directly without going through any
Redis command, so they require no live server.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def walk():
    """Return a callable that applies decode_walk with utf-8 / strict defaults."""
    from redis_rs_py._driver import _facade_decode_walk

    def _walk(value, encoding="utf-8", errors="strict"):
        return _facade_decode_walk(value, encoding, errors)

    return _walk


# ---------------------------------------------------------------------------
# Scalar leaves
# ---------------------------------------------------------------------------


def test_bytes_decoded(walk):
    assert walk(b"hello") == "hello"


def test_str_passthrough(walk):
    assert walk("already") == "already"


def test_int_passthrough(walk):
    assert walk(42) == 42


def test_float_passthrough(walk):
    assert walk(3.14) == 3.14


def test_none_passthrough(walk):
    assert walk(None) is None


def test_bool_passthrough(walk):
    assert walk(True) is True
    assert walk(False) is False


# ---------------------------------------------------------------------------
# Lists
# ---------------------------------------------------------------------------


def test_list_of_bytes(walk):
    assert walk([b"a", b"b"]) == ["a", "b"]


def test_list_passthrough_non_bytes(walk):
    assert walk([1, 2, 3]) == [1, 2, 3]


def test_list_mixed(walk):
    assert walk([b"a", 1, b"b"]) == ["a", 1, "b"]


def test_nested_list(walk):
    assert walk([b"x", [b"y", b"z"]]) == ["x", ["y", "z"]]


# ---------------------------------------------------------------------------
# Dicts — both keys and values must be walked
# ---------------------------------------------------------------------------


def test_dict_bytes_values(walk):
    assert walk({b"k": b"v"}) == {"k": "v"}


def test_dict_str_keys_bytes_values(walk):
    assert walk({"key": b"value"}) == {"key": "value"}


def test_dict_bytes_keys_str_values(walk):
    assert walk({b"key": "value"}) == {"key": "value"}


def test_dict_nested(walk):
    assert walk({b"outer": {b"inner": b"val"}}) == {"outer": {"inner": "val"}}


# ---------------------------------------------------------------------------
# Tuples
# ---------------------------------------------------------------------------


def test_tuple_decoded(walk):
    result = walk((b"a", b"b"))
    assert result == ("a", "b")
    assert isinstance(result, tuple)


def test_tuple_nested(walk):
    assert walk((b"x", (b"y",))) == ("x", ("y",))


# ---------------------------------------------------------------------------
# Sets
# ---------------------------------------------------------------------------


def test_set_decoded(walk):
    result = walk({b"a", b"b", b"c"})
    assert result == {"a", "b", "c"}
    assert isinstance(result, set)


# ---------------------------------------------------------------------------
# Encoding / errors
# ---------------------------------------------------------------------------


def test_encoding_latin1(walk):
    assert walk(b"\xe9", encoding="latin-1") == "\xe9"


def test_errors_replace():
    from redis_rs_py._driver import _facade_decode_walk

    # 0xFF is not valid UTF-8; "replace" should not raise
    result = _facade_decode_walk(b"\xff", "utf-8", "replace")
    assert "�" in result


def test_errors_strict_raises():
    from redis_rs_py._driver import _facade_decode_walk

    with pytest.raises(UnicodeDecodeError):
        _facade_decode_walk(b"\xff", "utf-8", "strict")
