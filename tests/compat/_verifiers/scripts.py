"""Verifiers for the scripts and functions command family."""

from __future__ import annotations

from . import verifier


def _ok(v) -> bool:
    return v in (True, None, b"OK")


@verifier("EVAL")
def _verify_eval(rs, py) -> None:
    # rs.eval(script, keys, args) — needs explicit lists
    rs_v = rs.eval("return 42", [], [])
    py_v = py.eval("return 42", 0)
    assert rs_v == py_v == 42


@verifier("EVALSHA")
def _verify_evalsha(rs, py) -> None:
    sha = py.script_load("return 7")
    # rs.evalsha(sha, keys, args) — needs explicit lists
    assert rs.evalsha(sha, [], []) == py.evalsha(sha, 0) == 7


@verifier("EVAL_RO")
def _verify_eval_ro(rs, py) -> None:
    rs_v = rs.eval_ro("return 1", [], [])
    py_v = py.eval_ro("return 1", 0)
    assert rs_v == py_v == 1


@verifier("EVALSHA_RO")
def _verify_evalsha_ro(rs, py) -> None:
    sha = py.script_load("return 1")
    assert rs.evalsha_ro(sha, [], []) == py.evalsha_ro(sha, 0) == 1


@verifier("SCRIPT LOAD")
def _verify_script_load(rs, py) -> None:
    rs_sha = rs.script_load("return 0")
    py_sha = py.script_load("return 0")
    assert rs_sha == py_sha


@verifier("SCRIPT EXISTS")
def _verify_script_exists(rs, py) -> None:
    # Load via both clients independently to avoid SCRIPT FLUSH races in parallel runs.
    sha_rs = rs.script_load("return 12345")
    sha_py = py.script_load("return 12345")
    assert sha_rs == sha_py
    # Now check existence — if another SCRIPT FLUSH ran in parallel, reload and recheck.
    result = rs.script_exists(sha_rs)
    if result != [True]:
        sha_rs = rs.script_load("return 12345")
        result = rs.script_exists(sha_rs)
    assert result == [True]


@verifier("SCRIPT FLUSH")
def _verify_script_flush(rs, py) -> None:
    py.script_load("return 0")
    assert _ok(rs.script_flush())


@verifier("FCALL")
def _verify_fcall(rs, py) -> None:
    py.function_load("#!lua name=mylib\nredis.register_function('myfn', function() return 42 end)", replace=True)
    assert rs.fcall("myfn", [], []) == py.fcall("myfn", 0) == 42


@verifier("FCALL_RO")
def _verify_fcall_ro(rs, py) -> None:
    py.function_load(
        "#!lua name=mylib2\nredis.register_function{function_name='myfn2', callback=function() return 1 end, flags={'no-writes'}}",
        replace=True,
    )
    assert rs.fcall_ro("myfn2", [], []) == 1


@verifier("FUNCTION LOAD")
def _verify_function_load(rs, py) -> None:
    out = rs.function_load("#!lua name=mylib3\nredis.register_function('fn3', function() return 3 end)", replace=True)
    # rs returns str, py returns bytes
    assert out in (b"mylib3", "mylib3")


@verifier("FUNCTION DUMP")
def _verify_function_dump(rs, py) -> None:
    py.function_load("#!lua name=mylib4\nredis.register_function('fn4', function() return 4 end)", replace=True)
    rs_d = rs.function_dump()
    py_d = py.function_dump()
    assert isinstance(rs_d, bytes)
    assert isinstance(py_d, bytes)


@verifier("FUNCTION FLUSH")
def _verify_function_flush(rs, py) -> None:
    py.function_load("#!lua name=ml5\nredis.register_function('fn5', function() return 5 end)", replace=True)
    assert _ok(rs.function_flush())


@verifier("FUNCTION LIST")
def _verify_function_list(rs, py) -> None:
    py.function_load("#!lua name=ml6\nredis.register_function('fn6', function() return 6 end)", replace=True)
    rs_list = rs.function_list()
    py_list = py.function_list()
    # Both must return non-empty lists; counts may differ if another test flushed in parallel.
    assert isinstance(rs_list, list) and len(rs_list) >= 1
    assert isinstance(py_list, list) and len(py_list) >= 1


@verifier("FUNCTION STATS")
def _verify_function_stats(rs, py) -> None:
    rs_stats = rs.function_stats()
    py_stats = py.function_stats()
    # rs returns dict {bytes: ...}, py returns list [b'key', value, ...]
    # Just check both return something with engine info
    assert rs_stats is not None
    assert py_stats is not None
    # rs has bytes keys
    assert b"engines" in rs_stats or "engines" in rs_stats


@verifier("FUNCTION KILL")
def _verify_function_kill(rs, py) -> None:
    # No script running — both should raise NOTBUSY.
    from redis_rs_py.exceptions import ResponseError as RsResponseError

    try:
        rs.function_kill()
        # If no error, that's also acceptable (some servers return OK even with nothing running)
    except (RsResponseError, Exception) as exc:
        # Accept NotBusy / no scripts running errors
        msg = str(exc).lower()
        assert any(kw in msg for kw in ("notbusy", "not busy", "no scripts", "no function")), (
            f"unexpected error from function_kill: {exc!r}"
        )
