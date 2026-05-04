"""Server-side scripting — EVAL, EVALSHA, FCALL, FUNCTION, SCRIPT *."""

import contextlib

import pytest
from redis_rs_py.exceptions import DataError, NoScriptError, ResponseError

# Script and function state lives on the SERVER, not in the per-worker DB.
# SCRIPT LOAD / FLUSH and FUNCTION LOAD / LIST / FLUSH all touch the same
# server-global cache, so parallel workers race each other (one worker's
# SCRIPT FLUSH purges another worker's just-loaded SHA). The xdist_group
# mark forces every test in this file onto the same worker, serialising the
# global-state mutations.
pytestmark = pytest.mark.xdist_group(name="redis_global_state")

# Standard Lua script: returns the first key.
ECHO_KEY_SCRIPT = "return KEYS[1]"
INCR_BY_SCRIPT = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"


class TestEval:
    def test_eval_returns_first_key_as_bytes(self, driver) -> None:
        result = driver.eval(ECHO_KEY_SCRIPT, ["mykey"], [])
        assert result == b"mykey"

    def test_eval_with_args_modifies_state(self, driver, redis_py_client) -> None:
        result = driver.eval(INCR_BY_SCRIPT, ["k"], [b"42"])
        assert result == b"42"
        assert redis_py_client.get("k") == b"42"

    def test_eval_returns_int(self, driver) -> None:
        result = driver.eval("return 99", [], [])
        assert result == 99

    def test_eval_returns_table_as_list(self, driver) -> None:
        result = driver.eval("return {1, 2, 'three'}", [], [])
        assert result == [1, 2, b"three"]

    def test_eval_nil_becomes_none(self, driver) -> None:
        # Lua nil is converted to RESP nil → Python None.
        result = driver.eval("return nil", [], [])
        assert result is None

    def test_eval_user_error_raises_response_error(self, driver) -> None:
        with pytest.raises(ResponseError):
            driver.eval("return redis.error_reply('user error')", [], [])

    @pytest.mark.asyncio
    async def test_aeval_basic(self, driver) -> None:
        assert await driver.aeval(ECHO_KEY_SCRIPT, ["k"], []) == b"k"


class TestEvalsha:
    def test_evalsha_unknown_raises_noscripterror(self, driver) -> None:
        with pytest.raises(NoScriptError):
            driver.evalsha("0" * 40, [], [])

    def test_evalsha_after_script_load(self, driver) -> None:
        sha = driver.script_load(ECHO_KEY_SCRIPT)
        assert driver.evalsha(sha, ["k"], []) == b"k"

    @pytest.mark.asyncio
    async def test_aevalsha_after_script_load(self, driver) -> None:
        sha = await driver.ascript_load(ECHO_KEY_SCRIPT)
        assert await driver.aevalsha(sha, ["k"], []) == b"k"


class TestEvalRo:
    """EVAL_RO / EVALSHA_RO — read-only variants (Redis 7+)."""

    def test_eval_ro_basic(self, driver) -> None:
        assert driver.eval_ro("return 7", [], []) == 7

    def test_eval_ro_rejects_writes(self, driver) -> None:
        # SET is a write; EVAL_RO must refuse to call it.
        with pytest.raises(ResponseError):
            driver.eval_ro("redis.call('SET', KEYS[1], 'x'); return 1", ["k"], [])

    def test_evalsha_ro(self, driver) -> None:
        sha = driver.script_load("return ARGV[1]")
        assert driver.evalsha_ro(sha, [], [b"value"]) == b"value"


class TestScriptLoad:
    def test_script_load_returns_sha1(self, driver) -> None:
        sha = driver.script_load("return 1")
        assert isinstance(sha, str)
        assert len(sha) == 40  # SHA1 hex = 40 chars

    def test_script_load_idempotent(self, driver) -> None:
        sha1 = driver.script_load("return 1")
        sha2 = driver.script_load("return 1")
        assert sha1 == sha2

    @pytest.mark.asyncio
    async def test_ascript_load(self, driver) -> None:
        sha = await driver.ascript_load("return 'ok'")
        assert isinstance(sha, str) and len(sha) == 40


class TestScriptExists:
    def test_script_exists_after_load(self, driver) -> None:
        sha = driver.script_load("return 1")
        assert driver.script_exists(sha) == [True]

    def test_script_exists_variadic(self, driver) -> None:
        sha = driver.script_load("return 1")
        assert driver.script_exists(sha, "0" * 40) == [True, False]

    def test_script_exists_unknown_only(self, driver) -> None:
        assert driver.script_exists("0" * 40) == [False]

    @pytest.mark.asyncio
    async def test_ascript_exists(self, driver) -> None:
        sha = await driver.ascript_load("return 1")
        assert await driver.ascript_exists(sha) == [True]


class TestScriptFlush:
    def test_script_flush_default_async_mode(self, driver) -> None:
        sha = driver.script_load("return 1")
        driver.script_flush()
        assert driver.script_exists(sha) == [False]

    def test_script_flush_sync_mode(self, driver) -> None:
        sha = driver.script_load("return 1")
        driver.script_flush(mode="SYNC")
        assert driver.script_exists(sha) == [False]

    def test_script_flush_async_mode_explicit(self, driver) -> None:
        sha = driver.script_load("return 1")
        driver.script_flush(mode="ASYNC")
        assert driver.script_exists(sha) == [False]

    def test_script_flush_invalid_mode_raises(self, driver) -> None:
        with pytest.raises(DataError):
            driver.script_flush(mode="WHATEVER")

    @pytest.mark.asyncio
    async def test_ascript_flush(self, driver) -> None:
        sha = await driver.ascript_load("return 1")
        await driver.ascript_flush()
        assert driver.script_exists(sha) == [False]


class TestScriptKill:
    def test_script_kill_with_no_script_running_raises(self, driver) -> None:
        # NOTBUSY is the server's response when nothing is running.
        # Valkey encodes this as "NotBusy: ..." so match case-insensitively.
        with pytest.raises(ResponseError, match=r"(?i)notbusy"):
            driver.script_kill()


SAMPLE_LIBRARY = """#!lua name=mylib
redis.register_function('myecho', function(keys, args) return args[1] end)
redis.register_function{
  function_name = 'mywrite',
  callback = function(keys, args) redis.call('SET', keys[1], args[1]); return 'OK' end
}
redis.register_function{
  function_name = 'myreadonly',
  callback = function(keys, args) return redis.call('GET', keys[1]) end,
  flags = {'no-writes'}
}
"""


class TestFcall:
    def _load(self, driver) -> None:
        # Make sure no other library named mylib exists from a previous test.
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY, replace=True)

    def test_fcall_basic(self, driver) -> None:
        self._load(driver)
        assert driver.fcall("myecho", [], [b"hello"]) == b"hello"

    def test_fcall_with_keys(self, driver, redis_py_client) -> None:
        self._load(driver)
        assert driver.fcall("mywrite", ["k"], [b"value"]) == b"OK"
        assert redis_py_client.get("k") == b"value"

    def test_fcall_unknown_function_raises(self, driver) -> None:
        with pytest.raises(ResponseError):
            driver.fcall("nonexistent", [], [])

    @pytest.mark.asyncio
    async def test_afcall(self, driver) -> None:
        self._load(driver)
        assert await driver.afcall("myecho", [], [b"x"]) == b"x"


class TestFcallRo:
    def _load(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY, replace=True)

    def test_fcall_ro_no_writes_function_works(self, driver) -> None:
        self._load(driver)
        driver.set("k", b"hello")
        # myreadonly is flagged 'no-writes' — fcall_ro accepts it.
        assert driver.fcall_ro("myreadonly", ["k"], []) == b"hello"

    def test_fcall_ro_rejects_writes(self, driver) -> None:
        self._load(driver)
        with pytest.raises(ResponseError):
            driver.fcall_ro("mywrite", ["k"], [b"v"])


class TestFunction:
    def test_function_load_returns_library_name(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        name = driver.function_load(SAMPLE_LIBRARY)
        assert name == "mylib"

    def test_function_load_replace(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY)
        # Without replace, re-loading raises.
        with pytest.raises(ResponseError):
            driver.function_load(SAMPLE_LIBRARY)
        # With replace, succeeds.
        assert driver.function_load(SAMPLE_LIBRARY, replace=True) == "mylib"

    def test_function_list_basic(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY)
        result = driver.function_list()
        # Result is a list of dicts (or arrays the upstream renders as lists).
        assert isinstance(result, list)
        assert any(b"mylib" in str(item).encode() for item in result)

    def test_function_list_with_library_filter(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY)
        result = driver.function_list(library="mylib")
        assert len(result) == 1

    def test_function_list_withcode(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY)
        result = driver.function_list(library="mylib", withcode=True)
        # When withcode=True the entry includes the script source under library_code.
        assert len(result) == 1
        entry = result[0]
        code = entry.get(b"library_code") or entry.get("library_code")
        if isinstance(code, bytes):
            assert SAMPLE_LIBRARY.encode() == code
        else:
            assert code == SAMPLE_LIBRARY

    def test_function_dump_returns_bytes(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY)
        dump = driver.function_dump()
        assert isinstance(dump, bytes)
        assert len(dump) > 0

    def test_function_restore_roundtrip(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY)
        dump = driver.function_dump()
        driver.function_flush()
        driver.function_restore(dump, policy="REPLACE")
        # After restore, the library is back.
        assert driver.fcall("myecho", [], [b"x"]) == b"x"

    def test_function_flush_removes_libraries(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        driver.function_load(SAMPLE_LIBRARY)
        driver.function_flush()
        assert driver.function_list() == []

    def test_function_stats_no_running(self, driver) -> None:
        # Returns either an empty stats blob or the schema with no running script.
        result = driver.function_stats()
        assert result is not None

    def test_function_kill_no_script_running(self, driver) -> None:
        with pytest.raises(ResponseError):
            driver.function_kill()

    @pytest.mark.asyncio
    async def test_afunction_load(self, driver) -> None:
        with contextlib.suppress(Exception):
            driver.function_delete("mylib")
        name = await driver.afunction_load(SAMPLE_LIBRARY)
        assert name == "mylib"
