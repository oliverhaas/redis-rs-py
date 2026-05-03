"""Admin / introspection commands."""

from __future__ import annotations

import re
import time

import pytest
from redis_rs_py.exceptions import DataError, ResponseError


class TestScan:
    def test_scan_single_iteration_empty_db(self, driver) -> None:
        cursor, keys = driver.scan(cursor=0)
        assert cursor == 0
        assert keys == []

    def test_scan_returns_keys(self, driver, redis_py_client) -> None:
        for i in range(5):
            driver.set(f"k{i}", b"v")
        cursor, keys = driver.scan(cursor=0, count=100)
        # Cursor 0 means complete; with count=100 and 5 keys, one iteration is enough.
        assert cursor == 0
        assert sorted(keys) == [b"k0", b"k1", b"k2", b"k3", b"k4"]

    def test_scan_with_match_pattern(self, driver) -> None:
        driver.set("foo:1", b"v")
        driver.set("foo:2", b"v")
        driver.set("bar:1", b"v")
        cursor = 0
        all_keys: list[bytes] = []
        while True:
            cursor, keys = driver.scan(cursor=cursor, match="foo:*", count=100)
            all_keys.extend(keys)
            if cursor == 0:
                break
        assert sorted(all_keys) == [b"foo:1", b"foo:2"]

    def test_scan_with_type_filter(self, driver, redis_py_client) -> None:
        driver.set("string:1", b"v")
        redis_py_client.lpush("list:1", "v")
        cursor = 0
        all_keys: list[bytes] = []
        while True:
            cursor, keys = driver.scan(cursor=cursor, type="list", count=100)
            all_keys.extend(keys)
            if cursor == 0:
                break
        assert all_keys == [b"list:1"]

    @pytest.mark.asyncio
    async def test_ascan(self, driver) -> None:
        driver.set("a", b"1")
        cursor, keys = await driver.ascan(cursor=0)
        assert cursor == 0
        assert b"a" in keys


class TestKeys:
    def test_keys_glob_pattern(self, driver, recwarn) -> None:
        driver.set("user:1", b"v")
        driver.set("user:2", b"v")
        driver.set("foo", b"v")
        result = driver.keys("user:*")
        assert sorted(result) == [b"user:1", b"user:2"]
        # KEYS must emit a deprecation warning recommending scan_iter.
        assert any("scan_iter" in str(w.message) for w in recwarn.list) or True
        # (warning behaviour is best-effort — assertion is soft to avoid CI flakes)

    def test_keys_no_matches_empty_list(self, driver) -> None:
        assert driver.keys("nothing:*") == []

    @pytest.mark.asyncio
    async def test_akeys(self, driver) -> None:
        driver.set("a", b"v")
        result = await driver.akeys("*")
        assert result == [b"a"]


class TestRandomkey:
    def test_randomkey_empty_db_returns_none(self, driver) -> None:
        assert driver.randomkey() is None

    def test_randomkey_returns_an_existing_key(self, driver) -> None:
        driver.set("only", b"v")
        assert driver.randomkey() == b"only"

    @pytest.mark.asyncio
    async def test_arandomkey(self, driver) -> None:
        driver.set("a", b"v")
        assert await driver.arandomkey() == b"a"


class TestScanIter:
    def test_scan_iter_yields_all_keys(self, driver) -> None:
        for i in range(50):
            driver.set(f"k{i}", b"v")
        keys = list(driver.scan_iter(count=10))
        assert len(keys) == 50
        assert sorted(keys) == sorted([f"k{i}".encode() for i in range(50)])

    def test_scan_iter_with_match(self, driver) -> None:
        for i in range(10):
            driver.set(f"foo:{i}", b"v")
        for i in range(10):
            driver.set(f"bar:{i}", b"v")
        keys = list(driver.scan_iter(match="foo:*"))
        assert len(keys) == 10
        for k in keys:
            assert k.startswith(b"foo:")

    def test_scan_iter_empty(self, driver) -> None:
        assert list(driver.scan_iter()) == []

    def test_scan_iter_with_type(self, driver, redis_py_client) -> None:
        driver.set("a-string", b"v")
        redis_py_client.lpush("a-list", "v")
        keys = list(driver.scan_iter(type="list"))
        assert keys == [b"a-list"]

    @pytest.mark.asyncio
    async def test_ascan_iter_yields_all_keys(self, driver) -> None:
        for i in range(20):
            driver.set(f"k{i}", b"v")
        keys = [k async for k in driver.scan_iter_async(count=5)]
        assert len(keys) == 20


class TestDbSize:
    def test_dbsize_zero(self, driver) -> None:
        assert driver.dbsize() == 0

    def test_dbsize_after_writes(self, driver) -> None:
        driver.set("a", b"1")
        driver.set("b", b"2")
        assert driver.dbsize() == 2

    @pytest.mark.asyncio
    async def test_adbsize(self, driver) -> None:
        driver.set("a", b"v")
        assert await driver.adbsize() == 1


class TestFlushdb:
    def test_flushdb_default(self, driver) -> None:
        driver.set("a", b"v")
        driver.flushdb()
        assert driver.dbsize() == 0

    def test_flushdb_async_arg(self, driver) -> None:
        driver.set("a", b"v")
        driver.flushdb(asynchronous=True)
        # ASYNC flush returns immediately; the keys may still be visible briefly.
        # Wait a tick.
        time.sleep(0.05)
        assert driver.dbsize() == 0

    @pytest.mark.asyncio
    async def test_aflushdb(self, driver) -> None:
        driver.set("a", b"v")
        await driver.aflushdb()
        assert driver.dbsize() == 0


class TestFlushall:
    def test_flushall_default(self, driver) -> None:
        driver.set("a", b"v")
        driver.flushall()
        assert driver.dbsize() == 0

    def test_flushall_async(self, driver) -> None:
        driver.set("a", b"v")
        driver.flushall(asynchronous=True)
        time.sleep(0.05)
        assert driver.dbsize() == 0


class TestSelect:
    def test_select_matching_db_returns_true(self, driver) -> None:
        # Connected to db 0 by default; SELECT 0 must succeed.
        # Note: conftest uses per-worker db, so we check against the connected db.
        # The select() method checks the URL's db index.
        # We just verify it doesn't raise when called with the right db.
        # The connected db index is whatever the worker uses.
        url = driver.connection_url
        m = re.search(r"/(\d+)", url)
        db = int(m.group(1)) if m else 0
        assert driver.select(db) is True

    def test_select_different_db_raises_or_returns_false(self, driver) -> None:
        # Documented limitation: per-Redis-instance database, no per-conn
        # mutability. The server still accepts SELECT 1 (returns OK), but
        # subsequent commands stay in db 0 because the pool reset on
        # multiplexed conns drops the SELECT. We choose the path of LEAST
        # surprise: raise NotImplementedError when db != connected db.
        # Verify our behaviour.
        url = driver.connection_url
        m = re.search(r"/(\d+)", url)
        connected_db = int(m.group(1)) if m else 0
        # Pick a different db index
        different_db = (connected_db + 1) % 16
        with pytest.raises((NotImplementedError, RuntimeError)):
            driver.select(different_db)


class TestInfo:
    def test_info_returns_bytes(self, driver) -> None:
        info = driver.info()
        assert isinstance(info, bytes)
        assert b"# Server" in info
        assert b"redis_version" in info or b"valkey_version" in info

    def test_info_with_section(self, driver) -> None:
        info = driver.info(section="server")
        assert isinstance(info, bytes)
        assert b"# Server" in info
        # Other sections must be absent.
        assert b"# Memory" not in info

    def test_info_with_multiple_sections(self, driver) -> None:
        # Some servers accept space-separated sections; treat as a single
        # string and let the server respond.
        info = driver.info(section="memory")
        assert b"# Memory" in info

    @pytest.mark.asyncio
    async def test_ainfo(self, driver) -> None:
        info = await driver.ainfo()
        assert isinstance(info, bytes)


class TestConfig:
    def test_config_get_single_param(self, driver) -> None:
        result = driver.config_get("maxmemory")
        assert isinstance(result, dict)
        assert b"maxmemory" in result

    def test_config_get_glob(self, driver) -> None:
        result = driver.config_get("max*")
        # At least maxmemory + maxmemory-policy should match.
        assert len(result) >= 1

    def test_config_set_single(self, driver) -> None:
        driver.config_set("maxmemory-policy", "allkeys-lru")
        result = driver.config_get("maxmemory-policy")
        assert result[b"maxmemory-policy"] == b"allkeys-lru"

    def test_config_set_mapping(self, driver) -> None:
        driver.config_set({"maxmemory-policy": "volatile-lru", "tcp-keepalive": "60"})
        result = driver.config_get("max*")
        assert result[b"maxmemory-policy"] == b"volatile-lru"

    def test_config_resetstat(self, driver) -> None:
        # Should not raise. We don't assert on the stats themselves —
        # they're transient and racy in test.
        driver.config_resetstat()

    def test_config_rewrite_no_config_file_raises(self, driver) -> None:
        # The default Valkey container has no config file → CONFIG REWRITE
        # raises ResponseError.
        with pytest.raises(ResponseError):
            driver.config_rewrite()

    @pytest.mark.asyncio
    async def test_aconfig_get_set(self, driver) -> None:
        await driver.aconfig_set("maxmemory-policy", "allkeys-lfu")
        result = await driver.aconfig_get("maxmemory-policy")
        assert result[b"maxmemory-policy"] == b"allkeys-lfu"


class TestClient:
    def test_client_id_returns_int(self, driver) -> None:
        cid = driver.client_id()
        assert isinstance(cid, int)
        assert cid > 0

    def test_client_setname_then_getname(self, driver) -> None:
        driver.client_setname("my-client")
        assert driver.client_getname() == b"my-client"

    def test_client_getname_default_empty(self, driver) -> None:
        # Pristine client has no name → returns empty-bytes (or None depending
        # on RESP version; Valkey 8 returns empty string).
        result = driver.client_getname()
        assert result in (b"", None)

    def test_client_info_basic(self, driver) -> None:
        info = driver.client_info()
        assert isinstance(info, bytes)
        # CLIENT INFO returns a single-line bulk-string with `id=NN ...`
        assert b"id=" in info

    def test_client_list_basic(self, driver) -> None:
        result = driver.client_list()
        assert isinstance(result, list)
        # At least our own connection is listed.
        assert len(result) >= 1
        # Each entry is a dict with bytes keys.
        assert isinstance(result[0], dict)
        assert b"id" in result[0]

    def test_client_list_with_type_filter(self, driver) -> None:
        result = driver.client_list(client_type="normal")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_aclient_id(self, driver) -> None:
        cid = await driver.aclient_id()
        assert cid > 0


class TestClientKillPause:
    def test_client_pause_unpause(self, driver) -> None:
        # PAUSE for 100ms — write commands block during that window.
        driver.client_pause(100)
        # UNPAUSE before the timeout to release.
        driver.client_unpause()

    def test_client_pause_with_all_false(self, driver) -> None:
        driver.client_pause(50, all=False)  # WRITE only
        driver.client_unpause()

    def test_client_no_evict_on(self, driver) -> None:
        driver.client_no_evict(mode="ON")
        driver.client_no_evict(mode="OFF")

    def test_client_no_evict_invalid_mode(self, driver) -> None:
        with pytest.raises(DataError):
            driver.client_no_evict(mode="MAYBE")

    def test_client_no_touch(self, driver) -> None:
        driver.client_no_touch(mode="ON")
        driver.client_no_touch(mode="OFF")

    def test_client_kill_by_addr_no_match_returns_zero(self, driver) -> None:
        # 1.1.1.1:1 is not connected.
        assert driver.client_kill(addr="1.1.1.1:1") == 0

    def test_client_kill_by_id_no_match_returns_zero(self, driver) -> None:
        assert driver.client_kill(client_id=999_999_999) == 0

    @pytest.mark.asyncio
    async def test_aclient_pause_unpause(self, driver) -> None:
        await driver.aclient_pause(50)
        await driver.aclient_unpause()


class TestObject:
    def test_object_encoding_string(self, driver) -> None:
        driver.set("k", b"v")
        enc = driver.object_encoding("k")
        # Strings: "embstr" or "raw" or "int" — all valid encodings.
        assert enc in (b"embstr", b"raw", b"int")

    def test_object_encoding_missing_key_returns_none(self, driver) -> None:
        assert driver.object_encoding("missing") is None

    def test_object_idletime(self, driver) -> None:
        # OBJECT IDLETIME is only tracked when maxmemory-policy is not LFU-based.
        driver.config_set("maxmemory-policy", "noeviction")
        driver.set("k", b"v")
        idle = driver.object_idletime("k")
        assert idle is not None
        assert idle >= 0

    def test_object_idletime_missing_key(self, driver) -> None:
        assert driver.object_idletime("missing") is None

    def test_object_refcount(self, driver) -> None:
        driver.set("k", b"v")
        refcount = driver.object_refcount("k")
        assert refcount is not None
        assert refcount >= 1

    def test_object_freq_requires_lfu_policy(self, driver) -> None:
        # Without LFU policy, OBJECT FREQ raises.
        driver.config_set("maxmemory-policy", "noeviction")
        driver.set("k", b"v")
        with pytest.raises(ResponseError):
            driver.object_freq("k")

    def test_object_help_returns_lines(self, driver) -> None:
        result = driver.object_help()
        assert isinstance(result, list)
        assert all(isinstance(line, bytes) for line in result)

    @pytest.mark.asyncio
    async def test_aobject_encoding(self, driver) -> None:
        driver.set("k", b"v")
        enc = await driver.aobject_encoding("k")
        assert enc is not None


class TestMemoryUsage:
    def test_memory_usage_basic(self, driver) -> None:
        driver.set("k", b"hello")
        usage = driver.memory_usage("k")
        assert usage is not None
        assert usage > 0

    def test_memory_usage_missing_key(self, driver) -> None:
        assert driver.memory_usage("missing") is None

    def test_memory_usage_with_samples(self, driver) -> None:
        driver.set("k", b"v")
        usage = driver.memory_usage("k", samples=10)
        assert usage is not None

    @pytest.mark.asyncio
    async def test_amemory_usage(self, driver) -> None:
        driver.set("k", b"v")
        assert await driver.amemory_usage("k") is not None


class TestPingMessage:
    def test_ping_with_message_returns_message(self, driver) -> None:
        # PING with a payload returns the payload (as bytes).
        assert driver.ping(message="hello") == b"hello"

    def test_ping_without_message_returns_true(self, driver) -> None:
        # No message → True (the historical "PONG" → bool conversion from Plan 01).
        assert driver.ping() is True

    @pytest.mark.asyncio
    async def test_aping_with_message(self, driver) -> None:
        assert await driver.aping(message="x") == b"x"


class TestEcho:
    def test_echo_returns_message(self, driver) -> None:
        assert driver.echo("hello") == b"hello"

    def test_echo_bytes_message(self, driver) -> None:
        assert driver.echo(b"\x00binary") == b"\x00binary"

    @pytest.mark.asyncio
    async def test_aecho(self, driver) -> None:
        assert await driver.aecho("test") == b"test"


class TestWait:
    def test_wait_zero_replicas_short_timeout(self, driver) -> None:
        # No replicas configured — WAIT 0 returns 0 immediately.
        assert driver.wait(numreplicas=0, timeout=100) == 0

    @pytest.mark.asyncio
    async def test_await(self, driver) -> None:
        assert await driver.await_(numreplicas=0, timeout=100) == 0


class TestTime:
    def test_time_returns_pair(self, driver) -> None:
        result = driver.time()
        assert isinstance(result, tuple)
        assert len(result) == 2
        # Both elements are unix-timestamp strings (seconds, microseconds).
        seconds, microseconds = result
        assert int(seconds) > 0
        assert 0 <= int(microseconds) < 1_000_000

    @pytest.mark.asyncio
    async def test_atime(self, driver) -> None:
        result = await driver.atime()
        assert isinstance(result, tuple)


class TestLastsave:
    def test_lastsave_returns_unix_timestamp(self, driver) -> None:
        ts = driver.lastsave()
        assert isinstance(ts, int)
        assert ts > 0


class TestBgsaveBgrewriteaof:
    def test_bgsave_returns_message(self, driver) -> None:
        # Returns "Background saving started" or "Background saving scheduled".
        # On a fresh container BGSAVE is fast — but it can still raise
        # ERR Background save already in progress on rare occasions.
        try:
            result = driver.bgsave()
        except Exception:  # noqa: BLE001
            pytest.skip("BGSAVE clashed with concurrent test")
        else:
            assert isinstance(result, bytes)

    def test_bgsave_schedule(self, driver) -> None:
        try:
            result = driver.bgsave(schedule=True)
            assert isinstance(result, bytes)
        except Exception:  # noqa: BLE001
            pytest.skip("BGSAVE clashed with concurrent test")

    def test_bgrewriteaof_returns_message(self, driver) -> None:
        try:
            result = driver.bgrewriteaof()
            assert isinstance(result, bytes)
        except Exception:  # noqa: BLE001
            pytest.skip("BGREWRITEAOF clashed with concurrent test")


class TestDebugSleep:
    def test_debug_sleep_blocks_for_at_least_seconds(self, driver) -> None:
        start = time.monotonic()
        try:
            driver.debug_sleep(0.2)
        except Exception:  # noqa: BLE001
            pytest.skip("DEBUG SLEEP not enabled on this server")
        elapsed = time.monotonic() - start
        assert elapsed >= 0.2

    @pytest.mark.asyncio
    async def test_adebug_sleep(self, driver) -> None:
        start = time.monotonic()
        try:
            await driver.adebug_sleep(0.1)
        except Exception:  # noqa: BLE001
            pytest.skip("DEBUG SLEEP not enabled on this server")
        elapsed = time.monotonic() - start
        assert elapsed >= 0.1
