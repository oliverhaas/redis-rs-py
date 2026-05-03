"""Stream commands — parity with redis-py output shapes."""

from __future__ import annotations

import threading
import time

import pytest
import redis as upstream_redis
from redis_rs_py.exceptions import DataError, ResponseError

# ---------------------------------------------------------------------------
# XADD basic
# ---------------------------------------------------------------------------


def test_xadd_basic_returns_id(driver) -> None:
    new_id = driver.xadd("s", "*", [("field1", b"value1"), ("field2", b"value2")])
    assert new_id is not None
    assert isinstance(new_id, str)
    # Format: <ms-timestamp>-<seq>
    assert "-" in new_id
    ms, seq = new_id.split("-")
    assert ms.isdigit()
    assert seq.isdigit()


def test_xadd_explicit_ms_seq_id(driver) -> None:
    new_id = driver.xadd("s", "1-1", [("f", b"v")])
    assert new_id == "1-1"


def test_xadd_partial_id_assigns_seq(driver) -> None:
    """Format `<ms>-*` lets the server pick the seq."""
    new_id = driver.xadd("s", "100-*", [("f", b"v")])
    assert new_id.startswith("100-")


@pytest.mark.asyncio
async def test_axadd_basic_returns_id(driver) -> None:
    new_id = await driver.axadd("s", "*", [("f", b"v")])
    assert isinstance(new_id, str)
    assert "-" in new_id


def test_xadd_appends_to_existing_stream(driver, redis_py_client) -> None:
    driver.xadd("s", "*", [("a", b"1")])
    driver.xadd("s", "*", [("b", b"2")])
    # Use the upstream client to verify the count.
    assert redis_py_client.xlen("s") == 2


# ---------------------------------------------------------------------------
# XADD option matrix
# ---------------------------------------------------------------------------


def test_xadd_nomkstream_no_stream_returns_none(driver) -> None:
    """NOMKSTREAM: don't create the stream if it doesn't exist."""
    result = driver.xadd("missing", "*", [("f", b"v")], nomkstream=True)
    assert result is None


def test_xadd_nomkstream_existing_stream_returns_id(driver) -> None:
    driver.xadd("s", "*", [("f", b"v")])
    result = driver.xadd("s", "*", [("f", b"v")], nomkstream=True)
    assert isinstance(result, str)


def test_xadd_maxlen_approximate(driver, redis_py_client) -> None:
    for _ in range(10):
        driver.xadd("s", "*", [("f", b"v")], maxlen=5, approximate=True)
    # Approximate trim might leave more than 5 (it's `~5`); tolerate up to 2x.
    n = redis_py_client.xlen("s")
    assert n >= 5
    assert n <= 10


def test_xadd_maxlen_exact(driver, redis_py_client) -> None:
    for _ in range(10):
        driver.xadd("s", "*", [("f", b"v")], maxlen=5, approximate=False)
    assert redis_py_client.xlen("s") == 5


def test_xadd_maxlen_with_limit_is_rejected_when_exact(driver) -> None:
    """LIMIT is only valid with approximate trim (~). With `=`, the server rejects."""
    with pytest.raises(ResponseError):
        driver.xadd("s", "*", [("f", b"v")], maxlen=5, approximate=False, limit=2)


def test_xadd_minid(driver, redis_py_client) -> None:
    driver.xadd("s", "1-0", [("f", b"a")])
    driver.xadd("s", "2-0", [("f", b"b")])
    driver.xadd("s", "3-0", [("f", b"c")])
    # Add another with minid=2-0 — should evict the 1-0 entry.
    driver.xadd("s", "4-0", [("f", b"d")], minid="2-0", approximate=False)
    ids = [e[0] for e in redis_py_client.xrange("s", "-", "+")]
    assert b"1-0" not in ids
    assert b"2-0" in ids
    assert b"3-0" in ids
    assert b"4-0" in ids


@pytest.mark.asyncio
async def test_axadd_full_options(driver, redis_py_client) -> None:
    new_id = await driver.axadd(
        "s",
        "*",
        [("f", b"v")],
        maxlen=100,
        approximate=True,
        limit=10,
    )
    assert isinstance(new_id, str)


# ---------------------------------------------------------------------------
# XLEN / XDEL / XACK
# ---------------------------------------------------------------------------


def test_xlen_empty_stream(driver) -> None:
    assert driver.xlen("missing") == 0


def test_xlen_after_adds(driver) -> None:
    driver.xadd("s", "*", [("f", b"v")])
    driver.xadd("s", "*", [("f", b"v")])
    assert driver.xlen("s") == 2


@pytest.mark.asyncio
async def test_axlen_async(driver) -> None:
    await driver.axadd("s", "*", [("f", b"v")])
    assert await driver.axlen("s") == 1


def test_xdel_single(driver) -> None:
    id1 = driver.xadd("s", "1-0", [("f", b"v")])
    _id2 = driver.xadd("s", "2-0", [("f", b"v")])
    assert driver.xdel("s", id1) == 1
    assert driver.xlen("s") == 1


def test_xdel_variadic(driver) -> None:
    id1 = driver.xadd("s", "1-0", [("f", b"v")])
    id2 = driver.xadd("s", "2-0", [("f", b"v")])
    _id3 = driver.xadd("s", "3-0", [("f", b"v")])
    # "999999999-0" is a valid ID that doesn't exist; server counts it as 0.
    assert driver.xdel("s", id1, id2, "999999999-0") == 2


@pytest.mark.asyncio
async def test_axdel_async(driver) -> None:
    id1 = await driver.axadd("s", "*", [("f", b"v")])
    assert await driver.axdel("s", id1) == 1


def test_xack_basic(driver, redis_py_client) -> None:
    """XACK: ack a delivered entry. Set up a consumer group inline."""
    id1 = driver.xadd("s", "*", [("f", b"v")])
    redis_py_client.xgroup_create("s", "g", id="0")
    # Read with the consumer group so the entry is in pending state.
    redis_py_client.xreadgroup("g", "c", {"s": ">"})
    assert driver.xack("s", "g", id1) == 1
    # ACKing again returns 0.
    assert driver.xack("s", "g", id1) == 0


def test_xack_variadic(driver, redis_py_client) -> None:
    id1 = driver.xadd("s", "*", [("f", b"v")])
    id2 = driver.xadd("s", "*", [("f", b"v")])
    redis_py_client.xgroup_create("s", "g", id="0")
    redis_py_client.xreadgroup("g", "c", {"s": ">"})
    assert driver.xack("s", "g", id1, id2) == 2


@pytest.mark.asyncio
async def test_axack_async(driver, redis_py_client) -> None:
    id1 = await driver.axadd("s", "*", [("f", b"v")])
    redis_py_client.xgroup_create("s", "g", id="0")
    redis_py_client.xreadgroup("g", "c", {"s": ">"})
    assert await driver.axack("s", "g", id1) == 1


# ---------------------------------------------------------------------------
# XRANGE / XREVRANGE
# ---------------------------------------------------------------------------


class TestXrange:
    def test_xrange_returns_list_of_id_dict_tuples(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f1", b"v1"), ("f2", b"v2")])
        driver.xadd("s", "2-0", [("f3", b"v3")])
        result = driver.xrange("s", "-", "+")
        assert result == [
            (b"1-0", {b"f1": b"v1", b"f2": b"v2"}),
            (b"2-0", {b"f3": b"v3"}),
        ]
        # Same call against the upstream client must return the same shape.
        assert result == redis_py_client.xrange("s", "-", "+")

    def test_xrange_with_count(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xadd("s", "2-0", [("f", b"v")])
        driver.xadd("s", "3-0", [("f", b"v")])
        result = driver.xrange("s", "-", "+", count=2)
        assert len(result) == 2
        assert result[0][0] == b"1-0"
        assert result[1][0] == b"2-0"

    def test_xrange_with_explicit_min_max(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xadd("s", "5-0", [("f", b"v")])
        driver.xadd("s", "10-0", [("f", b"v")])
        result = driver.xrange("s", "2-0", "8-0")
        assert [e[0] for e in result] == [b"5-0"]

    def test_xrange_empty_stream(self, driver) -> None:
        assert driver.xrange("missing", "-", "+") == []

    @pytest.mark.asyncio
    async def test_axrange(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("a", b"x")])
        result = await driver.axrange("s", "-", "+")
        assert result == [(b"1-0", {b"a": b"x"})]
        assert result == redis_py_client.xrange("s", "-", "+")


class TestXrevrange:
    def test_xrevrange_reverses_order(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v1")])
        driver.xadd("s", "2-0", [("f", b"v2")])
        driver.xadd("s", "3-0", [("f", b"v3")])
        result = driver.xrevrange("s", "+", "-")
        assert [e[0] for e in result] == [b"3-0", b"2-0", b"1-0"]
        assert result == redis_py_client.xrevrange("s", "+", "-")

    def test_xrevrange_with_count(self, driver) -> None:
        for ms in (1, 2, 3, 4, 5):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        result = driver.xrevrange("s", "+", "-", count=2)
        assert [e[0] for e in result] == [b"5-0", b"4-0"]

    @pytest.mark.asyncio
    async def test_axrevrange(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xadd("s", "2-0", [("f", b"v")])
        result = await driver.axrevrange("s", "+", "-")
        assert [e[0] for e in result] == [b"2-0", b"1-0"]


# ---------------------------------------------------------------------------
# XREAD
# ---------------------------------------------------------------------------


class TestXread:
    def test_xread_single_stream(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v1")])
        driver.xadd("s", "2-0", [("f", b"v2")])
        result = driver.xread({"s": "0"})
        # redis-py shape: {b"s": [(b"1-0", {b"f": b"v1"}), (b"2-0", {b"f": b"v2"})]}
        assert result == {b"s": [(b"1-0", {b"f": b"v1"}), (b"2-0", {b"f": b"v2"})]}
        # Validate our output shape manually - upstream redis-py may return
        # list-of-lists (RESP2) or dict (RESP3); we always return dict.
        assert b"s" in result
        assert result[b"s"] == [(b"1-0", {b"f": b"v1"}), (b"2-0", {b"f": b"v2"})]

    def test_xread_multiple_streams(self, driver, redis_py_client) -> None:
        driver.xadd("s1", "1-0", [("f", b"a")])
        driver.xadd("s2", "1-0", [("f", b"b")])
        result = driver.xread({"s1": "0", "s2": "0"})
        assert result == {
            b"s1": [(b"1-0", {b"f": b"a"})],
            b"s2": [(b"1-0", {b"f": b"b"})],
        }

    def test_xread_only_new(self, driver) -> None:
        """`$` means: only entries with id > current last."""
        driver.xadd("s", "*", [("f", b"v")])
        # Read with $ — no entries newer than the last one; result is None.
        result = driver.xread({"s": "$"})
        assert result is None

    def test_xread_with_count(self, driver) -> None:
        for ms in (1, 2, 3, 4, 5):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        result = driver.xread({"s": "0"}, count=2)
        assert len(result[b"s"]) == 2
        assert result[b"s"][0][0] == b"1-0"
        assert result[b"s"][1][0] == b"2-0"

    def test_xread_block_timeout_returns_none(self, driver) -> None:
        """Read with $ and a short block — no new entries → None."""
        driver.xadd("s", "*", [("f", b"v")])
        result = driver.xread({"s": "$"}, block=50)  # 50ms
        assert result is None

    def test_xread_block_with_concurrent_xadd(self, driver, valkey_url) -> None:
        """Block then have a second client XADD — should unblock and return."""
        driver.xadd("s", "*", [("f", b"v0")])  # baseline
        out: dict = {}

        def reader() -> None:
            out["result"] = driver.xread({"s": "$"}, block=2000)

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.1)  # ensure the reader is blocked
        # Use a fresh upstream client to add — `driver` is busy in the reader thread.
        rp = upstream_redis.Redis.from_url(valkey_url)
        rp.xadd("s", {"f": b"v1"})
        rp.close()

        t.join(timeout=5)
        assert out["result"] is not None
        assert b"s" in out["result"]
        assert out["result"][b"s"][0][1] == {b"f": b"v1"}

    @pytest.mark.asyncio
    async def test_axread_basic(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        result = await driver.axread({"s": "0"})
        assert result == {b"s": [(b"1-0", {b"f": b"v"})]}

    @pytest.mark.asyncio
    async def test_axread_block_timeout_returns_none(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        result = await driver.axread({"s": "$"}, block=50)
        assert result is None


# ---------------------------------------------------------------------------
# XREADGROUP
# ---------------------------------------------------------------------------


class TestXreadgroup:
    def _setup_group(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v1")])
        driver.xadd("s", "2-0", [("f", b"v2")])
        redis_py_client.xgroup_create("s", "g", id="0")

    def test_xreadgroup_pending_marker(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        # `>` — give me only undelivered entries (deliver them to consumer).
        result = driver.xreadgroup("g", "c1", {"s": ">"})
        assert result == {
            b"s": [
                (b"1-0", {b"f": b"v1"}),
                (b"2-0", {b"f": b"v2"}),
            ],
        }

    def test_xreadgroup_history(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        # First delivery — one entry to c1.
        driver.xreadgroup("g", "c1", {"s": ">"}, count=1)
        # Now read the history (id="0") for c1 — already-delivered ones.
        result = driver.xreadgroup("g", "c1", {"s": "0"})
        assert b"s" in result
        assert result[b"s"][0][0] == b"1-0"

    def test_xreadgroup_with_count(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        result = driver.xreadgroup("g", "c1", {"s": ">"}, count=1)
        assert len(result[b"s"]) == 1

    def test_xreadgroup_noack(self, driver, redis_py_client) -> None:
        """NOACK: the entries are delivered but never enter the pending list."""
        self._setup_group(driver, redis_py_client)
        result = driver.xreadgroup("g", "c1", {"s": ">"}, noack=True)
        assert result is not None
        # Pending should be empty after NOACK delivery.
        pending = redis_py_client.xpending("s", "g")
        assert pending["pending"] == 0

    def test_xreadgroup_block_timeout(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        # Drain everything first.
        driver.xreadgroup("g", "c1", {"s": ">"})
        # Now read again with `>` and a short block — nothing new → None.
        result = driver.xreadgroup("g", "c1", {"s": ">"}, block=50)
        assert result is None

    @pytest.mark.asyncio
    async def test_axreadgroup(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        result = await driver.axreadgroup("g", "c1", {"s": ">"})
        assert b"s" in result
        assert len(result[b"s"]) == 2


# ---------------------------------------------------------------------------
# XGROUP
# ---------------------------------------------------------------------------


class TestXgroup:
    def test_xgroup_create_basic(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        # Returns None (sync) on success — server returns "OK".
        driver.xgroup_create("s", "g", id="0")
        # Creating the same group again raises BUSYGROUP.
        with pytest.raises(ResponseError, match="BUSYGROUP"):
            driver.xgroup_create("s", "g", id="0")

    def test_xgroup_create_mkstream(self, driver) -> None:
        # mkstream=True creates the stream if missing.
        driver.xgroup_create("missing", "g", id="0", mkstream=True)
        # Stream now exists.
        assert driver.xlen("missing") == 0

    def test_xgroup_create_entries_read(self, driver) -> None:
        """Redis 7+ — ENTRIESREAD argument."""
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0", entries_read=5)

    def test_xgroup_setid(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xadd("s", "2-0", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        driver.xgroup_setid("s", "g", id="2-0")
        # Now `>` returns only id > 2-0.
        result = redis_py_client.xreadgroup("g", "c", {"s": ">"})
        assert result == []  # nothing new

    def test_xgroup_destroy(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        assert driver.xgroup_destroy("s", "g") == 1
        assert driver.xgroup_destroy("s", "g") == 0  # already gone

    def test_xgroup_createconsumer(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        # Returns 1 if created, 0 if already existed.
        assert driver.xgroup_createconsumer("s", "g", "c1") == 1
        assert driver.xgroup_createconsumer("s", "g", "c1") == 0

    def test_xgroup_delconsumer(self, driver, redis_py_client) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        # Read with consumer to register them with at least one pending entry.
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        # Delete consumer; returns the number of pending entries owned by them.
        assert driver.xgroup_delconsumer("s", "g", "c1") == 1

    @pytest.mark.asyncio
    async def test_axgroup_create_destroy(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        await driver.axgroup_create("s", "g", id="0")
        assert await driver.axgroup_destroy("s", "g") == 1


# ---------------------------------------------------------------------------
# XINFO
# ---------------------------------------------------------------------------


class TestXinfo:
    def test_xinfo_stream_basic_fields(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v1")])
        driver.xadd("s", "2-0", [("f", b"v2")])
        info = driver.xinfo_stream("s")
        # redis-py returns dict with bytes keys.
        assert isinstance(info, dict)
        assert info[b"length"] == 2
        # Check the load-bearing keys are present in our dict.
        for k in (b"length", b"first-entry", b"last-entry", b"groups"):
            assert k in info
        # Upstream redis-py may use string or bytes keys depending on version.
        upstream = redis_py_client.xinfo_stream("s")
        # Normalize upstream to check length value matches.
        upstream_length = upstream.get(b"length") or upstream.get("length")
        assert info[b"length"] == upstream_length

    def test_xinfo_stream_full(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        info = driver.xinfo_stream("s", full=True)
        assert isinstance(info, dict)
        assert b"length" in info

    def test_xinfo_groups_empty(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        result = driver.xinfo_groups("s")
        assert result == []

    def test_xinfo_groups_after_create(self, driver, redis_py_client) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        result = driver.xinfo_groups("s")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0][b"name"] == b"g"
        assert result[0][b"consumers"] == 0

    def test_xinfo_consumers(self, driver, redis_py_client) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        result = driver.xinfo_consumers("s", "g")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0][b"name"] == b"c1"
        assert result[0][b"pending"] == 1

    @pytest.mark.asyncio
    async def test_axinfo_stream(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        info = await driver.axinfo_stream("s")
        assert info[b"length"] == 1


# ---------------------------------------------------------------------------
# XTRIM
# ---------------------------------------------------------------------------


class TestXtrim:
    def test_xtrim_maxlen_exact(self, driver) -> None:
        for ms in range(1, 11):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = driver.xtrim("s", maxlen=5, approximate=False)
        assert removed == 5
        assert driver.xlen("s") == 5

    def test_xtrim_maxlen_approximate(self, driver) -> None:
        for ms in range(1, 21):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = driver.xtrim("s", maxlen=5, approximate=True)
        assert removed >= 0  # approximate trim might remove 0-15

    def test_xtrim_minid(self, driver) -> None:
        for ms in (1, 2, 3, 4, 5):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = driver.xtrim("s", minid="3-0", approximate=False)
        assert removed == 2

    def test_xtrim_with_limit(self, driver) -> None:
        for ms in range(1, 21):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = driver.xtrim("s", maxlen=5, approximate=True, limit=3)
        assert removed >= 0
        assert removed <= 15

    def test_xtrim_requires_maxlen_or_minid(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        with pytest.raises(DataError):
            driver.xtrim("s")

    @pytest.mark.asyncio
    async def test_axtrim(self, driver) -> None:
        for ms in range(1, 6):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = await driver.axtrim("s", maxlen=2, approximate=False)
        assert removed == 3


# ---------------------------------------------------------------------------
# XPENDING
# ---------------------------------------------------------------------------


class TestXpending:
    def _setup(self, driver, redis_py_client) -> tuple[str, str]:
        id1 = driver.xadd("s", "*", [("f", b"v1")])
        id2 = driver.xadd("s", "*", [("f", b"v2")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        return id1, id2

    def test_xpending_summary_no_pending(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="$")
        result = driver.xpending("s", "g")
        # No pending entries → 4-tuple with count=0, min/max None, empty list.
        assert result == (0, None, None, [])

    def test_xpending_summary_with_pending(self, driver, redis_py_client) -> None:
        id1, id2 = self._setup(driver, redis_py_client)
        result = driver.xpending("s", "g")
        count, min_id, max_id, consumers = result
        assert count == 2
        assert min_id == id1.encode()
        assert max_id == id2.encode()
        assert consumers == [(b"c1", 2)]

    def test_xpending_range_basic(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        result = driver.xpending("s", "g", min="-", max="+", count=10)
        # range form returns a list of dicts.
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0][b"consumer"] == b"c1"
        assert result[0][b"times_delivered"] == 1

    def test_xpending_range_with_consumer_filter(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        # Add a third entry and read it with a different consumer.
        _id3 = driver.xadd("s", "*", [("f", b"v3")])
        redis_py_client.xreadgroup("g", "c2", {"s": ">"})
        result = driver.xpending("s", "g", min="-", max="+", count=10, consumer="c1")
        assert len(result) == 2
        for row in result:
            assert row[b"consumer"] == b"c1"

    def test_xpending_range_with_idle(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        # idle=0 returns all entries idle at least 0ms (i.e. all).
        result = driver.xpending("s", "g", idle=0, min="-", max="+", count=10)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_axpending_summary(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        result = await driver.axpending("s", "g")
        count, _min, __max, _consumers = result
        assert count == 2

    @pytest.mark.asyncio
    async def test_axpending_range(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        result = await driver.axpending("s", "g", min="-", max="+", count=10)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# XCLAIM
# ---------------------------------------------------------------------------


class TestXclaim:
    def _make_pending(self, driver, redis_py_client) -> tuple[str, str]:
        id1 = driver.xadd("s", "*", [("f", b"v1")])
        id2 = driver.xadd("s", "*", [("f", b"v2")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        return id1, id2

    def test_xclaim_basic_returns_entries(self, driver, redis_py_client) -> None:
        id1, _id2 = self._make_pending(driver, redis_py_client)
        # Claim id1 from c1 to c2 (with min_idle=0 so nothing blocks).
        result = driver.xclaim("s", "g", "c2", min_idle_time=0, message_ids=[id1])
        # Same shape as XRANGE.
        assert result == [(id1.encode(), {b"f": b"v1"})]

    def test_xclaim_justid(self, driver, redis_py_client) -> None:
        id1, id2 = self._make_pending(driver, redis_py_client)
        result = driver.xclaim(
            "s",
            "g",
            "c2",
            min_idle_time=0,
            message_ids=[id1, id2],
            justid=True,
        )
        assert sorted(result) == sorted([id1.encode(), id2.encode()])

    def test_xclaim_force_creates_pending_if_missing(self, driver, redis_py_client) -> None:
        """FORCE: claim an id that's not in the PEL — server creates an entry."""
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xgroup_create("s", "g", id="$")  # no pending
        # 1-0 is not pending under group g. Without FORCE, claim returns [].
        no_force = driver.xclaim("s", "g", "c1", min_idle_time=0, message_ids=["1-0"])
        assert no_force == []
        with_force = driver.xclaim(
            "s",
            "g",
            "c1",
            min_idle_time=0,
            message_ids=["1-0"],
            force=True,
        )
        assert len(with_force) == 1
        assert with_force[0][0] == b"1-0"

    def test_xclaim_with_idle_time_setting(self, driver, redis_py_client) -> None:
        id1, _id2 = self._make_pending(driver, redis_py_client)
        # Set idle=100000 on the claimed entry.
        result = driver.xclaim("s", "g", "c2", min_idle_time=0, message_ids=[id1], idle=100000)
        assert len(result) == 1
        # Verify via XPENDING range that idle is at least 100s.
        pending = driver.xpending("s", "g", min="-", max="+", count=10)
        for row in pending:
            if row[b"message_id"] == id1.encode():
                assert row[b"time_since_delivered"] >= 100000

    def test_xclaim_min_idle_time_filters(self, driver, redis_py_client) -> None:
        """min_idle_time: claim only if the pending entry has been idle for at least that long."""
        id1, _ = self._make_pending(driver, redis_py_client)
        # The entry was just delivered — idle is ~0. Claiming with min_idle=10000 → empty.
        result = driver.xclaim(
            "s",
            "g",
            "c2",
            min_idle_time=10000,
            message_ids=[id1],
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_axclaim(self, driver, redis_py_client) -> None:
        id1, _id2 = self._make_pending(driver, redis_py_client)
        result = await driver.axclaim(
            "s",
            "g",
            "c2",
            min_idle_time=0,
            message_ids=[id1],
        )
        assert len(result) == 1
        assert result[0][0] == id1.encode()


# ---------------------------------------------------------------------------
# XAUTOCLAIM
# ---------------------------------------------------------------------------


class TestXautoclaim:
    def _make_pending(self, driver, redis_py_client) -> tuple[str, str]:
        id1 = driver.xadd("s", "*", [("f", b"v1")])
        id2 = driver.xadd("s", "*", [("f", b"v2")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        return id1, id2

    def test_xautoclaim_basic(self, driver, redis_py_client) -> None:
        id1, id2 = self._make_pending(driver, redis_py_client)
        next_id, entries, deleted = driver.xautoclaim(
            "s",
            "g",
            "c2",
            min_idle_time=0,
            start_id="0-0",
        )
        assert next_id == b"0-0"  # done — no more to claim
        assert len(entries) == 2
        assert entries[0][0] in {id1.encode(), id2.encode()}
        assert deleted == []

    def test_xautoclaim_with_count(self, driver, redis_py_client) -> None:
        for _ in range(5):
            driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        next_id, entries, _deleted = driver.xautoclaim(
            "s",
            "g",
            "c2",
            min_idle_time=0,
            start_id="0-0",
            count=2,
        )
        assert len(entries) == 2
        # next_id is non-zero — there's more to claim.
        assert next_id != b"0-0"

    def test_xautoclaim_justid(self, driver, redis_py_client) -> None:
        id1, id2 = self._make_pending(driver, redis_py_client)
        next_id, ids, _deleted = driver.xautoclaim(
            "s",
            "g",
            "c2",
            min_idle_time=0,
            start_id="0-0",
            justid=True,
        )
        assert next_id == b"0-0"
        assert sorted(ids) == sorted([id1.encode(), id2.encode()])

    def test_xautoclaim_min_idle_time_filters(self, driver, redis_py_client) -> None:
        self._make_pending(driver, redis_py_client)
        # The entries were just delivered — idle ~0ms. Auto-claim with min_idle=10000 → empty.
        next_id, entries, deleted = driver.xautoclaim(
            "s",
            "g",
            "c2",
            min_idle_time=10000,
            start_id="0-0",
        )
        assert next_id == b"0-0"
        assert entries == []
        assert deleted == []

    @pytest.mark.asyncio
    async def test_axautoclaim(self, driver, redis_py_client) -> None:
        self._make_pending(driver, redis_py_client)
        next_id, entries, _deleted = await driver.axautoclaim(
            "s",
            "g",
            "c2",
            min_idle_time=0,
            start_id="0-0",
        )
        assert next_id == b"0-0"
        assert len(entries) == 2


# ---------------------------------------------------------------------------
# XSETID
# ---------------------------------------------------------------------------


class TestXsetid:
    def test_xsetid_basic(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        # Set the last-id to 100-0 — subsequent XADD with `*` will use ms >= 100.
        driver.xsetid("s", "100-0")
        new_id = driver.xadd("s", "*", [("f", b"v")])
        ms, _ = new_id.split("-")
        assert int(ms) >= 100

    def test_xsetid_entries_added(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xsetid("s", "100-0", entries_added=42)
        info = driver.xinfo_stream("s")
        assert info[b"entries-added"] == 42

    def test_xsetid_max_deleted(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xsetid("s", "100-0", max_deleted_entry_id="50-0")
        info = driver.xinfo_stream("s")
        assert info[b"max-deleted-entry-id"] == b"50-0"

    @pytest.mark.asyncio
    async def test_axsetid(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        await driver.axsetid("s", "200-0")
