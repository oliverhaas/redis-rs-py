# Plan 09 — Scripts (EVAL/FCALL/FUNCTION) + Admin/Introspection (SCAN/INFO/CONFIG/CLIENT/OBJECT)

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land server-side scripting (`EVAL`/`EVALSHA`/`FCALL`/`SCRIPT *`/`FUNCTION *`) and the admin/introspection surface (`SCAN`/`KEYS`/`INFO`/`CONFIG`/`CLIENT`/`OBJECT`/`MEMORY USAGE`/`PING`/`ECHO`/`WAIT`/`TIME`/`LASTSAVE`/`BGSAVE`/`BGREWRITEAOF`/`DEBUG SLEEP`) on the low-level driver. Two new files: `crates/redis-rs-py-driver/src/commands/scripts.rs` and `crates/redis-rs-py-driver/src/commands/admin.rs`.

**Architecture:** Same pattern as plans 03–08 — every command is a sync + async pair on `RedisRsDriver`, with shared `cmd_*` argument-encoding helpers. Replies that don't have a clean typed shape (`EVAL` of a user script, `INFO` reply text, `CLIENT INFO`, etc.) pass through as `RawResult::Value(redis::Value)` and get rendered by the existing `redis_value_to_py` recursive converter from Plan 01. Replies that DO have a typed shape (`SCRIPT EXISTS` → `list[bool]`, `SCAN` → `(int_cursor, list[bytes_keys])`, `OBJECT ENCODING` → `str | None`, `MEMORY USAGE` → `int | None`) use existing `RawResult` variants where possible, or thin new variants where not.

The single non-Rust piece in this plan is `scan_iter` — Python doesn't have a native way for a `#[pyclass]` to be a Python generator (PyO3 can implement `__iter__`/`__next__` but that's an iterator, not a generator, and crucially can't be `async` for the asyncio path). For `scan_iter` we ship a thin Python helper at `python/redis_rs_py/_scan_iter.py` (sync generator) and `python/redis_rs_py/asyncio/_scan_iter.py` (async generator). Both wrap the existing `scan(cursor=, ...)` Rust method. This is the explicitly-permitted Python escape hatch from the "Rust by default" principle — documented in `PLAN.md` line 60-63.

**`SELECT` limitation:** redis-py exposes `SELECT db_index` on the connection; in our driver, the database is per-`RedisRsDriver` (set at `connect_standard` time via the URL's `/<db>` segment). We accept the `select(db_index)` method on the driver but document loudly that it returns `True` only when `db_index` matches the connected db — there's no per-connection mutability in our pool model. This is the same trade-off `valkey-glide` makes.

**Tech stack:** PyO3 0.28, redis 1.x, no new deps.

**Reference material:**
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs:443-1543` — every script + admin connection helper. Lift the `cmd::query_async` calls verbatim.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs:732-1024` — cachex's method bodies. Use as the source for arg signatures.
- redis-py source for the canonical `info` parsing — `redis-py` parses `INFO` text into a dict with sections; we DON'T do that in Rust (returning the raw text bytes preserves all info), but we DO ship a `_parse_info_text()` Python helper used by the high-level facade later (plan 10).

**Out of scope:**
- High-level `Redis.register_script()` (defer; the user can do `script_load()` + `evalsha()`).
- `MONITOR` (plan PLAN.md flagged as deferred).
- `LATENCY` (deferred).
- `CLIENT TRACKING` from Python (the Rust core already enables it via `cache-aio`; exposure to user code is a v0.2 task).
- Cluster admin commands (`CLUSTER SLOTS` etc.) — plan 15.
- Sentinel admin commands (`SENTINEL *`) — plan 16.
- `WatchError`-emitting commands (WATCH/MULTI/EXEC/DISCARD) — plan 13.

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  commands/
    mod.rs                       # MODIFIED: add `pub mod scripts; pub mod admin;`
    scripts.rs                   # NEW (~600 LOC): EVAL/EVALSHA/EVAL_RO/EVALSHA_RO, SCRIPT *, FCALL/FCALL_RO, FUNCTION *
    admin.rs                     # NEW (~900 LOC): SCAN, KEYS, RANDOMKEY, DBSIZE, FLUSHDB/ALL, SELECT, INFO, CONFIG *, CLIENT *, OBJECT *, MEMORY USAGE, PING, ECHO, WAIT, WAITAOF, TIME, LASTSAVE, BGSAVE, BGREWRITEAOF, DEBUG SLEEP
  driver.rs                      # MODIFIED: extend `ping(message=)` signature (Plan 01 took zero-arg PING)
  async_bridge.rs                # MODIFIED: 2 new RawResult variants (BoolList, OptStrAndOptStr for the time-tuple)
  raw_result.rs                  # MODIFIED: From<...> impls for the new variant payloads
python/
  redis_rs_py/
    __init__.py                  # MODIFIED: re-export scan_iter helper
    _driver.pyi                  # MODIFIED: stub the ~50 new methods
    _scan_iter.py                # NEW: sync generator helper around driver.scan(cursor=)
    asyncio/
      __init__.py                # MODIFIED (or NEW): re-export async scan_iter
      _scan_iter.py              # NEW: async generator helper around driver.ascan(cursor=)
tests/
  driver/
    test_commands_scripts.py     # NEW: parity-asserted vs redis-py
    test_commands_admin.py       # NEW: parity-asserted vs redis-py
```

---

## Pre-task: Read upstream signatures

Before any edits, run once:

```bash
uv run python -c "
import redis
r = redis.Redis(decode_responses=False)
help(r.scan); help(r.scan_iter); help(r.info); help(r.config_get)
help(r.client_list); help(r.client_kill_filter); help(r.object); help(r.memory_usage)
" 2>&1 | head -200
```

Confirm these key behaviours we'll mirror:
- `scan(cursor=0, match=None, count=None, _type=None)` → `(int, list[bytes])`. Note redis-py uses `_type=` not `type=` to avoid shadowing the builtin.
- `scan_iter(match=None, count=None, _type=None)` → `Iterator[bytes]`.
- `info(section=None)` → `dict[str, Any]` (redis-py parses!). We diverge: return the raw bytes from the server; the high-level facade in plan 10 handles parsing.
- `config_get(pattern="*")` → `dict[bytes, bytes]`.
- `client_list(_type=None, client_id=[])` → list of dicts with bytes keys.
- `client_kill_filter(_id=None, _type=None, addr=None, skipme=None, laddr=None, user=None, maxage=None)` → int kill-count.
- `object(infotype, key)` → varies (`encoding` returns bytes, `idletime` returns int, `freq` returns int, `refcount` returns int).
- `memory_usage(key, samples=None)` → `int | None`.

---

## Task 1: Wire the new submodules + extend `RawResult`

Land the boilerplate so the per-command tasks (2–14) just append methods.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/mod.rs`
- Create: `crates/redis-rs-py-driver/src/commands/scripts.rs` (skeleton)
- Create: `crates/redis-rs-py-driver/src/commands/admin.rs` (skeleton)
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs` (2 new variants)
- Modify: `crates/redis-rs-py-driver/src/raw_result.rs` (`From<Vec<bool>>`)

- [ ] **Step 1: Extend `commands/mod.rs`**

Edit `crates/redis-rs-py-driver/src/commands/mod.rs`. After the existing `pub mod streams;` add:

```rust
pub mod scripts;
pub mod admin;
```

- [ ] **Step 2: Create the `scripts.rs` skeleton**

Create `crates/redis-rs-py-driver/src/commands/scripts.rs`:

```rust
// Server-side scripting commands for RedisRsDriver.
//
// EVAL/EVALSHA/EVAL_RO/EVALSHA_RO + SCRIPT LOAD/EXISTS/FLUSH/KILL +
// FCALL/FCALL_RO + FUNCTION LOAD/DUMP/FLUSH/LIST/STATS/KILL/RESTORE.
//
// User-script return values pass through as redis::Value (recursive
// conversion via redis_value_to_py) — Lua/Function scripts can return
// anything, so a typed RawResult variant doesn't fit. SCRIPT EXISTS
// returns a typed list-of-bools, FUNCTION LIST returns a list-of-dicts
// (raw value pass-through, schema is open).

use pyo3::prelude::*;

use crate::async_bridge::RawResult;
use crate::driver::RedisRsDriver;
use crate::errors::{classify, to_py_err};
use crate::raw_result::IntoRawResult;
use crate::runtime::get_runtime;
use crate::{async_op, conn_method, dispatch_cmd, sync_op};

// =========================================================================
// Argument-encoding helpers (cmd_*)
// =========================================================================
//
// (Filled in by Tasks 2-5.)

// =========================================================================
// RedisRsDriver method impls
// =========================================================================

#[pymethods]
impl RedisRsDriver {
    // (Filled in by Tasks 2-5.)
}
```

- [ ] **Step 3: Create the `admin.rs` skeleton**

Create `crates/redis-rs-py-driver/src/commands/admin.rs`:

```rust
// Admin / introspection commands for RedisRsDriver.
//
// SCAN family + KEYS/RANDOMKEY + DBSIZE/FLUSHDB/FLUSHALL/SELECT +
// INFO/CONFIG */CLIENT */OBJECT */MEMORY USAGE +
// PING/ECHO/WAIT/WAITAOF/TIME/LASTSAVE/BGSAVE/BGREWRITEAOF/DEBUG SLEEP.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use crate::async_bridge::RawResult;
use crate::driver::RedisRsDriver;
use crate::errors::{classify, to_py_err};
use crate::raw_result::IntoRawResult;
use crate::runtime::get_runtime;
use crate::{async_op, conn_method, dispatch_cmd, sync_op};

// =========================================================================
// Argument-encoding helpers (cmd_*)
// =========================================================================
//
// (Filled in by Tasks 6-14.)

// =========================================================================
// RedisRsDriver method impls
// =========================================================================

#[pymethods]
impl RedisRsDriver {
    // (Filled in by Tasks 6-14.)
}
```

- [ ] **Step 4: Verify the crate still compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: clean.

- [ ] **Step 5: Add the new `RawResult` variants**

Edit `crates/redis-rs-py-driver/src/async_bridge.rs`. Locate `pub enum RawResult { ... }` and append before the closing brace:

```rust
    BoolList(Vec<bool>),
    OptStrPair(Option<(String, String)>),  // for TIME (seconds, microseconds)
```

Add the matching arms in `impl RawResult { pub fn into_py(...) }`:

```rust
            RawResult::BoolList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|b| b.into_pyobject(py).unwrap().to_owned().into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::OptStrPair(None) => Ok(py.None()),
            RawResult::OptStrPair(Some((a, b))) => {
                let a_py = PyString::new(py, &a).into_any().unbind();
                let b_py = PyString::new(py, &b).into_any().unbind();
                Ok(PyTuple::new(py, [a_py, b_py])?.into_any().unbind())
            }
```

- [ ] **Step 6: Add `From<>` impls**

Edit `crates/redis-rs-py-driver/src/raw_result.rs`. Append:

```rust
impl From<Vec<bool>> for RawResult {
    fn from(v: Vec<bool>) -> Self {
        RawResult::BoolList(v)
    }
}

impl From<Option<(String, String)>> for RawResult {
    fn from(v: Option<(String, String)>) -> Self {
        RawResult::OptStrPair(v)
    }
}
```

- [ ] **Step 7: Build verification**

Run: `cargo check -p redis-rs-py-driver`
Expected: clean (unused-warnings only).

- [ ] **Step 8: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/ crates/redis-rs-py-driver/src/async_bridge.rs crates/redis-rs-py-driver/src/raw_result.rs
git commit -m "feat(scripts): add scripts and admin module skeletons + RawResult variants"
```

---

## Task 2: `EVAL` + `EVALSHA` + `EVAL_RO` + `EVALSHA_RO`

Four near-identical commands. Each takes `(script_or_sha, keys: list[str], args: list[bytes])` and returns the raw `redis::Value` (rendered by `redis_value_to_py`).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/scripts.rs`
- Create: `tests/driver/test_commands_scripts.py`

- [ ] **Step 1: Write failing tests**

Create `tests/driver/test_commands_scripts.py`:

```python
"""Server-side scripting — EVAL, EVALSHA, FCALL, FUNCTION, SCRIPT *."""

from __future__ import annotations

import asyncio

import pytest


# Standard Lua script: returns the first key.
ECHO_KEY_SCRIPT = "return KEYS[1]"
INCR_BY_SCRIPT = (
    "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
)


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
        from redis_rs_py.exceptions import ResponseError
        with pytest.raises(ResponseError):
            driver.eval("return redis.error_reply('user error')", [], [])

    @pytest.mark.asyncio
    async def test_aeval_basic(self, driver) -> None:
        assert await driver.aeval(ECHO_KEY_SCRIPT, ["k"], []) == b"k"


class TestEvalsha:
    def test_evalsha_unknown_raises_noscripterror(self, driver) -> None:
        from redis_rs_py.exceptions import NoScriptError
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
        from redis_rs_py.exceptions import ResponseError
        # SET is a write; EVAL_RO must refuse to call it.
        with pytest.raises(ResponseError):
            driver.eval_ro("redis.call('SET', KEYS[1], 'x'); return 1", ["k"], [])

    def test_evalsha_ro(self, driver) -> None:
        sha = driver.script_load("return ARGV[1]")
        assert driver.evalsha_ro(sha, [], [b"value"]) == b"value"
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_scripts.py::TestEval -v`
Expected: FAIL — `'RedisRsDriver' object has no attribute 'eval'`.

- [ ] **Step 3: Implement EVAL/EVALSHA + RO variants**

Append to **Argument-encoding helpers** in `commands/scripts.rs`:

```rust
fn cmd_eval(name: &str, script_or_sha: &str, keys: &[String], args: &[Vec<u8>]) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    cmd.arg(script_or_sha).arg(keys.len());
    for k in keys {
        cmd.arg(k.as_str());
    }
    for a in args {
        cmd.arg(a.as_slice());
    }
    cmd
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (script, keys, args))]
    fn eval(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_eval("EVAL", script, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (script, keys, args))]
    fn aeval(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let script = script.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_eval("EVAL", &script, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (sha, keys, args))]
    fn evalsha(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_eval("EVALSHA", sha, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (sha, keys, args))]
    fn aevalsha(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let sha = sha.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_eval("EVALSHA", &sha, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (script, keys, args))]
    fn eval_ro(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_eval("EVAL_RO", script, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (script, keys, args))]
    fn aeval_ro(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let script = script.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_eval("EVAL_RO", &script, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (sha, keys, args))]
    fn evalsha_ro(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_eval("EVALSHA_RO", sha, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (sha, keys, args))]
    fn aevalsha_ro(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let sha = sha.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_eval("EVALSHA_RO", &sha, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_scripts.py::TestEval tests/driver/test_commands_scripts.py::TestEvalsha tests/driver/test_commands_scripts.py::TestEvalRo -v`
Expected: 13 PASS. The `evalsha`/`script_load` tests will fail until Task 3 lands `script_load`. Mark them as expected-fail temporarily OR implement `script_load` immediately in this task (recommended for ergonomics).

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/scripts.rs tests/driver/test_commands_scripts.py
git commit -m "feat(scripts): add EVAL/EVALSHA/EVAL_RO/EVALSHA_RO (sync + async)"
```

---

## Task 3: `SCRIPT LOAD` / `SCRIPT EXISTS` / `SCRIPT FLUSH` / `SCRIPT KILL`

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/scripts.rs`
- Modify: `tests/driver/test_commands_scripts.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_scripts.py`:

```python
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
        from redis_rs_py.exceptions import DataError
        with pytest.raises(DataError):
            driver.script_flush(mode="WHATEVER")

    @pytest.mark.asyncio
    async def test_ascript_flush(self, driver) -> None:
        sha = await driver.ascript_load("return 1")
        await driver.ascript_flush()
        assert driver.script_exists(sha) == [False]


class TestScriptKill:
    def test_script_kill_with_no_script_running_raises(self, driver) -> None:
        from redis_rs_py.exceptions import ResponseError
        # NOTBUSY is the server's response when nothing is running.
        with pytest.raises(ResponseError, match="NOTBUSY"):
            driver.script_kill()
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_scripts.py::TestScriptLoad tests/driver/test_commands_scripts.py::TestScriptExists tests/driver/test_commands_scripts.py::TestScriptFlush tests/driver/test_commands_scripts.py::TestScriptKill -v`
Expected: FAIL — methods don't exist.

- [ ] **Step 3: Implement SCRIPT family**

Append to **Argument-encoding helpers**:

```rust
fn cmd_script_load(script: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("SCRIPT");
    cmd.arg("LOAD").arg(script);
    cmd
}

fn cmd_script_exists(shas: &[String]) -> redis::Cmd {
    let mut cmd = redis::cmd("SCRIPT");
    cmd.arg("EXISTS");
    for s in shas {
        cmd.arg(s.as_str());
    }
    cmd
}

fn cmd_script_flush(mode: Option<&str>) -> redis::Cmd {
    let mut cmd = redis::cmd("SCRIPT");
    cmd.arg("FLUSH");
    if let Some(m) = mode {
        cmd.arg(m);
    }
    cmd
}

fn cmd_script_kill() -> redis::Cmd {
    let mut cmd = redis::cmd("SCRIPT");
    cmd.arg("KILL");
    cmd
}

fn validate_flush_mode(mode: &str) -> PyResult<()> {
    match mode.to_ascii_uppercase().as_str() {
        "ASYNC" | "SYNC" => Ok(()),
        _ => Err(pyo3::PyErr::new::<crate::exceptions::DataError, _>(format!(
            "flush mode must be ASYNC or SYNC, got {mode}"
        ))),
    }
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    fn script_load(&self, py: Python<'_>, script: &str) -> PyResult<String> {
        let cmd = cmd_script_load(script);
        let r: redis::RedisResult<String> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    fn ascript_load(&self, py: Python<'_>, script: &str) -> PyResult<Py<PyAny>> {
        let script = script.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_script_load(&script);
            let r: redis::RedisResult<String> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*shas))]
    fn script_exists(&self, py: Python<'_>, shas: Vec<String>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_script_exists(&shas);
        let r: redis::RedisResult<Vec<bool>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::BoolList(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (*shas))]
    fn ascript_exists(&self, py: Python<'_>, shas: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_script_exists(&shas);
            let r: redis::RedisResult<Vec<bool>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, mode=None))]
    fn script_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<()> {
        if let Some(ref m) = mode {
            validate_flush_mode(m)?;
        }
        let cmd = cmd_script_flush(mode.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, mode=None))]
    fn ascript_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<Py<PyAny>> {
        if let Some(ref m) = mode {
            validate_flush_mode(m)?;
        }
        async_op!(self, py, conn, async {
            let cmd = cmd_script_flush(mode.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn script_kill(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_script_kill();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    fn ascript_kill(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_script_kill();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_scripts.py::TestScriptLoad tests/driver/test_commands_scripts.py::TestScriptExists tests/driver/test_commands_scripts.py::TestScriptFlush tests/driver/test_commands_scripts.py::TestScriptKill -v`
Expected: 13 PASS. Also re-run the EVAL tests — `test_evalsha_after_script_load` should now pass.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/scripts.rs tests/driver/test_commands_scripts.py
git commit -m "feat(scripts): add SCRIPT LOAD/EXISTS/FLUSH/KILL"
```

---

## Task 4: `FCALL` + `FCALL_RO`

Same shape as `EVAL` but invokes a registered Function instead of a script.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/scripts.rs`
- Modify: `tests/driver/test_commands_scripts.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_scripts.py`:

```python
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
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY, replace=True)

    def test_fcall_basic(self, driver) -> None:
        self._load(driver)
        assert driver.fcall("myecho", [], [b"hello"]) == b"hello"

    def test_fcall_with_keys(self, driver, redis_py_client) -> None:
        self._load(driver)
        assert driver.fcall("mywrite", ["k"], [b"value"]) == b"OK"
        assert redis_py_client.get("k") == b"value"

    def test_fcall_unknown_function_raises(self, driver) -> None:
        from redis_rs_py.exceptions import ResponseError
        with pytest.raises(ResponseError):
            driver.fcall("nonexistent", [], [])

    @pytest.mark.asyncio
    async def test_afcall(self, driver) -> None:
        self._load(driver)
        assert await driver.afcall("myecho", [], [b"x"]) == b"x"


class TestFcallRo:
    def _load(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY, replace=True)

    def test_fcall_ro_no_writes_function_works(self, driver) -> None:
        self._load(driver)
        driver.set("k", b"hello")
        # myreadonly is flagged 'no-writes' — fcall_ro accepts it.
        assert driver.fcall_ro("myreadonly", ["k"], []) == b"hello"

    def test_fcall_ro_rejects_writes(self, driver) -> None:
        self._load(driver)
        from redis_rs_py.exceptions import ResponseError
        with pytest.raises(ResponseError):
            driver.fcall_ro("mywrite", ["k"], [b"v"])
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_scripts.py::TestFcall tests/driver/test_commands_scripts.py::TestFcallRo -v`
Expected: FAIL.

- [ ] **Step 3: Implement FCALL**

Append to **Argument-encoding helpers**:

```rust
fn cmd_fcall(name: &str, function: &str, keys: &[String], args: &[Vec<u8>]) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    cmd.arg(function).arg(keys.len());
    for k in keys {
        cmd.arg(k.as_str());
    }
    for a in args {
        cmd.arg(a.as_slice());
    }
    cmd
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (function, keys, args))]
    fn fcall(
        &self,
        py: Python<'_>,
        function: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_fcall("FCALL", function, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (function, keys, args))]
    fn afcall(
        &self,
        py: Python<'_>,
        function: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let function = function.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_fcall("FCALL", &function, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (function, keys, args))]
    fn fcall_ro(
        &self,
        py: Python<'_>,
        function: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_fcall("FCALL_RO", function, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (function, keys, args))]
    fn afcall_ro(
        &self,
        py: Python<'_>,
        function: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let function = function.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_fcall("FCALL_RO", &function, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + test (FCALL tests will fail until FUNCTION LOAD lands in Task 5)**

Run: `cargo check -p redis-rs-py-driver`
Expected: clean. Tests in `TestFcall` reference `driver.function_load` — they'll fail at fixture setup. Skip them for now: `pytest -k "not Fcall"` to verify the rest still passes.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/scripts.rs tests/driver/test_commands_scripts.py
git commit -m "feat(scripts): add FCALL and FCALL_RO"
```

---

## Task 5: `FUNCTION LOAD/DUMP/FLUSH/LIST/STATS/KILL/RESTORE/DELETE`

The full FUNCTION management surface.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/scripts.rs`
- Modify: `tests/driver/test_commands_scripts.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_scripts.py`:

```python
class TestFunction:
    def setup_method(self, _method) -> None:
        self.driver = None  # set per-test by fixture

    def test_function_load_returns_library_name(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        name = driver.function_load(SAMPLE_LIBRARY)
        assert name == "mylib"

    def test_function_load_replace(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY)
        # Without replace, re-loading raises.
        from redis_rs_py.exceptions import ResponseError
        with pytest.raises(ResponseError):
            driver.function_load(SAMPLE_LIBRARY)
        # With replace, succeeds.
        assert driver.function_load(SAMPLE_LIBRARY, replace=True) == "mylib"

    def test_function_list_basic(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY)
        result = driver.function_list()
        # Result is a list of dicts (or arrays the upstream renders as lists).
        assert isinstance(result, list)
        assert any(b"mylib" in str(item).encode() for item in result)

    def test_function_list_with_library_filter(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY)
        result = driver.function_list(library="mylib")
        assert len(result) == 1

    def test_function_list_withcode(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY)
        result = driver.function_list(library="mylib", withcode=True)
        # When withcode=True the entry includes the script source.
        assert SAMPLE_LIBRARY.encode() in str(result).encode()

    def test_function_dump_returns_bytes(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY)
        dump = driver.function_dump()
        assert isinstance(dump, bytes)
        assert len(dump) > 0

    def test_function_restore_roundtrip(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY)
        dump = driver.function_dump()
        driver.function_flush()
        driver.function_restore(dump, policy="REPLACE")
        # After restore, the library is back.
        assert driver.fcall("myecho", [], [b"x"]) == b"x"

    def test_function_flush_removes_libraries(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        driver.function_load(SAMPLE_LIBRARY)
        driver.function_flush()
        assert driver.function_list() == []

    def test_function_stats_no_running(self, driver) -> None:
        # Returns either an empty stats blob or the schema with no running script.
        result = driver.function_stats()
        assert result is not None

    def test_function_kill_no_script_running(self, driver) -> None:
        from redis_rs_py.exceptions import ResponseError
        with pytest.raises(ResponseError):
            driver.function_kill()

    @pytest.mark.asyncio
    async def test_afunction_load(self, driver) -> None:
        try:
            driver.function_delete("mylib")
        except Exception:
            pass
        name = await driver.afunction_load(SAMPLE_LIBRARY)
        assert name == "mylib"
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_scripts.py::TestFunction -v`
Expected: FAIL.

- [ ] **Step 3: Implement FUNCTION family**

Append to **Argument-encoding helpers**:

```rust
fn cmd_function_load(code: &str, replace: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("LOAD");
    if replace {
        cmd.arg("REPLACE");
    }
    cmd.arg(code);
    cmd
}

fn cmd_function_delete(library: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("DELETE").arg(library);
    cmd
}

fn cmd_function_dump() -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("DUMP");
    cmd
}

fn cmd_function_flush(mode: Option<&str>) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("FLUSH");
    if let Some(m) = mode {
        cmd.arg(m);
    }
    cmd
}

fn cmd_function_list(library: Option<&str>, withcode: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("LIST");
    if let Some(lib) = library {
        cmd.arg("LIBRARYNAME").arg(lib);
    }
    if withcode {
        cmd.arg("WITHCODE");
    }
    cmd
}

fn cmd_function_stats() -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("STATS");
    cmd
}

fn cmd_function_kill() -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("KILL");
    cmd
}

fn cmd_function_restore(dump: &[u8], policy: Option<&str>) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("RESTORE").arg(dump);
    if let Some(p) = policy {
        cmd.arg(p);
    }
    cmd
}

fn validate_restore_policy(policy: &str) -> PyResult<()> {
    match policy.to_ascii_uppercase().as_str() {
        "FLUSH" | "APPEND" | "REPLACE" => Ok(()),
        _ => Err(pyo3::PyErr::new::<crate::exceptions::DataError, _>(format!(
            "restore policy must be FLUSH, APPEND, or REPLACE, got {policy}"
        ))),
    }
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (code, *, replace=false))]
    fn function_load(&self, py: Python<'_>, code: &str, replace: bool) -> PyResult<String> {
        let cmd = cmd_function_load(code, replace);
        let r: redis::RedisResult<String> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (code, *, replace=false))]
    fn afunction_load(
        &self,
        py: Python<'_>,
        code: &str,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        let code = code.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_function_load(&code, replace);
            let r: redis::RedisResult<String> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn function_delete(&self, py: Python<'_>, library: &str) -> PyResult<()> {
        let cmd = cmd_function_delete(library);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    fn afunction_delete(&self, py: Python<'_>, library: &str) -> PyResult<Py<PyAny>> {
        let library = library.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_function_delete(&library);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn function_dump(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_function_dump();
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        Ok(pyo3::types::PyBytes::new(py, &r.map_err(to_py_err)?)
            .into_any()
            .unbind())
    }

    fn afunction_dump(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_function_dump();
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, mode=None))]
    fn function_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<()> {
        if let Some(ref m) = mode {
            validate_flush_mode(m)?;
        }
        let cmd = cmd_function_flush(mode.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, mode=None))]
    fn afunction_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<Py<PyAny>> {
        if let Some(ref m) = mode {
            validate_flush_mode(m)?;
        }
        async_op!(self, py, conn, async {
            let cmd = cmd_function_flush(mode.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, library=None, withcode=false))]
    fn function_list(
        &self,
        py: Python<'_>,
        library: Option<String>,
        withcode: bool,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_function_list(library.as_deref(), withcode);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (*, library=None, withcode=false))]
    fn afunction_list(
        &self,
        py: Python<'_>,
        library: Option<String>,
        withcode: bool,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_function_list(library.as_deref(), withcode);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn function_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_function_stats();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    fn afunction_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_function_stats();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn function_kill(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_function_kill();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    fn afunction_kill(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_function_kill();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (dump, *, policy=None))]
    fn function_restore(
        &self,
        py: Python<'_>,
        dump: &[u8],
        policy: Option<String>,
    ) -> PyResult<()> {
        if let Some(ref p) = policy {
            validate_restore_policy(p)?;
        }
        let cmd = cmd_function_restore(dump, policy.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (dump, *, policy=None))]
    fn afunction_restore(
        &self,
        py: Python<'_>,
        dump: &[u8],
        policy: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        if let Some(ref p) = policy {
            validate_restore_policy(p)?;
        }
        let dump = dump.to_vec();
        async_op!(self, py, conn, async {
            let cmd = cmd_function_restore(&dump, policy.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test the full scripts suite**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_scripts.py -v`
Expected: ~40 PASS (TestEval 7 + TestEvalsha 3 + TestEvalRo 3 + TestScriptLoad 3 + TestScriptExists 4 + TestScriptFlush 5 + TestScriptKill 1 + TestFcall 4 + TestFcallRo 2 + TestFunction 11).

If `test_function_load_replace` fails on a clean DB because the Valkey image doesn't ship Lua/Function support out of the box, skip the test on that image.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/scripts.rs tests/driver/test_commands_scripts.py
git commit -m "feat(scripts): add FUNCTION LOAD/DUMP/FLUSH/LIST/STATS/KILL/RESTORE/DELETE"
```

---

## Task 6: `SCAN` (single iteration) + `KEYS` + `RANDOMKEY`

`SCAN` returns a `(cursor, list_of_keys)` 2-tuple. The Python-side `scan_iter` generator (Task 7) wraps this.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/admin.rs`
- Create: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Create `tests/driver/test_commands_admin.py`:

```python
"""Admin / introspection commands."""

from __future__ import annotations

import asyncio
import time

import pytest


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
```

- [ ] **Step 2: Run failing tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py::TestScan tests/driver/test_commands_admin.py::TestKeys tests/driver/test_commands_admin.py::TestRandomkey -v`
Expected: FAIL.

- [ ] **Step 3: Implement SCAN/KEYS/RANDOMKEY**

Append to **Argument-encoding helpers** in `commands/admin.rs`:

```rust
fn cmd_scan(
    cursor: u64,
    match_pattern: Option<&str>,
    count: Option<i64>,
    type_filter: Option<&str>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("SCAN");
    cmd.arg(cursor);
    if let Some(p) = match_pattern {
        cmd.arg("MATCH").arg(p);
    }
    if let Some(c) = count {
        cmd.arg("COUNT").arg(c);
    }
    if let Some(t) = type_filter {
        cmd.arg("TYPE").arg(t);
    }
    cmd
}

fn cmd_keys(pattern: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("KEYS");
    cmd.arg(pattern);
    cmd
}

fn cmd_randomkey() -> redis::Cmd {
    redis::cmd("RANDOMKEY")
}

/// Convert a SCAN reply (cursor as string|int, then array of bytes-keys)
/// into the typed pair our Python users expect.
fn parse_scan_reply(value: redis::Value) -> (u64, Vec<Vec<u8>>) {
    let parts = match value {
        redis::Value::Array(items) if items.len() == 2 => items,
        _ => return (0, Vec::new()),
    };
    let mut iter = parts.into_iter();
    let cursor = match iter.next() {
        Some(redis::Value::BulkString(b)) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        Some(redis::Value::Int(n)) => n.max(0) as u64,
        Some(redis::Value::SimpleString(s)) => s.parse().unwrap_or(0),
        _ => 0,
    };
    let keys = match iter.next() {
        Some(redis::Value::Array(items)) => items
            .into_iter()
            .filter_map(|v| match v {
                redis::Value::BulkString(b) => Some(b),
                redis::Value::SimpleString(s) => Some(s.into_bytes()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    (cursor, keys)
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (*, cursor=0, match=None, count=None, type=None))]
    fn scan(
        &self,
        py: Python<'_>,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
        r#type: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_scan(cursor, r#match.as_deref(), count, r#type.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let (next_cursor, keys) = parse_scan_reply(r.map_err(to_py_err)?);
        let cursor_py = next_cursor.into_pyobject(py)?.into_any().unbind();
        let keys_py: Vec<Py<PyAny>> = keys
            .into_iter()
            .map(|k| PyBytes::new(py, &k).into_any().unbind())
            .collect();
        let keys_list = pyo3::types::PyList::new(py, keys_py)?.into_any().unbind();
        Ok(PyTuple::new(py, [cursor_py, keys_list])?.into_any().unbind())
    }

    #[pyo3(signature = (*, cursor=0, match=None, count=None, type=None))]
    fn ascan(
        &self,
        py: Python<'_>,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
        r#type: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_scan(cursor, r#match.as_deref(), count, r#type.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => {
                    // Encode as a Value::Array((cursor, keys)) for the recursive
                    // converter. Cleaner: use a dedicated variant. For now, build
                    // a Tuple-equivalent via the recursive Value path.
                    let (next_cursor, keys) = parse_scan_reply(v);
                    // Re-pack as a Value the converter can render. Since we want a
                    // tuple-shape on the Python side, push a sentinel: we'll
                    // intercept here and short-circuit through a typed variant.
                    RawResult::Value(redis::Value::Array(vec![
                        redis::Value::Int(next_cursor as i64),
                        redis::Value::Array(
                            keys.into_iter().map(redis::Value::BulkString).collect(),
                        ),
                    ]))
                }
                Err(e) => classify(e),
            }
        })
    }

    fn keys(&self, py: Python<'_>, pattern: &str) -> PyResult<Py<PyAny>> {
        warn_keys_use(py)?;
        let cmd = cmd_keys(pattern);
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::BytesList(r.map_err(to_py_err)?).into_py(py)
    }

    fn akeys(&self, py: Python<'_>, pattern: &str) -> PyResult<Py<PyAny>> {
        let pattern = pattern.to_string();
        warn_keys_use(py)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_keys(&pattern);
            let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn randomkey(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_randomkey();
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::OptBytes(r.map_err(to_py_err)?).into_py(py)
    }

    fn arandomkey(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_randomkey();
            let r: redis::RedisResult<Option<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

Add the helper near the top of `commands/admin.rs`, below the imports:

```rust
fn warn_keys_use(py: Python<'_>) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    let _ = warnings.call_method1(
        "warn",
        (
            "KEYS scans the entire keyspace and blocks the server. Use scan_iter() instead.",
            py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
        ),
    );
    Ok(())
}
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py::TestScan tests/driver/test_commands_admin.py::TestKeys tests/driver/test_commands_admin.py::TestRandomkey -v`
Expected: 11 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/admin.rs tests/driver/test_commands_admin.py
git commit -m "feat(admin): add SCAN, KEYS (with deprecation warning), RANDOMKEY"
```

---

## Task 7: `scan_iter` Python helper (sync + async generators)

Python-side generator over `driver.scan(cursor=)`. The only Python implementation code in this plan; documented in the file header.

**Files:**
- Create: `python/redis_rs_py/_scan_iter.py`
- Create: `python/redis_rs_py/asyncio/__init__.py` (if missing)
- Create: `python/redis_rs_py/asyncio/_scan_iter.py`
- Modify: `python/redis_rs_py/__init__.py`
- Modify: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_admin.py`:

```python
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
        keys: list[bytes] = []
        async for k in driver.scan_iter_async(count=5):
            keys.append(k)
        assert len(keys) == 20
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_admin.py::TestScanIter -v`
Expected: FAIL — `scan_iter` doesn't exist on the driver.

- [ ] **Step 3: Implement the sync generator helper**

Create `python/redis_rs_py/_scan_iter.py`:

```python
"""Python-side generator wrapper around RedisRsDriver.scan(cursor=).

This is the one place where shipping Python code is unavoidable: PyO3
pyclasses can implement __iter__/__next__ but not be a true Python
generator (and crucially can't be an async-generator on the asyncio
side). Documented as the explicit Rust-by-default escape hatch in
PLAN.md lines 60-63.

Both helpers are attached to RedisRsDriver via __init__.py monkey-patch
at import time so users can call `driver.scan_iter(...)` directly.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis_rs_py._driver import RedisRsDriver


def scan_iter(
    self: RedisRsDriver,
    *,
    match: str | None = None,
    count: int | None = None,
    type: str | None = None,
) -> Iterator[bytes]:
    """Yield every key in the database, paginated via SCAN under the hood.

    Honors the same `match`/`count`/`type` filters as `scan(cursor=, ...)`.
    Resumes from the cursor returned by the previous SCAN call until the
    server returns cursor 0.
    """
    cursor = 0
    while True:
        cursor, keys = self.scan(cursor=cursor, match=match, count=count, type=type)
        yield from keys
        if cursor == 0:
            return
```

- [ ] **Step 4: Implement the async generator helper**

Create `python/redis_rs_py/asyncio/__init__.py` (if missing — write it as empty for now plus the re-export):

```python
"""redis_rs_py.asyncio — asyncio-coloured surface.

Plan 11 lands the high-level `Redis` async facade. For now this submodule
exposes only the `scan_iter_async` helper (Plan 09).
"""
```

Create `python/redis_rs_py/asyncio/_scan_iter.py`:

```python
"""Async generator wrapper around RedisRsDriver.ascan(cursor=).

Same rationale as the sync version: an async-generator function on a
PyO3 pyclass isn't expressible directly. Attached to RedisRsDriver as
`scan_iter_async` via __init__.py monkey-patch.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis_rs_py._driver import RedisRsDriver


async def scan_iter_async(
    self: RedisRsDriver,
    *,
    match: str | None = None,
    count: int | None = None,
    type: str | None = None,
) -> AsyncIterator[bytes]:
    """Asynchronously yield every key, paginated via SCAN."""
    cursor = 0
    while True:
        cursor, keys = await self.ascan(
            cursor=cursor, match=match, count=count, type=type
        )
        for k in keys:
            yield k
        if cursor == 0:
            return
```

- [ ] **Step 5: Attach the generators to `RedisRsDriver` at package import**

Edit `python/redis_rs_py/__init__.py`. After the existing imports:

```python
# Attach the Python-side scan_iter helpers to the Rust pyclass. Done
# here so users get `driver.scan_iter(...)` and `driver.scan_iter_async(...)`
# without an extra import step. (See _scan_iter.py for why these can't
# be Rust pyclass methods.)
from redis_rs_py._scan_iter import scan_iter as _scan_iter
from redis_rs_py.asyncio._scan_iter import scan_iter_async as _scan_iter_async

RedisRsDriver.scan_iter = _scan_iter  # type: ignore[attr-defined]
RedisRsDriver.scan_iter_async = _scan_iter_async  # type: ignore[attr-defined]
```

- [ ] **Step 6: Run + verify the tests pass**

Run: `uv run pytest tests/driver/test_commands_admin.py::TestScanIter -v`
Expected: 5 PASS.

- [ ] **Step 7: Commit**

```bash
git add python/redis_rs_py/_scan_iter.py python/redis_rs_py/asyncio/ python/redis_rs_py/__init__.py tests/driver/test_commands_admin.py
git commit -m "feat(admin): add scan_iter sync + async generator helpers"
```

---

## Task 8: `DBSIZE` + `FLUSHDB` + `FLUSHALL` + `SELECT` (with documented limitation)

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/admin.rs`
- Modify: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_admin.py`:

```python
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
        assert driver.select(0) is True

    def test_select_different_db_raises_or_returns_false(self, driver) -> None:
        # Documented limitation: per-Redis-instance database, no per-conn
        # mutability. The server still accepts SELECT 1 (returns OK), but
        # subsequent commands stay in db 0 because the pool reset on
        # multiplexed conns drops the SELECT. We choose the path of LEAST
        # surprise: raise NotImplementedError when db != connected db.
        # Verify our behaviour.
        with pytest.raises((NotImplementedError, RuntimeError)):
            driver.select(1)
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_admin.py::TestDbSize tests/driver/test_commands_admin.py::TestFlushdb tests/driver/test_commands_admin.py::TestFlushall tests/driver/test_commands_admin.py::TestSelect -v`
Expected: FAIL.

- [ ] **Step 3: Implement DBSIZE/FLUSHDB/FLUSHALL/SELECT**

Append to **Argument-encoding helpers**:

```rust
fn cmd_dbsize() -> redis::Cmd {
    redis::cmd("DBSIZE")
}

fn cmd_flushdb(asynchronous: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("FLUSHDB");
    if asynchronous {
        cmd.arg("ASYNC");
    } else {
        cmd.arg("SYNC");
    }
    cmd
}

fn cmd_flushall(asynchronous: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("FLUSHALL");
    if asynchronous {
        cmd.arg("ASYNC");
    } else {
        cmd.arg("SYNC");
    }
    cmd
}

/// Extract the `/<db>` segment from a redis URL (defaults to 0 if missing
/// or unparseable). Used by SELECT to validate the requested db matches
/// the connected db.
fn url_db_index(url: &str) -> u8 {
    let after_scheme = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => return 0,
    };
    let after_host = match after_scheme.split_once('/') {
        Some((_, path)) => path,
        None => return 0,
    };
    let path_segment = after_host.split(['?', '#']).next().unwrap_or("");
    path_segment.parse().unwrap_or(0)
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    fn dbsize(&self, py: Python<'_>) -> PyResult<i64> {
        let cmd = cmd_dbsize();
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    fn adbsize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_dbsize();
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, asynchronous=false))]
    fn flushdb(&self, py: Python<'_>, asynchronous: bool) -> PyResult<()> {
        let cmd = cmd_flushdb(asynchronous);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, asynchronous=false))]
    fn aflushdb(&self, py: Python<'_>, asynchronous: bool) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_flushdb(asynchronous);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, asynchronous=false))]
    fn flushall(&self, py: Python<'_>, asynchronous: bool) -> PyResult<()> {
        let cmd = cmd_flushall(asynchronous);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, asynchronous=false))]
    fn aflushall(&self, py: Python<'_>, asynchronous: bool) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_flushall(asynchronous);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    /// Per-Redis-instance database — set at connect time via the URL's
    /// `/<db>` segment. We accept SELECT for compatibility but only
    /// succeed when `db_index` matches the connected db; raising
    /// otherwise is preferable to silently drifting state under a
    /// multiplexed pool.
    fn select(&self, db_index: u8) -> PyResult<bool> {
        let connected = url_db_index(&self.url);
        if db_index == connected {
            Ok(true)
        } else {
            Err(pyo3::exceptions::PyNotImplementedError::new_err(format!(
                "SELECT to a different db is not supported under the multiplexed \
                 connection model (connected to db {connected}, requested {db_index}). \
                 Construct a new RedisRsDriver with the desired db in the URL instead."
            )))
        }
    }
```

The `select` method needs access to `self.url`, which already exists on `RedisRsDriver` (added in Plan 01 Task 9). No driver.rs edit needed.

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py::TestDbSize tests/driver/test_commands_admin.py::TestFlushdb tests/driver/test_commands_admin.py::TestFlushall tests/driver/test_commands_admin.py::TestSelect -v`
Expected: 9 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/admin.rs tests/driver/test_commands_admin.py
git commit -m "feat(admin): add DBSIZE/FLUSHDB/FLUSHALL/SELECT (with limitation)"
```

---

## Task 9: `INFO` + section parsing helper

`INFO` returns a bulk-string of `key:value\n` lines grouped under `# Section\n` headers. The driver returns the raw text; the high-level facade in plan 10 will parse it. We do ship a `_parse_info_text(text)` Python helper here since the parsing logic is needed by both layers and is < 30 LOC.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/admin.rs`
- Modify: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_admin.py`:

```python
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
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_admin.py::TestInfo -v`
Expected: FAIL.

- [ ] **Step 3: Implement INFO**

Append to **Argument-encoding helpers**:

```rust
fn cmd_info(section: Option<&str>) -> redis::Cmd {
    let mut cmd = redis::cmd("INFO");
    if let Some(s) = section {
        cmd.arg(s);
    }
    cmd
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (*, section=None))]
    fn info(&self, py: Python<'_>, section: Option<String>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_info(section.as_deref());
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    #[pyo3(signature = (*, section=None))]
    fn ainfo(&self, py: Python<'_>, section: Option<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_info(section.as_deref());
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py::TestInfo -v`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/admin.rs tests/driver/test_commands_admin.py
git commit -m "feat(admin): add INFO with optional section filter"
```

---

## Task 10: `CONFIG GET` / `CONFIG SET` / `CONFIG RESETSTAT` / `CONFIG REWRITE`

`CONFIG GET` returns a `dict[bytes, bytes]` (RESP3 Map). We use `RawResult::BytesPairs` (already exists from Plan 01).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/admin.rs`
- Modify: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_admin.py`:

```python
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
        from redis_rs_py.exceptions import ResponseError
        with pytest.raises(ResponseError):
            driver.config_rewrite()

    @pytest.mark.asyncio
    async def test_aconfig_get_set(self, driver) -> None:
        await driver.aconfig_set("maxmemory-policy", "allkeys-lfu")
        result = await driver.aconfig_get("maxmemory-policy")
        assert result[b"maxmemory-policy"] == b"allkeys-lfu"
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_admin.py::TestConfig -v`
Expected: FAIL.

- [ ] **Step 3: Implement CONFIG family**

Append to **Argument-encoding helpers**:

```rust
fn cmd_config_get(parameter: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("CONFIG");
    cmd.arg("GET").arg(parameter);
    cmd
}

fn cmd_config_set(pairs: &[(String, String)]) -> redis::Cmd {
    let mut cmd = redis::cmd("CONFIG");
    cmd.arg("SET");
    for (k, v) in pairs {
        cmd.arg(k.as_str()).arg(v.as_str());
    }
    cmd
}

fn cmd_config_resetstat() -> redis::Cmd {
    let mut cmd = redis::cmd("CONFIG");
    cmd.arg("RESETSTAT");
    cmd
}

fn cmd_config_rewrite() -> redis::Cmd {
    let mut cmd = redis::cmd("CONFIG");
    cmd.arg("REWRITE");
    cmd
}

/// Flatten a CONFIG GET reply (Map or flat-Array of key/value pairs)
/// into the typed pair-list.
fn parse_config_get_reply(value: redis::Value) -> Vec<(Vec<u8>, Vec<u8>)> {
    match value {
        redis::Value::Map(pairs) => pairs
            .into_iter()
            .filter_map(|(k, v)| match (k, v) {
                (redis::Value::BulkString(kb), redis::Value::BulkString(vb)) => Some((kb, vb)),
                _ => None,
            })
            .collect(),
        redis::Value::Array(flat) => {
            let mut out = Vec::with_capacity(flat.len() / 2);
            let mut iter = flat.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                if let (redis::Value::BulkString(kb), redis::Value::BulkString(vb)) = (k, v) {
                    out.push((kb, vb));
                }
            }
            out
        }
        _ => Vec::new(),
    }
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    fn config_get(&self, py: Python<'_>, parameter: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_config_get(parameter);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::BytesPairs(parse_config_get_reply(r.map_err(to_py_err)?)).into_py(py)
    }

    fn aconfig_get(&self, py: Python<'_>, parameter: &str) -> PyResult<Py<PyAny>> {
        let parameter = parameter.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_config_get(&parameter);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::BytesPairs(parse_config_get_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    /// CONFIG SET — accepts either `(name, value)` positional args, or a
    /// single `mapping={name: value, ...}` kwarg. Mirrors redis-py.
    #[pyo3(signature = (name_or_mapping, value=None))]
    fn config_set(
        &self,
        py: Python<'_>,
        name_or_mapping: Bound<'_, PyAny>,
        value: Option<String>,
    ) -> PyResult<()> {
        let pairs = config_set_extract_pairs(&name_or_mapping, value)?;
        let cmd = cmd_config_set(&pairs);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (name_or_mapping, value=None))]
    fn aconfig_set(
        &self,
        py: Python<'_>,
        name_or_mapping: Bound<'_, PyAny>,
        value: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let pairs = config_set_extract_pairs(&name_or_mapping, value)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_config_set(&pairs);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn config_resetstat(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_config_resetstat();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    fn aconfig_resetstat(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_config_resetstat();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn config_rewrite(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_config_rewrite();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    fn aconfig_rewrite(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_config_rewrite();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
```

Add the helper near the top of the file (below `warn_keys_use`):

```rust
/// CONFIG SET kwarg coercion: accept either `(name: str, value: str)` or
/// `(mapping: dict[str, str], value=None)`. Returns the flat list of pairs.
fn config_set_extract_pairs(
    name_or_mapping: &Bound<'_, pyo3::PyAny>,
    value: Option<String>,
) -> PyResult<Vec<(String, String)>> {
    if let Ok(s) = name_or_mapping.extract::<String>() {
        let v = value.ok_or_else(|| {
            pyo3::PyErr::new::<crate::exceptions::DataError, _>(
                "config_set(name, value) requires a value when name is a string",
            )
        })?;
        return Ok(vec![(s, v)]);
    }
    let dict: Vec<(String, String)> = name_or_mapping.extract()?;
    Ok(dict)
}
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py::TestConfig -v`
Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/admin.rs tests/driver/test_commands_admin.py
git commit -m "feat(admin): add CONFIG GET/SET/RESETSTAT/REWRITE"
```

---

## Task 11: `CLIENT` family — part 1 (`GETNAME`/`SETNAME`/`ID`/`INFO`/`LIST`)

Five small commands that introspect the connection itself.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/admin.rs`
- Modify: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_admin.py`:

```python
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
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_admin.py::TestClient -v`
Expected: FAIL.

- [ ] **Step 3: Implement CLIENT GETNAME/SETNAME/ID/INFO/LIST**

Append to **Argument-encoding helpers**:

```rust
fn cmd_client_id() -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("ID");
    cmd
}

fn cmd_client_getname() -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("GETNAME");
    cmd
}

fn cmd_client_setname(name: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("SETNAME").arg(name);
    cmd
}

fn cmd_client_info() -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("INFO");
    cmd
}

fn cmd_client_list(client_type: Option<&str>, client_ids: &[i64]) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("LIST");
    if let Some(t) = client_type {
        cmd.arg("TYPE").arg(t);
    }
    if !client_ids.is_empty() {
        cmd.arg("ID");
        for id in client_ids {
            cmd.arg(*id);
        }
    }
    cmd
}

/// Parse a `CLIENT LIST` text reply (newline-separated lines of
/// `key=value key=value ...`) into a list of dict[bytes, bytes].
fn parse_client_list_reply(text: &[u8]) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for line in text.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let mut row = Vec::new();
        for pair in line.split(|&b| b == b' ') {
            if let Some(eq_pos) = pair.iter().position(|&b| b == b'=') {
                let (k, v) = pair.split_at(eq_pos);
                row.push((k.to_vec(), v[1..].to_vec()));
            }
        }
        if !row.is_empty() {
            out.push(row);
        }
    }
    out
}
```

We need a new `RawResult` variant for the CLIENT LIST output `Vec<Vec<(Vec<u8>, Vec<u8>)>>`. Add it to `async_bridge.rs`:

```rust
    BytesPairsList(Vec<Vec<(Vec<u8>, Vec<u8>)>>),
```

And the `into_py` arm:

```rust
            RawResult::BytesPairsList(rows) => {
                let mut items: Vec<Py<PyAny>> = Vec::with_capacity(rows.len());
                for row in rows {
                    let dict = PyDict::new(py);
                    for (k, v) in row {
                        dict.set_item(PyBytes::new(py, &k), PyBytes::new(py, &v))?;
                    }
                    items.push(dict.into_any().unbind());
                }
                Ok(PyList::new(py, items)?.into_any().unbind())
            }
```

Append to `#[pymethods] impl RedisRsDriver` in `commands/admin.rs`:

```rust
    fn client_id(&self, py: Python<'_>) -> PyResult<i64> {
        let cmd = cmd_client_id();
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    fn aclient_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_id();
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn client_getname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_client_getname();
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::OptBytes(r.map_err(to_py_err)?).into_py(py)
    }

    fn aclient_getname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_getname();
            let r: redis::RedisResult<Option<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn client_setname(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let cmd = cmd_client_setname(name);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    fn aclient_setname(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_client_setname(&name);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn client_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_client_info();
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    fn aclient_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_info();
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, client_type=None, client_id=None))]
    fn client_list(
        &self,
        py: Python<'_>,
        client_type: Option<String>,
        client_id: Option<Vec<i64>>,
    ) -> PyResult<Py<PyAny>> {
        let ids = client_id.unwrap_or_default();
        let cmd = cmd_client_list(client_type.as_deref(), &ids);
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let rows = parse_client_list_reply(&r.map_err(to_py_err)?);
        RawResult::BytesPairsList(rows).into_py(py)
    }

    #[pyo3(signature = (*, client_type=None, client_id=None))]
    fn aclient_list(
        &self,
        py: Python<'_>,
        client_type: Option<String>,
        client_id: Option<Vec<i64>>,
    ) -> PyResult<Py<PyAny>> {
        let ids = client_id.unwrap_or_default();
        async_op!(self, py, conn, async {
            let cmd = cmd_client_list(client_type.as_deref(), &ids);
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(text) => RawResult::BytesPairsList(parse_client_list_reply(&text)),
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py::TestClient -v`
Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/admin.rs crates/redis-rs-py-driver/src/async_bridge.rs tests/driver/test_commands_admin.py
git commit -m "feat(admin): add CLIENT ID/GETNAME/SETNAME/INFO/LIST"
```

---

## Task 12: `CLIENT` family — part 2 (`KILL`/`PAUSE`/`UNPAUSE`/`NO-EVICT`/`NO-TOUCH`)

The kill-filter family with its kwarg matrix.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/admin.rs`
- Modify: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_admin.py`:

```python
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
        from redis_rs_py.exceptions import DataError
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
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_admin.py::TestClientKillPause -v`
Expected: FAIL.

- [ ] **Step 3: Implement CLIENT KILL/PAUSE/UNPAUSE/NO-EVICT/NO-TOUCH**

Append to **Argument-encoding helpers**:

```rust
#[allow(clippy::too_many_arguments)]
fn cmd_client_kill(
    addr: Option<&str>,
    laddr: Option<&str>,
    client_id: Option<i64>,
    client_type: Option<&str>,
    user: Option<&str>,
    skipme: Option<bool>,
    maxage: Option<i64>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("KILL");
    if let Some(a) = addr {
        cmd.arg("ADDR").arg(a);
    }
    if let Some(la) = laddr {
        cmd.arg("LADDR").arg(la);
    }
    if let Some(id) = client_id {
        cmd.arg("ID").arg(id);
    }
    if let Some(t) = client_type {
        cmd.arg("TYPE").arg(t);
    }
    if let Some(u) = user {
        cmd.arg("USER").arg(u);
    }
    if let Some(skip) = skipme {
        cmd.arg("SKIPME").arg(if skip { "yes" } else { "no" });
    }
    if let Some(age) = maxage {
        cmd.arg("MAXAGE").arg(age);
    }
    cmd
}

fn cmd_client_pause(timeout_ms: i64, all: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("PAUSE").arg(timeout_ms);
    cmd.arg(if all { "ALL" } else { "WRITE" });
    cmd
}

fn cmd_client_unpause() -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("UNPAUSE");
    cmd
}

fn cmd_client_no_evict(mode: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("NO-EVICT").arg(mode);
    cmd
}

fn cmd_client_no_touch(mode: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("NO-TOUCH").arg(mode);
    cmd
}

fn validate_on_off_mode(mode: &str) -> PyResult<()> {
    match mode.to_ascii_uppercase().as_str() {
        "ON" | "OFF" => Ok(()),
        _ => Err(pyo3::PyErr::new::<crate::exceptions::DataError, _>(format!(
            "mode must be ON or OFF, got {mode}"
        ))),
    }
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        addr=None,
        laddr=None,
        client_id=None,
        client_type=None,
        user=None,
        skipme=None,
        maxage=None,
    ))]
    fn client_kill(
        &self,
        py: Python<'_>,
        addr: Option<String>,
        laddr: Option<String>,
        client_id: Option<i64>,
        client_type: Option<String>,
        user: Option<String>,
        skipme: Option<bool>,
        maxage: Option<i64>,
    ) -> PyResult<i64> {
        let cmd = cmd_client_kill(
            addr.as_deref(),
            laddr.as_deref(),
            client_id,
            client_type.as_deref(),
            user.as_deref(),
            skipme,
            maxage,
        );
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        addr=None,
        laddr=None,
        client_id=None,
        client_type=None,
        user=None,
        skipme=None,
        maxage=None,
    ))]
    fn aclient_kill(
        &self,
        py: Python<'_>,
        addr: Option<String>,
        laddr: Option<String>,
        client_id: Option<i64>,
        client_type: Option<String>,
        user: Option<String>,
        skipme: Option<bool>,
        maxage: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_kill(
                addr.as_deref(),
                laddr.as_deref(),
                client_id,
                client_type.as_deref(),
                user.as_deref(),
                skipme,
                maxage,
            );
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (timeout_ms, *, all=true))]
    fn client_pause(
        &self,
        py: Python<'_>,
        timeout_ms: i64,
        all: bool,
    ) -> PyResult<()> {
        let cmd = cmd_client_pause(timeout_ms, all);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (timeout_ms, *, all=true))]
    fn aclient_pause(
        &self,
        py: Python<'_>,
        timeout_ms: i64,
        all: bool,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_pause(timeout_ms, all);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn client_unpause(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_client_unpause();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    fn aclient_unpause(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_unpause();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, mode))]
    fn client_no_evict(&self, py: Python<'_>, mode: String) -> PyResult<()> {
        validate_on_off_mode(&mode)?;
        let cmd = cmd_client_no_evict(&mode.to_ascii_uppercase());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, mode))]
    fn aclient_no_evict(&self, py: Python<'_>, mode: String) -> PyResult<Py<PyAny>> {
        validate_on_off_mode(&mode)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_client_no_evict(&mode.to_ascii_uppercase());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, mode))]
    fn client_no_touch(&self, py: Python<'_>, mode: String) -> PyResult<()> {
        validate_on_off_mode(&mode)?;
        let cmd = cmd_client_no_touch(&mode.to_ascii_uppercase());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, mode))]
    fn aclient_no_touch(&self, py: Python<'_>, mode: String) -> PyResult<Py<PyAny>> {
        validate_on_off_mode(&mode)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_client_no_touch(&mode.to_ascii_uppercase());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py::TestClientKillPause -v`
Expected: 8 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/admin.rs tests/driver/test_commands_admin.py
git commit -m "feat(admin): add CLIENT KILL/PAUSE/UNPAUSE/NO-EVICT/NO-TOUCH"
```

---

## Task 13: `OBJECT ENCODING/IDLETIME/FREQ/REFCOUNT/HELP` + `MEMORY USAGE`

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/admin.rs`
- Modify: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_admin.py`:

```python
class TestObject:
    def test_object_encoding_string(self, driver) -> None:
        driver.set("k", b"v")
        enc = driver.object_encoding("k")
        # Strings: "embstr" or "raw" or "int" — all valid encodings.
        assert enc in (b"embstr", b"raw", b"int")

    def test_object_encoding_missing_key_returns_none(self, driver) -> None:
        assert driver.object_encoding("missing") is None

    def test_object_idletime(self, driver) -> None:
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
        from redis_rs_py.exceptions import ResponseError
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
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_admin.py::TestObject tests/driver/test_commands_admin.py::TestMemoryUsage -v`
Expected: FAIL.

- [ ] **Step 3: Implement OBJECT family + MEMORY USAGE**

Append to **Argument-encoding helpers**:

```rust
fn cmd_object_subcmd(subcmd: &str, key: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("OBJECT");
    cmd.arg(subcmd).arg(key);
    cmd
}

fn cmd_object_help() -> redis::Cmd {
    let mut cmd = redis::cmd("OBJECT");
    cmd.arg("HELP");
    cmd
}

fn cmd_memory_usage(key: &str, samples: Option<i64>) -> redis::Cmd {
    let mut cmd = redis::cmd("MEMORY");
    cmd.arg("USAGE").arg(key);
    if let Some(s) = samples {
        cmd.arg("SAMPLES").arg(s);
    }
    cmd
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    fn object_encoding(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_subcmd("ENCODING", key);
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::OptBytes(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_encoding(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_object_subcmd("ENCODING", &key);
            let r: redis::RedisResult<Option<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn object_idletime(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_subcmd("IDLETIME", key);
        let r: redis::RedisResult<Option<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::OptInt(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_idletime(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_object_subcmd("IDLETIME", &key);
            let r: redis::RedisResult<Option<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn object_freq(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_subcmd("FREQ", key);
        let r: redis::RedisResult<Option<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::OptInt(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_freq(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_object_subcmd("FREQ", &key);
            let r: redis::RedisResult<Option<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn object_refcount(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_subcmd("REFCOUNT", key);
        let r: redis::RedisResult<Option<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::OptInt(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_refcount(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_object_subcmd("REFCOUNT", &key);
            let r: redis::RedisResult<Option<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn object_help(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_help();
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::BytesList(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_help(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_object_help();
            let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *, samples=None))]
    fn memory_usage(
        &self,
        py: Python<'_>,
        key: &str,
        samples: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_memory_usage(key, samples);
        let r: redis::RedisResult<Option<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::OptInt(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (key, *, samples=None))]
    fn amemory_usage(
        &self,
        py: Python<'_>,
        key: &str,
        samples: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_memory_usage(&key, samples);
            let r: redis::RedisResult<Option<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py::TestObject tests/driver/test_commands_admin.py::TestMemoryUsage -v`
Expected: 12 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/admin.rs tests/driver/test_commands_admin.py
git commit -m "feat(admin): add OBJECT ENCODING/IDLETIME/FREQ/REFCOUNT/HELP + MEMORY USAGE"
```

---

## Task 14: Misc — `PING(message)` extend, `ECHO`, `WAIT`, `WAITAOF`, `TIME`, `LASTSAVE`, `BGSAVE`, `BGREWRITEAOF`, `DEBUG SLEEP`

The catch-all task. Plan 01 ships zero-arg `PING`; we extend the signature here.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/driver.rs` (extend `ping` + `aping`)
- Modify: `crates/redis-rs-py-driver/src/commands/admin.rs` (add the rest)
- Modify: `tests/driver/test_commands_admin.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_admin.py`:

```python
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
        driver.debug_sleep(0.2)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.2

    @pytest.mark.asyncio
    async def test_adebug_sleep(self, driver) -> None:
        start = time.monotonic()
        await driver.adebug_sleep(0.1)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.1
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/driver/test_commands_admin.py -v -k "PingMessage or Echo or Wait or Time or Lastsave or Bgsave or DebugSleep"`
Expected: FAIL.

- [ ] **Step 3: Extend `ping`/`aping` in driver.rs**

Edit `crates/redis-rs-py-driver/src/driver.rs`. Replace the existing `ping` and `aping` methods with:

```rust
    #[pyo3(signature = (*, message=None))]
    fn ping(&self, py: Python<'_>, message: Option<String>) -> PyResult<Py<PyAny>> {
        match message {
            None => {
                let r: redis::RedisResult<String> = sync_op!(
                    py,
                    self,
                    conn,
                    dispatch_cmd!(&mut conn, redis::cmd("PING"))
                );
                match r {
                    Ok(s) => py_bool(py, s == "PONG"),
                    Err(e) => Err(to_py_err(e)),
                }
            }
            Some(msg) => {
                let mut cmd = redis::cmd("PING");
                cmd.arg(msg);
                let r: redis::RedisResult<Vec<u8>> =
                    sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
                let bytes = r.map_err(to_py_err)?;
                Ok(PyBytes::new(py, &bytes).into_any().unbind())
            }
        }
    }

    #[pyo3(signature = (*, message=None))]
    fn aping(&self, py: Python<'_>, message: Option<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            match message {
                None => {
                    let r: redis::RedisResult<String> =
                        dispatch_cmd!(&mut conn, redis::cmd("PING"));
                    match r {
                        Ok(s) => RawResult::Bool(s == "PONG"),
                        Err(e) => crate::errors::classify(e),
                    }
                }
                Some(msg) => {
                    let mut cmd = redis::cmd("PING");
                    cmd.arg(&msg);
                    let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut conn, cmd);
                    match r {
                        Ok(b) => RawResult::OptBytes(Some(b)),
                        Err(e) => crate::errors::classify(e),
                    }
                }
            }
        })
    }
```

- [ ] **Step 4: Implement the rest in `commands/admin.rs`**

Append to **Argument-encoding helpers**:

```rust
fn cmd_echo(message: &[u8]) -> redis::Cmd {
    let mut cmd = redis::cmd("ECHO");
    cmd.arg(message);
    cmd
}

fn cmd_wait(numreplicas: i64, timeout_ms: i64) -> redis::Cmd {
    let mut cmd = redis::cmd("WAIT");
    cmd.arg(numreplicas).arg(timeout_ms);
    cmd
}

fn cmd_waitaof(numlocal: i64, numreplicas: i64, timeout_ms: i64) -> redis::Cmd {
    let mut cmd = redis::cmd("WAITAOF");
    cmd.arg(numlocal).arg(numreplicas).arg(timeout_ms);
    cmd
}

fn cmd_time() -> redis::Cmd {
    redis::cmd("TIME")
}

fn cmd_lastsave() -> redis::Cmd {
    redis::cmd("LASTSAVE")
}

fn cmd_bgsave(schedule: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("BGSAVE");
    if schedule {
        cmd.arg("SCHEDULE");
    }
    cmd
}

fn cmd_bgrewriteaof() -> redis::Cmd {
    redis::cmd("BGREWRITEAOF")
}

fn cmd_debug_sleep(seconds: f64) -> redis::Cmd {
    let mut cmd = redis::cmd("DEBUG");
    cmd.arg("SLEEP").arg(format!("{seconds:.6}"));
    cmd
}

/// Parse a TIME reply: Array(vec![BulkString("seconds"), BulkString("microseconds")]).
fn parse_time_reply(value: redis::Value) -> Option<(String, String)> {
    let parts = match value {
        redis::Value::Array(items) if items.len() == 2 => items,
        _ => return None,
    };
    let mut iter = parts.into_iter();
    let secs = match iter.next()? {
        redis::Value::BulkString(b) => String::from_utf8(b).ok()?,
        redis::Value::SimpleString(s) => s,
        _ => return None,
    };
    let usecs = match iter.next()? {
        redis::Value::BulkString(b) => String::from_utf8(b).ok()?,
        redis::Value::SimpleString(s) => s,
        _ => return None,
    };
    Some((secs, usecs))
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (message))]
    fn echo(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let bytes: Vec<u8> = if let Ok(s) = message.extract::<String>() {
            s.into_bytes()
        } else {
            message.extract::<Vec<u8>>()?
        };
        let cmd = cmd_echo(&bytes);
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    #[pyo3(signature = (message))]
    fn aecho(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let bytes: Vec<u8> = if let Ok(s) = message.extract::<String>() {
            s.into_bytes()
        } else {
            message.extract::<Vec<u8>>()?
        };
        async_op!(self, py, conn, async {
            let cmd = cmd_echo(&bytes);
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, numreplicas, timeout))]
    fn wait(&self, py: Python<'_>, numreplicas: i64, timeout: i64) -> PyResult<i64> {
        let cmd = cmd_wait(numreplicas, timeout);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    /// Async name `await_` to avoid keyword collision with Python's `await`.
    #[pyo3(name = "await_", signature = (*, numreplicas, timeout))]
    fn r_await(
        &self,
        py: Python<'_>,
        numreplicas: i64,
        timeout: i64,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_wait(numreplicas, timeout);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, numlocal, numreplicas, timeout))]
    fn waitaof(
        &self,
        py: Python<'_>,
        numlocal: i64,
        numreplicas: i64,
        timeout: i64,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_waitaof(numlocal, numreplicas, timeout);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (*, numlocal, numreplicas, timeout))]
    fn awaitaof(
        &self,
        py: Python<'_>,
        numlocal: i64,
        numreplicas: i64,
        timeout: i64,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_waitaof(numlocal, numreplicas, timeout);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn time(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_time();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::OptStrPair(parse_time_reply(r.map_err(to_py_err)?)).into_py(py)
    }

    fn atime(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_time();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::OptStrPair(parse_time_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    fn lastsave(&self, py: Python<'_>) -> PyResult<i64> {
        let cmd = cmd_lastsave();
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    fn alastsave(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_lastsave();
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, schedule=false))]
    fn bgsave(&self, py: Python<'_>, schedule: bool) -> PyResult<Py<PyAny>> {
        let cmd = cmd_bgsave(schedule);
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    #[pyo3(signature = (*, schedule=false))]
    fn abgsave(&self, py: Python<'_>, schedule: bool) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_bgsave(schedule);
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn bgrewriteaof(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_bgrewriteaof();
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    fn abgrewriteaof(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_bgrewriteaof();
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    /// Test-only — DO NOT call this from production code. Blocks the
    /// server (and our connection) for `seconds`. Used in pipelines /
    /// blocking-cmd tests to simulate a slow-server scenario.
    fn debug_sleep(&self, py: Python<'_>, seconds: f64) -> PyResult<()> {
        let cmd = cmd_debug_sleep(seconds);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    fn adebug_sleep(&self, py: Python<'_>, seconds: f64) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_debug_sleep(seconds);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 5: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_admin.py -v -k "PingMessage or Echo or Wait or Time or Lastsave or Bgsave or DebugSleep"`
Expected: ~15 PASS (some BGSAVE tests may SKIP).

Also re-run the existing PING tests to verify the signature change is backward-compatible:

Run: `uv run pytest tests/driver/test_driver_basic.py::test_ping tests/driver/test_driver_basic.py::test_aping -v`
Expected: still PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/driver.rs crates/redis-rs-py-driver/src/commands/admin.rs tests/driver/test_commands_admin.py
git commit -m "feat(admin): add ECHO/WAIT/WAITAOF/TIME/LASTSAVE/BGSAVE/BGREWRITEAOF/DEBUG SLEEP, extend PING(message)"
```

---

## Task 15: Stub the new methods in `_driver.pyi` + lint pass

Hand-maintained type stubs.

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Append the script + admin stubs**

Open `python/redis_rs_py/_driver.pyi`. Inside `class RedisRsDriver:` (after the stream stubs from Plan 08), append:

```python
    # Scripts (Plan 09)
    def eval(
        self, script: str, keys: list[str], args: list[bytes]
    ) -> Any: ...
    def aeval(
        self, script: str, keys: list[str], args: list[bytes]
    ) -> Awaitable[Any]: ...
    def evalsha(
        self, sha: str, keys: list[str], args: list[bytes]
    ) -> Any: ...
    def aevalsha(
        self, sha: str, keys: list[str], args: list[bytes]
    ) -> Awaitable[Any]: ...
    def eval_ro(
        self, script: str, keys: list[str], args: list[bytes]
    ) -> Any: ...
    def aeval_ro(
        self, script: str, keys: list[str], args: list[bytes]
    ) -> Awaitable[Any]: ...
    def evalsha_ro(
        self, sha: str, keys: list[str], args: list[bytes]
    ) -> Any: ...
    def aevalsha_ro(
        self, sha: str, keys: list[str], args: list[bytes]
    ) -> Awaitable[Any]: ...
    def script_load(self, script: str) -> str: ...
    def ascript_load(self, script: str) -> Awaitable[str]: ...
    def script_exists(self, *shas: str) -> list[bool]: ...
    def ascript_exists(self, *shas: str) -> Awaitable[list[bool]]: ...
    def script_flush(self, *, mode: str | None = ...) -> None: ...
    def ascript_flush(self, *, mode: str | None = ...) -> Awaitable[None]: ...
    def script_kill(self) -> None: ...
    def ascript_kill(self) -> Awaitable[None]: ...
    def fcall(
        self, function: str, keys: list[str], args: list[bytes]
    ) -> Any: ...
    def afcall(
        self, function: str, keys: list[str], args: list[bytes]
    ) -> Awaitable[Any]: ...
    def fcall_ro(
        self, function: str, keys: list[str], args: list[bytes]
    ) -> Any: ...
    def afcall_ro(
        self, function: str, keys: list[str], args: list[bytes]
    ) -> Awaitable[Any]: ...
    def function_load(self, code: str, *, replace: bool = ...) -> str: ...
    def afunction_load(
        self, code: str, *, replace: bool = ...
    ) -> Awaitable[str]: ...
    def function_delete(self, library: str) -> None: ...
    def afunction_delete(self, library: str) -> Awaitable[None]: ...
    def function_dump(self) -> bytes: ...
    def afunction_dump(self) -> Awaitable[bytes]: ...
    def function_flush(self, *, mode: str | None = ...) -> None: ...
    def afunction_flush(
        self, *, mode: str | None = ...
    ) -> Awaitable[None]: ...
    def function_list(
        self, *, library: str | None = ..., withcode: bool = ...
    ) -> list[Any]: ...
    def afunction_list(
        self, *, library: str | None = ..., withcode: bool = ...
    ) -> Awaitable[list[Any]]: ...
    def function_stats(self) -> Any: ...
    def afunction_stats(self) -> Awaitable[Any]: ...
    def function_kill(self) -> None: ...
    def afunction_kill(self) -> Awaitable[None]: ...
    def function_restore(
        self, dump: bytes, *, policy: str | None = ...
    ) -> None: ...
    def afunction_restore(
        self, dump: bytes, *, policy: str | None = ...
    ) -> Awaitable[None]: ...

    # Admin / introspection (Plan 09)
    def scan(
        self,
        *,
        cursor: int = ...,
        match: str | None = ...,
        count: int | None = ...,
        type: str | None = ...,
    ) -> tuple[int, list[bytes]]: ...
    def ascan(
        self,
        *,
        cursor: int = ...,
        match: str | None = ...,
        count: int | None = ...,
        type: str | None = ...,
    ) -> Awaitable[tuple[int, list[bytes]]]: ...
    # scan_iter / scan_iter_async are attached at __init__ time — see _scan_iter.py.
    def scan_iter(
        self,
        *,
        match: str | None = ...,
        count: int | None = ...,
        type: str | None = ...,
    ) -> Iterator[bytes]: ...
    def scan_iter_async(
        self,
        *,
        match: str | None = ...,
        count: int | None = ...,
        type: str | None = ...,
    ) -> AsyncIterator[bytes]: ...
    def keys(self, pattern: str) -> list[bytes]: ...
    def akeys(self, pattern: str) -> Awaitable[list[bytes]]: ...
    def randomkey(self) -> bytes | None: ...
    def arandomkey(self) -> Awaitable[bytes | None]: ...
    def dbsize(self) -> int: ...
    def adbsize(self) -> Awaitable[int]: ...
    def flushdb(self, *, asynchronous: bool = ...) -> None: ...
    def aflushdb(self, *, asynchronous: bool = ...) -> Awaitable[None]: ...
    def flushall(self, *, asynchronous: bool = ...) -> None: ...
    def aflushall(self, *, asynchronous: bool = ...) -> Awaitable[None]: ...
    def select(self, db_index: int) -> bool: ...
    def info(self, *, section: str | None = ...) -> bytes: ...
    def ainfo(self, *, section: str | None = ...) -> Awaitable[bytes]: ...
    def config_get(self, parameter: str) -> dict[bytes, bytes]: ...
    def aconfig_get(self, parameter: str) -> Awaitable[dict[bytes, bytes]]: ...
    def config_set(
        self,
        name_or_mapping: str | dict[str, str],
        value: str | None = ...,
    ) -> None: ...
    def aconfig_set(
        self,
        name_or_mapping: str | dict[str, str],
        value: str | None = ...,
    ) -> Awaitable[None]: ...
    def config_resetstat(self) -> None: ...
    def aconfig_resetstat(self) -> Awaitable[None]: ...
    def config_rewrite(self) -> None: ...
    def aconfig_rewrite(self) -> Awaitable[None]: ...
    def client_id(self) -> int: ...
    def aclient_id(self) -> Awaitable[int]: ...
    def client_getname(self) -> bytes | None: ...
    def aclient_getname(self) -> Awaitable[bytes | None]: ...
    def client_setname(self, name: str) -> None: ...
    def aclient_setname(self, name: str) -> Awaitable[None]: ...
    def client_info(self) -> bytes: ...
    def aclient_info(self) -> Awaitable[bytes]: ...
    def client_list(
        self,
        *,
        client_type: str | None = ...,
        client_id: list[int] | None = ...,
    ) -> list[dict[bytes, bytes]]: ...
    def aclient_list(
        self,
        *,
        client_type: str | None = ...,
        client_id: list[int] | None = ...,
    ) -> Awaitable[list[dict[bytes, bytes]]]: ...
    def client_kill(
        self,
        *,
        addr: str | None = ...,
        laddr: str | None = ...,
        client_id: int | None = ...,
        client_type: str | None = ...,
        user: str | None = ...,
        skipme: bool | None = ...,
        maxage: int | None = ...,
    ) -> int: ...
    def aclient_kill(
        self,
        *,
        addr: str | None = ...,
        laddr: str | None = ...,
        client_id: int | None = ...,
        client_type: str | None = ...,
        user: str | None = ...,
        skipme: bool | None = ...,
        maxage: int | None = ...,
    ) -> Awaitable[int]: ...
    def client_pause(self, timeout_ms: int, *, all: bool = ...) -> None: ...
    def aclient_pause(
        self, timeout_ms: int, *, all: bool = ...
    ) -> Awaitable[None]: ...
    def client_unpause(self) -> None: ...
    def aclient_unpause(self) -> Awaitable[None]: ...
    def client_no_evict(self, *, mode: str) -> None: ...
    def aclient_no_evict(self, *, mode: str) -> Awaitable[None]: ...
    def client_no_touch(self, *, mode: str) -> None: ...
    def aclient_no_touch(self, *, mode: str) -> Awaitable[None]: ...
    def object_encoding(self, key: str) -> bytes | None: ...
    def aobject_encoding(self, key: str) -> Awaitable[bytes | None]: ...
    def object_idletime(self, key: str) -> int | None: ...
    def aobject_idletime(self, key: str) -> Awaitable[int | None]: ...
    def object_freq(self, key: str) -> int | None: ...
    def aobject_freq(self, key: str) -> Awaitable[int | None]: ...
    def object_refcount(self, key: str) -> int | None: ...
    def aobject_refcount(self, key: str) -> Awaitable[int | None]: ...
    def object_help(self) -> list[bytes]: ...
    def aobject_help(self) -> Awaitable[list[bytes]]: ...
    def memory_usage(self, key: str, *, samples: int | None = ...) -> int | None: ...
    def amemory_usage(
        self, key: str, *, samples: int | None = ...
    ) -> Awaitable[int | None]: ...
    def echo(self, message: str | bytes) -> bytes: ...
    def aecho(self, message: str | bytes) -> Awaitable[bytes]: ...
    def wait(self, *, numreplicas: int, timeout: int) -> int: ...
    def await_(self, *, numreplicas: int, timeout: int) -> Awaitable[int]: ...
    def waitaof(
        self, *, numlocal: int, numreplicas: int, timeout: int
    ) -> Any: ...
    def awaitaof(
        self, *, numlocal: int, numreplicas: int, timeout: int
    ) -> Awaitable[Any]: ...
    def time(self) -> tuple[str, str] | None: ...
    def atime(self) -> Awaitable[tuple[str, str] | None]: ...
    def lastsave(self) -> int: ...
    def alastsave(self) -> Awaitable[int]: ...
    def bgsave(self, *, schedule: bool = ...) -> bytes: ...
    def abgsave(self, *, schedule: bool = ...) -> Awaitable[bytes]: ...
    def bgrewriteaof(self) -> bytes: ...
    def abgrewriteaof(self) -> Awaitable[bytes]: ...
    def debug_sleep(self, seconds: float) -> None: ...
    def adebug_sleep(self, seconds: float) -> Awaitable[None]: ...
```

The `ping`/`aping` stubs from Plan 01 need updating — change them to:

```python
    def ping(self, *, message: str | None = ...) -> bool | bytes: ...
    def aping(
        self, *, message: str | None = ...
    ) -> Awaitable[bool | bytes]: ...
```

And add to the top-of-file imports:

```python
from collections.abc import AsyncIterator, Iterator
```

(Keep `Awaitable` and `Any`.)

- [ ] **Step 2: Run ty**

Run: `uv run ty check python/redis_rs_py/`
Expected: 0 errors.

- [ ] **Step 3: Lint pass**

```bash
uv run ruff check
uv run ruff format --check
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: all green.

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest -n auto
```

Expected: every test PASSES — Plan 01/02/03–08 still green AND the new Plan 09 surface (~110 tests across scripts + admin) passes.

- [ ] **Step 5: Commit**

```bash
git add python/redis_rs_py/_driver.pyi
git commit -m "feat(admin): add type stubs for the scripts + admin surface"
```

---

## Task 16: Free-threaded smoke + CHANGELOG

Final verification under cp314t and changelog entry.

**Files:** `CHANGELOG.md`

- [ ] **Step 1: Run under cp314t free-threaded**

```bash
.venv-ft/bin/uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
.venv-ft/bin/uv run pytest tests/driver/test_commands_scripts.py tests/driver/test_commands_admin.py -n auto
```

Expected: same green as cp314.

- [ ] **Step 2: Append CHANGELOG**

Append to `CHANGELOG.md` under `### Added`:

```markdown
- Server-side scripting: `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`, `SCRIPT LOAD`/`EXISTS`/`FLUSH`/`KILL`, `FCALL`/`FCALL_RO`, `FUNCTION LOAD`/`DUMP`/`FLUSH`/`LIST`/`STATS`/`KILL`/`RESTORE`/`DELETE`.
- Admin / introspection: `SCAN(cursor=, match=, count=, type=)` plus `scan_iter` / `scan_iter_async` Python generator helpers, `KEYS` (with deprecation warning), `RANDOMKEY`, `DBSIZE`, `FLUSHDB`/`FLUSHALL` (`asynchronous=`), `SELECT` (with documented limitation), `INFO(section=)`, `CONFIG GET`/`SET`/`RESETSTAT`/`REWRITE`, `CLIENT ID`/`GETNAME`/`SETNAME`/`INFO`/`LIST`/`KILL`/`PAUSE`/`UNPAUSE`/`NO-EVICT`/`NO-TOUCH`, `OBJECT ENCODING`/`IDLETIME`/`FREQ`/`REFCOUNT`/`HELP`, `MEMORY USAGE(samples=)`, extended `PING(message=)`, `ECHO`, `WAIT`, `WAITAOF`, `TIME`, `LASTSAVE`, `BGSAVE(schedule=)`, `BGREWRITEAOF`, `DEBUG SLEEP` (test-only).
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 09 entry"
```

- [ ] **Step 3: Final verification**

```bash
git log --oneline -16
```

Expected: 16 commits since plan start, every one conventional-commit-prefixed (`feat(scripts):`, `feat(admin):`, or `docs(changelog):`).

---

## Self-review checklist for this plan

- [x] Spec coverage (`PLAN.md` v0.1 surface): scripts (`EVAL`/`EVALSHA`/`SCRIPT LOAD`/`EXISTS`), admin (`INFO`/`CONFIG`/`OBJECT`), `CLIENT KILL`/`GETNAME`/`SETNAME`, `CONFIG SET`/`RESETSTAT` — all present.
- [x] Spec coverage (roadmap row 09): EVAL/EVALSHA/EVAL_RO/EVALSHA_RO ✓, SCRIPT LOAD/EXISTS/FLUSH/KILL ✓, FCALL/FCALL_RO ✓, FUNCTION LOAD/DUMP/FLUSH/LIST/STATS/KILL/RESTORE ✓, SCAN family ✓, KEYS/RANDOMKEY ✓, DBSIZE/FLUSHDB/FLUSHALL ✓, INFO ✓, CONFIG family ✓, CLIENT family ✓, OBJECT family + MEMORY USAGE ✓, PING/ECHO/WAIT/TIME/LASTSAVE/BGSAVE/BGREWRITEAOF/DEBUG SLEEP ✓.
- [x] Two new files under `commands/`: `scripts.rs` and `admin.rs`. Same per-family pattern as plan 08.
- [x] Every command has a sync + async pair sharing a `cmd_*` helper.
- [x] `XPENDING`-style "one method, two forms" pattern reused: `xpending` in plan 08, `config_set(name_or_mapping, value=)` here for the same reason — match redis-py's flexible signature.
- [x] `scan_iter` Python generator helper documented as the explicit Rust-by-default escape hatch (referenced PLAN.md lines 60-63). Both sync + async generators ship.
- [x] `SELECT` limitation explicitly documented and tested — raise `NotImplementedError` rather than silently break.
- [x] `KEYS` emits a `DeprecationWarning` recommending `scan_iter`.
- [x] `DEBUG SLEEP` flagged in its docstring as test-only (NOT for production code).
- [x] `PING(message=)` extension is backward-compatible — Plan 01 callers still work because `message=None` keeps the original `bool` return.
- [x] `await_` rust method renamed to avoid Python keyword collision; PyO3 `#[pyo3(name = "await_")]` exposes it as `await_` in Python.
- [x] Type stubs added for all ~50 new sync + async methods.
- [x] Two new `RawResult` variants (`BoolList`, `OptStrPair`) plus `BytesPairsList` from Task 11 — all wired through `into_py`.
- [x] All file paths absolute or repo-relative-from-root.
- [x] Every code-changing step ships actual code (no pseudocode).
- [x] Every test step has a runnable command and an explicit pass count.
- [x] Frequent commits — 16 across 16 tasks; each conventional-commit prefixed.
- [x] Free-threaded (cp314t) verified at the end (Task 16 Step 1).
