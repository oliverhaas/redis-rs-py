# Plan 03 — String / key commands

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the full string-command surface on `RedisRsDriver`. Every command exists as a sync + async pair (`<cmd>` sync, `a<cmd>` async), with the exact signatures `redis-py` users expect (`set(name, value, ex=None, px=None, nx=False, xx=False, keepttl=False, get=False, exat=None, pxat=None)` etc.). The driver gains 50+ new methods covering: the full `SET` option matrix, `GET`/`GETEX`/`GETDEL`/`GETRANGE`/`SETRANGE`/`STRLEN`/`APPEND`, `MGET`/`MSET`/`MSETNX`, `INCR`/`INCRBY`/`INCRBYFLOAT`/`DECR`/`DECRBY`, `EXISTS`/`DEL`/`UNLINK`, the EXPIRE family + `TTL`/`PTTL`/`PERSIST`/`EXPIRETIME`/`PEXPIRETIME`, `RENAME`/`RENAMENX`/`TYPE`, `COPY`, `DUMP`/`RESTORE`.

**Architecture:** Each command lives in `crates/redis-rs-py-driver/src/commands/strings.rs` (new file; `driver.rs` is already at ~280 lines after plan 01 and would balloon past 800 if we kept all commands inline). The file holds a `#[pymethods] impl RedisRsDriver` block — PyO3 supports multiple `#[pymethods]` blocks per class as long as method names don't collide. The four canonical commands (`get`/`set`/`delete`/`ping`) stay in `driver.rs`; everything else moves to per-family files starting here.

Every command body uses the `async_op!` and `sync_op!` macros from `driver.rs`, plus `dispatch_cmd!` from `connection.rs`. Bodies build a `redis::Cmd` by hand when an option matrix or variadic tail is involved (every command in this plan except a handful that can ride `redis::AsyncCommands` directly). Connection-level helper methods land on `ValkeyConnInner` in `connection.rs`; the `RedisRsDriver` methods are thin wrappers that arg-marshal and call the helper.

**Tech Stack:** PyO3 0.28, redis 1.x (`AsyncCommands`, `cmd`, `Value`, `from_redis_value`), already in `Cargo.toml` from plan 01. No new dependencies.

**Reference material:**
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs:447-756` — every command in this plan has a working analogue here. The cachex `set_with_flags` (`client.rs:529`) is the prototype for our full SET matrix; we generalise its kwarg surface.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs:244-460` — connection-level helpers (`set_bytes`, `set_with_flags`, `expire`, `pexpire`, `key_type`, etc.). We port these into the new repo's `connection.rs` with the Standard-only single-arm `dispatch_cmd!` from plan 01.
- `redis-py` source: `redis/commands/core.py::SET` — the canonical signature we mirror. Verify with `python -c "import inspect, redis; print(inspect.signature(redis.Redis.set))"` before starting.
- `redis-rs` 1.x docs for `redis::cmd("...").arg(...).query_async(c)` — the by-hand command-construction path used throughout.

**Out of scope:** Bitstring commands (`BITCOUNT`/`BITOP`/`BITPOS`/`SETBIT`/`GETBIT` — defer to a v0.2 plan), `SUBSTR` (deprecated alias for `GETRANGE`), `PSETEX`/`SETEX` legacy (subsumed by the SET matrix). `OBJECT ENCODING`/`MEMORY USAGE` — those land in plan 09 (admin). `WAIT`/`WAITAOF` — plan 09.

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  lib.rs                       # MODIFIED: register the new commands::strings module
  driver.rs                    # UNCHANGED — canonical 4 commands stay
  connection.rs                # MODIFIED: ValkeyConnInner gains string helper methods
  commands/
    mod.rs                     # NEW: declares `pub mod strings;`
    strings.rs                 # NEW: #[pymethods] impl RedisRsDriver block for strings
python/redis_rs_py/
  _driver.pyi                  # MODIFIED: append signatures for every new command
tests/driver/
  test_commands_strings.py     # NEW: covers every command in this plan
```

---

## Task 1: Wire up the `commands` module hierarchy

Before any sub-task touches a real command, lay the file scaffolding.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/lib.rs`
- Create: `crates/redis-rs-py-driver/src/commands/mod.rs`
- Create: `crates/redis-rs-py-driver/src/commands/strings.rs`

- [ ] **Step 1: Add the module hierarchy**

In `crates/redis-rs-py-driver/src/lib.rs`, add `mod commands;` next to the other `mod` declarations:

```rust
mod async_bridge;
mod commands;
mod connection;
mod driver;
mod errors;
mod exceptions;
mod raw_result;
mod runtime;
mod test_helpers;
```

(Order alphabetic — keep the file tidy.)

- [ ] **Step 2: Create `commands/mod.rs`**

```rust
// Per-family command modules.
//
// Each file holds a `#[pymethods] impl RedisRsDriver` block adding that
// family's commands. PyO3 0.28 supports multiple `#[pymethods]` blocks
// per class as long as method names are unique across blocks.
//
// New families append a `pub mod <family>;` line below.

pub mod strings;
```

- [ ] **Step 3: Create the empty `commands/strings.rs` placeholder**

```rust
// String / key commands.
//
// Every method exists as a sync + async pair:
//   * `<cmd>(...)` — sync; releases the GIL via py.detach.
//   * `a<cmd>(...)` — async; returns a RedisRsAwaitable.
//
// Shared helpers live in driver.rs (macros) and connection.rs
// (per-command async fns on ValkeyConnInner).

use pyo3::prelude::*;

use crate::driver::RedisRsDriver;

#[pymethods]
impl RedisRsDriver {}
```

- [ ] **Step 4: Verify the crate still compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with one warning about the empty `impl` block. No errors.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/lib.rs crates/redis-rs-py-driver/src/commands/
git commit -m "refactor(driver): scaffold per-family commands module"
```

---

## Task 2: Sub-family A — Full `SET` option matrix

Extend `RedisRsDriver` with `set(...)` and `aset(...)` that take the full redis-py kwarg surface. The plan-01 `set` is overwritten in place — its 2-arg signature is a strict subset of the new surface, so existing `driver.set(key, value)` callers stay green.

The `redis-py` signature we mirror (verified via `inspect.signature(redis.Redis.set)`):

```python
def set(
    self, name, value,
    ex=None,        # int | timedelta | None — TTL seconds
    px=None,        # int | timedelta | None — TTL millis
    nx=False,       # only set if NOT exists
    xx=False,       # only set if exists
    keepttl=False,  # retain existing TTL
    get=False,      # return previous value
    exat=None,      # absolute expiry, seconds
    pxat=None,      # absolute expiry, millis
) -> bool | bytes | None:
    ...
```

Return contract:
- `get=False` and write succeeds → `True`.
- `get=False` and `nx`/`xx` predicate skipped the write → `None`.
- `get=True` and key existed → previous bytes value.
- `get=True` and key didn't exist → `None`.
- Mutually exclusive opts (`ex` + `px`, `keepttl` + `ex|px|exat|pxat`, `nx` + `xx`) → raise `DataError`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs` (add `set_full` helper on `ValkeyConnInner`)
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs` (add `set` + `aset`)
- Modify: `crates/redis-rs-py-driver/src/driver.rs` (delete the placeholder `set` + `aset`; they're moving to `strings.rs`)
- Test: `tests/driver/test_commands_strings.py`

- [ ] **Step 1: Write the failing test**

Create `tests/driver/test_commands_strings.py`:

```python
"""SET option-matrix tests."""

from __future__ import annotations

import pytest

from redis_rs_py.exceptions import DataError, ResponseError


def test_set_basic(driver) -> None:
    assert driver.set("k", b"v") is True
    assert driver.get("k") == b"v"


def test_set_with_ex(driver) -> None:
    assert driver.set("k", b"v", ex=60) is True
    # Round-trip TTL via the sync TTL path (lands later in this plan; for
    # now use the upstream client to verify).
    import redis as upstream

    rp = upstream.Redis.from_url(driver.connection_url)
    assert 0 < rp.ttl("k") <= 60
    rp.close()


def test_set_with_px(driver) -> None:
    assert driver.set("k", b"v", px=60_000) is True
    import redis as upstream

    rp = upstream.Redis.from_url(driver.connection_url)
    assert 0 < rp.pttl("k") <= 60_000
    rp.close()


def test_set_nx_when_missing(driver) -> None:
    assert driver.set("k", b"v", nx=True) is True
    assert driver.get("k") == b"v"


def test_set_nx_when_present_returns_none(driver) -> None:
    driver.set("k", b"old")
    assert driver.set("k", b"new", nx=True) is None
    assert driver.get("k") == b"old"


def test_set_xx_when_missing_returns_none(driver) -> None:
    assert driver.set("k", b"v", xx=True) is None
    assert driver.get("k") is None


def test_set_xx_when_present(driver) -> None:
    driver.set("k", b"old")
    assert driver.set("k", b"new", xx=True) is True
    assert driver.get("k") == b"new"


def test_set_get_true_with_previous(driver) -> None:
    driver.set("k", b"old")
    assert driver.set("k", b"new", get=True) == b"old"
    assert driver.get("k") == b"new"


def test_set_get_true_without_previous_returns_none(driver) -> None:
    assert driver.set("k", b"v", get=True) is None
    assert driver.get("k") == b"v"


def test_set_keepttl(driver) -> None:
    driver.set("k", b"v", ex=60)
    driver.set("k", b"v2", keepttl=True)
    import redis as upstream

    rp = upstream.Redis.from_url(driver.connection_url)
    assert 0 < rp.ttl("k") <= 60
    rp.close()


def test_set_exat(driver) -> None:
    import time

    deadline = int(time.time()) + 30
    assert driver.set("k", b"v", exat=deadline) is True
    import redis as upstream

    rp = upstream.Redis.from_url(driver.connection_url)
    assert 0 < rp.ttl("k") <= 30
    rp.close()


def test_set_pxat(driver) -> None:
    import time

    deadline_ms = int(time.time() * 1000) + 30_000
    assert driver.set("k", b"v", pxat=deadline_ms) is True


def test_set_nx_and_xx_raises(driver) -> None:
    with pytest.raises(DataError, match="nx and xx"):
        driver.set("k", b"v", nx=True, xx=True)


def test_set_ex_and_px_raises(driver) -> None:
    with pytest.raises(DataError, match="ex.*px"):
        driver.set("k", b"v", ex=10, px=10_000)


def test_set_keepttl_with_ex_raises(driver) -> None:
    with pytest.raises(DataError, match="keepttl"):
        driver.set("k", b"v", ex=10, keepttl=True)


@pytest.mark.asyncio
async def test_aset_full_matrix(driver) -> None:
    assert await driver.aset("k", b"v") is True
    assert await driver.aset("k", b"v2", xx=True) is True
    assert await driver.aset("k", b"v3", nx=True) is None
    assert await driver.aset("k", b"v4", get=True) == b"v2"


@pytest.mark.asyncio
async def test_aset_invalid_kwargs_raises(driver) -> None:
    with pytest.raises(DataError):
        await driver.aset("k", b"v", nx=True, xx=True)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/driver/test_commands_strings.py -v`
Expected: FAIL — every test errors with `TypeError: set() got an unexpected keyword argument 'ex'` or similar (the plan-01 `set` only accepts `ttl`).

- [ ] **Step 3: Add `set_full` helper on `ValkeyConnInner`**

Open `crates/redis-rs-py-driver/src/connection.rs` and add after the existing `impl ValkeyConnInner` block (or merge into the existing one):

```rust
impl ValkeyConnInner {
    /// Build and dispatch a SET with the full redis-py option matrix.
    ///
    /// Returns `redis::Value` so the caller can distinguish:
    ///   * `Okay` / `SimpleString("OK")` → write happened, no GET requested → True
    ///   * `Nil` → write skipped (NX/XX predicate failed) OR GET on missing key
    ///   * `BulkString(b)` → previous value (only when GET=true)
    pub async fn set_full(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
    ) -> redis::RedisResult<redis::Value> {
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(value.as_slice());
        if let Some(s) = ex {
            cmd.arg("EX").arg(s);
        }
        if let Some(ms) = px {
            cmd.arg("PX").arg(ms);
        }
        if let Some(ts) = exat {
            cmd.arg("EXAT").arg(ts);
        }
        if let Some(ts) = pxat {
            cmd.arg("PXAT").arg(ts);
        }
        if keepttl {
            cmd.arg("KEEPTTL");
        }
        if nx {
            cmd.arg("NX");
        }
        if xx {
            cmd.arg("XX");
        }
        if get {
            cmd.arg("GET");
        }
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Replace the placeholder `set`/`aset` in `driver.rs`**

Find the `set` and `aset` methods that landed in plan 01 (signature `(key, value, ttl=None)`). Delete them — they're being supplanted by the full-matrix versions in `commands/strings.rs`.

- [ ] **Step 5: Implement `set` + `aset` in `commands/strings.rs`**

Replace `crates/redis-rs-py-driver/src/commands/strings.rs`:

```rust
// String / key commands.
//
// Every method exists as a sync + async pair:
//   * `<cmd>(...)` — sync; releases the GIL via py.detach.
//   * `a<cmd>(...)` — async; returns a RedisRsAwaitable.
//
// Bodies use the async_op!/sync_op! macros from driver.rs and the
// per-command async fns on ValkeyConnInner from connection.rs.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::async_bridge::RawResult;
use crate::driver::{RedisRsDriver, py_bool, py_int, py_opt_bytes};
use crate::errors::{classify_error, to_py_err};
use crate::exceptions::{DataError, ExceptionClass};
use crate::raw_result::IntoRawResult;
use crate::{async_op, conn_method, dispatch_cmd, sync_op};

// =========================================================================
// Validation helpers shared by sync + async SET paths.
// =========================================================================

fn validate_set_kwargs(
    ex: Option<u64>,
    px: Option<u64>,
    exat: Option<i64>,
    pxat: Option<i64>,
    nx: bool,
    xx: bool,
    keepttl: bool,
) -> PyResult<()> {
    if nx && xx {
        return Err(PyErr::new::<DataError, _>(
            "nx and xx options are mutually exclusive",
        ));
    }
    let ttl_set = [ex.is_some(), px.is_some(), exat.is_some(), pxat.is_some()]
        .into_iter()
        .filter(|b| *b)
        .count();
    if ttl_set > 1 {
        return Err(PyErr::new::<DataError, _>(
            "only one of ex, px, exat, pxat may be set",
        ));
    }
    if keepttl && ttl_set > 0 {
        return Err(PyErr::new::<DataError, _>(
            "keepttl is mutually exclusive with ex/px/exat/pxat",
        ));
    }
    Ok(())
}

fn set_value_to_py(py: Python<'_>, v: redis::Value, get: bool) -> PyResult<Py<PyAny>> {
    match v {
        // Plain SET succeeded — `OK` / `SimpleString("OK")`.
        redis::Value::Okay => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::SimpleString(s) if s == "OK" => {
            Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
        }
        // Either the NX/XX predicate failed, or GET on a missing key.
        redis::Value::Nil => Ok(py.None()),
        // GET=true and key existed → previous bytes.
        redis::Value::BulkString(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        // Defensive: any other shape we treat as "OK" if we expected a write,
        // or "None" if we expected a GET. Should not happen with current Redis.
        _ if get => Ok(py.None()),
        _ => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
    }
}

#[pymethods]
impl RedisRsDriver {
    // ----- SET / aset -----------------------------------------------------

    #[pyo3(signature = (
        name,
        value,
        *,
        ex = None,
        px = None,
        nx = false,
        xx = false,
        keepttl = false,
        get = false,
        exat = None,
        pxat = None,
    ))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn set(
        &self,
        py: Python<'_>,
        name: &str,
        value: &[u8],
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
        exat: Option<i64>,
        pxat: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        validate_set_kwargs(ex, px, exat, pxat, nx, xx, keepttl)?;
        let value = value.to_vec();
        let r: redis::RedisResult<redis::Value> = sync_op!(py, self, conn, async {
            conn.set_full(name, value, ex, px, exat, pxat, nx, xx, keepttl, get)
                .await
        });
        let v = r.map_err(to_py_err)?;
        set_value_to_py(py, v, get)
    }

    #[pyo3(signature = (
        name,
        value,
        *,
        ex = None,
        px = None,
        nx = false,
        xx = false,
        keepttl = false,
        get = false,
        exat = None,
        pxat = None,
    ))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn aset(
        &self,
        py: Python<'_>,
        name: &str,
        value: &[u8],
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
        exat: Option<i64>,
        pxat: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        validate_set_kwargs(ex, px, exat, pxat, nx, xx, keepttl)?;
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<redis::Value> = conn
                .set_full(&name, value, ex, px, exat, pxat, nx, xx, keepttl, get)
                .await;
            match r {
                Ok(v) => RawResult::Value(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            }
        })
    }
}
```

(The `RawResult::Value` arm reaches the awaitable path which calls `redis_value_to_py` — but we need to special-case "OK"→True the same way the sync path does. Easiest: add a `RawResult::SetReply { value, get_requested }` variant. Cleaner: convert to a Python value here in the tokio task. But the tokio task can't touch the GIL. Solution: synthesise a `RawResult::Bool(true)` / `RawResult::OptBytes(Some(b))` / `RawResult::Nil` in the tokio task itself based on the redis::Value shape.)

Replace the async body's match with:

```rust
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<redis::Value> = conn
                .set_full(&name, value, ex, px, exat, pxat, nx, xx, keepttl, get)
                .await;
            match r {
                Ok(redis::Value::Okay) => RawResult::Bool(true),
                Ok(redis::Value::SimpleString(ref s)) if s == "OK" => RawResult::Bool(true),
                Ok(redis::Value::Nil) => RawResult::Nil,
                Ok(redis::Value::BulkString(b)) => RawResult::OptBytes(Some(b)),
                Ok(_) if get => RawResult::Nil,
                Ok(_) => RawResult::Bool(true),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            }
        })
```

(`PyString` and the unused `IntoRawResult` import will warn — leave both; subsequent sub-tasks consume them.)

- [ ] **Step 6: Build + run the tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v`
Expected: 16 PASS (all tests in the file so far). The `test_set_with_ex/px/exat/pxat/keepttl` tests rely on the upstream redis-py client for TTL inspection — that's fine; we land our own `ttl`/`pttl` later in this plan.

- [ ] **Step 7: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/driver.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add SET full option matrix"
```

---

## Task 3: Sub-family B — `GET` family + `APPEND` / `STRLEN` / `GETRANGE` / `SETRANGE`

`GET` already exists in `driver.rs` (plan 01). This sub-task adds `GETEX`, `GETDEL`, `GETRANGE`, `SETRANGE`, `STRLEN`, `APPEND`. `GET` is left in `driver.rs`.

`GETEX` accepts the same TTL kwargs as SET (`ex`/`px`/`exat`/`pxat`/`persist`). `GETDEL` is no-args. `GETRANGE`/`SETRANGE` take byte offsets. `STRLEN`/`APPEND` are straightforward.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs`
- Test: `tests/driver/test_commands_strings.py` (append)

- [ ] **Step 1: Append the failing tests**

Append to `tests/driver/test_commands_strings.py`:

```python
# ---------- GET family ----------


def test_getex_with_ex(driver) -> None:
    driver.set("k", b"v")
    assert driver.getex("k", ex=60) == b"v"
    import redis as upstream

    rp = upstream.Redis.from_url(driver.connection_url)
    assert 0 < rp.ttl("k") <= 60
    rp.close()


def test_getex_with_persist(driver) -> None:
    driver.set("k", b"v", ex=60)
    assert driver.getex("k", persist=True) == b"v"
    import redis as upstream

    rp = upstream.Redis.from_url(driver.connection_url)
    assert rp.ttl("k") == -1  # no TTL
    rp.close()


def test_getex_missing_returns_none(driver) -> None:
    assert driver.getex("missing") is None


def test_getex_invalid_kwargs_raises(driver) -> None:
    with pytest.raises(DataError):
        driver.getex("k", ex=10, px=10_000)


def test_getdel(driver) -> None:
    driver.set("k", b"v")
    assert driver.getdel("k") == b"v"
    assert driver.get("k") is None


def test_getdel_missing_returns_none(driver) -> None:
    assert driver.getdel("missing") is None


def test_getrange(driver) -> None:
    driver.set("k", b"hello world")
    assert driver.getrange("k", 0, 4) == b"hello"
    assert driver.getrange("k", 6, 10) == b"world"
    assert driver.getrange("k", 0, -1) == b"hello world"


def test_getrange_missing_returns_empty(driver) -> None:
    assert driver.getrange("missing", 0, 5) == b""


def test_setrange(driver) -> None:
    driver.set("k", b"hello world")
    assert driver.setrange("k", 6, b"redis") == 11
    assert driver.get("k") == b"hello redis"


def test_setrange_extends_string(driver) -> None:
    assert driver.setrange("k", 5, b"world") == 10
    assert driver.get("k") == b"\x00\x00\x00\x00\x00world"


def test_strlen(driver) -> None:
    driver.set("k", b"hello")
    assert driver.strlen("k") == 5


def test_strlen_missing_returns_zero(driver) -> None:
    assert driver.strlen("missing") == 0


def test_append_creates_key(driver) -> None:
    assert driver.append("k", b"hello") == 5
    assert driver.get("k") == b"hello"


def test_append_extends(driver) -> None:
    driver.set("k", b"hello")
    assert driver.append("k", b" world") == 11
    assert driver.get("k") == b"hello world"


@pytest.mark.asyncio
async def test_aget_family(driver) -> None:
    await driver.aset("k", b"hello")
    assert await driver.agetex("k", ex=60) == b"hello"
    assert await driver.agetdel("k") == b"hello"
    assert await driver.aget("k") is None
    await driver.aset("k", b"hello world")
    assert await driver.agetrange("k", 0, 4) == b"hello"
    assert await driver.asetrange("k", 6, b"redis") == 11
    assert await driver.astrlen("k") == 11
    assert await driver.aappend("k", b"!") == 12
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_strings.py -v -k "getex or getdel or getrange or setrange or strlen or append"`
Expected: FAIL — `AttributeError: 'RedisRsDriver' object has no attribute 'getex'` etc.

- [ ] **Step 3: Add the connection helpers**

Append to `crates/redis-rs-py-driver/src/connection.rs` inside `impl ValkeyConnInner`:

```rust
impl ValkeyConnInner {
    pub async fn getex(
        &mut self,
        key: &str,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        persist: bool,
    ) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("GETEX");
        cmd.arg(key);
        if let Some(s) = ex {
            cmd.arg("EX").arg(s);
        }
        if let Some(ms) = px {
            cmd.arg("PX").arg(ms);
        }
        if let Some(ts) = exat {
            cmd.arg("EXAT").arg(ts);
        }
        if let Some(ts) = pxat {
            cmd.arg("PXAT").arg(ts);
        }
        if persist {
            cmd.arg("PERSIST");
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn getdel(&mut self, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("GETDEL");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn getrange(
        &mut self,
        key: &str,
        start: i64,
        end: i64,
    ) -> redis::RedisResult<Vec<u8>> {
        let mut cmd = redis::cmd("GETRANGE");
        cmd.arg(key).arg(start).arg(end);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn setrange(
        &mut self,
        key: &str,
        offset: i64,
        value: &[u8],
    ) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("SETRANGE");
        cmd.arg(key).arg(offset).arg(value);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn strlen(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("STRLEN");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn append(&mut self, key: &str, value: &[u8]) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("APPEND");
        cmd.arg(key).arg(value);
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods in `commands/strings.rs`**

Inside the existing `#[pymethods] impl RedisRsDriver { ... }` block, append:

```rust
    // ----- GETEX / aGETEX -------------------------------------------------

    #[pyo3(signature = (
        name,
        *,
        ex = None,
        px = None,
        exat = None,
        pxat = None,
        persist = false,
    ))]
    fn getex(
        &self,
        py: Python<'_>,
        name: &str,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        persist: bool,
    ) -> PyResult<Py<PyAny>> {
        // Validate: at most one TTL option, and persist is mutually exclusive
        // with the TTL options.
        let ttl_set = [ex.is_some(), px.is_some(), exat.is_some(), pxat.is_some()]
            .into_iter()
            .filter(|b| *b)
            .count();
        if ttl_set > 1 {
            return Err(PyErr::new::<DataError, _>(
                "only one of ex, px, exat, pxat may be set",
            ));
        }
        if persist && ttl_set > 0 {
            return Err(PyErr::new::<DataError, _>(
                "persist is mutually exclusive with ex/px/exat/pxat",
            ));
        }
        let r: redis::RedisResult<Option<Vec<u8>>> = sync_op!(py, self, conn, async {
            conn.getex(name, ex, px, exat, pxat, persist).await
        });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (
        name,
        *,
        ex = None,
        px = None,
        exat = None,
        pxat = None,
        persist = false,
    ))]
    fn agetex(
        &self,
        py: Python<'_>,
        name: &str,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        persist: bool,
    ) -> PyResult<Py<PyAny>> {
        let ttl_set = [ex.is_some(), px.is_some(), exat.is_some(), pxat.is_some()]
            .into_iter()
            .filter(|b| *b)
            .count();
        if ttl_set > 1 {
            return Err(PyErr::new::<DataError, _>(
                "only one of ex, px, exat, pxat may be set",
            ));
        }
        if persist && ttl_set > 0 {
            return Err(PyErr::new::<DataError, _>(
                "persist is mutually exclusive with ex/px/exat/pxat",
            ));
        }
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.getex(&name, ex, px, exat, pxat, persist)
                .await
                .into_raw_result()
        })
    }

    // ----- GETDEL / aGETDEL ----------------------------------------------

    fn getdel(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.getdel(name).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn agetdel(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.getdel(&name).await.into_raw_result()
        })
    }

    // ----- GETRANGE / aGETRANGE ------------------------------------------

    fn getrange(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, async { conn.getrange(name, start, end).await });
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    fn agetrange(
        &self,
        py: Python<'_>,
        name: &str,
        start: i64,
        end: i64,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            match conn.getrange(&name, start, end).await {
                Ok(b) => RawResult::OptBytes(Some(b)),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            }
        })
    }

    // ----- SETRANGE / aSETRANGE ------------------------------------------

    fn setrange(
        &self,
        py: Python<'_>,
        name: &str,
        offset: i64,
        value: &[u8],
    ) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.setrange(name, offset, value).await })
            .map_err(to_py_err)
    }

    fn asetrange(
        &self,
        py: Python<'_>,
        name: &str,
        offset: i64,
        value: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.setrange(&name, offset, &value).await.into_raw_result()
        })
    }

    // ----- STRLEN / aSTRLEN ----------------------------------------------

    fn strlen(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.strlen(name).await }).map_err(to_py_err)
    }

    fn astrlen(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.strlen(&name).await.into_raw_result()
        })
    }

    // ----- APPEND / aAPPEND ----------------------------------------------

    fn append(&self, py: Python<'_>, name: &str, value: &[u8]) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.append(name, value).await }).map_err(to_py_err)
    }

    fn aappend(&self, py: Python<'_>, name: &str, value: &[u8]) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.append(&name, &value).await.into_raw_result()
        })
    }
```

(Drop the unused `PyString` import from the head of `strings.rs` if clippy complains; it's used by later sub-tasks but if clippy-deny-warnings is on, prefix with `_`.)

- [ ] **Step 5: Build + run the tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v`
Expected: 31 PASS (16 from Task 2 + 15 new).

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add GETEX/GETDEL/GETRANGE/SETRANGE/STRLEN/APPEND"
```

---

## Task 4: Sub-family C — `MGET` / `MSET` / `MSETNX`

`MGET` returns a list aligned with `keys`, with `None` for missing entries. `MSET` is fire-and-forget. `MSETNX` returns a bool — fails if any key already exists.

`redis-py` accepts the mapping form: `mset({"k1": "v1", "k2": "v2"})`. We mirror that — the Rust signature takes a `HashMap<String, Vec<u8>>` (PyO3 0.28 auto-converts a Python dict).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs`
- Test: `tests/driver/test_commands_strings.py` (append)

- [ ] **Step 1: Append the failing tests**

```python
# ---------- MGET / MSET / MSETNX ----------


def test_mget(driver) -> None:
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.mget(["a", "b", "missing"]) == [b"1", b"2", None]


def test_mget_empty_returns_empty_list(driver) -> None:
    assert driver.mget([]) == []


def test_mset(driver) -> None:
    driver.mset({"a": b"1", "b": b"2"})
    assert driver.get("a") == b"1"
    assert driver.get("b") == b"2"


def test_msetnx_when_all_missing(driver) -> None:
    assert driver.msetnx({"a": b"1", "b": b"2"}) is True
    assert driver.get("a") == b"1"


def test_msetnx_when_any_exists_returns_false(driver) -> None:
    driver.set("a", b"old")
    assert driver.msetnx({"a": b"new", "b": b"2"}) is False
    assert driver.get("a") == b"old"
    assert driver.get("b") is None


@pytest.mark.asyncio
async def test_amget_amset_amsetnx(driver) -> None:
    await driver.amset({"a": b"1", "b": b"2"})
    assert await driver.amget(["a", "b", "x"]) == [b"1", b"2", None]
    assert await driver.amsetnx({"c": b"3", "a": b"X"}) is False
    assert await driver.aget("a") == b"1"
    assert await driver.aget("c") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_strings.py -v -k "mget or mset or msetnx"`
Expected: FAIL — `AttributeError: 'RedisRsDriver' object has no attribute 'mget'`.

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs` inside `impl ValkeyConnInner`:

```rust
impl ValkeyConnInner {
    pub async fn mget(
        &mut self,
        keys: &[String],
    ) -> redis::RedisResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut cmd = redis::cmd("MGET");
        for k in keys {
            cmd.arg(k.as_str());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn mset(&mut self, entries: &[(String, Vec<u8>)]) -> redis::RedisResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut cmd = redis::cmd("MSET");
        for (k, v) in entries {
            cmd.arg(k.as_str()).arg(v.as_slice());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn msetnx(
        &mut self,
        entries: &[(String, Vec<u8>)],
    ) -> redis::RedisResult<bool> {
        if entries.is_empty() {
            return Ok(true);
        }
        let mut cmd = redis::cmd("MSETNX");
        for (k, v) in entries {
            cmd.arg(k.as_str()).arg(v.as_slice());
        }
        let r: i64 = crate::dispatch_cmd!(self, cmd)?;
        Ok(r == 1)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/strings.rs`:

```rust
    // ----- MGET / aMGET --------------------------------------------------

    fn mget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Option<Vec<u8>>>> =
            sync_op!(py, self, conn, async { conn.mget(&keys).await });
        let v = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = v
            .into_iter()
            .map(|o| match o {
                Some(b) => PyBytes::new(py, &b).into_any().unbind(),
                None => py.None(),
            })
            .collect();
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    fn amget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            conn.mget(&keys).await.into_raw_result()
        })
    }

    // ----- MSET / aMSET --------------------------------------------------

    fn mset(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<()> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        sync_op!(py, self, conn, async { conn.mset(&entries).await }).map_err(to_py_err)
    }

    fn amset(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        async_op!(self, py, conn, async {
            conn.mset(&entries).await.into_raw_result()
        })
    }

    // ----- MSETNX / aMSETNX ----------------------------------------------

    fn msetnx(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<bool> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        sync_op!(py, self, conn, async { conn.msetnx(&entries).await }).map_err(to_py_err)
    }

    fn amsetnx(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        async_op!(self, py, conn, async {
            conn.msetnx(&entries).await.into_raw_result()
        })
    }
```

- [ ] **Step 5: Build + run the tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v -k "mget or mset or msetnx"`
Expected: 6 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add MGET/MSET/MSETNX"
```

---

## Task 5: Sub-family D — `INCR` family

`INCR`, `INCRBY`, `INCRBYFLOAT`, `DECR`, `DECRBY`. Returns the post-increment value (`int` for INCR/INCRBY/DECR/DECRBY; `float` for INCRBYFLOAT).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs`
- Test: `tests/driver/test_commands_strings.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- INCR / DECR family ----------


def test_incr_creates_key_at_one(driver) -> None:
    assert driver.incr("counter") == 1
    assert driver.incr("counter") == 2


def test_incrby(driver) -> None:
    assert driver.incrby("counter", 10) == 10
    assert driver.incrby("counter", 5) == 15
    assert driver.incrby("counter", -3) == 12


def test_incrbyfloat(driver) -> None:
    assert driver.incrbyfloat("counter", 1.5) == pytest.approx(1.5)
    assert driver.incrbyfloat("counter", 2.25) == pytest.approx(3.75)


def test_decr(driver) -> None:
    driver.set("counter", b"10")
    assert driver.decr("counter") == 9
    assert driver.decr("counter") == 8


def test_decrby(driver) -> None:
    driver.set("counter", b"100")
    assert driver.decrby("counter", 25) == 75


def test_incr_on_non_numeric_raises(driver) -> None:
    driver.set("k", b"not-a-number")
    with pytest.raises(ResponseError):
        driver.incr("k")


@pytest.mark.asyncio
async def test_aincr_family(driver) -> None:
    assert await driver.aincr("c") == 1
    assert await driver.aincrby("c", 5) == 6
    assert await driver.adecr("c") == 5
    assert await driver.adecrby("c", 2) == 3
    assert await driver.aincrbyfloat("c", 0.5) == pytest.approx(3.5)
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_strings.py -v -k "incr or decr"`
Expected: FAIL — `AttributeError`.

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn incrby(&mut self, key: &str, delta: i64) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("INCRBY");
        cmd.arg(key).arg(delta);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn incrbyfloat(&mut self, key: &str, delta: f64) -> redis::RedisResult<f64> {
        let mut cmd = redis::cmd("INCRBYFLOAT");
        cmd.arg(key).arg(delta);
        let s: String = crate::dispatch_cmd!(self, cmd)?;
        s.parse::<f64>().map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "INCRBYFLOAT response was not a valid float",
                e.to_string(),
            ))
        })
    }

    pub async fn decrby(&mut self, key: &str, delta: i64) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("DECRBY");
        cmd.arg(key).arg(delta);
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/strings.rs`:

```rust
    // ----- INCR / aINCR --------------------------------------------------

    fn incr(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.incrby(name, 1).await }).map_err(to_py_err)
    }

    fn aincr(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incrby(&name, 1).await.into_raw_result()
        })
    }

    // ----- INCRBY / aINCRBY ----------------------------------------------

    fn incrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.incrby(name, amount).await }).map_err(to_py_err)
    }

    fn aincrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incrby(&name, amount).await.into_raw_result()
        })
    }

    // ----- INCRBYFLOAT / aINCRBYFLOAT ------------------------------------

    fn incrbyfloat(&self, py: Python<'_>, name: &str, amount: f64) -> PyResult<f64> {
        sync_op!(py, self, conn, async { conn.incrbyfloat(name, amount).await })
            .map_err(to_py_err)
    }

    fn aincrbyfloat(
        &self,
        py: Python<'_>,
        name: &str,
        amount: f64,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incrbyfloat(&name, amount).await.into_raw_result()
        })
    }

    // ----- DECR / aDECR --------------------------------------------------

    fn decr(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.decrby(name, 1).await }).map_err(to_py_err)
    }

    fn adecr(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.decrby(&name, 1).await.into_raw_result()
        })
    }

    // ----- DECRBY / aDECRBY ----------------------------------------------

    fn decrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.decrby(name, amount).await }).map_err(to_py_err)
    }

    fn adecrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.decrby(&name, amount).await.into_raw_result()
        })
    }
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v -k "incr or decr"`
Expected: 7 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add INCR/INCRBY/INCRBYFLOAT/DECR/DECRBY"
```

---

## Task 6: Sub-family E — `EXISTS` (variadic) / `DEL` / `UNLINK`

The plan-01 `delete` already exists with the variadic `*keys` shape — this task replaces it with a properly-named pair (`delete`/`adelete` stays — the canonical 4 commands keep their plan-01 names) but adds `unlink`/`aunlink` and `exists`/`aexists` (the latter is variadic and returns the count, matching redis-py).

`exists` in redis-py is variadic and returns an `int` — the number of keys that exist (0 to len(keys)). `EXISTS k k k` returns 3 if `k` exists. We mirror that exactly.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs`
- Test: `tests/driver/test_commands_strings.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- EXISTS / DEL / UNLINK ----------


def test_exists_single(driver) -> None:
    driver.set("a", b"1")
    assert driver.exists("a") == 1
    assert driver.exists("missing") == 0


def test_exists_variadic(driver) -> None:
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.exists("a", "b", "missing") == 2


def test_exists_counts_duplicates(driver) -> None:
    driver.set("a", b"1")
    # EXISTS counts each occurrence even on duplicates.
    assert driver.exists("a", "a", "a") == 3


def test_exists_empty_returns_zero(driver) -> None:
    assert driver.exists() == 0


def test_unlink(driver) -> None:
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.unlink("a", "b", "missing") == 2
    assert driver.get("a") is None


def test_unlink_empty_returns_zero(driver) -> None:
    assert driver.unlink() == 0


@pytest.mark.asyncio
async def test_aexists_aunlink(driver) -> None:
    await driver.aset("a", b"1")
    await driver.aset("b", b"2")
    assert await driver.aexists("a", "b", "x") == 2
    assert await driver.aunlink("a", "b") == 2
    assert await driver.aexists("a") == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_strings.py -v -k "exists or unlink"`
Expected: FAIL — `exists` doesn't take multiple args yet (or doesn't exist).

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn exists_many(&mut self, keys: &[String]) -> redis::RedisResult<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("EXISTS");
        for k in keys {
            cmd.arg(k.as_str());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn unlink_many(&mut self, keys: &[String]) -> redis::RedisResult<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("UNLINK");
        for k in keys {
            cmd.arg(k.as_str());
        }
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/strings.rs`:

```rust
    // ----- EXISTS / aEXISTS (variadic) -----------------------------------

    #[pyo3(signature = (*names))]
    fn exists(&self, py: Python<'_>, names: Vec<String>) -> PyResult<i64> {
        if names.is_empty() {
            return Ok(0);
        }
        sync_op!(py, self, conn, async { conn.exists_many(&names).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (*names))]
    fn aexists(&self, py: Python<'_>, names: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if names.is_empty() {
                return RawResult::Int(0);
            }
            conn.exists_many(&names).await.into_raw_result()
        })
    }

    // ----- UNLINK / aUNLINK (variadic) -----------------------------------

    #[pyo3(signature = (*names))]
    fn unlink(&self, py: Python<'_>, names: Vec<String>) -> PyResult<i64> {
        if names.is_empty() {
            return Ok(0);
        }
        sync_op!(py, self, conn, async { conn.unlink_many(&names).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (*names))]
    fn aunlink(&self, py: Python<'_>, names: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if names.is_empty() {
                return RawResult::Int(0);
            }
            conn.unlink_many(&names).await.into_raw_result()
        })
    }
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v -k "exists or unlink"`
Expected: 8 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add EXISTS variadic and UNLINK"
```

---

## Task 7: Sub-family F — `EXPIRE` family + `TTL` / `PTTL` / `PERSIST` / `EXPIRETIME` / `PEXPIRETIME`

Nine commands: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `EXPIRETIME`, `PEXPIRETIME`, `TTL`, `PTTL`, `PERSIST`.

The four set-TTL commands (`EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`) all accept the same option suffix in Redis 7+: `NX`/`XX`/`GT`/`LT`. We expose those as kwargs (`nx=False`, `xx=False`, `gt=False`, `lt=False`).

`TTL`/`PTTL` return -2 if key doesn't exist, -1 if no TTL, ≥0 otherwise. `EXPIRETIME`/`PEXPIRETIME` return the absolute deadline (or -2/-1 with the same semantics).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs`
- Test: `tests/driver/test_commands_strings.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- EXPIRE family + TTL ----------


def test_expire_returns_true(driver) -> None:
    driver.set("k", b"v")
    assert driver.expire("k", 60) is True
    assert 0 < driver.ttl("k") <= 60


def test_expire_missing_returns_false(driver) -> None:
    assert driver.expire("missing", 60) is False


def test_pexpire(driver) -> None:
    driver.set("k", b"v")
    assert driver.pexpire("k", 60_000) is True
    assert 0 < driver.pttl("k") <= 60_000


def test_expireat(driver) -> None:
    import time

    driver.set("k", b"v")
    assert driver.expireat("k", int(time.time()) + 30) is True
    assert 0 < driver.ttl("k") <= 30


def test_pexpireat(driver) -> None:
    import time

    driver.set("k", b"v")
    assert driver.pexpireat("k", int(time.time() * 1000) + 30_000) is True


def test_expire_with_xx_when_no_ttl_returns_false(driver) -> None:
    driver.set("k", b"v")
    # XX = only set TTL if there's already a TTL. None exists yet → False.
    assert driver.expire("k", 60, xx=True) is False


def test_expire_with_nx_when_no_ttl_returns_true(driver) -> None:
    driver.set("k", b"v")
    assert driver.expire("k", 60, nx=True) is True


def test_expire_with_gt(driver) -> None:
    driver.set("k", b"v", ex=100)
    # GT = only update if new TTL is greater than current.
    assert driver.expire("k", 50, gt=True) is False
    assert driver.expire("k", 200, gt=True) is True


def test_expire_with_lt(driver) -> None:
    driver.set("k", b"v", ex=100)
    assert driver.expire("k", 200, lt=True) is False
    assert driver.expire("k", 50, lt=True) is True


def test_ttl_no_expiry_returns_minus_one(driver) -> None:
    driver.set("k", b"v")
    assert driver.ttl("k") == -1


def test_ttl_missing_returns_minus_two(driver) -> None:
    assert driver.ttl("missing") == -2


def test_pttl_missing_returns_minus_two(driver) -> None:
    assert driver.pttl("missing") == -2


def test_expiretime(driver) -> None:
    import time

    deadline = int(time.time()) + 60
    driver.set("k", b"v", exat=deadline)
    assert driver.expiretime("k") == deadline


def test_expiretime_no_expiry_returns_minus_one(driver) -> None:
    driver.set("k", b"v")
    assert driver.expiretime("k") == -1


def test_expiretime_missing_returns_minus_two(driver) -> None:
    assert driver.expiretime("missing") == -2


def test_pexpiretime(driver) -> None:
    import time

    deadline_ms = int(time.time() * 1000) + 60_000
    driver.set("k", b"v", pxat=deadline_ms)
    assert driver.pexpiretime("k") == deadline_ms


def test_persist(driver) -> None:
    driver.set("k", b"v", ex=60)
    assert driver.persist("k") is True
    assert driver.ttl("k") == -1


def test_persist_no_ttl_returns_false(driver) -> None:
    driver.set("k", b"v")
    assert driver.persist("k") is False


@pytest.mark.asyncio
async def test_aexpire_family(driver) -> None:
    await driver.aset("k", b"v")
    assert await driver.aexpire("k", 60) is True
    assert 0 < await driver.attl("k") <= 60
    assert await driver.apexpire("k", 90_000) is True
    assert await driver.apersist("k") is True
    assert await driver.attl("k") == -1
    import time

    assert await driver.aexpireat("k", int(time.time()) + 60) is True
    assert await driver.apexpireat("k", int(time.time() * 1000) + 60_000) is True
    et = await driver.aexpiretime("k")
    assert et > 0
    pet = await driver.apexpiretime("k")
    assert pet > 0
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_strings.py -v -k "expire or ttl or persist"`
Expected: FAIL — methods missing or `expire` ignores `xx`/`nx`/`gt`/`lt`.

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn expire_full(
        &mut self,
        key: &str,
        seconds: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("EXPIRE");
        cmd.arg(key).arg(seconds);
        append_expire_flag(&mut cmd, nx, xx, gt, lt);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn pexpire_full(
        &mut self,
        key: &str,
        milliseconds: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("PEXPIRE");
        cmd.arg(key).arg(milliseconds);
        append_expire_flag(&mut cmd, nx, xx, gt, lt);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn expireat_full(
        &mut self,
        key: &str,
        ts_seconds: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("EXPIREAT");
        cmd.arg(key).arg(ts_seconds);
        append_expire_flag(&mut cmd, nx, xx, gt, lt);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn pexpireat_full(
        &mut self,
        key: &str,
        ts_milliseconds: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("PEXPIREAT");
        cmd.arg(key).arg(ts_milliseconds);
        append_expire_flag(&mut cmd, nx, xx, gt, lt);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn ttl(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("TTL");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn pttl(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("PTTL");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn expiretime(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("EXPIRETIME");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn pexpiretime(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("PEXPIRETIME");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn persist(&mut self, key: &str) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("PERSIST");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }
}

fn append_expire_flag(cmd: &mut redis::Cmd, nx: bool, xx: bool, gt: bool, lt: bool) {
    if nx {
        cmd.arg("NX");
    } else if xx {
        cmd.arg("XX");
    } else if gt {
        cmd.arg("GT");
    } else if lt {
        cmd.arg("LT");
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/strings.rs`:

```rust
    // ----- EXPIRE family -------------------------------------------------

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn expire(
        &self,
        py: Python<'_>,
        name: &str,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.expire_full(name, time, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn aexpire(
        &self,
        py: Python<'_>,
        name: &str,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        validate_expire_flags(nx, xx, gt, lt)?;
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.expire_full(&name, time, nx, xx, gt, lt)
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn pexpire(
        &self,
        py: Python<'_>,
        name: &str,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.pexpire_full(name, time, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn apexpire(
        &self,
        py: Python<'_>,
        name: &str,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        validate_expire_flags(nx, xx, gt, lt)?;
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pexpire_full(&name, time, nx, xx, gt, lt)
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn expireat(
        &self,
        py: Python<'_>,
        name: &str,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.expireat_full(name, when, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn aexpireat(
        &self,
        py: Python<'_>,
        name: &str,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        validate_expire_flags(nx, xx, gt, lt)?;
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.expireat_full(&name, when, nx, xx, gt, lt)
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn pexpireat(
        &self,
        py: Python<'_>,
        name: &str,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.pexpireat_full(name, when, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::fn_params_excessive_bools)]
    fn apexpireat(
        &self,
        py: Python<'_>,
        name: &str,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        validate_expire_flags(nx, xx, gt, lt)?;
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pexpireat_full(&name, when, nx, xx, gt, lt)
                .await
                .into_raw_result()
        })
    }

    // ----- TTL / aTTL ----------------------------------------------------

    fn ttl(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.ttl(name).await }).map_err(to_py_err)
    }

    fn attl(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.ttl(&name).await.into_raw_result()
        })
    }

    fn pttl(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.pttl(name).await }).map_err(to_py_err)
    }

    fn apttl(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pttl(&name).await.into_raw_result()
        })
    }

    fn expiretime(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.expiretime(name).await }).map_err(to_py_err)
    }

    fn aexpiretime(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.expiretime(&name).await.into_raw_result()
        })
    }

    fn pexpiretime(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.pexpiretime(name).await }).map_err(to_py_err)
    }

    fn apexpiretime(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pexpiretime(&name).await.into_raw_result()
        })
    }

    fn persist(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, async { conn.persist(name).await }).map_err(to_py_err)
    }

    fn apersist(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.persist(&name).await.into_raw_result()
        })
    }
```

Add `validate_expire_flags` near `validate_set_kwargs` at the top of `commands/strings.rs`:

```rust
fn validate_expire_flags(nx: bool, xx: bool, gt: bool, lt: bool) -> PyResult<()> {
    let count = [nx, xx, gt, lt].into_iter().filter(|b| *b).count();
    if count > 1 {
        return Err(PyErr::new::<DataError, _>(
            "at most one of nx, xx, gt, lt may be set on EXPIRE-family commands",
        ));
    }
    Ok(())
}
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v -k "expire or ttl or persist"`
Expected: 18 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add EXPIRE family + TTL/PTTL/PERSIST/EXPIRETIME"
```

---

## Task 8: Sub-family G — `RENAME` / `RENAMENX` / `TYPE`

`RENAME` returns OK or raises `ResponseError` if source doesn't exist. `RENAMENX` returns bool. `TYPE` returns the type name as a `str` (`"string"`, `"list"`, `"hash"`, `"set"`, `"zset"`, `"stream"`, `"none"` for missing keys).

The Rust method is named `key_type` (since `type` is a Rust keyword); PyO3's `#[pyo3(name = "type")]` exposes it as `driver.type(...)` from Python — but `.type` is also a Python keyword. `redis-py` calls it `Redis.type()` — to be safe we ALSO expose an alias `key_type()` for Python users who don't want to use `getattr`. Test coverage validates both.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs`
- Test: `tests/driver/test_commands_strings.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- RENAME / RENAMENX / TYPE ----------


def test_rename(driver) -> None:
    driver.set("a", b"v")
    driver.rename("a", "b")
    assert driver.get("a") is None
    assert driver.get("b") == b"v"


def test_rename_missing_source_raises(driver) -> None:
    with pytest.raises(ResponseError):
        driver.rename("missing", "b")


def test_renamenx_when_dest_missing(driver) -> None:
    driver.set("a", b"v")
    assert driver.renamenx("a", "b") is True
    assert driver.get("b") == b"v"


def test_renamenx_when_dest_exists_returns_false(driver) -> None:
    driver.set("a", b"v")
    driver.set("b", b"existing")
    assert driver.renamenx("a", "b") is False
    assert driver.get("a") == b"v"
    assert driver.get("b") == b"existing"


def test_type_string(driver) -> None:
    driver.set("k", b"v")
    # `type` is a Python keyword; getattr is the portable form.
    assert getattr(driver, "type")("k") == "string"


def test_type_alias_key_type(driver) -> None:
    driver.set("k", b"v")
    assert driver.key_type("k") == "string"


def test_type_missing_returns_none_string(driver) -> None:
    assert driver.key_type("missing") == "none"


@pytest.mark.asyncio
async def test_arename_atype(driver) -> None:
    await driver.aset("a", b"v")
    await driver.arename("a", "b")
    assert await driver.aget("b") == b"v"
    assert await driver.akey_type("b") == "string"
    await driver.aset("c", b"x")
    assert await driver.arenamenx("c", "b") is False
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_strings.py -v -k "rename or type"`
Expected: FAIL — methods missing.

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn rename(&mut self, src: &str, dst: &str) -> redis::RedisResult<()> {
        let mut cmd = redis::cmd("RENAME");
        cmd.arg(src).arg(dst);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn renamenx(&mut self, src: &str, dst: &str) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("RENAMENX");
        cmd.arg(src).arg(dst);
        let r: i64 = crate::dispatch_cmd!(self, cmd)?;
        Ok(r == 1)
    }

    pub async fn key_type(&mut self, key: &str) -> redis::RedisResult<String> {
        let mut cmd = redis::cmd("TYPE");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/strings.rs`:

```rust
    // ----- RENAME / aRENAME ----------------------------------------------

    fn rename(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        sync_op!(py, self, conn, async { conn.rename(src, dst).await }).map_err(to_py_err)
    }

    fn arename(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<Py<PyAny>> {
        let src = src.to_string();
        let dst = dst.to_string();
        async_op!(self, py, conn, async {
            conn.rename(&src, &dst).await.into_raw_result()
        })
    }

    fn renamenx(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, async { conn.renamenx(src, dst).await }).map_err(to_py_err)
    }

    fn arenamenx(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<Py<PyAny>> {
        let src = src.to_string();
        let dst = dst.to_string();
        async_op!(self, py, conn, async {
            conn.renamenx(&src, &dst).await.into_raw_result()
        })
    }

    // ----- TYPE / aTYPE --------------------------------------------------
    //
    // `type` is a Python keyword (and a Rust keyword). We expose two names:
    //   * `type` — matches redis-py exactly (call via getattr in Python).
    //   * `key_type` — convenience alias for codebases that prefer not to
    //     use getattr.

    #[pyo3(name = "type")]
    fn type_(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        sync_op!(py, self, conn, async { conn.key_type(name).await }).map_err(to_py_err)
    }

    fn key_type(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        sync_op!(py, self, conn, async { conn.key_type(name).await }).map_err(to_py_err)
    }

    #[pyo3(name = "atype")]
    fn atype(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.key_type(&name).await.into_raw_result()
        })
    }

    fn akey_type(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.key_type(&name).await.into_raw_result()
        })
    }
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v -k "rename or type"`
Expected: 7 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add RENAME/RENAMENX and TYPE (with key_type alias)"
```

---

## Task 9: Sub-family H — `COPY`

`COPY src dst [DB destination-db] [REPLACE]`. Returns 1 if copied, 0 otherwise.
Signature: `copy(source, destination, *, db=None, replace=False)`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs`
- Test: `tests/driver/test_commands_strings.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- COPY ----------


def test_copy_basic(driver) -> None:
    driver.set("a", b"v")
    assert driver.copy("a", "b") is True
    assert driver.get("b") == b"v"


def test_copy_when_dest_exists_no_replace_returns_false(driver) -> None:
    driver.set("a", b"v")
    driver.set("b", b"existing")
    assert driver.copy("a", "b") is False
    assert driver.get("b") == b"existing"


def test_copy_with_replace(driver) -> None:
    driver.set("a", b"v")
    driver.set("b", b"existing")
    assert driver.copy("a", "b", replace=True) is True
    assert driver.get("b") == b"v"


def test_copy_missing_source_returns_false(driver) -> None:
    assert driver.copy("missing", "dst") is False


def test_copy_with_db_to_other_db(driver) -> None:
    driver.set("a", b"v")
    # The default driver is on db 0. COPY can target a different db.
    assert driver.copy("a", "b", db=1) is True
    # Verify via the upstream client connecting to db 1.
    import redis as upstream

    rp = upstream.Redis.from_url(driver.connection_url, db=1)
    try:
        assert rp.get("b") == b"v"
        rp.delete("b")
    finally:
        rp.close()


@pytest.mark.asyncio
async def test_acopy(driver) -> None:
    await driver.aset("a", b"v")
    assert await driver.acopy("a", "b") is True
    assert await driver.acopy("a", "b") is False
    assert await driver.acopy("a", "b", replace=True) is True
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_strings.py -v -k "copy"`
Expected: FAIL — `AttributeError`.

- [ ] **Step 3: Add the connection helper**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn copy(
        &mut self,
        src: &str,
        dst: &str,
        db: Option<i64>,
        replace: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("COPY");
        cmd.arg(src).arg(dst);
        if let Some(d) = db {
            cmd.arg("DB").arg(d);
        }
        if replace {
            cmd.arg("REPLACE");
        }
        let r: i64 = crate::dispatch_cmd!(self, cmd)?;
        Ok(r == 1)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/strings.rs`:

```rust
    // ----- COPY / aCOPY --------------------------------------------------

    #[pyo3(signature = (source, destination, *, db = None, replace = false))]
    fn copy(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        db: Option<i64>,
        replace: bool,
    ) -> PyResult<bool> {
        sync_op!(py, self, conn, async {
            conn.copy(source, destination, db, replace).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (source, destination, *, db = None, replace = false))]
    fn acopy(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        db: Option<i64>,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        let source = source.to_string();
        let destination = destination.to_string();
        async_op!(self, py, conn, async {
            conn.copy(&source, &destination, db, replace).await.into_raw_result()
        })
    }
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v -k "copy"`
Expected: 6 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add COPY with db= and replace= kwargs"
```

---

## Task 10: Sub-family I — `DUMP` / `RESTORE`

`DUMP key` returns the serialised payload as bytes (or None for missing keys).
`RESTORE key ttl serialized [REPLACE] [ABSTTL] [IDLETIME ms] [FREQ count]` returns OK on success.

`redis-py` signature for `restore`:
```python
def restore(self, name, ttl, value, replace=False, absttl=False, idletime=None, frequency=None) -> bytes:
    ...
```

We mirror that. (`frequency` is the LFU-mode access counter; `idletime` is the LRU-mode idle seconds.)

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/strings.rs`
- Test: `tests/driver/test_commands_strings.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- DUMP / RESTORE ----------


def test_dump_returns_bytes(driver) -> None:
    driver.set("k", b"v")
    payload = driver.dump("k")
    assert isinstance(payload, bytes)
    assert len(payload) > 0


def test_dump_missing_returns_none(driver) -> None:
    assert driver.dump("missing") is None


def test_dump_then_restore_round_trip(driver) -> None:
    driver.set("k", b"hello")
    payload = driver.dump("k")
    assert driver.restore("k2", 0, payload) is True
    assert driver.get("k2") == b"hello"


def test_restore_existing_key_without_replace_raises(driver) -> None:
    driver.set("k", b"v")
    payload = driver.dump("k")
    driver.set("dst", b"existing")
    with pytest.raises(ResponseError, match="(?i)busy"):
        driver.restore("dst", 0, payload)


def test_restore_with_replace(driver) -> None:
    driver.set("k", b"new")
    payload = driver.dump("k")
    driver.set("dst", b"old")
    assert driver.restore("dst", 0, payload, replace=True) is True
    assert driver.get("dst") == b"new"


def test_restore_with_idletime(driver) -> None:
    driver.set("k", b"v")
    payload = driver.dump("k")
    assert driver.restore("dst", 0, payload, idletime=10) is True


def test_restore_with_absttl(driver) -> None:
    import time

    driver.set("k", b"v")
    payload = driver.dump("k")
    deadline_ms = int(time.time() * 1000) + 30_000
    assert driver.restore("dst", deadline_ms, payload, absttl=True) is True
    assert 0 < driver.pttl("dst") <= 30_000


@pytest.mark.asyncio
async def test_adump_arestore(driver) -> None:
    await driver.aset("k", b"hello")
    payload = await driver.adump("k")
    assert isinstance(payload, bytes)
    assert await driver.arestore("k2", 0, payload) is True
    assert await driver.aget("k2") == b"hello"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_strings.py -v -k "dump or restore"`
Expected: FAIL — `AttributeError`.

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn dump(&mut self, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("DUMP");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn restore(
        &mut self,
        key: &str,
        ttl_ms: i64,
        serialized: &[u8],
        replace: bool,
        absttl: bool,
        idletime: Option<u64>,
        frequency: Option<u64>,
    ) -> redis::RedisResult<()> {
        let mut cmd = redis::cmd("RESTORE");
        cmd.arg(key).arg(ttl_ms).arg(serialized);
        if replace {
            cmd.arg("REPLACE");
        }
        if absttl {
            cmd.arg("ABSTTL");
        }
        if let Some(it) = idletime {
            cmd.arg("IDLETIME").arg(it);
        }
        if let Some(f) = frequency {
            cmd.arg("FREQ").arg(f);
        }
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/strings.rs`:

```rust
    // ----- DUMP / aDUMP --------------------------------------------------

    fn dump(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.dump(name).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn adump(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.dump(&name).await.into_raw_result()
        })
    }

    // ----- RESTORE / aRESTORE --------------------------------------------
    //
    // Returns True on OK to keep parity with the boolean return shape used
    // throughout the driver — redis-py returns the literal string "OK".
    // Callers who care about the literal can read the high-level façade
    // (plan 10), which preserves the redis-py shape.

    #[pyo3(signature = (
        name,
        ttl,
        value,
        *,
        replace = false,
        absttl = false,
        idletime = None,
        frequency = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn restore(
        &self,
        py: Python<'_>,
        name: &str,
        ttl: i64,
        value: &[u8],
        replace: bool,
        absttl: bool,
        idletime: Option<u64>,
        frequency: Option<u64>,
    ) -> PyResult<bool> {
        sync_op!(py, self, conn, async {
            conn.restore(name, ttl, value, replace, absttl, idletime, frequency)
                .await
        })
        .map_err(to_py_err)?;
        Ok(true)
    }

    #[pyo3(signature = (
        name,
        ttl,
        value,
        *,
        replace = false,
        absttl = false,
        idletime = None,
        frequency = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn arestore(
        &self,
        py: Python<'_>,
        name: &str,
        ttl: i64,
        value: &[u8],
        replace: bool,
        absttl: bool,
        idletime: Option<u64>,
        frequency: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            match conn
                .restore(&name, ttl, &value, replace, absttl, idletime, frequency)
                .await
            {
                Ok(()) => RawResult::Bool(true),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            }
        })
    }
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_strings.py -v -k "dump or restore"`
Expected: 8 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/strings.rs tests/driver/test_commands_strings.py
git commit -m "feat(strings): add DUMP/RESTORE with full option matrix"
```

---

## Task 11: Type stubs

Append signatures for every command landed in this plan to `python/redis_rs_py/_driver.pyi`. Keep them in the same order as the commands appear in `commands/strings.rs`.

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Edit `_driver.pyi`**

Inside the existing `class RedisRsDriver:` block (above the `# Internal test helpers` comment), append:

```python
    # --- SET full matrix -------------------------------------------------
    def set(  # type: ignore[override]
        self,
        name: str,
        value: bytes,
        *,
        ex: int | None = ...,
        px: int | None = ...,
        nx: bool = ...,
        xx: bool = ...,
        keepttl: bool = ...,
        get: bool = ...,
        exat: int | None = ...,
        pxat: int | None = ...,
    ) -> bool | bytes | None: ...
    def aset(  # type: ignore[override]
        self,
        name: str,
        value: bytes,
        *,
        ex: int | None = ...,
        px: int | None = ...,
        nx: bool = ...,
        xx: bool = ...,
        keepttl: bool = ...,
        get: bool = ...,
        exat: int | None = ...,
        pxat: int | None = ...,
    ) -> Awaitable[bool | bytes | None]: ...

    # --- GET family ------------------------------------------------------
    def getex(
        self,
        name: str,
        *,
        ex: int | None = ...,
        px: int | None = ...,
        exat: int | None = ...,
        pxat: int | None = ...,
        persist: bool = ...,
    ) -> bytes | None: ...
    def agetex(
        self,
        name: str,
        *,
        ex: int | None = ...,
        px: int | None = ...,
        exat: int | None = ...,
        pxat: int | None = ...,
        persist: bool = ...,
    ) -> Awaitable[bytes | None]: ...
    def getdel(self, name: str) -> bytes | None: ...
    def agetdel(self, name: str) -> Awaitable[bytes | None]: ...
    def getrange(self, name: str, start: int, end: int) -> bytes: ...
    def agetrange(self, name: str, start: int, end: int) -> Awaitable[bytes]: ...
    def setrange(self, name: str, offset: int, value: bytes) -> int: ...
    def asetrange(self, name: str, offset: int, value: bytes) -> Awaitable[int]: ...
    def strlen(self, name: str) -> int: ...
    def astrlen(self, name: str) -> Awaitable[int]: ...
    def append(self, name: str, value: bytes) -> int: ...
    def aappend(self, name: str, value: bytes) -> Awaitable[int]: ...

    # --- MGET / MSET / MSETNX --------------------------------------------
    def mget(self, keys: list[str]) -> list[bytes | None]: ...
    def amget(self, keys: list[str]) -> Awaitable[list[bytes | None]]: ...
    def mset(self, mapping: dict[str, bytes]) -> None: ...
    def amset(self, mapping: dict[str, bytes]) -> Awaitable[None]: ...
    def msetnx(self, mapping: dict[str, bytes]) -> bool: ...
    def amsetnx(self, mapping: dict[str, bytes]) -> Awaitable[bool]: ...

    # --- INCR / DECR family ---------------------------------------------
    def incr(self, name: str) -> int: ...
    def aincr(self, name: str) -> Awaitable[int]: ...
    def incrby(self, name: str, amount: int) -> int: ...
    def aincrby(self, name: str, amount: int) -> Awaitable[int]: ...
    def incrbyfloat(self, name: str, amount: float) -> float: ...
    def aincrbyfloat(self, name: str, amount: float) -> Awaitable[float]: ...
    def decr(self, name: str) -> int: ...
    def adecr(self, name: str) -> Awaitable[int]: ...
    def decrby(self, name: str, amount: int) -> int: ...
    def adecrby(self, name: str, amount: int) -> Awaitable[int]: ...

    # --- EXISTS / UNLINK -------------------------------------------------
    def exists(self, *names: str) -> int: ...
    def aexists(self, *names: str) -> Awaitable[int]: ...
    def unlink(self, *names: str) -> int: ...
    def aunlink(self, *names: str) -> Awaitable[int]: ...

    # --- EXPIRE family + TTL --------------------------------------------
    def expire(
        self,
        name: str,
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> bool: ...
    def aexpire(
        self,
        name: str,
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> Awaitable[bool]: ...
    def pexpire(
        self,
        name: str,
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> bool: ...
    def apexpire(
        self,
        name: str,
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> Awaitable[bool]: ...
    def expireat(
        self,
        name: str,
        when: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> bool: ...
    def aexpireat(
        self,
        name: str,
        when: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> Awaitable[bool]: ...
    def pexpireat(
        self,
        name: str,
        when: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> bool: ...
    def apexpireat(
        self,
        name: str,
        when: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> Awaitable[bool]: ...
    def ttl(self, name: str) -> int: ...
    def attl(self, name: str) -> Awaitable[int]: ...
    def pttl(self, name: str) -> int: ...
    def apttl(self, name: str) -> Awaitable[int]: ...
    def expiretime(self, name: str) -> int: ...
    def aexpiretime(self, name: str) -> Awaitable[int]: ...
    def pexpiretime(self, name: str) -> int: ...
    def apexpiretime(self, name: str) -> Awaitable[int]: ...
    def persist(self, name: str) -> bool: ...
    def apersist(self, name: str) -> Awaitable[bool]: ...

    # --- RENAME / TYPE ---------------------------------------------------
    def rename(self, src: str, dst: str) -> None: ...
    def arename(self, src: str, dst: str) -> Awaitable[None]: ...
    def renamenx(self, src: str, dst: str) -> bool: ...
    def arenamenx(self, src: str, dst: str) -> Awaitable[bool]: ...
    def type(self, name: str) -> str: ...
    def atype(self, name: str) -> Awaitable[str]: ...
    def key_type(self, name: str) -> str: ...
    def akey_type(self, name: str) -> Awaitable[str]: ...

    # --- COPY ------------------------------------------------------------
    def copy(
        self,
        source: str,
        destination: str,
        *,
        db: int | None = ...,
        replace: bool = ...,
    ) -> bool: ...
    def acopy(
        self,
        source: str,
        destination: str,
        *,
        db: int | None = ...,
        replace: bool = ...,
    ) -> Awaitable[bool]: ...

    # --- DUMP / RESTORE --------------------------------------------------
    def dump(self, name: str) -> bytes | None: ...
    def adump(self, name: str) -> Awaitable[bytes | None]: ...
    def restore(
        self,
        name: str,
        ttl: int,
        value: bytes,
        *,
        replace: bool = ...,
        absttl: bool = ...,
        idletime: int | None = ...,
        frequency: int | None = ...,
    ) -> bool: ...
    def arestore(
        self,
        name: str,
        ttl: int,
        value: bytes,
        *,
        replace: bool = ...,
        absttl: bool = ...,
        idletime: int | None = ...,
        frequency: int | None = ...,
    ) -> Awaitable[bool]: ...
```

- [ ] **Step 2: Run ty check**

Run: `uv run ty check python/redis_rs_py/`
Expected: 0 errors. (Plan-01 uses `from collections.abc import Awaitable` and a `Generic`-friendly `Awaitable[T]`. If ty complains about `bool | bytes | None` not being a valid return type, switch to `typing.Union[bool, bytes, None]` — but PEP 604 unions should work on python 3.10+.)

- [ ] **Step 3: Commit**

```bash
git add python/redis_rs_py/_driver.pyi
git commit -m "feat(strings): add type stubs for every string command"
```

---

## Task 12: Lint, format, full-suite green check

Catch any clippy nits, ruff format issues, or test regressions before signing off.

- [ ] **Step 1: Run formatters**

```bash
cargo fmt --all
uv run ruff format
uv run ruff check --fix
```

Expected: no output beyond reformat counts.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: no warnings. If clippy flags `too_many_arguments` on the SET/RESTORE methods, the `#[allow(clippy::too_many_arguments)]` attributes already silence it. If clippy flags `fn_params_excessive_bools` on EXPIRE/SET, the `#[allow(clippy::fn_params_excessive_bools)]` attributes silence it.

- [ ] **Step 3: Run the full string suite**

Run: `uv run pytest tests/driver/test_commands_strings.py -v`
Expected: 80+ PASS, 0 FAIL. Sub-task counts: SET 16 + GET-family 15 + MGET 6 + INCR 7 + EXISTS 8 + EXPIRE 18 + RENAME 7 + COPY 6 + DUMP 8 = 91 total.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -n auto`
Expected: every test PASSES across `tests/driver/`, `tests/async_bridge/`, `tests/exceptions/`, `tests/test_smoke.py`.

- [ ] **Step 5: Commit (no-op if formatters made no changes)**

If `cargo fmt`/`ruff` modified files:

```bash
git add -u
git commit -m "style(strings): cargo fmt + ruff format"
```

- [ ] **Step 6: Add CHANGELOG entry**

Edit `CHANGELOG.md` and append under `### Added`:

```markdown
- Driver string commands: full `SET` option matrix (`ex`/`px`/`nx`/`xx`/`keepttl`/`get`/`exat`/`pxat`), `GETEX`, `GETDEL`, `GETRANGE`, `SETRANGE`, `STRLEN`, `APPEND`, `MGET`, `MSET`, `MSETNX`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `DECR`, `DECRBY`, variadic `EXISTS`, `UNLINK`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` (with `nx`/`xx`/`gt`/`lt` kwargs), `EXPIRETIME`, `PEXPIRETIME`, `TTL`, `PTTL`, `PERSIST`, `RENAME`, `RENAMENX`, `TYPE` (alias `key_type`), `COPY` (with `db=`/`replace=`), `DUMP`, `RESTORE` (with `replace`/`absttl`/`idletime`/`frequency`). Sync + async pair for every command.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 03 entry"
```

---

## Self-review checklist for this plan

- [x] **Spec coverage** — Roadmap `03-commands-strings.md` row says: full SET matrix, GETEX, GETDEL, GETRANGE, SETRANGE, STRLEN, APPEND, MGET/MSET/MSETNX, INCR/INCRBY/INCRBYFLOAT/DECR/DECRBY, EXISTS/DEL/UNLINK, EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT/EXPIRETIME/PEXPIRETIME/TTL/PTTL/PERSIST, RENAME/RENAMENX, TYPE, COPY, DUMP, RESTORE. Every item has a sub-task. (`DEL` already exists from plan 01 — confirmed in driver.rs canonical 4 commands.)
- [x] **No placeholders** — every step has runnable commands and explicit pass/fail expectations. No "implement following the pattern".
- [x] **Type consistency** — Rust `fn set(...) -> PyResult<Py<PyAny>>` ↔ stub `def set(...) -> bool | bytes | None` ↔ tests `assert driver.set(...) is True | == b"..." | is None`. Async variants return `Awaitable[T]` of the same `T`. EXPIRE family methods return `bool` sync / `Awaitable[bool]` async; TTL family returns `int`/`Awaitable[int]`.
- [x] **Sync + async pair for every command** — checked file-by-file. Every method body in `commands/strings.rs` has both forms. `type` has `type`/`atype`; `key_type` has `key_type`/`akey_type`.
- [x] **Validation lives in Rust** — `validate_set_kwargs` and `validate_expire_flags` are Rust functions raising `DataError` (PyO3-defined exception from plan 02). No Python-side validation.
- [x] **No new dependencies** — `Cargo.toml` is unchanged. Every method uses `redis::cmd` + `dispatch_cmd!` + types already present from plan 01.
- [x] **Test fixture reuse** — every test takes `driver` (defined in `tests/conftest.py` from plan 01); `connection_url` getter from plan 01 is used by the upstream-client TTL inspections.
- [x] **Free-threaded safety** — no new globals introduced; `RedisRsDriver` already `Send + Sync` from plan 01; the `commands/strings.rs` module adds methods only.
- [x] **Conventional commits** — every commit prefix is `feat(strings):`, `style(strings):`, or `docs(changelog):`.
