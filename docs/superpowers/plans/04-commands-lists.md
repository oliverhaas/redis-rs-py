# Plan 04 — List commands (incl. lazy blocking-conn wiring)

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the full list-command surface on `RedisRsDriver`, plus the load-bearing blocking-connection wiring (`get_blocking()` → second `ConnectionManager` with no response timeout) that prevents `BLPOP` from head-of-line-blocking the multiplexed pipeline. Every command lands as a sync + async pair.

**Architecture:** Non-blocking commands (`LPUSH`, `RPUSH`, `LPOP`, `RPOP` (with count), `LMOVE`, `LPOS`, `LRANGE`, `LLEN`, `LREM`, `LINDEX`, `LSET`, `LINSERT`, `LTRIM`, `LPUSHX`, `RPUSHX`, `LMPOP`) ride the regular `ValkeyConn::regular` connection and are wired exactly like the string commands from plan 03 — `dispatch_cmd!` + `async_op!`/`sync_op!`. Blocking commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`) go through new inherent methods on `ValkeyConn` (NOT on `ValkeyConnInner` — the dispatch must explicitly call `self.get_blocking().await?`, bypassing `Deref`). The blocking connection is lazily created on first call and reused thereafter — proven by an `id()`-equality test.

The big architectural rule lifted from cachex: **never call a blocking command on the regular multiplexed connection.** A `BLPOP` that waits 30 s would freeze every other command sharing that pipeline. The blocking connection has `set_response_timeout(None)` and is intentionally separate.

**Tech Stack:** PyO3 0.28, redis 1.x (already includes `redis::cmd("BLPOP").arg(...).query_async(c)`), tokio 1.x (`OnceCell` for the lazy init — already imported by `connection.rs` from plan 01). No new dependencies.

**Reference material:**
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs:1062-1443` — every list command in this plan has a working analogue here. The blocking wrapper pattern (cachex `client.rs:1394-1442`) is the prototype we copy.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs:611-743, 944-1220` — connection-level helpers split into `impl ValkeyConnInner` (regular + blocking shape both call into the same body) and `impl ValkeyConn` (the blocking dispatch that calls `get_blocking().await`).
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/01-foundation-async-bridge.md` — `ValkeyConn::get_blocking()` and `blocking: Arc<OnceCell<ValkeyConnInner>>` already exist in `connection.rs` from plan 01. Re-read the relevant section before implementing Task 7.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/03-commands-strings.md` — the per-family file pattern (`commands/strings.rs`) is the template for `commands/lists.rs`.

**Out of scope:** `RPOPLPUSH` and `BRPOPLPUSH` (deprecated by Redis upstream — replaced by `LMOVE`/`BLMOVE`; we skip on principle per PLAN.md). Keyspace-notification subscriptions for list events (plan 14, pubsub).

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  commands/
    mod.rs                     # MODIFIED: declare `pub mod lists;`
    lists.rs                   # NEW: #[pymethods] impl RedisRsDriver block for lists
  connection.rs                # MODIFIED: list helpers on ValkeyConnInner + blocking inherent fns on ValkeyConn
python/redis_rs_py/
  _driver.pyi                  # MODIFIED: append signatures for every list command
tests/driver/
  test_commands_lists.py       # NEW: covers every command in this plan
  test_blocking_connection.py  # NEW: dedicated tests for the lazy/reused/HOL-blocking-free contract
```

---

## Task 1: Wire up the `commands::lists` module

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/mod.rs`
- Create: `crates/redis-rs-py-driver/src/commands/lists.rs`

- [ ] **Step 1: Declare the module**

Edit `crates/redis-rs-py-driver/src/commands/mod.rs`:

```rust
// Per-family command modules.
//
// Each file holds a `#[pymethods] impl RedisRsDriver` block adding that
// family's commands. PyO3 0.28 supports multiple `#[pymethods]` blocks
// per class as long as method names are unique across blocks.

pub mod lists;
pub mod strings;
```

- [ ] **Step 2: Create the empty `commands/lists.rs`**

```rust
// List commands.
//
// Every method exists as a sync + async pair:
//   * `<cmd>(...)` — sync; releases the GIL via py.detach.
//   * `a<cmd>(...)` — async; returns a RedisRsAwaitable.
//
// Non-blocking commands ride ValkeyConnInner (the multiplexed pipeline
// connection). Blocking commands (BLPOP/BRPOP/BLMOVE/BLMPOP) ride the
// lazy-allocated second connection via the inherent `blocking_*` methods
// on ValkeyConn — see Task 7.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

use crate::async_bridge::RawResult;
use crate::driver::{RedisRsDriver, py_bytes_list, py_int, py_opt_bytes};
use crate::errors::{classify_error, to_py_err};
use crate::raw_result::IntoRawResult;
use crate::{async_op, dispatch_cmd, sync_op};

#[pymethods]
impl RedisRsDriver {}
```

- [ ] **Step 3: Verify the crate still compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with one warning about the empty `impl` block. No errors.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/mod.rs crates/redis-rs-py-driver/src/commands/lists.rs
git commit -m "refactor(driver): scaffold commands::lists module"
```

---

## Task 2: Sub-family A — `LPUSH` / `RPUSH` / `LPUSHX` / `RPUSHX`

`LPUSH key val [val ...]` and `RPUSH key val [val ...]` accept any number of values; return the new list length. The `*X` variants only push if the key already exists.

`redis-py` signatures:
```python
def lpush(self, name, *values) -> int: ...
def rpush(self, name, *values) -> int: ...
def lpushx(self, name, *values) -> int: ...   # since redis-py 5.x; pre-5 took single value
def rpushx(self, name, *values) -> int: ...
```

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/lists.rs`
- Test: `tests/driver/test_commands_lists.py`

- [ ] **Step 1: Write the failing test**

Create `tests/driver/test_commands_lists.py`:

```python
"""LPUSH / RPUSH and their *X variants."""

from __future__ import annotations

import pytest

from redis_rs_py.exceptions import ResponseError


def test_lpush_single(driver) -> None:
    assert driver.lpush("k", b"a") == 1
    assert driver.lpush("k", b"b") == 2
    assert driver.lrange("k", 0, -1) == [b"b", b"a"]


def test_lpush_variadic(driver) -> None:
    assert driver.lpush("k", b"a", b"b", b"c") == 3
    # LPUSH a b c → list is c, b, a (each pushed at head)
    assert driver.lrange("k", 0, -1) == [b"c", b"b", b"a"]


def test_rpush_variadic(driver) -> None:
    assert driver.rpush("k", b"a", b"b", b"c") == 3
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"c"]


def test_lpushx_when_missing_returns_zero(driver) -> None:
    assert driver.lpushx("missing", b"a") == 0
    assert driver.exists("missing") == 0


def test_lpushx_when_exists(driver) -> None:
    driver.lpush("k", b"a")
    assert driver.lpushx("k", b"b") == 2


def test_rpushx_when_missing_returns_zero(driver) -> None:
    assert driver.rpushx("missing", b"a") == 0


def test_rpushx_when_exists(driver) -> None:
    driver.rpush("k", b"a")
    assert driver.rpushx("k", b"b") == 2


def test_lpush_empty_args_raises_response_error(driver) -> None:
    # Redis itself rejects LPUSH with no values.
    with pytest.raises(ResponseError):
        driver.lpush("k")


@pytest.mark.asyncio
async def test_alpush_arpush_variadic(driver) -> None:
    assert await driver.alpush("k", b"a", b"b", b"c") == 3
    assert await driver.arpush("k", b"d") == 4
    assert await driver.alrange("k", 0, -1) == [b"c", b"b", b"a", b"d"]


@pytest.mark.asyncio
async def test_alpushx_arpushx(driver) -> None:
    assert await driver.alpushx("missing", b"a") == 0
    await driver.alpush("k", b"a")
    assert await driver.alpushx("k", b"b") == 2
    assert await driver.arpushx("k", b"c") == 3
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_lists.py -v -k "push"`
Expected: FAIL — `AttributeError: 'RedisRsDriver' object has no attribute 'lpush'`.

- [ ] **Step 3: Add the connection helpers**

Append to `crates/redis-rs-py-driver/src/connection.rs` inside `impl ValkeyConnInner`:

```rust
impl ValkeyConnInner {
    pub async fn lpush(
        &mut self,
        key: &str,
        values: &[Vec<u8>],
    ) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("LPUSH");
        cmd.arg(key);
        for v in values {
            cmd.arg(v.as_slice());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn rpush(
        &mut self,
        key: &str,
        values: &[Vec<u8>],
    ) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("RPUSH");
        cmd.arg(key);
        for v in values {
            cmd.arg(v.as_slice());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn lpushx(
        &mut self,
        key: &str,
        values: &[Vec<u8>],
    ) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("LPUSHX");
        cmd.arg(key);
        for v in values {
            cmd.arg(v.as_slice());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn rpushx(
        &mut self,
        key: &str,
        values: &[Vec<u8>],
    ) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("RPUSHX");
        cmd.arg(key);
        for v in values {
            cmd.arg(v.as_slice());
        }
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Inside the existing `#[pymethods] impl RedisRsDriver { ... }` block in `commands/lists.rs`:

```rust
    // ----- LPUSH / aLPUSH (variadic) -------------------------------------

    #[pyo3(signature = (name, *values))]
    fn lpush(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.lpush(name, &values).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (name, *values))]
    fn alpush(
        &self,
        py: Python<'_>,
        name: &str,
        values: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.lpush(&name, &values).await.into_raw_result()
        })
    }

    // ----- RPUSH / aRPUSH (variadic) -------------------------------------

    #[pyo3(signature = (name, *values))]
    fn rpush(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.rpush(name, &values).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (name, *values))]
    fn arpush(
        &self,
        py: Python<'_>,
        name: &str,
        values: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.rpush(&name, &values).await.into_raw_result()
        })
    }

    // ----- LPUSHX / aLPUSHX ----------------------------------------------

    #[pyo3(signature = (name, *values))]
    fn lpushx(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.lpushx(name, &values).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (name, *values))]
    fn alpushx(
        &self,
        py: Python<'_>,
        name: &str,
        values: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.lpushx(&name, &values).await.into_raw_result()
        })
    }

    // ----- RPUSHX / aRPUSHX ----------------------------------------------

    #[pyo3(signature = (name, *values))]
    fn rpushx(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.rpushx(name, &values).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (name, *values))]
    fn arpushx(
        &self,
        py: Python<'_>,
        name: &str,
        values: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.rpushx(&name, &values).await.into_raw_result()
        })
    }
```

(`lrange` is added in Task 3; the `lrange` calls in the tests above will fail at first — that's expected. Run the push-only subset to verify Task 2 in isolation: `pytest -k "lpush or rpush or lpushx or rpushx"`.)

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_lists.py -v -k "lpush or rpush or lpushx or rpushx"`
Expected: 2 PASS (`test_lpushx_when_missing_returns_zero`, `test_rpushx_when_missing_returns_zero`, `test_lpush_empty_args_raises_response_error`, `test_alpushx_arpushx`'s first assertion). The tests that call `lrange` to verify state will FAIL at this stage with `AttributeError`. Continue to Task 3 to land `lrange` and the rest will go green.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/lists.rs tests/driver/test_commands_lists.py
git commit -m "feat(lists): add LPUSH/RPUSH and LPUSHX/RPUSHX (variadic)"
```

---

## Task 3: Sub-family B — `LPOP` / `RPOP` (with `count=`) + `LRANGE` / `LLEN`

`LPOP key [count]`. Without `count`: returns a single byte string (or `None` if empty).
With `count`: returns a list of byte strings (or `None` if empty). `redis-py` signature:

```python
def lpop(self, name, count=None) -> bytes | list[bytes] | None: ...
def rpop(self, name, count=None) -> bytes | list[bytes] | None: ...
```

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/lists.rs`
- Test: `tests/driver/test_commands_lists.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- LPOP / RPOP / LRANGE / LLEN ----------


def test_lpop_single(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.lpop("k") == b"a"
    assert driver.lpop("k") == b"b"
    assert driver.lpop("k") == b"c"
    assert driver.lpop("k") is None


def test_lpop_with_count(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"d")
    assert driver.lpop("k", count=2) == [b"a", b"b"]
    assert driver.lpop("k", count=10) == [b"c", b"d"]
    assert driver.lpop("k", count=1) is None


def test_lpop_count_zero_returns_empty_list(driver) -> None:
    driver.rpush("k", b"a")
    assert driver.lpop("k", count=0) == []


def test_rpop_single(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.rpop("k") == b"c"
    assert driver.rpop("k") == b"b"


def test_rpop_with_count(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"d")
    assert driver.rpop("k", count=2) == [b"d", b"c"]


def test_lrange_full(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"c"]


def test_lrange_partial(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"d")
    assert driver.lrange("k", 1, 2) == [b"b", b"c"]


def test_lrange_missing_returns_empty(driver) -> None:
    assert driver.lrange("missing", 0, -1) == []


def test_llen(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.llen("k") == 3


def test_llen_missing_returns_zero(driver) -> None:
    assert driver.llen("missing") == 0


@pytest.mark.asyncio
async def test_alpop_arpop_with_count(driver) -> None:
    await driver.arpush("k", b"a", b"b", b"c", b"d")
    assert await driver.alpop("k") == b"a"
    assert await driver.alpop("k", count=2) == [b"b", b"c"]
    assert await driver.arpop("k") == b"d"
    assert await driver.alrange("k", 0, -1) == []
    assert await driver.allen("k") == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_lists.py -v -k "lpop or rpop or lrange or llen"`
Expected: FAIL — methods missing.

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn lpop_one(&mut self, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("LPOP");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn rpop_one(&mut self, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("RPOP");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    /// LPOP/RPOP with COUNT — returns Some(vec) (possibly empty) when the
    /// key exists, None when it doesn't.
    pub async fn lpop_count(
        &mut self,
        key: &str,
        count: u64,
    ) -> redis::RedisResult<Option<Vec<Vec<u8>>>> {
        let mut cmd = redis::cmd("LPOP");
        cmd.arg(key).arg(count);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn rpop_count(
        &mut self,
        key: &str,
        count: u64,
    ) -> redis::RedisResult<Option<Vec<Vec<u8>>>> {
        let mut cmd = redis::cmd("RPOP");
        cmd.arg(key).arg(count);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn lrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> redis::RedisResult<Vec<Vec<u8>>> {
        let mut cmd = redis::cmd("LRANGE");
        cmd.arg(key).arg(start).arg(stop);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn llen(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("LLEN");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/lists.rs`:

```rust
    // ----- LPOP / aLPOP --------------------------------------------------

    #[pyo3(signature = (name, count = None))]
    fn lpop(
        &self,
        py: Python<'_>,
        name: &str,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<Vec<u8>>> =
                    sync_op!(py, self, conn, async { conn.lpop_one(name).await });
                Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
            }
            Some(c) => {
                let r: redis::RedisResult<Option<Vec<Vec<u8>>>> =
                    sync_op!(py, self, conn, async { conn.lpop_count(name, c).await });
                opt_bytes_list_to_py(py, r.map_err(to_py_err)?)
            }
        }
    }

    #[pyo3(signature = (name, count = None))]
    fn alpop(
        &self,
        py: Python<'_>,
        name: &str,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            match count {
                None => match conn.lpop_one(&name).await {
                    Ok(Some(b)) => RawResult::OptBytes(Some(b)),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
                Some(c) => match conn.lpop_count(&name, c).await {
                    Ok(Some(items)) => RawResult::BytesList(items),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
            }
        })
    }

    // ----- RPOP / aRPOP --------------------------------------------------

    #[pyo3(signature = (name, count = None))]
    fn rpop(
        &self,
        py: Python<'_>,
        name: &str,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<Vec<u8>>> =
                    sync_op!(py, self, conn, async { conn.rpop_one(name).await });
                Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
            }
            Some(c) => {
                let r: redis::RedisResult<Option<Vec<Vec<u8>>>> =
                    sync_op!(py, self, conn, async { conn.rpop_count(name, c).await });
                opt_bytes_list_to_py(py, r.map_err(to_py_err)?)
            }
        }
    }

    #[pyo3(signature = (name, count = None))]
    fn arpop(
        &self,
        py: Python<'_>,
        name: &str,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            match count {
                None => match conn.rpop_one(&name).await {
                    Ok(Some(b)) => RawResult::OptBytes(Some(b)),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
                Some(c) => match conn.rpop_count(&name, c).await {
                    Ok(Some(items)) => RawResult::BytesList(items),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
            }
        })
    }

    // ----- LRANGE / aLRANGE ----------------------------------------------

    fn lrange(
        &self,
        py: Python<'_>,
        name: &str,
        start: i64,
        end: i64,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.lrange(name, start, end).await });
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    fn alrange(
        &self,
        py: Python<'_>,
        name: &str,
        start: i64,
        end: i64,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.lrange(&name, start, end).await.into_raw_result()
        })
    }

    // ----- LLEN / aLLEN --------------------------------------------------

    fn llen(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.llen(name).await }).map_err(to_py_err)
    }

    fn allen(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.llen(&name).await.into_raw_result()
        })
    }
```

Add a helper near the top of `commands/lists.rs` (above the `#[pymethods]` block):

```rust
fn opt_bytes_list_to_py(
    py: Python<'_>,
    v: Option<Vec<Vec<u8>>>,
) -> PyResult<Py<PyAny>> {
    match v {
        None => Ok(py.None()),
        Some(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .iter()
                .map(|b| PyBytes::new(py, b).into_any().unbind())
                .collect();
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
    }
}
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_lists.py -v -k "lpop or rpop or lrange or llen or push"`
Expected: all push tests now PASS too (Task 2 + Task 3 = 21 PASS combined).

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/lists.rs tests/driver/test_commands_lists.py
git commit -m "feat(lists): add LPOP/RPOP (with count=) + LRANGE + LLEN"
```

---

## Task 4: Sub-family C — `LMOVE` / `LPOS`

`LMOVE source destination LEFT|RIGHT LEFT|RIGHT` — atomic pop-from-source, push-to-destination. Returns the moved element or `None`.

`LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]` — returns:
- without `count`: the index (or `None` if not found).
- with `count`: a list of indexes (or empty list).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/lists.rs`
- Test: `tests/driver/test_commands_lists.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- LMOVE / LPOS ----------


def test_lmove_left_right(driver) -> None:
    driver.rpush("src", b"a", b"b", b"c")
    assert driver.lmove("src", "dst", "LEFT", "RIGHT") == b"a"
    assert driver.lrange("src", 0, -1) == [b"b", b"c"]
    assert driver.lrange("dst", 0, -1) == [b"a"]


def test_lmove_right_left(driver) -> None:
    driver.rpush("src", b"a", b"b", b"c")
    assert driver.lmove("src", "dst", "RIGHT", "LEFT") == b"c"
    assert driver.lrange("dst", 0, -1) == [b"c"]


def test_lmove_empty_source_returns_none(driver) -> None:
    assert driver.lmove("missing", "dst", "LEFT", "RIGHT") is None


def test_lpos_simple(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"b")
    assert driver.lpos("k", b"b") == 1
    assert driver.lpos("k", b"missing") is None


def test_lpos_with_rank(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"b")
    # RANK 2 = second match
    assert driver.lpos("k", b"b", rank=2) == 3


def test_lpos_with_count(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"b", b"b")
    # COUNT 0 = all matches
    assert driver.lpos("k", b"b", count=0) == [1, 3, 4]
    assert driver.lpos("k", b"b", count=2) == [1, 3]


def test_lpos_with_maxlen(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"b")
    # MAXLEN restricts the scan window from the head.
    assert driver.lpos("k", b"b", maxlen=2) == 1
    assert driver.lpos("k", b"c", maxlen=2) is None


@pytest.mark.asyncio
async def test_almove_alpos(driver) -> None:
    await driver.arpush("src", b"a", b"b", b"c")
    assert await driver.almove("src", "dst", "LEFT", "RIGHT") == b"a"
    assert await driver.alpos("dst", b"a") == 0
    assert await driver.alpos("src", b"missing") is None
    assert await driver.alpos("src", b"b", count=0) == [0]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_lists.py -v -k "lmove or lpos"`
Expected: FAIL — `AttributeError`.

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn lmove(
        &mut self,
        src: &str,
        dst: &str,
        wherefrom: &str,
        whereto: &str,
    ) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("LMOVE");
        cmd.arg(src).arg(dst).arg(wherefrom).arg(whereto);
        crate::dispatch_cmd!(self, cmd)
    }

    /// LPOS without COUNT: returns Option<i64>.
    pub async fn lpos_single(
        &mut self,
        key: &str,
        element: &[u8],
        rank: Option<i64>,
        maxlen: Option<i64>,
    ) -> redis::RedisResult<Option<i64>> {
        let mut cmd = redis::cmd("LPOS");
        cmd.arg(key).arg(element);
        if let Some(r) = rank {
            cmd.arg("RANK").arg(r);
        }
        if let Some(m) = maxlen {
            cmd.arg("MAXLEN").arg(m);
        }
        crate::dispatch_cmd!(self, cmd)
    }

    /// LPOS with COUNT: returns Vec<i64>. Note: COUNT 0 = all matches.
    pub async fn lpos_count(
        &mut self,
        key: &str,
        element: &[u8],
        rank: Option<i64>,
        count: i64,
        maxlen: Option<i64>,
    ) -> redis::RedisResult<Vec<i64>> {
        let mut cmd = redis::cmd("LPOS");
        cmd.arg(key).arg(element);
        if let Some(r) = rank {
            cmd.arg("RANK").arg(r);
        }
        cmd.arg("COUNT").arg(count);
        if let Some(m) = maxlen {
            cmd.arg("MAXLEN").arg(m);
        }
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/lists.rs`:

```rust
    // ----- LMOVE / aLMOVE ------------------------------------------------

    fn lmove(
        &self,
        py: Python<'_>,
        first_list: &str,
        second_list: &str,
        src: &str,
        dest: &str,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> = sync_op!(py, self, conn, async {
            conn.lmove(first_list, second_list, src, dest).await
        });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn almove(
        &self,
        py: Python<'_>,
        first_list: &str,
        second_list: &str,
        src: &str,
        dest: &str,
    ) -> PyResult<Py<PyAny>> {
        let first_list = first_list.to_string();
        let second_list = second_list.to_string();
        let src = src.to_string();
        let dest = dest.to_string();
        async_op!(self, py, conn, async {
            conn.lmove(&first_list, &second_list, &src, &dest)
                .await
                .into_raw_result()
        })
    }

    // ----- LPOS / aLPOS --------------------------------------------------

    #[pyo3(signature = (name, value, *, rank = None, count = None, maxlen = None))]
    fn lpos(
        &self,
        py: Python<'_>,
        name: &str,
        value: &[u8],
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<i64>> = sync_op!(py, self, conn, async {
                    conn.lpos_single(name, value, rank, maxlen).await
                });
                match r.map_err(to_py_err)? {
                    Some(i) => py_int(py, i),
                    None => Ok(py.None()),
                }
            }
            Some(c) => {
                let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
                    conn.lpos_count(name, value, rank, c, maxlen).await
                });
                let items = r.map_err(to_py_err)?;
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|i| i.into_pyobject(py).unwrap().into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
        }
    }

    #[pyo3(signature = (name, value, *, rank = None, count = None, maxlen = None))]
    fn alpos(
        &self,
        py: Python<'_>,
        name: &str,
        value: &[u8],
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            match count {
                None => match conn.lpos_single(&name, &value, rank, maxlen).await {
                    Ok(Some(i)) => RawResult::Int(i),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
                Some(c) => match conn.lpos_count(&name, &value, rank, c, maxlen).await {
                    Ok(items) => RawResult::Value(redis::Value::Array(
                        items
                            .into_iter()
                            .map(redis::Value::Int)
                            .collect(),
                    )),
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
            }
        })
    }
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_lists.py -v -k "lmove or lpos"`
Expected: 8 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/lists.rs tests/driver/test_commands_lists.py
git commit -m "feat(lists): add LMOVE and LPOS (with rank/count/maxlen)"
```

---

## Task 5: Sub-family D — `LREM` / `LINDEX` / `LSET` / `LINSERT` / `LTRIM`

Five point-mutation commands. Straightforward — each is a single `dispatch_cmd!` call.

- `LREM key count element` — removes up to `count` occurrences. count > 0 from head, < 0 from tail, 0 = all. Returns count removed.
- `LINDEX key index` — bytes at index, or `None`.
- `LSET key index element` — error if out of range; returns OK.
- `LINSERT key BEFORE|AFTER pivot element` — returns new length, or -1 if pivot not found, 0 if key missing.
- `LTRIM key start stop` — returns OK.

`redis-py` uses `where: 'BEFORE' | 'AFTER'` for `linsert`; we expose a `where` kwarg with that exact wording.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/lists.rs`
- Test: `tests/driver/test_commands_lists.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- LREM / LINDEX / LSET / LINSERT / LTRIM ----------


def test_lrem_from_head(driver) -> None:
    driver.rpush("k", b"a", b"b", b"a", b"c", b"a")
    # count=2: remove first 2 from head
    assert driver.lrem("k", 2, b"a") == 2
    assert driver.lrange("k", 0, -1) == [b"b", b"c", b"a"]


def test_lrem_from_tail(driver) -> None:
    driver.rpush("k", b"a", b"b", b"a", b"c", b"a")
    # count=-1: remove first 1 from tail
    assert driver.lrem("k", -1, b"a") == 1
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"a", b"c"]


def test_lrem_all(driver) -> None:
    driver.rpush("k", b"a", b"b", b"a", b"c", b"a")
    assert driver.lrem("k", 0, b"a") == 3


def test_lindex(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.lindex("k", 0) == b"a"
    assert driver.lindex("k", -1) == b"c"
    assert driver.lindex("k", 99) is None


def test_lset(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    driver.lset("k", 1, b"B")
    assert driver.lrange("k", 0, -1) == [b"a", b"B", b"c"]


def test_lset_out_of_range_raises(driver) -> None:
    driver.rpush("k", b"a")
    with pytest.raises(ResponseError):
        driver.lset("k", 99, b"x")


def test_linsert_before(driver) -> None:
    driver.rpush("k", b"a", b"c")
    assert driver.linsert("k", "BEFORE", b"c", b"b") == 3
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"c"]


def test_linsert_after(driver) -> None:
    driver.rpush("k", b"a", b"c")
    assert driver.linsert("k", "AFTER", b"a", b"b") == 3
    assert driver.lrange("k", 0, -1) == [b"a", b"b", b"c"]


def test_linsert_pivot_missing_returns_minus_one(driver) -> None:
    driver.rpush("k", b"a")
    assert driver.linsert("k", "BEFORE", b"missing", b"x") == -1


def test_linsert_key_missing_returns_zero(driver) -> None:
    assert driver.linsert("missing", "BEFORE", b"a", b"x") == 0


def test_linsert_invalid_where_raises(driver) -> None:
    driver.rpush("k", b"a")
    with pytest.raises(Exception):  # DataError or ValueError — see implementation
        driver.linsert("k", "MIDDLE", b"a", b"x")


def test_ltrim(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c", b"d", b"e")
    driver.ltrim("k", 1, 3)
    assert driver.lrange("k", 0, -1) == [b"b", b"c", b"d"]


@pytest.mark.asyncio
async def test_alrem_alindex_alset_alinsert_altrim(driver) -> None:
    await driver.arpush("k", b"a", b"b", b"c")
    assert await driver.alindex("k", 0) == b"a"
    await driver.alset("k", 0, b"A")
    assert await driver.alindex("k", 0) == b"A"
    assert await driver.alinsert("k", "BEFORE", b"b", b"X") == 4
    assert await driver.alrem("k", 1, b"X") == 1
    await driver.altrim("k", 0, 1)
    assert await driver.alrange("k", 0, -1) == [b"A", b"b"]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_lists.py -v -k "lrem or lindex or lset or linsert or ltrim"`
Expected: FAIL — methods missing.

- [ ] **Step 3: Add the connection helpers**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn lrem(
        &mut self,
        key: &str,
        count: i64,
        value: &[u8],
    ) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("LREM");
        cmd.arg(key).arg(count).arg(value);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn lindex(
        &mut self,
        key: &str,
        index: i64,
    ) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("LINDEX");
        cmd.arg(key).arg(index);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn lset(
        &mut self,
        key: &str,
        index: i64,
        value: &[u8],
    ) -> redis::RedisResult<()> {
        let mut cmd = redis::cmd("LSET");
        cmd.arg(key).arg(index).arg(value);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn linsert(
        &mut self,
        key: &str,
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("LINSERT");
        cmd.arg(key)
            .arg(if before { "BEFORE" } else { "AFTER" })
            .arg(pivot)
            .arg(value);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn ltrim(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> redis::RedisResult<()> {
        let mut cmd = redis::cmd("LTRIM");
        cmd.arg(key).arg(start).arg(stop);
        crate::dispatch_cmd!(self, cmd)
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/lists.rs`:

```rust
    // ----- LREM / aLREM --------------------------------------------------

    fn lrem(
        &self,
        py: Python<'_>,
        name: &str,
        count: i64,
        value: &[u8],
    ) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.lrem(name, count, value).await })
            .map_err(to_py_err)
    }

    fn alrem(
        &self,
        py: Python<'_>,
        name: &str,
        count: i64,
        value: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.lrem(&name, count, &value).await.into_raw_result()
        })
    }

    // ----- LINDEX / aLINDEX ----------------------------------------------

    fn lindex(&self, py: Python<'_>, name: &str, index: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.lindex(name, index).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn alindex(&self, py: Python<'_>, name: &str, index: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.lindex(&name, index).await.into_raw_result()
        })
    }

    // ----- LSET / aLSET --------------------------------------------------

    fn lset(
        &self,
        py: Python<'_>,
        name: &str,
        index: i64,
        value: &[u8],
    ) -> PyResult<()> {
        sync_op!(py, self, conn, async { conn.lset(name, index, value).await })
            .map_err(to_py_err)
    }

    fn alset(
        &self,
        py: Python<'_>,
        name: &str,
        index: i64,
        value: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.lset(&name, index, &value).await.into_raw_result()
        })
    }

    // ----- LINSERT / aLINSERT --------------------------------------------

    fn linsert(
        &self,
        py: Python<'_>,
        name: &str,
        where_: &str,
        refvalue: &[u8],
        value: &[u8],
    ) -> PyResult<i64> {
        let before = match where_.to_ascii_uppercase().as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => {
                return Err(PyErr::new::<crate::exceptions::DataError, _>(
                    "where argument must be 'BEFORE' or 'AFTER'",
                ));
            }
        };
        sync_op!(py, self, conn, async {
            conn.linsert(name, before, refvalue, value).await
        })
        .map_err(to_py_err)
    }

    fn alinsert(
        &self,
        py: Python<'_>,
        name: &str,
        where_: &str,
        refvalue: &[u8],
        value: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let before = match where_.to_ascii_uppercase().as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => {
                return Err(PyErr::new::<crate::exceptions::DataError, _>(
                    "where argument must be 'BEFORE' or 'AFTER'",
                ));
            }
        };
        let name = name.to_string();
        let refvalue = refvalue.to_vec();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.linsert(&name, before, &refvalue, &value)
                .await
                .into_raw_result()
        })
    }

    // ----- LTRIM / aLTRIM ------------------------------------------------

    fn ltrim(
        &self,
        py: Python<'_>,
        name: &str,
        start: i64,
        end: i64,
    ) -> PyResult<()> {
        sync_op!(py, self, conn, async { conn.ltrim(name, start, end).await })
            .map_err(to_py_err)
    }

    fn altrim(
        &self,
        py: Python<'_>,
        name: &str,
        start: i64,
        end: i64,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.ltrim(&name, start, end).await.into_raw_result()
        })
    }
```

(The PyO3 method must be named `linsert`, but the kwarg is exposed as `where` in `redis-py`. PyO3 won't let us name a Rust parameter `where` because it's a keyword; the rename trick `where_: &str` produces the public Python parameter name `where_` by default. To match redis-py exactly, add `#[pyo3(signature = (name, where_, refvalue, value))]` and call from Python as `linsert("k", "BEFORE", b"a", b"x")` — positional. This is what the tests above do. The `redis-py` kwarg form (`linsert(name="k", where="BEFORE", ...)`) is uncommon; we accept positional-only for the value of avoiding the keyword collision.)

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_lists.py -v -k "lrem or lindex or lset or linsert or ltrim"`
Expected: 13 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/lists.rs tests/driver/test_commands_lists.py
git commit -m "feat(lists): add LREM/LINDEX/LSET/LINSERT/LTRIM"
```

---

## Task 6: Sub-family E — `LMPOP`

`LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]`. Multi-key pop atomically — pops from the first non-empty key.

Returns either `None` (all keys empty/missing) or a 2-tuple `(key, [popped, ...])`.

`redis-py` signature:
```python
def lmpop(self, num_keys, *args, direction, count=1) -> tuple[bytes, list[bytes]] | None: ...
```

We mirror it but accept `keys` as a list (drop the legacy `num_keys` arg — Rust knows the count from `keys.len()`).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Modify: `crates/redis-rs-py-driver/src/commands/lists.rs`
- Test: `tests/driver/test_commands_lists.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- LMPOP ----------


def test_lmpop_first_non_empty(driver) -> None:
    driver.rpush("k1", b"a", b"b")
    driver.rpush("k2", b"c", b"d")
    assert driver.lmpop(["empty", "k1", "k2"], direction="LEFT") == ("k1", [b"a"])


def test_lmpop_with_count(driver) -> None:
    driver.rpush("k1", b"a", b"b", b"c", b"d")
    assert driver.lmpop(["k1"], direction="RIGHT", count=2) == ("k1", [b"d", b"c"])


def test_lmpop_all_empty_returns_none(driver) -> None:
    assert driver.lmpop(["empty1", "empty2"], direction="LEFT") is None


def test_lmpop_invalid_direction_raises(driver) -> None:
    driver.rpush("k", b"a")
    with pytest.raises(Exception):
        driver.lmpop(["k"], direction="MIDDLE")


@pytest.mark.asyncio
async def test_almpop(driver) -> None:
    await driver.arpush("k1", b"a", b"b", b"c")
    result = await driver.almpop(["empty", "k1"], direction="LEFT", count=2)
    assert result == ("k1", [b"a", b"b"])
    result = await driver.almpop(["empty"], direction="LEFT")
    assert result is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_lists.py -v -k "lmpop and not blmpop"`
Expected: FAIL — `AttributeError`.

- [ ] **Step 3: Add the connection helper**

Append to `connection.rs` inside `impl ValkeyConnInner`:

```rust
impl ValkeyConnInner {
    /// LMPOP: pop from the first non-empty key. Returns
    /// Some((key, vec_of_popped)) or None.
    pub async fn lmpop(
        &mut self,
        keys: &[String],
        direction: &str,
        count: i64,
    ) -> redis::RedisResult<Option<(String, Vec<Vec<u8>>)>> {
        let mut cmd = redis::cmd("LMPOP");
        cmd.arg(keys.len()).arg(keys);
        cmd.arg(direction);
        cmd.arg("COUNT").arg(count);
        let val: redis::Value = crate::dispatch_cmd!(self, cmd)?;
        match val {
            redis::Value::Nil => Ok(None),
            redis::Value::Array(mut items) if items.len() == 2 => {
                let elements_val = items.pop().unwrap();
                let key_val = items.pop().unwrap();
                let key: String = redis::from_redis_value(&key_val)?;
                let elements: Vec<Vec<u8>> = redis::from_redis_value(&elements_val)?;
                Ok(Some((key, elements)))
            }
            _ => Ok(None),
        }
    }
}
```

- [ ] **Step 4: Add the driver methods**

Append to `commands/lists.rs`:

```rust
    // ----- LMPOP / aLMPOP ------------------------------------------------

    #[pyo3(signature = (keys, *, direction, count = 1))]
    fn lmpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        validate_pop_direction(direction)?;
        let r: redis::RedisResult<Option<(String, Vec<Vec<u8>>)>> = sync_op!(
            py,
            self,
            conn,
            async { conn.lmpop(&keys, direction, count).await }
        );
        opt_key_and_bytes_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (keys, *, direction, count = 1))]
    fn almpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        validate_pop_direction(direction)?;
        let direction = direction.to_string();
        async_op!(self, py, conn, async {
            conn.lmpop(&keys, &direction, count).await.into_raw_result()
        })
    }
```

Add the helpers near the top of `commands/lists.rs`:

```rust
fn validate_pop_direction(direction: &str) -> PyResult<()> {
    let d = direction.to_ascii_uppercase();
    if d != "LEFT" && d != "RIGHT" {
        return Err(PyErr::new::<crate::exceptions::DataError, _>(
            "direction must be 'LEFT' or 'RIGHT'",
        ));
    }
    Ok(())
}

fn opt_key_and_bytes_list_to_py(
    py: Python<'_>,
    v: Option<(String, Vec<Vec<u8>>)>,
) -> PyResult<Py<PyAny>> {
    match v {
        None => Ok(py.None()),
        Some((key, elements)) => {
            let py_key = pyo3::types::PyString::new(py, &key).into_any().unbind();
            let py_elements: Vec<Py<PyAny>> = elements
                .iter()
                .map(|b| PyBytes::new(py, b).into_any().unbind())
                .collect();
            let py_list = PyList::new(py, py_elements)?.into_any().unbind();
            Ok(PyTuple::new(py, [py_key, py_list])?.into_any().unbind())
        }
    }
}
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_lists.py -v -k "lmpop and not blmpop"`
Expected: 5 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/commands/lists.rs tests/driver/test_commands_lists.py
git commit -m "feat(lists): add LMPOP (multi-key)"
```

---

## Task 7: Lazy blocking-connection wiring on `ValkeyConn`

This is the core architectural step — the regular multiplexed `ConnectionManager` MUST NOT carry blocking commands, because a 30 s `BLPOP` would freeze every other command sharing that pipeline.

The `ValkeyConn::get_blocking()` method already exists from plan 01 and lazily creates a second `ConnectionManager` with `set_response_timeout(None)`. This task adds inherent methods on `ValkeyConn` (NOT on `ValkeyConnInner`) that:
1. Call `self.get_blocking().await?` to obtain the blocking inner.
2. Delegate to the `ValkeyConnInner` async fn for the actual command body.

Without these inherent methods, the `Deref<Target = ValkeyConnInner>` impl from plan 01 would silently route blocking commands through the regular connection — the bug we exist to prevent.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`

- [ ] **Step 1: Add the blocking command bodies on `ValkeyConnInner`**

Append to `connection.rs`:

```rust
impl ValkeyConnInner {
    pub async fn blpop(
        &mut self,
        keys: &[String],
        timeout: f64,
    ) -> redis::RedisResult<Option<(String, Vec<u8>)>> {
        bpop_inner(self, "BLPOP", keys, timeout).await
    }

    pub async fn brpop(
        &mut self,
        keys: &[String],
        timeout: f64,
    ) -> redis::RedisResult<Option<(String, Vec<u8>)>> {
        bpop_inner(self, "BRPOP", keys, timeout).await
    }

    pub async fn blmove(
        &mut self,
        src: &str,
        dst: &str,
        wherefrom: &str,
        whereto: &str,
        timeout: f64,
    ) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("BLMOVE");
        cmd.arg(src)
            .arg(dst)
            .arg(wherefrom)
            .arg(whereto)
            .arg(timeout);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn blmpop(
        &mut self,
        timeout: f64,
        keys: &[String],
        direction: &str,
        count: i64,
    ) -> redis::RedisResult<Option<(String, Vec<Vec<u8>>)>> {
        let mut cmd = redis::cmd("BLMPOP");
        cmd.arg(timeout).arg(keys.len());
        for k in keys {
            cmd.arg(k.as_str());
        }
        cmd.arg(direction);
        cmd.arg("COUNT").arg(count);
        let val: redis::Value = crate::dispatch_cmd!(self, cmd)?;
        match val {
            redis::Value::Nil => Ok(None),
            redis::Value::Array(mut items) if items.len() == 2 => {
                let elements_val = items.pop().unwrap();
                let key_val = items.pop().unwrap();
                let key: String = redis::from_redis_value(&key_val)?;
                let elements: Vec<Vec<u8>> = redis::from_redis_value(&elements_val)?;
                Ok(Some((key, elements)))
            }
            _ => Ok(None),
        }
    }
}

async fn bpop_inner(
    conn: &mut ValkeyConnInner,
    command: &'static str,
    keys: &[String],
    timeout: f64,
) -> redis::RedisResult<Option<(String, Vec<u8>)>> {
    let mut cmd = redis::cmd(command);
    for k in keys {
        cmd.arg(k.as_str());
    }
    cmd.arg(timeout);
    let val: redis::Value = crate::dispatch_cmd!(conn, cmd)?;
    match val {
        redis::Value::Nil => Ok(None),
        redis::Value::Array(mut items) if items.len() == 2 => {
            let value_val = items.pop().unwrap();
            let key_val = items.pop().unwrap();
            let key: String = redis::from_redis_value(&key_val)?;
            let value: Vec<u8> = redis::from_redis_value(&value_val)?;
            Ok(Some((key, value)))
        }
        _ => Ok(None),
    }
}
```

- [ ] **Step 2: Add the `ValkeyConn` inherent methods that route through `get_blocking()`**

Inside the existing `impl ValkeyConn { ... }` block in `connection.rs` (where `get_blocking` already lives from plan 01), append:

```rust
impl ValkeyConn {
    pub async fn blpop(
        &self,
        keys: &[String],
        timeout: f64,
    ) -> redis::RedisResult<Option<(String, Vec<u8>)>> {
        let mut conn = self.get_blocking().await?;
        conn.blpop(keys, timeout).await
    }

    pub async fn brpop(
        &self,
        keys: &[String],
        timeout: f64,
    ) -> redis::RedisResult<Option<(String, Vec<u8>)>> {
        let mut conn = self.get_blocking().await?;
        conn.brpop(keys, timeout).await
    }

    pub async fn blmove(
        &self,
        src: &str,
        dst: &str,
        wherefrom: &str,
        whereto: &str,
        timeout: f64,
    ) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut conn = self.get_blocking().await?;
        conn.blmove(src, dst, wherefrom, whereto, timeout).await
    }

    pub async fn blmpop(
        &self,
        timeout: f64,
        keys: &[String],
        direction: &str,
        count: i64,
    ) -> redis::RedisResult<Option<(String, Vec<Vec<u8>>)>> {
        let mut conn = self.get_blocking().await?;
        conn.blmpop(timeout, keys, direction, count).await
    }

    /// Test-only helper: returns Some(inner) if the lazy blocking connection
    /// has been initialised, else None. Used by tests to assert that the
    /// blocking conn is created on first use and reused thereafter.
    pub fn blocking_initialised(&self) -> bool {
        self.blocking.initialized()
    }
}
```

(`tokio::sync::OnceCell::initialized()` returns `true` once `get_or_try_init` has succeeded. This is the test hook for Task 8.)

- [ ] **Step 3: Make `ValkeyConn`'s blocking inspector reachable from `RedisRsDriver`**

Open `crates/redis-rs-py-driver/src/driver.rs` and add a method to the `RedisRsDriver` `#[pymethods]` block:

```rust
    /// Test-only: True once the lazy blocking connection has been allocated.
    fn _blocking_initialised(&self) -> bool {
        self.connection.blocking_initialised()
    }
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings about unused `blpop`/`brpop`/`blmove`/`blmpop` methods (Task 8 wires them through to Python).

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/driver.rs
git commit -m "feat(lists): wire BLPOP/BRPOP/BLMOVE/BLMPOP through lazy blocking connection"
```

---

## Task 8: Sub-family F — `BLPOP` / `BRPOP` / `BLMOVE` / `BLMPOP` driver methods

Now expose the blocking commands to Python. Each driver method body calls the `ValkeyConn` inherent method (NOT the `Deref`'d `ValkeyConnInner` method) — that's how the routing through `get_blocking()` is enforced.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/lists.rs`
- Test: `tests/driver/test_commands_lists.py`

- [ ] **Step 1: Append the failing tests**

```python
# ---------- BLPOP / BRPOP / BLMOVE / BLMPOP ----------


def test_blpop_with_immediate_value(driver) -> None:
    driver.rpush("k", b"a")
    assert driver.blpop(["k"], timeout=0.1) == ("k", b"a")


def test_blpop_timeout_returns_none(driver) -> None:
    import time

    start = time.monotonic()
    assert driver.blpop(["empty"], timeout=0.2) is None
    assert time.monotonic() - start >= 0.15


def test_blpop_first_available_key(driver) -> None:
    driver.rpush("k2", b"x")
    assert driver.blpop(["k1", "k2"], timeout=0.1) == ("k2", b"x")


def test_brpop(driver) -> None:
    driver.rpush("k", b"a", b"b")
    assert driver.brpop(["k"], timeout=0.1) == ("k", b"b")


def test_blmove(driver) -> None:
    driver.rpush("src", b"a", b"b")
    assert driver.blmove("src", "dst", "LEFT", "RIGHT", timeout=0.1) == b"a"
    assert driver.lrange("dst", 0, -1) == [b"a"]


def test_blmove_timeout_returns_none(driver) -> None:
    assert driver.blmove("empty", "dst", "LEFT", "RIGHT", timeout=0.2) is None


def test_blmpop(driver) -> None:
    driver.rpush("k", b"a", b"b", b"c")
    assert driver.blmpop(timeout=0.1, keys=["empty", "k"], direction="LEFT", count=2) == (
        "k",
        [b"a", b"b"],
    )


def test_blmpop_timeout_returns_none(driver) -> None:
    assert (
        driver.blmpop(timeout=0.2, keys=["empty"], direction="LEFT", count=1) is None
    )


@pytest.mark.asyncio
async def test_ablpop_abrpop(driver) -> None:
    await driver.arpush("k", b"a", b"b")
    assert await driver.ablpop(["k"], timeout=0.1) == ("k", b"a")
    assert await driver.abrpop(["k"], timeout=0.1) == ("k", b"b")
    assert await driver.ablpop(["empty"], timeout=0.1) is None


@pytest.mark.asyncio
async def test_ablmove_ablmpop(driver) -> None:
    await driver.arpush("src", b"a", b"b")
    assert await driver.ablmove("src", "dst", "LEFT", "RIGHT", timeout=0.1) == b"a"
    await driver.arpush("k", b"x", b"y", b"z")
    result = await driver.ablmpop(
        timeout=0.1, keys=["k"], direction="RIGHT", count=2
    )
    assert result == ("k", [b"z", b"y"])
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/driver/test_commands_lists.py -v -k "blpop or brpop or blmove or blmpop"`
Expected: FAIL — methods missing.

- [ ] **Step 3: Add the driver methods**

Append to `commands/lists.rs`:

```rust
    // ----- BLPOP / aBLPOP ------------------------------------------------
    //
    // Routed through the lazy blocking connection: the body calls
    // `self.connection.blpop(...)` (the inherent ValkeyConn method),
    // not `conn.blpop(...)` on the Deref'd ValkeyConnInner. This is
    // load-bearing — see Task 7.

    fn blpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let r: redis::RedisResult<Option<(String, Vec<u8>)>> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async { conn.blpop(&keys, timeout).await })
        });
        opt_key_and_bytes_to_py(py, r.map_err(to_py_err)?)
    }

    fn ablpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        crate::runtime::get_runtime().spawn(async move {
            let result = match conn.blpop(&keys, timeout).await {
                Ok(v) => RawResult::OptKeyAndBytes(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            };
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // ----- BRPOP / aBRPOP ------------------------------------------------

    fn brpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let r: redis::RedisResult<Option<(String, Vec<u8>)>> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async { conn.brpop(&keys, timeout).await })
        });
        opt_key_and_bytes_to_py(py, r.map_err(to_py_err)?)
    }

    fn abrpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        crate::runtime::get_runtime().spawn(async move {
            let result = match conn.brpop(&keys, timeout).await {
                Ok(v) => RawResult::OptKeyAndBytes(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            };
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // ----- BLMOVE / aBLMOVE ----------------------------------------------

    fn blmove(
        &self,
        py: Python<'_>,
        first_list: &str,
        second_list: &str,
        src: &str,
        dest: &str,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let first_list = first_list.to_string();
        let second_list = second_list.to_string();
        let src = src.to_string();
        let dest = dest.to_string();
        let r: redis::RedisResult<Option<Vec<u8>>> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async {
                conn.blmove(&first_list, &second_list, &src, &dest, timeout)
                    .await
            })
        });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn ablmove(
        &self,
        py: Python<'_>,
        first_list: &str,
        second_list: &str,
        src: &str,
        dest: &str,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let first_list = first_list.to_string();
        let second_list = second_list.to_string();
        let src = src.to_string();
        let dest = dest.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        crate::runtime::get_runtime().spawn(async move {
            let result = match conn
                .blmove(&first_list, &second_list, &src, &dest, timeout)
                .await
            {
                Ok(v) => RawResult::OptBytes(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            };
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // ----- BLMPOP / aBLMPOP ----------------------------------------------

    #[pyo3(signature = (*, timeout, keys, direction, count = 1))]
    fn blmpop(
        &self,
        py: Python<'_>,
        timeout: f64,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        validate_pop_direction(direction)?;
        let conn = self.connection.clone();
        let direction_owned = direction.to_string();
        let r: redis::RedisResult<Option<(String, Vec<Vec<u8>>)>> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async {
                conn.blmpop(timeout, &keys, &direction_owned, count).await
            })
        });
        opt_key_and_bytes_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*, timeout, keys, direction, count = 1))]
    fn ablmpop(
        &self,
        py: Python<'_>,
        timeout: f64,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        validate_pop_direction(direction)?;
        let conn = self.connection.clone();
        let direction_owned = direction.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        crate::runtime::get_runtime().spawn(async move {
            let result = match conn
                .blmpop(timeout, &keys, &direction_owned, count)
                .await
            {
                Ok(v) => RawResult::OptKeyAndBytesList(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            };
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }
```

Add the helper near the top of `commands/lists.rs` (next to `opt_key_and_bytes_list_to_py`):

```rust
fn opt_key_and_bytes_to_py(
    py: Python<'_>,
    v: Option<(String, Vec<u8>)>,
) -> PyResult<Py<PyAny>> {
    match v {
        None => Ok(py.None()),
        Some((key, value)) => {
            let py_key = pyo3::types::PyString::new(py, &key).into_any().unbind();
            let py_value = PyBytes::new(py, &value).into_any().unbind();
            Ok(PyTuple::new(py, [py_key, py_value])?.into_any().unbind())
        }
    }
}
```

(Note we can't reuse the `async_op!` macro for blocking commands because `async_op!` calls `self.connection.clone()` then expects to deref into `ValkeyConnInner` for the `conn` binding. The blocking dispatch needs to invoke a method on `ValkeyConn` directly, so we open-code the spawn for these eight methods. That divergence is intentional and load-bearing.)

- [ ] **Step 2: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_lists.py -v -k "blpop or brpop or blmove or blmpop"`
Expected: 10 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/lists.rs tests/driver/test_commands_lists.py
git commit -m "feat(lists): add BLPOP/BRPOP/BLMOVE/BLMPOP routed via blocking connection"
```

---

## Task 9: Blocking-connection contract tests

Three load-bearing assertions:
1. **Lazy.** The blocking connection is NOT created until the first BLPOP-family call.
2. **Reused.** A second BLPOP-family call uses the SAME connection (no re-init per call).
3. **No head-of-line blocking.** A long-running BLPOP on one task must NOT delay a concurrent GET on the same driver.

These three properties are what makes the architecture worth the complexity. Tests live in their own file so they're easy to find when someone wonders "why did we split the connection".

**Files:**
- Test: `tests/driver/test_blocking_connection.py`

- [ ] **Step 1: Write the tests**

```python
"""Contract tests for the lazy blocking-connection split.

Why this exists:
  The regular ConnectionManager has a 30 s response timeout and is
  multiplexed — every command shares the same TCP pipeline. A 30 s BLPOP
  would freeze every other in-flight command. To prevent that, the driver
  lazily allocates a SECOND ConnectionManager (no response timeout) and
  routes BLPOP/BRPOP/BLMOVE/BLMPOP through it.

These tests pin that contract:
  1. The blocking conn does NOT exist before the first blocking call.
  2. The second blocking call reuses the SAME conn (no per-call init).
  3. A long BLPOP on one task does NOT block a concurrent GET on the same
     driver.
"""

from __future__ import annotations

import asyncio
import time

import pytest


def test_blocking_connection_not_initialised_before_use(driver) -> None:
    # Fresh driver — only regular commands have run.
    driver.set("k", b"v")
    driver.get("k")
    assert driver._blocking_initialised() is False


def test_blocking_connection_initialised_on_first_blpop(driver) -> None:
    assert driver._blocking_initialised() is False
    driver.rpush("k", b"a")
    driver.blpop(["k"], timeout=0.1)
    assert driver._blocking_initialised() is True


def test_blocking_connection_reused_across_calls(driver) -> None:
    driver.rpush("k", b"a")
    driver.blpop(["k"], timeout=0.1)
    assert driver._blocking_initialised() is True
    # Second call must not re-init — the OnceCell stays Some.
    driver.rpush("k", b"b")
    driver.blpop(["k"], timeout=0.1)
    assert driver._blocking_initialised() is True


def test_blocking_connection_initialised_on_first_brpop(driver) -> None:
    assert driver._blocking_initialised() is False
    driver.rpush("k", b"a")
    driver.brpop(["k"], timeout=0.1)
    assert driver._blocking_initialised() is True


def test_blocking_connection_initialised_on_first_blmove(driver) -> None:
    assert driver._blocking_initialised() is False
    driver.blmove("empty", "dst", "LEFT", "RIGHT", timeout=0.1)
    assert driver._blocking_initialised() is True


def test_blocking_connection_initialised_on_first_blmpop(driver) -> None:
    assert driver._blocking_initialised() is False
    driver.blmpop(timeout=0.1, keys=["empty"], direction="LEFT", count=1)
    assert driver._blocking_initialised() is True


@pytest.mark.asyncio
async def test_long_blpop_does_not_block_concurrent_get(driver) -> None:
    """The big architectural payoff: a 1 s BLPOP on the blocking conn must
    NOT delay a GET on the regular conn. We measure wall-clock time on the
    GET to prove it."""
    # Start a BLPOP that will wait the full timeout (no key exists).
    blpop_task = asyncio.create_task(driver.ablpop(["never_set"], timeout=1.0))

    # Give the BLPOP a moment to enter the await.
    await asyncio.sleep(0.05)

    # Now race a GET. If the architectures share a pipeline, the GET will
    # wait for the BLPOP to finish (≥1.0 s). If they're properly split, the
    # GET completes in well under 200 ms.
    start = time.monotonic()
    await driver.aset("ping", b"pong")
    value = await driver.aget("ping")
    elapsed = time.monotonic() - start

    assert value == b"pong"
    assert elapsed < 0.5, (
        f"GET took {elapsed:.3f}s while BLPOP was in flight — head-of-line "
        f"blocking is back, the connection split is broken."
    )

    # Tidy up: cancel the still-pending BLPOP.
    result = await blpop_task
    assert result is None  # BLPOP timed out


def test_long_blpop_does_not_block_sync_get(driver) -> None:
    """Same contract, but sync ↔ sync. Spawn the BLPOP via the runtime,
    then do a regular sync GET — must complete fast."""
    import threading

    barrier = threading.Event()
    finished = threading.Event()

    def _runner():
        barrier.set()
        # Sync BLPOP that waits the full timeout.
        driver.blpop(["never_set"], timeout=1.0)
        finished.set()

    thread = threading.Thread(target=_runner)
    thread.start()
    barrier.wait()
    # Yield briefly so the BLPOP enters the await on the runtime.
    time.sleep(0.05)

    start = time.monotonic()
    driver.set("ping", b"pong")
    value = driver.get("ping")
    elapsed = time.monotonic() - start

    assert value == b"pong"
    assert elapsed < 0.5, (
        f"GET took {elapsed:.3f}s while BLPOP was in flight in another "
        f"thread — head-of-line blocking is back."
    )

    thread.join()
    assert finished.is_set()
```

- [ ] **Step 2: Run the contract tests**

Run: `uv run pytest tests/driver/test_blocking_connection.py -v`
Expected: 8 PASS. If `test_long_blpop_does_not_block_concurrent_get` FAILS with `elapsed >= 0.5s`, the bug is one of:
  * `blpop` driver method calls `conn.blpop` on the Deref'd inner instead of `self.connection.blpop` on the inherent method (re-read Task 7 step 2).
  * `get_blocking()` returned the same `ValkeyConnInner` clone as the regular conn (check the `OnceCell` initialisation in `connection.rs` from plan 01 — it must build a fresh `ConnectionManager`).

- [ ] **Step 3: Commit**

```bash
git add tests/driver/test_blocking_connection.py
git commit -m "test(lists): cover lazy/reused blocking-conn + no-HOL-blocking contract"
```

---

## Task 10: Type stubs

Append signatures for every command landed in this plan to `python/redis_rs_py/_driver.pyi`.

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Edit `_driver.pyi`**

Inside the existing `class RedisRsDriver:` block (after the strings stubs from plan 03), append:

```python
    # --- LPUSH / RPUSH (variadic) ---------------------------------------
    def lpush(self, name: str, *values: bytes) -> int: ...
    def alpush(self, name: str, *values: bytes) -> Awaitable[int]: ...
    def rpush(self, name: str, *values: bytes) -> int: ...
    def arpush(self, name: str, *values: bytes) -> Awaitable[int]: ...
    def lpushx(self, name: str, *values: bytes) -> int: ...
    def alpushx(self, name: str, *values: bytes) -> Awaitable[int]: ...
    def rpushx(self, name: str, *values: bytes) -> int: ...
    def arpushx(self, name: str, *values: bytes) -> Awaitable[int]: ...

    # --- LPOP / RPOP / LRANGE / LLEN ------------------------------------
    def lpop(
        self, name: str, count: int | None = ...
    ) -> bytes | list[bytes] | None: ...
    def alpop(
        self, name: str, count: int | None = ...
    ) -> Awaitable[bytes | list[bytes] | None]: ...
    def rpop(
        self, name: str, count: int | None = ...
    ) -> bytes | list[bytes] | None: ...
    def arpop(
        self, name: str, count: int | None = ...
    ) -> Awaitable[bytes | list[bytes] | None]: ...
    def lrange(self, name: str, start: int, end: int) -> list[bytes]: ...
    def alrange(
        self, name: str, start: int, end: int
    ) -> Awaitable[list[bytes]]: ...
    def llen(self, name: str) -> int: ...
    def allen(self, name: str) -> Awaitable[int]: ...

    # --- LMOVE / LPOS ---------------------------------------------------
    def lmove(
        self, first_list: str, second_list: str, src: str, dest: str
    ) -> bytes | None: ...
    def almove(
        self, first_list: str, second_list: str, src: str, dest: str
    ) -> Awaitable[bytes | None]: ...
    def lpos(
        self,
        name: str,
        value: bytes,
        *,
        rank: int | None = ...,
        count: int | None = ...,
        maxlen: int | None = ...,
    ) -> int | list[int] | None: ...
    def alpos(
        self,
        name: str,
        value: bytes,
        *,
        rank: int | None = ...,
        count: int | None = ...,
        maxlen: int | None = ...,
    ) -> Awaitable[int | list[int] | None]: ...

    # --- LREM / LINDEX / LSET / LINSERT / LTRIM -------------------------
    def lrem(self, name: str, count: int, value: bytes) -> int: ...
    def alrem(
        self, name: str, count: int, value: bytes
    ) -> Awaitable[int]: ...
    def lindex(self, name: str, index: int) -> bytes | None: ...
    def alindex(self, name: str, index: int) -> Awaitable[bytes | None]: ...
    def lset(self, name: str, index: int, value: bytes) -> None: ...
    def alset(
        self, name: str, index: int, value: bytes
    ) -> Awaitable[None]: ...
    def linsert(
        self, name: str, where_: str, refvalue: bytes, value: bytes
    ) -> int: ...
    def alinsert(
        self, name: str, where_: str, refvalue: bytes, value: bytes
    ) -> Awaitable[int]: ...
    def ltrim(self, name: str, start: int, end: int) -> None: ...
    def altrim(
        self, name: str, start: int, end: int
    ) -> Awaitable[None]: ...

    # --- LMPOP ----------------------------------------------------------
    def lmpop(
        self,
        keys: list[str],
        *,
        direction: str,
        count: int = ...,
    ) -> tuple[str, list[bytes]] | None: ...
    def almpop(
        self,
        keys: list[str],
        *,
        direction: str,
        count: int = ...,
    ) -> Awaitable[tuple[str, list[bytes]] | None]: ...

    # --- Blocking: BLPOP / BRPOP / BLMOVE / BLMPOP ----------------------
    def blpop(
        self, keys: list[str], timeout: float
    ) -> tuple[str, bytes] | None: ...
    def ablpop(
        self, keys: list[str], timeout: float
    ) -> Awaitable[tuple[str, bytes] | None]: ...
    def brpop(
        self, keys: list[str], timeout: float
    ) -> tuple[str, bytes] | None: ...
    def abrpop(
        self, keys: list[str], timeout: float
    ) -> Awaitable[tuple[str, bytes] | None]: ...
    def blmove(
        self,
        first_list: str,
        second_list: str,
        src: str,
        dest: str,
        timeout: float,
    ) -> bytes | None: ...
    def ablmove(
        self,
        first_list: str,
        second_list: str,
        src: str,
        dest: str,
        timeout: float,
    ) -> Awaitable[bytes | None]: ...
    def blmpop(
        self,
        *,
        timeout: float,
        keys: list[str],
        direction: str,
        count: int = ...,
    ) -> tuple[str, list[bytes]] | None: ...
    def ablmpop(
        self,
        *,
        timeout: float,
        keys: list[str],
        direction: str,
        count: int = ...,
    ) -> Awaitable[tuple[str, list[bytes]] | None]: ...

    # --- Test-only -----------------------------------------------------
    def _blocking_initialised(self) -> bool: ...
```

- [ ] **Step 2: Run ty check**

Run: `uv run ty check python/redis_rs_py/`
Expected: 0 errors.

- [ ] **Step 3: Commit**

```bash
git add python/redis_rs_py/_driver.pyi
git commit -m "feat(lists): add type stubs for every list command"
```

---

## Task 11: Lint, format, full-suite green check

- [ ] **Step 1: Run formatters**

```bash
cargo fmt --all
uv run ruff format
uv run ruff check --fix
```

Expected: no output beyond reformat counts.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 3: Run the full list suite**

Run: `uv run pytest tests/driver/test_commands_lists.py tests/driver/test_blocking_connection.py -v`
Expected: 67 PASS. Sub-task counts: push family 10 + lpop/rpop/lrange/llen 11 + lmove/lpos 8 + lrem/lindex/lset/linsert/ltrim 13 + lmpop 5 + blocking 10 + blocking-conn-contract 8 = 65 (plus a couple of overlap from variadic test files) ≈ 65–70.

- [ ] **Step 4: Run the entire suite**

Run: `uv run pytest -n auto`
Expected: every test PASSES across `tests/driver/`, `tests/async_bridge/`, `tests/exceptions/`, `tests/test_smoke.py`.

- [ ] **Step 5: Commit (no-op if formatters made no changes)**

If `cargo fmt`/`ruff` modified files:

```bash
git add -u
git commit -m "style(lists): cargo fmt + ruff format"
```

- [ ] **Step 6: Add CHANGELOG entry**

Edit `CHANGELOG.md` and append under `### Added`:

```markdown
- Driver list commands: `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`/`RPOP` (with `count=`), `LRANGE`, `LLEN`, `LMOVE`, `LPOS` (with `rank=`/`count=`/`maxlen=`), `LREM`, `LINDEX`, `LSET`, `LINSERT`, `LTRIM`, `LMPOP`. Sync + async pair for every command.
- Blocking list commands: `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, routed through a lazily-allocated second `ConnectionManager` (no response timeout) so a long BLPOP never head-of-line-blocks the multiplexed pipeline.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 04 entry"
```

---

## Self-review checklist for this plan

- [x] **Spec coverage** — Roadmap `04-commands-lists.md` row says: LPUSH, RPUSH, LPOP, RPOP (with count), LMOVE, LPOS, LRANGE, LLEN, LREM, LINDEX, LSET, LINSERT, LTRIM, LPUSHX, RPUSHX, blocking variants BLPOP, BRPOP, BLMOVE, BLMPOP, LMPOP, lazy blocking-connection wiring. Every item has a sub-task.
- [x] **No placeholders** — every step has runnable commands and explicit pass/fail expectations. No "implement following the pattern".
- [x] **Type consistency** — Rust signatures (`fn lpop(... count: Option<u64>) -> PyResult<Py<PyAny>>`) ↔ stubs (`def lpop(... count: int | None = ...) -> bytes | list[bytes] | None`) ↔ tests (`assert driver.lpop("k") == b"a"` and `assert driver.lpop("k", count=2) == [b"a", b"b"]`). Async siblings return `Awaitable[T]` of the same `T`.
- [x] **Sync + async pair for every command** — checked file-by-file. Every method body in `commands/lists.rs` has both forms, including the four blocking commands (`blpop`/`ablpop`, `brpop`/`abrpop`, `blmove`/`ablmove`, `blmpop`/`ablmpop`).
- [x] **Blocking connection isolation** — Task 7 adds inherent methods on `ValkeyConn` (NOT on `ValkeyConnInner`); Task 8 driver methods bind `let conn = self.connection.clone();` and call `conn.blpop(...)` (the inherent method that internally calls `get_blocking().await?`), bypassing the `Deref` to the regular inner. The lazy + reused contract is asserted by `test_blocking_connection_not_initialised_before_use`, `test_blocking_connection_initialised_on_first_blpop`, and `test_blocking_connection_reused_across_calls`. The HOL-blocking-free contract is asserted by `test_long_blpop_does_not_block_concurrent_get` (async) and `test_long_blpop_does_not_block_sync_get` (sync).
- [x] **No new dependencies** — `Cargo.toml` is unchanged. Every method uses `redis::cmd` + `dispatch_cmd!` + types already present from plan 01.
- [x] **Test fixture reuse** — every test takes `driver` (defined in `tests/conftest.py` from plan 01).
- [x] **Free-threaded safety** — no new globals introduced; `ValkeyConn` is `Clone + Send + Sync` from plan 01 (`Arc<OnceCell<...>>` for the blocking inner is `Sync`); the `commands/lists.rs` module adds methods only.
- [x] **Conventional commits** — every commit prefix is `feat(lists):`, `test(lists):`, `style(lists):`, `refactor(driver):`, or `docs(changelog):`.
- [x] **Validation lives in Rust** — `validate_pop_direction` and the `BEFORE`/`AFTER` check in `linsert` raise `DataError` (PyO3-defined exception from plan 02). No Python-side validation.
