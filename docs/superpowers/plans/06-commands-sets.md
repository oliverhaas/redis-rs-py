# Plan 06 — Set commands

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the full v0.1 set-command surface on `RedisRsDriver` — variadic membership mutators (`SADD`/`SREM`), enumeration (`SMEMBERS` returning Python `set`), membership tests (`SISMEMBER`/`SMISMEMBER`/`SCARD`), random sampling (`SPOP`/`SRANDMEMBER` with count semantics), inter-set algebra (`SINTER`/`SUNION`/`SDIFF`/`SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE`/`SINTERCARD`), atomic membership move (`SMOVE`), and cursor-based iteration (`SSCAN`). Each command ships as a sync (`sxxx`) + async (`asxxx`) pair backed by a live Valkey via testcontainers.

**Architecture:** Per the Plan-01 file-structure invariants, each command family lives in its own file. This plan creates `crates/redis-rs-py-driver/src/commands/sets.rs` with one `impl RedisRsDriver` block holding every set command. The new file slots into the `commands` module created by Plan 05 (`commands/mod.rs::pub mod sets;`). To return native Python `set` objects (matching redis-py's `decode_responses=False` default), this plan adds a `RawResult::SetOfBytes(Vec<Vec<u8>>)` variant whose `into_py` calls `PySet::new`. `SPOP`/`SRANDMEMBER` are tri-mode: with no `count` they return single bytes (or `None`); with `count` they return a Python `set` (or `list` for `SRANDMEMBER` with negative count, which allows repeats). `SINTERCARD` and `SSCAN` use `redis::cmd("...")` with explicit `dispatch_cmd!` calls.

**Tech Stack:** PyO3 0.28 (`#[pyclass]`, `#[pyo3(signature = ...)]`, `PySet`, `PyList`, `PyBytes`), tokio 1.x, redis 1.x (`AsyncCommands`, `Cmd`), testcontainers (Valkey 8.0) on the Python side. Python 3.14 + 3.14t.

**Reference material:**
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/01-foundation-async-bridge.md` — defines `async_op!`, `sync_op!`, `conn_method!`, `dispatch_cmd!`, `IntoRawResult`, and the `py_*` helper functions in `driver.rs`.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/05-commands-hashes.md` — establishes the `commands/` module-path convention this plan extends.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs:1645-1919` — cachex's existing implementations for `sadd`/`srem`/`smembers`/`sismember`/`scard`/`sinter`/`sunion`/`sdiff`/`smove`/`smismember`/`spop`/`srandmember`/`sdiffstore`/`sinterstore`/`sunionstore`. Cachex returns lists for `SMEMBERS` and friends; we widen to a Python `set` to match redis-py.
- redis-py `redis/commands/core.py::SetCommands` for the canonical sentinel value `count=None` → single result, `count=N` → collection result for `SPOP`/`SRANDMEMBER`.
- Redis docs: https://redis.io/commands/sintercard/ — the `LIMIT 0` sentinel disables the cap.

**Out of scope for this plan:**
- The high-level `Redis` façade method bindings — that's plan 10. This plan only exposes commands on the low-level `RedisRsDriver`.
- Set commands inside pipelines/transactions — plan 13.
- `decode_responses=True` mode — plan 12 (the bytes-vs-str flip happens in the façade decoder).
- A redis-py-shaped `SSCAN` async iterator — for v0.1 we expose the cursor-based primitive and leave iteration to the façade in plan 10.

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  commands/
    mod.rs                     # MODIFIED: add `pub mod sets;`
    sets.rs                    # NEW: every set command on RedisRsDriver
  raw_result.rs                # MODIFIED: add From for SetOfBytes-shaped returns
  async_bridge.rs              # MODIFIED: add RawResult::SetOfBytes variant
  driver.rs                    # MODIFIED: add py_set_of_bytes helper
python/
  redis_rs_py/
    _driver.pyi                # MODIFIED: add set-command method stubs
tests/
  driver/
    test_commands_sets.py      # NEW: end-to-end coverage of every set command
```

---

## Task 1: Add the `commands::sets` module path + `SetOfBytes` return shape

Wire the new module file and the new `RawResult` variant so subsequent tasks compile.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/mod.rs`
- Create: `crates/redis-rs-py-driver/src/commands/sets.rs`
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs`
- Modify: `crates/redis-rs-py-driver/src/raw_result.rs`
- Modify: `crates/redis-rs-py-driver/src/driver.rs`

- [ ] **Step 1: Add `pub mod sets;` to `commands/mod.rs`**

Edit `crates/redis-rs-py-driver/src/commands/mod.rs`:

```rust
// Per-family command modules. Each file re-opens `impl RedisRsDriver`
// with the commands for one Redis data-type family, plus any helpers
// that family needs.

pub mod hashes;
pub mod sets;
```

- [ ] **Step 2: Stub `commands/sets.rs`**

Create `crates/redis-rs-py-driver/src/commands/sets.rs`:

```rust
// Set commands on RedisRsDriver.
//
// Filled in by Plan 06 — for now an empty pyclass-extension block so the
// `mod sets;` declaration in commands/mod.rs compiles.

use crate::driver::RedisRsDriver;
use pyo3::prelude::*;

#[pymethods]
impl RedisRsDriver {}
```

- [ ] **Step 3: Add `SetOfBytes` to `RawResult`**

Edit `crates/redis-rs-py-driver/src/async_bridge.rs`. In the `RawResult` enum, add:

```rust
    SetOfBytes(Vec<Vec<u8>>),
```

In the `into_py` match block, add the arm (alongside `BytesList`):

```rust
            RawResult::SetOfBytes(items) => {
                let py_set = pyo3::types::PySet::empty(py)?;
                for b in items {
                    py_set.add(pyo3::types::PyBytes::new(py, &b))?;
                }
                Ok(py_set.into_any().unbind())
            }
```

- [ ] **Step 4: Add `py_set_of_bytes` helper to `driver.rs`**

Edit `crates/redis-rs-py-driver/src/driver.rs`. Add (alongside the other `py_*` helpers) — keep it `pub(crate)`:

```rust
#[allow(dead_code)]
pub(crate) fn py_set_of_bytes(py: Python<'_>, v: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
    let s = pyo3::types::PySet::empty(py)?;
    for b in v {
        s.add(pyo3::types::PyBytes::new(py, &b))?;
    }
    Ok(s.into_any().unbind())
}
```

- [ ] **Step 5: `IntoRawResult` impl is intentionally NOT added for `Vec<Vec<u8>>` → SetOfBytes**

The existing `From<Vec<Vec<u8>>> for RawResult` returns `BytesList` (a Python list). Sets need set-shaped output. Set bodies will explicitly construct `RawResult::SetOfBytes(...)` rather than relying on the `IntoRawResult` blanket impl. No edit to `raw_result.rs` is needed for this task.

- [ ] **Step 6: Verify the crate still compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with only unused-warnings about the new variant + helper.

- [ ] **Step 7: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/ crates/redis-rs-py-driver/src/async_bridge.rs crates/redis-rs-py-driver/src/driver.rs
git commit -m "feat(sets): scaffold commands/sets.rs and SetOfBytes RawResult"
```

---

## Task 2: SADD / SREM (variadic mutators)

Sub-task (a). Both are variadic and return the count of elements actually added/removed (existing/missing members are silently skipped).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/sets.rs`
- Test: `tests/driver/test_commands_sets.py`

- [ ] **Step 1: Write the failing test for the (a) sub-task**

Create `tests/driver/test_commands_sets.py`:

```python
"""Set command coverage on RedisRsDriver — sub-task (a): SADD/SREM."""

from __future__ import annotations

import pytest


# --- SADD ----------------------------------------------------------------

def test_sadd_returns_added_count(driver) -> None:
    assert driver.sadd("s", b"a", b"b", b"c") == 3
    # Re-adding existing members → 0 newly added.
    assert driver.sadd("s", b"a", b"b") == 0
    # Mix of new and existing.
    assert driver.sadd("s", b"a", b"d") == 1


def test_sadd_empty_members_returns_zero(driver) -> None:
    # Edge case: variadic with zero members must not crash; redis-py raises,
    # but we choose to return 0 for ergonomic safety. Document this in the
    # compatibility matrix in plan 17.
    assert driver.sadd("s") == 0


@pytest.mark.asyncio
async def test_asadd(driver) -> None:
    assert await driver.asadd("s", b"x", b"y") == 2


# --- SREM ----------------------------------------------------------------

def test_srem_returns_removed_count(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    assert driver.srem("s", b"a", b"missing") == 1
    assert driver.srem("s", b"b", b"c") == 2


def test_srem_missing_key_returns_zero(driver) -> None:
    assert driver.srem("missing", b"a") == 0


@pytest.mark.asyncio
async def test_asrem(driver) -> None:
    await driver.asadd("s", b"a", b"b")
    assert await driver.asrem("s", b"a") == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_sets.py -v`
Expected: every test FAILS with `AttributeError: 'builtins.RedisRsDriver' object has no attribute 'sadd'`.

- [ ] **Step 3: Implement SADD / SREM**

Replace `crates/redis-rs-py-driver/src/commands/sets.rs`:

```rust
// Set commands on RedisRsDriver.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PySet, PyTuple};
use redis::AsyncCommands;

use crate::async_bridge::RawResult;
use crate::driver::{py_bool, py_int, py_opt_bytes, py_set_of_bytes, RedisRsDriver};
use crate::errors::to_py_err;
use crate::raw_result::IntoRawResult;
use crate::{async_op, conn_method, dispatch_cmd, sync_op};

#[pymethods]
impl RedisRsDriver {
    // =====================================================================
    // (a) SADD / SREM
    // =====================================================================

    #[pyo3(signature = (key, *members))]
    fn sadd(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return py_int(py, 0);
        }
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.sadd(key, &members)));
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *members))]
    fn asadd(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn_method!(&mut conn, c, c.sadd(&key, &members));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn srem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return py_int(py, 0);
        }
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.srem(key, &members)));
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *members))]
    fn asrem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn_method!(&mut conn, c, c.srem(&key, &members));
            r.into_raw_result()
        })
    }
}
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_sets.py -v`
Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/sets.rs tests/driver/test_commands_sets.py
git commit -m "feat(sets): add SADD/SREM variadic mutators"
```

---

## Task 3: SMEMBERS / SCARD

Sub-task (b). `SMEMBERS` returns a Python `set[bytes]` — the load-bearing call to `RawResult::SetOfBytes` (or `py_set_of_bytes` on the sync side). `SCARD` returns int.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/sets.rs`
- Modify: `tests/driver/test_commands_sets.py`

- [ ] **Step 1: Append the (b) tests**

Append to `tests/driver/test_commands_sets.py`:

```python
# --- SMEMBERS ------------------------------------------------------------

def test_smembers_returns_python_set(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    got = driver.smembers("s")
    assert isinstance(got, set)
    assert got == {b"a", b"b", b"c"}


def test_smembers_missing_returns_empty_set(driver) -> None:
    got = driver.smembers("missing")
    assert isinstance(got, set)
    assert got == set()


@pytest.mark.asyncio
async def test_asmembers(driver) -> None:
    await driver.asadd("s", b"x", b"y")
    got = await driver.asmembers("s")
    assert isinstance(got, set)
    assert got == {b"x", b"y"}


# --- SCARD ---------------------------------------------------------------

def test_scard(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    assert driver.scard("s") == 3


def test_scard_missing_is_zero(driver) -> None:
    assert driver.scard("missing") == 0


@pytest.mark.asyncio
async def test_ascard(driver) -> None:
    await driver.asadd("s", b"a")
    assert await driver.ascard("s") == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_sets.py -v -k "smembers or scard"`
Expected: 6 FAIL.

- [ ] **Step 3: Implement SMEMBERS / SCARD**

Append inside the `#[pymethods]` block of `commands/sets.rs`:

```rust
    // =====================================================================
    // (b) SMEMBERS / SCARD
    // =====================================================================

    #[pyo3(signature = (key))]
    fn smembers(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.smembers(key)));
        py_set_of_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn asmembers(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                conn_method!(&mut conn, c, c.smembers(&key));
            match r {
                Ok(v) => RawResult::SetOfBytes(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (key))]
    fn scard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.scard(key)));
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ascard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn_method!(&mut conn, c, c.scard(&key));
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_sets.py -v -k "smembers or scard"`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/sets.rs tests/driver/test_commands_sets.py
git commit -m "feat(sets): add SMEMBERS/SCARD with Python set return"
```

---

## Task 4: SISMEMBER / SMISMEMBER

Sub-task (c). `SISMEMBER` returns bool, `SMISMEMBER` returns `list[bool]` matching the input order.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/sets.rs`
- Modify: `tests/driver/test_commands_sets.py`

- [ ] **Step 1: Append the (c) tests**

Append to `tests/driver/test_commands_sets.py`:

```python
# --- SISMEMBER -----------------------------------------------------------

def test_sismember_present(driver) -> None:
    driver.sadd("s", b"a", b"b")
    assert driver.sismember("s", b"a") is True


def test_sismember_absent(driver) -> None:
    driver.sadd("s", b"a")
    assert driver.sismember("s", b"missing") is False
    assert driver.sismember("missing-key", b"a") is False


@pytest.mark.asyncio
async def test_asismember(driver) -> None:
    await driver.asadd("s", b"a")
    assert await driver.asismember("s", b"a") is True


# --- SMISMEMBER ----------------------------------------------------------

def test_smismember_returns_list_of_bools(driver) -> None:
    driver.sadd("s", b"a", b"c")
    got = driver.smismember("s", b"a", b"b", b"c")
    assert isinstance(got, list)
    assert got == [True, False, True]


def test_smismember_missing_key(driver) -> None:
    assert driver.smismember("missing", b"a", b"b") == [False, False]


def test_smismember_empty_members_returns_empty_list(driver) -> None:
    driver.sadd("s", b"a")
    assert driver.smismember("s") == []


@pytest.mark.asyncio
async def test_asmismember(driver) -> None:
    await driver.asadd("s", b"a")
    assert await driver.asmismember("s", b"a", b"b") == [True, False]
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_sets.py -v -k "sismember or smismember"`
Expected: 7 FAIL.

- [ ] **Step 3: Add a `BoolList` variant to `RawResult` for the SMISMEMBER reply**

`SMISMEMBER` returns a flat array of integers (1/0) per member. To return a Python list of bools, add a new variant. Edit `crates/redis-rs-py-driver/src/async_bridge.rs`. In the enum:

```rust
    BoolList(Vec<bool>),
```

In `into_py`:

```rust
            RawResult::BoolList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|b| {
                        b.into_pyobject(py)
                            .map(|v| v.to_owned().into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
```

And the `From<Vec<bool>>` impl in `raw_result.rs`:

```rust
impl From<Vec<bool>> for RawResult {
    fn from(v: Vec<bool>) -> Self {
        RawResult::BoolList(v)
    }
}
```

- [ ] **Step 4: Implement SISMEMBER / SMISMEMBER in `commands/sets.rs`**

Append inside the `#[pymethods]` block:

```rust
    // =====================================================================
    // (c) SISMEMBER / SMISMEMBER
    // =====================================================================

    #[pyo3(signature = (key, member))]
    fn sismember(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<bool> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.sismember(key, member)));
        py_bool(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, member))]
    fn asismember(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<bool> =
                conn_method!(&mut conn, c, c.sismember(&key, &member));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn smismember(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return Ok(PyList::empty(py).into_any().unbind());
        }
        let r: redis::RedisResult<Vec<bool>> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("SMISMEMBER");
            cmd.arg(key);
            for m in &members {
                cmd.arg(m.as_slice());
            }
            dispatch_cmd!(&mut conn, cmd)
        });
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|b| {
                b.into_pyobject(py)
                    .map(|v| v.to_owned().into_any().unbind())
            })
            .collect::<PyResult<_>>()?;
        Ok(PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, *members))]
    fn asmismember(
        &self,
        py: Python<'_>,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::BoolList(Vec::new());
            }
            let mut cmd = redis::cmd("SMISMEMBER");
            cmd.arg(&key);
            for m in &members {
                cmd.arg(m.as_slice());
            }
            let r: redis::RedisResult<Vec<bool>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_sets.py -v -k "sismember or smismember"`
Expected: 7 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/sets.rs crates/redis-rs-py-driver/src/async_bridge.rs crates/redis-rs-py-driver/src/raw_result.rs tests/driver/test_commands_sets.py
git commit -m "feat(sets): add SISMEMBER/SMISMEMBER with BoolList RawResult"
```

---

## Task 5: SPOP / SRANDMEMBER (single vs count semantics)

Sub-task (d). Both have tri-mode return:
- `count=None` → single bytes (or `None` if empty/missing)
- `count` positive → Python `set[bytes]` (distinct members)
- For `SRANDMEMBER` only: `count` negative → Python `list[bytes]` (members with replacement, can repeat).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/sets.rs`
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs`
- Modify: `tests/driver/test_commands_sets.py`

- [ ] **Step 1: Append the (d) tests**

Append to `tests/driver/test_commands_sets.py`:

```python
# --- SPOP ----------------------------------------------------------------

def test_spop_no_count_returns_single_bytes(driver) -> None:
    driver.sadd("s", b"only")
    got = driver.spop("s")
    assert got == b"only"
    assert driver.scard("s") == 0


def test_spop_no_count_missing_returns_none(driver) -> None:
    assert driver.spop("missing") is None


def test_spop_with_count_returns_set(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    got = driver.spop("s", count=2)
    assert isinstance(got, set)
    assert len(got) == 2
    assert got.issubset({b"a", b"b", b"c"})
    assert driver.scard("s") == 1


def test_spop_with_count_zero_returns_empty_set(driver) -> None:
    driver.sadd("s", b"a")
    got = driver.spop("s", count=0)
    assert isinstance(got, set)
    assert got == set()


def test_spop_with_count_more_than_size(driver) -> None:
    driver.sadd("s", b"a", b"b")
    got = driver.spop("s", count=10)
    assert isinstance(got, set)
    assert got == {b"a", b"b"}


@pytest.mark.asyncio
async def test_aspop(driver) -> None:
    await driver.asadd("s", b"x")
    assert await driver.aspop("s") == b"x"


@pytest.mark.asyncio
async def test_aspop_with_count(driver) -> None:
    await driver.asadd("s", b"a", b"b")
    got = await driver.aspop("s", count=1)
    assert isinstance(got, set) and len(got) == 1


# --- SRANDMEMBER ---------------------------------------------------------

def test_srandmember_no_count_returns_single_bytes(driver) -> None:
    driver.sadd("s", b"a", b"b")
    got = driver.srandmember("s")
    assert got in (b"a", b"b")
    assert driver.scard("s") == 2  # SRANDMEMBER does not pop


def test_srandmember_no_count_missing_returns_none(driver) -> None:
    assert driver.srandmember("missing") is None


def test_srandmember_with_positive_count_returns_distinct_set(driver) -> None:
    driver.sadd("s", b"a", b"b", b"c")
    got = driver.srandmember("s", count=2)
    assert isinstance(got, set)
    assert len(got) == 2  # distinct
    assert got.issubset({b"a", b"b", b"c"})


def test_srandmember_with_negative_count_returns_list_with_repeats(driver) -> None:
    driver.sadd("s", b"only")
    got = driver.srandmember("s", count=-3)
    assert isinstance(got, list)
    assert got == [b"only", b"only", b"only"]


@pytest.mark.asyncio
async def test_asrandmember(driver) -> None:
    await driver.asadd("s", b"a")
    assert await driver.asrandmember("s") == b"a"
    assert await driver.asrandmember("s", count=1) == {b"a"}
    assert await driver.asrandmember("s", count=-2) == [b"a", b"a"]
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_sets.py -v -k "spop or srandmember"`
Expected: every test FAILS.

- [ ] **Step 3: Implement SPOP / SRANDMEMBER**

Append inside the `#[pymethods]` block in `commands/sets.rs`:

```rust
    // =====================================================================
    // (d) SPOP / SRANDMEMBER
    // =====================================================================

    #[pyo3(signature = (key, count=None))]
    fn spop(&self, py: Python<'_>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<Vec<u8>>> =
                    sync_op!(py, self, conn, conn_method!(&mut conn, c, c.spop(key)));
                Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
            }
            Some(n) => {
                let r: redis::RedisResult<Vec<Vec<u8>>> = sync_op!(py, self, conn, async {
                    let mut cmd = redis::cmd("SPOP");
                    cmd.arg(key).arg(n);
                    dispatch_cmd!(&mut conn, cmd)
                });
                py_set_of_bytes(py, r.map_err(to_py_err)?)
            }
        }
    }

    #[pyo3(signature = (key, count=None))]
    fn aspop(&self, py: Python<'_>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            match count {
                None => {
                    let r: redis::RedisResult<Option<Vec<u8>>> =
                        conn_method!(&mut conn, c, c.spop(&key));
                    r.into_raw_result()
                }
                Some(n) => {
                    let mut cmd = redis::cmd("SPOP");
                    cmd.arg(&key).arg(n);
                    let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
                    match r {
                        Ok(v) => RawResult::SetOfBytes(v),
                        Err(e) => crate::errors::classify(e),
                    }
                }
            }
        })
    }

    #[pyo3(signature = (key, count=None))]
    fn srandmember(&self, py: Python<'_>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<Vec<u8>>> =
                    sync_op!(py, self, conn, conn_method!(&mut conn, c, c.srandmember(key)));
                Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
            }
            Some(n) => {
                let r: redis::RedisResult<Vec<Vec<u8>>> = sync_op!(py, self, conn, async {
                    let mut cmd = redis::cmd("SRANDMEMBER");
                    cmd.arg(key).arg(n);
                    dispatch_cmd!(&mut conn, cmd)
                });
                let items = r.map_err(to_py_err)?;
                if n < 0 {
                    // Negative count → list (repeats allowed).
                    let py_items: Vec<Py<PyAny>> = items
                        .iter()
                        .map(|b| PyBytes::new(py, b).into_any().unbind())
                        .collect();
                    Ok(PyList::new(py, py_items)?.into_any().unbind())
                } else {
                    // Non-negative → distinct → set.
                    py_set_of_bytes(py, items)
                }
            }
        }
    }

    #[pyo3(signature = (key, count=None))]
    fn asrandmember(&self, py: Python<'_>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            match count {
                None => {
                    let r: redis::RedisResult<Option<Vec<u8>>> =
                        conn_method!(&mut conn, c, c.srandmember(&key));
                    r.into_raw_result()
                }
                Some(n) => {
                    let mut cmd = redis::cmd("SRANDMEMBER");
                    cmd.arg(&key).arg(n);
                    let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
                    match r {
                        Ok(v) if n < 0 => RawResult::BytesList(v),
                        Ok(v) => RawResult::SetOfBytes(v),
                        Err(e) => crate::errors::classify(e),
                    }
                }
            }
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_sets.py -v -k "spop or srandmember"`
Expected: 12 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/sets.rs tests/driver/test_commands_sets.py
git commit -m "feat(sets): add SPOP/SRANDMEMBER with single/count/negative-count modes"
```

---

## Task 6: SINTER / SUNION / SDIFF + STORE variants + SINTERCARD

Sub-task (e). The read variants take variadic keys and return `set[bytes]`. The STORE variants write the result to a destination key and return the cardinality. `SINTERCARD` accepts `LIMIT 0` (the sentinel for unlimited).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/sets.rs`
- Modify: `tests/driver/test_commands_sets.py`

- [ ] **Step 1: Append the (e) tests**

Append to `tests/driver/test_commands_sets.py`:

```python
# --- SINTER / SUNION / SDIFF (read) -------------------------------------

def test_sinter(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"2", b"3", b"4")
    got = driver.sinter("a", "b")
    assert isinstance(got, set)
    assert got == {b"2", b"3"}


def test_sinter_with_missing_key_is_empty(driver) -> None:
    driver.sadd("a", b"1", b"2")
    assert driver.sinter("a", "missing") == set()


def test_sunion(driver) -> None:
    driver.sadd("a", b"1", b"2")
    driver.sadd("b", b"2", b"3")
    got = driver.sunion("a", "b")
    assert isinstance(got, set)
    assert got == {b"1", b"2", b"3"}


def test_sdiff(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"2")
    assert driver.sdiff("a", "b") == {b"1", b"3"}


@pytest.mark.asyncio
async def test_asinter(driver) -> None:
    await driver.asadd("a", b"1", b"2")
    await driver.asadd("b", b"2")
    assert await driver.asinter("a", "b") == {b"2"}


# --- SINTERSTORE / SUNIONSTORE / SDIFFSTORE -----------------------------

def test_sinterstore(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"2", b"3", b"4")
    n = driver.sinterstore("dest", "a", "b")
    assert n == 2
    assert driver.smembers("dest") == {b"2", b"3"}


def test_sunionstore(driver) -> None:
    driver.sadd("a", b"1")
    driver.sadd("b", b"2")
    n = driver.sunionstore("dest", "a", "b")
    assert n == 2
    assert driver.smembers("dest") == {b"1", b"2"}


def test_sdiffstore(driver) -> None:
    driver.sadd("a", b"1", b"2")
    driver.sadd("b", b"2")
    n = driver.sdiffstore("dest", "a", "b")
    assert n == 1
    assert driver.smembers("dest") == {b"1"}


@pytest.mark.asyncio
async def test_asinterstore(driver) -> None:
    await driver.asadd("a", b"1")
    assert await driver.asinterstore("dest", "a") == 1


# --- SINTERCARD ---------------------------------------------------------

def test_sintercard_no_limit(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"2", b"3", b"4")
    assert driver.sintercard("a", "b") == 2


def test_sintercard_with_limit(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3", b"4")
    driver.sadd("b", b"1", b"2", b"3", b"4")
    # Cap result at 2.
    assert driver.sintercard("a", "b", limit=2) == 2


def test_sintercard_limit_zero_means_unlimited(driver) -> None:
    driver.sadd("a", b"1", b"2", b"3")
    driver.sadd("b", b"1", b"2", b"3")
    assert driver.sintercard("a", "b", limit=0) == 3


@pytest.mark.asyncio
async def test_asintercard(driver) -> None:
    await driver.asadd("a", b"1", b"2")
    await driver.asadd("b", b"2", b"3")
    assert await driver.asintercard("a", "b") == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_sets.py -v -k "sinter or sunion or sdiff"`
Expected: every test FAILS.

- [ ] **Step 3: Implement the inter-set algebra commands**

Append inside the `#[pymethods]` block of `commands/sets.rs`:

```rust
    // =====================================================================
    // (e) SINTER / SUNION / SDIFF + STORE + SINTERCARD
    // =====================================================================

    #[pyo3(signature = (*keys))]
    fn sinter(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.sinter(&keys)));
        py_set_of_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*keys))]
    fn asinter(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                conn_method!(&mut conn, c, c.sinter(&keys));
            match r {
                Ok(v) => RawResult::SetOfBytes(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (*keys))]
    fn sunion(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.sunion(&keys)));
        py_set_of_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*keys))]
    fn asunion(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                conn_method!(&mut conn, c, c.sunion(&keys));
            match r {
                Ok(v) => RawResult::SetOfBytes(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (*keys))]
    fn sdiff(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.sdiff(&keys)));
        py_set_of_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*keys))]
    fn asdiff(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                conn_method!(&mut conn, c, c.sdiff(&keys));
            match r {
                Ok(v) => RawResult::SetOfBytes(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (destination, *keys))]
    fn sinterstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.sinterstore(destination, &keys))
        );
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (destination, *keys))]
    fn asinterstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let destination = destination.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> =
                conn_method!(&mut conn, c, c.sinterstore(&destination, &keys));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (destination, *keys))]
    fn sunionstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.sunionstore(destination, &keys))
        );
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (destination, *keys))]
    fn asunionstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let destination = destination.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> =
                conn_method!(&mut conn, c, c.sunionstore(&destination, &keys));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (destination, *keys))]
    fn sdiffstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.sdiffstore(destination, &keys))
        );
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (destination, *keys))]
    fn asdiffstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let destination = destination.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> =
                conn_method!(&mut conn, c, c.sdiffstore(&destination, &keys));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*keys, limit=None))]
    fn sintercard(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("SINTERCARD");
            cmd.arg(keys.len());
            for k in &keys {
                cmd.arg(k);
            }
            if let Some(lim) = limit {
                cmd.arg("LIMIT").arg(lim);
            }
            dispatch_cmd!(&mut conn, cmd)
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*keys, limit=None))]
    fn asintercard(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("SINTERCARD");
            cmd.arg(keys.len());
            for k in &keys {
                cmd.arg(k);
            }
            if let Some(lim) = limit {
                cmd.arg("LIMIT").arg(lim);
            }
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_sets.py -v -k "sinter or sunion or sdiff"`
Expected: 13 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/sets.rs tests/driver/test_commands_sets.py
git commit -m "feat(sets): add SINTER/SUNION/SDIFF families plus SINTERCARD"
```

---

## Task 7: SMOVE

Sub-task (f). Atomically moves a member from one set to another. Returns bool indicating whether the move happened (the member existed in the source).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/sets.rs`
- Modify: `tests/driver/test_commands_sets.py`

- [ ] **Step 1: Append the (f) tests**

Append to `tests/driver/test_commands_sets.py`:

```python
# --- SMOVE ---------------------------------------------------------------

def test_smove_member_present(driver) -> None:
    driver.sadd("src", b"a", b"b")
    driver.sadd("dst", b"x")
    assert driver.smove("src", "dst", b"a") is True
    assert driver.smembers("src") == {b"b"}
    assert driver.smembers("dst") == {b"a", b"x"}


def test_smove_member_absent(driver) -> None:
    driver.sadd("src", b"a")
    assert driver.smove("src", "dst", b"missing") is False


def test_smove_already_in_destination(driver) -> None:
    driver.sadd("src", b"a")
    driver.sadd("dst", b"a")
    # Per Redis: removed from src, dst unchanged but the move "succeeded".
    assert driver.smove("src", "dst", b"a") is True
    assert driver.scard("src") == 0
    assert driver.smembers("dst") == {b"a"}


@pytest.mark.asyncio
async def test_asmove(driver) -> None:
    await driver.asadd("src", b"x")
    assert await driver.asmove("src", "dst", b"x") is True
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_sets.py -v -k smove`
Expected: 4 FAIL.

- [ ] **Step 3: Implement SMOVE**

Append inside the `#[pymethods]` block:

```rust
    // =====================================================================
    // (f) SMOVE
    // =====================================================================

    #[pyo3(signature = (source, destination, member))]
    fn smove(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        member: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<bool> = sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.smove(source, destination, member))
        );
        py_bool(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (source, destination, member))]
    fn asmove(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        member: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let source = source.to_string();
        let destination = destination.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<bool> =
                conn_method!(&mut conn, c, c.smove(&source, &destination, &member));
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_sets.py -v -k smove`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/sets.rs tests/driver/test_commands_sets.py
git commit -m "feat(sets): add SMOVE atomic membership move"
```

---

## Task 8: SSCAN

Sub-task (g). Cursor-based iteration with `MATCH`/`COUNT`. Returns `(cursor: int, members: set[bytes])`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/sets.rs`
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs`
- Modify: `tests/driver/test_commands_sets.py`

- [ ] **Step 1: Append the (g) tests**

Append to `tests/driver/test_commands_sets.py`:

```python
# --- SSCAN ---------------------------------------------------------------

def test_sscan_full_iteration(driver) -> None:
    expected = {f"m{i}".encode() for i in range(20)}
    driver.sadd("s", *expected)

    seen: set[bytes] = set()
    cursor = 0
    while True:
        cursor, batch = driver.sscan("s", cursor=cursor)
        assert isinstance(batch, set)
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == expected


def test_sscan_with_match(driver) -> None:
    driver.sadd("s", b"foo:1", b"foo:2", b"bar:1")
    cursor = 0
    seen: set[bytes] = set()
    while True:
        cursor, batch = driver.sscan("s", cursor=cursor, match="foo:*")
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == {b"foo:1", b"foo:2"}


def test_sscan_with_count(driver) -> None:
    driver.sadd("s", *[f"k{i}".encode() for i in range(50)])
    cursor, batch = driver.sscan("s", cursor=0, count=10)
    assert isinstance(batch, set)
    seen: set[bytes] = set(batch)
    while cursor != 0:
        cursor, batch = driver.sscan("s", cursor=cursor, count=10)
        seen.update(batch)
    assert len(seen) == 50


@pytest.mark.asyncio
async def test_asscan(driver) -> None:
    await driver.asadd("s", b"a", b"b")
    cursor, batch = await driver.asscan("s", cursor=0)
    assert isinstance(batch, set)
    assert batch.issubset({b"a", b"b"})
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_sets.py -v -k sscan`
Expected: 4 FAIL.

- [ ] **Step 3: Add the `SScan` variant to `RawResult`**

Edit `crates/redis-rs-py-driver/src/async_bridge.rs`. In the enum:

```rust
    SScan { cursor: u64, members: Vec<Vec<u8>> },
```

In `into_py`:

```rust
            RawResult::SScan { cursor, members } => {
                let cursor_py = cursor.into_pyobject(py)?.into_any().unbind();
                let py_set = pyo3::types::PySet::empty(py)?;
                for b in members {
                    py_set.add(pyo3::types::PyBytes::new(py, &b))?;
                }
                Ok(pyo3::types::PyTuple::new(
                    py,
                    [cursor_py, py_set.into_any().unbind()],
                )?
                .into_any()
                .unbind())
            }
```

- [ ] **Step 4: Implement SSCAN in `commands/sets.rs`**

Append inside the `#[pymethods]` block:

```rust
    // =====================================================================
    // (g) SSCAN
    // =====================================================================

    #[pyo3(signature = (key, *, cursor=0, match=None, count=None))]
    #[allow(non_snake_case)]
    fn sscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("SSCAN");
            cmd.arg(key).arg(cursor);
            if let Some(p) = &r#match {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            dispatch_cmd!(&mut conn, cmd)
        });
        let value = r.map_err(to_py_err)?;
        let (cursor, members) = parse_sscan_reply(value)?;
        let cursor_py = cursor.into_pyobject(py)?.into_any().unbind();
        let py_set = py_set_of_bytes(py, members)?;
        Ok(PyTuple::new(py, [cursor_py, py_set])?.into_any().unbind())
    }

    #[pyo3(signature = (key, *, cursor=0, match=None, count=None))]
    #[allow(non_snake_case)]
    fn asscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("SSCAN");
            cmd.arg(&key).arg(cursor);
            if let Some(p) = &r#match {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => match parse_sscan_reply(v) {
                    Ok((cursor, members)) => RawResult::SScan { cursor, members },
                    Err(e) => RawResult::Error(
                        crate::exceptions::ExceptionClass::ResponseError,
                        e.to_string(),
                    ),
                },
                Err(e) => crate::errors::classify(e),
            }
        })
    }
```

Outside the `#[pymethods]` block, add the helper:

```rust
fn parse_sscan_reply(value: redis::Value) -> PyResult<(u64, Vec<Vec<u8>>)> {
    if let redis::Value::Array(items) = value
        && items.len() == 2
    {
        let mut iter = items.into_iter();
        let cursor_v = iter.next().unwrap();
        let payload = iter.next().unwrap();
        let cursor: u64 = match cursor_v {
            redis::Value::BulkString(b) => std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            redis::Value::Int(n) => n as u64,
            _ => 0,
        };
        let members = match payload {
            redis::Value::Array(items) => items
                .into_iter()
                .filter_map(|item| match item {
                    redis::Value::BulkString(b) => Some(b),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        return Ok((cursor, members));
    }
    Err(pyo3::exceptions::PyValueError::new_err(
        "SSCAN reply did not match the [cursor, members] shape",
    ))
}
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_sets.py -v -k sscan`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/sets.rs crates/redis-rs-py-driver/src/async_bridge.rs tests/driver/test_commands_sets.py
git commit -m "feat(sets): add SSCAN with MATCH/COUNT cursor iteration"
```

---

## Task 9: Update `_driver.pyi` stubs for every set command

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Append the set-command stubs**

Append to the `class RedisRsDriver:` block in `python/redis_rs_py/_driver.pyi`:

```python
    # --- Sets (Plan 06) --------------------------------------------------
    def sadd(self, key: str, *members: bytes) -> int: ...
    def asadd(self, key: str, *members: bytes) -> Awaitable[int]: ...
    def srem(self, key: str, *members: bytes) -> int: ...
    def asrem(self, key: str, *members: bytes) -> Awaitable[int]: ...
    def smembers(self, key: str) -> set[bytes]: ...
    def asmembers(self, key: str) -> Awaitable[set[bytes]]: ...
    def scard(self, key: str) -> int: ...
    def ascard(self, key: str) -> Awaitable[int]: ...
    def sismember(self, key: str, member: bytes) -> bool: ...
    def asismember(self, key: str, member: bytes) -> Awaitable[bool]: ...
    def smismember(self, key: str, *members: bytes) -> list[bool]: ...
    def asmismember(self, key: str, *members: bytes) -> Awaitable[list[bool]]: ...
    def spop(
        self, key: str, count: int | None = ...
    ) -> bytes | set[bytes] | None: ...
    def aspop(
        self, key: str, count: int | None = ...
    ) -> Awaitable[bytes | set[bytes] | None]: ...
    def srandmember(
        self, key: str, count: int | None = ...
    ) -> bytes | set[bytes] | list[bytes] | None: ...
    def asrandmember(
        self, key: str, count: int | None = ...
    ) -> Awaitable[bytes | set[bytes] | list[bytes] | None]: ...
    def sinter(self, *keys: str) -> set[bytes]: ...
    def asinter(self, *keys: str) -> Awaitable[set[bytes]]: ...
    def sunion(self, *keys: str) -> set[bytes]: ...
    def asunion(self, *keys: str) -> Awaitable[set[bytes]]: ...
    def sdiff(self, *keys: str) -> set[bytes]: ...
    def asdiff(self, *keys: str) -> Awaitable[set[bytes]]: ...
    def sinterstore(self, destination: str, *keys: str) -> int: ...
    def asinterstore(self, destination: str, *keys: str) -> Awaitable[int]: ...
    def sunionstore(self, destination: str, *keys: str) -> int: ...
    def asunionstore(self, destination: str, *keys: str) -> Awaitable[int]: ...
    def sdiffstore(self, destination: str, *keys: str) -> int: ...
    def asdiffstore(self, destination: str, *keys: str) -> Awaitable[int]: ...
    def sintercard(self, *keys: str, limit: int | None = ...) -> int: ...
    def asintercard(self, *keys: str, limit: int | None = ...) -> Awaitable[int]: ...
    def smove(self, source: str, destination: str, member: bytes) -> bool: ...
    def asmove(
        self, source: str, destination: str, member: bytes
    ) -> Awaitable[bool]: ...
    def sscan(
        self,
        key: str,
        *,
        cursor: int = ...,
        match: str | None = ...,
        count: int | None = ...,
    ) -> tuple[int, set[bytes]]: ...
    def asscan(
        self,
        key: str,
        *,
        cursor: int = ...,
        match: str | None = ...,
        count: int | None = ...,
    ) -> Awaitable[tuple[int, set[bytes]]]: ...
```

- [ ] **Step 2: Run ty + ruff**

```bash
uv run ty check python/redis_rs_py/
uv run ruff check
uv run ruff format --check
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add python/redis_rs_py/_driver.pyi
git commit -m "feat(sets): add type stubs for all set commands"
```

---

## Task 10: Final lint pass + free-threaded smoke + CHANGELOG

**Files:** none modified — verification + CHANGELOG.

- [ ] **Step 1: Run linters**

```bash
uv run ruff check
uv run ruff format --check
uv run ty check python/redis_rs_py/
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: all green.

- [ ] **Step 2: Run the full sets test file**

```bash
uv run pytest tests/driver/test_commands_sets.py -v
```

Expected: every test PASSES (no FAIL).

- [ ] **Step 3: Run the suite under cp314t**

```bash
.venv-ft/bin/uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
.venv-ft/bin/uv run pytest tests/driver/test_commands_sets.py -n auto
```

Expected: same green.

- [ ] **Step 4: Add CHANGELOG entry**

Append under `### Added` in `CHANGELOG.md`:

```markdown
- Set commands: `SADD`, `SREM` (variadic), `SMEMBERS` (returns Python `set[bytes]`), `SISMEMBER` (returns bool), `SMISMEMBER` (returns `list[bool]`), `SCARD`, `SINTER`/`SUNION`/`SDIFF` (variadic, return Python `set[bytes]`), `SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE`, `SINTERCARD` (with `limit=`, `0` = unlimited), `SMOVE`, `SPOP` (with optional `count=` — single bytes / set / None semantics), `SRANDMEMBER` (with optional `count=` — single bytes / set / list-with-repeats for negative count), `SSCAN` (with `match=`/`count=`).
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 06 entry"
```

---

## Self-review checklist for this plan

- [x] Spec coverage — every command in the assignment block has a sub-task: SADD (variadic), SREM (variadic), SMEMBERS (set[bytes]), SISMEMBER (bool), SMISMEMBER (list[bool]), SCARD, SINTER/SUNION/SDIFF (variadic, set[bytes]), SINTERSTORE/SUNIONSTORE/SDIFFSTORE, SINTERCARD (with limit=), SMOVE, SPOP (count single/set semantics), SRANDMEMBER (count single/set/negative-list semantics), SSCAN.
- [x] No placeholders: every step ships actual code, every test step ships an explicit pass/fail expectation.
- [x] Type consistency: Rust signatures (`smembers(&self, py, key: &str) -> Py<PyAny>` returning `py_set_of_bytes`) match `.pyi` stubs (`def smembers(self, key: str) -> set[bytes]`) match test usage (`assert isinstance(driver.smembers("s"), set)`).
- [x] `RawResult::SetOfBytes(Vec<Vec<u8>>)` added with `into_py` calling `PySet::new` — implements the assignment's "Make sure `set` (Python type) is constructed in Rust" requirement.
- [x] SPOP/SRANDMEMBER tri-mode behavior: `count=None` → bytes/None; `count >= 0` → set; SRANDMEMBER `count < 0` → list (repeats) — all three modes covered by tests.
- [x] All file paths absolute or repo-relative-from-root.
- [x] Sub-task grouping matches the assignment: (a) SADD/SREM, (b) SMEMBERS/SCARD, (c) SISMEMBER/SMISMEMBER, (d) SPOP/SRANDMEMBER, (e) SINTER/SUNION/SDIFF + STORE + SINTERCARD, (f) SMOVE, (g) SSCAN.
- [x] Frequent commits: 10 across 10 tasks, each independently revertable, each with conventional-commit `feat(sets):` / `docs(changelog):` prefixes.
- [x] `commands/sets.rs` module path declared from `commands/mod.rs::pub mod sets;` (which Plan 05 already wired into `lib.rs::mod commands;`).
- [x] Out-of-scope items deferred to later plans: façade bindings (10), pipelines (13), `decode_responses=True` (12), iterator helper (10).
