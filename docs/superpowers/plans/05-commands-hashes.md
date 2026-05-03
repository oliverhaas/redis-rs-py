# Plan 05 — Hash commands

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the full v0.1 hash-command surface on `RedisRsDriver` — single-field accessors (`HGET`/`HSET`/`HSETNX`), multi-field accessors (`HMSET`/`HGETALL`/`HMGET`), maintenance (`HDEL`/`HEXISTS`/`HLEN`), enumeration (`HKEYS`/`HVALS`/`HRANDFIELD`), counters (`HINCRBY`/`HINCRBYFLOAT`), the cursor-based `HSCAN` (with `MATCH`/`COUNT`/`NOVALUES` modifiers), and the Redis 7.4 hash-field TTL family (`HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT`/`HEXPIRETIME`/`HPEXPIRETIME`/`HTTL`/`HPTTL`/`HPERSIST` with the `NX`/`XX`/`GT`/`LT` modifier matrix). Each command ships as a sync (`hxxx`) + async (`ahxxx`) pair backed by a live Valkey via testcontainers.

**Architecture:** Per the Plan-01 file-structure invariants, each command family lives in its own file. This plan creates `crates/redis-rs-py-driver/src/commands/hashes.rs` with one `impl RedisRsDriver` block holding every hash command. The `commands` directory becomes a Rust module tree (`commands::hashes`), declared from `lib.rs`. Bodies use the `async_op!`/`sync_op!`/`conn_method!`/`dispatch_cmd!` macros from Plan 01 and the `IntoRawResult` trait from `raw_result.rs`. The TTL family does not yet have first-class `redis::AsyncCommands` methods on every redis-rs version we target, so those bodies build a `redis::Cmd` by hand and dispatch via `dispatch_cmd!`. `HMSET` is deprecated upstream — accepted for compatibility but emits a one-shot `DeprecationWarning` per process via `warnings.warn` invoked through `py.import("warnings")`.

**Tech Stack:** PyO3 0.28 (`#[pyclass]`, `#[pyo3(signature = ...)]`, `PyDict`, `PyList`, `PyTuple`, `PyBytes`), tokio 1.x, redis 1.x (`AsyncCommands`, `Cmd::new`, `Cmd::arg`), testcontainers (Valkey 8.0) on the Python side. Python 3.14 + 3.14t.

**Reference material:**
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/01-foundation-async-bridge.md` — defines `async_op!`, `sync_op!`, `conn_method!`, `dispatch_cmd!`, `IntoRawResult`, and the `py_*` helper functions in `driver.rs`.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs:1445-1643` — cachex's existing implementations for `hget`/`hset`/`hsetnx`/`hgetall`/`hdel`/`hincrby`/`hincrbyfloat`/`hkeys`/`hvals`/`hexists`/`hlen`/`hmget`/`hmset` (this plan ports them verbatim, then adds the rest).
- redis-py `redis/commands/core.py::HashCommands` for the canonical kwarg shape of `hset(name, key=None, value=None, mapping=None, items=None)` and the `hexpire(name, seconds, *fields, nx=False, xx=False, gt=False, lt=False)` family.
- Redis docs: https://redis.io/commands/hexpire/ — return shape `[int]` with one entry per field (1 = applied, 0 = condition not met, -2 = no such field, 2 = field deleted by HPERSIST). Same shape for the whole TTL family.

**Out of scope for this plan:**
- The high-level `Redis` façade method bindings — that's plan 10. This plan only exposes commands on the low-level `RedisRsDriver`.
- Hash commands inside pipelines/transactions — plan 13 wires those through.
- `decode_responses=True` mode — plan 12.
- The redis-py-shaped `HSCAN` async-iterator — for v0.1 we expose the cursor-based primitive and leave iteration to the façade in plan 10.

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  lib.rs                       # MODIFIED: add `mod commands;` declaration
  commands/
    mod.rs                     # NEW: declares `pub mod hashes;`
    hashes.rs                  # NEW: every hash command on RedisRsDriver
  raw_result.rs                # MODIFIED: add From<Vec<i64>> for RawResult (TTL family return shape)
  async_bridge.rs              # MODIFIED: add RawResult::IntList(Vec<i64>) variant
python/
  redis_rs_py/
    _driver.pyi                # MODIFIED: add hash-command method stubs
tests/
  driver/
    test_commands_hashes.py    # NEW: end-to-end coverage of every hash command
```

---

## Task 1: Add the `commands` module tree and the `Vec<i64>` return shape

Wire the new module path so subsequent tasks compile, and add the one new `RawResult` variant the TTL family needs.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/lib.rs`
- Create: `crates/redis-rs-py-driver/src/commands/mod.rs`
- Create: `crates/redis-rs-py-driver/src/commands/hashes.rs`
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs`
- Modify: `crates/redis-rs-py-driver/src/raw_result.rs`

- [ ] **Step 1: Declare the `commands` module in `lib.rs`**

Edit `crates/redis-rs-py-driver/src/lib.rs`. After the existing `mod test_helpers;` line add:

```rust
mod commands;
```

Keep the rest of the file untouched. The new module is declared but empty; the file still compiles after Step 2 lands the `mod.rs`.

- [ ] **Step 2: Create `commands/mod.rs`**

Create `crates/redis-rs-py-driver/src/commands/mod.rs`:

```rust
// Per-family command modules. Each file re-opens `impl RedisRsDriver`
// with the commands for one Redis data-type family, plus any helpers
// that family needs.

pub mod hashes;
```

- [ ] **Step 3: Stub `commands/hashes.rs`**

Create `crates/redis-rs-py-driver/src/commands/hashes.rs`:

```rust
// Hash commands on RedisRsDriver.
//
// Filled in by Plan 05 — for now an empty pyclass-extension block so the
// `mod hashes;` declaration in commands/mod.rs compiles.

use crate::driver::RedisRsDriver;
use pyo3::prelude::*;

#[pymethods]
impl RedisRsDriver {}
```

- [ ] **Step 4: Add the `IntList` variant to `RawResult`**

Edit `crates/redis-rs-py-driver/src/async_bridge.rs`. Inside the `pub enum RawResult { ... }` block, add the new variant alongside the existing ones:

```rust
    IntList(Vec<i64>),
```

In the `impl RawResult { fn into_py(...) ... }` block, add the matching arm:

```rust
            RawResult::IntList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
```

- [ ] **Step 5: Add the `From<Vec<i64>>` impl in `raw_result.rs`**

Edit `crates/redis-rs-py-driver/src/raw_result.rs`. Append after the existing `From<Vec<String>>` impl:

```rust
impl From<Vec<i64>> for RawResult {
    fn from(v: Vec<i64>) -> Self {
        RawResult::IntList(v)
    }
}
```

- [ ] **Step 6: Verify the crate still compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with only unused-warnings about the new variant (it's not used yet — Task 7 wires it).

- [ ] **Step 7: Commit**

```bash
git add crates/redis-rs-py-driver/src/lib.rs crates/redis-rs-py-driver/src/commands/ crates/redis-rs-py-driver/src/async_bridge.rs crates/redis-rs-py-driver/src/raw_result.rs
git commit -m "feat(hashes): scaffold commands/hashes.rs and IntList RawResult"
```

---

## Task 2: HGET / HSET / HSETNX (single field accessors)

Sub-task (a) of the plan. `HSET` accepts the redis-py-shaped `(name, key=None, value=None, mapping=None)` signature plus variadic `(field, value, field, value, ...)` positional pairs. Returns the number of fields that were *new* (existing fields are updated but not counted).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/hashes.rs`
- Test: `tests/driver/test_commands_hashes.py`

- [ ] **Step 1: Write the failing test for the (a) sub-task**

Create `tests/driver/test_commands_hashes.py`:

```python
"""Hash command coverage on RedisRsDriver — sub-task (a): HGET/HSET/HSETNX."""

from __future__ import annotations

import pytest


# --- HGET / HSET ---------------------------------------------------------

def test_hset_single_field_returns_count(driver) -> None:
    # First insert: 1 new field
    assert driver.hset("h", "f", b"v") == 1
    # Update existing field: 0 new fields
    assert driver.hset("h", "f", b"v2") == 0
    assert driver.hget("h", "f") == b"v2"


def test_hget_missing_returns_none(driver) -> None:
    assert driver.hget("missing-key", "missing-field") is None
    driver.hset("h", "f", b"v")
    assert driver.hget("h", "missing-field") is None


def test_hset_variadic_positional_pairs(driver) -> None:
    # redis-py signature: hset(name, key, value, mapping=None, items=None)
    # plus variadic items: hset(name, *items)
    n = driver.hset("h", "f1", b"v1", "f2", b"v2", "f3", b"v3")
    assert n == 3
    assert driver.hget("h", "f1") == b"v1"
    assert driver.hget("h", "f2") == b"v2"
    assert driver.hget("h", "f3") == b"v3"


def test_hset_with_mapping_kwarg(driver) -> None:
    n = driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    assert n == 3
    assert driver.hget("h", "a") == b"1"
    assert driver.hget("h", "b") == b"2"


def test_hset_mixes_positional_and_mapping(driver) -> None:
    n = driver.hset("h", "f", b"v", mapping={"m1": b"x", "m2": b"y"})
    assert n == 3
    assert driver.hget("h", "f") == b"v"
    assert driver.hget("h", "m1") == b"x"


def test_hset_empty_raises_data_error(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="at least one"):
        driver.hset("h")


def test_hset_odd_positional_count_raises_data_error(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="even"):
        driver.hset("h", "f1", b"v1", "lonely")


# --- HSETNX --------------------------------------------------------------

def test_hsetnx_inserts_when_absent(driver) -> None:
    assert driver.hsetnx("h", "f", b"v") is True
    assert driver.hget("h", "f") == b"v"


def test_hsetnx_skips_when_present(driver) -> None:
    driver.hset("h", "f", b"original")
    assert driver.hsetnx("h", "f", b"replacement") is False
    assert driver.hget("h", "f") == b"original"


# --- async pair ----------------------------------------------------------

@pytest.mark.asyncio
async def test_ahset_ahget_basic(driver) -> None:
    assert await driver.ahset("h", "f", b"v") == 1
    assert await driver.ahget("h", "f") == b"v"


@pytest.mark.asyncio
async def test_ahset_with_mapping(driver) -> None:
    n = await driver.ahset("h", mapping={"a": b"1", "b": b"2"})
    assert n == 2


@pytest.mark.asyncio
async def test_ahsetnx(driver) -> None:
    assert await driver.ahsetnx("h", "f", b"v") is True
    assert await driver.ahsetnx("h", "f", b"v2") is False
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_hashes.py -v`
Expected: every test FAILS with `AttributeError: 'builtins.RedisRsDriver' object has no attribute 'hset'` (and similar).

- [ ] **Step 3: Implement HGET / HSET / HSETNX in `hashes.rs`**

Replace `crates/redis-rs-py-driver/src/commands/hashes.rs`:

```rust
// Hash commands on RedisRsDriver.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyTuple};
use redis::AsyncCommands;

use crate::async_bridge::RawResult;
use crate::driver::{
    py_bool, py_bytes_list, py_bytes_pairs, py_int, py_opt_bytes, py_string_list, RedisRsDriver,
};
use crate::errors::to_py_err;
use crate::exceptions::{DataError, ExceptionClass};
use crate::raw_result::IntoRawResult;
use crate::{async_op, conn_method, dispatch_cmd, sync_op};

// =========================================================================
// Helpers shared by every multi-field command
// =========================================================================

/// Flatten redis-py-style positional args + a `mapping` kwarg into a
/// `Vec<(String, Vec<u8>)>` ready for HSET/HMSET. Mirrors
/// redis.commands.core.HashCommands.hset() input handling.
fn collect_field_value_pairs(
    items: &Bound<'_, PyTuple>,
    mapping: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<(String, Vec<u8>)>> {
    let mut pairs: Vec<(String, Vec<u8>)> = Vec::new();
    if items.len() % 2 != 0 {
        return Err(PyErr::new::<DataError, _>(
            "HSET items must be an even number of (field, value) positional args",
        ));
    }
    let mut i = 0;
    while i < items.len() {
        let field: String = items.get_item(i)?.extract()?;
        let value: Vec<u8> = items.get_item(i + 1)?.extract()?;
        pairs.push((field, value));
        i += 2;
    }
    if let Some(m) = mapping {
        for (k, v) in m.iter() {
            let field: String = k.extract()?;
            let value: Vec<u8> = v.extract()?;
            pairs.push((field, value));
        }
    }
    if pairs.is_empty() {
        return Err(PyErr::new::<DataError, _>(
            "HSET requires at least one (field, value) pair or a non-empty mapping=",
        ));
    }
    Ok(pairs)
}

#[pymethods]
impl RedisRsDriver {
    // =====================================================================
    // (a) HGET / HSET / HSETNX
    // =====================================================================

    #[pyo3(signature = (key, field))]
    fn hget(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hget(key, field)));
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (key, field))]
    fn ahget(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Option<Vec<u8>>> =
                conn_method!(&mut conn, c, c.hget(&key, &field));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *items, mapping=None))]
    fn hset(
        &self,
        py: Python<'_>,
        key: &str,
        items: &Bound<'_, PyTuple>,
        mapping: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let pairs = collect_field_value_pairs(items, mapping)?;
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            conn_method!(&mut conn, c, c.hset_multiple(key, &pairs))
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *items, mapping=None))]
    fn ahset(
        &self,
        py: Python<'_>,
        key: &str,
        items: &Bound<'_, PyTuple>,
        mapping: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let pairs = collect_field_value_pairs(items, mapping)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> =
                conn_method!(&mut conn, c, c.hset_multiple(&key, &pairs));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, field, value))]
    fn hsetnx(&self, py: Python<'_>, key: &str, field: &str, value: &[u8]) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<bool> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hset_nx(key, field, value)));
        py_bool(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, field, value))]
    fn ahsetnx(
        &self,
        py: Python<'_>,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<bool> =
                conn_method!(&mut conn, c, c.hset_nx(&key, &field, &value));
            r.into_raw_result()
        })
    }
}
```

Note: `redis::AsyncCommands::hset_multiple` is the redis-rs equivalent of HSET with `[(field, value), ...]`. It returns the number of *new* fields, which matches redis-py's contract.

- [ ] **Step 4: Build + run the (a) tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_hashes.py -v`
Expected: 12 PASS (the 12 tests written in Step 1).

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/hashes.rs tests/driver/test_commands_hashes.py
git commit -m "feat(hashes): add HGET/HSET/HSETNX with mapping= and variadic pairs"
```

---

## Task 3: HMSET + HGETALL + HMGET (multi-field accessors)

Sub-task (b). `HMSET` is deprecated in Redis 4.0+ but redis-py still accepts it; we accept it for compat with a one-shot `DeprecationWarning`. `HGETALL` returns `dict[bytes, bytes]`. `HMGET` returns `list[bytes | None]` preserving order and missing-field positions.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/hashes.rs`
- Modify: `tests/driver/test_commands_hashes.py`

- [ ] **Step 1: Append the (b) tests**

Append to `tests/driver/test_commands_hashes.py`:

```python
# --- HGETALL -------------------------------------------------------------

def test_hgetall_returns_dict_of_bytes(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    got = driver.hgetall("h")
    assert isinstance(got, dict)
    assert got == {b"a": b"1", b"b": b"2"}


def test_hgetall_missing_key_returns_empty_dict(driver) -> None:
    assert driver.hgetall("missing") == {}


@pytest.mark.asyncio
async def test_ahgetall(driver) -> None:
    await driver.ahset("h", mapping={"x": b"1"})
    assert await driver.ahgetall("h") == {b"x": b"1"}


# --- HMGET ---------------------------------------------------------------

def test_hmget_preserves_order_and_missing_fields(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "c": b"3"})
    got = driver.hmget("h", "a", "b", "c")
    assert got == [b"1", None, b"3"]


def test_hmget_empty_fields_returns_empty_list(driver) -> None:
    driver.hset("h", "f", b"v")
    assert driver.hmget("h") == []


def test_hmget_missing_key_returns_all_none(driver) -> None:
    assert driver.hmget("missing", "a", "b") == [None, None]


@pytest.mark.asyncio
async def test_ahmget(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1"})
    assert await driver.ahmget("h", "a", "b") == [b"1", None]


# --- HMSET (deprecated upstream) -----------------------------------------

def test_hmset_writes_all_fields(driver) -> None:
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        driver.hmset("h", {"a": b"1", "b": b"2"})
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)
    assert driver.hgetall("h") == {b"a": b"1", b"b": b"2"}


def test_hmset_empty_mapping_raises_data_error(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="empty"):
        driver.hmset("h", {})


@pytest.mark.asyncio
async def test_ahmset(driver) -> None:
    import warnings

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        await driver.ahmset("h", {"a": b"1"})
    assert await driver.ahgetall("h") == {b"a": b"1"}
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_hashes.py -v -k "hgetall or hmget or hmset"`
Expected: every new test FAILS with `AttributeError`.

- [ ] **Step 3: Implement HGETALL / HMGET / HMSET**

Append inside the `#[pymethods] impl RedisRsDriver { ... }` block in `crates/redis-rs-py-driver/src/commands/hashes.rs` (before the closing brace):

```rust
    // =====================================================================
    // (b) HGETALL / HMGET / HMSET
    // =====================================================================

    #[pyo3(signature = (key))]
    fn hgetall(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<(Vec<u8>, Vec<u8>)>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hgetall(key)));
        py_bytes_pairs(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahgetall(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<(Vec<u8>, Vec<u8>)>> =
                conn_method!(&mut conn, c, c.hgetall(&key));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *fields))]
    fn hmget(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        if fields.is_empty() {
            return py_bytes_list(py, Vec::new()).map(|_| {
                pyo3::types::PyList::empty(py).into_any().unbind()
            });
        }
        let r: Result<Vec<Option<Vec<u8>>>, _> = sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.hget(key, &fields))
        );
        let raw = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = raw
            .into_iter()
            .map(|opt| match opt {
                Some(bytes) => pyo3::types::PyBytes::new(py, &bytes).into_any().unbind(),
                None => py.None(),
            })
            .collect();
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, *fields))]
    fn ahmget(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if fields.is_empty() {
                return RawResult::OptBytesList(Vec::new());
            }
            let r: redis::RedisResult<Vec<Option<Vec<u8>>>> =
                conn_method!(&mut conn, c, c.hget(&key, &fields));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, mapping))]
    fn hmset(
        &self,
        py: Python<'_>,
        key: &str,
        mapping: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        warn_hmset_deprecated(py)?;
        let pairs = mapping_to_pairs(mapping)?;
        let r: redis::RedisResult<()> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hset_multiple::<_, _, _, ()>(key, &pairs)));
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (key, mapping))]
    fn ahmset(
        &self,
        py: Python<'_>,
        key: &str,
        mapping: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyAny>> {
        warn_hmset_deprecated(py)?;
        let pairs = mapping_to_pairs(mapping)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<()> =
                conn_method!(&mut conn, c, c.hset_multiple::<_, _, _, ()>(&key, &pairs));
            r.into_raw_result()
        })
    }
```

Outside the `#[pymethods]` block (still in the same file), add the two helpers:

```rust
fn mapping_to_pairs(mapping: &Bound<'_, PyDict>) -> PyResult<Vec<(String, Vec<u8>)>> {
    if mapping.is_empty() {
        return Err(PyErr::new::<DataError, _>(
            "HMSET requires a non-empty mapping",
        ));
    }
    let mut out = Vec::with_capacity(mapping.len());
    for (k, v) in mapping.iter() {
        let field: String = k.extract()?;
        let value: Vec<u8> = v.extract()?;
        out.push((field, value));
    }
    Ok(out)
}

fn warn_hmset_deprecated(py: Python<'_>) -> PyResult<()> {
    // Mirror redis-py: every HMSET call warns once (we let the standard
    // `warnings` filter de-duplicate via category + module).
    let warnings = py.import("warnings")?;
    warnings.call_method1(
        "warn",
        (
            "HMSET is deprecated. Use HSET instead.",
            py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
        ),
    )?;
    Ok(())
}
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_hashes.py -v -k "hgetall or hmget or hmset"`
Expected: 11 PASS (3 hgetall + 4 hmget + 3 hmset + the async pair).

- [ ] **Step 5: Run the full hashes-test file to make sure (a) still passes**

Run: `uv run pytest tests/driver/test_commands_hashes.py -v`
Expected: 23 PASS (12 from (a) + 11 from (b)).

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/hashes.rs tests/driver/test_commands_hashes.py
git commit -m "feat(hashes): add HMSET/HGETALL/HMGET multi-field accessors"
```

---

## Task 4: HDEL / HEXISTS / HLEN

Sub-task (c). `HDEL` is variadic and returns the number of fields actually removed.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/hashes.rs`
- Modify: `tests/driver/test_commands_hashes.py`

- [ ] **Step 1: Append the (c) tests**

Append to `tests/driver/test_commands_hashes.py`:

```python
# --- HDEL ----------------------------------------------------------------

def test_hdel_variadic_returns_count(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    assert driver.hdel("h", "a", "b", "missing") == 2
    assert driver.hgetall("h") == {b"c": b"3"}


def test_hdel_missing_key_returns_zero(driver) -> None:
    assert driver.hdel("missing", "a", "b") == 0


def test_hdel_no_fields_returns_zero(driver) -> None:
    driver.hset("h", "f", b"v")
    assert driver.hdel("h") == 0


@pytest.mark.asyncio
async def test_ahdel(driver) -> None:
    await driver.ahset("h", mapping={"x": b"1", "y": b"2"})
    assert await driver.ahdel("h", "x", "z") == 1


# --- HEXISTS -------------------------------------------------------------

def test_hexists_present(driver) -> None:
    driver.hset("h", "f", b"v")
    assert driver.hexists("h", "f") is True


def test_hexists_absent(driver) -> None:
    driver.hset("h", "f", b"v")
    assert driver.hexists("h", "missing") is False
    assert driver.hexists("missing-key", "f") is False


@pytest.mark.asyncio
async def test_ahexists(driver) -> None:
    await driver.ahset("h", "f", b"v")
    assert await driver.ahexists("h", "f") is True
    assert await driver.ahexists("h", "g") is False


# --- HLEN ----------------------------------------------------------------

def test_hlen(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    assert driver.hlen("h") == 3


def test_hlen_missing_key_is_zero(driver) -> None:
    assert driver.hlen("missing") == 0


@pytest.mark.asyncio
async def test_ahlen(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1"})
    assert await driver.ahlen("h") == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_hashes.py -v -k "hdel or hexists or hlen"`
Expected: every new test FAILS.

- [ ] **Step 3: Implement HDEL / HEXISTS / HLEN**

Append inside the `#[pymethods]` block in `commands/hashes.rs`:

```rust
    // =====================================================================
    // (c) HDEL / HEXISTS / HLEN
    // =====================================================================

    #[pyo3(signature = (key, *fields))]
    fn hdel(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        if fields.is_empty() {
            return py_int(py, 0);
        }
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hdel(key, &fields)));
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *fields))]
    fn ahdel(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if fields.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn_method!(&mut conn, c, c.hdel(&key, &fields));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, field))]
    fn hexists(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<bool> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hexists(key, field)));
        py_bool(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, field))]
    fn ahexists(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<bool> =
                conn_method!(&mut conn, c, c.hexists(&key, &field));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key))]
    fn hlen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hlen(key)));
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahlen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn_method!(&mut conn, c, c.hlen(&key));
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_hashes.py -v -k "hdel or hexists or hlen"`
Expected: 10 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/hashes.rs tests/driver/test_commands_hashes.py
git commit -m "feat(hashes): add HDEL/HEXISTS/HLEN"
```

---

## Task 5: HKEYS / HVALS / HRANDFIELD

Sub-task (d). `HKEYS` returns `list[bytes]` (cachex returns strings; we choose bytes to match redis-py default `decode_responses=False`). `HRANDFIELD` has tri-mode behaviour: no `count` → single bytes (or None), `count > 0` → distinct list, `count < 0` → list with replacement; `withvalues=True` flips list to `list[(field, value)]`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/hashes.rs`
- Modify: `tests/driver/test_commands_hashes.py`

- [ ] **Step 1: Append the (d) tests**

Append to `tests/driver/test_commands_hashes.py`:

```python
# --- HKEYS / HVALS -------------------------------------------------------

def test_hkeys_returns_list_of_bytes(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    keys = driver.hkeys("h")
    assert isinstance(keys, list)
    assert sorted(keys) == [b"a", b"b"]


def test_hkeys_missing_key_returns_empty(driver) -> None:
    assert driver.hkeys("missing") == []


def test_hvals_returns_list_of_bytes(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    vals = driver.hvals("h")
    assert isinstance(vals, list)
    assert sorted(vals) == [b"1", b"2"]


@pytest.mark.asyncio
async def test_ahkeys_ahvals(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1"})
    assert await driver.ahkeys("h") == [b"a"]
    assert await driver.ahvals("h") == [b"1"]


# --- HRANDFIELD ----------------------------------------------------------

def test_hrandfield_no_count_returns_single_bytes(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    got = driver.hrandfield("h")
    assert got in (b"a", b"b", b"c")


def test_hrandfield_missing_returns_none(driver) -> None:
    assert driver.hrandfield("missing") is None


def test_hrandfield_with_positive_count_returns_distinct_list(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2", "c": b"3"})
    got = driver.hrandfield("h", count=2)
    assert isinstance(got, list)
    assert len(got) == 2
    assert len(set(got)) == 2  # distinct


def test_hrandfield_with_negative_count_allows_repeats(driver) -> None:
    driver.hset("h", "only", b"v")
    got = driver.hrandfield("h", count=-3)
    assert got == [b"only", b"only", b"only"]


def test_hrandfield_withvalues(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    got = driver.hrandfield("h", count=2, withvalues=True)
    assert isinstance(got, list)
    assert all(isinstance(item, tuple) and len(item) == 2 for item in got)
    keys = {pair[0] for pair in got}
    assert keys.issubset({b"a", b"b"})


@pytest.mark.asyncio
async def test_ahrandfield(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1"})
    assert await driver.ahrandfield("h") == b"a"
    assert await driver.ahrandfield("h", count=1) == [b"a"]
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_hashes.py -v -k "hkeys or hvals or hrandfield"`
Expected: every new test FAILS.

- [ ] **Step 3: Implement HKEYS / HVALS / HRANDFIELD**

Append inside the `#[pymethods]` block in `commands/hashes.rs`:

```rust
    // =====================================================================
    // (d) HKEYS / HVALS / HRANDFIELD
    // =====================================================================

    #[pyo3(signature = (key))]
    fn hkeys(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hkeys(key)));
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahkeys(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = conn_method!(&mut conn, c, c.hkeys(&key));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key))]
    fn hvals(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.hvals(key)));
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahvals(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = conn_method!(&mut conn, c, c.hvals(&key));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, count=None, withvalues=false))]
    fn hrandfield(
        &self,
        py: Python<'_>,
        key: &str,
        count: Option<i64>,
        withvalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("HRANDFIELD");
            cmd.arg(key);
            if let Some(c) = count {
                cmd.arg(c);
                if withvalues {
                    cmd.arg("WITHVALUES");
                }
            }
            dispatch_cmd!(&mut conn, cmd)
        });
        let value = r.map_err(to_py_err)?;
        render_hrandfield(py, value, count, withvalues)
    }

    #[pyo3(signature = (key, count=None, withvalues=false))]
    fn ahrandfield(
        &self,
        py: Python<'_>,
        key: &str,
        count: Option<i64>,
        withvalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("HRANDFIELD");
            cmd.arg(&key);
            if let Some(c) = count {
                cmd.arg(c);
                if withvalues {
                    cmd.arg("WITHVALUES");
                }
            }
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::Value(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }
```

Outside the `#[pymethods]` block, add the renderer helper:

```rust
fn render_hrandfield(
    py: Python<'_>,
    value: redis::Value,
    count: Option<i64>,
    withvalues: bool,
) -> PyResult<Py<PyAny>> {
    use pyo3::types::{PyBytes, PyList, PyTuple};
    match (count, value) {
        // No count → single bytes or None.
        (None, redis::Value::Nil) => Ok(py.None()),
        (None, redis::Value::BulkString(b)) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        // count without WITHVALUES → flat list[bytes].
        (Some(_), redis::Value::Array(items)) if !withvalues => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| match item {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    redis::Value::Nil => py.None(),
                    other => PyBytes::new(py, format!("{other:?}").as_bytes())
                        .into_any()
                        .unbind(),
                })
                .collect();
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        // count + WITHVALUES → list[tuple[bytes, bytes]].
        // RESP2 returns a flat array [field, value, field, value, ...];
        // RESP3 returns an array of two-element arrays. Handle both.
        (Some(_), redis::Value::Array(items)) if withvalues => {
            let mut pairs: Vec<Py<PyAny>> = Vec::new();
            // Detect shape from first item.
            let nested = items
                .first()
                .map(|first| matches!(first, redis::Value::Array(_)))
                .unwrap_or(false);
            if nested {
                for item in items {
                    if let redis::Value::Array(inner) = item
                        && inner.len() == 2
                    {
                        let field = match &inner[0] {
                            redis::Value::BulkString(b) => PyBytes::new(py, b).into_any().unbind(),
                            _ => py.None(),
                        };
                        let val = match &inner[1] {
                            redis::Value::BulkString(b) => PyBytes::new(py, b).into_any().unbind(),
                            _ => py.None(),
                        };
                        pairs.push(PyTuple::new(py, [field, val])?.into_any().unbind());
                    }
                }
            } else {
                let mut iter = items.into_iter();
                while let (Some(field_v), Some(val_v)) = (iter.next(), iter.next()) {
                    let field = match field_v {
                        redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                        _ => py.None(),
                    };
                    let val = match val_v {
                        redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                        _ => py.None(),
                    };
                    pairs.push(PyTuple::new(py, [field, val])?.into_any().unbind());
                }
            }
            Ok(PyList::new(py, pairs)?.into_any().unbind())
        }
        (_, redis::Value::Nil) => Ok(py.None()),
        (_, other) => {
            // Unexpected shape — surface the raw repr for debugging rather
            // than silently swallowing.
            Ok(pyo3::types::PyString::new(py, &format!("{other:?}"))
                .into_any()
                .unbind())
        }
    }
}
```

The async path returns `RawResult::Value(...)`, which goes through `redis_value_to_py` and produces a flat list — that's *not* the same shape as the sync path for `WITHVALUES`. Wrap the async render too: replace the body of `ahrandfield` with one that uses a custom `RawResult` mapping (already present via the `Value` variant) but post-processes inside `into_py`. The minimal fix is to expose the renderer via a module-level function call that doesn't need GIL inside the spawn — instead, marshal the raw `Value` through the awaitable and let the sync renderer run on the GIL side.

Replace `ahrandfield` in the `#[pymethods]` block with:

```rust
    #[pyo3(signature = (key, count=None, withvalues=false))]
    fn ahrandfield(
        &self,
        py: Python<'_>,
        key: &str,
        count: Option<i64>,
        withvalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        crate::runtime::get_runtime().spawn(async move {
            let mut cmd = redis::cmd("HRANDFIELD");
            cmd.arg(&key);
            if let Some(c) = count {
                cmd.arg(c);
                if withvalues {
                    cmd.arg("WITHVALUES");
                }
            }
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            let raw = match r {
                Ok(v) => RawResult::HRandfield(v, count, withvalues),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }
```

Then, in `crates/redis-rs-py-driver/src/async_bridge.rs`, add a new variant carrying the discriminant the renderer needs:

```rust
    HRandfield(redis::Value, Option<i64>, bool),
```

And, in the `into_py` match, the arm:

```rust
            RawResult::HRandfield(v, count, withvalues) => {
                crate::commands::hashes::render_hrandfield(py, v, count, withvalues)
            }
```

And expose `render_hrandfield` as `pub(crate)` in `commands/hashes.rs`:

```rust
pub(crate) fn render_hrandfield(...) -> PyResult<Py<PyAny>> { ... }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_hashes.py -v -k "hkeys or hvals or hrandfield"`
Expected: 9 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/hashes.rs crates/redis-rs-py-driver/src/async_bridge.rs tests/driver/test_commands_hashes.py
git commit -m "feat(hashes): add HKEYS/HVALS/HRANDFIELD with WITHVALUES rendering"
```

---

## Task 6: HINCRBY / HINCRBYFLOAT

Sub-task (e). Two simple counter commands; `HINCRBY` returns int, `HINCRBYFLOAT` returns float. Both can raise `ResponseError` if the field exists but isn't a number.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/hashes.rs`
- Modify: `tests/driver/test_commands_hashes.py`

- [ ] **Step 1: Append the (e) tests**

Append to `tests/driver/test_commands_hashes.py`:

```python
# --- HINCRBY -------------------------------------------------------------

def test_hincrby_creates_field_at_zero(driver) -> None:
    assert driver.hincrby("h", "counter", 5) == 5
    assert driver.hget("h", "counter") == b"5"


def test_hincrby_increments_existing(driver) -> None:
    driver.hset("h", "counter", b"10")
    assert driver.hincrby("h", "counter", 7) == 17
    assert driver.hincrby("h", "counter", -3) == 14


def test_hincrby_on_non_integer_raises_response_error(driver) -> None:
    from redis_rs_py.exceptions import ResponseError

    driver.hset("h", "f", b"not-a-number")
    with pytest.raises(ResponseError):
        driver.hincrby("h", "f", 1)


@pytest.mark.asyncio
async def test_ahincrby(driver) -> None:
    assert await driver.ahincrby("h", "c", 5) == 5
    assert await driver.ahincrby("h", "c", 5) == 10


# --- HINCRBYFLOAT --------------------------------------------------------

def test_hincrbyfloat_creates_field(driver) -> None:
    assert driver.hincrbyfloat("h", "f", 1.5) == pytest.approx(1.5)


def test_hincrbyfloat_increments_existing(driver) -> None:
    driver.hset("h", "f", b"3.14")
    assert driver.hincrbyfloat("h", "f", 0.86) == pytest.approx(4.0)


def test_hincrbyfloat_on_non_float_raises(driver) -> None:
    from redis_rs_py.exceptions import ResponseError

    driver.hset("h", "f", b"nope")
    with pytest.raises(ResponseError):
        driver.hincrbyfloat("h", "f", 1.0)


@pytest.mark.asyncio
async def test_ahincrbyfloat(driver) -> None:
    val = await driver.ahincrbyfloat("h", "f", 2.5)
    assert val == pytest.approx(2.5)
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_hashes.py -v -k "hincrby"`
Expected: 8 FAIL.

- [ ] **Step 3: Implement HINCRBY / HINCRBYFLOAT**

Append inside the `#[pymethods]` block:

```rust
    // =====================================================================
    // (e) HINCRBY / HINCRBYFLOAT
    // =====================================================================

    #[pyo3(signature = (key, field, amount))]
    fn hincrby(
        &self,
        py: Python<'_>,
        key: &str,
        field: &str,
        amount: i64,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.hincr(key, field, amount))
        );
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, field, amount))]
    fn ahincrby(
        &self,
        py: Python<'_>,
        key: &str,
        field: &str,
        amount: i64,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> =
                conn_method!(&mut conn, c, c.hincr(&key, &field, amount));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, field, amount))]
    fn hincrbyfloat(
        &self,
        py: Python<'_>,
        key: &str,
        field: &str,
        amount: f64,
    ) -> PyResult<f64> {
        sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.hincr(key, field, amount))
        )
        .map_err(to_py_err)
    }

    #[pyo3(signature = (key, field, amount))]
    fn ahincrbyfloat(
        &self,
        py: Python<'_>,
        key: &str,
        field: &str,
        amount: f64,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<f64> =
                conn_method!(&mut conn, c, c.hincr(&key, &field, amount));
            r.into_raw_result()
        })
    }
```

`redis::AsyncCommands::hincr` is generic over the increment type (`ToRedisArgs`) — the same method covers both i64 and f64.

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_hashes.py -v -k "hincrby"`
Expected: 8 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/hashes.rs tests/driver/test_commands_hashes.py
git commit -m "feat(hashes): add HINCRBY/HINCRBYFLOAT counters"
```

---

## Task 7: HSCAN

Sub-task (f). Cursor-based iteration with `MATCH`/`COUNT`/`NOVALUES`. Returns `(cursor: int, items)`. With `NOVALUES`, `items` is `list[bytes]` (just field names); without it, `items` is `dict[bytes, bytes]` (field → value pairs flattened from the server's flat list).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/hashes.rs`
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs`
- Modify: `tests/driver/test_commands_hashes.py`

- [ ] **Step 1: Append the (f) tests**

Append to `tests/driver/test_commands_hashes.py`:

```python
# --- HSCAN ---------------------------------------------------------------

def test_hscan_full_iteration(driver) -> None:
    expected = {f"f{i}".encode(): str(i).encode() for i in range(20)}
    driver.hset("h", mapping={k.decode(): v for k, v in expected.items()})

    seen: dict[bytes, bytes] = {}
    cursor = 0
    while True:
        cursor, batch = driver.hscan("h", cursor=cursor)
        assert isinstance(batch, dict)
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == expected


def test_hscan_with_match(driver) -> None:
    driver.hset(
        "h",
        mapping={"foo:1": b"a", "foo:2": b"b", "bar:1": b"c"},
    )
    cursor = 0
    seen: dict[bytes, bytes] = {}
    while True:
        cursor, batch = driver.hscan("h", cursor=cursor, match="foo:*")
        seen.update(batch)
        if cursor == 0:
            break
    assert seen == {b"foo:1": b"a", b"foo:2": b"b"}


def test_hscan_count_is_a_hint(driver) -> None:
    driver.hset("h", mapping={f"k{i}": str(i).encode() for i in range(50)})
    cursor, batch = driver.hscan("h", cursor=0, count=10)
    # COUNT is just a hint — server may return more or fewer.
    assert isinstance(batch, dict)
    # Eventually consume the whole hash.
    seen: dict[bytes, bytes] = dict(batch)
    while cursor != 0:
        cursor, batch = driver.hscan("h", cursor=cursor, count=10)
        seen.update(batch)
    assert len(seen) == 50


def test_hscan_novalues_returns_field_list(driver) -> None:
    driver.hset("h", mapping={"a": b"1", "b": b"2"})
    cursor, batch = driver.hscan("h", cursor=0, novalues=True)
    assert isinstance(batch, list)
    while cursor != 0:
        cursor, more = driver.hscan("h", cursor=cursor, novalues=True)
        batch.extend(more)
    assert sorted(batch) == [b"a", b"b"]


@pytest.mark.asyncio
async def test_ahscan(driver) -> None:
    await driver.ahset("h", mapping={"a": b"1", "b": b"2"})
    cursor, batch = await driver.ahscan("h", cursor=0)
    assert b"a" in batch and b"b" in batch
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_hashes.py -v -k hscan`
Expected: 5 FAIL.

- [ ] **Step 3: Add the `HScan` variant to `RawResult`**

Edit `crates/redis-rs-py-driver/src/async_bridge.rs`. In the enum:

```rust
    HScan { cursor: u64, value: redis::Value, novalues: bool },
```

In `into_py`:

```rust
            RawResult::HScan { cursor, value, novalues } => {
                crate::commands::hashes::render_hscan(py, cursor, value, novalues)
            }
```

- [ ] **Step 4: Implement HSCAN in `commands/hashes.rs`**

Inside the `#[pymethods]` block, append:

```rust
    // =====================================================================
    // (f) HSCAN
    // =====================================================================

    #[pyo3(signature = (key, *, cursor=0, match=None, count=None, novalues=false))]
    #[allow(non_snake_case)]
    fn hscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
        novalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("HSCAN");
            cmd.arg(key).arg(cursor);
            if let Some(p) = &r#match {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            if novalues {
                cmd.arg("NOVALUES");
            }
            dispatch_cmd!(&mut conn, cmd)
        });
        let value = r.map_err(to_py_err)?;
        let (cursor, payload) = split_scan_reply(value)?;
        render_hscan(py, cursor, payload, novalues)
    }

    #[pyo3(signature = (key, *, cursor=0, match=None, count=None, novalues=false))]
    #[allow(non_snake_case)]
    fn ahscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
        novalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        crate::runtime::get_runtime().spawn(async move {
            let mut cmd = redis::cmd("HSCAN");
            cmd.arg(&key).arg(cursor);
            if let Some(p) = &r#match {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            if novalues {
                cmd.arg("NOVALUES");
            }
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            let raw = match r {
                Ok(v) => match split_scan_reply(v) {
                    Ok((cursor, payload)) => RawResult::HScan {
                        cursor,
                        value: payload,
                        novalues,
                    },
                    Err(e) => RawResult::Error(ExceptionClass::ResponseError, e.to_string()),
                },
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }
```

Outside the `#[pymethods]` block, add the helpers:

```rust
fn split_scan_reply(value: redis::Value) -> PyResult<(u64, redis::Value)> {
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
        return Ok((cursor, payload));
    }
    Err(PyErr::new::<DataError, _>(
        "HSCAN reply did not match the [cursor, items] shape",
    ))
}

pub(crate) fn render_hscan(
    py: Python<'_>,
    cursor: u64,
    value: redis::Value,
    novalues: bool,
) -> PyResult<Py<PyAny>> {
    use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
    let cursor_py = cursor.into_pyobject(py)?.into_any().unbind();
    let payload = match value {
        redis::Value::Array(items) if novalues => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| match item {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    _ => py.None(),
                })
                .collect();
            PyList::new(py, py_items)?.into_any().unbind()
        }
        redis::Value::Array(items) => {
            let dict = PyDict::new(py);
            let mut iter = items.into_iter();
            while let (Some(k_v), Some(v_v)) = (iter.next(), iter.next()) {
                let k = match k_v {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    _ => py.None(),
                };
                let v = match v_v {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    _ => py.None(),
                };
                dict.set_item(k, v)?;
            }
            dict.into_any().unbind()
        }
        _ => PyList::empty(py).into_any().unbind(),
    };
    Ok(PyTuple::new(py, [cursor_py, payload])?.into_any().unbind())
}
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_hashes.py -v -k hscan`
Expected: 5 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/hashes.rs crates/redis-rs-py-driver/src/async_bridge.rs tests/driver/test_commands_hashes.py
git commit -m "feat(hashes): add HSCAN with MATCH/COUNT/NOVALUES"
```

---

## Task 8: Hash-field TTL family (Redis 7.4+)

Sub-task (g). Nine commands: `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HEXPIRETIME`, `HPEXPIRETIME`, `HTTL`, `HPTTL`, `HPERSIST`. The setters share a `(key, fields, time, *, nx=False, xx=False, gt=False, lt=False)` signature; the readers take `(key, fields)`. All return `list[int]` with one entry per field, mirroring the server's reply.

**Reference:** https://redis.io/commands/hexpire/

Return-code semantics (per Redis):
- `1` — applied
- `0` — condition (NX/XX/GT/LT) not met
- `-2` — no such field
- `2` — field deleted (HEXPIRE with seconds=0 or HPERSIST when no TTL was set)
- For TTL/EXPIRETIME readers: `-1` if the field exists but has no TTL, `-2` if the field doesn't exist.

This family needs Valkey ≥ 7.4. The test file probes `INFO server` once and skips the whole class if older.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/hashes.rs`
- Modify: `tests/driver/test_commands_hashes.py`

- [ ] **Step 1: Append the version-gated test class for the (g) sub-task**

Append to `tests/driver/test_commands_hashes.py`:

```python
# --- Hash-field TTL family (Redis 7.4+) ----------------------------------

def _server_supports_hexpire(driver) -> bool:
    """Version probe — HEXPIRE family lands in Valkey/Redis 7.4."""
    import redis as upstream

    rp = upstream.Redis.from_url(driver.connection_url)
    try:
        info = rp.info("server")
        version = info.get("redis_version") or info.get("valkey_version") or "0.0.0"
        major, minor, *_ = (int(x) for x in version.split("-")[0].split("."))
    finally:
        rp.close()
    return (major, minor) >= (7, 4)


@pytest.fixture
def hexpire_driver(driver):
    if not _server_supports_hexpire(driver):
        pytest.skip("hash-field TTL family requires Redis/Valkey >= 7.4")
    return driver


# --- HEXPIRE / HPEXPIRE --------------------------------------------------

def test_hexpire_basic(hexpire_driver) -> None:
    hexpire_driver.hset("h", mapping={"a": b"1", "b": b"2"})
    got = hexpire_driver.hexpire("h", ["a", "b", "missing"], 60)
    assert got == [1, 1, -2]


def test_hexpire_nx_only_sets_when_no_ttl(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    assert hexpire_driver.hexpire("h", ["f"], 60, nx=True) == [1]
    # Second call with NX must report condition not met.
    assert hexpire_driver.hexpire("h", ["f"], 120, nx=True) == [0]


def test_hexpire_xx_only_sets_when_ttl_present(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    assert hexpire_driver.hexpire("h", ["f"], 60, xx=True) == [0]
    hexpire_driver.hexpire("h", ["f"], 60)
    assert hexpire_driver.hexpire("h", ["f"], 120, xx=True) == [1]


def test_hexpire_gt_lt_modifiers(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    hexpire_driver.hexpire("h", ["f"], 60)
    # GT — only set if new > current
    assert hexpire_driver.hexpire("h", ["f"], 30, gt=True) == [0]
    assert hexpire_driver.hexpire("h", ["f"], 120, gt=True) == [1]
    # LT — only set if new < current (current is now 120)
    assert hexpire_driver.hexpire("h", ["f"], 200, lt=True) == [0]
    assert hexpire_driver.hexpire("h", ["f"], 30, lt=True) == [1]


def test_hpexpire_milliseconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    assert hexpire_driver.hpexpire("h", ["f"], 60_000) == [1]


@pytest.mark.asyncio
async def test_ahexpire(hexpire_driver) -> None:
    await hexpire_driver.ahset("h", mapping={"a": b"1"})
    assert await hexpire_driver.ahexpire("h", ["a"], 60) == [1]


# --- HEXPIREAT / HPEXPIREAT ----------------------------------------------

def test_hexpireat_with_unix_seconds(hexpire_driver) -> None:
    import time

    hexpire_driver.hset("h", "f", b"v")
    future = int(time.time()) + 600
    assert hexpire_driver.hexpireat("h", ["f"], future) == [1]


def test_hpexpireat_with_unix_milliseconds(hexpire_driver) -> None:
    import time

    hexpire_driver.hset("h", "f", b"v")
    future_ms = int(time.time() * 1000) + 600_000
    assert hexpire_driver.hpexpireat("h", ["f"], future_ms) == [1]


# --- HTTL / HPTTL --------------------------------------------------------

def test_httl_returns_seconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", mapping={"a": b"1", "b": b"2"})
    hexpire_driver.hexpire("h", ["a"], 100)
    got = hexpire_driver.httl("h", ["a", "b", "missing"])
    assert got[0] > 0
    assert got[0] <= 100
    assert got[1] == -1  # no TTL set
    assert got[2] == -2  # no such field


def test_hpttl_returns_milliseconds(hexpire_driver) -> None:
    hexpire_driver.hset("h", "f", b"v")
    hexpire_driver.hpexpire("h", ["f"], 50_000)
    got = hexpire_driver.hpttl("h", ["f"])
    assert 0 < got[0] <= 50_000


@pytest.mark.asyncio
async def test_ahttl(hexpire_driver) -> None:
    await hexpire_driver.ahset("h", "f", b"v")
    await hexpire_driver.ahexpire("h", ["f"], 60)
    got = await hexpire_driver.ahttl("h", ["f"])
    assert got[0] > 0


# --- HEXPIRETIME / HPEXPIRETIME ------------------------------------------

def test_hexpiretime_returns_unix_seconds(hexpire_driver) -> None:
    import time

    hexpire_driver.hset("h", "f", b"v")
    when = int(time.time()) + 100
    hexpire_driver.hexpireat("h", ["f"], when)
    got = hexpire_driver.hexpiretime("h", ["f", "missing"])
    assert got[0] == when
    assert got[1] == -2


def test_hpexpiretime_returns_unix_milliseconds(hexpire_driver) -> None:
    import time

    hexpire_driver.hset("h", "f", b"v")
    when_ms = int(time.time() * 1000) + 100_000
    hexpire_driver.hpexpireat("h", ["f"], when_ms)
    got = hexpire_driver.hpexpiretime("h", ["f"])
    assert abs(got[0] - when_ms) < 1000  # within 1s tolerance


# --- HPERSIST ------------------------------------------------------------

def test_hpersist_removes_ttl(hexpire_driver) -> None:
    hexpire_driver.hset("h", mapping={"a": b"1", "b": b"2"})
    hexpire_driver.hexpire("h", ["a"], 100)
    got = hexpire_driver.hpersist("h", ["a", "b", "missing"])
    assert got == [1, -1, -2]
    # And HTTL should now report -1 for `a`.
    assert hexpire_driver.httl("h", ["a"]) == [-1]


@pytest.mark.asyncio
async def test_ahpersist(hexpire_driver) -> None:
    await hexpire_driver.ahset("h", "f", b"v")
    await hexpire_driver.ahexpire("h", ["f"], 60)
    assert await hexpire_driver.ahpersist("h", ["f"]) == [1]
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_hashes.py -v -k "hexpire or hpexpire or httl or hpttl or hpersist"`
Expected: every new test FAILS (or skips on Valkey < 7.4).

- [ ] **Step 3: Implement the TTL family in `commands/hashes.rs`**

The family follows a tight pattern: build `redis::cmd("HEXPIRE")` (or whichever), add positional args, optionally append the `NX`/`XX`/`GT`/`LT` modifier, then `FIELDS <count> <f1> <f2> ...`. The reply is `Vec<i64>`.

Add the helper at the bottom of `commands/hashes.rs` (outside `#[pymethods]`):

```rust
fn validate_ttl_modifiers(nx: bool, xx: bool, gt: bool, lt: bool) -> PyResult<Option<&'static str>> {
    let modifier_count = [nx, xx, gt, lt].iter().filter(|x| **x).count();
    if modifier_count > 1 {
        return Err(PyErr::new::<DataError, _>(
            "Only one of NX, XX, GT, LT can be set at a time",
        ));
    }
    Ok(if nx {
        Some("NX")
    } else if xx {
        Some("XX")
    } else if gt {
        Some("GT")
    } else if lt {
        Some("LT")
    } else {
        None
    })
}

fn build_ttl_setter_cmd(
    name: &'static str,
    key: &str,
    fields: &[String],
    time: i64,
    modifier: Option<&'static str>,
) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    cmd.arg(key).arg(time);
    if let Some(m) = modifier {
        cmd.arg(m);
    }
    cmd.arg("FIELDS").arg(fields.len()).arg(fields);
    cmd
}

fn build_ttl_reader_cmd(name: &'static str, key: &str, fields: &[String]) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    cmd.arg(key);
    cmd.arg("FIELDS").arg(fields.len()).arg(fields);
    cmd
}
```

And inside the `#[pymethods]` block, append all 18 methods (one sync + one async per command):

```rust
    // =====================================================================
    // (g) Hash-field TTL family (Redis 7.4+)
    // =====================================================================

    // --- HEXPIRE -------------------------------------------------------

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    fn hexpire(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let cmd = build_ttl_setter_cmd("HEXPIRE", key, &fields, time, modifier);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    fn ahexpire(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_setter_cmd("HEXPIRE", &key, &fields, time, modifier);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    // --- HPEXPIRE ------------------------------------------------------

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    fn hpexpire(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let cmd = build_ttl_setter_cmd("HPEXPIRE", key, &fields, time, modifier);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    fn ahpexpire(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_setter_cmd("HPEXPIRE", &key, &fields, time, modifier);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    // --- HEXPIREAT -----------------------------------------------------

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    fn hexpireat(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let cmd = build_ttl_setter_cmd("HEXPIREAT", key, &fields, time, modifier);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    fn ahexpireat(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_setter_cmd("HEXPIREAT", &key, &fields, time, modifier);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    // --- HPEXPIREAT ----------------------------------------------------

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    fn hpexpireat(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let cmd = build_ttl_setter_cmd("HPEXPIREAT", key, &fields, time, modifier);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    fn ahpexpireat(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_setter_cmd("HPEXPIREAT", &key, &fields, time, modifier);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    // --- HEXPIRETIME / HPEXPIRETIME / HTTL / HPTTL / HPERSIST ----------

    #[pyo3(signature = (key, fields))]
    fn hexpiretime(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let cmd = build_ttl_reader_cmd("HEXPIRETIME", key, &fields);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields))]
    fn ahexpiretime(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_reader_cmd("HEXPIRETIME", &key, &fields);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, fields))]
    fn hpexpiretime(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let cmd = build_ttl_reader_cmd("HPEXPIRETIME", key, &fields);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields))]
    fn ahpexpiretime(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_reader_cmd("HPEXPIRETIME", &key, &fields);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, fields))]
    fn httl(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let cmd = build_ttl_reader_cmd("HTTL", key, &fields);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields))]
    fn ahttl(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_reader_cmd("HTTL", &key, &fields);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, fields))]
    fn hpttl(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let cmd = build_ttl_reader_cmd("HPTTL", key, &fields);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields))]
    fn ahpttl(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_reader_cmd("HPTTL", &key, &fields);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, fields))]
    fn hpersist(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let cmd = build_ttl_reader_cmd("HPERSIST", key, &fields);
        let r: redis::RedisResult<Vec<i64>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|n| n.into_pyobject(py).map(|v| v.into_any().unbind()))
            .collect::<PyResult<_>>()?;
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, fields))]
    fn ahpersist(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_ttl_reader_cmd("HPERSIST", &key, &fields);
            let r: redis::RedisResult<Vec<i64>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run the TTL tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_hashes.py -v -k "hexpire or hpexpire or httl or hpttl or hpersist"`
Expected: 17 PASS (or all SKIP if your test container is Valkey < 7.4 — `valkey/valkey:8.0` from Plan 01's `conftest.py` satisfies the version gate, so they should PASS).

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/hashes.rs tests/driver/test_commands_hashes.py
git commit -m "feat(hashes): add Redis 7.4 hash-field TTL family"
```

---

## Task 9: Update `_driver.pyi` stubs for every hash command

Hand-maintained type stubs so consumers and the linter see every new method.

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Append the hash-command stubs**

Append to the `class RedisRsDriver:` block in `python/redis_rs_py/_driver.pyi`:

```python
    # --- Hashes (Plan 05) ------------------------------------------------
    def hget(self, key: str, field: str) -> bytes | None: ...
    def ahget(self, key: str, field: str) -> Awaitable[bytes | None]: ...
    def hset(
        self,
        key: str,
        *items: str | bytes,
        mapping: dict[str, bytes] | None = ...,
    ) -> int: ...
    def ahset(
        self,
        key: str,
        *items: str | bytes,
        mapping: dict[str, bytes] | None = ...,
    ) -> Awaitable[int]: ...
    def hsetnx(self, key: str, field: str, value: bytes) -> bool: ...
    def ahsetnx(self, key: str, field: str, value: bytes) -> Awaitable[bool]: ...
    def hmset(self, key: str, mapping: dict[str, bytes]) -> None: ...
    def ahmset(self, key: str, mapping: dict[str, bytes]) -> Awaitable[None]: ...
    def hgetall(self, key: str) -> dict[bytes, bytes]: ...
    def ahgetall(self, key: str) -> Awaitable[dict[bytes, bytes]]: ...
    def hmget(self, key: str, *fields: str) -> list[bytes | None]: ...
    def ahmget(self, key: str, *fields: str) -> Awaitable[list[bytes | None]]: ...
    def hdel(self, key: str, *fields: str) -> int: ...
    def ahdel(self, key: str, *fields: str) -> Awaitable[int]: ...
    def hexists(self, key: str, field: str) -> bool: ...
    def ahexists(self, key: str, field: str) -> Awaitable[bool]: ...
    def hlen(self, key: str) -> int: ...
    def ahlen(self, key: str) -> Awaitable[int]: ...
    def hkeys(self, key: str) -> list[bytes]: ...
    def ahkeys(self, key: str) -> Awaitable[list[bytes]]: ...
    def hvals(self, key: str) -> list[bytes]: ...
    def ahvals(self, key: str) -> Awaitable[list[bytes]]: ...
    def hincrby(self, key: str, field: str, amount: int) -> int: ...
    def ahincrby(self, key: str, field: str, amount: int) -> Awaitable[int]: ...
    def hincrbyfloat(self, key: str, field: str, amount: float) -> float: ...
    def ahincrbyfloat(
        self, key: str, field: str, amount: float
    ) -> Awaitable[float]: ...
    def hrandfield(
        self,
        key: str,
        count: int | None = ...,
        withvalues: bool = ...,
    ) -> bytes | list[bytes] | list[tuple[bytes, bytes]] | None: ...
    def ahrandfield(
        self,
        key: str,
        count: int | None = ...,
        withvalues: bool = ...,
    ) -> Awaitable[bytes | list[bytes] | list[tuple[bytes, bytes]] | None]: ...
    def hscan(
        self,
        key: str,
        *,
        cursor: int = ...,
        match: str | None = ...,
        count: int | None = ...,
        novalues: bool = ...,
    ) -> tuple[int, dict[bytes, bytes] | list[bytes]]: ...
    def ahscan(
        self,
        key: str,
        *,
        cursor: int = ...,
        match: str | None = ...,
        count: int | None = ...,
        novalues: bool = ...,
    ) -> Awaitable[tuple[int, dict[bytes, bytes] | list[bytes]]]: ...
    # Hash-field TTL family (Redis 7.4+)
    def hexpire(
        self,
        key: str,
        fields: list[str],
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> list[int]: ...
    def ahexpire(
        self,
        key: str,
        fields: list[str],
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> Awaitable[list[int]]: ...
    def hpexpire(
        self,
        key: str,
        fields: list[str],
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> list[int]: ...
    def ahpexpire(
        self,
        key: str,
        fields: list[str],
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> Awaitable[list[int]]: ...
    def hexpireat(
        self,
        key: str,
        fields: list[str],
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> list[int]: ...
    def ahexpireat(
        self,
        key: str,
        fields: list[str],
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> Awaitable[list[int]]: ...
    def hpexpireat(
        self,
        key: str,
        fields: list[str],
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> list[int]: ...
    def ahpexpireat(
        self,
        key: str,
        fields: list[str],
        time: int,
        *,
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
    ) -> Awaitable[list[int]]: ...
    def hexpiretime(self, key: str, fields: list[str]) -> list[int]: ...
    def ahexpiretime(self, key: str, fields: list[str]) -> Awaitable[list[int]]: ...
    def hpexpiretime(self, key: str, fields: list[str]) -> list[int]: ...
    def ahpexpiretime(self, key: str, fields: list[str]) -> Awaitable[list[int]]: ...
    def httl(self, key: str, fields: list[str]) -> list[int]: ...
    def ahttl(self, key: str, fields: list[str]) -> Awaitable[list[int]]: ...
    def hpttl(self, key: str, fields: list[str]) -> list[int]: ...
    def ahpttl(self, key: str, fields: list[str]) -> Awaitable[list[int]]: ...
    def hpersist(self, key: str, fields: list[str]) -> list[int]: ...
    def ahpersist(self, key: str, fields: list[str]) -> Awaitable[list[int]]: ...
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
git commit -m "feat(hashes): add type stubs for all hash commands"
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

- [ ] **Step 2: Run the full hashes test file**

```bash
uv run pytest tests/driver/test_commands_hashes.py -v
```

Expected: every test PASSES (no FAIL, possibly some SKIP if the container's server reports < 7.4 — Valkey 8.0 supports it).

- [ ] **Step 3: Run the suite under cp314t**

```bash
.venv-ft/bin/uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
.venv-ft/bin/uv run pytest tests/driver/test_commands_hashes.py -n auto
```

Expected: same green.

- [ ] **Step 4: Add CHANGELOG entry**

Append under `### Added` in `CHANGELOG.md`:

```markdown
- Hash commands: `HGET`, `HSET` (with `mapping=` kwarg + variadic positional pairs), `HSETNX`, `HMSET` (with one-shot `DeprecationWarning` for upstream-deprecated parity), `HGETALL`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HRANDFIELD` (with `count=`/`withvalues=`), `HSCAN` (with `match=`/`count=`/`novalues=`).
- Redis 7.4 hash-field TTL family: `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HEXPIRETIME`, `HPEXPIRETIME`, `HTTL`, `HPTTL`, `HPERSIST` — with the `NX`/`XX`/`GT`/`LT` modifier matrix.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 05 entry"
```

---

## Self-review checklist for this plan

- [x] Spec coverage — every command in the assignment block has a sub-task: HGET, HSET (mapping + variadic), HSETNX, HMSET (with DeprecationWarning), HGETALL (dict[bytes,bytes]), HDEL (variadic), HINCRBY, HINCRBYFLOAT, HKEYS, HVALS, HEXISTS, HLEN, HMGET (variadic), HSCAN (match/count/novalues), HRANDFIELD (count/withvalues), HEXPIRE, HPEXPIRE, HEXPIREAT, HPEXPIREAT, HEXPIRETIME, HPEXPIRETIME, HTTL, HPTTL, HPERSIST.
- [x] No placeholders: every step ships actual code, every test step ships an explicit pass/fail expectation.
- [x] Type consistency: Rust signatures (`hexpire(... fields: Vec<String>, time: i64, nx: bool, ...)`) match `.pyi` stubs (`hexpire(key: str, fields: list[str], time: int, *, nx: bool = ...) -> list[int]`) match test usage (`hexpire("h", ["a"], 60, nx=True)`).
- [x] TTL family gated correctly: `_server_supports_hexpire` probes via `INFO server` and `pytest.skip`s on < 7.4.
- [x] HMSET deprecation: explicit `warnings.warn(..., DeprecationWarning)` in both sync + async paths, asserted in tests.
- [x] All file paths absolute or repo-relative-from-root.
- [x] Sub-task grouping matches the assignment: (a) HGET/HSET/HSETNX, (b) HMSET/HGETALL/HMGET, (c) HDEL/HEXISTS/HLEN, (d) HKEYS/HVALS/HRANDFIELD, (e) HINCRBY/HINCRBYFLOAT, (f) HSCAN, (g) hash-field TTL family.
- [x] Frequent commits: 10 across 10 tasks, each independently revertable, each with conventional-commit `feat(hashes):` / `docs(changelog):` prefixes.
- [x] `commands/hashes.rs` module path declared from `lib.rs` via `mod commands;` + `commands/mod.rs::pub mod hashes;`.
- [x] Out-of-scope items deferred to later plans: façade bindings (10), pipelines (13), `decode_responses=True` (12).
