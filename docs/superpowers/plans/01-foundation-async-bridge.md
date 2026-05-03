# Plan 01 — Foundation: async bridge + driver skeleton

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the load-bearing infrastructure — process-global tokio runtime (PID-checked, fork-safe), `RedisRsAwaitable` (custom asyncio bridge with cancellation), `ValkeyConn` two-layer connection wrapper, `RedisRsDriver` pyclass with `connect_standard`, plus the eight `_test_*` awaitable helpers — proven end-to-end by `aget`/`get`/`aset`/`set`/`adelete`/`delete`/`aping`/`ping` against a live Valkey via `testcontainers`.

**Architecture:** Lift the cachex pattern verbatim where it makes sense. `OnceLock<Runtime>` + `AtomicU32` PID guard + `Mutex<Option<(u32, &'static Runtime)>>` fork-rebuild path → `get_runtime()`. `RedisRsAwaitable` with 5-poll busy-yield and callback-mode fallback (already designed; port file). `ValkeyConn` = inner enum (Standard variant only this plan, Cluster/Sentinel land in plans 15/16) wrapping `redis::aio::ConnectionManager`, with `Deref` to a method-bearing inner type and a lazy `OnceCell<ValkeyConnInner>` for the blocking conn (used by plan 04). Two macros — `async_op!` and `sync_op!` — collapse per-method boilerplate. `IntoRawResult` trait + a starter set of `From<T> for RawResult` impls form the typed boundary.

**Tech Stack:** Rust 2024 edition, PyO3 0.28 (extension-module), tokio 1.x (rt-multi-thread, sync, time), redis 1.x (tokio-comp, connection-manager, tokio-rustls-comp, tls-rustls-insecure, cache-aio), rustls 0.23 (ring), pytest + pytest-asyncio + testcontainers on the Python side. Python 3.14 + 3.14t.

**Reference material:**
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/async_bridge.rs` (607 LOC) — the full design we're porting verbatim. Header comment notes upstream is `django-vcache`.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs` (top half, lines 1–1381) — the standard-mode connection wiring, dispatch macros, TLS opts, `url_with_resp3`.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs` — for the `async_op!`/`sync_op!` macros, `IntoRawResult` trait, error classification helpers, the `aget`/`get`/`aset`/`set` reference implementations.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/test_helpers.rs` — verbatim port.

**Out of scope for this plan:** Cluster + Sentinel (plans 15, 16). All command families beyond the four canonical examples (plans 03–09). The full redis-py exception hierarchy (plan 02 — for now we use `PyConnectionError` and `PyRuntimeError` as placeholders, exactly like cachex). The high-level `Redis` façade (plan 10).

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  lib.rs                       # pymodule registration (extended)
  runtime.rs                   # tokio runtime + PID-checked fork-safe singleton
  async_bridge.rs              # RedisRsAwaitable + RawResult + redis_value_to_py
  raw_result.rs                # IntoRawResult trait + starter From<T> for RawResult impls
  connection.rs                # ValkeyConn + ValkeyConnInner + connect_standard + dispatch macros
  driver.rs                    # RedisRsDriver pyclass + async_op!/sync_op! macros + 4 canonical commands
  errors.rs                    # classify(), to_py_err() (placeholder versions; refined in plan 02)
  test_helpers.rs              # eight _test_* pyfunctions for awaitable testing
tests/
  conftest.py                  # live Valkey via testcontainers; xdist-safe session fixture
  driver/
    __init__.py
    test_runtime.py            # runtime singleton, PID rebuild, fork safety
    test_connection_standard.py# connect_standard happy path, bad URLs, TLS opts plumb-through
    test_driver_basic.py       # aget/get/aset/set/adelete/delete/aping/ping
  async_bridge/
    __init__.py
    test_resolved.py           # resolved-* helpers: bytes/none/int
    test_delayed.py            # delayed bytes → forces callback mode
    test_pending_dropped.py    # pending + dropped + cancel
    test_errors.py             # error + server_error
    test_done_callbacks.py     # add_done_callback (with + without context)
    test_cancel_in_callback.py # cancel after callback mode is active
```

---

## Task 1: Wire up the new module structure

Bring the new submodules into `lib.rs` so subsequent tasks compile, and lift the dependency surface to match cachex.

**Files:**
- Modify: `crates/redis-rs-py-driver/Cargo.toml` (already matches cachex — verify only)
- Modify: `crates/redis-rs-py-driver/src/lib.rs:1-7`

- [ ] **Step 1: Verify Cargo.toml deps match the cachex feature set**

Read `crates/redis-rs-py-driver/Cargo.toml`. The dependencies block must already say:

```toml
[dependencies]
pyo3 = { version = "0.28", features = ["extension-module"] }
tokio = { version = "1", features = ["rt-multi-thread", "sync", "time"] }
redis = { version = "1", features = [
  "tokio-comp",
  "connection-manager",
  "cluster",
  "cluster-async",
  "tokio-rustls-comp",
  "tls-rustls-insecure",
  "cache-aio",
] }
rustls = { version = "0.23", features = ["ring"] }
```

If anything differs, edit to match. No new deps in this plan.

- [ ] **Step 2: Replace `lib.rs` with the module skeleton**

Overwrite `crates/redis-rs-py-driver/src/lib.rs` with:

```rust
// redis-rs-py-driver — Rust I/O driver for the redis-rs-py Python package.
//
// The async bridge and the standard-connection wiring in this crate are
// derived from django-vcache (MIT, David Burke / GlitchTip), via the
// django-cachex-redis-rs prototype. Keep async_bridge.rs and the upstream
// half of connection.rs in lockstep with django-vcache; if you want to
// diverge, open a discussion first — the design is load-bearing.

mod async_bridge;
mod connection;
mod driver;
mod errors;
mod raw_result;
mod runtime;
mod test_helpers;

use pyo3::prelude::*;

#[pymodule]
fn _driver(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<async_bridge::RedisRsAwaitable>()?;
    m.add_class::<driver::RedisRsDriver>()?;

    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_none, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_int, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_delayed_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_pending, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_dropped, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_error, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_server_error, m)?)?;

    Ok(())
}
```

- [ ] **Step 3: Stub out the new modules so the crate still compiles**

Create the empty placeholder files (next tasks will flesh them out). All seven must exist or `cargo check` fails on the `mod` declarations.

```bash
for f in async_bridge connection driver errors raw_result runtime test_helpers; do
  printf '// placeholder — populated by Plan 01\n' > "crates/redis-rs-py-driver/src/${f}.rs"
done
```

- [ ] **Step 4: Verify the crate still compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings only about unused modules. No errors.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/Cargo.toml crates/redis-rs-py-driver/src/
git commit -m "refactor(driver): add module skeleton for foundation plan"
```

---

## Task 2: Tokio runtime singleton with PID-based fork rebuild

Lift the `OnceLock<Runtime>` + `AtomicU32` PID guard pattern from `django-cachex-redis-rs/src/async_bridge.rs:17-71` into its own `runtime.rs` module so other modules can depend on it without pulling in the whole bridge.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/runtime.rs`
- Test: `tests/driver/test_runtime.py`

- [ ] **Step 1: Write the failing test for the runtime singleton**

Create `tests/driver/__init__.py` (empty) and `tests/driver/test_runtime.py`:

```python
"""Tests that cover the process-global tokio runtime singleton.

The runtime itself is opaque from Python — these tests exercise it
indirectly by constructing the test-helper awaitables (which all use
``get_runtime().spawn(...)``) and asserting they resolve.
"""

import asyncio
import os

import pytest

from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_runtime_resolves_simple_value() -> None:
    awaitable = _driver._test_resolved_int(42)
    assert await awaitable == 42


@pytest.mark.asyncio
async def test_runtime_resolves_after_spawn_blocking() -> None:
    awaitable = _driver._test_delayed_bytes(b"ok", 50)  # 50ms forces callback mode
    assert await awaitable == b"ok"


def test_runtime_survives_fork() -> None:
    """After fork, the parent runtime's threads are dead in the child.
    The PID-checked rebuild should produce a fresh runtime that resolves."""
    pid = os.fork()
    if pid == 0:
        # Child: must rebuild the runtime and the next call must succeed.
        async def _go() -> int:
            return await _driver._test_resolved_int(7)

        try:
            result = asyncio.run(_go())
            os._exit(0 if result == 7 else 1)
        except Exception:  # noqa: BLE001
            os._exit(2)
    else:
        _, status = os.waitpid(pid, 0)
        assert os.WIFEXITED(status), "child crashed"
        assert os.WEXITSTATUS(status) == 0, f"child reported failure: {os.WEXITSTATUS(status)}"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_runtime.py -v`
Expected: build fails (no `_test_resolved_int` etc. in `_driver`), or if build succeeds the import line raises `AttributeError`. Either is the expected red.

- [ ] **Step 3: Implement `runtime.rs`**

Replace `crates/redis-rs-py-driver/src/runtime.rs`:

```rust
// Process-global tokio runtime with fork-safe rebuild.
//
// Verbatim port of django-vcache's runtime singleton (MIT,
// David Burke / GlitchTip), via django-cachex-redis-rs.
//
// Fast path (~99.99% of calls): atomic PID check + OnceLock::get() →
// `&'static Runtime`, no locks, no allocations.
//
// Slow path: first call ever (OnceLock init) or first call after fork
// (Mutex-protected rebuild). After fork we leak the new runtime via
// `Box::leak` because dropping a tokio runtime that has dead worker
// threads from the parent process can hang.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static RUNTIME_PID: AtomicU32 = AtomicU32::new(0);
static FORK_RUNTIME: Mutex<Option<(u32, &'static Runtime)>> = Mutex::new(None);

#[inline]
pub fn get_runtime() -> &'static Runtime {
    let pid = std::process::id();
    if RUNTIME_PID.load(Ordering::Relaxed) == pid {
        return RUNTIME.get().unwrap();
    }
    init_or_fork_runtime(pid)
}

#[cold]
fn init_or_fork_runtime(pid: u32) -> &'static Runtime {
    let stored = RUNTIME_PID.load(Ordering::Relaxed);

    if stored == 0 {
        let rt = RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime")
        });
        RUNTIME_PID.store(pid, Ordering::Relaxed);
        return rt;
    }

    let mut guard = FORK_RUNTIME.lock().unwrap();
    if let Some((stored_pid, rt)) = *guard
        && stored_pid == pid
    {
        return rt;
    }
    let rt: &'static Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime"),
    ));
    *guard = Some((pid, rt));
    rt
}
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings about `get_runtime` being unused (not yet wired into anything else).

- [ ] **Step 5: Commit (tests still failing — that's fine, runtime is needed by Task 3)**

```bash
git add crates/redis-rs-py-driver/src/runtime.rs tests/driver/__init__.py tests/driver/test_runtime.py
git commit -m "feat(runtime): add fork-safe tokio runtime singleton"
```

---

## Task 3: `RawResult` enum + `redis_value_to_py` recursive converter

Lift the `RawResult` enum and the `redis::Value` → Python recursive converter from `django-cachex-redis-rs/src/async_bridge.rs:73-260`. Keep the variant set wide on day one — the foundation needs to know how to build every shape we'll later need so plans 03–09 don't have to back-edit `async_bridge.rs`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs`

- [ ] **Step 1: Replace `async_bridge.rs` with the type and converter halves only**

Overwrite `crates/redis-rs-py-driver/src/async_bridge.rs` with the contents below. (Task 4 fills in the `RedisRsAwaitable` half.) This is paste-grade code from `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/async_bridge.rs:73-260`, with one rename: the file no longer hosts `get_runtime()` (that lives in `runtime.rs`) and the `use` block is updated accordingly.

```rust
// RawResult typed boundary + recursive redis::Value → Python conversion.
//
// Variants are kept wide on day one so the command-family plans (03–09)
// can return without back-editing this file. New variants can be added
// freely as commands need them.
//
// Lifted from django-vcache (MIT, David Burke / GlitchTip) via
// django-cachex-redis-rs. The RedisRsAwaitable half lives below in
// the second region of this file and is also a verbatim port.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

pub enum RawResult {
    Nil,
    OptBytes(Option<Vec<u8>>),
    Bool(bool),
    Int(i64),
    OptInt(Option<i64>),
    F64(f64),
    OptF64(Option<f64>),
    Str(String),
    OptStr(Option<String>),
    OptBytesList(Vec<Option<Vec<u8>>>),
    BytesList(Vec<Vec<u8>>),
    StringList(Vec<String>),
    BytesPairs(Vec<(Vec<u8>, Vec<u8>)>),
    ScoredMembers(Vec<(Vec<u8>, f64)>),
    OptKeyAndBytesList(Option<(String, Vec<Vec<u8>>)>),
    OptKeyAndBytes(Option<(String, Vec<u8>)>),
    CursorAndStrings(u64, Vec<String>),
    Value(redis::Value),
    Error(String),
    ServerError(String),
}

fn redis_value_to_py(py: Python<'_>, v: redis::Value) -> PyResult<Py<PyAny>> {
    match v {
        redis::Value::Nil => Ok(py.None()),
        redis::Value::Int(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        redis::Value::BulkString(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        redis::Value::SimpleString(s) => Ok(PyBytes::new(py, s.as_bytes()).into_any().unbind()),
        redis::Value::Boolean(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::Double(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        redis::Value::Okay => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::Array(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::Map(pairs) => {
            let dict = PyDict::new(py);
            for (k, val) in pairs {
                let k_py = redis_value_to_py(py, k)?;
                let v_py = redis_value_to_py(py, val)?;
                dict.set_item(k_py, v_py)?;
            }
            Ok(dict.into_any().unbind())
        }
        redis::Value::Set(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::Attribute { data, .. } => redis_value_to_py(py, *data),
        redis::Value::Push { kind: _, data } => {
            let py_items: Vec<Py<PyAny>> = data
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::BigNumber(n) => Ok(PyString::new(py, &n.to_string()).into_any().unbind()),
        redis::Value::VerbatimString { text, .. } => {
            Ok(PyBytes::new(py, text.as_bytes()).into_any().unbind())
        }
        redis::Value::ServerError(e) => {
            Err(pyo3::exceptions::PyRuntimeError::new_err(format!("{e:?}")))
        }
        other => Ok(PyString::new(py, &format!("{other:?}")).into_any().unbind()),
    }
}

impl RawResult {
    pub fn into_py(self, py: Python<'_>) -> Result<Py<PyAny>, PyErr> {
        match self {
            RawResult::Nil => Ok(py.None()),
            RawResult::OptBytes(Some(b)) => Ok(PyBytes::new(py, &b).into_any().unbind()),
            RawResult::OptBytes(None) => Ok(py.None()),
            RawResult::Bool(b) => Ok(b.into_pyobject(py).unwrap().to_owned().into_any().unbind()),
            RawResult::Int(n) => Ok(n.into_pyobject(py).unwrap().into_any().unbind()),
            RawResult::Str(s) => Ok(PyString::new(py, &s).into_any().unbind()),
            RawResult::OptStr(Some(s)) => Ok(PyString::new(py, &s).into_any().unbind()),
            RawResult::OptStr(None) => Ok(py.None()),
            RawResult::OptBytesList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|r| match r {
                        Some(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                        None => py.None(),
                    })
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::BytesList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .iter()
                    .map(|b| PyBytes::new(py, b).into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::StringList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .iter()
                    .map(|s| PyString::new(py, s).into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::OptKeyAndBytesList(Some((key, values))) => {
                let py_values: Vec<Py<PyAny>> = values
                    .iter()
                    .map(|b| PyBytes::new(py, b).into_any().unbind())
                    .collect();
                let py_key = PyString::new(py, &key).into_any().unbind();
                let py_list = PyList::new(py, py_values)?.into_any().unbind();
                Ok(PyTuple::new(py, [py_key, py_list])?.into_any().unbind())
            }
            RawResult::OptKeyAndBytesList(None) => Ok(py.None()),
            RawResult::OptKeyAndBytes(Some((key, value))) => {
                let py_key = PyString::new(py, &key).into_any().unbind();
                let py_value = PyBytes::new(py, &value).into_any().unbind();
                Ok(PyTuple::new(py, [py_key, py_value])?.into_any().unbind())
            }
            RawResult::OptKeyAndBytes(None) => Ok(py.None()),
            RawResult::CursorAndStrings(cursor, keys) => {
                let py_cursor = cursor.into_pyobject(py)?.into_any().unbind();
                let py_items: Vec<Py<PyAny>> = keys
                    .iter()
                    .map(|s| PyString::new(py, s).into_any().unbind())
                    .collect();
                let py_list = PyList::new(py, py_items)?.into_any().unbind();
                Ok(PyTuple::new(py, [py_cursor, py_list])?.into_any().unbind())
            }
            RawResult::OptInt(Some(n)) => Ok(n.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptInt(None) => Ok(py.None()),
            RawResult::F64(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptF64(Some(f)) => Ok(f.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptF64(None) => Ok(py.None()),
            RawResult::BytesPairs(pairs) => {
                let dict = PyDict::new(py);
                for (k, v) in pairs {
                    let k_py = PyBytes::new(py, &k).into_any().unbind();
                    let v_py = PyBytes::new(py, &v).into_any().unbind();
                    dict.set_item(k_py, v_py)?;
                }
                Ok(dict.into_any().unbind())
            }
            RawResult::ScoredMembers(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|(member, score)| {
                        let m_py = PyBytes::new(py, &member).into_any().unbind();
                        let s_py = score.into_pyobject(py)?.into_any().unbind();
                        Ok(PyTuple::new(py, [m_py, s_py])?.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::Value(v) => redis_value_to_py(py, v),
            RawResult::Error(e) => Err(pyo3::exceptions::PyConnectionError::new_err(e)),
            RawResult::ServerError(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}
```

- [ ] **Step 2: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with unused-variant warnings only. The `RedisRsAwaitable` reference in `lib.rs` will fail because it doesn't exist yet — comment out the `m.add_class::<async_bridge::RedisRsAwaitable>()?;` and the eight `_test_*` registrations temporarily, OR jump straight to Task 4 to land them. Per TDD discipline, do the minimal thing: comment out the registrations, get green, commit, then re-enable in Task 4.

Edit `crates/redis-rs-py-driver/src/lib.rs:13` block — comment lines 13 and 15-22 out:

```rust
    // m.add_class::<async_bridge::RedisRsAwaitable>()?;
    m.add_class::<driver::RedisRsDriver>()?;

    // m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_bytes, m)?)?;
    // m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_none, m)?)?;
    // ...etc...
```

(But: the `RedisRsDriver` line will also fail because `driver.rs` is still the placeholder. Comment it out too — same edit.)

After edits the only un-commented `add_*` is `m.add("__version__", ...)`.

- [ ] **Step 3: Verify build succeeds**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished`, only warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/async_bridge.rs crates/redis-rs-py-driver/src/lib.rs
git commit -m "feat(driver): add RawResult enum and redis::Value→Python converter"
```

---

## Task 4: `RedisRsAwaitable` pyclass

Port the awaitable half of `async_bridge.rs` from cachex (lines 262–607). This is the file with the most subtle behavior in the whole codebase — the comments at the top explain the 5-poll busy-yield + callback-mode design. Do not adapt or "improve" it on first port.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs` (append the awaitable region)

- [ ] **Step 1: Append the awaitable region to `async_bridge.rs`**

After the `impl RawResult { ... }` block, append:

```rust
// =========================================================================
// RedisRsAwaitable — deferred-callback async bridge.
//
// Verbatim port of django-vcache's RedisRsAwaitable (MIT, David Burke /
// GlitchTip), via django-cachex-redis-rs. Keep this region in lockstep
// with upstream — the design (5-poll busy-yield, callback fallback,
// done-callbacks with optional contextvars.Context, cancel that wakes
// callbacks) is load-bearing.
// =========================================================================

use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::runtime::get_runtime;

struct DoneCallback {
    callback: Py<PyAny>,
    context: Option<Py<PyAny>>,
}

struct CallbackState {
    event_loop: Py<PyAny>,
    callbacks: Vec<DoneCallback>,
    result_slot: Arc<Mutex<Option<Result<RawResult, ()>>>>,
}

#[pyclass]
pub struct RedisRsAwaitable {
    rx: Option<oneshot::Receiver<RawResult>>,
    value: Option<Py<PyAny>>,
    error: Option<Py<PyAny>>,
    resolved: bool,
    cancelled: bool,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
    polls: u8,
    cb: Option<Box<CallbackState>>,
}

fn cancelled_error(py: Python<'_>) -> PyErr {
    if let Ok(asyncio) = py.import("asyncio")
        && let Ok(cls) = asyncio.getattr("CancelledError")
        && let Ok(exc) = cls.call0()
    {
        return PyErr::from_value(exc.into_any());
    }
    pyo3::exceptions::PyRuntimeError::new_err("cancelled")
}

fn deliver_value(
    this: &mut RedisRsAwaitable,
    py: Python<'_>,
    val: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    this.resolved = true;
    this.value = Some(val.clone_ref(py));
    let stop = py
        .get_type::<pyo3::exceptions::PyStopIteration>()
        .call1((val,))?;
    Err(PyErr::from_value(stop.into_any()))
}

fn deliver_error(this: &mut RedisRsAwaitable, py: Python<'_>, err: PyErr) -> PyResult<Py<PyAny>> {
    this.resolved = true;
    this.error = Some(err.value(py).clone().into_any().unbind());
    Err(err)
}

#[pymethods]
impl RedisRsAwaitable {
    fn __await__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[getter]
    fn _loop(&self) -> Option<&Py<PyAny>> {
        self.cb.as_ref().map(|cb| &cb.event_loop)
    }

    fn __next__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut this = slf.borrow_mut(py);

        if this.cancelled {
            return Err(cancelled_error(py));
        }

        if this.resolved {
            if let Some(ref exc) = this.error {
                return Err(PyErr::from_value(exc.bind(py).clone()));
            }
            if let Some(ref value) = this.value {
                let stop = py
                    .get_type::<pyo3::exceptions::PyStopIteration>()
                    .call1((value,))?;
                return Err(PyErr::from_value(stop.into_any()));
            }
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "awaitable already consumed",
            ));
        }

        if let Some(ref cb) = this.cb {
            let maybe = cb.result_slot.lock().unwrap().take();
            if let Some(raw_result) = maybe {
                this.cb = None;
                return match raw_result {
                    Ok(raw) => match raw.into_py(py) {
                        Ok(val) => deliver_value(&mut this, py, val),
                        Err(e) => deliver_error(&mut this, py, e),
                    },
                    Err(()) => deliver_error(
                        &mut this,
                        py,
                        pyo3::exceptions::PyRuntimeError::new_err("operation was dropped"),
                    ),
                };
            }
        }

        if let Some(rx) = this.rx.as_mut() {
            match rx.try_recv() {
                Ok(raw) => {
                    this.rx = None;
                    return match raw.into_py(py) {
                        Ok(val) => deliver_value(&mut this, py, val),
                        Err(e) => deliver_error(&mut this, py, e),
                    };
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    this.rx = None;
                    return deliver_error(
                        &mut this,
                        py,
                        pyo3::exceptions::PyRuntimeError::new_err("operation was dropped"),
                    );
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
        } else if this.resolved {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "awaitable already consumed",
            ));
        }

        this.polls += 1;

        if this.polls <= 5 {
            drop(this);
            return Ok(py.None());
        }

        let rx = this.rx.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("awaitable already consumed")
        })?;

        let asyncio = py.import("asyncio")?;
        let event_loop = asyncio.call_method0("get_running_loop")?;
        this._asyncio_future_blocking = true;

        let event_loop_ref = event_loop.clone().into_any().unbind();
        let awaitable_ref = slf.clone_ref(py).into_any();
        let result_slot = Arc::new(Mutex::new(None));
        this.cb = Some(Box::new(CallbackState {
            event_loop: event_loop.into_any().unbind(),
            callbacks: Vec::new(),
            result_slot: result_slot.clone(),
        }));
        get_runtime().spawn(async move {
            let raw = rx.await;
            let raw_result = match raw {
                Ok(r) => Ok(r),
                Err(_) => Err(()),
            };
            *result_slot.lock().unwrap() = Some(raw_result);
            tokio::task::spawn_blocking(move || {
                Python::try_attach(|py| {
                    if let Ok(wake) = awaitable_ref.getattr(py, "_wake") {
                        let _ =
                            event_loop_ref.call_method1(py, "call_soon_threadsafe", (wake,));
                    }
                });
            });
        });

        drop(this);
        Ok(slf.into_any())
    }

    fn _wake(slf: Py<Self>, py: Python<'_>) {
        let callbacks = {
            let mut this = slf.borrow_mut(py);
            this.cb
                .as_mut()
                .map(|cb| std::mem::take(&mut cb.callbacks))
                .unwrap_or_default()
        };
        for done_cb in callbacks {
            if let Some(ref ctx) = done_cb.context {
                let _ = ctx.call_method1(py, "run", (&done_cb.callback, &slf));
            } else {
                let _ = done_cb.callback.call1(py, (&slf,));
            }
        }
    }

    #[pyo3(signature = (fn_cb, *, context=None))]
    fn add_done_callback(&mut self, fn_cb: Py<PyAny>, context: Option<Py<PyAny>>) {
        if let Some(ref mut cb) = self.cb {
            cb.callbacks.push(DoneCallback {
                callback: fn_cb,
                context,
            });
        }
    }

    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.cancelled {
            return Err(cancelled_error(py));
        }
        if let Some(ref exc) = self.error {
            return Err(PyErr::from_value(exc.bind(py).clone()));
        }
        match &self.value {
            Some(v) => Ok(v.clone_ref(py)),
            None => Ok(py.None()),
        }
    }

    fn exception(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.cancelled {
            let asyncio = py.import("asyncio")?;
            let exc = asyncio.getattr("CancelledError")?.call0()?;
            return Ok(exc.into_any().unbind());
        }
        match &self.error {
            Some(exc) => Ok(exc.clone_ref(py)),
            None => Ok(py.None()),
        }
    }

    #[pyo3(signature = (msg=None))]
    fn cancel(slf: Py<Self>, py: Python<'_>, msg: Option<Py<PyAny>>) -> bool {
        let mut this = slf.borrow_mut(py);
        let _ = msg;
        if this.resolved || this.cancelled {
            return false;
        }
        this.cancelled = true;
        this.rx = None;
        let cb_state = this.cb.take();
        drop(this);
        if let Some(cb) = cb_state {
            for done_cb in cb.callbacks {
                let kwargs = pyo3::types::PyDict::new(py);
                if let Some(ref ctx) = done_cb.context {
                    let _ = kwargs.set_item("context", ctx);
                }
                let _ = cb.event_loop.call_method(
                    py,
                    "call_soon",
                    (&done_cb.callback, slf.bind(py)),
                    Some(&kwargs),
                );
            }
        }
        true
    }

    fn cancelled(&self) -> bool {
        self.cancelled
    }

    fn done(&self) -> bool {
        self.resolved || self.cancelled
    }
}

impl RedisRsAwaitable {
    pub fn new(rx: oneshot::Receiver<RawResult>) -> Self {
        RedisRsAwaitable {
            rx: Some(rx),
            value: None,
            error: None,
            resolved: false,
            cancelled: false,
            _asyncio_future_blocking: false,
            polls: 0,
            cb: None,
        }
    }
}
```

- [ ] **Step 2: Re-enable the awaitable + test-helper registrations in `lib.rs`**

Edit `crates/redis-rs-py-driver/src/lib.rs` — uncomment the `add_class::<async_bridge::RedisRsAwaitable>()` line (still leave the `RedisRsDriver` line and `_test_*` registrations commented; we land them in Task 7 and Task 8 respectively, but the driver class is needed by Task 5 → uncomment its line now too if Task 5 lands first). For this task: just uncomment the `RedisRsAwaitable` line.

- [ ] **Step 3: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings about `RedisRsAwaitable::new` being unused.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/async_bridge.rs crates/redis-rs-py-driver/src/lib.rs
git commit -m "feat(driver): add RedisRsAwaitable asyncio bridge"
```

---

## Task 5: `test_helpers.rs` — eight `_test_*` pyfunctions

Port `django-cachex-redis-rs/src/test_helpers.rs` verbatim. These are the unit-test fixtures for `RedisRsAwaitable` — they let us exercise resolved / pending / dropped / errored / delayed paths without needing a Redis server.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/test_helpers.rs`
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (uncomment registrations)
- Test: `tests/async_bridge/__init__.py` (empty), `tests/async_bridge/test_resolved.py`, `tests/async_bridge/test_delayed.py`, `tests/async_bridge/test_pending_dropped.py`, `tests/async_bridge/test_errors.py`

- [ ] **Step 1: Write failing tests for the resolved-* helpers**

`tests/async_bridge/__init__.py`:

```python
```

`tests/async_bridge/test_resolved.py`:

```python
"""Resolved-state RedisRsAwaitable helpers."""

import pytest

from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_resolved_bytes() -> None:
    assert await _driver._test_resolved_bytes(b"hello") == b"hello"


@pytest.mark.asyncio
async def test_resolved_none() -> None:
    assert await _driver._test_resolved_none() is None


@pytest.mark.asyncio
async def test_resolved_int() -> None:
    assert await _driver._test_resolved_int(7) == 7


@pytest.mark.asyncio
async def test_resolved_int_negative() -> None:
    assert await _driver._test_resolved_int(-1) == -1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/async_bridge/test_resolved.py -v`
Expected: FAIL with `AttributeError: module 'redis_rs_py._driver' has no attribute '_test_resolved_bytes'`.

- [ ] **Step 3: Implement `test_helpers.rs`**

Replace `crates/redis-rs-py-driver/src/test_helpers.rs`:

```rust
// Test scaffolding for RedisRsAwaitable.
//
// Each function constructs a RedisRsAwaitable in a specific resolution
// state without going through the I/O surface, so the awaitable protocol
// can be exercised end-to-end in unit tests against the production class.
//
// Verbatim port of django-vcache's test_helpers.rs (MIT, David Burke /
// GlitchTip), via django-cachex-redis-rs.

use pyo3::prelude::*;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::async_bridge::{RawResult, RedisRsAwaitable};
use crate::runtime::get_runtime;

#[pyfunction]
pub fn _test_resolved_bytes(b: Vec<u8>) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::OptBytes(Some(b)));
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_resolved_none() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::OptBytes(None));
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_resolved_int(n: i64) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::Int(n));
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_delayed_bytes(b: Vec<u8>, delay_ms: u64) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    get_runtime().spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let _ = tx.send(RawResult::OptBytes(Some(b)));
    });
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_pending() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel::<RawResult>();
    // Leak the tx so the rx never closes. The awaitable is intentionally
    // never resolved — used to test cancellation paths.
    std::mem::forget(tx);
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_dropped() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel::<RawResult>();
    drop(tx);
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_error(msg: String) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::Error(msg));
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_server_error(msg: String) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::ServerError(msg));
    RedisRsAwaitable::new(rx)
}
```

- [ ] **Step 4: Re-enable the test-helper registrations in `lib.rs`**

Uncomment all eight `m.add_function(wrap_pyfunction!(test_helpers::_test_*, m)?)?;` lines in `crates/redis-rs-py-driver/src/lib.rs`.

- [ ] **Step 5: Build + run the resolved tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/async_bridge/test_resolved.py -v`
Expected: 4 PASS.

- [ ] **Step 6: Add the delayed-bytes test (forces callback mode)**

`tests/async_bridge/test_delayed.py`:

```python
"""Delayed-resolution awaitable — forces 6+ poll misses → callback mode."""

import pytest

from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_delayed_resolves() -> None:
    assert await _driver._test_delayed_bytes(b"slow", 50) == b"slow"


@pytest.mark.asyncio
async def test_delayed_zero_ms_still_works() -> None:
    """A zero-delay sleep still defers across at least one event-loop tick."""
    assert await _driver._test_delayed_bytes(b"fast", 0) == b"fast"


@pytest.mark.asyncio
async def test_delayed_marks_callback_state() -> None:
    """After we await past 5 polls, the awaitable should have a _loop."""
    awaitable = _driver._test_delayed_bytes(b"x", 100)
    # Avoid `await` here so we can introspect mid-flight: schedule a quick
    # asyncio.sleep so the await scheduler iterates past the busy-yield window.
    import asyncio
    task = asyncio.create_task(_consume(awaitable))
    await asyncio.sleep(0.02)
    # By now we've polled at least 6 times and entered callback mode.
    assert awaitable._loop is not None
    await task


async def _consume(aw):
    return await aw
```

Run: `uv run pytest tests/async_bridge/test_delayed.py -v`
Expected: 3 PASS.

- [ ] **Step 7: Add the pending + dropped + cancel tests**

`tests/async_bridge/test_pending_dropped.py`:

```python
"""Pending (never resolves) and dropped (sender drops) awaitables."""

import asyncio

import pytest

from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_pending_never_resolves_within_window() -> None:
    aw = _driver._test_pending()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(aw, timeout=0.1)


@pytest.mark.asyncio
async def test_dropped_raises_runtime_error() -> None:
    aw = _driver._test_dropped()
    with pytest.raises(RuntimeError, match="dropped"):
        await aw


@pytest.mark.asyncio
async def test_pending_can_be_cancelled() -> None:
    aw = _driver._test_pending()
    task = asyncio.create_task(_consume(aw))
    await asyncio.sleep(0.02)  # let the task enter callback mode
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert aw.cancelled() is True
    assert aw.done() is True


async def _consume(aw):
    return await aw
```

Run: `uv run pytest tests/async_bridge/test_pending_dropped.py -v`
Expected: 3 PASS.

- [ ] **Step 8: Add the error tests**

`tests/async_bridge/test_errors.py`:

```python
"""Error and ServerError variants."""

import pytest

from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_error_raises_connection_error() -> None:
    aw = _driver._test_error("could not connect")
    with pytest.raises(ConnectionError, match="could not connect"):
        await aw


@pytest.mark.asyncio
async def test_server_error_raises_runtime_error() -> None:
    aw = _driver._test_server_error("WRONGTYPE")
    with pytest.raises(RuntimeError, match="WRONGTYPE"):
        await aw


@pytest.mark.asyncio
async def test_resolved_then_result_returns_value() -> None:
    aw = _driver._test_resolved_int(99)
    assert await aw == 99
    assert aw.result() == 99
    assert aw.exception() is None


@pytest.mark.asyncio
async def test_errored_then_exception_returns_exc() -> None:
    aw = _driver._test_error("boom")
    with pytest.raises(ConnectionError):
        await aw
    exc = aw.exception()
    assert isinstance(exc, ConnectionError)
```

Run: `uv run pytest tests/async_bridge/test_errors.py -v`
Expected: 4 PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/redis-rs-py-driver/src/test_helpers.rs crates/redis-rs-py-driver/src/lib.rs tests/async_bridge/
git commit -m "feat(driver): add _test_* awaitable helpers and bridge tests"
```

---

## Task 6: `add_done_callback` + `cancel` regression coverage

The Task 5 tests cover the basic happy path. Now pin down the `add_done_callback(fn, *, context=ctx)` contract (asyncio.Task's path) and the cancel-after-callback-mode path explicitly. These two are the parts of `RedisRsAwaitable` that are most likely to break under future edits.

**Files:**
- Test: `tests/async_bridge/test_done_callbacks.py`
- Test: `tests/async_bridge/test_cancel_in_callback.py`

- [ ] **Step 1: Write the done-callback test**

`tests/async_bridge/test_done_callbacks.py`:

```python
"""add_done_callback with and without contextvars.Context."""

import asyncio
import contextvars

import pytest

from redis_rs_py import _driver

VAR: contextvars.ContextVar[str] = contextvars.ContextVar("VAR", default="default")


@pytest.mark.asyncio
async def test_done_callback_without_context() -> None:
    aw = _driver._test_delayed_bytes(b"x", 50)
    seen: list[object] = []

    # Trigger entry into callback mode by yielding past the busy-yield window.
    task = asyncio.create_task(_consume(aw))
    await asyncio.sleep(0.02)

    aw.add_done_callback(lambda fut: seen.append(fut))
    await task
    # _wake fires the callback synchronously after StopIteration delivery.
    assert len(seen) == 1
    assert seen[0] is aw


@pytest.mark.asyncio
async def test_done_callback_runs_in_provided_context() -> None:
    aw = _driver._test_delayed_bytes(b"x", 50)
    captured: list[str] = []

    ctx = contextvars.copy_context()
    ctx.run(VAR.set, "from-context")

    task = asyncio.create_task(_consume(aw))
    await asyncio.sleep(0.02)

    aw.add_done_callback(lambda _fut: captured.append(VAR.get()), context=ctx)
    await task

    # The callback ran inside `ctx`, so it must observe the value set there
    # rather than the default.
    assert captured == ["from-context"]


async def _consume(aw):
    return await aw
```

Run: `uv run pytest tests/async_bridge/test_done_callbacks.py -v`
Expected: 2 PASS.

- [ ] **Step 2: Write the cancel-in-callback-mode test**

`tests/async_bridge/test_cancel_in_callback.py`:

```python
"""Cancel after callback-mode initialisation must wake pending callbacks."""

import asyncio

import pytest

from redis_rs_py import _driver


@pytest.mark.asyncio
async def test_cancel_wakes_pending_done_callback() -> None:
    aw = _driver._test_pending()
    fired = asyncio.Event()

    task = asyncio.create_task(_consume(aw))
    await asyncio.sleep(0.02)  # callback mode entered

    aw.add_done_callback(lambda _fut: fired.set())

    assert aw.cancel() is True
    # Cancellation must schedule the callback (loop.call_soon).
    await asyncio.wait_for(fired.wait(), timeout=0.5)

    with pytest.raises(asyncio.CancelledError):
        await task

    assert aw.cancel() is False  # already cancelled


async def _consume(aw):
    return await aw
```

Run: `uv run pytest tests/async_bridge/test_cancel_in_callback.py -v`
Expected: 1 PASS.

- [ ] **Step 3: Run the full async_bridge suite**

Run: `uv run pytest tests/async_bridge/ -v`
Expected: 16 PASS, 0 FAIL.

- [ ] **Step 4: Commit**

```bash
git add tests/async_bridge/test_done_callbacks.py tests/async_bridge/test_cancel_in_callback.py
git commit -m "test(async_bridge): add done-callback + cancel-in-callback regressions"
```

---

## Task 7: `errors.rs` placeholder + `raw_result.rs` `IntoRawResult` trait

Two small support files. `errors.rs` holds `is_connection_error`, `classify`, and `to_py_err` from cachex (unchanged — plan 02 will swap these to the redis-py exception hierarchy). `raw_result.rs` holds the `IntoRawResult` trait + a starter set of `From<T> for RawResult` impls so command bodies can write `.into_raw_result()`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/errors.rs`
- Modify: `crates/redis-rs-py-driver/src/raw_result.rs`

- [ ] **Step 1: Implement `errors.rs`**

Replace `crates/redis-rs-py-driver/src/errors.rs`:

```rust
// Error classification helpers.
//
// PLACEHOLDER: returns PyConnectionError / PyRuntimeError pairs (matching
// django-cachex). Plan 02 swaps these for the full redis.exceptions
// hierarchy (RedisError, ConnectionError, TimeoutError, ResponseError,
// BusyLoadingError, NoScriptError, ReadOnlyError, etc.).

use pyo3::PyErr;

use crate::async_bridge::RawResult;

pub fn is_connection_error(e: &redis::RedisError) -> bool {
    matches!(
        e.kind(),
        redis::ErrorKind::Io
            | redis::ErrorKind::Server(redis::ServerErrorKind::BusyLoading)
            | redis::ErrorKind::Server(redis::ServerErrorKind::TryAgain)
            | redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly)
    ) || e.is_connection_dropped()
        || e.is_connection_refusal()
        || e.is_timeout()
}

pub fn classify(e: redis::RedisError) -> RawResult {
    if is_connection_error(&e) {
        RawResult::Error(e.to_string())
    } else {
        RawResult::ServerError(e.to_string())
    }
}

pub fn to_py_err(e: redis::RedisError) -> PyErr {
    if is_connection_error(&e) {
        pyo3::exceptions::PyConnectionError::new_err(e.to_string())
    } else {
        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
    }
}
```

- [ ] **Step 2: Implement `raw_result.rs`**

Replace `crates/redis-rs-py-driver/src/raw_result.rs`:

```rust
// IntoRawResult trait + From<T> for RawResult impls.
//
// Lets command bodies write `.await.into_raw_result()` regardless of the
// concrete redis-rs return type. Each typed return needs:
//   1. A RawResult variant (in async_bridge.rs).
//   2. A `From<T> for RawResult` impl here (so IntoRawResult covers it).
//   3. (Optional) A sync py_* helper in driver.rs for the matching sync method.

use crate::async_bridge::RawResult;
use crate::errors::classify;

pub trait IntoRawResult {
    fn into_raw_result(self) -> RawResult;
}

impl<T> IntoRawResult for redis::RedisResult<T>
where
    T: Into<RawResult>,
{
    fn into_raw_result(self) -> RawResult {
        match self {
            Ok(v) => v.into(),
            Err(e) => classify(e),
        }
    }
}

impl From<()> for RawResult {
    fn from(_: ()) -> Self {
        RawResult::Nil
    }
}

impl From<bool> for RawResult {
    fn from(v: bool) -> Self {
        RawResult::Bool(v)
    }
}

impl From<i64> for RawResult {
    fn from(v: i64) -> Self {
        RawResult::Int(v)
    }
}

impl From<u64> for RawResult {
    fn from(v: u64) -> Self {
        // u64 → i64 truncating cast is fine: redis returns signed counts and
        // u64 returns from EXISTS/DEL fit in i64 range in any realistic setup.
        RawResult::Int(v as i64)
    }
}

impl From<f64> for RawResult {
    fn from(v: f64) -> Self {
        RawResult::F64(v)
    }
}

impl From<Option<i64>> for RawResult {
    fn from(v: Option<i64>) -> Self {
        RawResult::OptInt(v)
    }
}

impl From<Option<f64>> for RawResult {
    fn from(v: Option<f64>) -> Self {
        RawResult::OptF64(v)
    }
}

impl From<Vec<u8>> for RawResult {
    fn from(v: Vec<u8>) -> Self {
        RawResult::OptBytes(Some(v))
    }
}

impl From<Option<Vec<u8>>> for RawResult {
    fn from(v: Option<Vec<u8>>) -> Self {
        RawResult::OptBytes(v)
    }
}

impl From<String> for RawResult {
    fn from(v: String) -> Self {
        RawResult::Str(v)
    }
}

impl From<Option<String>> for RawResult {
    fn from(v: Option<String>) -> Self {
        RawResult::OptStr(v)
    }
}

impl From<Vec<Vec<u8>>> for RawResult {
    fn from(v: Vec<Vec<u8>>) -> Self {
        RawResult::BytesList(v)
    }
}

impl From<Vec<Option<Vec<u8>>>> for RawResult {
    fn from(v: Vec<Option<Vec<u8>>>) -> Self {
        RawResult::OptBytesList(v)
    }
}

impl From<Vec<String>> for RawResult {
    fn from(v: Vec<String>) -> Self {
        RawResult::StringList(v)
    }
}

impl From<Vec<(Vec<u8>, Vec<u8>)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        RawResult::BytesPairs(v)
    }
}

impl From<Vec<(Vec<u8>, f64)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, f64)>) -> Self {
        RawResult::ScoredMembers(v)
    }
}

impl From<Option<(String, Vec<u8>)>> for RawResult {
    fn from(v: Option<(String, Vec<u8>)>) -> Self {
        RawResult::OptKeyAndBytes(v)
    }
}

impl From<Option<(String, Vec<Vec<u8>>)>> for RawResult {
    fn from(v: Option<(String, Vec<Vec<u8>>)>) -> Self {
        RawResult::OptKeyAndBytesList(v)
    }
}

impl From<(u64, Vec<String>)> for RawResult {
    fn from(v: (u64, Vec<String>)) -> Self {
        RawResult::CursorAndStrings(v.0, v.1)
    }
}

impl From<redis::Value> for RawResult {
    fn from(v: redis::Value) -> Self {
        RawResult::Value(v)
    }
}
```

- [ ] **Step 3: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with unused-warning noise only.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/errors.rs crates/redis-rs-py-driver/src/raw_result.rs
git commit -m "feat(driver): add error classification and IntoRawResult trait"
```

---

## Task 8: `connection.rs` — `ValkeyConn`/`ValkeyConnInner` (Standard variant only) + `connect_standard`

Lift the connection wrapper from `django-cachex-redis-rs/src/connection.rs:1-1381`, but for this plan keep only the `Standard` variant of `ValkeyConnInner`. The Cluster + Sentinel variants land in plans 15 and 16 — leaving them out now means the dispatch macros are a single-arm match that compiles trivially. The blocking-conn lazy `OnceCell` stays in (plan 04 needs it).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`

- [ ] **Step 1: Write `connection.rs`**

Replace `crates/redis-rs-py-driver/src/connection.rs`:

```rust
// Connection wrappers and pool wiring.
//
// This is the "standard" half of django-vcache's connection.rs (MIT,
// David Burke / GlitchTip), via django-cachex-redis-rs. Cluster and
// Sentinel variants land in plans 15 and 16; they slot in as new
// `ValkeyConnInner` arms without changing the public API.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::caching::CacheConfig;
use redis::{Client, RedisResult};
use tokio::sync::OnceCell;

#[derive(Clone, Debug)]
pub struct TlsOpts {
    pub root_cert: Option<Vec<u8>>,
    pub client_cert: Option<Vec<u8>>,
    pub client_key: Option<Vec<u8>>,
}

impl TlsOpts {
    fn to_tls_certs(&self) -> redis::TlsCertificates {
        let mut builder = redis::TlsCertificates::default();
        if let Some(ref ca) = self.root_cert {
            builder = builder.with_root_certificates(ca.clone());
        }
        if let (Some(cert), Some(key)) = (self.client_cert.as_ref(), self.client_key.as_ref()) {
            builder = builder.with_client_authentication(redis::ClientTlsConfig {
                client_cert: cert.clone(),
                client_key: key.clone(),
            });
        }
        builder
    }
}

#[derive(Clone, Debug)]
pub struct ClientCacheOpts {
    pub max_size: usize,
    pub ttl_secs: u64,
}

#[derive(Clone)]
enum ConnConfig {
    Standard {
        url: Arc<str>,
        tls_opts: Option<TlsOpts>,
    },
}

#[derive(Clone)]
pub enum ValkeyConnInner {
    Standard(ConnectionManager),
}

#[derive(Clone)]
pub struct ValkeyConn {
    regular: ValkeyConnInner,
    blocking: Arc<OnceCell<ValkeyConnInner>>,
    config: ConnConfig,
}

impl std::ops::Deref for ValkeyConn {
    type Target = ValkeyConnInner;
    fn deref(&self) -> &Self::Target {
        &self.regular
    }
}

impl std::ops::DerefMut for ValkeyConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.regular
    }
}

impl ValkeyConn {
    /// Lazily initialize a second connection for blocking commands so they
    /// don't head-of-line-block the multiplexed pipeline. Used by Plan 04.
    pub async fn get_blocking(&self) -> RedisResult<ValkeyConnInner> {
        let conn = self
            .blocking
            .get_or_try_init(|| async { build_blocking(&self.config).await })
            .await?;
        Ok(conn.clone())
    }

    pub fn cache_statistics(&self) -> Option<redis::caching::CacheStatistics> {
        match &self.regular {
            ValkeyConnInner::Standard(c) => c.get_cache_statistics(),
        }
    }
}

// =========================================================================
// URL helpers
// =========================================================================

/// Force `protocol=resp3` on every URL so client-side caching works and
/// reply types are unified across topologies.
pub fn url_with_resp3(url: &str) -> String {
    if url.contains("protocol=") {
        return url.to_string();
    }
    let (base, fragment) = match url.split_once('#') {
        Some((b, f)) => (b, Some(f)),
        None => (url, None),
    };
    let sep = if base.contains('?') { '&' } else { '?' };
    let mut out = format!("{base}{sep}protocol=resp3");
    if let Some(f) = fragment {
        out.push('#');
        out.push_str(f);
    }
    out
}

// =========================================================================
// Constructors
// =========================================================================

fn create_client(url: &str, tls_opts: Option<&TlsOpts>) -> RedisResult<Client> {
    match tls_opts {
        Some(opts) => Client::build_with_tls(url, opts.to_tls_certs()),
        None => Client::open(url),
    }
}

fn conn_manager_config(cache: Option<&ClientCacheOpts>) -> ConnectionManagerConfig {
    let mut cfg = ConnectionManagerConfig::new()
        .set_pipeline_buffer_size(1000)
        .set_response_timeout(Some(Duration::from_secs(30)));
    if let Some(opts) = cache {
        let cc = CacheConfig::new()
            .set_size(NonZeroUsize::new(opts.max_size).unwrap_or(NonZeroUsize::MIN))
            .set_default_client_ttl(Duration::from_secs(opts.ttl_secs));
        cfg = cfg.set_cache_config(cc);
    }
    cfg
}

fn blocking_conn_manager_config() -> ConnectionManagerConfig {
    ConnectionManagerConfig::new()
        .set_pipeline_buffer_size(1000)
        .set_response_timeout(None)
}

pub async fn connect_standard(
    url: &str,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
) -> Result<ValkeyConn, String> {
    let url = url_with_resp3(url);
    let client = create_client(&url, tls_opts.as_ref()).map_err(|e| e.to_string())?;
    let cfg = conn_manager_config(cache_opts.as_ref());
    let mgr = ConnectionManager::new_with_config(client, cfg)
        .await
        .map_err(|e| e.to_string())?;
    Ok(ValkeyConn {
        regular: ValkeyConnInner::Standard(mgr),
        blocking: Arc::new(OnceCell::new()),
        config: ConnConfig::Standard {
            url: Arc::from(url),
            tls_opts,
        },
    })
}

async fn build_blocking(cfg: &ConnConfig) -> RedisResult<ValkeyConnInner> {
    match cfg {
        ConnConfig::Standard { url, tls_opts } => {
            let client = create_client(url, tls_opts.as_ref())?;
            let cfg = blocking_conn_manager_config();
            let mgr = ConnectionManager::new_with_config(client, cfg).await?;
            Ok(ValkeyConnInner::Standard(mgr))
        }
    }
}

// =========================================================================
// Dispatch macros
// =========================================================================

/// For commands that build a `redis::Cmd` by hand and call `.query_async`.
#[macro_export]
macro_rules! dispatch_cmd {
    ($self:expr, $cmd:expr) => {
        match $self {
            $crate::connection::ValkeyConnInner::Standard(c) => $cmd.query_async(c).await,
        }
    };
}

/// For commands that call a method on `redis::AsyncCommands`.
#[macro_export]
macro_rules! conn_method {
    ($self:expr, $c:ident, $op:expr) => {
        match $self {
            $crate::connection::ValkeyConnInner::Standard($c) => $op.await,
        }
    };
}
```

- [ ] **Step 2: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with unused warnings only.

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs
git commit -m "feat(driver): add ValkeyConn standard wrapper and connect_standard"
```

---

## Task 9: `driver.rs` — `RedisRsDriver` pyclass + `connect_standard` factory + 4 canonical commands

End-to-end proof: `aget`/`get`/`aset`/`set`/`adelete`/`delete`/`aping`/`ping`. Defines the `async_op!` and `sync_op!` macros that every command-family plan will reuse. After this task lands, plans 03–09 are mechanical — each adds one `From` impl to `raw_result.rs` (if needed) plus a method on `RedisRsDriver` per command.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/driver.rs`
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (uncomment `RedisRsDriver` registration)

- [ ] **Step 1: Add the failing tests for the four canonical commands**

`tests/conftest.py` (replace whatever's there — it's currently empty):

```python
"""Live-Valkey fixtures for the driver and façade test suites.

We use testcontainers to bring up a single shared Valkey instance per
pytest session. The `valkey_url` fixture is xdist-safe: the worker that
wins the race owns the container; other workers wait on a sidecar file.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from filelock import FileLock
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

VALKEY_IMAGE = os.environ.get("REDIS_RS_PY_VALKEY_IMAGE", "valkey/valkey:8.0")


def _spawn_valkey() -> tuple[DockerContainer, str]:
    container = DockerContainer(VALKEY_IMAGE).with_exposed_ports(6379)
    container.start()
    wait_for_logs(container, "Ready to accept connections", timeout=30)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)
    return container, f"redis://{host}:{port}/0"


@pytest.fixture(scope="session")
def valkey_url(tmp_path_factory: pytest.TempPathFactory, worker_id: str) -> Iterator[str]:
    if worker_id == "master":
        container, url = _spawn_valkey()
        try:
            yield url
        finally:
            container.stop()
        return

    root = tmp_path_factory.getbasetemp().parent
    lockfile = root / "valkey.lock"
    urlfile = root / "valkey.url"

    with FileLock(str(lockfile)):
        if urlfile.exists():
            container = None
            url = urlfile.read_text().strip()
        else:
            container, url = _spawn_valkey()
            urlfile.write_text(url)

    try:
        yield url
    finally:
        if container is not None:
            container.stop()
            urlfile.unlink(missing_ok=True)


@pytest.fixture
def driver(valkey_url: str):
    from redis_rs_py._driver import RedisRsDriver

    drv = RedisRsDriver.connect_standard(valkey_url)
    # FLUSHDB so each test starts clean. We call sync `flushdb` once it lands;
    # for now use the upstream redis-py client.
    import redis

    rp = redis.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    return drv
```

`tests/driver/test_driver_basic.py`:

```python
"""End-to-end smoke tests for the canonical 4 commands."""

import asyncio

import pytest


def test_set_get_sync(driver) -> None:
    driver.set("key", b"value")
    assert driver.get("key") == b"value"


def test_get_missing_returns_none(driver) -> None:
    assert driver.get("missing") is None


def test_delete_returns_count(driver) -> None:
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.delete("a", "b", "c") == 2


def test_ping(driver) -> None:
    assert driver.ping() is True


@pytest.mark.asyncio
async def test_aset_aget_async(driver) -> None:
    await driver.aset("k", b"v")
    assert await driver.aget("k") == b"v"


@pytest.mark.asyncio
async def test_aget_missing_returns_none(driver) -> None:
    assert await driver.aget("missing") is None


@pytest.mark.asyncio
async def test_adelete_returns_count(driver) -> None:
    await driver.aset("a", b"1")
    await driver.aset("b", b"2")
    assert await driver.adelete("a", "b", "c") == 2


@pytest.mark.asyncio
async def test_aping(driver) -> None:
    assert await driver.aping() is True


def test_set_with_ttl(driver) -> None:
    driver.set("key", b"value", ttl=60)
    # Use the upstream client to verify TTL was applied (no `ttl` command yet).
    import redis as upstream

    rp = upstream.Redis.from_url(driver_url := driver.connection_url())
    assert 0 < rp.ttl("key") <= 60
    rp.close()


def test_connect_standard_bad_url_raises_connection_error() -> None:
    from redis_rs_py._driver import RedisRsDriver

    with pytest.raises(ConnectionError):
        RedisRsDriver.connect_standard("redis://127.0.0.1:1/0")
```

(`driver.connection_url()` is referenced by `test_set_with_ttl` — implement it as a `#[getter]` returning the resolved-with-resp3 URL. Add it to the `RedisRsDriver` impl in step 2.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_driver_basic.py -v`
Expected: FAIL with `AttributeError: type object 'RedisRsDriver' has no attribute 'connect_standard'` (and the rest).

- [ ] **Step 3: Implement `driver.rs`**

Replace `crates/redis-rs-py-driver/src/driver.rs`:

```rust
// RedisRsDriver pyclass — the typed, method-per-command surface.
//
// Each command exists as a sync + async pair:
//   * sync `<cmd>(...)` releases the GIL with `py.detach`, blocks on the
//     runtime, returns the typed value directly.
//   * async `a<cmd>(...)` spawns onto the runtime, returns a
//     RedisRsAwaitable. The tokio task never touches the GIL; it sends
//     a RawResult through a oneshot::channel.
//
// Commands are organized into per-family files under `commands/` (added
// by plans 03-09). This file holds the driver class itself, the
// connect_* factories, and the four canonical examples
// (get/set/delete/ping).

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use redis::AsyncCommands;

use crate::async_bridge::{RawResult, RedisRsAwaitable};
use crate::connection::{ClientCacheOpts, TlsOpts, ValkeyConn, connect_standard};
use crate::errors::to_py_err;
use crate::raw_result::IntoRawResult;
use crate::runtime::get_runtime;
use crate::{conn_method, dispatch_cmd};

// =========================================================================
// Macros: async_op! and sync_op!
// =========================================================================

/// Spawn an async block on the runtime, return a RedisRsAwaitable to Python.
/// `$body` must evaluate to a `RawResult`.
#[macro_export]
macro_rules! async_op {
    ($self:expr, $py:expr, $conn:ident, $body:expr) => {{
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = $crate::async_bridge::RedisRsAwaitable::new(rx);
        let mut $conn = $self.connection.clone();
        $crate::runtime::get_runtime().spawn(async move {
            let result: $crate::async_bridge::RawResult = async { $body }.await;
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject($py)?.into_any().unbind())
    }};
}

/// Block on the runtime in a GIL-released closure, return the inner Result.
#[macro_export]
macro_rules! sync_op {
    ($py:expr, $self:expr, $conn:ident, $body:expr) => {{
        let mut $conn = $self.connection.clone();
        $py.detach(|| $crate::runtime::get_runtime().block_on(async { $body }))
    }};
}

// =========================================================================
// Sync conversion helpers (Rust → Python; mirror the RawResult variants)
// =========================================================================

pub(crate) fn py_opt_bytes(py: Python<'_>, v: Option<Vec<u8>>) -> Py<PyAny> {
    match v {
        Some(b) => PyBytes::new(py, &b).into_any().unbind(),
        None => py.None(),
    }
}

pub(crate) fn py_int(py: Python<'_>, v: i64) -> PyResult<Py<PyAny>> {
    Ok(v.into_pyobject(py)?.into_any().unbind())
}

pub(crate) fn py_bool(py: Python<'_>, v: bool) -> PyResult<Py<PyAny>> {
    Ok(v.into_pyobject(py)?.to_owned().into_any().unbind())
}

#[allow(dead_code)]
pub(crate) fn py_bytes_list(py: Python<'_>, v: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
    let items: Vec<Py<PyAny>> = v
        .iter()
        .map(|b| PyBytes::new(py, b).into_any().unbind())
        .collect();
    Ok(PyList::new(py, items)?.into_any().unbind())
}

#[allow(dead_code)]
pub(crate) fn py_string_list(py: Python<'_>, v: Vec<String>) -> PyResult<Py<PyAny>> {
    let items: Vec<Py<PyAny>> = v
        .iter()
        .map(|s| PyString::new(py, s).into_any().unbind())
        .collect();
    Ok(PyList::new(py, items)?.into_any().unbind())
}

#[allow(dead_code)]
pub(crate) fn py_bytes_pairs(py: Python<'_>, v: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (k, val) in v {
        dict.set_item(
            PyBytes::new(py, &k).into_any().unbind(),
            PyBytes::new(py, &val).into_any().unbind(),
        )?;
    }
    Ok(dict.into_any().unbind())
}

#[allow(dead_code)]
pub(crate) fn py_tuple2(py: Python<'_>, a: Py<PyAny>, b: Py<PyAny>) -> PyResult<Py<PyAny>> {
    Ok(PyTuple::new(py, [a, b])?.into_any().unbind())
}

// =========================================================================
// Driver pyclass
// =========================================================================

#[pyclass(module = "redis_rs_py._driver")]
pub struct RedisRsDriver {
    connection: ValkeyConn,
    url: String,
}

#[pymethods]
impl RedisRsDriver {
    #[staticmethod]
    #[pyo3(signature = (
        url,
        *,
        cache_max_size = None,
        cache_ttl_secs = None,
        ssl_ca_certs = None,
        ssl_certfile = None,
        ssl_keyfile = None,
    ))]
    fn connect_standard(
        py: Python<'_>,
        url: String,
        cache_max_size: Option<usize>,
        cache_ttl_secs: Option<u64>,
        ssl_ca_certs: Option<Vec<u8>>,
        ssl_certfile: Option<Vec<u8>>,
        ssl_keyfile: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        let cache_opts = match (cache_max_size, cache_ttl_secs) {
            (None, None) => None,
            (max, ttl) => Some(ClientCacheOpts {
                max_size: max.unwrap_or(10_000),
                ttl_secs: ttl.unwrap_or(300),
            }),
        };
        let tls_opts =
            if ssl_ca_certs.is_some() || ssl_certfile.is_some() || ssl_keyfile.is_some() {
                Some(TlsOpts {
                    root_cert: ssl_ca_certs,
                    client_cert: ssl_certfile,
                    client_key: ssl_keyfile,
                })
            } else {
                None
            };
        let url_clone = url.clone();
        let conn = py.detach(|| {
            get_runtime().block_on(async {
                connect_standard(&url_clone, cache_opts, tls_opts).await
            })
        });
        match conn {
            Ok(c) => Ok(RedisRsDriver {
                connection: c,
                url,
            }),
            Err(e) => Err(pyo3::exceptions::PyConnectionError::new_err(e)),
        }
    }

    #[getter]
    fn connection_url(&self) -> &str {
        &self.url
    }

    fn cache_statistics(&self) -> Option<(usize, usize, usize)> {
        self.connection.cache_statistics().map(|s| {
            (
                s.hit_count as usize,
                s.miss_count as usize,
                s.invalidate_count as usize,
            )
        })
    }

    // --- get / aget --------------------------------------------------------

    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.get(key)));
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn aget(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Option<Vec<u8>>> =
                conn_method!(&mut conn, c, c.get(&key));
            r.into_raw_result()
        })
    }

    // --- set / aset --------------------------------------------------------

    #[pyo3(signature = (key, value, ttl=None))]
    fn set(
        &self,
        py: Python<'_>,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> PyResult<()> {
        let value = value.to_vec();
        let r: redis::RedisResult<()> = sync_op!(py, self, conn, async {
            match ttl {
                Some(s) => conn_method!(&mut conn, c, c.set_ex::<_, _, ()>(key, value, s)),
                None => conn_method!(&mut conn, c, c.set::<_, _, ()>(key, value)),
            }
        });
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn aset(
        &self,
        py: Python<'_>,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<()> = match ttl {
                Some(s) => conn_method!(&mut conn, c, c.set_ex::<_, _, ()>(&key, value, s)),
                None => conn_method!(&mut conn, c, c.set::<_, _, ()>(&key, value)),
            };
            r.into_raw_result()
        })
    }

    // --- delete / adelete --------------------------------------------------

    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.del(&keys)));
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (*keys))]
    fn adelete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if keys.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn_method!(&mut conn, c, c.del(&keys));
            r.into_raw_result()
        })
    }

    // --- ping / aping ------------------------------------------------------

    fn ping(&self, py: Python<'_>) -> PyResult<bool> {
        let r: redis::RedisResult<String> = sync_op!(
            py,
            self,
            conn,
            dispatch_cmd!(&mut conn, redis::cmd("PING"))
        );
        match r {
            Ok(s) => Ok(s == "PONG"),
            Err(e) => Err(to_py_err(e)),
        }
    }

    fn aping(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<String> =
                dispatch_cmd!(&mut conn, redis::cmd("PING"));
            match r {
                Ok(s) => RawResult::Bool(s == "PONG"),
                Err(e) => crate::errors::classify(e),
            }
        })
    }
}
```

- [ ] **Step 4: Re-enable `RedisRsDriver` registration in `lib.rs`**

In `crates/redis-rs-py-driver/src/lib.rs`, uncomment `m.add_class::<driver::RedisRsDriver>()?;` (it should already be uncommented from Task 1's skeleton — verify).

- [ ] **Step 5: Build + run the tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/ tests/async_bridge/ -v`
Expected: all 25+ tests PASS. (Driver-basic 11; runtime 3; resolved 4; delayed 3; pending 3; errors 4; done-callbacks 2; cancel-in-callback 1.)

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/driver.rs crates/redis-rs-py-driver/src/lib.rs tests/conftest.py tests/driver/test_driver_basic.py
git commit -m "feat(driver): add RedisRsDriver with get/set/delete/ping (sync + async)"
```

---

## Task 10: `connect_standard` constructor coverage

The basic test in Task 9 covered the happy path; this task pins down the constructor surface — TLS opts plumb-through, cache opts plumb-through, bad URL raising `ConnectionError`, the URL getter reflecting the resp3 rewrite.

**Files:**
- Test: `tests/driver/test_connection_standard.py`

- [ ] **Step 1: Write the test**

```python
"""connect_standard constructor surface."""

import pytest


def test_url_is_resp3_rewritten(driver) -> None:
    assert "protocol=resp3" in driver.connection_url


def test_connect_standard_bad_url_raises_connection_error() -> None:
    from redis_rs_py._driver import RedisRsDriver

    with pytest.raises(ConnectionError):
        RedisRsDriver.connect_standard("redis://127.0.0.1:1/0")


def test_connect_standard_invalid_scheme_raises() -> None:
    from redis_rs_py._driver import RedisRsDriver

    with pytest.raises(ConnectionError):
        RedisRsDriver.connect_standard("not-a-url")


def test_connect_standard_with_cache_opts_does_not_raise(valkey_url: str) -> None:
    from redis_rs_py._driver import RedisRsDriver

    drv = RedisRsDriver.connect_standard(
        valkey_url, cache_max_size=100, cache_ttl_secs=60
    )
    drv.set("k", b"v")
    assert drv.get("k") == b"v"
    # Read back twice; cache should report at least one hit.
    drv.get("k")
    stats = drv.cache_statistics()
    assert stats is not None  # client-side caching is enabled
    hits, misses, _invalidates = stats
    assert hits + misses > 0


def test_connect_standard_without_cache_returns_no_stats(driver) -> None:
    # `driver` fixture connects without cache opts.
    assert driver.cache_statistics() is None
```

- [ ] **Step 2: Run the tests**

Run: `uv run pytest tests/driver/test_connection_standard.py -v`
Expected: 5 PASS. If `cache_statistics()` returns None even when cache is enabled, check that the resp3 rewrite is happening (cache requires RESP3) — `connection.connection_url` should contain `protocol=resp3`.

- [ ] **Step 3: Commit**

```bash
git add tests/driver/test_connection_standard.py
git commit -m "test(driver): cover connect_standard constructor surface"
```

---

## Task 11: Stub `python/redis_rs_py/_driver.pyi` for the new surface

Hand-maintained until `pyo3-stub-gen` becomes viable for our setup.

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Write the stub**

```python
"""Type stubs for the _driver Rust extension module.

Hand-maintained until pyo3-stub-gen becomes viable for our setup; new
commands (plans 03-09) extend this file as they land.
"""

from __future__ import annotations

from typing import Any, Awaitable

__version__: str

class RedisRsAwaitable:
    _asyncio_future_blocking: bool
    def __await__(self) -> RedisRsAwaitable: ...
    def __iter__(self) -> RedisRsAwaitable: ...
    def __next__(self) -> Any: ...
    def add_done_callback(
        self,
        fn_cb: Any,
        *,
        context: Any | None = ...,
    ) -> None: ...
    def cancel(self, msg: Any | None = ...) -> bool: ...
    def cancelled(self) -> bool: ...
    def done(self) -> bool: ...
    def result(self) -> Any: ...
    def exception(self) -> Any: ...

class RedisRsDriver:
    @staticmethod
    def connect_standard(
        url: str,
        *,
        cache_max_size: int | None = ...,
        cache_ttl_secs: int | None = ...,
        ssl_ca_certs: bytes | None = ...,
        ssl_certfile: bytes | None = ...,
        ssl_keyfile: bytes | None = ...,
    ) -> RedisRsDriver: ...
    @property
    def connection_url(self) -> str: ...
    def cache_statistics(self) -> tuple[int, int, int] | None: ...
    def get(self, key: str) -> bytes | None: ...
    def aget(self, key: str) -> Awaitable[bytes | None]: ...
    def set(self, key: str, value: bytes, ttl: int | None = ...) -> None: ...
    def aset(
        self, key: str, value: bytes, ttl: int | None = ...
    ) -> Awaitable[None]: ...
    def delete(self, *keys: str) -> int: ...
    def adelete(self, *keys: str) -> Awaitable[int]: ...
    def ping(self) -> bool: ...
    def aping(self) -> Awaitable[bool]: ...

# Internal test helpers — exported but underscore-prefixed.
def _test_resolved_bytes(b: bytes) -> RedisRsAwaitable: ...
def _test_resolved_none() -> RedisRsAwaitable: ...
def _test_resolved_int(n: int) -> RedisRsAwaitable: ...
def _test_delayed_bytes(b: bytes, delay_ms: int) -> RedisRsAwaitable: ...
def _test_pending() -> RedisRsAwaitable: ...
def _test_dropped() -> RedisRsAwaitable: ...
def _test_error(msg: str) -> RedisRsAwaitable: ...
def _test_server_error(msg: str) -> RedisRsAwaitable: ...
```

- [ ] **Step 2: Run ty check**

Run: `uv run ty check python/redis_rs_py/`
Expected: 0 errors. (If ty complains about `Awaitable[...]` import, switch to `from collections.abc import Awaitable`.)

- [ ] **Step 3: Commit**

```bash
git add python/redis_rs_py/_driver.pyi
git commit -m "feat(driver): add type stubs for foundation surface"
```

---

## Task 12: Update `python/redis_rs_py/__init__.py` re-exports

The package public API stays minimal until plan 10 adds the façade. For now we expose `RedisRsDriver` so users (and downstream plan tests) can `from redis_rs_py import RedisRsDriver`.

**Files:**
- Modify: `python/redis_rs_py/__init__.py`

- [ ] **Step 1: Edit `__init__.py`**

```python
"""redis-rs-py — high-performance, drop-in replacement for redis-py.

The public surface is added by plan 10 (sync facade) and plan 11
(asyncio facade). For now, only the low-level driver is exposed.
"""

from redis_rs_py._driver import RedisRsAwaitable, RedisRsDriver, __version__

__all__ = ["RedisRsAwaitable", "RedisRsDriver", "__version__"]
```

- [ ] **Step 2: Smoke-test the re-exports**

Run: `uv run python -c "from redis_rs_py import RedisRsDriver, RedisRsAwaitable, __version__; print(__version__)"`
Expected: prints the version string from `Cargo.toml` (`0.1.0-alpha.1` or whatever it is now).

- [ ] **Step 3: Update `tests/test_smoke.py` to also assert the new exports**

Replace `tests/test_smoke.py`:

```python
import redis_rs_py
from redis_rs_py import _driver


def test_driver_module_imports() -> None:
    assert hasattr(_driver, "__version__")


def test_package_exports_version() -> None:
    assert isinstance(redis_rs_py.__version__, str)


def test_package_exports_driver_class() -> None:
    assert hasattr(redis_rs_py, "RedisRsDriver")


def test_package_exports_awaitable_class() -> None:
    assert hasattr(redis_rs_py, "RedisRsAwaitable")
```

Run: `uv run pytest tests/test_smoke.py -v`
Expected: 4 PASS.

- [ ] **Step 4: Commit**

```bash
git add python/redis_rs_py/__init__.py tests/test_smoke.py
git commit -m "feat(public): re-export RedisRsDriver and RedisRsAwaitable"
```

---

## Task 13: Free-threaded smoke run + final lint pass

Verify nothing in the new code regresses the cp314t free-threaded build, and the project passes ruff/ty/cargo-fmt/clippy.

**Files:** none modified — verification only.

- [ ] **Step 1: Run the linters**

```bash
uv run ruff check
uv run ruff format --check
uv run ty check python/redis_rs_py/
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: all five succeed with no output (or only "X files reformatted").

- [ ] **Step 2: Run the full test suite under `python3.14`**

```bash
uv run pytest -n auto
```

Expected: every test PASSES. The fixture spins up one Valkey container per session.

- [ ] **Step 3: Run the suite under `python3.14t` (free-threaded)**

```bash
uv venv --python 3.14t .venv-ft
.venv-ft/bin/uv sync --group dev
.venv-ft/bin/uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
.venv-ft/bin/uv run pytest -n auto
```

Expected: same green. If anything fails only under free-threaded, the most likely culprit is a `Mutex` reentry under multi-thread access — investigate before patching.

- [ ] **Step 4: Commit a CHANGELOG entry**

Create `CHANGELOG.md` (it doesn't exist yet):

```markdown
# Changelog

All notable changes to redis-rs-py will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Tokio runtime singleton with PID-checked fork-safe rebuild (lifted from `django-cachex-redis-rs` / `django-vcache`).
- `RedisRsAwaitable` — custom asyncio bridge with 5-poll busy-yield + callback-mode fallback, full done-callback + cancellation support.
- `RedisRsDriver.connect_standard(url, **opts)` factory with cache + TLS plumb-through.
- Canonical commands `get`/`aget`/`set`/`aset`/`delete`/`adelete`/`ping`/`aping` proving the end-to-end pipeline.
- Eight `_test_*` awaitable helpers for unit-testing the bridge in isolation.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 01 entry"
```

- [ ] **Step 5: Final verification**

```bash
git log --oneline -20
```

Expected: 13 new commits since the start of the plan, in roughly the order of the tasks. Every commit message follows conventional commits.

---

## Self-review checklist for this plan

- [x] Spec coverage (`PLAN.md`): Architecture section — `RedisRsDriver` low-level driver ✓, single tokio multi-thread runtime ✓, custom awaitable bridge ✓, `RedisRsAwaitable.cancel()` honors `task.cancel()` ✓, fork safety (PID-checked registry) ✓.
- [x] Spec coverage: Risks — "asyncio cancellation" risk mitigated by Task 6 explicit cancel-in-callback test; "Fork safety" risk mitigated by Task 2 `test_runtime_survives_fork`.
- [x] Out-of-scope items deferred to their respective plans: full exception hierarchy → 02; commands beyond the 4 canonical → 03–09; high-level façade → 10/11.
- [x] No placeholder text in any task body.
- [x] Type consistency: `RedisRsDriver.connect_standard` signature in driver.rs matches the `.pyi` stub matches the test usage.
- [x] All file paths absolute or repo-relative-from-root, never "above" or "the file we just edited".
- [x] Every code-changing step ships the actual code.
- [x] Every test step has a runnable command and an explicit pass/fail expectation.
- [x] Frequent commits — 13 across 13 tasks, each independently revertable.
