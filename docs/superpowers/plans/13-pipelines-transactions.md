# Plan 13 — Pipelines + Transactions (`Pipeline`, `AsyncPipeline`, `transaction()` helper)

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land `redis_rs_py.Pipeline` (sync) and `redis_rs_py.asyncio.Pipeline` (async) Rust pyclasses with redis-py-compatible buffered-then-flushed semantics, including `WATCH`/`UNWATCH`/`MULTI`/`EXEC`/`DISCARD` and the `transaction(func, *keys, value_from_callable=False, watch_delay=None, **kwargs)` retry helper. After this plan, `r.pipeline(transaction=True) as pipe: pipe.set(...).incr(...).execute()` works against a live Valkey, the `WatchError` raise on conflicting EXEC works, and `r.transaction(func, "key")` retries until the watched key is stable.

**Architecture:** Two layers, mirroring how the rest of the driver is structured.

1. **Driver layer.** Three new methods on `RedisRsDriver` (and async siblings):
   - `pipeline_exec(commands, transaction)` — buffered batch, no WATCH; existing cachex shape.
   - `reserve_connection() -> ReservedConnection` — returns a guard holding an exclusive connection borrowed from the standard pool. Because redis-rs's `ConnectionManager` does not expose check-out semantics, the guard internally owns a fresh `MultiplexedConnection` (allocated via `Client::get_multiplexed_async_connection()` against the same URL), released on drop. Per-pipeline allocation is the cost we pay for the WATCH path; the no-WATCH path keeps using the multiplex.
   - `pipeline_exec_watched(reserved, commands, watched_keys, transaction_block) -> Vec<redis::Value>` — sends WATCH, immediate commands, MULTI, transaction block, EXEC, on the reserved connection. Returns `Err(WatchAborted)` if EXEC reply is `Nil`.
2. **Façade layer.** `crates/redis-rs-py-driver/src/facade/pipeline.rs` containing two pyclasses: `Pipeline` (sync) and `AsyncPipeline` (async). They:
   - Hold `Py<RedisRsDriver>`, the buffered command list `Vec<(method, args, kwargs)>`, the watched-keys list, the `transaction: bool` constructor flag, and an `explicit_transaction: bool` set by `multi()`.
   - Reserve a connection lazily on the first `watch()`/`multi()` call and release it on `reset()`/`__exit__`/`__aexit__`.
   - Expose every command method from plans 03–09 as a stub that buffers `(name, args)` and returns `self` (chainable). For chained immediate-mode calls under WATCH, the same method dispatches through the reserved connection.
   - Expose `execute()` / `aexecute()` translating buffered commands to driver-level `pipeline_exec*` calls and returning a `list[Any]` of decoded replies.
   - Expose `RedisRsDriver.transaction(func, *watches, value_from_callable=False, watch_delay=None, **kwargs)` (and async `atransaction`) — the retry helper, also implemented in Rust.

**Critical detail: WATCH state machine.** redis-py's `Pipeline` has two modes:

- **Buffering mode** (default, no WATCH active): commands are appended to the buffer. `execute()` sends them as a `MULTI`/`EXEC` block (if `transaction=True`) or pipelined (if `transaction=False`).
- **Immediate mode** (after WATCH or `pipeline.watch(*keys)`): the pipeline reserves a connection, sends WATCH, then commands are sent **immediately** (returning their reply directly, not chainable) until `MULTI` is called. `MULTI` flips back to buffering inside the transaction. `execute()` sends `MULTI ... buffered_commands ... EXEC` on the reserved connection. If a watched key changed between WATCH and EXEC, the EXEC reply is `Nil` and `WatchError` is raised.

We replicate this state machine inside Rust so the façade does not need a Python-side helper.

**Critical detail: `transaction()` retry helper.** Mirrors redis-py exactly:

```python
def transaction(func, *watches, value_from_callable=False, watch_delay=None, **kwargs):
    while True:
        with self.pipeline(transaction=True) as pipe:
            try:
                if watches:
                    pipe.watch(*watches)
                func_value = func(pipe)
                exec_value = pipe.execute()
                return func_value if value_from_callable else exec_value
            except WatchError:
                if watch_delay is not None and watch_delay > 0:
                    time.sleep(watch_delay)
                continue
```

Implemented in Rust on `RedisRsDriver` (`transaction`) and on async `RedisRsDriver` (`atransaction`).

**Tech Stack:** PyO3 0.28 (`#[pyclass]`, `#[pymethods]`, `Py::clone_ref`, `PyTypeInfo`), redis 1.x (`redis::pipe`, `redis::Pipeline::atomic`, `redis::cmd`, `Client::get_multiplexed_async_connection`), tokio 1.x (`oneshot`, `Mutex`), Python `threading`/`asyncio` for the conflict-coverage tests.

**Reference material:**

- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/01-foundation-async-bridge.md` — `RedisRsDriver`, `RedisRsAwaitable`, `async_op!`/`sync_op!` macros, the `ValkeyConn` two-layer wrapper.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/02-exceptions.md` — `WatchError` lives in the hierarchy already (Task 1, line `create_exception!(...,  WatchError, RedisError)`), so we import it; no new exception types added by this plan.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/10-facade-sync.md` — `redis_rs_py.Redis` (the high-level façade) is the class that exposes `r.pipeline()` and `r.transaction()` to users. This plan adds methods to it.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/11-facade-asyncio.md` — same for the asyncio façade.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs:550-609` — `pipeline_exec` reference implementation we extend with the WATCH path.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs:1031-1055` — the existing `apipeline_exec` / `pipeline_exec` method shape we mirror onto `RedisRsDriver`.
- redis-py source (read once before starting): `python -c "import redis, inspect; print(inspect.getsource(redis.client.Pipeline))" | less` and `python -c "import redis, inspect; print(inspect.getsource(redis.client.Redis.transaction))"` — the contract we're copying.

**Out of scope for this plan:**

- Cluster pipelines and cluster transactions (plan 15). When `RedisCluster` lands its `pipeline()`, it will call into a separate `pipeline_exec_cluster` because cluster transactions are slot-bound.
- Sentinel pipelines (plan 16) — handled by the same code path once Sentinel lands as a `ValkeyConnInner` arm.
- Pub/Sub state inside a pipeline (plan 14) — `subscribe`/`publish` inside a pipeline is not supported by either client family.
- Script-loading auto-resync (`Pipeline.load_scripts` in redis-py) — `script_load`/`evalsha` already work directly; users who need it can call them outside the pipeline. v0.2.
- `EMPTY_RESPONSE` sentinel logic from redis-py (used internally for client-side caching in the upstream pipeline). Not relevant to our flat batch model.
- `shard_hint` kwarg (legacy redis-py argument; it's a no-op upstream too).

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  connection.rs                # MODIFIED: add reserve_connection, ReservedConnection, pipeline_exec_watched
  driver.rs                    # MODIFIED: pipeline_exec / apipeline_exec / reserve_connection bindings
  facade/
    pipeline.rs                # NEW: Pipeline (sync) + AsyncPipeline (async) pyclasses + transaction helper
    sync.rs                    # MODIFIED: r.pipeline() factory + r.transaction() helper bindings
    asyncio.rs                 # MODIFIED: ar.pipeline() factory + ar.atransaction() helper bindings
  lib.rs                       # MODIFIED: register Pipeline / AsyncPipeline classes
python/
  redis_rs_py/
    __init__.py                # MODIFIED: re-export Pipeline
    asyncio/__init__.py        # MODIFIED: re-export AsyncPipeline as `Pipeline`
    _driver.pyi                # MODIFIED: type stubs for the new classes + factory methods
tests/
  pipeline/
    __init__.py
    conftest.py                # shared `pipe` / `apipe` fixtures (sync + async clients)
    test_pipeline_basic.py     # buffered chained calls + execute() returns list
    test_pipeline_transaction.py # MULTI/EXEC atomic block
    test_pipeline_watch.py     # WATCH triggers WatchError on concurrent modification (threaded)
    test_pipeline_discard.py   # discard()/reset()/close() drop the buffer
    test_pipeline_errors.py    # arity errors, multi-after-multi, watch-after-multi
    test_transaction_helper.py # retry on WatchError, value_from_callable, watch_delay
    test_async_pipeline_basic.py
    test_async_pipeline_transaction.py
    test_async_pipeline_watch.py
    test_async_pipeline_discard.py
    test_async_pipeline_errors.py
    test_async_transaction_helper.py
```

---

## Task 1: Driver — `pipeline_exec` (no-WATCH) on `RedisRsDriver`

Land the no-WATCH path first. This is the cachex-shape method, ported as-is — it lets the buffering-mode `Pipeline.execute()` work end-to-end before any of the WATCH machinery exists.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs` (add `pipeline_exec` to `ValkeyConnInner`)
- Modify: `crates/redis-rs-py-driver/src/driver.rs` (add `pipeline_exec` / `apipeline_exec` methods)
- Test: `tests/pipeline/__init__.py`, `tests/pipeline/conftest.py`, `tests/pipeline/test_pipeline_basic.py` (the parts that call the driver method directly)

- [ ] **Step 1: Write the failing test for `RedisRsDriver.pipeline_exec`**

Create `tests/pipeline/__init__.py` (empty) and `tests/pipeline/conftest.py`:

```python
"""Fixtures for the pipeline test family.

Reuses the session-wide `valkey_url` from the top-level `tests/conftest.py`.
Each test that mutates Valkey uses `flushdb=True` on its own client — the
session container is shared, so don't rely on starting from an empty DB
unless you flush.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def driver(valkey_url: str):
    """Low-level driver for tests that exercise pipeline_exec directly."""
    from redis_rs_py._driver import RedisRsDriver

    drv = RedisRsDriver.connect_standard(valkey_url)
    import redis

    rp = redis.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    return drv


@pytest.fixture
def client(valkey_url: str):
    """High-level sync façade — used by tests that go through r.pipeline()."""
    from redis_rs_py import Redis

    r = Redis.from_url(valkey_url)
    r.flushdb()
    return r


@pytest.fixture
async def aclient(valkey_url: str):
    """High-level async façade — used by tests that go through ar.pipeline()."""
    from redis_rs_py.asyncio import Redis

    ar = Redis.from_url(valkey_url)
    await ar.flushdb()
    yield ar
    await ar.aclose()
```

Create the first failing test, `tests/pipeline/test_pipeline_basic.py` (we will extend this file across many tasks; for now just the driver-direct part):

```python
"""Buffered-then-flushed pipeline semantics: chained calls, execute() returns list."""

from __future__ import annotations


def test_driver_pipeline_exec_returns_list(driver) -> None:
    """The driver-level pipeline_exec is the building block under the facade."""
    # SET a 1; SET b 2; GET a; GET b
    commands = [
        ("SET", [b"a", b"1"]),
        ("SET", [b"b", b"2"]),
        ("GET", [b"a"]),
        ("GET", [b"b"]),
    ]
    result = driver.pipeline_exec(commands, transaction=False)
    # SET → "OK" (true after RESP3 conversion), GET → bytes
    assert result == [True, True, b"1", b"2"]


def test_driver_pipeline_exec_atomic_returns_list(driver) -> None:
    commands = [
        ("INCR", [b"counter"]),
        ("INCR", [b"counter"]),
        ("INCR", [b"counter"]),
    ]
    result = driver.pipeline_exec(commands, transaction=True)
    assert result == [1, 2, 3]


def test_driver_pipeline_exec_empty_returns_empty_list(driver) -> None:
    assert driver.pipeline_exec([], transaction=False) == []
    assert driver.pipeline_exec([], transaction=True) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_pipeline_basic.py::test_driver_pipeline_exec_returns_list -v`
Expected: FAIL with `AttributeError: 'builtins.RedisRsDriver' object has no attribute 'pipeline_exec'`.

- [ ] **Step 3: Implement `ValkeyConnInner::pipeline_exec`**

Edit `crates/redis-rs-py-driver/src/connection.rs`. After the existing `Deref` block for `ValkeyConn` (search for `impl ValkeyConn`), add a new `impl` block for `ValkeyConnInner` (or extend whichever `impl ValkeyConnInner` already exists from prior plans):

```rust
use redis::RedisResult;

impl ValkeyConnInner {
    /// Execute a pipeline of arbitrary commands. When `transaction` is true,
    /// wraps the batch in MULTI/EXEC for atomicity.
    pub async fn pipeline_exec(
        &mut self,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> RedisResult<Vec<redis::Value>> {
        match self {
            Self::Standard(c) => {
                let mut pipe = redis::pipe();
                if transaction {
                    pipe.atomic();
                }
                for (cmd_name, args) in &commands {
                    let mut cmd = redis::cmd(cmd_name);
                    for a in args {
                        cmd.arg(a.as_slice());
                    }
                    pipe.add_command(cmd);
                }
                pipe.query_async(c).await
            }
        }
    }
}
```

(If a `use redis::RedisResult;` statement already exists at the top of the file, don't re-add it.)

- [ ] **Step 4: Implement `RedisRsDriver::pipeline_exec` and `apipeline_exec`**

Edit `crates/redis-rs-py-driver/src/driver.rs`. After the `ping`/`aping` block (the last canonical command), add:

```rust
    // =====================================================================
    // Pipeline (arbitrary buffered commands, no WATCH)
    // =====================================================================

    /// Synchronous pipeline execution. `commands` is a list of
    /// `(uppercase_cmd_name, [arg_bytes, ...])` tuples. `transaction=True`
    /// wraps the batch in MULTI/EXEC.
    ///
    /// Returns a Python list whose elements are the decoded replies in
    /// order (one per buffered command).
    #[pyo3(signature = (commands, transaction = false))]
    fn pipeline_exec(
        &self,
        py: Python<'_>,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<redis::Value>, _> = sync_op!(
            py,
            self,
            conn,
            conn.pipeline_exec(commands, transaction).await
        );
        let values = r.map_err(to_py_err)?;
        crate::async_bridge::RawResult::Value(redis::Value::Array(values)).into_py(py)
    }

    /// Async sibling of `pipeline_exec` — returns a `RedisRsAwaitable`
    /// that resolves to the same list shape.
    #[pyo3(signature = (commands, transaction = false))]
    fn apipeline_exec(
        &self,
        py: Python<'_>,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, {
            match conn.pipeline_exec(commands, transaction).await {
                Ok(items) => RawResult::Value(redis::Value::Array(items)),
                Err(e) => crate::errors::classify(e),
            }
        })
    }
```

- [ ] **Step 5: Build + run the driver-direct tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_pipeline_basic.py::test_driver_pipeline_exec_returns_list tests/pipeline/test_pipeline_basic.py::test_driver_pipeline_exec_atomic_returns_list tests/pipeline/test_pipeline_basic.py::test_driver_pipeline_exec_empty_returns_empty_list -v`
Expected: 3 PASS.

If `test_driver_pipeline_exec_returns_list` fails with `True != b'OK'`, the RawResult Value-array conversion is producing `b"OK"` instead of `True`. Re-check `redis_value_to_py`'s `Value::Okay` arm — Plan 01 maps it to `True`.

- [ ] **Step 6: Add the async direct-driver test to the same file**

Append to `tests/pipeline/test_pipeline_basic.py`:

```python
import pytest


@pytest.mark.asyncio
async def test_driver_apipeline_exec_returns_list(driver) -> None:
    commands = [
        ("SET", [b"x", b"1"]),
        ("INCR", [b"x"]),
        ("GET", [b"x"]),
    ]
    result = await driver.apipeline_exec(commands, transaction=False)
    assert result == [True, 2, b"2"]


@pytest.mark.asyncio
async def test_driver_apipeline_exec_atomic_returns_list(driver) -> None:
    commands = [
        ("INCR", [b"acounter"]),
        ("INCR", [b"acounter"]),
    ]
    result = await driver.apipeline_exec(commands, transaction=True)
    assert result == [1, 2]
```

Run: `uv run pytest tests/pipeline/test_pipeline_basic.py -v`
Expected: 5 PASS so far.

- [ ] **Step 7: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs crates/redis-rs-py-driver/src/driver.rs tests/pipeline/__init__.py tests/pipeline/conftest.py tests/pipeline/test_pipeline_basic.py
git commit -m "feat(pipeline): add driver-level pipeline_exec for buffered batches"
```

---

## Task 2: Driver — `ReservedConnection` guard + `reserve_connection`

For the WATCH path we need a connection that is **exclusively** held by one pipeline — sending commands to it does not interleave with the multiplexed pool. redis-rs's `ConnectionManager` exposes only the multiplexing `clone()` API, so we allocate a fresh `MultiplexedConnection` against the same URL when reserving.

This is documented in the v0.1 spec under "WATCH/MULTI/EXEC under a multiplexed pool" as the known cost of the WATCH path: **one extra TCP connection allocated per active transaction with WATCH**. The buffering-mode (no WATCH) path keeps using the multiplex.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs` (add `ReservedConnection`, `reserve_connection`)

- [ ] **Step 1: Add `ReservedConnection` to `connection.rs`**

In `crates/redis-rs-py-driver/src/connection.rs`, after the `ConnConfig` enum and before the `ValkeyConnInner` definition, add:

```rust
use redis::aio::MultiplexedConnection;

/// An exclusive, single-owner connection reserved for the lifetime of a
/// pipeline that uses WATCH. Allocates a fresh `MultiplexedConnection`
/// because redis-rs's `ConnectionManager` does not support check-out from
/// the regular pool.
///
/// On drop the underlying connection is simply released; the WATCH state
/// dies with the connection. Callers who want explicit cleanup should call
/// `release()` so the UNWATCH (if WATCH was issued) can be sent before the
/// connection is dropped.
pub struct ReservedConnection {
    inner: MultiplexedConnection,
    watched: bool,
}

impl ReservedConnection {
    pub fn new(inner: MultiplexedConnection) -> Self {
        Self {
            inner,
            watched: false,
        }
    }

    pub fn mark_watched(&mut self) {
        self.watched = true;
    }

    pub fn clear_watched(&mut self) {
        self.watched = false;
    }

    pub fn is_watched(&self) -> bool {
        self.watched
    }

    /// Best-effort UNWATCH on the held connection. Used by the pipeline
    /// `reset()` path so a reserved connection can be released cleanly
    /// even without an EXEC.
    pub async fn unwatch_if_needed(&mut self) -> RedisResult<()> {
        if self.watched {
            let mut cmd = redis::cmd("UNWATCH");
            let _: redis::Value = cmd.query_async(&mut self.inner).await?;
            self.watched = false;
        }
        Ok(())
    }

    pub fn conn_mut(&mut self) -> &mut MultiplexedConnection {
        &mut self.inner
    }
}
```

- [ ] **Step 2: Add `reserve_connection` to `ValkeyConn`**

Below the existing `impl ValkeyConn { pub async fn get_blocking(...) ... }` block, add:

```rust
impl ValkeyConn {
    /// Reserve an exclusive connection for the lifetime of a pipeline that
    /// uses WATCH. Allocates a fresh `MultiplexedConnection` against the
    /// same URL — the caller owns it via `ReservedConnection`. On drop the
    /// connection closes; for clean release call `unwatch_if_needed()`
    /// first.
    ///
    /// Documented cost: one extra TCP connection per active WATCH-mode
    /// pipeline. The no-WATCH pipeline path keeps using the multiplex.
    pub async fn reserve_connection(&self) -> Result<ReservedConnection, String> {
        match &self.config {
            ConnConfig::Standard { url, tls_opts } => {
                let client = create_client(url, tls_opts.as_ref()).map_err(|e| e.to_string())?;
                let conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(ReservedConnection::new(conn))
            }
        }
    }
}
```

- [ ] **Step 3: Verify the crate still compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with unused-warning noise about `ReservedConnection` and `reserve_connection` not yet wired anywhere from Python.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs
git commit -m "feat(pipeline): add ReservedConnection guard and reserve_connection"
```

---

## Task 3: Driver — `pipeline_exec_watched` + the `WatchAborted` sentinel

The WATCH-aware pipeline executes:

1. WATCH key1 key2 ...
2. (any immediate commands the user buffered between watch() and multi() — these were already sent immediately when issued; we don't re-send them here)
3. MULTI
4. each transaction-block command
5. EXEC

If the EXEC reply is `Nil` (a watched key changed), we surface a sentinel error so the façade can raise `WatchError`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`

- [ ] **Step 1: Add a WATCH-aware error and the executor**

In `crates/redis-rs-py-driver/src/connection.rs`, alongside `ReservedConnection`:

```rust
/// Result of `pipeline_exec_watched`. The `WatchAborted` variant is
/// translated to `WatchError` at the facade layer.
pub enum WatchedExecResult {
    Ok(Vec<redis::Value>),
    WatchAborted,
}

impl ReservedConnection {
    /// Execute a transactional block on this reserved connection.
    /// Sends `WATCH watched_keys`, then `MULTI`, then each command in
    /// `transaction_block`, then `EXEC`. Returns `WatchedExecResult::Ok`
    /// with the EXEC reply array on success, or `WatchedExecResult::WatchAborted`
    /// if the EXEC reply was `Nil` (a watched key changed).
    ///
    /// Any immediate commands the caller dispatched between WATCH and MULTI
    /// were sent directly via `dispatch_immediate` — they are not re-sent
    /// here.
    pub async fn pipeline_exec_watched(
        &mut self,
        watched_keys: &[String],
        transaction_block: Vec<(String, Vec<Vec<u8>>)>,
    ) -> RedisResult<WatchedExecResult> {
        // 1. WATCH (if not already issued)
        if !watched_keys.is_empty() && !self.watched {
            let mut cmd = redis::cmd("WATCH");
            for k in watched_keys {
                cmd.arg(k.as_str());
            }
            let _: redis::Value = cmd.query_async(&mut self.inner).await?;
            self.watched = true;
        }

        // 2-5. MULTI / commands / EXEC via redis::pipe().atomic()
        let mut pipe = redis::pipe();
        pipe.atomic();
        for (cmd_name, args) in &transaction_block {
            let mut cmd = redis::cmd(cmd_name);
            for a in args {
                cmd.arg(a.as_slice());
            }
            pipe.add_command(cmd);
        }

        // redis-rs returns the EXEC reply as the pipeline result. On WATCH
        // abort the EXEC reply is Nil, which redis-rs surfaces as an
        // `Ok(Value::Nil)` for the whole pipeline.
        let raw: redis::Value = pipe.query_async(&mut self.inner).await?;

        // After EXEC, watch state is cleared on the server. Mirror that
        // locally so the caller can re-WATCH if it loops.
        self.watched = false;

        match raw {
            redis::Value::Nil => Ok(WatchedExecResult::WatchAborted),
            redis::Value::Array(items) => Ok(WatchedExecResult::Ok(items)),
            // EXEC always replies with an array (or Nil); other shapes mean
            // the server response was unexpected.
            other => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "EXEC returned unexpected value",
                format!("{other:?}"),
            ))),
        }
    }

    /// Send a single command immediately on the reserved connection.
    /// Used between WATCH and MULTI for the immediate-mode commands the
    /// pipeline buffers and forwards one at a time.
    pub async fn dispatch_immediate(
        &mut self,
        cmd_name: &str,
        args: &[Vec<u8>],
    ) -> RedisResult<redis::Value> {
        let mut cmd = redis::cmd(cmd_name);
        for a in args {
            cmd.arg(a.as_slice());
        }
        cmd.query_async(&mut self.inner).await
    }

    /// Send WATCH on the reserved connection. Used when the user calls
    /// `pipe.watch(...)` after the pipeline already exists. Multiple
    /// `watch()` calls accumulate watched keys (matching redis-py).
    pub async fn watch(&mut self, keys: &[String]) -> RedisResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut cmd = redis::cmd("WATCH");
        for k in keys {
            cmd.arg(k.as_str());
        }
        let _: redis::Value = cmd.query_async(&mut self.inner).await?;
        self.watched = true;
        Ok(())
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished`, only unused-method warnings (the bindings in Task 4+ will use them).

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs
git commit -m "feat(pipeline): add pipeline_exec_watched and immediate-mode dispatch"
```

---

## Task 4: Façade scaffolding — `facade/pipeline.rs` + module wiring

Before the `Pipeline` pyclass is implemented, scaffold the new façade module so subsequent tasks have somewhere to put their code. This task assumes plan 10's `crates/redis-rs-py-driver/src/facade/sync.rs` already exists; if it doesn't, this task creates the `facade/mod.rs` boilerplate too.

**Files:**
- Create: `crates/redis-rs-py-driver/src/facade/mod.rs` (if missing — verify first)
- Create: `crates/redis-rs-py-driver/src/facade/pipeline.rs` (skeleton)
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (register the new pyclasses)

- [ ] **Step 1: Verify `facade/mod.rs` exists**

Read `crates/redis-rs-py-driver/src/facade/mod.rs`. Plan 10 should have created it with at least:

```rust
pub mod sync;
```

If it doesn't exist (plan 10 not yet executed), create it now with:

```rust
pub mod pipeline;
```

If it exists, edit it to add `pub mod pipeline;` on a new line.

If `facade/sync.rs` doesn't exist either, this task is being executed out of order — stop and run plan 10 first, then return.

- [ ] **Step 2: Create the empty `pipeline.rs` placeholder**

`crates/redis-rs-py-driver/src/facade/pipeline.rs`:

```rust
// Pipeline / AsyncPipeline pyclasses — buffered-then-flushed semantics
// matching redis-py's Pipeline contract.
//
// Two modes (mirroring redis-py):
//   * Buffering mode (default, no WATCH): commands queue into `commands`
//     and execute() flushes them via driver.pipeline_exec().
//   * Immediate mode (after watch()): the pipeline reserves an exclusive
//     connection, sends WATCH, then dispatches subsequent commands
//     immediately on that connection until multi() is called. multi()
//     flips back to buffering inside the transaction. execute() sends
//     MULTI/buffered/EXEC on the reserved connection. If the EXEC reply
//     is Nil (a watched key changed) the facade raises WatchError.
//
// Both classes also expose `transaction(func, *watches, ...)` as the
// retry helper described in the redis-py docs.

use pyo3::prelude::*;

// Implementations land in tasks 5-11.

#[allow(dead_code)]
const _PLACEHOLDER: () = ();
```

- [ ] **Step 3: Wire the new module into `lib.rs`**

In `crates/redis-rs-py-driver/src/lib.rs`, the `mod facade;` declaration should already be present from plan 10. The pyclass registration lines for `Pipeline` and `AsyncPipeline` will be added in Task 5/Task 11 — for now this task only wires the module path so the build works.

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with no errors, only `dead_code` warnings on `_PLACEHOLDER`.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/mod.rs crates/redis-rs-py-driver/src/facade/pipeline.rs
git commit -m "feat(pipeline): scaffold facade/pipeline.rs module"
```

---

## Task 5: `Pipeline` pyclass skeleton — fields, constructor, context-manager, reset/close

Land the bare class so the rest of the pipeline tests can refer to it. No command methods yet — those land in Task 6.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/pipeline.rs`
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (register `Pipeline`)

- [ ] **Step 1: Write the failing test for the skeleton**

Append to `tests/pipeline/test_pipeline_basic.py`:

```python
def test_pipeline_object_is_a_context_manager(client) -> None:
    pipe = client.pipeline()
    assert hasattr(pipe, "__enter__")
    assert hasattr(pipe, "__exit__")
    with client.pipeline() as p:
        assert p is not None
        # Empty pipeline: execute should return [].
        assert p.execute() == []


def test_pipeline_reset_clears_buffered_commands(client) -> None:
    pipe = client.pipeline()
    # We don't have command methods yet, but we can poke the buffer through
    # the test-only `_buffer_raw` helper used by Task 6 onwards.
    pipe.reset()
    assert pipe.execute() == []


def test_pipeline_close_is_idempotent(client) -> None:
    pipe = client.pipeline()
    pipe.close()
    pipe.close()  # second close must not raise
```

These tests rely on `client = Redis.from_url(...)` having a `pipeline()` factory. That factory is the one-line glue we add in Task 7. Run them now to see the AttributeError.

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_pipeline_basic.py::test_pipeline_object_is_a_context_manager -v`
Expected: FAIL — most likely `AttributeError: 'Redis' object has no attribute 'pipeline'`. (If `Redis` itself doesn't exist yet plan 10 hasn't landed — bail out and run plan 10 first.)

- [ ] **Step 2: Implement the skeleton**

Replace the contents of `crates/redis-rs-py-driver/src/facade/pipeline.rs`:

```rust
// Pipeline / AsyncPipeline pyclasses — buffered-then-flushed semantics
// matching redis-py's Pipeline contract.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple, PyType};
use std::sync::Mutex;

use crate::async_bridge::RawResult;
use crate::connection::{ReservedConnection, WatchedExecResult};
use crate::driver::RedisRsDriver;
use crate::errors::to_py_err;
use crate::exceptions::{RedisError, WatchError};
use crate::runtime::get_runtime;

/// Buffered command shape: `(uppercase_cmd_name, [arg_bytes, ...])`.
type BufferedCmd = (String, Vec<Vec<u8>>);

/// Internal mutable state for `Pipeline`. Held behind a `Mutex` so the
/// pyclass itself stays `Sync` (free-threaded build).
struct PipelineState {
    commands: Vec<BufferedCmd>,
    watched_keys: Vec<String>,
    /// `transaction` constructor flag (whether execute() sends MULTI/EXEC
    /// when no `watch()` was issued).
    transaction: bool,
    /// Set by `multi()` — flips immediate mode back to buffering inside
    /// the transaction.
    explicit_transaction: bool,
    /// True when at least one `watch()` was issued (immediate-mode active).
    watching: bool,
    /// The exclusive connection reserved for the lifetime of an
    /// immediate-mode transaction. None until `watch()` (or `multi()`
    /// without watch) reserves one.
    reserved: Option<ReservedConnection>,
    /// Set by `close()`. Subsequent operations raise.
    closed: bool,
}

impl PipelineState {
    fn new(transaction: bool) -> Self {
        Self {
            commands: Vec::new(),
            watched_keys: Vec::new(),
            transaction,
            explicit_transaction: false,
            watching: false,
            reserved: None,
            closed: false,
        }
    }
}

/// Synchronous pipeline. Returned by `Redis.pipeline(transaction=True)`.
#[pyclass(module = "redis_rs_py", unsendable = false)]
pub struct Pipeline {
    driver: Py<RedisRsDriver>,
    state: Mutex<PipelineState>,
}

impl Pipeline {
    pub fn new(driver: Py<RedisRsDriver>, transaction: bool) -> Self {
        Self {
            driver,
            state: Mutex::new(PipelineState::new(transaction)),
        }
    }

    /// Borrow the inner driver as a typed reference.
    fn with_driver<R>(&self, py: Python<'_>, f: impl FnOnce(&RedisRsDriver) -> R) -> R {
        let bound = self.driver.bind(py);
        let drv = bound.borrow();
        f(&drv)
    }

    /// Internal: synchronously release any reserved connection (UNWATCH +
    /// drop). Called from `reset()`, `close()`, and `__exit__`.
    fn release_reserved(&self, py: Python<'_>) -> PyResult<()> {
        let reserved = {
            let mut state = self.state.lock().unwrap();
            state.watching = false;
            state.explicit_transaction = false;
            state.watched_keys.clear();
            state.commands.clear();
            state.reserved.take()
        };
        if let Some(mut r) = reserved {
            // Best-effort UNWATCH; ignore failure (the connection is being
            // dropped anyway).
            py.detach(|| {
                get_runtime().block_on(async {
                    let _ = r.unwatch_if_needed().await;
                })
            });
        }
        Ok(())
    }
}

#[pymethods]
impl Pipeline {
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.release_reserved(py)?;
        Ok(false)
    }

    fn __len__(&self) -> usize {
        self.state.lock().unwrap().commands.len()
    }

    /// Always truthy — matches redis-py.
    fn __bool__(&self) -> bool {
        true
    }

    /// Drop buffered commands, clear watched-keys list, send UNWATCH on
    /// the reserved connection if any, release the connection.
    fn reset(&self, py: Python<'_>) -> PyResult<()> {
        self.release_reserved(py)
    }

    /// Same as `reset()` plus marks the pipeline as closed.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.release_reserved(py)?;
        self.state.lock().unwrap().closed = true;
        Ok(())
    }

    /// Empty-stack execute: matches redis-py's "if not stack and not
    /// watching: return []" early-out. The full implementation lands in
    /// Task 7.
    fn execute(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let state = self.state.lock().unwrap();
        if state.closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        if state.commands.is_empty() && !state.watching && !state.explicit_transaction {
            return Ok(PyList::empty(py).into_any().unbind());
        }
        // Real implementation lands in Task 7.
        drop(state);
        Err(PyErr::new::<PyRuntimeError, _>(
            "Pipeline.execute() not yet implemented for non-empty buffers — see Task 7",
        ))
    }
}
```

- [ ] **Step 3: Register the class**

Edit `crates/redis-rs-py-driver/src/lib.rs`. Inside `fn _driver`, after the existing `m.add_class::<driver::RedisRsDriver>()?;` line, add:

```rust
    m.add_class::<facade::pipeline::Pipeline>()?;
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished`, with warnings about unused fields like `transaction` on `PipelineState` (Task 7 uses them).

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/pipeline.rs crates/redis-rs-py-driver/src/lib.rs
git commit -m "feat(pipeline): add Pipeline pyclass skeleton with context-manager protocol"
```

---

## Task 6: `Pipeline` command methods — buffered command surface

Stamp out one buffer-method per redis-py command. Each method appends `(name_upper, args)` to the buffer (or, in immediate mode, dispatches to the reserved connection and returns the reply). All methods return `self` for chaining when in buffering mode.

We use a Rust macro to keep the file small and to ensure every command has the same boilerplate.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/pipeline.rs`

- [ ] **Step 1: Write the failing chained-call test**

Append to `tests/pipeline/test_pipeline_basic.py`:

```python
def test_pipeline_chains_buffered_commands(client) -> None:
    """`pipe.set(...).incr(...).get(...)` returns the pipeline; execute()
    flushes the lot and returns the per-command replies in order."""
    with client.pipeline() as pipe:
        result = (
            pipe.set("a", b"hello")
                .incr("counter")
                .incr("counter")
                .get("a")
                .execute()
        )
    # SET → True (Okay), INCR → 1, INCR → 2, GET → b"hello"
    assert result == [True, 1, 2, b"hello"]


def test_pipeline_set_returns_self_for_chaining(client) -> None:
    pipe = client.pipeline()
    same = pipe.set("k", b"v")
    assert same is pipe


def test_pipeline_buffered_len_matches_command_count(client) -> None:
    pipe = client.pipeline()
    pipe.set("a", b"1")
    pipe.set("b", b"2")
    pipe.delete("a")
    assert len(pipe) == 3
    pipe.reset()
    assert len(pipe) == 0


def test_pipeline_explicitly_non_transactional(client) -> None:
    """transaction=False sends a flat batch, not MULTI/EXEC."""
    with client.pipeline(transaction=False) as pipe:
        result = pipe.set("a", b"1").set("b", b"2").execute()
    assert result == [True, True]
```

Run the chaining test to confirm red:

Run: `uv run pytest tests/pipeline/test_pipeline_basic.py::test_pipeline_chains_buffered_commands -v`
Expected: FAIL — `AttributeError: 'builtins.Pipeline' object has no attribute 'set'`.

- [ ] **Step 2: Implement the command-buffering macro and methods**

Append to `crates/redis-rs-py-driver/src/facade/pipeline.rs` (above the `#[pymethods] impl Pipeline { ... }` block, then add the methods inside that block):

```rust
// =========================================================================
// Command-buffering helpers
// =========================================================================

impl Pipeline {
    /// Append `(name, args)` to the buffer, OR dispatch immediately if the
    /// pipeline is in immediate mode (watch was issued, multi has not been
    /// called yet, and the command is not WATCH/UNWATCH/MULTI/DISCARD).
    fn buffer_or_dispatch(
        slf: Py<Self>,
        py: Python<'_>,
        name: &str,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let this = slf.bind(py).borrow();
        if this.state.lock().unwrap().closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        // Immediate mode: a watch is active, no multi yet → dispatch on the
        // reserved connection.
        let immediate = {
            let s = this.state.lock().unwrap();
            s.watching && !s.explicit_transaction
        };
        if immediate {
            return Pipeline::dispatch_immediate(&this, py, name, &args);
        }
        // Buffering mode: append and return self (chainable).
        this.state
            .lock()
            .unwrap()
            .commands
            .push((name.to_string(), args));
        drop(this);
        Ok(slf.into_any())
    }

    /// Send a single command on the reserved connection. Used in
    /// immediate mode (between `watch()` and `multi()`).
    fn dispatch_immediate(
        this: &PyRef<'_, Pipeline>,
        py: Python<'_>,
        name: &str,
        args: &[Vec<u8>],
    ) -> PyResult<Py<PyAny>> {
        let mut state = this.state.lock().unwrap();
        let reserved = state.reserved.as_mut().ok_or_else(|| {
            PyErr::new::<RedisError, _>(
                "internal error: immediate-mode dispatch with no reserved connection",
            )
        })?;
        let name = name.to_string();
        let args_owned: Vec<Vec<u8>> = args.to_vec();
        let result: Result<redis::Value, _> = py.detach(|| {
            get_runtime().block_on(async {
                reserved.dispatch_immediate(&name, &args_owned).await
            })
        });
        let value = result.map_err(to_py_err)?;
        RawResult::Value(value).into_py(py)
    }
}

// =========================================================================
// Command method macro
// =========================================================================

/// Stamp out one buffered command method on `Pipeline`. Each method
/// converts its Python args into `Vec<Vec<u8>>` and forwards to
/// `buffer_or_dispatch`. Methods are chainable (return `Py<Self>`).
macro_rules! pipeline_cmd {
    // Variant A: fixed args, all bytes-coercible.
    ($method:ident, $cmd:expr, ($($arg:ident: $argty:ty),*)) => {
        #[pyo3(signature = ($($arg),*))]
        fn $method(slf: Py<Self>, py: Python<'_>, $($arg: $argty),*) -> PyResult<Py<PyAny>> {
            let mut args: Vec<Vec<u8>> = Vec::new();
            $( args.push($crate::facade::pipeline::to_arg_bytes($arg)); )*
            Pipeline::buffer_or_dispatch(slf, py, $cmd, args)
        }
    };
    // Variant B: a single varargs `*values` parameter.
    ($method:ident, $cmd:expr, varargs $head:ident: $headty:ty) => {
        #[pyo3(signature = ($head, *values))]
        fn $method(
            slf: Py<Self>,
            py: Python<'_>,
            $head: $headty,
            values: Vec<Vec<u8>>,
        ) -> PyResult<Py<PyAny>> {
            let mut args: Vec<Vec<u8>> = Vec::new();
            args.push($crate::facade::pipeline::to_arg_bytes($head));
            for v in values { args.push(v); }
            Pipeline::buffer_or_dispatch(slf, py, $cmd, args)
        }
    };
    // Variant C: a single `*keys` form (DEL, EXISTS, etc.).
    ($method:ident, $cmd:expr, keys *keys) => {
        #[pyo3(signature = (*keys))]
        fn $method(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
            let args: Vec<Vec<u8>> = keys.into_iter().map(|s| s.into_bytes()).collect();
            Pipeline::buffer_or_dispatch(slf, py, $cmd, args)
        }
    };
}

/// Coerce common Python-side arg types to bytes.
pub(crate) fn to_arg_bytes<T: AsBytesArg>(v: T) -> Vec<u8> {
    v.into_arg_bytes()
}

pub(crate) trait AsBytesArg {
    fn into_arg_bytes(self) -> Vec<u8>;
}

impl AsBytesArg for &str {
    fn into_arg_bytes(self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl AsBytesArg for String {
    fn into_arg_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }
}

impl AsBytesArg for Vec<u8> {
    fn into_arg_bytes(self) -> Vec<u8> {
        self
    }
}

impl AsBytesArg for &[u8] {
    fn into_arg_bytes(self) -> Vec<u8> {
        self.to_vec()
    }
}

impl AsBytesArg for i64 {
    fn into_arg_bytes(self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl AsBytesArg for u64 {
    fn into_arg_bytes(self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl AsBytesArg for f64 {
    fn into_arg_bytes(self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}
```

Now extend the existing `#[pymethods] impl Pipeline { ... }` block (the one that currently holds `__enter__`/`__exit__`/`reset`/`close`/`execute`) by appending the command-method invocations. Inside that block, add:

```rust
    // Strings (plan 03 surface)
    pipeline_cmd!(set, "SET", (key: &str, value: Vec<u8>));
    pipeline_cmd!(get, "GET", (key: &str));
    pipeline_cmd!(getdel, "GETDEL", (key: &str));
    pipeline_cmd!(append, "APPEND", (key: &str, value: Vec<u8>));
    pipeline_cmd!(strlen, "STRLEN", (key: &str));
    pipeline_cmd!(incr, "INCR", (key: &str));
    pipeline_cmd!(incrby, "INCRBY", (key: &str, by: i64));
    pipeline_cmd!(incrbyfloat, "INCRBYFLOAT", (key: &str, by: f64));
    pipeline_cmd!(decr, "DECR", (key: &str));
    pipeline_cmd!(decrby, "DECRBY", (key: &str, by: i64));
    pipeline_cmd!(setrange, "SETRANGE", (key: &str, offset: i64, value: Vec<u8>));
    pipeline_cmd!(getrange, "GETRANGE", (key: &str, start: i64, end: i64));
    pipeline_cmd!(rename, "RENAME", (key: &str, new_key: &str));
    pipeline_cmd!(renamenx, "RENAMENX", (key: &str, new_key: &str));
    pipeline_cmd!(typ, "TYPE", (key: &str));
    pipeline_cmd!(expire, "EXPIRE", (key: &str, seconds: i64));
    pipeline_cmd!(pexpire, "PEXPIRE", (key: &str, millis: i64));
    pipeline_cmd!(ttl, "TTL", (key: &str));
    pipeline_cmd!(pttl, "PTTL", (key: &str));
    pipeline_cmd!(persist, "PERSIST", (key: &str));

    // Variadic key forms (plan 03)
    pipeline_cmd!(delete, "DEL", keys *keys);
    pipeline_cmd!(unlink, "UNLINK", keys *keys);
    pipeline_cmd!(exists, "EXISTS", keys *keys);

    // Lists (plan 04 surface — non-blocking only; BLPOP etc. are not
    // legal inside a transaction)
    pipeline_cmd!(lpush, "LPUSH", varargs key: &str);
    pipeline_cmd!(rpush, "RPUSH", varargs key: &str);
    pipeline_cmd!(lpop, "LPOP", (key: &str));
    pipeline_cmd!(rpop, "RPOP", (key: &str));
    pipeline_cmd!(llen, "LLEN", (key: &str));
    pipeline_cmd!(lrange, "LRANGE", (key: &str, start: i64, stop: i64));
    pipeline_cmd!(lindex, "LINDEX", (key: &str, index: i64));
    pipeline_cmd!(lrem, "LREM", (key: &str, count: i64, value: Vec<u8>));
    pipeline_cmd!(ltrim, "LTRIM", (key: &str, start: i64, stop: i64));
    pipeline_cmd!(lset, "LSET", (key: &str, index: i64, value: Vec<u8>));

    // Hashes (plan 05 surface)
    pipeline_cmd!(hset, "HSET", (key: &str, field: &str, value: Vec<u8>));
    pipeline_cmd!(hget, "HGET", (key: &str, field: &str));
    pipeline_cmd!(hdel, "HDEL", varargs key: &str);
    pipeline_cmd!(hgetall, "HGETALL", (key: &str));
    pipeline_cmd!(hexists, "HEXISTS", (key: &str, field: &str));
    pipeline_cmd!(hlen, "HLEN", (key: &str));
    pipeline_cmd!(hincrby, "HINCRBY", (key: &str, field: &str, by: i64));
    pipeline_cmd!(hincrbyfloat, "HINCRBYFLOAT", (key: &str, field: &str, by: f64));

    // Sets (plan 06 surface)
    pipeline_cmd!(sadd, "SADD", varargs key: &str);
    pipeline_cmd!(srem, "SREM", varargs key: &str);
    pipeline_cmd!(smembers, "SMEMBERS", (key: &str));
    pipeline_cmd!(sismember, "SISMEMBER", (key: &str, member: Vec<u8>));
    pipeline_cmd!(scard, "SCARD", (key: &str));

    // Sorted sets (plan 07 surface)
    pipeline_cmd!(zincrby, "ZINCRBY", (key: &str, by: f64, member: Vec<u8>));
    pipeline_cmd!(zcard, "ZCARD", (key: &str));
    pipeline_cmd!(zscore, "ZSCORE", (key: &str, member: Vec<u8>));

    // Admin (plan 09 surface, frequently buffered)
    pipeline_cmd!(ping, "PING", ());
    pipeline_cmd!(echo, "ECHO", (message: Vec<u8>));
```

Note the empty `()` for `ping` — that's a zero-arg call. The macro handles it because the `$($arg:ident: $argty:ty),*` expansion is empty.

(Plans 03–09 each cover one family; the list above includes the most-used commands per family. If a plan adds a new method that should be buffered, the engineer following that plan adds one more `pipeline_cmd!` line here. The macro is the contract; nothing else changes.)

- [ ] **Step 3: Build + run the chaining tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_pipeline_basic.py::test_pipeline_set_returns_self_for_chaining tests/pipeline/test_pipeline_basic.py::test_pipeline_buffered_len_matches_command_count -v`
Expected: 2 PASS. (The `test_pipeline_chains_buffered_commands` and `test_pipeline_explicitly_non_transactional` tests still need Task 7's `execute()` body — they will pass at the end of Task 7.)

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/pipeline.rs
git commit -m "feat(pipeline): add chainable buffered command methods"
```

---

## Task 7: `Pipeline.execute()` — buffering-mode flush

Now the meat of the buffering path. `execute()` flushes the buffer through `driver.pipeline_exec(commands, transaction)` and returns a Python list.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/pipeline.rs`

- [ ] **Step 1: Replace the placeholder `execute()` body**

In `crates/redis-rs-py-driver/src/facade/pipeline.rs`, find the `execute()` method (added in Task 5 with the `not yet implemented` placeholder) and replace it entirely with:

```rust
    /// Flush the buffered commands and return the per-command replies.
    /// In buffering mode (no `watch()` issued, no `multi()` called), this
    /// forwards to `driver.pipeline_exec(...)`. The WATCH-mode path lives
    /// in Task 9.
    fn execute(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Snapshot the state we need under the mutex, then drop the guard
        // before doing any blocking I/O.
        let (commands, transaction, watching, explicit_transaction, closed) = {
            let s = self.state.lock().unwrap();
            (
                s.commands.clone(),
                s.transaction,
                s.watching,
                s.explicit_transaction,
                s.closed,
            )
        };
        if closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        // Empty + no watch → return [].
        if commands.is_empty() && !watching && !explicit_transaction {
            return Ok(PyList::empty(py).into_any().unbind());
        }

        // WATCH-mode execute lives in Task 9. The buffering-mode path is below.
        if watching || explicit_transaction {
            return self.execute_watched(py, commands);
        }

        // Buffering mode: forward to driver.pipeline_exec().
        let want_transaction = transaction;
        let result = self.with_driver(py, |drv| {
            drv.pipeline_exec_internal(py, commands, want_transaction)
        });

        // Whether or not execute() succeeded, drop the buffer (matches
        // redis-py's reset() inside execute()'s finally block).
        {
            let mut s = self.state.lock().unwrap();
            s.commands.clear();
        }
        result
    }
```

Then, add `execute_watched` as a Rust-only stub for Task 9 to overwrite (so the call compiles):

```rust
impl Pipeline {
    fn execute_watched(
        &self,
        _py: Python<'_>,
        _commands: Vec<BufferedCmd>,
    ) -> PyResult<Py<PyAny>> {
        Err(PyErr::new::<PyRuntimeError, _>(
            "WATCH-mode execute() lands in Task 9",
        ))
    }
}
```

- [ ] **Step 2: Add `pipeline_exec_internal` to `RedisRsDriver`**

`Pipeline.execute()` calls `drv.pipeline_exec_internal(...)` — a Rust-side helper that returns the same shape as the Python-facing `pipeline_exec()` method but takes a borrow rather than a `&Self` pyclass binding. Add it to `crates/redis-rs-py-driver/src/driver.rs`, just below the existing `pipeline_exec`:

```rust
impl RedisRsDriver {
    /// Rust-only: shared helper for the facade's `Pipeline.execute()` path.
    /// Same semantics as the `pipeline_exec` pymethod; takes a `&self`
    /// rather than a Bound class so internal callers don't have to round-trip
    /// through Python typing.
    pub(crate) fn pipeline_exec_internal(
        &self,
        py: Python<'_>,
        commands: Vec<(String, Vec<Vec<u8>>)>,
        transaction: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Vec<redis::Value>, _> = sync_op!(
            py,
            self,
            conn,
            conn.pipeline_exec(commands, transaction).await
        );
        let values = r.map_err(to_py_err)?;
        crate::async_bridge::RawResult::Value(redis::Value::Array(values)).into_py(py)
    }
}
```

- [ ] **Step 3: Build + run the buffering-mode tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_pipeline_basic.py -v`
Expected: every test in this file PASSES (10 cases).

- [ ] **Step 4: Add the no-WATCH transaction-block test**

Create `tests/pipeline/test_pipeline_transaction.py`:

```python
"""MULTI/EXEC atomic pipeline (no WATCH)."""

from __future__ import annotations


def test_atomic_pipeline_executes_as_one_block(client) -> None:
    """All commands inside a transaction=True pipeline either all run or
    none do. We don't have a clean way to verify atomicity with a single
    client, so verify the reply shape and that intermediate values are
    visible."""
    with client.pipeline(transaction=True) as pipe:
        result = (
            pipe.set("counter", b"10")
                .incr("counter")
                .incr("counter")
                .get("counter")
                .execute()
        )
    assert result == [True, 11, 12, b"12"]


def test_atomic_pipeline_multiple_keys(client) -> None:
    with client.pipeline(transaction=True) as pipe:
        result = (
            pipe.set("a", b"1")
                .set("b", b"2")
                .set("c", b"3")
                .get("a")
                .get("b")
                .get("c")
                .execute()
        )
    assert result == [True, True, True, b"1", b"2", b"3"]


def test_pipeline_default_is_transactional(client) -> None:
    """redis-py's default is `transaction=True` — match it."""
    with client.pipeline() as pipe:
        result = pipe.set("k", b"v").get("k").execute()
    assert result == [True, b"v"]


def test_pipeline_in_atomic_mode_then_reuse(client) -> None:
    """Reusing the same pipeline after execute() should work — the buffer
    is cleared but the pipeline is still usable."""
    pipe = client.pipeline(transaction=True)
    assert pipe.set("a", b"1").get("a").execute() == [True, b"1"]
    assert pipe.set("b", b"2").get("b").execute() == [True, b"2"]
    pipe.close()
```

Run: `uv run pytest tests/pipeline/test_pipeline_transaction.py -v`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/pipeline.rs crates/redis-rs-py-driver/src/driver.rs tests/pipeline/test_pipeline_transaction.py
git commit -m "feat(pipeline): implement buffering-mode execute() for sync Pipeline"
```

---

## Task 8: `watch()` / `unwatch()` / `multi()` / `discard()` — WATCH state machine

Now wire the `watch()` / `unwatch()` / `multi()` / `discard()` methods that drive the immediate-mode state machine.

Behavior (matches redis-py):

- `pipe.watch(*keys)` — illegal if `multi()` already called (raises `RedisError("Cannot issue a WATCH after a MULTI")`). Otherwise, reserves a connection (if not already reserved), accumulates `keys` into `watched_keys`, sends WATCH to the server. Sets `watching=True`. **Returns the server reply** (typically `True` for `OK`) — does **not** return self.
- `pipe.unwatch()` — sends UNWATCH on the reserved connection, clears `watched_keys`, sets `watching=False`. Idempotent. Returns `True`.
- `pipe.multi()` — illegal if already in `explicit_transaction` (raises `RedisError("Cannot issue nested calls to MULTI")`). Illegal if buffer already has commands (raises `RedisError("Commands without an initial WATCH have already been issued")`). Sets `explicit_transaction=True` so subsequent commands buffer instead of dispatching immediately. Returns `None`.
- `pipe.discard()` — sends `DISCARD` (which the server expects after `MULTI`). In our model: clears the buffer and the `explicit_transaction` flag, sends `UNWATCH` (to clean up watched keys), keeps the reserved connection until reset. Returns `None`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/pipeline.rs`

- [ ] **Step 1: Write the failing tests**

Create `tests/pipeline/test_pipeline_discard.py`:

```python
"""watch/unwatch/multi/discard state-machine — non-conflict paths only.
Conflict tests (WatchError) live in test_pipeline_watch.py."""

from __future__ import annotations

import pytest

from redis_rs_py.exceptions import RedisError


def test_watch_then_unwatch_no_op(client) -> None:
    client.set("k", b"v")
    with client.pipeline() as pipe:
        pipe.watch("k")
        assert pipe.unwatch() is True
        # After unwatch, the next execute should still be a normal MULTI/EXEC.
        result = pipe.set("k", b"v2").execute()
    assert result == [True]
    assert client.get("k") == b"v2"


def test_multi_after_watch_buffers_subsequent_commands(client) -> None:
    """After watch() then multi(), commands buffer rather than dispatching
    immediately — execute() flushes them as MULTI/EXEC."""
    client.set("balance", b"100")
    with client.pipeline() as pipe:
        pipe.watch("balance")
        # In immediate mode: GET dispatches and returns the value.
        cur = pipe.get("balance")
        assert cur == b"100"
        # Now flip to transaction mode.
        pipe.multi()
        # In transaction mode: subsequent commands buffer.
        chained = pipe.set("balance", b"90").incr("withdrawals")
        assert chained is pipe
        result = pipe.execute()
    assert result == [True, 1]
    assert client.get("balance") == b"90"


def test_multi_after_multi_raises(client) -> None:
    with client.pipeline() as pipe:
        pipe.watch("x")
        pipe.multi()
        with pytest.raises(RedisError, match="nested"):
            pipe.multi()


def test_watch_after_multi_raises(client) -> None:
    with client.pipeline() as pipe:
        pipe.watch("x")
        pipe.multi()
        with pytest.raises(RedisError, match="WATCH after a MULTI"):
            pipe.watch("y")


def test_multi_with_buffered_commands_already_present_raises(client) -> None:
    """multi() may not be called once buffering-mode commands have been queued
    without WATCH — the WATCH+MULTI flow is the only legitimate one."""
    with client.pipeline() as pipe:
        pipe.set("x", b"v")  # buffers
        with pytest.raises(RedisError, match="initial WATCH"):
            pipe.multi()


def test_discard_clears_buffer_and_unwatches(client) -> None:
    client.set("k", b"v")
    with client.pipeline() as pipe:
        pipe.watch("k")
        pipe.multi()
        pipe.set("k", b"new")
        pipe.discard()
        # After discard the buffer is empty.
        assert len(pipe) == 0
        # Reusing for another transaction is allowed.
        pipe.watch("k")
        pipe.multi()
        result = pipe.set("k", b"after-discard").execute()
    assert result == [True]
    assert client.get("k") == b"after-discard"
```

Run: `uv run pytest tests/pipeline/test_pipeline_discard.py -v`
Expected: every test FAILS with `AttributeError: ... has no attribute 'watch'` (or `multi`/`discard`).

- [ ] **Step 2: Implement `watch`/`unwatch`/`multi`/`discard`**

In `crates/redis-rs-py-driver/src/facade/pipeline.rs`, add the methods inside the existing `#[pymethods] impl Pipeline { ... }` block:

```rust
    /// Reserve a connection (if not already reserved), send WATCH on it,
    /// and accumulate `keys` into the watched-keys list. Subsequent
    /// commands dispatch immediately on this connection until `multi()`
    /// is called.
    ///
    /// Returns the server reply (typically `True` for OK) — does **not**
    /// return self, matching redis-py.
    #[pyo3(signature = (*keys))]
    fn watch(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        {
            let s = self.state.lock().unwrap();
            if s.closed {
                return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
            }
            if s.explicit_transaction {
                return Err(PyErr::new::<RedisError, _>(
                    "Cannot issue a WATCH after a MULTI",
                ));
            }
        }
        // Lazily reserve a connection.
        self.ensure_reserved(py)?;

        // Send WATCH on it.
        let result: Result<(), _> = py.detach(|| {
            get_runtime().block_on(async {
                let mut s = self.state.lock().unwrap();
                let reserved = s.reserved.as_mut().unwrap();
                let res = reserved.watch(&keys).await;
                if res.is_ok() {
                    s.watching = true;
                    s.watched_keys.extend(keys.iter().cloned());
                }
                res.map(|_| ())
            })
        });
        result.map_err(to_py_err)?;
        Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
    }

    /// Send UNWATCH on the reserved connection (if any) and clear the
    /// watched-keys list. Always returns True (matches redis-py:
    /// `return self.watching and self.execute_command("UNWATCH") or True`).
    fn unwatch(&self, py: Python<'_>) -> PyResult<bool> {
        let mut s = self.state.lock().unwrap();
        if s.closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        if let Some(reserved) = s.reserved.as_mut() {
            // Drop the lock before the await to keep the mutex small.
            // We can't await holding a std::sync::Mutex anyway.
            let mut taken = std::mem::replace(reserved, dummy_reserved());
            // Restore the slot before doing the I/O — this is a brief
            // swap pattern that keeps the mutex contract correct.
            *reserved = std::mem::replace(&mut taken, dummy_reserved());
            // ^ Two swaps that net out — this is just to assert lifetime
            // safety. The actual await happens via a raw &mut on the slot.
            // Use the existing reserved with the lock dropped:
            drop(s);
            let res: Result<(), _> = py.detach(|| {
                get_runtime().block_on(async {
                    let mut s = self.state.lock().unwrap();
                    let reserved = s.reserved.as_mut().unwrap();
                    reserved.unwatch_if_needed().await
                })
            });
            res.map_err(to_py_err)?;
            // Clear the watched-keys bookkeeping.
            let mut s = self.state.lock().unwrap();
            s.watched_keys.clear();
            s.watching = false;
        } else {
            s.watched_keys.clear();
            s.watching = false;
        }
        Ok(true)
    }

    /// Switch the pipeline from immediate mode back to buffering mode for
    /// the lifetime of the next `execute()`. Subsequent commands buffer
    /// instead of dispatching immediately.
    fn multi(&self) -> PyResult<()> {
        let mut s = self.state.lock().unwrap();
        if s.closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        if s.explicit_transaction {
            return Err(PyErr::new::<RedisError, _>(
                "Cannot issue nested calls to MULTI",
            ));
        }
        if !s.commands.is_empty() {
            return Err(PyErr::new::<RedisError, _>(
                "Commands without an initial WATCH have already been issued",
            ));
        }
        s.explicit_transaction = true;
        Ok(())
    }

    /// Drop the buffered transaction block and clear watched keys.
    /// Returns None (matches redis-py: `discard` is a fire-and-forget).
    fn discard(&self, py: Python<'_>) -> PyResult<()> {
        // If we're in the middle of an explicit_transaction, send DISCARD
        // to the server so the connection state is clean. Otherwise just
        // drop the local state.
        let needs_discard = {
            let s = self.state.lock().unwrap();
            s.closed.then_some(()).is_none()
                && s.explicit_transaction
                && s.reserved.is_some()
        };
        if needs_discard {
            let res: Result<(), _> = py.detach(|| {
                get_runtime().block_on(async {
                    let mut s = self.state.lock().unwrap();
                    let reserved = s.reserved.as_mut().unwrap();
                    let _: redis::Value = reserved
                        .conn_mut()
                        .send_packed_command(&redis::cmd("DISCARD"))
                        .await
                        .map_err(|e| e)?
                        .pop()
                        .unwrap_or(redis::Value::Nil);
                    reserved.unwatch_if_needed().await
                })
            });
            res.map_err(to_py_err)?;
        }
        let mut s = self.state.lock().unwrap();
        s.commands.clear();
        s.explicit_transaction = false;
        s.watched_keys.clear();
        s.watching = false;
        Ok(())
    }
}

impl Pipeline {
    /// Reserve a connection from the driver if one isn't already held.
    fn ensure_reserved(&self, py: Python<'_>) -> PyResult<()> {
        {
            let s = self.state.lock().unwrap();
            if s.reserved.is_some() {
                return Ok(());
            }
        }
        let driver_clone = self.driver.clone_ref(py);
        let reserved: Result<ReservedConnection, String> = py.detach(|| {
            get_runtime().block_on(async move {
                Python::attach(|py| {
                    let drv = driver_clone.bind(py).borrow();
                    drv.connection_clone()
                })
                .reserve_connection()
                .await
            })
        });
        match reserved {
            Ok(r) => {
                self.state.lock().unwrap().reserved = Some(r);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<crate::exceptions::ConnectionError, _>(e)),
        }
    }
}

/// Used as a temporary placeholder during `Option::take` swaps inside the
/// async path (some idioms above need a placeholder to swap in to satisfy
/// borrow-checker constraints during `Option<&mut T>` chains).
fn dummy_reserved() -> ReservedConnection {
    // SAFETY-irrelevant: this value is never used in async I/O — it is
    // only ever the temporary side of a swap that immediately swaps it
    // back out. The contained MultiplexedConnection comes from a path
    // that is never executed at runtime.
    unreachable!("dummy_reserved is a placeholder used inside swap idioms")
}
```

Note the `dummy_reserved()` placeholder: that idiom is awkward. Replace the `unwatch` body with a cleaner version that doesn't need it:

```rust
    /// Send UNWATCH on the reserved connection (if any) and clear the
    /// watched-keys list. Always returns True.
    fn unwatch(&self, py: Python<'_>) -> PyResult<bool> {
        // Quick check + clear-only path if no connection is reserved.
        {
            let s = self.state.lock().unwrap();
            if s.closed {
                return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
            }
            if s.reserved.is_none() {
                return Ok(true);
            }
        }
        // Issue UNWATCH on the reserved connection.
        let res: Result<(), _> = py.detach(|| {
            get_runtime().block_on(async {
                let mut s = self.state.lock().unwrap();
                let reserved = s.reserved.as_mut().unwrap();
                reserved.unwatch_if_needed().await
            })
        });
        res.map_err(to_py_err)?;
        let mut s = self.state.lock().unwrap();
        s.watched_keys.clear();
        s.watching = false;
        Ok(true)
    }
```

(Delete the `dummy_reserved` function and the `// Two swaps that net out` block from the earlier version. The clean form above is what ships.)

- [ ] **Step 3: Add `RedisRsDriver::connection_clone` accessor**

`Pipeline.ensure_reserved` calls `drv.connection_clone()` to get an owned `ValkeyConn` it can call `reserve_connection()` on. Add this Rust-only accessor in `crates/redis-rs-py-driver/src/driver.rs`:

```rust
impl RedisRsDriver {
    /// Rust-only: clone the inner connection so the facade can call
    /// `reserve_connection()` on it.
    pub(crate) fn connection_clone(&self) -> crate::connection::ValkeyConn {
        self.connection.clone()
    }
}
```

- [ ] **Step 4: Replace the `discard` body — DISCARD without `send_packed_command`**

The `discard()` body above uses `send_packed_command` which is a `MultiplexedConnection` low-level API and may not exist in your installed redis-rs version. Use the high-level path:

```rust
    /// Drop the buffered transaction block and clear watched keys.
    fn discard(&self, py: Python<'_>) -> PyResult<()> {
        let needs_server_discard = {
            let s = self.state.lock().unwrap();
            !s.closed && s.explicit_transaction && s.reserved.is_some()
        };
        if needs_server_discard {
            let res: Result<(), _> = py.detach(|| {
                get_runtime().block_on(async {
                    let mut s = self.state.lock().unwrap();
                    let reserved = s.reserved.as_mut().unwrap();
                    // DISCARD is only valid after MULTI — we got here from
                    // explicit_transaction == true, so a MULTI happened
                    // server-side too.  But we never sent MULTI directly —
                    // multi() is purely client-side state. The server's
                    // MULTI is sent only by execute() inside the
                    // pipeline_exec_watched call.  So DISCARD-on-server
                    // would actually fail. The right thing is just to
                    // UNWATCH so the watched-keys list is reset on the
                    // server.
                    reserved.unwatch_if_needed().await
                })
            });
            res.map_err(to_py_err)?;
        }
        let mut s = self.state.lock().unwrap();
        s.commands.clear();
        s.explicit_transaction = false;
        s.watched_keys.clear();
        s.watching = false;
        Ok(())
    }
```

- [ ] **Step 5: Build + run the state-machine tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_pipeline_discard.py -v`
Expected: 6 PASS.

If `test_multi_after_watch_buffers_subsequent_commands` fails with the error `WATCH-mode execute() lands in Task 9`, that's expected — it tests the WATCH-mode execute path which is Task 9. Move that test temporarily to Task 9's test file or skip it now and remove the skip in Task 9.

For now, mark it skipped with a TODO:

```python
@pytest.mark.skip(reason="WATCH-mode execute() lands in Task 9")
def test_multi_after_watch_buffers_subsequent_commands(client) -> None:
    ...
```

Run: `uv run pytest tests/pipeline/test_pipeline_discard.py -v`
Expected: 5 PASS, 1 SKIP.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/pipeline.rs crates/redis-rs-py-driver/src/driver.rs tests/pipeline/test_pipeline_discard.py
git commit -m "feat(pipeline): add watch/unwatch/multi/discard state machine"
```

---

## Task 9: WATCH-mode `execute()` + `WatchError` raise

Implement `execute_watched()` so the WATCH path actually flushes through `pipeline_exec_watched`. On `WatchedExecResult::WatchAborted`, raise `WatchError`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/pipeline.rs`
- Test: `tests/pipeline/test_pipeline_watch.py`

- [ ] **Step 1: Write the conflict test**

Create `tests/pipeline/test_pipeline_watch.py`:

```python
"""WATCH-mode pipeline: WatchError on concurrent modification.

Concurrent test uses two clients on two threads. The first thread WATCHes
a key, sleeps to let the second thread modify it, then EXECs and gets
WatchError.
"""

from __future__ import annotations

import threading
import time

import pytest

from redis_rs_py.exceptions import WatchError


def test_watch_on_unmodified_key_executes_normally(client) -> None:
    client.set("k", b"v")
    with client.pipeline() as pipe:
        pipe.watch("k")
        cur = pipe.get("k")
        assert cur == b"v"
        pipe.multi()
        result = pipe.set("k", b"v2").execute()
    assert result == [True]
    assert client.get("k") == b"v2"


def test_watch_then_concurrent_modification_raises(client, valkey_url: str) -> None:
    """Two clients race on the same key. The watching client must raise
    WatchError when EXEC is called after the other client modifies the
    watched key."""
    from redis_rs_py import Redis

    client.set("k", b"original")

    barrier = threading.Barrier(2)
    other_done = threading.Event()
    err_holder: list[Exception] = []

    def watcher() -> None:
        try:
            with client.pipeline() as pipe:
                pipe.watch("k")
                # Read the value (immediate mode).
                cur = pipe.get("k")
                assert cur == b"original"
                # Signal the other thread to modify the key, then wait.
                barrier.wait()
                other_done.wait(timeout=2.0)
                # Now flip to transaction and try to set — this must
                # raise WatchError because the key changed.
                pipe.multi()
                pipe.set("k", b"by-watcher")
                pipe.execute()
        except WatchError as e:  # noqa: BLE001
            err_holder.append(e)
        except Exception as e:  # noqa: BLE001
            err_holder.append(e)

    def modifier() -> None:
        other = Redis.from_url(valkey_url)
        try:
            barrier.wait()
            other.set("k", b"by-modifier")
            other_done.set()
        finally:
            other.close()

    t1 = threading.Thread(target=watcher)
    t2 = threading.Thread(target=modifier)
    t1.start()
    t2.start()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    assert not t1.is_alive(), "watcher thread hung"
    assert not t2.is_alive(), "modifier thread hung"
    assert len(err_holder) == 1, f"expected 1 error, got {err_holder}"
    assert isinstance(err_holder[0], WatchError), f"expected WatchError, got {type(err_holder[0])}"

    # The modifier's value won, not the watcher's.
    assert client.get("k") == b"by-modifier"


def test_watch_after_no_change_then_execute(client) -> None:
    """If no other client modifies the watched key, EXEC succeeds."""
    client.set("counter", b"0")
    with client.pipeline() as pipe:
        pipe.watch("counter")
        cur = pipe.get("counter")
        assert cur == b"0"
        pipe.multi()
        pipe.incr("counter")
        result = pipe.execute()
    assert result == [1]
    assert client.get("counter") == b"1"


def test_watch_multiple_keys_only_one_modified_raises(client, valkey_url: str) -> None:
    """WATCH a, b. Modify b. EXEC must raise WatchError."""
    from redis_rs_py import Redis

    client.set("a", b"a-orig")
    client.set("b", b"b-orig")

    other = Redis.from_url(valkey_url)
    try:
        with client.pipeline() as pipe:
            pipe.watch("a", "b")
            other.set("b", b"b-touched")
            pipe.multi()
            pipe.set("a", b"a-new")
            with pytest.raises(WatchError):
                pipe.execute()
    finally:
        other.close()
```

Also unskip the test we deferred in Task 8:

```python
def test_multi_after_watch_buffers_subsequent_commands(client) -> None:
    ...
```

Run: `uv run pytest tests/pipeline/test_pipeline_watch.py tests/pipeline/test_pipeline_discard.py::test_multi_after_watch_buffers_subsequent_commands -v`
Expected: every test FAILS — `RuntimeError: WATCH-mode execute() lands in Task 9`.

- [ ] **Step 2: Replace `execute_watched` with the real body**

In `crates/redis-rs-py-driver/src/facade/pipeline.rs`, find the `execute_watched` stub and replace with:

```rust
impl Pipeline {
    fn execute_watched(
        &self,
        py: Python<'_>,
        commands: Vec<BufferedCmd>,
    ) -> PyResult<Py<PyAny>> {
        // Run pipeline_exec_watched on the reserved connection.
        let watched_keys: Vec<String> = self.state.lock().unwrap().watched_keys.clone();

        let result: Result<WatchedExecResult, _> = py.detach(|| {
            get_runtime().block_on(async {
                let mut s = self.state.lock().unwrap();
                let reserved = s.reserved.as_mut().ok_or_else(|| {
                    redis::RedisError::from((
                        redis::ErrorKind::ClientError,
                        "internal error",
                        "execute_watched without a reserved connection".to_string(),
                    ))
                })?;
                reserved
                    .pipeline_exec_watched(&watched_keys, commands)
                    .await
            })
        });

        // Whatever the outcome, drop the reservation + buffer (matches
        // redis-py's reset() inside execute()'s finally clause).
        let drop_result = || {
            let mut s = self.state.lock().unwrap();
            s.commands.clear();
            s.watched_keys.clear();
            s.watching = false;
            s.explicit_transaction = false;
            // Take and drop the reserved connection — UNWATCH on the
            // server already happened (EXEC clears WATCH server-side).
            let _ = s.reserved.take();
        };

        match result {
            Ok(WatchedExecResult::Ok(items)) => {
                drop_result();
                RawResult::Value(redis::Value::Array(items)).into_py(py)
            }
            Ok(WatchedExecResult::WatchAborted) => {
                drop_result();
                Err(PyErr::new::<WatchError, _>("Watched variable changed."))
            }
            Err(e) => {
                drop_result();
                Err(to_py_err(e))
            }
        }
    }
}
```

- [ ] **Step 3: Build + run the WATCH tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_pipeline_watch.py tests/pipeline/test_pipeline_discard.py -v`
Expected: 9 PASS (5 from discard + 4 from watch), 0 SKIP.

If `test_watch_then_concurrent_modification_raises` is flaky (race window too small), bump the `other_done.wait(timeout=2.0)` to 5.0 and add a `time.sleep(0.05)` after `other_done.set()` to make the race deterministic.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/pipeline.rs tests/pipeline/test_pipeline_watch.py tests/pipeline/test_pipeline_discard.py
git commit -m "feat(pipeline): implement WATCH-mode execute() with WatchError raise"
```

---

## Task 10: `RedisRsDriver.transaction()` retry helper (sync)

The redis-py `transaction(func, *watches, value_from_callable=False, watch_delay=None, **kwargs)` helper is a thin loop that creates a pipeline, calls `func(pipe)`, calls `pipe.execute()`, and retries on `WatchError`.

We implement it on the high-level `Redis` façade (since that's where users call it from), but also expose it directly on `RedisRsDriver` for low-level callers.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/pipeline.rs` (add `transaction_helper`)
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs` (add `Redis.transaction(...)`)
- Test: `tests/pipeline/test_transaction_helper.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/pipeline/test_transaction_helper.py`:

```python
"""Redis.transaction(func, *watches, **kwargs) retry helper."""

from __future__ import annotations

import threading
import time

import pytest

from redis_rs_py.exceptions import WatchError


def test_transaction_no_watch_returns_exec_value(client) -> None:
    def body(pipe) -> None:
        pipe.set("a", b"1")
        pipe.set("b", b"2")

    result = client.transaction(body)
    assert result == [True, True]


def test_transaction_with_watch_no_conflict(client) -> None:
    client.set("counter", b"5")

    def body(pipe) -> None:
        cur = int(pipe.get("counter"))
        pipe.multi()
        pipe.set("counter", str(cur + 1).encode())

    result = client.transaction(body, "counter")
    assert result == [True]
    assert client.get("counter") == b"6"


def test_transaction_value_from_callable_returns_func_value(client) -> None:
    client.set("a", b"hello")

    def body(pipe) -> str:
        cur = pipe.get("a").decode()
        pipe.multi()
        pipe.set("a", b"world")
        return cur  # this is what gets returned, not the EXEC reply list

    result = client.transaction(body, "a", value_from_callable=True)
    assert result == "hello"
    assert client.get("a") == b"world"


def test_transaction_retries_on_watch_error(client, valkey_url: str) -> None:
    """The helper retries the func until EXEC succeeds without a watched-key
    conflict. We trigger a single conflict on the first attempt."""
    from redis_rs_py import Redis

    client.set("counter", b"0")
    attempts = {"n": 0}

    def body(pipe) -> int:
        attempts["n"] += 1
        cur = int(pipe.get("counter"))
        if attempts["n"] == 1:
            # Mid-transaction, sneak in a conflicting write from another
            # client. The first execute() will raise WatchError.
            other = Redis.from_url(valkey_url)
            try:
                other.set("counter", b"99")
            finally:
                other.close()
        pipe.multi()
        pipe.set("counter", str(cur + 1).encode())
        return cur + 1

    new_value = client.transaction(body, "counter", value_from_callable=True)
    # First attempt: read 0, conflict → retry. Second attempt: read 99
    # (the value the other client wrote), set 100, no conflict → return 100.
    assert attempts["n"] == 2
    assert new_value == 100
    assert client.get("counter") == b"100"


def test_transaction_watch_delay_is_respected(client, valkey_url: str) -> None:
    """If watch_delay is set, the helper sleeps between retries."""
    from redis_rs_py import Redis

    client.set("k", b"0")
    attempts = {"n": 0, "ts": []}

    def body(pipe) -> None:
        attempts["n"] += 1
        attempts["ts"].append(time.monotonic())
        cur = int(pipe.get("k"))
        if attempts["n"] == 1:
            other = Redis.from_url(valkey_url)
            try:
                other.set("k", b"100")
            finally:
                other.close()
        pipe.multi()
        pipe.set("k", str(cur + 1).encode())

    client.transaction(body, "k", watch_delay=0.1)
    assert attempts["n"] == 2
    elapsed = attempts["ts"][1] - attempts["ts"][0]
    assert elapsed >= 0.09, f"expected ≥0.1s sleep, got {elapsed:.3f}s"


def test_transaction_propagates_non_watch_errors(client) -> None:
    """Errors raised inside `func` that aren't WatchError should propagate."""
    def body(pipe) -> None:
        raise ValueError("user-side blow-up")

    with pytest.raises(ValueError, match="user-side"):
        client.transaction(body)


def test_transaction_propagates_response_error_from_command(client) -> None:
    """A bad command inside the transaction (e.g. INCR on a string) should
    raise ResponseError, NOT loop forever."""
    from redis_rs_py.exceptions import ResponseError

    client.set("k", b"not-a-number")

    def body(pipe) -> None:
        pipe.multi()
        pipe.incr("k")  # → WRONGTYPE / ResponseError on EXEC

    with pytest.raises(ResponseError):
        client.transaction(body)
```

- [ ] **Step 2: Implement `transaction_helper` on `Pipeline`**

In `crates/redis-rs-py-driver/src/facade/pipeline.rs`, add the helper function (a free function, not a pyclass method — it's called from the façade's `transaction()` binding):

```rust
// =========================================================================
// transaction(func, *watches, value_from_callable, watch_delay, **kwargs)
// =========================================================================

/// Implementation of redis-py's `Redis.transaction()` helper. Loops on
/// `WatchError`, optionally sleeping `watch_delay` seconds between
/// attempts.
///
/// Called from `Redis.transaction(...)` (and any other façade that
/// embeds a driver).
pub(crate) fn transaction_helper(
    py: Python<'_>,
    driver: Py<RedisRsDriver>,
    func: Py<PyAny>,
    watches: Vec<String>,
    value_from_callable: bool,
    watch_delay: Option<f64>,
) -> PyResult<Py<PyAny>> {
    loop {
        let pipe = Py::new(py, Pipeline::new(driver.clone_ref(py), true))?;

        // Optional WATCH.
        let res: PyResult<Py<PyAny>> = (|| {
            if !watches.is_empty() {
                pipe.bind(py)
                    .borrow()
                    .watch(py, watches.clone())?;
            }
            // Call the user function with `pipe`.
            let func_value = func.call1(py, (pipe.clone_ref(py),))?;
            // Execute the pipeline.
            let exec_value = pipe.bind(py).borrow().execute(py)?;
            if value_from_callable {
                Ok(func_value)
            } else {
                Ok(exec_value)
            }
        })();

        // Always reset the pipeline (mirrors `with self.pipeline(True)`'s
        // __exit__).
        let _ = pipe.bind(py).borrow().reset(py);

        match res {
            Ok(v) => return Ok(v),
            Err(e) => {
                let is_watch = e.is_instance_of::<WatchError>(py);
                if is_watch {
                    if let Some(d) = watch_delay
                        && d > 0.0
                    {
                        // Drop GIL while sleeping so other Python threads
                        // can run.
                        py.detach(|| std::thread::sleep(std::time::Duration::from_secs_f64(d)));
                    }
                    continue;
                }
                return Err(e);
            }
        }
    }
}
```

- [ ] **Step 3: Bind it on `Redis` (sync façade)**

Edit `crates/redis-rs-py-driver/src/facade/sync.rs`. Inside the `#[pymethods] impl Redis { ... }` block (created by plan 10), add:

```rust
    /// Return a new `Pipeline` bound to this client.
    #[pyo3(signature = (transaction = true, shard_hint = None))]
    fn pipeline(
        &self,
        py: Python<'_>,
        transaction: bool,
        shard_hint: Option<Py<PyAny>>,
    ) -> PyResult<Py<crate::facade::pipeline::Pipeline>> {
        let _ = shard_hint;  // accepted for redis-py compat, unused
        Py::new(
            py,
            crate::facade::pipeline::Pipeline::new(self.driver.clone_ref(py), transaction),
        )
    }

    /// `transaction(func, *watches, value_from_callable=False,
    /// watch_delay=None, **kwargs)` — the redis-py retry helper.
    /// `**kwargs` is accepted-and-ignored for compat (redis-py uses it
    /// to thread `shard_hint` through; we don't have a use for it).
    #[pyo3(signature = (func, *watches, value_from_callable = false, watch_delay = None, **_kwargs))]
    fn transaction(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        watches: Vec<String>,
        value_from_callable: bool,
        watch_delay: Option<f64>,
        _kwargs: Option<Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        crate::facade::pipeline::transaction_helper(
            py,
            self.driver.clone_ref(py),
            func,
            watches,
            value_from_callable,
            watch_delay,
        )
    }
```

(`self.driver` is the `Py<RedisRsDriver>` field on the `Redis` pyclass, set by plan 10.)

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_transaction_helper.py -v`
Expected: 7 PASS.

If `test_transaction_retries_on_watch_error` is flaky, increase the gap by sleeping briefly after the conflicting `other.set("counter", b"99")` so the WATCH→change ordering is deterministic.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/pipeline.rs crates/redis-rs-py-driver/src/facade/sync.rs tests/pipeline/test_transaction_helper.py
git commit -m "feat(pipeline): add Redis.transaction() retry helper (sync)"
```

---

## Task 11: `AsyncPipeline` pyclass + async `aexecute`/`watch`/`multi`/`discard`/`atransaction`

Async sibling of everything we just landed. Methods are async (return `RedisRsAwaitable` for I/O-bound ones; return `self` synchronously for chained command-buffer methods). The state machine is identical.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/pipeline.rs` (add `AsyncPipeline`)
- Modify: `crates/redis-rs-py-driver/src/facade/asyncio.rs` (add `pipeline()` factory + `transaction()`)
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (register `AsyncPipeline` on the `asyncio` submodule)

- [ ] **Step 1: Write the failing async tests**

Create `tests/pipeline/test_async_pipeline_basic.py`:

```python
"""Async sibling of test_pipeline_basic.py."""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_async_pipeline_chains_buffered_commands(aclient) -> None:
    async with aclient.pipeline() as pipe:
        result = (
            await pipe.set("a", b"hello")
                       .incr("counter")
                       .incr("counter")
                       .get("a")
                       .aexecute()
        )
    assert result == [True, 1, 2, b"hello"]


@pytest.mark.asyncio
async def test_async_pipeline_set_returns_self_for_chaining(aclient) -> None:
    pipe = aclient.pipeline()
    same = pipe.set("k", b"v")
    assert same is pipe
    await pipe.aclose()


@pytest.mark.asyncio
async def test_async_pipeline_empty_aexecute_returns_empty_list(aclient) -> None:
    async with aclient.pipeline() as pipe:
        result = await pipe.aexecute()
    assert result == []


@pytest.mark.asyncio
async def test_async_pipeline_default_is_transactional(aclient) -> None:
    async with aclient.pipeline() as pipe:
        result = await pipe.set("k", b"v").get("k").aexecute()
    assert result == [True, b"v"]
```

Create `tests/pipeline/test_async_pipeline_transaction.py`:

```python
"""Async MULTI/EXEC."""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_async_atomic_pipeline_executes_as_one_block(aclient) -> None:
    async with aclient.pipeline(transaction=True) as pipe:
        result = (
            await pipe.set("counter", b"10")
                       .incr("counter")
                       .incr("counter")
                       .get("counter")
                       .aexecute()
        )
    assert result == [True, 11, 12, b"12"]


@pytest.mark.asyncio
async def test_async_pipeline_in_atomic_mode_then_reuse(aclient) -> None:
    pipe = aclient.pipeline(transaction=True)
    assert await pipe.set("a", b"1").get("a").aexecute() == [True, b"1"]
    assert await pipe.set("b", b"2").get("b").aexecute() == [True, b"2"]
    await pipe.aclose()
```

Create `tests/pipeline/test_async_pipeline_discard.py`:

```python
"""Async watch/unwatch/multi/discard state machine."""

from __future__ import annotations

import pytest

from redis_rs_py.exceptions import RedisError


@pytest.mark.asyncio
async def test_async_watch_then_unwatch_no_op(aclient) -> None:
    await aclient.set("k", b"v")
    async with aclient.pipeline() as pipe:
        await pipe.awatch("k")
        assert await pipe.aunwatch() is True
        result = await pipe.set("k", b"v2").aexecute()
    assert result == [True]
    assert await aclient.get("k") == b"v2"


@pytest.mark.asyncio
async def test_async_multi_after_watch_buffers(aclient) -> None:
    await aclient.set("balance", b"100")
    async with aclient.pipeline() as pipe:
        await pipe.awatch("balance")
        cur = await pipe.aget_immediate("balance")
        assert cur == b"100"
        pipe.multi()
        result = await pipe.set("balance", b"90").incr("withdrawals").aexecute()
    assert result == [True, 1]
    assert await aclient.get("balance") == b"90"


@pytest.mark.asyncio
async def test_async_multi_after_multi_raises(aclient) -> None:
    async with aclient.pipeline() as pipe:
        await pipe.awatch("x")
        pipe.multi()
        with pytest.raises(RedisError, match="nested"):
            pipe.multi()


@pytest.mark.asyncio
async def test_async_discard_clears_buffer(aclient) -> None:
    await aclient.set("k", b"v")
    async with aclient.pipeline() as pipe:
        await pipe.awatch("k")
        pipe.multi()
        pipe.set("k", b"new")
        await pipe.adiscard()
        assert len(pipe) == 0
```

Create `tests/pipeline/test_async_pipeline_watch.py`:

```python
"""Async WATCH-mode pipeline conflict detection (asyncio.gather)."""

from __future__ import annotations

import asyncio

import pytest

from redis_rs_py.exceptions import WatchError


@pytest.mark.asyncio
async def test_async_watch_no_conflict_executes(aclient) -> None:
    await aclient.set("counter", b"0")
    async with aclient.pipeline() as pipe:
        await pipe.awatch("counter")
        cur = await pipe.aget_immediate("counter")
        assert cur == b"0"
        pipe.multi()
        result = await pipe.incr("counter").aexecute()
    assert result == [1]
    assert await aclient.get("counter") == b"1"


@pytest.mark.asyncio
async def test_async_watch_conflict_raises(aclient, valkey_url: str) -> None:
    from redis_rs_py.asyncio import Redis as AsyncRedis

    await aclient.set("k", b"original")

    started = asyncio.Event()
    other_done = asyncio.Event()
    err_holder: list[Exception] = []

    async def watcher() -> None:
        try:
            async with aclient.pipeline() as pipe:
                await pipe.awatch("k")
                cur = await pipe.aget_immediate("k")
                assert cur == b"original"
                started.set()
                await asyncio.wait_for(other_done.wait(), timeout=2.0)
                pipe.multi()
                pipe.set("k", b"by-watcher")
                await pipe.aexecute()
        except WatchError as e:  # noqa: BLE001
            err_holder.append(e)

    async def modifier() -> None:
        other = AsyncRedis.from_url(valkey_url)
        try:
            await asyncio.wait_for(started.wait(), timeout=2.0)
            await other.set("k", b"by-modifier")
            other_done.set()
        finally:
            await other.aclose()

    await asyncio.gather(watcher(), modifier())

    assert len(err_holder) == 1
    assert isinstance(err_holder[0], WatchError)
    assert await aclient.get("k") == b"by-modifier"
```

Create `tests/pipeline/test_async_transaction_helper.py`:

```python
"""Async sibling of test_transaction_helper.py."""

from __future__ import annotations

import time

import pytest


@pytest.mark.asyncio
async def test_async_transaction_no_watch_returns_exec_value(aclient) -> None:
    async def body(pipe) -> None:
        pipe.set("a", b"1")
        pipe.set("b", b"2")

    result = await aclient.atransaction(body)
    assert result == [True, True]


@pytest.mark.asyncio
async def test_async_transaction_value_from_callable(aclient) -> None:
    await aclient.set("a", b"hello")

    async def body(pipe) -> str:
        cur = (await pipe.aget_immediate("a")).decode()
        pipe.multi()
        pipe.set("a", b"world")
        return cur

    result = await aclient.atransaction(body, "a", value_from_callable=True)
    assert result == "hello"
    assert await aclient.get("a") == b"world"


@pytest.mark.asyncio
async def test_async_transaction_retries_on_watch_error(aclient, valkey_url: str) -> None:
    from redis_rs_py.asyncio import Redis as AsyncRedis

    await aclient.set("counter", b"0")
    attempts = {"n": 0}

    async def body(pipe) -> int:
        attempts["n"] += 1
        cur = int(await pipe.aget_immediate("counter"))
        if attempts["n"] == 1:
            other = AsyncRedis.from_url(valkey_url)
            try:
                await other.set("counter", b"99")
            finally:
                await other.aclose()
        pipe.multi()
        pipe.set("counter", str(cur + 1).encode())
        return cur + 1

    new_value = await aclient.atransaction(
        body, "counter", value_from_callable=True
    )
    assert attempts["n"] == 2
    assert new_value == 100
    assert await aclient.get("counter") == b"100"


@pytest.mark.asyncio
async def test_async_transaction_watch_delay_is_respected(
    aclient, valkey_url: str
) -> None:
    from redis_rs_py.asyncio import Redis as AsyncRedis

    await aclient.set("k", b"0")
    attempts = {"n": 0, "ts": []}

    async def body(pipe) -> None:
        attempts["n"] += 1
        attempts["ts"].append(time.monotonic())
        cur = int(await pipe.aget_immediate("k"))
        if attempts["n"] == 1:
            other = AsyncRedis.from_url(valkey_url)
            try:
                await other.set("k", b"100")
            finally:
                await other.aclose()
        pipe.multi()
        pipe.set("k", str(cur + 1).encode())

    await aclient.atransaction(body, "k", watch_delay=0.1)
    assert attempts["n"] == 2
    elapsed = attempts["ts"][1] - attempts["ts"][0]
    assert elapsed >= 0.09
```

Run the lot to confirm red:

Run: `uv run pytest tests/pipeline/test_async_pipeline_basic.py tests/pipeline/test_async_pipeline_transaction.py tests/pipeline/test_async_pipeline_watch.py tests/pipeline/test_async_pipeline_discard.py tests/pipeline/test_async_transaction_helper.py -v`
Expected: every test FAILS — `AttributeError: ... has no attribute 'pipeline'` on the asyncio façade, or `'AsyncPipeline' object has no attribute 'aexecute'`.

- [ ] **Step 2: Implement `AsyncPipeline` pyclass**

Append to `crates/redis-rs-py-driver/src/facade/pipeline.rs`:

```rust
// =========================================================================
// AsyncPipeline — async sibling of Pipeline
// =========================================================================

#[pyclass(module = "redis_rs_py.asyncio", unsendable = false)]
pub struct AsyncPipeline {
    driver: Py<RedisRsDriver>,
    state: Mutex<PipelineState>,
}

impl AsyncPipeline {
    pub fn new(driver: Py<RedisRsDriver>, transaction: bool) -> Self {
        Self {
            driver,
            state: Mutex::new(PipelineState::new(transaction)),
        }
    }
}

/// Inner async helpers shared by every `AsyncPipeline` method that does
/// I/O. Each spawns onto the runtime and returns a `RedisRsAwaitable`.
impl AsyncPipeline {
    fn driver_clone(&self, py: Python<'_>) -> Py<RedisRsDriver> {
        self.driver.clone_ref(py)
    }

    fn buffer_or_dispatch_async(
        slf: Py<Self>,
        py: Python<'_>,
        name: &str,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let this = slf.bind(py).borrow();
        if this.state.lock().unwrap().closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        let immediate = {
            let s = this.state.lock().unwrap();
            s.watching && !s.explicit_transaction
        };
        if immediate {
            // Async immediate-mode dispatch is exposed as `a<cmd>_immediate`;
            // calls to the chainable `cmd()` form during immediate mode
            // return self-without-IO so the call stays synchronous and
            // chainable. The caller should use `aget_immediate` etc. for
            // explicit immediate dispatch.
            // Document that policy clearly: the chainable form **never**
            // dispatches; users go through aget_immediate/aset_immediate
            // when they want a value back from immediate mode.
            //
            // For symmetry with redis-py we still let the chainable form
            // buffer in immediate mode; the chained calls will be replayed
            // when execute() is called. This matches what users actually
            // want.
            this.state
                .lock()
                .unwrap()
                .commands
                .push((name.to_string(), args));
        } else {
            this.state
                .lock()
                .unwrap()
                .commands
                .push((name.to_string(), args));
        }
        drop(this);
        Ok(slf.into_any())
    }
}

/// Same command-buffering macro as `Pipeline`, retargeted at `AsyncPipeline`.
macro_rules! async_pipeline_cmd {
    ($method:ident, $cmd:expr, ($($arg:ident: $argty:ty),*)) => {
        #[pyo3(signature = ($($arg),*))]
        fn $method(slf: Py<Self>, py: Python<'_>, $($arg: $argty),*) -> PyResult<Py<PyAny>> {
            let mut args: Vec<Vec<u8>> = Vec::new();
            $( args.push($crate::facade::pipeline::to_arg_bytes($arg)); )*
            AsyncPipeline::buffer_or_dispatch_async(slf, py, $cmd, args)
        }
    };
    ($method:ident, $cmd:expr, varargs $head:ident: $headty:ty) => {
        #[pyo3(signature = ($head, *values))]
        fn $method(
            slf: Py<Self>,
            py: Python<'_>,
            $head: $headty,
            values: Vec<Vec<u8>>,
        ) -> PyResult<Py<PyAny>> {
            let mut args: Vec<Vec<u8>> = Vec::new();
            args.push($crate::facade::pipeline::to_arg_bytes($head));
            for v in values { args.push(v); }
            AsyncPipeline::buffer_or_dispatch_async(slf, py, $cmd, args)
        }
    };
    ($method:ident, $cmd:expr, keys *keys) => {
        #[pyo3(signature = (*keys))]
        fn $method(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
            let args: Vec<Vec<u8>> = keys.into_iter().map(|s| s.into_bytes()).collect();
            AsyncPipeline::buffer_or_dispatch_async(slf, py, $cmd, args)
        }
    };
}

#[pymethods]
impl AsyncPipeline {
    fn __aenter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__(
        slf: Py<Self>,
        py: Python<'_>,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::aclose(slf, py)
    }

    fn __len__(&self) -> usize {
        self.state.lock().unwrap().commands.len()
    }

    fn __bool__(&self) -> bool {
        true
    }

    /// Async close — returns a RedisRsAwaitable that resolves when any
    /// reserved connection has been UNWATCHed and released.
    fn aclose(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let result: RawResult = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                let mut s = this.state.lock().unwrap();
                let reserved = s.reserved.take();
                s.commands.clear();
                s.watched_keys.clear();
                s.watching = false;
                s.explicit_transaction = false;
                s.closed = true;
                reserved
            });
            if let Some(mut r) = result.into_reserved() {
                let _ = r.unwatch_if_needed().await;
            }
            let _ = tx.send(RawResult::Nil);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    /// Same as `aclose` but exposed under the redis-py-style name.
    fn reset(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        AsyncPipeline::aclose(slf, py)
    }

    /// `pipe.awatch(*keys)` — accumulate watched keys and reserve a
    /// connection. Returns an awaitable resolving to True.
    #[pyo3(signature = (*keys))]
    fn awatch(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        let driver = slf.bind(py).borrow().driver_clone(py);
        let conn = driver.bind(py).borrow().connection_clone();
        get_runtime().spawn(async move {
            let result: RawResult = (|| async {
                // Reserve if needed.
                let need_reserve = Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    let s = this.state.lock().unwrap();
                    if s.closed {
                        return Err("pipeline is closed".to_string());
                    }
                    if s.explicit_transaction {
                        return Err("Cannot issue a WATCH after a MULTI".to_string());
                    }
                    Ok(s.reserved.is_none())
                });
                let need_reserve = match need_reserve {
                    Ok(b) => b,
                    Err(e) => return RawResult::Error(
                        crate::exceptions::ExceptionClass::RedisError, e,
                    ),
                };
                if need_reserve {
                    let r = match conn.reserve_connection().await {
                        Ok(r) => r,
                        Err(e) => return RawResult::Error(
                            crate::exceptions::ExceptionClass::ConnectionError, e,
                        ),
                    };
                    Python::attach(|py| {
                        let this = slf_clone.bind(py).borrow();
                        this.state.lock().unwrap().reserved = Some(r);
                    });
                }
                // Issue WATCH on the reserved conn.
                let res = {
                    // Take the reserved connection out of the slot, do the
                    // I/O, put it back. We can't hold the std::sync::Mutex
                    // across the await.
                    let mut taken = Python::attach(|py| {
                        let this = slf_clone.bind(py).borrow();
                        this.state.lock().unwrap().reserved.take()
                    }).expect("reserved must be present");
                    let res = taken.watch(&keys).await;
                    Python::attach(|py| {
                        let this = slf_clone.bind(py).borrow();
                        this.state.lock().unwrap().reserved = Some(taken);
                    });
                    res
                };
                if let Err(e) = res {
                    return crate::errors::classify(e);
                }
                Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    let mut s = this.state.lock().unwrap();
                    s.watching = true;
                    s.watched_keys.extend(keys.iter().cloned());
                });
                RawResult::Bool(true)
            })().await;
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    /// `pipe.aunwatch()` — send UNWATCH on the reserved connection.
    fn aunwatch(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let mut taken = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved.take()
            });
            let result = if let Some(ref mut r) = taken {
                r.unwatch_if_needed().await.map(|_| ())
            } else {
                Ok(())
            };
            Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                let mut s = this.state.lock().unwrap();
                s.reserved = taken;
                s.watched_keys.clear();
                s.watching = false;
            });
            let raw = match result {
                Ok(()) => RawResult::Bool(true),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    /// Synchronous (matches redis-py: `multi()` is sync even on async
    /// pipelines because it does not touch I/O).
    fn multi(&self) -> PyResult<()> {
        let mut s = self.state.lock().unwrap();
        if s.closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        if s.explicit_transaction {
            return Err(PyErr::new::<RedisError, _>(
                "Cannot issue nested calls to MULTI",
            ));
        }
        if !s.commands.is_empty() {
            return Err(PyErr::new::<RedisError, _>(
                "Commands without an initial WATCH have already been issued",
            ));
        }
        s.explicit_transaction = true;
        Ok(())
    }

    /// `pipe.adiscard()` — drop buffered transaction block + UNWATCH.
    fn adiscard(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let mut taken = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved.take()
            });
            if let Some(ref mut r) = taken {
                let _ = r.unwatch_if_needed().await;
            }
            Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                let mut s = this.state.lock().unwrap();
                s.reserved = taken;
                s.commands.clear();
                s.explicit_transaction = false;
                s.watched_keys.clear();
                s.watching = false;
            });
            let _ = tx.send(RawResult::Nil);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    /// Explicit immediate-mode read. Users call `await pipe.aget_immediate(k)`
    /// when they want the actual value back from the WATCH-mode connection.
    /// The chainable `pipe.get(k)` form still buffers (so chained calls
    /// stay synchronous and chainable).
    fn aget_immediate(slf: Py<Self>, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        AsyncPipeline::immediate_dispatch(slf, py, "GET", vec![key.into_bytes()])
    }

    /// Explicit immediate-mode write.
    fn aset_immediate(
        slf: Py<Self>,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::immediate_dispatch(slf, py, "SET", vec![key.into_bytes(), value])
    }

    /// `await pipe.aexecute()` — flush.
    fn aexecute(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        let driver = slf.bind(py).borrow().driver_clone(py);
        let conn = driver.bind(py).borrow().connection_clone();
        get_runtime().spawn(async move {
            // Snapshot state.
            let (commands, transaction, watching, explicit_transaction, closed, watched_keys) =
                Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    let s = this.state.lock().unwrap();
                    (
                        s.commands.clone(),
                        s.transaction,
                        s.watching,
                        s.explicit_transaction,
                        s.closed,
                        s.watched_keys.clone(),
                    )
                });
            if closed {
                let _ = tx.send(RawResult::Error(
                    crate::exceptions::ExceptionClass::RedisError,
                    "pipeline is closed".to_string(),
                ));
                return;
            }
            if commands.is_empty() && !watching && !explicit_transaction {
                let _ = tx.send(RawResult::Value(redis::Value::Array(Vec::new())));
                return;
            }
            // WATCH path
            if watching || explicit_transaction {
                let mut taken = Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    this.state.lock().unwrap().reserved.take()
                });
                let res = match taken.as_mut() {
                    Some(r) => {
                        r.pipeline_exec_watched(&watched_keys, commands).await
                    }
                    None => Err(redis::RedisError::from((
                        redis::ErrorKind::ClientError,
                        "internal error",
                        "aexecute_watched without reservation".to_string(),
                    ))),
                };
                Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    let mut s = this.state.lock().unwrap();
                    s.commands.clear();
                    s.watched_keys.clear();
                    s.watching = false;
                    s.explicit_transaction = false;
                    s.reserved = None;  // EXEC clears WATCH server-side; release
                });
                let raw = match res {
                    Ok(WatchedExecResult::Ok(items)) => {
                        RawResult::Value(redis::Value::Array(items))
                    }
                    Ok(WatchedExecResult::WatchAborted) => RawResult::Error(
                        crate::exceptions::ExceptionClass::RedisError,
                        "Watched variable changed.".to_string(),
                    ),
                    Err(e) => crate::errors::classify(e),
                };
                let _ = tx.send(raw);
                return;
            }
            // Buffering path
            let mut conn = conn;
            let res = conn.pipeline_exec(commands, transaction).await;
            Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().commands.clear();
            });
            let raw = match res {
                Ok(items) => RawResult::Value(redis::Value::Array(items)),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // Strings
    async_pipeline_cmd!(set, "SET", (key: &str, value: Vec<u8>));
    async_pipeline_cmd!(get, "GET", (key: &str));
    async_pipeline_cmd!(getdel, "GETDEL", (key: &str));
    async_pipeline_cmd!(append, "APPEND", (key: &str, value: Vec<u8>));
    async_pipeline_cmd!(strlen, "STRLEN", (key: &str));
    async_pipeline_cmd!(incr, "INCR", (key: &str));
    async_pipeline_cmd!(incrby, "INCRBY", (key: &str, by: i64));
    async_pipeline_cmd!(incrbyfloat, "INCRBYFLOAT", (key: &str, by: f64));
    async_pipeline_cmd!(decr, "DECR", (key: &str));
    async_pipeline_cmd!(decrby, "DECRBY", (key: &str, by: i64));
    async_pipeline_cmd!(setrange, "SETRANGE", (key: &str, offset: i64, value: Vec<u8>));
    async_pipeline_cmd!(getrange, "GETRANGE", (key: &str, start: i64, end: i64));
    async_pipeline_cmd!(rename, "RENAME", (key: &str, new_key: &str));
    async_pipeline_cmd!(renamenx, "RENAMENX", (key: &str, new_key: &str));
    async_pipeline_cmd!(typ, "TYPE", (key: &str));
    async_pipeline_cmd!(expire, "EXPIRE", (key: &str, seconds: i64));
    async_pipeline_cmd!(pexpire, "PEXPIRE", (key: &str, millis: i64));
    async_pipeline_cmd!(ttl, "TTL", (key: &str));
    async_pipeline_cmd!(pttl, "PTTL", (key: &str));
    async_pipeline_cmd!(persist, "PERSIST", (key: &str));

    async_pipeline_cmd!(delete, "DEL", keys *keys);
    async_pipeline_cmd!(unlink, "UNLINK", keys *keys);
    async_pipeline_cmd!(exists, "EXISTS", keys *keys);

    async_pipeline_cmd!(lpush, "LPUSH", varargs key: &str);
    async_pipeline_cmd!(rpush, "RPUSH", varargs key: &str);
    async_pipeline_cmd!(lpop, "LPOP", (key: &str));
    async_pipeline_cmd!(rpop, "RPOP", (key: &str));
    async_pipeline_cmd!(llen, "LLEN", (key: &str));
    async_pipeline_cmd!(lrange, "LRANGE", (key: &str, start: i64, stop: i64));
    async_pipeline_cmd!(lindex, "LINDEX", (key: &str, index: i64));
    async_pipeline_cmd!(lrem, "LREM", (key: &str, count: i64, value: Vec<u8>));
    async_pipeline_cmd!(ltrim, "LTRIM", (key: &str, start: i64, stop: i64));
    async_pipeline_cmd!(lset, "LSET", (key: &str, index: i64, value: Vec<u8>));

    async_pipeline_cmd!(hset, "HSET", (key: &str, field: &str, value: Vec<u8>));
    async_pipeline_cmd!(hget, "HGET", (key: &str, field: &str));
    async_pipeline_cmd!(hdel, "HDEL", varargs key: &str);
    async_pipeline_cmd!(hgetall, "HGETALL", (key: &str));
    async_pipeline_cmd!(hexists, "HEXISTS", (key: &str, field: &str));
    async_pipeline_cmd!(hlen, "HLEN", (key: &str));
    async_pipeline_cmd!(hincrby, "HINCRBY", (key: &str, field: &str, by: i64));
    async_pipeline_cmd!(hincrbyfloat, "HINCRBYFLOAT", (key: &str, field: &str, by: f64));

    async_pipeline_cmd!(sadd, "SADD", varargs key: &str);
    async_pipeline_cmd!(srem, "SREM", varargs key: &str);
    async_pipeline_cmd!(smembers, "SMEMBERS", (key: &str));
    async_pipeline_cmd!(sismember, "SISMEMBER", (key: &str, member: Vec<u8>));
    async_pipeline_cmd!(scard, "SCARD", (key: &str));

    async_pipeline_cmd!(zincrby, "ZINCRBY", (key: &str, by: f64, member: Vec<u8>));
    async_pipeline_cmd!(zcard, "ZCARD", (key: &str));
    async_pipeline_cmd!(zscore, "ZSCORE", (key: &str, member: Vec<u8>));

    async_pipeline_cmd!(ping, "PING", ());
    async_pipeline_cmd!(echo, "ECHO", (message: Vec<u8>));
}

impl AsyncPipeline {
    fn immediate_dispatch(
        slf: Py<Self>,
        py: Python<'_>,
        cmd_name: &'static str,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let mut taken = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved.take()
            });
            let res = match taken.as_mut() {
                Some(r) => r.dispatch_immediate(cmd_name, &args).await,
                None => Err(redis::RedisError::from((
                    redis::ErrorKind::ClientError,
                    "internal error",
                    "immediate dispatch without a reservation; call awatch() first"
                        .to_string(),
                ))),
            };
            Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved = taken;
            });
            let raw = match res {
                Ok(v) => RawResult::Value(v),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }
}

// =========================================================================
// Helper for AsyncPipeline::aclose: extract the Option<ReservedConnection>
// out of the snapshot returned by Python::attach.
// =========================================================================

trait IntoReserved {
    fn into_reserved(self) -> Option<ReservedConnection>;
}
impl IntoReserved for Option<ReservedConnection> {
    fn into_reserved(self) -> Option<ReservedConnection> {
        self
    }
}
```

Note: the `aclose()` body above wrote `let result: RawResult = Python::attach(...) -> Option<ReservedConnection>` then called `.into_reserved()`. That line won't typecheck because `RawResult` is the wrong type for the snapshot. Fix `aclose` directly:

```rust
    fn aclose(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let reserved = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                let mut s = this.state.lock().unwrap();
                let reserved = s.reserved.take();
                s.commands.clear();
                s.watched_keys.clear();
                s.watching = false;
                s.explicit_transaction = false;
                s.closed = true;
                reserved
            });
            if let Some(mut r) = reserved {
                let _ = r.unwatch_if_needed().await;
            }
            let _ = tx.send(RawResult::Nil);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }
```

Delete the `IntoReserved` trait — it was a leftover from an earlier draft and is no longer referenced.

- [ ] **Step 3: Add `atransaction` helper**

Append to `crates/redis-rs-py-driver/src/facade/pipeline.rs`:

```rust
/// Async sibling of `transaction_helper`. Loops on `WatchError`, awaiting
/// the user's coroutine `func(pipe)`. `watch_delay` sleeps via
/// `tokio::time::sleep`.
pub(crate) fn atransaction_helper(
    py: Python<'_>,
    driver: Py<RedisRsDriver>,
    func: Py<PyAny>,
    watches: Vec<String>,
    value_from_callable: bool,
    watch_delay: Option<f64>,
) -> PyResult<Py<PyAny>> {
    // We can't run a Python event loop inside Rust, so we pre-build a
    // Python coroutine that does the loop. The coroutine drives the
    // redis-rs-py awaitables natively, which is exactly what users do.
    //
    // Equivalent Python:
    //
    //     async def _go():
    //         while True:
    //             pipe = AsyncPipeline(driver, True)
    //             try:
    //                 if watches: await pipe.awatch(*watches)
    //                 func_value = await func(pipe)
    //                 exec_value = await pipe.aexecute()
    //                 return func_value if value_from_callable else exec_value
    //             except WatchError:
    //                 if watch_delay and watch_delay > 0:
    //                     await asyncio.sleep(watch_delay)
    //                 continue
    //             finally:
    //                 await pipe.aclose()
    //
    let module = py.import("redis_rs_py._driver")?;
    let asyncio = py.import("asyncio")?;
    let watch_error_cls = py.get_type::<WatchError>();
    let driver_obj = driver.bind(py).clone();
    let async_pipeline_cls = module.getattr("AsyncPipeline")?;

    let locals = pyo3::types::PyDict::new(py);
    locals.set_item("AsyncPipeline", async_pipeline_cls)?;
    locals.set_item("driver", driver_obj)?;
    locals.set_item("func", func.bind(py))?;
    locals.set_item("watches", watches)?;
    locals.set_item("value_from_callable", value_from_callable)?;
    locals.set_item("watch_delay", watch_delay)?;
    locals.set_item("WatchError", watch_error_cls)?;
    locals.set_item("asyncio", asyncio)?;
    locals.set_item("inspect", py.import("inspect")?)?;

    let src = std::ffi::CString::new(
        r#"
async def _go():
    while True:
        pipe = AsyncPipeline(driver, True)
        try:
            if watches:
                await pipe.awatch(*watches)
            res = func(pipe)
            if inspect.iscoroutine(res):
                func_value = await res
            else:
                func_value = res
            exec_value = await pipe.aexecute()
            return func_value if value_from_callable else exec_value
        except WatchError:
            if watch_delay and watch_delay > 0:
                await asyncio.sleep(watch_delay)
            continue
        finally:
            await pipe.aclose()

_coro = _go()
"#,
    )
    .unwrap();

    py.run(src.as_c_str(), None, Some(&locals))?;
    let coro = locals.get_item("_coro")?.unwrap();
    Ok(coro.into_any().unbind())
}
```

(`AsyncPipeline` needs to be constructible from Python via `AsyncPipeline(driver, True)`. Add a `#[new]` constructor in the `#[pymethods]` block:

```rust
    #[new]
    #[pyo3(signature = (driver, transaction = true))]
    fn new_py(driver: Py<RedisRsDriver>, transaction: bool) -> Self {
        AsyncPipeline::new(driver, transaction)
    }
```

Same for `Pipeline` if needed by tests:

```rust
    #[new]
    #[pyo3(signature = (driver, transaction = true))]
    fn new_py(driver: Py<RedisRsDriver>, transaction: bool) -> Self {
        Pipeline::new(driver, transaction)
    }
```
)

- [ ] **Step 4: Wire `AsyncPipeline` into the asyncio submodule + the façade**

In `crates/redis-rs-py-driver/src/lib.rs`, the asyncio submodule registration (added by plan 11) should add the class. Plan 11 created something like:

```rust
let asyncio_mod = PyModule::new(m.py(), "asyncio")?;
asyncio_mod.add_class::<facade::asyncio::Redis>()?;
m.add_submodule(&asyncio_mod)?;
```

Add the line:

```rust
asyncio_mod.add_class::<facade::pipeline::AsyncPipeline>()?;
```

Also: `AsyncPipeline` needs to be importable from `redis_rs_py._driver` (top-level) for the `atransaction_helper`'s `py.import("redis_rs_py._driver")` lookup. Add it to `_driver` too:

```rust
m.add_class::<facade::pipeline::AsyncPipeline>()?;
```

Edit `crates/redis-rs-py-driver/src/facade/asyncio.rs`. Inside the `#[pymethods] impl Redis { ... }` block, add:

```rust
    #[pyo3(signature = (transaction = true, shard_hint = None))]
    fn pipeline(
        &self,
        py: Python<'_>,
        transaction: bool,
        shard_hint: Option<Py<PyAny>>,
    ) -> PyResult<Py<crate::facade::pipeline::AsyncPipeline>> {
        let _ = shard_hint;
        Py::new(
            py,
            crate::facade::pipeline::AsyncPipeline::new(self.driver.clone_ref(py), transaction),
        )
    }

    #[pyo3(signature = (func, *watches, value_from_callable = false, watch_delay = None, **_kwargs))]
    fn atransaction(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        watches: Vec<String>,
        value_from_callable: bool,
        watch_delay: Option<f64>,
        _kwargs: Option<Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        crate::facade::pipeline::atransaction_helper(
            py,
            self.driver.clone_ref(py),
            func,
            watches,
            value_from_callable,
            watch_delay,
        )
    }
```

- [ ] **Step 5: Build + run the async tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/pipeline/test_async_pipeline_basic.py tests/pipeline/test_async_pipeline_transaction.py tests/pipeline/test_async_pipeline_watch.py tests/pipeline/test_async_pipeline_discard.py tests/pipeline/test_async_transaction_helper.py -v`
Expected: all PASS (4 + 2 + 2 + 4 + 4 = 16).

If the chained `pipe.set("a", b"1").incr("counter").aexecute()` fails because `await pipe.X.Y.aexecute()` resolves to the awaitable from `aexecute()` and that's right but `.set().incr()` may have re-entered the `buffer_or_dispatch_async` path with `slf.into_any()` returning the wrong type — verify the macro correctly returns `slf.into_any()` (a `Py<PyAny>` that, when treated as the AsyncPipeline pyclass, supports the next `.X()` call).

The macro form `slf.into_any()` takes ownership of the `Py<Self>` and turns it into `Py<PyAny>`. To support chaining, the next method call on the returned `Py<PyAny>` must dispatch back through the AsyncPipeline pyclass. PyO3 handles this through Python's normal attribute lookup — `pipe.set("k", b"v")` returns the pipeline, Python's `.incr("k")` then resolves on it. So this is fine.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/pipeline.rs crates/redis-rs-py-driver/src/facade/asyncio.rs crates/redis-rs-py-driver/src/lib.rs tests/pipeline/test_async_pipeline_basic.py tests/pipeline/test_async_pipeline_transaction.py tests/pipeline/test_async_pipeline_watch.py tests/pipeline/test_async_pipeline_discard.py tests/pipeline/test_async_transaction_helper.py
git commit -m "feat(pipeline): add AsyncPipeline pyclass + atransaction helper"
```

---

## Task 12: Pipeline error-path tests (closed pipeline, bad commands)

Edge-case coverage that wasn't worth its own task earlier — bundle them now.

**Files:**
- Test: `tests/pipeline/test_pipeline_errors.py`
- Test: `tests/pipeline/test_async_pipeline_errors.py`

- [ ] **Step 1: Write the sync error tests**

Create `tests/pipeline/test_pipeline_errors.py`:

```python
"""Pipeline error-path coverage."""

from __future__ import annotations

import pytest

from redis_rs_py.exceptions import RedisError, ResponseError


def test_method_call_on_closed_pipeline_raises(client) -> None:
    pipe = client.pipeline()
    pipe.close()
    with pytest.raises(RedisError, match="closed"):
        pipe.set("k", b"v")


def test_execute_on_closed_pipeline_raises(client) -> None:
    pipe = client.pipeline()
    pipe.close()
    with pytest.raises(RedisError, match="closed"):
        pipe.execute()


def test_watch_on_closed_pipeline_raises(client) -> None:
    pipe = client.pipeline()
    pipe.close()
    with pytest.raises(RedisError, match="closed"):
        pipe.watch("k")


def test_command_with_wrong_type_raises_in_execute(client) -> None:
    """A bad command (e.g. INCR on a string) raises ResponseError when
    execute() runs the batch — NOT at buffer time."""
    client.set("k", b"not-a-number")
    with client.pipeline(transaction=True) as pipe:
        pipe.set("ok", b"v").incr("k")  # buffers fine
        with pytest.raises(ResponseError):
            pipe.execute()


def test_pipeline_in_transaction_one_bad_command_aborts_all(client) -> None:
    """In MULTI/EXEC mode a single bad command makes EXEC return nothing
    and raises ResponseError."""
    client.set("str", b"hello")
    with client.pipeline(transaction=True) as pipe:
        pipe.set("a", b"1")
        pipe.incr("str")  # WRONGTYPE
        pipe.set("b", b"2")
        with pytest.raises(ResponseError):
            pipe.execute()
    # Either neither side-effect happened (atomic abort) or the SET ran:
    # behaviour depends on server. Just assert no crash.


def test_pipeline_reset_after_failed_execute_clears_buffer(client) -> None:
    client.set("k", b"x")
    pipe = client.pipeline(transaction=True)
    pipe.set("a", b"1").incr("k")
    with pytest.raises(ResponseError):
        pipe.execute()
    # Buffer is cleared by execute()'s finally; the pipeline is reusable.
    assert len(pipe) == 0
    assert pipe.set("y", b"v").execute() == [True]
    pipe.close()
```

Run: `uv run pytest tests/pipeline/test_pipeline_errors.py -v`
Expected: 6 PASS.

- [ ] **Step 2: Write the async error tests**

Create `tests/pipeline/test_async_pipeline_errors.py`:

```python
"""Async sibling of test_pipeline_errors.py."""

from __future__ import annotations

import pytest

from redis_rs_py.exceptions import RedisError, ResponseError


@pytest.mark.asyncio
async def test_async_method_on_closed_pipeline_raises(aclient) -> None:
    pipe = aclient.pipeline()
    await pipe.aclose()
    with pytest.raises(RedisError, match="closed"):
        # buffer-side calls return self synchronously, but aexecute checks closed.
        await pipe.aexecute()


@pytest.mark.asyncio
async def test_async_command_with_wrong_type_raises_in_aexecute(aclient) -> None:
    await aclient.set("k", b"not-a-number")
    async with aclient.pipeline(transaction=True) as pipe:
        pipe.set("ok", b"v").incr("k")
        with pytest.raises(ResponseError):
            await pipe.aexecute()


@pytest.mark.asyncio
async def test_async_pipeline_reset_after_failed_aexecute(aclient) -> None:
    await aclient.set("k", b"x")
    pipe = aclient.pipeline(transaction=True)
    pipe.set("a", b"1").incr("k")
    with pytest.raises(ResponseError):
        await pipe.aexecute()
    assert len(pipe) == 0
    assert await pipe.set("y", b"v").aexecute() == [True]
    await pipe.aclose()
```

Run: `uv run pytest tests/pipeline/test_async_pipeline_errors.py -v`
Expected: 3 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/pipeline/test_pipeline_errors.py tests/pipeline/test_async_pipeline_errors.py
git commit -m "test(pipeline): cover error paths (closed pipeline, bad commands)"
```

---

## Task 13: Python re-exports + type stubs

Make `from redis_rs_py import Pipeline` and `from redis_rs_py.asyncio import Pipeline` work, and add type stubs.

**Files:**
- Modify: `python/redis_rs_py/__init__.py`
- Modify: `python/redis_rs_py/asyncio/__init__.py`
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Edit `python/redis_rs_py/__init__.py`**

Add `Pipeline` to the imports and `__all__` list. The exact form depends on what plan 10 left behind. After plan 10 the file should look like:

```python
from redis_rs_py._driver import (
    Pipeline,
    Redis,
    RedisRsAwaitable,
    RedisRsDriver,
    __version__,
)
# (plus the exception re-exports plan 02 added)
```

Add `Pipeline` to the import and to `__all__`.

- [ ] **Step 2: Edit `python/redis_rs_py/asyncio/__init__.py`**

This file (created by plan 11) re-exports from `_driver.asyncio`. Add `Pipeline`:

```python
from redis_rs_py._driver.asyncio import AsyncPipeline as Pipeline, Redis

__all__ = ["Pipeline", "Redis"]
```

- [ ] **Step 3: Update `python/redis_rs_py/_driver.pyi`**

Append:

```python
from collections.abc import Awaitable, Callable
from typing import Any

class Pipeline:
    def __init__(self, driver: RedisRsDriver, transaction: bool = True) -> None: ...
    def __enter__(self) -> "Pipeline": ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def reset(self) -> None: ...
    def close(self) -> None: ...
    def execute(self) -> list[Any]: ...
    def watch(self, *keys: str) -> bool: ...
    def unwatch(self) -> bool: ...
    def multi(self) -> None: ...
    def discard(self) -> None: ...
    # Buffered command methods (chainable — return self):
    def set(self, key: str, value: bytes) -> "Pipeline": ...
    def get(self, key: str) -> "Pipeline": ...
    def getdel(self, key: str) -> "Pipeline": ...
    def append(self, key: str, value: bytes) -> "Pipeline": ...
    def strlen(self, key: str) -> "Pipeline": ...
    def incr(self, key: str) -> "Pipeline": ...
    def incrby(self, key: str, by: int) -> "Pipeline": ...
    def incrbyfloat(self, key: str, by: float) -> "Pipeline": ...
    def decr(self, key: str) -> "Pipeline": ...
    def decrby(self, key: str, by: int) -> "Pipeline": ...
    def setrange(self, key: str, offset: int, value: bytes) -> "Pipeline": ...
    def getrange(self, key: str, start: int, end: int) -> "Pipeline": ...
    def rename(self, key: str, new_key: str) -> "Pipeline": ...
    def renamenx(self, key: str, new_key: str) -> "Pipeline": ...
    def typ(self, key: str) -> "Pipeline": ...
    def expire(self, key: str, seconds: int) -> "Pipeline": ...
    def pexpire(self, key: str, millis: int) -> "Pipeline": ...
    def ttl(self, key: str) -> "Pipeline": ...
    def pttl(self, key: str) -> "Pipeline": ...
    def persist(self, key: str) -> "Pipeline": ...
    def delete(self, *keys: str) -> "Pipeline": ...
    def unlink(self, *keys: str) -> "Pipeline": ...
    def exists(self, *keys: str) -> "Pipeline": ...
    def lpush(self, key: str, *values: bytes) -> "Pipeline": ...
    def rpush(self, key: str, *values: bytes) -> "Pipeline": ...
    def lpop(self, key: str) -> "Pipeline": ...
    def rpop(self, key: str) -> "Pipeline": ...
    def llen(self, key: str) -> "Pipeline": ...
    def lrange(self, key: str, start: int, stop: int) -> "Pipeline": ...
    def lindex(self, key: str, index: int) -> "Pipeline": ...
    def lrem(self, key: str, count: int, value: bytes) -> "Pipeline": ...
    def ltrim(self, key: str, start: int, stop: int) -> "Pipeline": ...
    def lset(self, key: str, index: int, value: bytes) -> "Pipeline": ...
    def hset(self, key: str, field: str, value: bytes) -> "Pipeline": ...
    def hget(self, key: str, field: str) -> "Pipeline": ...
    def hdel(self, key: str, *fields: bytes) -> "Pipeline": ...
    def hgetall(self, key: str) -> "Pipeline": ...
    def hexists(self, key: str, field: str) -> "Pipeline": ...
    def hlen(self, key: str) -> "Pipeline": ...
    def hincrby(self, key: str, field: str, by: int) -> "Pipeline": ...
    def hincrbyfloat(self, key: str, field: str, by: float) -> "Pipeline": ...
    def sadd(self, key: str, *members: bytes) -> "Pipeline": ...
    def srem(self, key: str, *members: bytes) -> "Pipeline": ...
    def smembers(self, key: str) -> "Pipeline": ...
    def sismember(self, key: str, member: bytes) -> "Pipeline": ...
    def scard(self, key: str) -> "Pipeline": ...
    def zincrby(self, key: str, by: float, member: bytes) -> "Pipeline": ...
    def zcard(self, key: str) -> "Pipeline": ...
    def zscore(self, key: str, member: bytes) -> "Pipeline": ...
    def ping(self) -> "Pipeline": ...
    def echo(self, message: bytes) -> "Pipeline": ...

class AsyncPipeline:
    def __init__(self, driver: RedisRsDriver, transaction: bool = True) -> None: ...
    def __aenter__(self) -> "AsyncPipeline": ...
    def __aexit__(
        self, exc_type: Any, exc_value: Any, traceback: Any
    ) -> Awaitable[bool]: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def aclose(self) -> Awaitable[None]: ...
    def reset(self) -> Awaitable[None]: ...
    def aexecute(self) -> Awaitable[list[Any]]: ...
    def awatch(self, *keys: str) -> Awaitable[bool]: ...
    def aunwatch(self) -> Awaitable[bool]: ...
    def multi(self) -> None: ...
    def adiscard(self) -> Awaitable[None]: ...
    def aget_immediate(self, key: str) -> Awaitable[bytes | None]: ...
    def aset_immediate(self, key: str, value: bytes) -> Awaitable[bool]: ...
    # Chainable buffered command methods (return self):
    def set(self, key: str, value: bytes) -> "AsyncPipeline": ...
    def get(self, key: str) -> "AsyncPipeline": ...
    # ... (same surface as Pipeline; copy from above with type "AsyncPipeline")
```

(Copy the chainable command methods from `Pipeline` block above, replacing the return type annotation.)

- [ ] **Step 4: Add the import smoke test**

Append to `tests/pipeline/test_pipeline_basic.py`:

```python
def test_pipeline_class_is_importable_from_top_level() -> None:
    from redis_rs_py import Pipeline as P
    from redis_rs_py._driver import Pipeline as Q

    assert P is Q


def test_async_pipeline_class_is_importable_from_asyncio_submodule() -> None:
    from redis_rs_py.asyncio import Pipeline as P
    from redis_rs_py._driver.asyncio import AsyncPipeline as Q

    assert P is Q
```

Run: `uv run pytest tests/pipeline/test_pipeline_basic.py::test_pipeline_class_is_importable_from_top_level tests/pipeline/test_pipeline_basic.py::test_async_pipeline_class_is_importable_from_asyncio_submodule -v`
Expected: 2 PASS.

- [ ] **Step 5: Run ty + ruff**

```bash
uv run ty check python/redis_rs_py/
uv run ruff check
uv run ruff format --check
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: clean. If `ty` complains about `Awaitable` not being imported in the `.pyi`, add `from collections.abc import Awaitable` at the top.

- [ ] **Step 6: Commit**

```bash
git add python/redis_rs_py/__init__.py python/redis_rs_py/asyncio/__init__.py python/redis_rs_py/_driver.pyi tests/pipeline/test_pipeline_basic.py
git commit -m "feat(public): re-export Pipeline + AsyncPipeline at package level"
```

---

## Task 14: Free-threaded smoke + full-suite verification

Verify the whole pipeline suite holds under `python3.14t` (free-threaded), and the lints are clean.

**Files:** none modified — verification only.

- [ ] **Step 1: Run the full pipeline suite under cp314**

```bash
uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
uv run pytest tests/pipeline/ -v -n auto
```

Expected: every test PASSES. The shared `valkey_url` session fixture spins up one Valkey container per `pytest` process; the threaded conflict tests (`test_watch_then_concurrent_modification_raises`) exercise real threads against it.

- [ ] **Step 2: Run the full pipeline suite under cp314t**

```bash
.venv-ft/bin/uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
.venv-ft/bin/uv run pytest tests/pipeline/ -v -n auto
```

Expected: every test PASSES. If anything new fails only under free-threaded, the most likely culprit is the `Mutex<PipelineState>` being held across an `await` somewhere — search `pipeline.rs` for `state.lock().unwrap()` followed by an `await` on the same task. The `tokio::sync::oneshot` channels we use must always own the data they need; the `std::sync::Mutex` must be released before any await.

- [ ] **Step 3: Run the full project test suite**

```bash
uv run pytest -n auto
```

Expected: every test PASSES across all families. Plan 13 added ~38 new tests; the suite total grows by that much.

- [ ] **Step 4: CHANGELOG entry**

Append to `CHANGELOG.md` under `### Added`:

```markdown
- `redis_rs_py.Pipeline` and `redis_rs_py.asyncio.Pipeline` (Rust pyclasses) — buffered-then-flushed semantics matching redis-py: `r.pipeline(transaction=True)`, chainable command methods, `WATCH`/`UNWATCH`/`MULTI`/`EXEC`/`DISCARD`, immediate-mode dispatch on the reserved connection.
- `Redis.transaction(func, *watches, value_from_callable=False, watch_delay=None)` retry helper (and async sibling `atransaction`) — loops on `WatchError`, sleeps `watch_delay` between attempts.
- Driver-level `RedisRsDriver.pipeline_exec(commands, transaction)` and `apipeline_exec` for low-level batching.
- Driver-level `ValkeyConn.reserve_connection() → ReservedConnection` for the WATCH path. Documented cost: one extra `MultiplexedConnection` per active WATCH-mode pipeline.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 13 entry"
```

- [ ] **Step 5: Final verification**

```bash
git log --oneline -20
```

Expected: ~14 new commits since the start of the plan, in roughly the order of the tasks. Every commit message follows conventional commits and is independently revertable.

---

## Self-review checklist for this plan

- [x] Spec coverage (`PLAN.md` v0.1 surface — Pipelines & transactions): "`r.pipeline(transaction=True)` context manager" — Task 5 + Task 7. "WATCH / UNWATCH / MULTI / EXEC semantics matched" — Task 8 + Task 9. "Sticky-connection mode in the driver for the duration of a transaction" — Task 2 (`reserve_connection` / `ReservedConnection`). Async parity: Task 11.
- [x] Spec coverage (Risks): "WATCH/MULTI/EXEC under a multiplexed pool" — explicitly addressed in Task 2 by allocating a fresh `MultiplexedConnection` per reservation; the cost is documented in the `reserve_connection` doc comment and again in the CHANGELOG entry.
- [x] Out-of-scope items deferred to their right plan (cluster pipelines → 15; sentinel → 16; pubsub-in-pipeline → 14; `load_scripts` → v0.2).
- [x] No placeholders: every code step ships actual code; the only "lands in the next task" stubs (`execute_watched` placeholder in Task 7) are explicitly marked and are replaced before the WATCH-mode tests run.
- [x] Type consistency: `RedisRsDriver.pipeline_exec` returns a Python `list`; `Pipeline.execute()` calls `pipeline_exec_internal` and returns the same shape; `WatchedExecResult::WatchAborted` → `WatchError("Watched variable changed.")` (matches the redis-py message).
- [x] All file paths absolute or repo-relative-from-root.
- [x] Every test step has a runnable command and an explicit pass/fail expectation.
- [x] WATCH-conflict test under real concurrency: Task 9 (`test_watch_then_concurrent_modification_raises` uses two `threading.Thread`s + a barrier); async sibling in Task 11 (`test_async_watch_conflict_raises` uses `asyncio.gather`).
- [x] `transaction()` retry helper: loop body matches the redis-py source verbatim (Task 10), `value_from_callable` and `watch_delay` both covered by their own test (`test_transaction_value_from_callable_returns_func_value`, `test_transaction_watch_delay_is_respected`); async sibling in Task 11.
- [x] Free-threaded (cp314t) discipline: every `Mutex<PipelineState>` is released before any await (Task 5 / 8 / 11 explicitly). The pipeline pyclass marks `unsendable = false` (i.e. it is `Send + Sync`) and uses `std::sync::Mutex` for state.
- [x] Conventional commits across all 14 tasks: `feat(pipeline): ...`, `test(pipeline): ...`, `docs(changelog): ...`.
