# Plan 11 — Asyncio façade: `redis_rs_py.asyncio.Redis`

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the asyncio façade — `redis_rs_py.asyncio.Redis` — entirely in Rust as a `#[pyclass]`. Mirrors the redis-py asyncio module: same constructor as the sync façade (plan 10), same method names (no `a`-prefix at the façade layer — the prefix is a driver convention only), but every command returns a `RedisRsAwaitable` produced by the driver's `a<command>` method. `aclose`, `__aenter__`, `__aexit__` round out the lifecycle. The class is exposed via `_driver.asyncio.Redis` (a true PyO3 submodule) and re-exported from the Python `redis_rs_py.asyncio` package.

**Architecture:** Same shape as plan 10 — `Redis` is a `#[pyclass(subclass)]` holding an `Arc<RedisRsDriver>`. Constructor is byte-identical (same kwargs, same warn-once flow); we *re-use* the `FacadeConfig` struct and the URL parser from `facade::sync` rather than duplicating them. A new macro `define_async_facade_command!` mirrors `define_facade_command!` but calls `a<driver_method>` and returns the awaitable directly. PyO3 0.28 submodule registration uses `PyModule::new(py, "asyncio")` + `parent.add_submodule(&m)` (documented at https://pyo3.rs/v0.28.0/module.html#python-submodules).

**Tech Stack:** PyO3 0.28 (`PyModule::new`, `add_submodule`, `#[pyclass(subclass)]`), no new Rust deps. Python: `pytest-asyncio` for the test suite; no other Python deps.

**Reference material:**
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/10-facade-sync.md` — sister plan; this plan re-uses `FacadeConfig`, `parse_url`, `IMPLEMENTED_KWARGS`, and the warn-once helper. Read it first.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/01-foundation-async-bridge.md` — `RedisRsAwaitable` (the return type of every façade async method) and the driver's `a<command>` naming convention.
- `python -c "import redis.asyncio, inspect; print(inspect.signature(redis.asyncio.Redis.__init__))"` — should match the sync signature exactly modulo deprecation warnings; sanity-check it before starting.
- PyO3 0.28 submodule pattern: `let m = PyModule::new(py, "asyncio")?; m.add_class::<Redis>()?; parent.add_submodule(&m)?;` then on the Python side `sys.modules["redis_rs_py._driver.asyncio"] = _driver.asyncio` is required for `import redis_rs_py._driver.asyncio` to work — the submodule needs to be registered in `sys.modules` because PyO3 doesn't auto-register submodules. Documented at https://pyo3.rs/v0.28.0/module.html#python-submodules.

**Out of scope for this plan:**
- Async pipeline / pubsub / transaction — `pipeline()` / `pubsub()` raise `NotImplementedError` pointing at plans 13 / 14, like the sync façade does.
- `decode_responses=True` — plan 12 wires the asyncio decoder; this plan stores the flag, ignores it on output.
- `aclose()` cancellation propagation beyond what `RedisRsAwaitable` already does — plan 01 covers awaitable cancellation; this plan doesn't extend that contract.

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  facade/
    mod.rs                      # MODIFIED: add `pub mod asyncio_mod;`
    asyncio_mod.rs              # NEW: the asyncio.Redis pyclass
  lib.rs                        # MODIFIED: register the `asyncio` submodule
python/redis_rs_py/
  asyncio/
    __init__.py                 # NEW: re-exports from _driver.asyncio
  __init__.py                   # MODIFIED: trigger submodule registration in sys.modules
  _driver.pyi                   # MODIFIED: stub for the asyncio submodule
tests/facade/
  test_asyncio_constructor.py   # NEW
  test_asyncio_from_url.py      # NEW
  test_asyncio_commands_smoke.py # NEW: every async method exists and round-trips
  test_asyncio_close.py         # NEW: aclose + async context manager
```

---

## Task 1: Submodule plumbing — `_driver.asyncio` registration

PyO3 0.28's submodule registration is two-step: build the module, register on the parent, then *also* set `sys.modules["parent._driver.asyncio"]` from Python so `import redis_rs_py._driver.asyncio` resolves. We do the `sys.modules` insert from `_driver`'s `#[pymodule]` body so consumers don't have to think about it.

**Files:**
- New: `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs` (placeholder)
- Modify: `crates/redis-rs-py-driver/src/facade/mod.rs`
- Modify: `crates/redis-rs-py-driver/src/lib.rs`

- [ ] **Step 1: Create the placeholder file**

```bash
printf '// placeholder — populated by Plan 11 Task 2\n' > crates/redis-rs-py-driver/src/facade/asyncio_mod.rs
```

- [ ] **Step 2: Add the module declaration to `facade/mod.rs`**

Edit `crates/redis-rs-py-driver/src/facade/mod.rs`:

```rust
// Façade module — declares submodules implemented across plans 10-12.

pub mod asyncio_mod;
pub mod kwargs;
pub mod sync;
```

- [ ] **Step 3: Register the asyncio submodule in `lib.rs`**

Edit `crates/redis-rs-py-driver/src/lib.rs`. Inside `fn _driver(m: &Bound<'_, PyModule>) -> PyResult<()>`, after the existing `m.add_class::<facade::sync::Redis>()?;` registration, append:

```rust
    // asyncio submodule — registered both as a PyO3 submodule and into
    // sys.modules so `import redis_rs_py._driver.asyncio` resolves.
    let asyncio_mod = PyModule::new(m.py(), "asyncio")?;
    facade::asyncio_mod::register(m.py(), &asyncio_mod)?;
    m.add_submodule(&asyncio_mod)?;

    // PyO3 0.28: submodules are NOT auto-added to sys.modules. Do it
    // manually so `from redis_rs_py._driver.asyncio import Redis` and
    // dotted import paths work.
    let sys = m.py().import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("redis_rs_py._driver.asyncio", &asyncio_mod)?;
```

(`facade::asyncio_mod::register` doesn't exist yet — the next task lands it. Comment-out the call line until then so we can verify the build progress.)

For now, comment the body out:

```rust
    // let asyncio_mod = PyModule::new(m.py(), "asyncio")?;
    // facade::asyncio_mod::register(m.py(), &asyncio_mod)?;
    // m.add_submodule(&asyncio_mod)?;
    // let sys = m.py().import("sys")?;
    // let modules = sys.getattr("modules")?;
    // modules.set_item("redis_rs_py._driver.asyncio", &asyncio_mod)?;
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings only.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/mod.rs crates/redis-rs-py-driver/src/facade/asyncio_mod.rs crates/redis-rs-py-driver/src/lib.rs
git commit -m "feat(asyncio): scaffold _driver.asyncio submodule"
```

---

## Task 2: `asyncio_mod.rs` — `Redis` pyclass + `define_async_facade_command!` macro

This task delivers everything from plan 10's Tasks 3 + 4 in one go: the `Redis` pyclass, constructor, `from_url`, lifecycle, the `define_async_facade_command!` macro, and the *first* command-family block (strings) as a working proof. Subsequent tasks (3-7) are mechanical macro pastes for the remaining families.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs`
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (uncomment registration)
- New: `python/redis_rs_py/asyncio/__init__.py`
- Modify: `python/redis_rs_py/__init__.py` (force-import the asyncio submodule)
- Test: `tests/facade/test_asyncio_constructor.py`, `test_asyncio_from_url.py`, `test_asyncio_close.py`, `test_asyncio_commands_smoke.py` (strings section)

- [ ] **Step 1: Write the failing constructor test**

`tests/facade/test_asyncio_constructor.py`:

```python
"""Constructor surface for redis_rs_py.asyncio.Redis."""

from __future__ import annotations

import pytest

from redis_rs_py.asyncio import Redis


@pytest.mark.asyncio
async def test_default_constructor_accepts_no_kwargs(valkey_url: str) -> None:
    from urllib.parse import urlparse

    parts = urlparse(valkey_url)
    r = Redis(host=parts.hostname, port=parts.port)
    assert await r.ping() is True
    await r.aclose()


@pytest.mark.asyncio
async def test_constructor_accepts_full_redis_py_kwarg_surface() -> None:
    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError

    with pytest.raises(RedisConnectionError):
        Redis(
            host="127.0.0.1",
            port=1,
            db=0,
            password=None,
            socket_timeout=None,
            socket_connect_timeout=None,
            socket_keepalive=False,
            socket_keepalive_options=None,
            connection_pool=None,
            unix_socket_path=None,
            encoding="utf-8",
            encoding_errors="strict",
            charset=None,
            errors=None,
            decode_responses=False,
            retry_on_timeout=False,
            retry_on_error=None,
            ssl=False,
            ssl_keyfile=None,
            ssl_certfile=None,
            ssl_cert_reqs="required",
            ssl_ca_certs=None,
            ssl_ca_path=None,
            ssl_ca_data=None,
            ssl_check_hostname=False,
            ssl_password=None,
            ssl_validate_ocsp=False,
            ssl_validate_ocsp_stapled=False,
            ssl_ocsp_context=None,
            ssl_ocsp_expected_cert=None,
            ssl_min_version=None,
            ssl_ciphers=None,
            max_connections=None,
            single_connection_client=False,
            health_check_interval=0,
            client_name=None,
            lib_name="redis-rs-py",
            lib_version="0.0.0",
            username=None,
            retry=None,
            redis_connect_func=None,
            credential_provider=None,
            protocol=2,
            cache=None,
            cache_config=None,
            event_dispatcher=None,
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/facade/test_asyncio_constructor.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'redis_rs_py.asyncio'`.

- [ ] **Step 3: Implement `asyncio_mod.rs`**

Replace `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs`:

```rust
// Asyncio façade: redis_rs_py.asyncio.Redis.
//
// Mirrors `redis.asyncio.Redis` — same constructor as the sync façade
// (plan 10), every command method returns a RedisRsAwaitable bridged
// from the driver's a<command> method. Re-uses FacadeConfig +
// parse_url + IMPLEMENTED_KWARGS from `facade::sync` so the kwarg
// surface, URL grammar, and warn-once dedup state are shared with
// the sync façade.

#![allow(clippy::too_many_arguments)]

use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};
use std::sync::Arc;

use crate::driver::RedisRsDriver;
use crate::facade::kwargs::{IMPLEMENTED_KWARGS, accept_and_warn};
use crate::facade::sync::FacadeConfig;

/// Re-export of the sync façade's URL parser. We don't make it pub on
/// `facade::sync` to keep its surface narrow; instead we expose a local
/// thin wrapper that does `super::sync::parse_url` via a free function
/// in `sync.rs`. Plan 10 marks `parse_url` as `pub(crate)` so we can
/// reach it here.
fn build_driver_for_config(py: Python<'_>, cfg: &FacadeConfig) -> PyResult<Py<RedisRsDriver>> {
    crate::facade::sync::build_driver(py, cfg)
}

#[pyclass(subclass, module = "redis_rs_py._driver.asyncio", name = "Redis")]
pub struct Redis {
    pub(crate) driver: Option<Arc<Py<RedisRsDriver>>>,
    pub(crate) config: FacadeConfig,
}

impl Redis {
    fn driver_or_raise(&self) -> PyResult<Arc<Py<RedisRsDriver>>> {
        match &self.driver {
            Some(d) => Ok(d.clone()),
            None => Err(PyValueError::new_err(
                "Redis client is closed; create a new one or use an async context manager",
            )),
        }
    }
}

#[pymethods]
impl Redis {
    #[new]
    #[pyo3(signature = (
        host = "localhost".to_string(),
        port = 6379,
        db = 0,
        password = None,
        socket_timeout = None,
        socket_connect_timeout = None,
        socket_keepalive = false,
        socket_keepalive_options = None,
        connection_pool = None,
        unix_socket_path = None,
        encoding = "utf-8".to_string(),
        encoding_errors = "strict".to_string(),
        charset = None,
        errors = None,
        decode_responses = false,
        retry_on_timeout = false,
        retry_on_error = None,
        ssl = false,
        ssl_keyfile = None,
        ssl_certfile = None,
        ssl_cert_reqs = "required".to_string(),
        ssl_ca_certs = None,
        ssl_ca_path = None,
        ssl_ca_data = None,
        ssl_check_hostname = false,
        ssl_password = None,
        ssl_validate_ocsp = false,
        ssl_validate_ocsp_stapled = false,
        ssl_ocsp_context = None,
        ssl_ocsp_expected_cert = None,
        ssl_min_version = None,
        ssl_ciphers = None,
        max_connections = None,
        single_connection_client = false,
        health_check_interval = 0,
        client_name = None,
        lib_name = None,
        lib_version = None,
        username = None,
        retry = None,
        redis_connect_func = None,
        credential_provider = None,
        protocol = 2,
        cache = None,
        cache_config = None,
        event_dispatcher = None,
        **extra
    ))]
    fn new(
        py: Python<'_>,
        host: String,
        port: u16,
        db: i64,
        password: Option<String>,
        socket_timeout: Option<f64>,
        socket_connect_timeout: Option<Py<PyAny>>,
        socket_keepalive: bool,
        socket_keepalive_options: Option<Py<PyAny>>,
        connection_pool: Option<Py<PyAny>>,
        unix_socket_path: Option<Py<PyAny>>,
        encoding: String,
        encoding_errors: String,
        charset: Option<Py<PyAny>>,
        errors: Option<Py<PyAny>>,
        decode_responses: bool,
        retry_on_timeout: bool,
        retry_on_error: Option<Py<PyAny>>,
        ssl: bool,
        ssl_keyfile: Option<String>,
        ssl_certfile: Option<String>,
        ssl_cert_reqs: String,
        ssl_ca_certs: Option<String>,
        ssl_ca_path: Option<Py<PyAny>>,
        ssl_ca_data: Option<Py<PyAny>>,
        ssl_check_hostname: bool,
        ssl_password: Option<Py<PyAny>>,
        ssl_validate_ocsp: bool,
        ssl_validate_ocsp_stapled: bool,
        ssl_ocsp_context: Option<Py<PyAny>>,
        ssl_ocsp_expected_cert: Option<Py<PyAny>>,
        ssl_min_version: Option<Py<PyAny>>,
        ssl_ciphers: Option<Py<PyAny>>,
        max_connections: Option<usize>,
        single_connection_client: bool,
        health_check_interval: u64,
        client_name: Option<String>,
        lib_name: Option<String>,
        lib_version: Option<String>,
        username: Option<String>,
        retry: Option<Py<PyAny>>,
        redis_connect_func: Option<Py<PyAny>>,
        credential_provider: Option<Py<PyAny>>,
        protocol: i64,
        cache: Option<Py<PyAny>>,
        cache_config: Option<Py<PyAny>>,
        event_dispatcher: Option<Py<PyAny>>,
        extra: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let _ = (
            socket_connect_timeout,
            socket_keepalive,
            socket_keepalive_options,
            unix_socket_path,
            charset,
            errors,
            retry_on_timeout,
            retry_on_error,
            ssl_cert_reqs,
            ssl_ca_path,
            ssl_ca_data,
            ssl_check_hostname,
            ssl_password,
            ssl_validate_ocsp,
            ssl_validate_ocsp_stapled,
            ssl_ocsp_context,
            ssl_ocsp_expected_cert,
            ssl_min_version,
            ssl_ciphers,
            single_connection_client,
            lib_name,
            lib_version,
            retry,
            redis_connect_func,
            credential_provider,
            cache,
            cache_config,
            event_dispatcher,
            connection_pool,
        );
        accept_and_warn(py, IMPLEMENTED_KWARGS, extra.as_ref())?;

        let config = FacadeConfig {
            host,
            port,
            db,
            password,
            username,
            ssl,
            ssl_keyfile,
            ssl_certfile,
            ssl_ca_certs,
            socket_timeout,
            max_connections,
            health_check_interval,
            client_name,
            protocol,
            decode_responses,
            encoding,
            encoding_errors,
        };

        let driver = build_driver_for_config(py, &config)?;
        Ok(Self {
            driver: Some(Arc::new(driver)),
            config,
        })
    }

    #[classmethod]
    #[pyo3(signature = (url, **kwargs))]
    fn from_url(
        cls: &Bound<'_, PyType>,
        py: Python<'_>,
        url: String,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        // Parse via the sync façade's exported helper.
        let url_cfg = crate::facade::sync::parse_url(&url)?;
        let merged = match kwargs {
            Some(d) => d,
            None => PyDict::new(py),
        };
        merged.set_item("host", url_cfg.host)?;
        merged.set_item("port", url_cfg.port)?;
        merged.set_item("db", url_cfg.db)?;
        if let Some(p) = url_cfg.password {
            merged.set_item("password", p)?;
        }
        if let Some(u) = url_cfg.username {
            merged.set_item("username", u)?;
        }
        if url_cfg.ssl {
            merged.set_item("ssl", true)?;
        }
        let empty = PyTuple::empty(py);
        cls.call(empty, Some(&merged)).map(Bound::unbind)
    }

    // --- lifecycle --------------------------------------------------------

    fn aclose<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.driver = None;
        // Return a no-op coroutine so `await client.aclose()` works.
        let asyncio = py.import("asyncio")?;
        let empty: Py<PyAny> = py.None();
        asyncio.call_method1("sleep", (0.0_f64, empty))
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let asyncio = py.import("asyncio")?;
        let coro = asyncio.call_method1("sleep", (0.0_f64, slf.into_pyobject(py)?))?;
        Ok(coro)
    }

    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    fn __aexit__<'py>(
        &mut self,
        py: Python<'py>,
        exc_type: Option<Py<PyAny>>,
        exc_val: Option<Py<PyAny>>,
        exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = (exc_type, exc_val, exc_tb);
        self.driver = None;
        let asyncio = py.import("asyncio")?;
        let empty: Py<PyAny> = false.into_pyobject(py)?.to_owned().into_any().unbind();
        asyncio.call_method1("sleep", (0.0_f64, empty))
    }

    // --- placeholders -----------------------------------------------------

    #[pyo3(signature = (transaction = true, shard_hint = None))]
    fn pipeline(
        &self,
        transaction: bool,
        shard_hint: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (transaction, shard_hint);
        Err(PyNotImplementedError::new_err(
            "Async Pipeline is implemented by plan 13.",
        ))
    }

    #[pyo3(signature = (**kwargs))]
    fn pubsub(&self, kwargs: Option<Bound<'_, PyDict>>) -> PyResult<Py<PyAny>> {
        let _ = kwargs;
        Err(PyNotImplementedError::new_err(
            "Async PubSub is implemented by plan 14.",
        ))
    }

    #[pyo3(signature = (func, *watches, value_from_callable = false, watch_delay = None, **kwargs))]
    fn transaction(
        &self,
        func: Py<PyAny>,
        watches: &Bound<'_, PyTuple>,
        value_from_callable: bool,
        watch_delay: Option<f64>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (func, watches, value_from_callable, watch_delay, kwargs);
        Err(PyNotImplementedError::new_err(
            "Async transaction() is implemented by plan 13.",
        ))
    }
}

// =========================================================================
// define_async_facade_command! — emit a one-line method delegating to
// the driver's a<method> by Python-level dispatch.
// =========================================================================

#[macro_export]
macro_rules! define_async_facade_command {
    (
        $method:ident,
        ( $( $arg:ident : $ty:ty $(= $default:expr)? ),* $(,)? ),
        $driver_method:ident
    ) => {
        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = ( $( $arg $(= $default)? ),* ))]
        fn $method(
            &self,
            py: Python<'_>,
            $( $arg : $ty ),*
        ) -> PyResult<Py<PyAny>> {
            let drv = self.driver_or_raise()?;
            let bound = drv.bind(py);
            let args = ( $( $arg, )* );
            Ok(bound
                .call_method1(stringify!($driver_method), args)?
                .unbind())
        }
    };

    (
        $method:ident,
        ( $( $arg:ident : $ty:ty $(= $default:expr)? ),* $(,)? ),
        $driver_method:ident,
        [ $( $forward:ident ),* $(,)? ]
    ) => {
        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = ( $( $arg $(= $default)? ),* ))]
        fn $method(
            &self,
            py: Python<'_>,
            $( $arg : $ty ),*
        ) -> PyResult<Py<PyAny>> {
            let drv = self.driver_or_raise()?;
            let bound = drv.bind(py);
            let args = ( $( $forward, )* );
            Ok(bound
                .call_method1(stringify!($driver_method), args)?
                .unbind())
        }
    };
}

// =========================================================================
// String commands — plan 03 surface, async variants.
// =========================================================================

#[pymethods]
impl Redis {
    define_async_facade_command!(get, (key: String), aget);

    #[pyo3(signature = (
        key, value,
        ex = None, px = None,
        nx = false, xx = false, keepttl = false, get = false,
        exat = None, pxat = None,
    ))]
    fn set(
        &self,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
        ex: Option<i64>,
        px: Option<i64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
        exat: Option<i64>,
        pxat: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        let kwargs = PyDict::new(py);
        if let Some(v) = ex {
            kwargs.set_item("ex", v)?;
        }
        if let Some(v) = px {
            kwargs.set_item("px", v)?;
        }
        if nx {
            kwargs.set_item("nx", true)?;
        }
        if xx {
            kwargs.set_item("xx", true)?;
        }
        if keepttl {
            kwargs.set_item("keepttl", true)?;
        }
        if get {
            kwargs.set_item("get", true)?;
        }
        if let Some(v) = exat {
            kwargs.set_item("exat", v)?;
        }
        if let Some(v) = pxat {
            kwargs.set_item("pxat", v)?;
        }
        Ok(bound
            .call_method("aset", (key, value), Some(&kwargs))?
            .unbind())
    }

    define_async_facade_command!(getex, (key: String, ex: Option<i64> = None, px: Option<i64> = None, exat: Option<i64> = None, pxat: Option<i64> = None, persist: bool = false), agetex);
    define_async_facade_command!(getdel, (key: String), agetdel);
    define_async_facade_command!(copy, (source: String, destination: String, db: Option<i64> = None, replace: bool = false), acopy);
    define_async_facade_command!(incr, (key: String, amount: i64 = 1), aincr_by);
    define_async_facade_command!(incrby, (key: String, amount: i64 = 1), aincr_by);
    define_async_facade_command!(incrbyfloat, (key: String, amount: f64 = 1.0), aincr_by_float);
    define_async_facade_command!(decr, (key: String, amount: i64 = 1), adecr_by);
    define_async_facade_command!(decrby, (key: String, amount: i64 = 1), adecr_by);
    define_async_facade_command!(append, (key: String, value: Vec<u8>), aappend);
    define_async_facade_command!(strlen, (key: String), astrlen);

    #[pyo3(signature = (*keys))]
    fn mget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("amget", (keys,))?.unbind())
    }

    fn mset(&self, py: Python<'_>, mapping: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("amset", (mapping,))?.unbind())
    }

    fn msetnx(&self, py: Python<'_>, mapping: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("amsetnx", (mapping,))?.unbind())
    }

    define_async_facade_command!(setrange, (key: String, offset: i64, value: Vec<u8>), asetrange);
    define_async_facade_command!(getrange, (key: String, start: i64, end: i64), agetrange);

    #[pyo3(signature = (*keys))]
    fn exists(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("aexists", (keys,))?.unbind())
    }

    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("adelete", (keys,))?.unbind())
    }

    #[pyo3(signature = (*keys))]
    fn unlink(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("aunlink", (keys,))?.unbind())
    }

    define_async_facade_command!(expire, (key: String, time: i64, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), aexpire);
    define_async_facade_command!(pexpire, (key: String, time: i64, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), apexpire);
    define_async_facade_command!(expireat, (key: String, when: i64, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), aexpireat);
    define_async_facade_command!(pexpireat, (key: String, when: i64, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), apexpireat);
    define_async_facade_command!(expiretime, (key: String), aexpiretime);
    define_async_facade_command!(pexpiretime, (key: String), apexpiretime);
    define_async_facade_command!(ttl, (key: String), attl);
    define_async_facade_command!(pttl, (key: String), apttl);
    define_async_facade_command!(persist, (key: String), apersist);
    define_async_facade_command!(rename, (src: String, dst: String), arename);
    define_async_facade_command!(renamenx, (src: String, dst: String), arenamenx);
    define_async_facade_command!(type, (key: String), atype, [key]);
    define_async_facade_command!(dump, (key: String), adump);
    define_async_facade_command!(restore, (key: String, ttl: i64, value: Vec<u8>, replace: bool = false, absttl: bool = false, idletime: Option<i64> = None, freq: Option<i64> = None), arestore);

    define_async_facade_command!(ping, (), aping);
}

// =========================================================================
// Submodule registration entry point. Called by lib.rs when building
// the asyncio submodule.
// =========================================================================

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Redis>()?;
    Ok(())
}
```

- [ ] **Step 4: Make `parse_url` and `build_driver` reachable**

Plan 10 declared `parse_url` and `build_driver` as plain free functions (not `pub`). Update `crates/redis-rs-py-driver/src/facade/sync.rs` to make them `pub(crate)`:

```rust
pub(crate) fn parse_url(input: &str) -> PyResult<UrlConfig> { /* ... */ }
pub(crate) fn build_driver(py: Python<'_>, cfg: &FacadeConfig) -> PyResult<Py<RedisRsDriver>> { /* ... */ }
```

Also make `UrlConfig` `pub(crate)` and its fields accessible (mark each field `pub(crate)`).

- [ ] **Step 5: Re-enable the registration in `lib.rs`**

Uncomment the asyncio block from Task 1 Step 3:

```rust
    let asyncio_mod = PyModule::new(m.py(), "asyncio")?;
    facade::asyncio_mod::register(m.py(), &asyncio_mod)?;
    m.add_submodule(&asyncio_mod)?;

    let sys = m.py().import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("redis_rs_py._driver.asyncio", &asyncio_mod)?;
```

- [ ] **Step 6: Create the Python re-export package**

`python/redis_rs_py/asyncio/__init__.py`:

```python
"""Asyncio façade — `redis_rs_py.asyncio.Redis`.

Mirrors `redis.asyncio` from upstream redis-py: same constructor
surface as the sync façade (`redis_rs_py.Redis`), every method returns
an awaitable.
"""

# Force the parent _driver to load — this is what registers the
# `redis_rs_py._driver.asyncio` submodule in `sys.modules`.
import redis_rs_py._driver  # noqa: F401
from redis_rs_py._driver.asyncio import Redis

__all__ = ["Redis"]
```

- [ ] **Step 7: Run the constructor tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/facade/test_asyncio_constructor.py -v`
Expected: 2 PASS.

- [ ] **Step 8: Add the `from_url` test**

`tests/facade/test_asyncio_from_url.py`:

```python
"""asyncio.Redis.from_url URL parsing."""

from __future__ import annotations

import pytest

from redis_rs_py.asyncio import Redis
from redis_rs_py.exceptions import ConnectionError as RedisConnectionError


@pytest.mark.asyncio
async def test_from_url_basic(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    assert await r.ping() is True
    await r.aclose()


@pytest.mark.asyncio
async def test_from_url_with_db_in_query(valkey_url: str) -> None:
    base = valkey_url.split("?", 1)[0]
    r = Redis.from_url(f"{base}?db=2")
    assert await r.ping() is True
    await r.aclose()


@pytest.mark.asyncio
async def test_from_url_with_userinfo() -> None:
    with pytest.raises(RedisConnectionError):
        Redis.from_url("redis://default:secret@127.0.0.1:1/0")


@pytest.mark.asyncio
async def test_from_url_invalid_scheme_raises_value_error() -> None:
    with pytest.raises(ValueError, match="scheme"):
        Redis.from_url("http://127.0.0.1:6379/0")


@pytest.mark.asyncio
async def test_from_url_kwargs_lower_precedence(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url, host="impossible.invalid", port=1)
    assert await r.ping() is True
    await r.aclose()
```

Run: `uv run pytest tests/facade/test_asyncio_from_url.py -v`
Expected: 5 PASS.

- [ ] **Step 9: Add the close + async-context-manager test**

`tests/facade/test_asyncio_close.py`:

```python
"""Async lifecycle: aclose + __aenter__/__aexit__."""

from __future__ import annotations

import pytest

from redis_rs_py.asyncio import Redis


@pytest.mark.asyncio
async def test_aclose_drops_driver(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    assert await r.ping() is True
    await r.aclose()
    with pytest.raises(ValueError, match="closed"):
        await r.ping()


@pytest.mark.asyncio
async def test_async_context_manager(valkey_url: str) -> None:
    async with Redis.from_url(valkey_url) as r:
        assert await r.ping() is True
    with pytest.raises(ValueError, match="closed"):
        await r.ping()


@pytest.mark.asyncio
async def test_double_aclose_is_idempotent(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    await r.aclose()
    await r.aclose()
```

Run: `uv run pytest tests/facade/test_asyncio_close.py -v`
Expected: 3 PASS.

- [ ] **Step 10: Add the strings smoke test**

`tests/facade/test_asyncio_commands_smoke.py`:

```python
"""Smoke tests for every asyncio façade command method."""

from __future__ import annotations

import pytest

from redis_rs_py.asyncio import Redis


@pytest.fixture
async def r(valkey_url: str):
    import redis as upstream

    rp = upstream.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    client = Redis.from_url(valkey_url)
    yield client
    await client.aclose()


# --- strings --------------------------------------------------------------


@pytest.mark.asyncio
async def test_string_get_set(r: Redis) -> None:
    await r.set("k", b"v")
    assert await r.get("k") == b"v"


@pytest.mark.asyncio
async def test_string_get_set_with_ex(r: Redis) -> None:
    await r.set("k", b"v", ex=60)
    assert 0 < await r.ttl("k") <= 60


@pytest.mark.asyncio
async def test_string_getex_getdel(r: Redis) -> None:
    await r.set("k", b"v")
    assert await r.getex("k", ex=30) == b"v"
    assert await r.getdel("k") == b"v"
    assert await r.get("k") is None


@pytest.mark.asyncio
async def test_string_copy(r: Redis) -> None:
    await r.set("a", b"v")
    await r.copy("a", "b")
    assert await r.get("b") == b"v"


@pytest.mark.asyncio
async def test_string_incr_decr(r: Redis) -> None:
    assert await r.incr("c") == 1
    assert await r.incrby("c", 4) == 5
    assert await r.decr("c") == 4
    assert await r.decrby("c", 2) == 2


@pytest.mark.asyncio
async def test_string_incrbyfloat(r: Redis) -> None:
    assert await r.incrbyfloat("f", 1.5) == 1.5


@pytest.mark.asyncio
async def test_string_append_strlen(r: Redis) -> None:
    await r.set("k", b"hello")
    assert await r.append("k", b" world") == 11
    assert await r.strlen("k") == 11


@pytest.mark.asyncio
async def test_string_mget_mset_msetnx(r: Redis) -> None:
    await r.mset({"a": b"1", "b": b"2"})
    assert await r.mget("a", "b") == [b"1", b"2"]
    assert await r.msetnx({"x": b"1"}) in (True, 1)
    assert await r.msetnx({"x": b"2"}) in (False, 0)


@pytest.mark.asyncio
async def test_string_setrange_getrange(r: Redis) -> None:
    await r.set("k", b"hello world")
    await r.setrange("k", 6, b"REDIS")
    assert await r.getrange("k", 0, -1) == b"hello REDIS"


@pytest.mark.asyncio
async def test_string_exists_delete_unlink(r: Redis) -> None:
    await r.set("a", b"1")
    await r.set("b", b"2")
    assert await r.exists("a", "b", "c") == 2
    assert await r.delete("a") == 1
    assert await r.unlink("b") == 1


@pytest.mark.asyncio
async def test_string_expire_persist(r: Redis) -> None:
    await r.set("k", b"v")
    await r.expire("k", 100)
    assert 0 < await r.ttl("k") <= 100
    await r.persist("k")
    assert await r.ttl("k") in (-1, None)


@pytest.mark.asyncio
async def test_string_pexpire_pttl(r: Redis) -> None:
    await r.set("k", b"v")
    await r.pexpire("k", 100_000)
    assert 0 < await r.pttl("k") <= 100_000


@pytest.mark.asyncio
async def test_string_expireat_pexpireat(r: Redis) -> None:
    import time

    await r.set("k", b"v")
    await r.expireat("k", int(time.time()) + 100)
    await r.set("k2", b"v")
    await r.pexpireat("k2", int(time.time() * 1000) + 100_000)
    assert await r.ttl("k") <= 100
    assert await r.pttl("k2") <= 100_000


@pytest.mark.asyncio
async def test_string_expiretime_pexpiretime(r: Redis) -> None:
    await r.set("k", b"v")
    await r.expire("k", 100)
    assert await r.expiretime("k") > 0
    assert await r.pexpiretime("k") > 0


@pytest.mark.asyncio
async def test_string_rename_renamenx(r: Redis) -> None:
    await r.set("a", b"v")
    await r.rename("a", "b")
    assert await r.get("b") == b"v"
    await r.set("a", b"x")
    assert await r.renamenx("a", "b") in (False, 0)


@pytest.mark.asyncio
async def test_string_type(r: Redis) -> None:
    await r.set("k", b"v")
    assert await r.type("k") in (b"string", "string")


@pytest.mark.asyncio
async def test_string_dump_restore(r: Redis) -> None:
    await r.set("k", b"v")
    blob = await r.dump("k")
    assert blob is not None
    await r.delete("k")
    await r.restore("k", 0, blob)
    assert await r.get("k") == b"v"


@pytest.mark.asyncio
async def test_ping(r: Redis) -> None:
    assert await r.ping() is True
```

Run: `uv run pytest tests/facade/test_asyncio_commands_smoke.py -v`
Expected: 18 PASS.

- [ ] **Step 11: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/asyncio_mod.rs crates/redis-rs-py-driver/src/facade/sync.rs crates/redis-rs-py-driver/src/lib.rs python/redis_rs_py/asyncio/__init__.py tests/facade/test_asyncio_constructor.py tests/facade/test_asyncio_from_url.py tests/facade/test_asyncio_close.py tests/facade/test_asyncio_commands_smoke.py
git commit -m "feat(asyncio): add Redis pyclass with constructor, from_url, lifecycle, strings"
```

---

## Task 3: List commands (async)

Mirror Plan 10 Task 5. Macro pastes only — no new design decisions.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs`
- Test: `tests/facade/test_asyncio_commands_smoke.py`

- [ ] **Step 1: Append the list `#[pymethods]` block**

```rust
// =========================================================================
// List commands — plan 04 surface, async variants.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (key, *values))]
    fn lpush(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("alpush", (key, values))?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn rpush(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("arpush", (key, values))?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn lpushx(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("alpushx", (key, values))?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn rpushx(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("arpushx", (key, values))?.unbind())
    }

    #[pyo3(signature = (key, count = None))]
    fn lpop(&self, py: Python<'_>, key: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        Ok(match count {
            None => bound.call_method1("alpop", (key,))?,
            Some(n) => bound.call_method1("alpop_count", (key, n))?,
        }
        .unbind())
    }

    #[pyo3(signature = (key, count = None))]
    fn rpop(&self, py: Python<'_>, key: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        Ok(match count {
            None => bound.call_method1("arpop", (key,))?,
            Some(n) => bound.call_method1("arpop_count", (key, n))?,
        }
        .unbind())
    }

    define_async_facade_command!(lmove, (src: String, dst: String, wherefrom: String = "LEFT".to_string(), whereto: String = "RIGHT".to_string()), almove);
    define_async_facade_command!(lpos, (key: String, value: Vec<u8>, rank: Option<i64> = None, count: Option<i64> = None, maxlen: Option<i64> = None), alpos);
    define_async_facade_command!(lrange, (key: String, start: i64, end: i64), alrange);
    define_async_facade_command!(llen, (key: String), allen);
    define_async_facade_command!(lrem, (key: String, count: i64, value: Vec<u8>), alrem);
    define_async_facade_command!(lindex, (key: String, index: i64), alindex);
    define_async_facade_command!(lset, (key: String, index: i64, value: Vec<u8>), alset);
    define_async_facade_command!(linsert, (key: String, where_: String, pivot: Vec<u8>, value: Vec<u8>), alinsert);
    define_async_facade_command!(ltrim, (key: String, start: i64, end: i64), altrim);

    #[pyo3(signature = (keys, timeout = 0.0))]
    fn blpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("ablpop", (keys, timeout))?.unbind())
    }

    #[pyo3(signature = (keys, timeout = 0.0))]
    fn brpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("abrpop", (keys, timeout))?.unbind())
    }

    #[pyo3(signature = (src, dst, wherefrom, whereto, timeout = 0.0))]
    fn blmove(
        &self,
        py: Python<'_>,
        src: String,
        dst: String,
        wherefrom: String,
        whereto: String,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("ablmove", (src, dst, wherefrom, whereto, timeout))?
            .unbind())
    }

    #[pyo3(signature = (timeout, numkeys, keys, direction, count = None))]
    fn blmpop(
        &self,
        py: Python<'_>,
        timeout: f64,
        numkeys: i64,
        keys: Vec<String>,
        direction: String,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("ablmpop", (timeout, numkeys, keys, direction, count))?
            .unbind())
    }

    #[pyo3(signature = (numkeys, keys, direction, count = None))]
    fn lmpop(
        &self,
        py: Python<'_>,
        numkeys: i64,
        keys: Vec<String>,
        direction: String,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("almpop", (numkeys, keys, direction, count))?
            .unbind())
    }
}
```

- [ ] **Step 2: Append list smoke tests**

Append to `tests/facade/test_asyncio_commands_smoke.py`:

```python
# --- lists ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_push_pop(r: Redis) -> None:
    await r.rpush("L", b"a", b"b", b"c")
    assert await r.llen("L") == 3
    assert await r.lpop("L") == b"a"
    assert await r.rpop("L") == b"c"


@pytest.mark.asyncio
async def test_list_lpushx_rpushx(r: Redis) -> None:
    await r.lpush("L", b"a")
    await r.lpushx("L", b"b")
    await r.rpushx("L", b"c")
    assert await r.lrange("L", 0, -1) == [b"b", b"a", b"c"]


@pytest.mark.asyncio
async def test_list_lmove_lpos_lrem(r: Redis) -> None:
    await r.rpush("S", b"a", b"b", b"a")
    await r.lmove("S", "D", "LEFT", "RIGHT")
    assert await r.lpos("S", b"a") == 0
    assert await r.lrem("S", 1, b"a") == 1


@pytest.mark.asyncio
async def test_list_lindex_lset_linsert_ltrim(r: Redis) -> None:
    await r.rpush("L", b"a", b"c")
    await r.linsert("L", "BEFORE", b"c", b"b")
    assert await r.lindex("L", 1) == b"b"
    await r.lset("L", 0, b"X")
    await r.ltrim("L", 0, 1)
    assert await r.llen("L") == 2


@pytest.mark.asyncio
async def test_list_blpop_brpop_immediate(r: Redis) -> None:
    await r.rpush("L", b"x", b"y")
    assert await r.blpop(["L"], timeout=1.0) == (b"L", b"x")
    assert await r.brpop(["L"], timeout=1.0) == (b"L", b"y")


@pytest.mark.asyncio
async def test_list_blmove_lmpop_blmpop(r: Redis) -> None:
    await r.rpush("S", b"a")
    await r.blmove("S", "D", "LEFT", "RIGHT", timeout=1.0)
    await r.rpush("L", b"a", b"b")
    assert await r.lmpop(1, ["L"], "LEFT", count=2) is not None
    assert await r.blmpop(1.0, 1, ["empty"], "LEFT", count=1) is None
```

Run: `uv run pytest tests/facade/test_asyncio_commands_smoke.py -v -k list`
Expected: 6 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/asyncio_mod.rs tests/facade/test_asyncio_commands_smoke.py
git commit -m "feat(asyncio): add list commands"
```

---

## Task 4: Hash + set commands (async)

Combine the two smaller families to keep the plan sequence short. Macro pastes only.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs`
- Test: `tests/facade/test_asyncio_commands_smoke.py`

- [ ] **Step 1: Append the hash `#[pymethods]` block**

```rust
// =========================================================================
// Hash commands — plan 05 surface, async variants.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (name, key = None, value = None, mapping = None, items = None))]
    fn hset(
        &self,
        py: Python<'_>,
        name: String,
        key: Option<String>,
        value: Option<Vec<u8>>,
        mapping: Option<Bound<'_, PyDict>>,
        items: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let kwargs = PyDict::new(py);
        if let Some(k) = key {
            kwargs.set_item("field", k)?;
        }
        if let Some(v) = value {
            kwargs.set_item("value", v)?;
        }
        if let Some(m) = mapping {
            kwargs.set_item("mapping", m)?;
        }
        if let Some(i) = items {
            kwargs.set_item("items", i)?;
        }
        Ok(drv
            .bind(py)
            .call_method("ahset", (name,), Some(&kwargs))?
            .unbind())
    }

    define_async_facade_command!(hsetnx, (name: String, key: String, value: Vec<u8>), ahsetnx);
    define_async_facade_command!(hmset, (name: String, mapping: Bound<'_, PyDict>), ahmset);
    define_async_facade_command!(hget, (name: String, key: String), ahget);
    define_async_facade_command!(hgetall, (name: String), ahgetall);

    #[pyo3(signature = (name, *keys))]
    fn hdel(&self, py: Python<'_>, name: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("ahdel", (name, keys))?.unbind())
    }

    define_async_facade_command!(hincrby, (name: String, key: String, amount: i64 = 1), ahincrby);
    define_async_facade_command!(hincrbyfloat, (name: String, key: String, amount: f64 = 1.0), ahincrbyfloat);
    define_async_facade_command!(hkeys, (name: String), ahkeys);
    define_async_facade_command!(hvals, (name: String), ahvals);
    define_async_facade_command!(hexists, (name: String, key: String), ahexists);
    define_async_facade_command!(hlen, (name: String), ahlen);

    #[pyo3(signature = (name, *keys))]
    fn hmget(&self, py: Python<'_>, name: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("ahmget", (name, keys))?.unbind())
    }

    define_async_facade_command!(hscan, (name: String, cursor: u64 = 0, match_: Option<String> = None, count: Option<i64> = None, no_values: bool = false), ahscan, [name, cursor, match_, count, no_values]);
    define_async_facade_command!(hrandfield, (key: String, count: Option<i64> = None, withvalues: bool = false), ahrandfield);

    define_async_facade_command!(hexpire, (name: String, seconds: i64, fields: Vec<String>, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), ahexpire);
    define_async_facade_command!(hpexpire, (name: String, milliseconds: i64, fields: Vec<String>, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), ahpexpire);
    define_async_facade_command!(hexpireat, (name: String, unix_time_seconds: i64, fields: Vec<String>, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), ahexpireat);
    define_async_facade_command!(hpexpireat, (name: String, unix_time_milliseconds: i64, fields: Vec<String>, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), ahpexpireat);
    define_async_facade_command!(hexpiretime, (name: String, fields: Vec<String>), ahexpiretime);
    define_async_facade_command!(hpexpiretime, (name: String, fields: Vec<String>), ahpexpiretime);
    define_async_facade_command!(httl, (name: String, fields: Vec<String>), ahttl);
    define_async_facade_command!(hpttl, (name: String, fields: Vec<String>), ahpttl);
    define_async_facade_command!(hpersist, (name: String, fields: Vec<String>), ahpersist);
}
```

- [ ] **Step 2: Append the set `#[pymethods]` block**

```rust
// =========================================================================
// Set commands — plan 06 surface, async variants.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (name, *values))]
    fn sadd(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asadd", (name, values))?.unbind())
    }

    #[pyo3(signature = (name, *values))]
    fn srem(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asrem", (name, values))?.unbind())
    }

    define_async_facade_command!(smembers, (name: String), asmembers);
    define_async_facade_command!(sismember, (name: String, value: Vec<u8>), asismember);

    #[pyo3(signature = (name, *values))]
    fn smismember(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asmismember", (name, values))?.unbind())
    }

    define_async_facade_command!(scard, (name: String), ascard);

    #[pyo3(signature = (*keys))]
    fn sinter(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asinter", (keys,))?.unbind())
    }

    #[pyo3(signature = (*keys))]
    fn sunion(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asunion", (keys,))?.unbind())
    }

    #[pyo3(signature = (*keys))]
    fn sdiff(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asdiff", (keys,))?.unbind())
    }

    #[pyo3(signature = (dest, *keys))]
    fn sinterstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asinterstore", (dest, keys))?.unbind())
    }

    #[pyo3(signature = (dest, *keys))]
    fn sunionstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asunionstore", (dest, keys))?.unbind())
    }

    #[pyo3(signature = (dest, *keys))]
    fn sdiffstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("asdiffstore", (dest, keys))?.unbind())
    }

    define_async_facade_command!(sintercard, (numkeys: i64, keys: Vec<String>, limit: Option<i64> = None), asintercard);
    define_async_facade_command!(smove, (src: String, dst: String, value: Vec<u8>), asmove);

    #[pyo3(signature = (name, count = None))]
    fn spop(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        Ok(match count {
            None => bound.call_method1("aspop", (name,))?,
            Some(n) => bound.call_method1("aspop_count", (name, n))?,
        }
        .unbind())
    }

    #[pyo3(signature = (name, count = None))]
    fn srandmember(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        Ok(match count {
            None => bound.call_method1("asrandmember", (name,))?,
            Some(n) => bound.call_method1("asrandmember_count", (name, n))?,
        }
        .unbind())
    }

    define_async_facade_command!(sscan, (name: String, cursor: u64 = 0, match_: Option<String> = None, count: Option<i64> = None), asscan, [name, cursor, match_, count]);
}
```

- [ ] **Step 3: Append hash + set smoke tests**

```python
# --- hashes ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_hash_set_get(r: Redis) -> None:
    await r.hset("H", "f", b"v")
    assert await r.hget("H", "f") == b"v"


@pytest.mark.asyncio
async def test_hash_hsetnx(r: Redis) -> None:
    assert await r.hsetnx("H", "f", b"v") in (True, 1)
    assert await r.hsetnx("H", "f", b"x") in (False, 0)


@pytest.mark.asyncio
async def test_hash_hmset_hmget_hgetall(r: Redis) -> None:
    await r.hmset("H", {"a": b"1", "b": b"2"})
    assert await r.hmget("H", "a", "b") == [b"1", b"2"]
    assert await r.hgetall("H") == {b"a": b"1", b"b": b"2"}


@pytest.mark.asyncio
async def test_hash_hdel_hexists_hlen_hkeys_hvals(r: Redis) -> None:
    await r.hmset("H", {"a": b"1", "b": b"2"})
    assert await r.hexists("H", "a") in (True, 1)
    assert await r.hdel("H", "a") == 1
    assert await r.hlen("H") == 1
    assert sorted(await r.hkeys("H")) == [b"b"]
    assert sorted(await r.hvals("H")) == [b"2"]


@pytest.mark.asyncio
async def test_hash_hincrby_hincrbyfloat(r: Redis) -> None:
    assert await r.hincrby("H", "n", 4) == 4
    assert await r.hincrbyfloat("H", "f", 1.5) == 1.5


@pytest.mark.asyncio
async def test_hash_hscan_hrandfield(r: Redis) -> None:
    await r.hmset("H", {"a": b"1", "b": b"2"})
    cursor, batch = await r.hscan("H")
    assert cursor == 0
    assert len(batch) == 4
    val = await r.hrandfield("H")
    assert val in (b"a", b"b")


# --- sets -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_add_card_members(r: Redis) -> None:
    assert await r.sadd("S", b"a", b"b", b"c") == 3
    assert await r.scard("S") == 3
    assert set(await r.smembers("S")) == {b"a", b"b", b"c"}


@pytest.mark.asyncio
async def test_set_ismember_smismember(r: Redis) -> None:
    await r.sadd("S", b"a")
    assert await r.sismember("S", b"a") in (True, 1)
    assert await r.smismember("S", b"a", b"x") in ([True, False], [1, 0])


@pytest.mark.asyncio
async def test_set_inter_union_diff_store(r: Redis) -> None:
    await r.sadd("A", b"a", b"b")
    await r.sadd("B", b"b", b"c")
    assert set(await r.sinter("A", "B")) == {b"b"}
    assert set(await r.sunion("A", "B")) == {b"a", b"b", b"c"}
    assert set(await r.sdiff("A", "B")) == {b"a"}
    assert await r.sinterstore("X", "A", "B") == 1
    assert await r.sunionstore("Y", "A", "B") == 3
    assert await r.sdiffstore("Z", "A", "B") == 1


@pytest.mark.asyncio
async def test_set_intercard_smove_spop_srandmember_sscan(r: Redis) -> None:
    await r.sadd("A", b"a", b"b")
    await r.sadd("B", b"a")
    assert await r.sintercard(2, ["A", "B"]) == 1
    await r.smove("A", "B", b"b")
    assert (await r.spop("B")) in (b"a", b"b")
    assert (await r.srandmember("B")) in (b"a", b"b", None)
    cursor, batch = await r.sscan("B")
    assert cursor == 0
```

Run: `uv run pytest tests/facade/test_asyncio_commands_smoke.py -v -k "hash or set"`
Expected: 10 PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/asyncio_mod.rs tests/facade/test_asyncio_commands_smoke.py
git commit -m "feat(asyncio): add hash and set commands"
```

---

## Task 5: Sorted-set commands (async)

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs`
- Test: `tests/facade/test_asyncio_commands_smoke.py`

- [ ] **Step 1: Append the zset `#[pymethods]` block**

Mirror Plan 10 Task 8 verbatim, replacing every `call_method("z…", …)` / `call_method1("z…")` with the `a`-prefixed driver method (`azadd`, `azrem`, `azrange`, ...). The macro entries become `define_async_facade_command!` calls.

```rust
// =========================================================================
// Sorted-set commands — plan 07 surface, async variants.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (
        name, mapping,
        nx = false, xx = false, gt = false, lt = false, ch = false, incr = false,
    ))]
    fn zadd(
        &self,
        py: Python<'_>,
        name: String,
        mapping: Bound<'_, PyDict>,
        nx: bool, xx: bool, gt: bool, lt: bool, ch: bool, incr: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let kwargs = PyDict::new(py);
        if nx { kwargs.set_item("nx", true)?; }
        if xx { kwargs.set_item("xx", true)?; }
        if gt { kwargs.set_item("gt", true)?; }
        if lt { kwargs.set_item("lt", true)?; }
        if ch { kwargs.set_item("ch", true)?; }
        if incr { kwargs.set_item("incr", true)?; }
        Ok(drv.bind(py).call_method("azadd", (name, mapping), Some(&kwargs))?.unbind())
    }

    #[pyo3(signature = (name, *values))]
    fn zrem(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azrem", (name, values))?.unbind())
    }

    #[pyo3(signature = (
        name, start, end,
        desc = false, withscores = false, score_cast_func = None,
        byscore = false, bylex = false, offset = None, num = None,
    ))]
    fn zrange(
        &self,
        py: Python<'_>,
        name: String,
        start: Py<PyAny>,
        end: Py<PyAny>,
        desc: bool,
        withscores: bool,
        score_cast_func: Option<Py<PyAny>>,
        byscore: bool, bylex: bool,
        offset: Option<i64>, num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = score_cast_func;
        let drv = self.driver_or_raise()?;
        let kwargs = PyDict::new(py);
        if desc { kwargs.set_item("rev", true)?; }
        if withscores { kwargs.set_item("withscores", true)?; }
        if byscore { kwargs.set_item("byscore", true)?; }
        if bylex { kwargs.set_item("bylex", true)?; }
        if let Some(o) = offset { kwargs.set_item("offset", o)?; }
        if let Some(n) = num { kwargs.set_item("count", n)?; }
        Ok(drv.bind(py).call_method("azrange", (name, start, end), Some(&kwargs))?.unbind())
    }

    define_async_facade_command!(zrangebyscore, (name: String, min_: Py<PyAny>, max_: Py<PyAny>, start: Option<i64> = None, num: Option<i64> = None, withscores: bool = false), azrangebyscore, [name, min_, max_, start, num, withscores]);
    define_async_facade_command!(zrangebylex, (name: String, min_: Vec<u8>, max_: Vec<u8>, start: Option<i64> = None, num: Option<i64> = None), azrangebylex, [name, min_, max_, start, num]);
    define_async_facade_command!(zrevrangebyscore, (name: String, max_: Py<PyAny>, min_: Py<PyAny>, start: Option<i64> = None, num: Option<i64> = None, withscores: bool = false), azrevrangebyscore, [name, max_, min_, start, num, withscores]);
    define_async_facade_command!(zrevrangebylex, (name: String, max_: Vec<u8>, min_: Vec<u8>, start: Option<i64> = None, num: Option<i64> = None), azrevrangebylex, [name, max_, min_, start, num]);
    define_async_facade_command!(zrangestore, (dest: String, src: String, start: Py<PyAny>, end: Py<PyAny>, byscore: bool = false, bylex: bool = false, desc: bool = false, offset: Option<i64> = None, num: Option<i64> = None), azrangestore);
    define_async_facade_command!(zincrby, (name: String, amount: f64, value: Vec<u8>), azincrby);
    define_async_facade_command!(zcard, (name: String), azcard);
    define_async_facade_command!(zscore, (name: String, value: Vec<u8>), azscore);

    #[pyo3(signature = (name, *values))]
    fn zmscore(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azmscore", (name, values))?.unbind())
    }

    define_async_facade_command!(zrank, (name: String, value: Vec<u8>, withscore: bool = false), azrank);
    define_async_facade_command!(zrevrank, (name: String, value: Vec<u8>, withscore: bool = false), azrevrank);
    define_async_facade_command!(zremrangebyrank, (name: String, min_: i64, max_: i64), azremrangebyrank, [name, min_, max_]);
    define_async_facade_command!(zremrangebyscore, (name: String, min_: Py<PyAny>, max_: Py<PyAny>), azremrangebyscore, [name, min_, max_]);
    define_async_facade_command!(zremrangebylex, (name: String, min_: Vec<u8>, max_: Vec<u8>), azremrangebylex, [name, min_, max_]);
    define_async_facade_command!(zcount, (name: String, min_: Py<PyAny>, max_: Py<PyAny>), azcount, [name, min_, max_]);
    define_async_facade_command!(zlexcount, (name: String, min_: Vec<u8>, max_: Vec<u8>), azlexcount, [name, min_, max_]);

    #[pyo3(signature = (name, count = None))]
    fn zpopmin(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azpopmin", (name, count))?.unbind())
    }

    #[pyo3(signature = (name, count = None))]
    fn zpopmax(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azpopmax", (name, count))?.unbind())
    }

    define_async_facade_command!(bzpopmin, (keys: Vec<String>, timeout: f64 = 0.0), abzpopmin);
    define_async_facade_command!(bzpopmax, (keys: Vec<String>, timeout: f64 = 0.0), abzpopmax);

    #[pyo3(signature = (numkeys, keys, min_or_max, count = None))]
    fn zmpop(
        &self,
        py: Python<'_>,
        numkeys: i64,
        keys: Vec<String>,
        min_or_max: String,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azmpop", (numkeys, keys, min_or_max, count))?.unbind())
    }

    #[pyo3(signature = (timeout, numkeys, keys, min_or_max, count = None))]
    fn bzmpop(
        &self,
        py: Python<'_>,
        timeout: f64,
        numkeys: i64,
        keys: Vec<String>,
        min_or_max: String,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("abzmpop", (timeout, numkeys, keys, min_or_max, count))?.unbind())
    }

    define_async_facade_command!(zrandmember, (name: String, count: Option<i64> = None, withscores: bool = false), azrandmember);
    define_async_facade_command!(zscan, (name: String, cursor: u64 = 0, match_: Option<String> = None, count: Option<i64> = None, score_cast_func: Option<Py<PyAny>> = None), azscan, [name, cursor, match_, count]);

    #[pyo3(signature = (keys, aggregate = None, withscores = false))]
    fn zunion(&self, py: Python<'_>, keys: Vec<String>, aggregate: Option<String>, withscores: bool) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azunion", (keys, aggregate, withscores))?.unbind())
    }

    #[pyo3(signature = (keys, aggregate = None, withscores = false))]
    fn zinter(&self, py: Python<'_>, keys: Vec<String>, aggregate: Option<String>, withscores: bool) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azinter", (keys, aggregate, withscores))?.unbind())
    }

    #[pyo3(signature = (keys, withscores = false))]
    fn zdiff(&self, py: Python<'_>, keys: Vec<String>, withscores: bool) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azdiff", (keys, withscores))?.unbind())
    }

    #[pyo3(signature = (dest, keys, aggregate = None))]
    fn zunionstore(&self, py: Python<'_>, dest: String, keys: Vec<String>, aggregate: Option<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azunionstore", (dest, keys, aggregate))?.unbind())
    }

    #[pyo3(signature = (dest, keys, aggregate = None))]
    fn zinterstore(&self, py: Python<'_>, dest: String, keys: Vec<String>, aggregate: Option<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azinterstore", (dest, keys, aggregate))?.unbind())
    }

    #[pyo3(signature = (dest, keys))]
    fn zdiffstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("azdiffstore", (dest, keys))?.unbind())
    }
}
```

- [ ] **Step 2: Append zset smoke tests**

```python
# --- zsets ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_zset_basic(r: Redis) -> None:
    await r.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert await r.zcard("Z") == 3
    assert await r.zscore("Z", b"b") == 2.0
    assert await r.zrange("Z", 0, -1) == [b"a", b"b", b"c"]
    assert await r.zrange("Z", 0, -1, withscores=True) == [
        (b"a", 1.0), (b"b", 2.0), (b"c", 3.0),
    ]


@pytest.mark.asyncio
async def test_zset_rev_zincrby_zrank(r: Redis) -> None:
    await r.zadd("Z", {"a": 1, "b": 2})
    assert await r.zrange("Z", 0, -1, desc=True) == [b"b", b"a"]
    assert await r.zincrby("Z", 3.0, b"a") == 4.0
    assert await r.zrank("Z", b"b") == 0


@pytest.mark.asyncio
async def test_zset_zrem_zpopmin_zpopmax(r: Redis) -> None:
    await r.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert await r.zrem("Z", b"b") == 1
    assert (await r.zpopmin("Z"))[0] == (b"a", 1.0)
    assert (await r.zpopmax("Z"))[0] == (b"c", 3.0)


@pytest.mark.asyncio
async def test_zset_zmscore_zcount_zscan(r: Redis) -> None:
    await r.zadd("Z", {"a": 1, "b": 2})
    assert await r.zmscore("Z", b"a", b"x") == [1.0, None]
    assert await r.zcount("Z", 1, 2) == 2
    cursor, batch = await r.zscan("Z")
    assert cursor == 0


@pytest.mark.asyncio
async def test_zset_set_ops_store(r: Redis) -> None:
    await r.zadd("A", {"a": 1, "b": 2})
    await r.zadd("B", {"b": 3, "c": 4})
    assert await r.zunionstore("U", ["A", "B"]) == 3
    assert await r.zinterstore("I", ["A", "B"]) == 1
    assert await r.zdiffstore("D", ["A", "B"]) == 1
```

Run: `uv run pytest tests/facade/test_asyncio_commands_smoke.py -v -k zset`
Expected: 5 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/asyncio_mod.rs tests/facade/test_asyncio_commands_smoke.py
git commit -m "feat(asyncio): add sorted-set commands"
```

---

## Task 6: Stream + scripts/admin commands (async)

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs`
- Test: `tests/facade/test_asyncio_commands_smoke.py`

- [ ] **Step 1: Append the streams `#[pymethods]` block**

Mirror Plan 10 Task 9, replacing every driver method name with the `a`-prefixed variant (`axadd`, `axlen`, etc.):

```rust
// =========================================================================
// Stream commands — plan 08 surface, async variants.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (
        name, fields,
        id = "*".to_string(),
        maxlen = None, approximate = true, nomkstream = false,
        minid = None, limit = None,
    ))]
    fn xadd(
        &self,
        py: Python<'_>,
        name: String,
        fields: Bound<'_, PyDict>,
        id: String,
        maxlen: Option<i64>, approximate: bool, nomkstream: bool,
        minid: Option<String>, limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("id", id)?;
        if let Some(m) = maxlen { kwargs.set_item("maxlen", m)?; }
        kwargs.set_item("approximate", approximate)?;
        if nomkstream { kwargs.set_item("nomkstream", true)?; }
        if let Some(m) = minid { kwargs.set_item("minid", m)?; }
        if let Some(l) = limit { kwargs.set_item("limit", l)?; }
        Ok(drv.bind(py).call_method("axadd", (name, fields), Some(&kwargs))?.unbind())
    }

    define_async_facade_command!(xlen, (name: String), axlen);
    define_async_facade_command!(xrange, (name: String, min_: String = "-".to_string(), max_: String = "+".to_string(), count: Option<i64> = None), axrange, [name, min_, max_, count]);
    define_async_facade_command!(xrevrange, (name: String, max_: String = "+".to_string(), min_: String = "-".to_string(), count: Option<i64> = None), axrevrange, [name, max_, min_, count]);

    #[pyo3(signature = (streams, count = None, block = None))]
    fn xread(&self, py: Python<'_>, streams: Bound<'_, PyDict>, count: Option<i64>, block: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("axread", (streams, count, block))?.unbind())
    }

    #[pyo3(signature = (groupname, consumername, streams, count = None, block = None, noack = false))]
    fn xreadgroup(
        &self,
        py: Python<'_>,
        groupname: String,
        consumername: String,
        streams: Bound<'_, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
        noack: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("axreadgroup", (groupname, consumername, streams, count, block, noack))?.unbind())
    }

    #[pyo3(signature = (name, groupname, *ids))]
    fn xack(&self, py: Python<'_>, name: String, groupname: String, ids: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("axack", (name, groupname, ids))?.unbind())
    }

    #[pyo3(signature = (name, *ids))]
    fn xdel(&self, py: Python<'_>, name: String, ids: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("axdel", (name, ids))?.unbind())
    }

    define_async_facade_command!(xgroup_create, (name: String, groupname: String, id: String = "$".to_string(), mkstream: bool = false, entries_read: Option<i64> = None), axgroup_create);
    define_async_facade_command!(xgroup_setid, (name: String, groupname: String, id: String, entries_read: Option<i64> = None), axgroup_setid);
    define_async_facade_command!(xgroup_destroy, (name: String, groupname: String), axgroup_destroy);
    define_async_facade_command!(xgroup_delconsumer, (name: String, groupname: String, consumername: String), axgroup_delconsumer);
    define_async_facade_command!(xgroup_createconsumer, (name: String, groupname: String, consumername: String), axgroup_createconsumer);
    define_async_facade_command!(xinfo_stream, (name: String, full: bool = false), axinfo_stream);
    define_async_facade_command!(xinfo_groups, (name: String), axinfo_groups);
    define_async_facade_command!(xinfo_consumers, (name: String, groupname: String), axinfo_consumers);
    define_async_facade_command!(xtrim, (name: String, maxlen: Option<i64> = None, approximate: bool = true, minid: Option<String> = None, limit: Option<i64> = None), axtrim);

    #[pyo3(signature = (name, groupname, idle = None, min_id = None, max_id = None, count = None, consumername = None))]
    fn xpending(
        &self,
        py: Python<'_>,
        name: String, groupname: String,
        idle: Option<i64>, min_id: Option<String>, max_id: Option<String>,
        count: Option<i64>, consumername: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("axpending", (name, groupname, idle, min_id, max_id, count, consumername))?.unbind())
    }

    #[pyo3(signature = (name, groupname, consumername, min_idle_time, ids, idle = None, time = None, retrycount = None, force = false, justid = false))]
    fn xclaim(
        &self,
        py: Python<'_>,
        name: String, groupname: String, consumername: String,
        min_idle_time: i64, ids: Vec<String>,
        idle: Option<i64>, time: Option<i64>, retrycount: Option<i64>,
        force: bool, justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("axclaim", (name, groupname, consumername, min_idle_time, ids, idle, time, retrycount, force, justid))?.unbind())
    }

    #[pyo3(signature = (name, groupname, consumername, min_idle_time, start = "0-0".to_string(), count = None, justid = false))]
    fn xautoclaim(
        &self,
        py: Python<'_>,
        name: String, groupname: String, consumername: String,
        min_idle_time: i64, start: String, count: Option<i64>, justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("axautoclaim", (name, groupname, consumername, min_idle_time, start, count, justid))?.unbind())
    }

    define_async_facade_command!(xsetid, (name: String, id: String, entries_added: Option<i64> = None, max_deleted_id: Option<String> = None), axsetid);
}
```

- [ ] **Step 2: Append the scripts/admin `#[pymethods]` block**

```rust
// =========================================================================
// Scripts + admin — plan 09 surface, async variants.
// =========================================================================

#[pymethods]
impl Redis {
    define_async_facade_command!(eval, (script: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), aeval, [script, numkeys, keys_and_args]);
    define_async_facade_command!(eval_ro, (script: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), aeval_ro, [script, numkeys, keys_and_args]);
    define_async_facade_command!(evalsha, (sha: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), aevalsha, [sha, numkeys, keys_and_args]);
    define_async_facade_command!(evalsha_ro, (sha: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), aevalsha_ro, [sha, numkeys, keys_and_args]);
    define_async_facade_command!(script_load, (script: String), ascript_load);

    #[pyo3(signature = (*shas))]
    fn script_exists(&self, py: Python<'_>, shas: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("ascript_exists", (shas,))?.unbind())
    }

    fn script_flush(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method0("ascript_flush")?.unbind())
    }

    define_async_facade_command!(fcall, (function: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), afcall, [function, numkeys, keys_and_args]);
    define_async_facade_command!(fcall_ro, (function: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), afcall_ro, [function, numkeys, keys_and_args]);
    define_async_facade_command!(function_load, (code: String, replace: bool = false), afunction_load);
    define_async_facade_command!(function_dump, (), afunction_dump);
    define_async_facade_command!(function_flush, (mode: Option<String> = None), afunction_flush);
    define_async_facade_command!(function_list, (library: Option<String> = None, withcode: bool = false), afunction_list);
    define_async_facade_command!(function_stats, (), afunction_stats);
    define_async_facade_command!(function_kill, (), afunction_kill);

    define_async_facade_command!(scan, (cursor: u64 = 0, match_: Option<String> = None, count: Option<i64> = None, type_: Option<String> = None), ascan, [cursor, match_, count, type_]);

    #[pyo3(signature = (match_ = None, count = None, type_ = None))]
    fn scan_iter(&self, py: Python<'_>, match_: Option<String>, count: Option<i64>, type_: Option<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("ascan_iter", (match_, count, type_))?.unbind())
    }

    define_async_facade_command!(keys, (pattern: String = "*".to_string()), akeys);
    define_async_facade_command!(randomkey, (), arandomkey);
    define_async_facade_command!(dbsize, (), adbsize);
    define_async_facade_command!(flushdb, (asynchronous: bool = false), aflushdb);
    define_async_facade_command!(flushall, (asynchronous: bool = false), aflushall);

    define_async_facade_command!(info, (section: Option<String> = None), ainfo);
    define_async_facade_command!(config_get, (pattern: String = "*".to_string()), aconfig_get);
    define_async_facade_command!(config_set, (parameter: String, value: Py<PyAny>), aconfig_set);
    define_async_facade_command!(config_resetstat, (), aconfig_resetstat);
    define_async_facade_command!(config_rewrite, (), aconfig_rewrite);

    #[pyo3(signature = (
        _id = None, _type = None, addr = None,
        skipme = true, laddr = None, user = None, maxage = None,
    ))]
    fn client_kill(
        &self,
        py: Python<'_>,
        _id: Option<i64>, _type: Option<String>, addr: Option<String>,
        skipme: bool, laddr: Option<String>, user: Option<String>, maxage: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("aclient_kill", (_id, _type, addr, skipme, laddr, user, maxage))?.unbind())
    }

    define_async_facade_command!(client_getname, (), aclient_getname);
    define_async_facade_command!(client_setname, (name: String), aclient_setname);
    define_async_facade_command!(client_list, (_type: Option<String> = None, client_id: Option<Vec<i64>> = None), aclient_list);
    define_async_facade_command!(client_id, (), aclient_id);
    define_async_facade_command!(client_info, (), aclient_info);
    define_async_facade_command!(client_pause, (timeout: i64, all: bool = true), aclient_pause);
    define_async_facade_command!(client_unpause, (), aclient_unpause);
    define_async_facade_command!(client_no_evict, (mode: String), aclient_no_evict);
    define_async_facade_command!(client_no_touch, (mode: String), aclient_no_touch);

    define_async_facade_command!(object_encoding, (name: String), aobject_encoding);
    define_async_facade_command!(object_idletime, (name: String), aobject_idletime);
    define_async_facade_command!(object_freq, (name: String), aobject_freq);
    define_async_facade_command!(object_refcount, (name: String), aobject_refcount);
    define_async_facade_command!(object_help, (), aobject_help);
    define_async_facade_command!(memory_usage, (key: String, samples: Option<i64> = None), amemory_usage);

    define_async_facade_command!(echo, (value: Vec<u8>), aecho);
    define_async_facade_command!(wait, (numreplicas: i64, timeout: i64), await_);
    define_async_facade_command!(waitaof, (numlocal: i64, numreplicas: i64, timeout: i64), awaitaof);
    define_async_facade_command!(time, (), atime);
    define_async_facade_command!(lastsave, (), alastsave);
    define_async_facade_command!(bgsave, (schedule: bool = false), abgsave);
    define_async_facade_command!(bgrewriteaof, (), abgrewriteaof);
    define_async_facade_command!(debug_sleep, (seconds: f64), adebug_sleep);
}
```

(Note: the driver method for `wait` is `await_` to avoid the Rust `await` keyword collision; the façade method is `wait`. If the driver actually exposes it as `await_`, the macro forwards correctly. Otherwise rename to whatever the driver actually exports.)

- [ ] **Step 3: Append stream + scripts/admin smoke tests**

```python
# --- streams --------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_xadd_xlen_xrange(r: Redis) -> None:
    id1 = await r.xadd("S", {"f": b"1"})
    id2 = await r.xadd("S", {"f": b"2"})
    assert await r.xlen("S") == 2
    rng = await r.xrange("S")
    assert len(rng) == 2
    assert (await r.xrevrange("S"))[0][0] == id2
    assert (await r.xrange("S", min_=id1, max_=id1))[0][0] == id1


@pytest.mark.asyncio
async def test_stream_xread_xreadgroup_xack(r: Redis) -> None:
    await r.xadd("S", {"f": b"v"})
    await r.xgroup_create("S", "G", id="0", mkstream=False)
    msgs = await r.xreadgroup("G", "C1", {"S": ">"})
    assert msgs
    first = (await r.xrange("S"))[0][0]
    assert await r.xack("S", "G", first) == 1


@pytest.mark.asyncio
async def test_stream_xdel_xtrim(r: Redis) -> None:
    id1 = await r.xadd("S", {"f": b"1"})
    await r.xadd("S", {"f": b"2"})
    assert await r.xdel("S", id1) == 1
    await r.xtrim("S", maxlen=1, approximate=False)
    assert await r.xlen("S") <= 1


@pytest.mark.asyncio
async def test_stream_xinfo_xpending(r: Redis) -> None:
    await r.xadd("S", {"f": b"v"})
    await r.xgroup_create("S", "G", id="0", mkstream=False)
    await r.xreadgroup("G", "C1", {"S": ">"})
    assert await r.xinfo_stream("S")
    assert await r.xinfo_groups("S")
    assert await r.xpending("S", "G")


@pytest.mark.asyncio
async def test_stream_xclaim_xautoclaim_xsetid(r: Redis) -> None:
    id1 = await r.xadd("S", {"f": b"v"})
    await r.xgroup_create("S", "G", id="0", mkstream=False)
    await r.xreadgroup("G", "C1", {"S": ">"})
    assert await r.xclaim("S", "G", "C2", 0, [id1])
    assert await r.xautoclaim("S", "G", "C3", 0) is not None
    await r.xsetid("S", "100-0")  # noqa: PLR2004


# --- scripts + admin ------------------------------------------------------


@pytest.mark.asyncio
async def test_scripts_eval_evalsha(r: Redis) -> None:
    sha = await r.script_load("return KEYS[1]")
    assert await r.evalsha(sha, 1, ["hello"]) == b"hello"
    assert await r.eval("return 1", 0, []) == 1


@pytest.mark.asyncio
async def test_admin_scan_keys_dbsize(r: Redis) -> None:
    await r.set("k1", b"v")
    await r.set("k2", b"v")
    cursor, batch = await r.scan()
    assert cursor == 0
    keys = await r.keys("k*")
    assert set(keys) >= {b"k1", b"k2"} or set(keys) >= {"k1", "k2"}
    assert await r.dbsize() >= 2


@pytest.mark.asyncio
async def test_admin_info_config(r: Redis) -> None:
    info = await r.info()
    assert info
    cfg = await r.config_get("maxmemory")
    assert cfg is not None
    await r.config_resetstat()


@pytest.mark.asyncio
async def test_admin_client_apis(r: Redis) -> None:
    await r.client_setname("test-client")
    assert await r.client_getname() in (b"test-client", "test-client")
    assert await r.client_id() > 0
    assert await r.client_list()


@pytest.mark.asyncio
async def test_admin_object_memory(r: Redis) -> None:
    await r.set("k", b"v")
    assert await r.object_encoding("k") is not None
    assert await r.memory_usage("k") > 0


@pytest.mark.asyncio
async def test_admin_basic(r: Redis) -> None:
    assert await r.echo(b"hi") == b"hi"
    t = await r.time()
    assert isinstance(t, (list, tuple)) and len(t) == 2
    await r.lastsave()
    await r.bgsave()
    await r.bgrewriteaof()
```

Run: `uv run pytest tests/facade/test_asyncio_commands_smoke.py -v -k "stream or scripts or admin"`
Expected: 11 PASS.

- [ ] **Step 4: Run the full asyncio façade suite**

Run: `uv run pytest tests/facade/test_asyncio_constructor.py tests/facade/test_asyncio_from_url.py tests/facade/test_asyncio_close.py tests/facade/test_asyncio_commands_smoke.py -v`
Expected: every test PASSES.

- [ ] **Step 5: Run lint**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
uv run ruff check tests/facade/
uv run ruff format --check tests/facade/
```

Expected: green.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/asyncio_mod.rs tests/facade/test_asyncio_commands_smoke.py
git commit -m "feat(asyncio): add stream and scripts/admin commands"
```

---

## Task 7: Stubs + CHANGELOG

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Append the asyncio stub block to `_driver.pyi`**

Append:

```python
class _AsyncioRedis:
    """Stub mirror of redis_rs_py._driver.asyncio.Redis."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_keepalive: bool = False,
        socket_keepalive_options: Any = None,
        connection_pool: Any = None,
        unix_socket_path: str | None = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        charset: Any = None,
        errors: Any = None,
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        retry_on_error: Any = None,
        ssl: bool = False,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: str = "required",
        ssl_ca_certs: str | None = None,
        ssl_ca_path: Any = None,
        ssl_ca_data: Any = None,
        ssl_check_hostname: bool = False,
        ssl_password: Any = None,
        ssl_validate_ocsp: bool = False,
        ssl_validate_ocsp_stapled: bool = False,
        ssl_ocsp_context: Any = None,
        ssl_ocsp_expected_cert: Any = None,
        ssl_min_version: Any = None,
        ssl_ciphers: Any = None,
        max_connections: int | None = None,
        single_connection_client: bool = False,
        health_check_interval: int = 0,
        client_name: str | None = None,
        lib_name: str | None = None,
        lib_version: str | None = None,
        username: str | None = None,
        retry: Any = None,
        redis_connect_func: Any = None,
        credential_provider: Any = None,
        protocol: int = 2,
        cache: Any = None,
        cache_config: Any = None,
        event_dispatcher: Any = None,
        **kwargs: Any,
    ) -> None: ...
    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> _AsyncioRedis: ...
    async def aclose(self) -> None: ...
    async def __aenter__(self) -> _AsyncioRedis: ...
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool: ...
    def pipeline(self, transaction: bool = True, shard_hint: Any = None) -> Any: ...
    def pubsub(self, **kwargs: Any) -> Any: ...
    def transaction(self, func: Any, *watches: Any, value_from_callable: bool = False, watch_delay: float | None = None, **kwargs: Any) -> Any: ...
    def get(self, key: str) -> Awaitable[bytes | None]: ...
    def set(self, key: str, value: bytes, ex: int | None = None, px: int | None = None, nx: bool = False, xx: bool = False, keepttl: bool = False, get: bool = False, exat: int | None = None, pxat: int | None = None) -> Awaitable[Any]: ...
    # ... (rest of method surface mirrors the sync stub but Awaitable-wrapped)


class asyncio:  # noqa: N801
    Redis = _AsyncioRedis
```

- [ ] **Step 2: Add the CHANGELOG entry**

Append under `### Added`:

```markdown
- `redis_rs_py.asyncio.Redis` — Rust-backed asyncio façade matching `redis.asyncio.Redis`'s constructor surface and method names. Every command returns a `RedisRsAwaitable`. Submodule registered as a true PyO3 submodule under `_driver.asyncio`, re-exported from `redis_rs_py.asyncio`.
- `__aenter__` / `__aexit__` / `aclose` round out the async lifecycle; post-close use raises `ValueError`.
- `define_async_facade_command!` macro keeps Tasks 3-6 mechanical: each command family is a single block of macro pastes.
```

```bash
git add python/redis_rs_py/_driver.pyi CHANGELOG.md
git commit -m "docs(asyncio): add stubs and changelog entry"
```

---

## Self-review checklist for this plan

- [x] Spec coverage: "redis_rs_py.asyncio.Redis mirrors redis.asyncio.Redis. Same method names — no `a`-prefix at the façade layer." — verified by every command method in Tasks 2-6 being named identically to the sync façade.
- [x] Architecture: façade lives entirely in Rust per the "Rust by default" principle. Python `asyncio/__init__.py` is a one-line re-export.
- [x] Constructor signature is byte-identical to the sync façade (Plan 10 Task 3) — same kwarg list, same defaults, same `**extra` warn-once flow. `accept_and_warn` from plan 10 is reused, not re-implemented.
- [x] Submodule registration uses PyO3 0.28's documented pattern: `PyModule::new` + `add_submodule` + manual `sys.modules` insert. Documented in Task 1 with a comment explaining why the `sys.modules` step is required.
- [x] `define_async_facade_command!` macro keeps Tasks 3-6 mechanical: each command family is a single big macro block + one smoke test per family.
- [x] Pipeline / PubSub / transaction are placeholders pointing at plans 13 / 14.
- [x] `aclose` / `__aenter__` / `__aexit__` are documented and tested (Task 2 Step 9).
- [x] Plan 10 cross-references: `FacadeConfig`, `parse_url`, `build_driver`, `IMPLEMENTED_KWARGS`, `accept_and_warn` are all marked `pub(crate)` so the asyncio module can re-use them — Task 2 Step 4 explicitly notes the visibility lift.
- [x] All file paths match the file-structure section.
- [x] Length: ~1500 lines, references plan 10's macro instead of re-explaining it.
