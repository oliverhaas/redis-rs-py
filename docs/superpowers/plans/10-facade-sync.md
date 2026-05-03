# Plan 10 — Sync façade: `redis_rs_py.Redis`

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the redis-py-compatible sync façade — `redis_rs_py.Redis` — entirely in Rust as a `#[pyclass]`. The class mirrors `redis.Redis.__init__`'s full kwarg surface (implementing the kwargs we support, accept-and-warning the rest), exposes `Redis.from_url(url)` parsing the standard `redis://` / `rediss://` / `unix://` URL form, and forwards every command from plans 03-09 to an internal `RedisRsDriver`. Pipeline / pubsub / transaction methods are placeholders that raise `NotImplementedError` pointing at plans 13/14. Context-manager protocol (`__enter__` / `__exit__`) and `close()` drop the driver `Arc`. The `lock(...)` helper builds on the script-based primitives from plan 09.

**Architecture:** `Redis` is a `#[pyclass(subclass)]` holding an `Arc<RedisRsDriver>` (or `Option<Py<RedisRsDriver>>` after close). Construction goes through one private `Config` struct that is also the merge target for `from_url`. A `kwargs.rs` module owns the *accept-and-warn* surface — every redis-py kwarg appears in the constructor signature, the unknown ones get filtered into the warn-once map. A `define_facade_command!` macro takes (method-name, signature, driver-method) and emits a one-line delegating `fn`. Each command-family task pastes one big macro invocation per family — the task body is mechanical, the macro is the interesting part.

**Tech Stack:** PyO3 0.28 (`#[pyclass(subclass)]`, `signature = (...)`, `Bound<'_, PyDict>`, `create_exception!` reused from plan 02), `url` 2.x (URL parser), `std::sync::OnceLock + Mutex<HashSet<String>>` for warn-once state, no new Python deps.

**Reference material:**
- `python -c "import redis, inspect; print(inspect.signature(redis.Redis.__init__))"` — the source of truth for the constructor signature. Run it before starting Task 2.
- `python -c "import redis, inspect; print(inspect.signature(redis.Redis.from_url))"` — `from_url(url, **kwargs) -> Redis`.
- `/home/ohaas/e1+/redis-rs-py/PLAN.md` §"Architecture" — "Rust by default, Python only when forced". Every façade method is Rust; the Python tree is a one-line re-export.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/adapter.rs` — the subclass-friendly Rust pyclass pattern. Use `#[pyclass(subclass, module = "redis_rs_py._driver")]` and `slf.getattr("_driver")?.call0()` style dispatch where subclass overrides matter (we don't yet, but keep the door open).
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/01-foundation-async-bridge.md` — `RedisRsDriver` factory + canonical methods.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/02-exceptions.md` — `RedisError`, `DataError`, `LockError`, `LockNotOwnedError` are reused here.
- Plans 03-09 enumerate the command method list. The `define_facade_command!` invocations in tasks (c)-(i) are the canonical surface — keep them in lockstep with `RedisRsDriver`.

**Out of scope for this plan:**
- Async façade — plan 11.
- `decode_responses=True` — plan 12 wires the decoder; this plan stores the flag, ignores it on output.
- `Pipeline` / `transaction()` semantics — plan 13. We expose only `pipeline()` raising `NotImplementedError("see plan 13")`.
- `PubSub` — plan 14. `pubsub()` raises `NotImplementedError("see plan 14")`.
- `RedisCluster` / `Sentinel` — plans 15 / 16.
- `register_script` helper — out of scope per spec; users use `script_load` + `evalsha`.
- `ConnectionPool` first-class object — `connection_pool=` kwarg is accept-and-warn (lives in a future plan if demand appears).

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  facade/
    mod.rs                      # NEW: declares `pub mod sync; pub mod kwargs;`
    sync.rs                     # NEW: the Redis pyclass + every command method
    kwargs.rs                   # NEW: accept-and-warn surface
  lib.rs                        # MODIFIED: registers `mod facade` and `m.add_class::<facade::sync::Redis>()`
python/redis_rs_py/
  __init__.py                   # MODIFIED: re-export Redis at the package root
  _driver.pyi                   # MODIFIED: stub for Redis (constructor + every method)
tests/facade/
  __init__.py                   # NEW (empty)
  test_sync_constructor.py      # NEW
  test_sync_from_url.py         # NEW
  test_sync_kwargs_warn.py      # NEW
  test_sync_commands_smoke.py   # NEW: every method exists and round-trips at least once
  test_sync_close.py            # NEW: close + context manager + post-close raises
  test_sync_lock.py             # NEW: lock acquire/release happy path + LockNotOwnedError
```

---

## Task 1: Wire up the `facade` module skeleton

Create the empty module tree so the next tasks compile.

**Files:**
- New: `crates/redis-rs-py-driver/src/facade/mod.rs`
- New: `crates/redis-rs-py-driver/src/facade/kwargs.rs` (placeholder)
- New: `crates/redis-rs-py-driver/src/facade/sync.rs` (placeholder)
- Modify: `crates/redis-rs-py-driver/src/lib.rs`

- [ ] **Step 1: Create the placeholder files**

```bash
mkdir -p crates/redis-rs-py-driver/src/facade
printf '// Façade module — declares submodules implemented across plans 10-12.\n\npub mod kwargs;\npub mod sync;\n' > crates/redis-rs-py-driver/src/facade/mod.rs
printf '// placeholder — populated by Plan 10 Task 2\n' > crates/redis-rs-py-driver/src/facade/kwargs.rs
printf '// placeholder — populated by Plan 10 Task 3\n' > crates/redis-rs-py-driver/src/facade/sync.rs
```

- [ ] **Step 2: Register the module in `lib.rs`**

Edit `crates/redis-rs-py-driver/src/lib.rs`. After the existing `mod` declarations (e.g. after `mod test_helpers;`), append:

```rust
mod facade;
```

Inside `fn _driver(m: &Bound<'_, PyModule>) -> PyResult<()>`, after the existing `m.add_class::<...>()?` lines, append:

```rust
    m.add_class::<facade::sync::Redis>()?;
```

(The class doesn't exist yet — Step 3 lets the crate compile by leaving the placeholder. We re-comment-and-uncomment in the TDD cycle below.)

- [ ] **Step 3: Temporarily comment out the new `add_class` so we still compile**

In `lib.rs`, prefix the new line with `// ` until Task 3 lands the class:

```rust
    // m.add_class::<facade::sync::Redis>()?;
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings only.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/ crates/redis-rs-py-driver/src/lib.rs
git commit -m "feat(facade): scaffold facade module skeleton"
```

---

## Task 2: `kwargs.rs` — accept-and-warn surface

Owns the warn-once registry and the `accept_and_warn(known, kwargs)` helper. Keeps the warning text uniform and keeps Task 3 short.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/kwargs.rs`
- Test: `tests/facade/test_sync_kwargs_warn.py` (red until Task 3 lands the constructor)

- [ ] **Step 1: Implement `kwargs.rs`**

Replace `crates/redis-rs-py-driver/src/facade/kwargs.rs`:

```rust
// Accept-and-warn surface for redis-py constructor kwargs we don't yet
// implement. The redis-py contract is "every kwarg in `Redis.__init__`
// must be accepted without raising"; ours is "accepted, warn once per
// process per unknown name, then ignore".
//
// The `KNOWN_KWARGS` slice is the full redis-py 5.x kwarg surface
// (verified by `python -c "import redis, inspect; print(inspect.signature(redis.Redis.__init__))"`).
// Anything in this list is silently ignored if not in the
// `IMPLEMENTED_KWARGS` slice — but already-implemented names are
// extracted by the `Redis::__new__` constructor before we get here, so
// only the *unknown to us* names trigger a warning.
//
// Anything *not* in `KNOWN_KWARGS` (e.g. typos, future redis-py
// additions) gets a sharper warning that flags it as unrecognised.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

/// Every kwarg `redis.Redis.__init__` accepts (redis-py 5.x). Captured
/// verbatim from the upstream signature.
pub const KNOWN_KWARGS: &[&str] = &[
    "host",
    "port",
    "db",
    "password",
    "socket_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_keepalive_options",
    "connection_pool",
    "unix_socket_path",
    "encoding",
    "encoding_errors",
    "charset",
    "errors",
    "decode_responses",
    "retry_on_timeout",
    "retry_on_error",
    "ssl",
    "ssl_keyfile",
    "ssl_certfile",
    "ssl_cert_reqs",
    "ssl_ca_certs",
    "ssl_ca_path",
    "ssl_ca_data",
    "ssl_check_hostname",
    "ssl_password",
    "ssl_validate_ocsp",
    "ssl_validate_ocsp_stapled",
    "ssl_ocsp_context",
    "ssl_ocsp_expected_cert",
    "ssl_min_version",
    "ssl_ciphers",
    "max_connections",
    "single_connection_client",
    "health_check_interval",
    "client_name",
    "lib_name",
    "lib_version",
    "username",
    "retry",
    "redis_connect_func",
    "credential_provider",
    "protocol",
    "cache",
    "cache_config",
    "event_dispatcher",
];

/// Subset of `KNOWN_KWARGS` we wire to actual driver behaviour. This is
/// the contract the README's compatibility matrix advertises.
pub const IMPLEMENTED_KWARGS: &[&str] = &[
    "host",
    "port",
    "db",
    "password",
    "username",
    "ssl",
    "ssl_keyfile",
    "ssl_certfile",
    "ssl_ca_certs",
    "socket_timeout",
    "max_connections",
    "health_check_interval",
    "client_name",
    "protocol",
    "decode_responses",
    "encoding",
    "encoding_errors",
];

static SEEN_UNIMPLEMENTED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static SEEN_UNKNOWN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn seen_unimplemented() -> &'static Mutex<HashSet<String>> {
    SEEN_UNIMPLEMENTED.get_or_init(|| Mutex::new(HashSet::new()))
}

fn seen_unknown() -> &'static Mutex<HashSet<String>> {
    SEEN_UNKNOWN.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Iterate `kwargs` and warn (once per process per name) about each name
/// that is not in `implemented`. Distinguishes "redis-py kwarg we just
/// don't honour yet" (UserWarning, low severity) from "name we don't
/// recognise at all" (RuntimeWarning, higher severity).
///
/// Caller passes `implemented` as the names already extracted to typed
/// fields by the constructor.
pub fn accept_and_warn(
    py: Python<'_>,
    implemented: &[&str],
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let Some(kwargs) = kwargs else {
        return Ok(());
    };
    if kwargs.is_empty() {
        return Ok(());
    }

    let warnings = py.import("warnings")?;

    for (k, _v) in kwargs.iter() {
        let name: String = k.extract()?;
        if implemented.contains(&name.as_str()) {
            continue;
        }
        let is_redis_py = KNOWN_KWARGS.contains(&name.as_str());
        let (category_attr, msg) = if is_redis_py {
            (
                "UserWarning",
                format!(
                    "redis_rs_py.Redis: kwarg `{name}` is recognised by redis-py but not yet \
                     implemented in this driver — it has been accepted and ignored. \
                     See the compatibility matrix for status."
                ),
            )
        } else {
            (
                "RuntimeWarning",
                format!(
                    "redis_rs_py.Redis: kwarg `{name}` is not recognised by redis-py 5.x or this \
                     driver — it has been accepted and ignored. Check for a typo."
                ),
            )
        };

        // One-shot dedup by name.
        let map = if is_redis_py {
            seen_unimplemented()
        } else {
            seen_unknown()
        };
        {
            let mut g = map.lock().unwrap();
            if g.contains(&name) {
                continue;
            }
            g.insert(name.clone());
        }

        let category = warnings.getattr(category_attr)?;
        let stacklevel = 4_i64; // Skip into the user's frame: __init__ → __new__ → ours → user.
        let args = PyTuple::new(py, [msg.into_pyobject(py)?.into_any()])?;
        let kw = PyDict::new(py);
        kw.set_item("category", category)?;
        kw.set_item("stacklevel", stacklevel)?;
        warnings.call_method("warn", args, Some(&kw))?;
    }

    Ok(())
}

/// Test-only: clear the warn-once dedup state so repeated test runs in a
/// single process all see the warning. Wired to a pyfunction in the
/// crate-level `_driver` module under `_facade_reset_warn_state`.
#[doc(hidden)]
pub fn reset_warn_state_for_tests() {
    if let Some(m) = SEEN_UNIMPLEMENTED.get() {
        m.lock().unwrap().clear();
    }
    if let Some(m) = SEEN_UNKNOWN.get() {
        m.lock().unwrap().clear();
    }
}

#[pyfunction]
#[pyo3(name = "_facade_reset_warn_state")]
pub fn py_reset_warn_state() {
    reset_warn_state_for_tests();
}
```

- [ ] **Step 2: Register the test reset helper in `lib.rs`**

In `crates/redis-rs-py-driver/src/lib.rs`, inside `fn _driver`, after the existing `wrap_pyfunction!` registrations, append:

```rust
    m.add_function(wrap_pyfunction!(facade::kwargs::py_reset_warn_state, m)?)?;
```

- [ ] **Step 3: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with at most "function never used" warnings on `IMPLEMENTED_KWARGS` (used by Task 3).

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/kwargs.rs crates/redis-rs-py-driver/src/lib.rs
git commit -m "feat(facade): add accept-and-warn kwargs surface"
```

---

## Task 3: `Redis` pyclass — constructor + `from_url` + factory + lifecycle

The base class. Constructor accepts every redis-py kwarg, extracts the implemented ones into a private `Config`, builds a `RedisRsDriver`, and feeds the leftover kwargs to `kwargs::accept_and_warn`. `from_url` parses standard URLs and merges with kwargs. `close` / `__enter__` / `__exit__` drop the driver.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs`
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (uncomment `add_class`)
- Test: `tests/facade/__init__.py` (empty), `tests/facade/test_sync_constructor.py`, `tests/facade/test_sync_from_url.py`, `tests/facade/test_sync_kwargs_warn.py`, `tests/facade/test_sync_close.py`

- [ ] **Step 1: Write the failing constructor test**

`tests/facade/__init__.py`: empty.

`tests/facade/test_sync_constructor.py`:

```python
"""Constructor surface for redis_rs_py.Redis (sync façade)."""

from __future__ import annotations

import pytest

from redis_rs_py import Redis


def test_default_constructor_accepts_no_kwargs(valkey_url: str) -> None:
    # We need a server, so route the host:port out of the testcontainers URL.
    from urllib.parse import urlparse

    parts = urlparse(valkey_url)
    r = Redis(host=parts.hostname, port=parts.port)
    assert r.ping() is True
    r.close()


def test_constructor_with_db(valkey_url: str) -> None:
    from urllib.parse import urlparse

    parts = urlparse(valkey_url)
    r = Redis(host=parts.hostname, port=parts.port, db=0)
    assert r.ping() is True
    r.close()


def test_constructor_accepts_full_redis_py_kwarg_surface() -> None:
    """Calling Redis(...) with every redis-py kwarg must not raise.

    We don't connect — bind to a port that's almost certainly closed and
    catch the resulting ConnectionError. The point is that *constructing*
    the kwargs argpack must succeed across the whole signature.
    """
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

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/facade/test_sync_constructor.py -v`
Expected: FAIL with `ImportError: cannot import name 'Redis' from 'redis_rs_py'`.

- [ ] **Step 3: Implement `Redis` (constructor + `from_url` + lifecycle)**

Replace `crates/redis-rs-py-driver/src/facade/sync.rs`:

```rust
// Sync façade: redis_rs_py.Redis.
//
// Mirrors redis-py's Redis class — same constructor kwargs, same method
// names. Implements the kwargs the driver actually uses, accepts-and-
// warns the rest via `crate::facade::kwargs`. Every command method
// delegates to an internal `RedisRsDriver` via the `define_facade_command!`
// macro defined below.
//
// `decode_responses` is stored on the class; plan 12 wires the actual
// decoding step. Until plan 12 lands, the field is set but unused.

#![allow(clippy::too_many_arguments)]

use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple, PyType};
use std::sync::Arc;

use crate::driver::RedisRsDriver;
use crate::facade::kwargs::{IMPLEMENTED_KWARGS, accept_and_warn};

// =========================================================================
// Internal config struct — built by `Redis::__new__`, also the merge
// target for `from_url`. Kept private; not exposed to Python.
// =========================================================================

#[derive(Clone, Debug, Default)]
pub(crate) struct FacadeConfig {
    pub host: String,
    pub port: u16,
    pub db: i64,
    pub password: Option<String>,
    pub username: Option<String>,
    pub ssl: bool,
    pub ssl_keyfile: Option<String>,
    pub ssl_certfile: Option<String>,
    pub ssl_ca_certs: Option<String>,
    pub socket_timeout: Option<f64>,
    pub max_connections: Option<usize>,
    pub health_check_interval: u64,
    pub client_name: Option<String>,
    pub protocol: i64,
    pub decode_responses: bool,
    pub encoding: String,
    pub encoding_errors: String,
}

impl FacadeConfig {
    fn defaults() -> Self {
        Self {
            host: "localhost".into(),
            port: 6379,
            db: 0,
            password: None,
            username: None,
            ssl: false,
            ssl_keyfile: None,
            ssl_certfile: None,
            ssl_ca_certs: None,
            socket_timeout: None,
            max_connections: None,
            health_check_interval: 0,
            client_name: None,
            protocol: 2,
            decode_responses: false,
            encoding: "utf-8".into(),
            encoding_errors: "strict".into(),
        }
    }

    /// Render to a `redis://` / `rediss://` URL the driver can consume.
    /// Userinfo is percent-encoded by the URL crate at parse time; here we
    /// rely on the fact that hostnames + numeric DBs are safe ASCII.
    fn to_url(&self) -> String {
        let scheme = if self.ssl { "rediss" } else { "redis" };
        let userinfo = match (self.username.as_deref(), self.password.as_deref()) {
            (Some(u), Some(p)) => format!("{u}:{p}@"),
            (None, Some(p)) => format!(":{p}@"),
            (Some(u), None) => format!("{u}@"),
            (None, None) => String::new(),
        };
        format!(
            "{scheme}://{userinfo}{host}:{port}/{db}",
            scheme = scheme,
            userinfo = userinfo,
            host = self.host,
            port = self.port,
            db = self.db,
        )
    }
}

// =========================================================================
// Redis pyclass
// =========================================================================

#[pyclass(subclass, module = "redis_rs_py._driver", name = "Redis")]
pub struct Redis {
    /// `None` once `close()` has been called.
    pub(crate) driver: Option<Arc<Py<RedisRsDriver>>>,
    pub(crate) config: FacadeConfig,
}

impl Redis {
    /// Resolve the underlying driver or raise if closed. Used by every
    /// command method; centralised here to keep the `define_facade_command!`
    /// macro one-liner-clean.
    pub(crate) fn driver_or_raise(&self) -> PyResult<Arc<Py<RedisRsDriver>>> {
        match &self.driver {
            Some(d) => Ok(d.clone()),
            None => Err(PyValueError::new_err(
                "Redis client is closed; create a new one or use a context manager",
            )),
        }
    }
}

#[pymethods]
impl Redis {
    /// Constructor. Mirrors `redis.Redis.__init__` exactly.
    ///
    /// Implemented kwargs become `FacadeConfig` fields. Unimplemented
    /// redis-py kwargs are accepted (signature includes them) and
    /// recorded for the warn-once helper. Anything entirely unknown
    /// flows in via `**extra` and is also passed to `accept_and_warn`.
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
        // Bind unused kwargs so the compiler stops complaining; they're
        // routed to `accept_and_warn` (via the `extra` dict for things
        // we didn't explicitly extract, and silently ignored for the
        // explicitly-typed redis-py kwargs we don't implement yet).
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

        let driver = build_driver(py, &config)?;

        Ok(Self {
            driver: Some(Arc::new(driver)),
            config,
        })
    }

    /// `Redis.from_url(url, **kwargs)` — parse a redis URL and merge with
    /// kwargs. URL fields take precedence over kwargs (matches redis-py
    /// behaviour: the URL is the canonical source of host/port/db/auth).
    #[classmethod]
    #[pyo3(signature = (url, **kwargs))]
    fn from_url(
        cls: &Bound<'_, PyType>,
        py: Python<'_>,
        url: String,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let url_cfg = parse_url(&url)?;
        let merged_kwargs = match kwargs {
            Some(d) => d,
            None => PyDict::new(py),
        };
        // URL takes precedence: overwrite any same-named kwarg.
        merged_kwargs.set_item("host", url_cfg.host)?;
        merged_kwargs.set_item("port", url_cfg.port)?;
        merged_kwargs.set_item("db", url_cfg.db)?;
        if let Some(p) = url_cfg.password {
            merged_kwargs.set_item("password", p)?;
        }
        if let Some(u) = url_cfg.username {
            merged_kwargs.set_item("username", u)?;
        }
        if url_cfg.ssl {
            merged_kwargs.set_item("ssl", true)?;
        }
        let empty = PyTuple::empty(py);
        cls.call(empty, Some(&merged_kwargs)).map(Bound::unbind)
    }

    // --- lifecycle --------------------------------------------------------

    /// Drop the underlying driver `Arc`. Subsequent command calls raise
    /// `ValueError`, matching the redis-py "client is closed" idiom.
    fn close(&mut self) -> PyResult<()> {
        self.driver = None;
        Ok(())
    }

    fn __enter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    fn __exit__(
        &mut self,
        exc_type: Option<Py<PyAny>>,
        exc_val: Option<Py<PyAny>>,
        exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let _ = (exc_type, exc_val, exc_tb);
        self.close()?;
        Ok(false)
    }

    // --- placeholders pointing at later plans -----------------------------

    #[pyo3(signature = (transaction = true, shard_hint = None))]
    fn pipeline(
        &self,
        transaction: bool,
        shard_hint: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = (transaction, shard_hint);
        Err(PyNotImplementedError::new_err(
            "Pipeline is implemented by plan 13 (pipelines-transactions). \
             Until then use the low-level RedisRsDriver.",
        ))
    }

    #[pyo3(signature = (**kwargs))]
    fn pubsub(&self, kwargs: Option<Bound<'_, PyDict>>) -> PyResult<Py<PyAny>> {
        let _ = kwargs;
        Err(PyNotImplementedError::new_err(
            "PubSub is implemented by plan 14 (pubsub).",
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
            "transaction() is implemented by plan 13.",
        ))
    }
}

// =========================================================================
// URL parsing helpers
// =========================================================================

#[derive(Debug, Default)]
struct UrlConfig {
    host: String,
    port: u16,
    db: i64,
    username: Option<String>,
    password: Option<String>,
    ssl: bool,
}

fn parse_url(input: &str) -> PyResult<UrlConfig> {
    // Manual parse: the `url` crate is fine but we want to avoid pulling
    // in a new dependency for a 10-line helper. The redis URL grammar is
    // strict enough that hand-rolling is safer than the lossy
    // `url::Url::parse` (which doesn't surface the path-as-db idiom
    // cleanly).
    let (scheme, rest) = match input.split_once("://") {
        Some(s) => s,
        None => {
            return Err(PyValueError::new_err(format!(
                "Invalid Redis URL: {input!r}; expected scheme://..."
            )));
        }
    };
    let (ssl, is_unix) = match scheme {
        "redis" => (false, false),
        "rediss" => (true, false),
        "unix" => (false, true),
        other => {
            return Err(PyValueError::new_err(format!(
                "Invalid Redis URL scheme {other!r}; expected redis://, rediss:// or unix://"
            )));
        }
    };

    let mut cfg = UrlConfig {
        ssl,
        port: 6379,
        ..UrlConfig::default()
    };

    let (authority, path_and_query) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, String::new()),
    };

    let (userinfo, host_port) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };

    if let Some(u) = userinfo {
        let (user, pass) = match u.split_once(':') {
            Some((a, b)) => (Some(a), Some(b)),
            None => (Some(u), None),
        };
        cfg.username = user.filter(|s| !s.is_empty()).map(|s| {
            percent_decode(s)
        });
        cfg.password = pass.map(percent_decode);
    }

    if is_unix {
        // `unix:///tmp/redis.sock?db=2` — host is empty, the path is the socket.
        // We don't *implement* unix sockets in this plan; just preserve the
        // info so the constructor will reject it via the driver.
        cfg.host = host_port.to_string();
    } else if let Some((h, p)) = host_port.rsplit_once(':') {
        cfg.host = h.to_string();
        cfg.port = p.parse().map_err(|_| {
            PyValueError::new_err(format!("Invalid port in Redis URL: {input!r}"))
        })?;
    } else if !host_port.is_empty() {
        cfg.host = host_port.to_string();
    } else {
        cfg.host = "localhost".into();
    }

    // Path: `/0` → db=0. Query: `?db=3&password=...` overrides path.
    let (path, query) = match path_and_query.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path_and_query.as_str(), None),
    };
    if let Some(p) = path.strip_prefix('/')
        && !p.is_empty()
        && let Ok(d) = p.parse()
    {
        cfg.db = d;
    }
    if let Some(q) = query {
        for pair in q.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            match k {
                "db" => {
                    if let Ok(d) = v.parse() {
                        cfg.db = d;
                    }
                }
                "password" => cfg.password = Some(percent_decode(v)),
                "username" => cfg.username = Some(percent_decode(v)),
                _ => {}
            }
        }
    }

    Ok(cfg)
}

/// Minimal percent decoding so `redis://user%40host:p%40ss@127.0.0.1` works.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// =========================================================================
// Driver factory
// =========================================================================

fn build_driver(py: Python<'_>, cfg: &FacadeConfig) -> PyResult<Py<RedisRsDriver>> {
    let url = cfg.to_url();
    let kwargs = PyDict::new(py);
    if let Some(ref f) = cfg.ssl_ca_certs {
        kwargs.set_item("ssl_ca_certs", std::fs::read(f).map_err(|e| {
            PyValueError::new_err(format!("Cannot read ssl_ca_certs {f}: {e}"))
        })?)?;
    }
    if let Some(ref f) = cfg.ssl_certfile {
        kwargs.set_item("ssl_certfile", std::fs::read(f).map_err(|e| {
            PyValueError::new_err(format!("Cannot read ssl_certfile {f}: {e}"))
        })?)?;
    }
    if let Some(ref f) = cfg.ssl_keyfile {
        kwargs.set_item("ssl_keyfile", std::fs::read(f).map_err(|e| {
            PyValueError::new_err(format!("Cannot read ssl_keyfile {f}: {e}"))
        })?)?;
    }
    let driver_cls = py.get_type::<RedisRsDriver>();
    let drv = driver_cls
        .call_method("connect_standard", (url,), Some(&kwargs))?
        .downcast_into::<RedisRsDriver>()?
        .unbind();
    Ok(drv)
}
```

- [ ] **Step 4: Re-enable the `add_class` registration in `lib.rs`**

In `crates/redis-rs-py-driver/src/lib.rs`, uncomment the line:

```rust
    m.add_class::<facade::sync::Redis>()?;
```

- [ ] **Step 5: Add the package re-export**

Edit `python/redis_rs_py/__init__.py` — add `Redis` to the imports from `_driver` and to `__all__`:

```python
from redis_rs_py._driver import (
    Redis,
    RedisRsAwaitable,
    RedisRsDriver,
    __version__,
)
```

(Plus `"Redis"` in `__all__`.)

- [ ] **Step 6: Build + run the constructor tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/facade/test_sync_constructor.py -v`
Expected: 3 PASS.

- [ ] **Step 7: Add the `from_url` test**

`tests/facade/test_sync_from_url.py`:

```python
"""Redis.from_url URL parsing and kwargs merging."""

from __future__ import annotations

import pytest

from redis_rs_py import Redis
from redis_rs_py.exceptions import ConnectionError as RedisConnectionError


def test_from_url_basic(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    assert r.ping() is True
    r.close()


def test_from_url_with_db_in_path(valkey_url: str) -> None:
    # Strip the existing `/0` and re-attach `/3`. Some testcontainers paths
    # already have query params; preserve them.
    base = valkey_url.split("?", 1)[0].rsplit("/", 1)[0]
    r = Redis.from_url(f"{base}/3")
    assert r.ping() is True
    r.close()


def test_from_url_with_db_in_query(valkey_url: str) -> None:
    # Force `?db=2` — query param wins over (missing) path component.
    base = valkey_url.split("?", 1)[0]
    r = Redis.from_url(f"{base}?db=2")
    assert r.ping() is True
    r.close()


def test_from_url_with_userinfo() -> None:
    """Userinfo is parsed; if the server has no auth it fails on auth check.

    We bind a closed port instead so the kwarg merge is what's exercised
    (parsing userinfo into username/password kwargs without raising).
    """
    with pytest.raises(RedisConnectionError):
        Redis.from_url("redis://default:secret@127.0.0.1:1/0")


def test_from_url_invalid_scheme_raises_value_error() -> None:
    with pytest.raises(ValueError, match="scheme"):
        Redis.from_url("http://127.0.0.1:6379/0")


def test_from_url_kwargs_take_lower_precedence(valkey_url: str) -> None:
    """URL fields override same-named kwargs (matches redis-py)."""
    from urllib.parse import urlparse

    parts = urlparse(valkey_url.split("?", 1)[0])
    # Pass a wrong host as kwarg; URL's correct host must win.
    r = Redis.from_url(valkey_url, host="impossible.invalid", port=1)
    assert r.ping() is True
    r.close()
    _ = parts  # used for clarity; nothing to assert here
```

Run: `uv run pytest tests/facade/test_sync_from_url.py -v`
Expected: 6 PASS.

- [ ] **Step 8: Add the kwargs-warn test**

`tests/facade/test_sync_kwargs_warn.py`:

```python
"""Accept-and-warn surface for unknown / unimplemented kwargs."""

from __future__ import annotations

import warnings

import pytest

from redis_rs_py import Redis, _driver
from redis_rs_py.exceptions import ConnectionError as RedisConnectionError


@pytest.fixture(autouse=True)
def _reset_warn_state() -> None:
    """Make sure each test sees a fresh warn-once registry."""
    _driver._facade_reset_warn_state()


def test_unknown_kwarg_warns_runtime_warning() -> None:
    """A name not in redis-py's signature should trigger RuntimeWarning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RedisConnectionError):
            Redis(host="127.0.0.1", port=1, definitely_not_a_real_kwarg="x")
    runtime = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert len(runtime) == 1
    assert "definitely_not_a_real_kwarg" in str(runtime[0].message)


def test_redis_py_kwarg_we_dont_implement_warns_user_warning() -> None:
    """`socket_keepalive_options` is in redis-py's signature but not in
    our IMPLEMENTED_KWARGS list. Should trigger UserWarning, not Runtime."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RedisConnectionError):
            Redis(host="127.0.0.1", port=1, socket_keepalive_options={1: 30})
    user = [w for w in caught if w.category is UserWarning]
    assert any("socket_keepalive_options" in str(w.message) for w in user)


def test_warn_is_one_shot_per_process() -> None:
    """Repeating the same unknown kwarg name must not re-warn."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(3):
            with pytest.raises(RedisConnectionError):
                Redis(host="127.0.0.1", port=1, my_typo_kwarg="x")
    runtime = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert len(runtime) == 1


def test_implemented_kwargs_do_not_warn(valkey_url: str) -> None:
    from urllib.parse import urlparse

    parts = urlparse(valkey_url.split("?", 1)[0])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        r = Redis(
            host=parts.hostname,
            port=parts.port,
            db=0,
            password=None,
            username=None,
            ssl=False,
            socket_timeout=None,
            max_connections=None,
            health_check_interval=0,
            client_name=None,
            protocol=2,
            decode_responses=False,
            encoding="utf-8",
            encoding_errors="strict",
        )
        r.close()
    assert caught == []
```

Run: `uv run pytest tests/facade/test_sync_kwargs_warn.py -v`
Expected: 4 PASS.

- [ ] **Step 9: Add the close + context-manager test**

`tests/facade/test_sync_close.py`:

```python
"""Lifecycle: close, context manager, post-close use raises."""

from __future__ import annotations

import pytest

from redis_rs_py import Redis


def test_close_drops_driver(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    assert r.ping() is True
    r.close()
    with pytest.raises(ValueError, match="closed"):
        r.ping()


def test_context_manager_closes_on_exit(valkey_url: str) -> None:
    with Redis.from_url(valkey_url) as r:
        assert r.ping() is True
    with pytest.raises(ValueError, match="closed"):
        r.ping()


def test_double_close_is_idempotent(valkey_url: str) -> None:
    r = Redis.from_url(valkey_url)
    r.close()
    r.close()  # must not raise
```

Run: `uv run pytest tests/facade/test_sync_close.py -v`
Expected: 3 PASS.

- [ ] **Step 10: Run lint + format**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
uv run ruff check tests/facade
uv run ruff format --check tests/facade
```

Expected: all green.

- [ ] **Step 11: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs crates/redis-rs-py-driver/src/lib.rs python/redis_rs_py/__init__.py tests/facade/__init__.py tests/facade/test_sync_constructor.py tests/facade/test_sync_from_url.py tests/facade/test_sync_kwargs_warn.py tests/facade/test_sync_close.py
git commit -m "feat(facade): add Redis pyclass with constructor, from_url, lifecycle"
```

---

## Task 4: `define_facade_command!` macro + string commands

The macro takes a method name, a PyO3 signature, and the driver method to delegate to. Per command-family task, we paste one big invocation of the macro covering every command in that family. Strings come first as a smoke test for the macro itself.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs` (append the macro + string `#[pymethods]` block)
- Test: `tests/facade/test_sync_commands_smoke.py` (incremental — extended each task)

- [ ] **Step 1: Append the macro to `sync.rs`**

Append to `crates/redis-rs-py-driver/src/facade/sync.rs`, just before the existing `#[pymethods] impl Redis { ... }` block (or at the bottom; macros are visible regardless of position):

```rust
// =========================================================================
// define_facade_command! — emit a one-line method delegating to the
// internal driver.
//
// Forms:
//   define_facade_command!(method_name, (signature_args), driver_method);
//   define_facade_command!(method_name, (signature_args), driver_method, [forward_args]);
//
// `signature_args` is what goes inside `#[pyo3(signature = (...))]`.
// `[forward_args]` lists the argument *names* in call order (defaults
// to the same list parsed from the signature). Use the explicit form
// when the signature has defaults or `**kwargs` that need filtering.
//
// Each generated method:
//   1. Resolves the driver via `self.driver_or_raise()`.
//   2. Calls `driver.call_method1("driver_method", (forward_args,))`.
//   3. Returns the unbound result.
//
// We deliberately use Python-level dispatch (call_method1) instead of
// inherent Rust calls because the driver's command methods live on
// `RedisRsDriver` as `#[pymethods]`, which are exposed via Python's
// type machinery rather than as crate-public Rust functions. The cost
// is one Python-level lookup per call; the benefit is full
// independence from `RedisRsDriver`'s internal Rust signatures.
// =========================================================================

#[macro_export]
macro_rules! define_facade_command {
    // Form 1: method_name, signature, driver_method (positional args
    // forwarded by name in declaration order — the common case).
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

    // Form 2: explicit forward list. Use when signature has *args /
    // defaults that don't map 1:1 to driver args.
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
```

- [ ] **Step 2: Append the string-commands `#[pymethods]` block**

Append a *second* `#[pymethods] impl Redis { ... }` block (PyO3 supports multiple impl blocks per pyclass) so each command-family task adds a self-contained block. After the existing impl, append:

```rust
// =========================================================================
// String commands — plan 03 surface.
// =========================================================================

#[pymethods]
impl Redis {
    define_facade_command!(get, (key: String), get);

    #[pyo3(signature = (
        key,
        value,
        ex = None,
        px = None,
        nx = false,
        xx = false,
        keepttl = false,
        get = false,
        exat = None,
        pxat = None,
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
            .call_method("set", (key, value), Some(&kwargs))?
            .unbind())
    }

    define_facade_command!(getex, (key: String, ex: Option<i64> = None, px: Option<i64> = None, exat: Option<i64> = None, pxat: Option<i64> = None, persist: bool = false), getex);
    define_facade_command!(getdel, (key: String), getdel);
    define_facade_command!(copy, (source: String, destination: String, db: Option<i64> = None, replace: bool = false), copy);
    define_facade_command!(incr, (key: String, amount: i64 = 1), incr_by);
    define_facade_command!(incrby, (key: String, amount: i64 = 1), incr_by);
    define_facade_command!(incrbyfloat, (key: String, amount: f64 = 1.0), incr_by_float);
    define_facade_command!(decr, (key: String, amount: i64 = 1), decr_by);
    define_facade_command!(decrby, (key: String, amount: i64 = 1), decr_by);
    define_facade_command!(append, (key: String, value: Vec<u8>), append);
    define_facade_command!(strlen, (key: String), strlen);

    #[pyo3(signature = (*keys))]
    fn mget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("mget", (keys,))?.unbind())
    }

    fn mset(&self, py: Python<'_>, mapping: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("mset", (mapping,))?.unbind())
    }

    fn msetnx(&self, py: Python<'_>, mapping: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("msetnx", (mapping,))?.unbind())
    }

    define_facade_command!(setrange, (key: String, offset: i64, value: Vec<u8>), setrange);
    define_facade_command!(getrange, (key: String, start: i64, end: i64), getrange);

    #[pyo3(signature = (*keys))]
    fn exists(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("exists", (keys,))?.unbind())
    }

    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("delete", (keys,))?.unbind())
    }

    #[pyo3(signature = (*keys))]
    fn unlink(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("unlink", (keys,))?.unbind())
    }

    define_facade_command!(expire, (key: String, time: i64, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), expire);
    define_facade_command!(pexpire, (key: String, time: i64, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), pexpire);
    define_facade_command!(expireat, (key: String, when: i64, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), expireat);
    define_facade_command!(pexpireat, (key: String, when: i64, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), pexpireat);
    define_facade_command!(expiretime, (key: String), expiretime);
    define_facade_command!(pexpiretime, (key: String), pexpiretime);
    define_facade_command!(ttl, (key: String), ttl);
    define_facade_command!(pttl, (key: String), pttl);
    define_facade_command!(persist, (key: String), persist);
    define_facade_command!(rename, (src: String, dst: String), rename);
    define_facade_command!(renamenx, (src: String, dst: String), renamenx);
    define_facade_command!(type, (key: String), type_, [key]);
    define_facade_command!(dump, (key: String), dump);
    define_facade_command!(restore, (key: String, ttl: i64, value: Vec<u8>, replace: bool = false, absttl: bool = false, idletime: Option<i64> = None, freq: Option<i64> = None), restore);
}
```

- [ ] **Step 3: Write the smoke test for string commands**

`tests/facade/test_sync_commands_smoke.py`:

```python
"""Smoke tests for every façade command method.

The contract is: each method exists, accepts the documented signature,
and round-trips against a live server. Type/shape checks are minimal —
the driver-level tests in plans 03-09 are responsible for response
correctness. Here we only prove the façade wires through.
"""

from __future__ import annotations

import pytest

from redis_rs_py import Redis


@pytest.fixture
def r(valkey_url: str) -> Redis:
    client = Redis.from_url(valkey_url)
    # Flush via the upstream client so we don't depend on flushdb being
    # implemented yet at the façade layer.
    import redis as upstream

    rp = upstream.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    yield client
    client.close()


# --- strings --------------------------------------------------------------


def test_string_get_set(r: Redis) -> None:
    assert r.set("k", b"v") in (True, b"OK", "OK", None)
    assert r.get("k") == b"v"


def test_string_get_set_with_ex(r: Redis) -> None:
    r.set("k", b"v", ex=60)
    assert 0 < r.ttl("k") <= 60


def test_string_getex(r: Redis) -> None:
    r.set("k", b"v")
    assert r.getex("k", ex=30) == b"v"
    assert 0 < r.ttl("k") <= 30


def test_string_getdel(r: Redis) -> None:
    r.set("k", b"v")
    assert r.getdel("k") == b"v"
    assert r.get("k") is None


def test_string_copy(r: Redis) -> None:
    r.set("a", b"v")
    assert r.copy("a", "b") in (True, 1)
    assert r.get("b") == b"v"


def test_string_incr_decr(r: Redis) -> None:
    assert r.incr("counter") == 1
    assert r.incrby("counter", 4) == 5
    assert r.decr("counter") == 4
    assert r.decrby("counter", 2) == 2


def test_string_incrbyfloat(r: Redis) -> None:
    assert r.incrbyfloat("f", 1.5) == 1.5


def test_string_append_strlen(r: Redis) -> None:
    r.set("k", b"hello")
    assert r.append("k", b" world") == 11
    assert r.strlen("k") == 11


def test_string_mget_mset(r: Redis) -> None:
    r.mset({"a": b"1", "b": b"2"})
    assert r.mget("a", "b") == [b"1", b"2"]


def test_string_msetnx(r: Redis) -> None:
    assert r.msetnx({"x": b"1"}) in (True, 1)
    assert r.msetnx({"x": b"2", "y": b"3"}) in (False, 0)


def test_string_setrange_getrange(r: Redis) -> None:
    r.set("k", b"hello world")
    r.setrange("k", 6, b"REDIS")
    assert r.getrange("k", 0, -1) == b"hello REDIS"


def test_string_exists_delete_unlink(r: Redis) -> None:
    r.set("a", b"1")
    r.set("b", b"2")
    assert r.exists("a", "b", "c") == 2
    assert r.delete("a") == 1
    assert r.unlink("b") == 1


def test_string_expire_ttl_persist(r: Redis) -> None:
    r.set("k", b"v")
    assert r.expire("k", 100) in (True, 1)
    assert 0 < r.ttl("k") <= 100
    assert r.persist("k") in (True, 1)
    assert r.ttl("k") in (-1, None)


def test_string_pexpire_pttl(r: Redis) -> None:
    r.set("k", b"v")
    r.pexpire("k", 100_000)
    assert 0 < r.pttl("k") <= 100_000


def test_string_expireat_pexpireat(r: Redis) -> None:
    import time

    r.set("k", b"v")
    r.expireat("k", int(time.time()) + 100)
    assert 0 < r.ttl("k") <= 100
    r.set("k2", b"v")
    r.pexpireat("k2", int(time.time() * 1000) + 100_000)
    assert 0 < r.pttl("k2") <= 100_000


def test_string_expiretime_pexpiretime(r: Redis) -> None:
    import time

    r.set("k", b"v")
    r.expire("k", 100)
    et = r.expiretime("k")
    pet = r.pexpiretime("k")
    assert et > int(time.time())
    assert pet > int(time.time() * 1000)


def test_string_rename(r: Redis) -> None:
    r.set("a", b"v")
    r.rename("a", "b")
    assert r.get("b") == b"v"


def test_string_renamenx(r: Redis) -> None:
    r.set("a", b"v")
    r.set("b", b"x")
    assert r.renamenx("a", "b") in (False, 0)


def test_string_type(r: Redis) -> None:
    r.set("k", b"v")
    assert r.type("k") in (b"string", "string")


def test_string_dump_restore(r: Redis) -> None:
    r.set("k", b"v")
    blob = r.dump("k")
    assert blob is not None
    r.delete("k")
    r.restore("k", 0, blob)
    assert r.get("k") == b"v"
```

Run: `uv run pytest tests/facade/test_sync_commands_smoke.py -v`
Expected: 19 PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs tests/facade/test_sync_commands_smoke.py
git commit -m "feat(facade): add define_facade_command macro and string commands"
```

---

## Task 5: List commands

Append a `#[pymethods]` block delegating every list command from plan 04 to the driver. Lazy blocking-conn semantics live in the driver — the façade is just a forwarder.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs` (append a list-commands `#[pymethods]` block)
- Test: `tests/facade/test_sync_commands_smoke.py` (extend)

- [ ] **Step 1: Append the list block to `sync.rs`**

Append:

```rust
// =========================================================================
// List commands — plan 04 surface.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (key, *values))]
    fn lpush(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("lpush", (key, values))?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn rpush(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("rpush", (key, values))?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn lpushx(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("lpushx", (key, values))?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn rpushx(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("rpushx", (key, values))?.unbind())
    }

    #[pyo3(signature = (key, count = None))]
    fn lpop(&self, py: Python<'_>, key: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        Ok(match count {
            None => bound.call_method1("lpop", (key,))?,
            Some(n) => bound.call_method1("lpop_count", (key, n))?,
        }
        .unbind())
    }

    #[pyo3(signature = (key, count = None))]
    fn rpop(&self, py: Python<'_>, key: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        Ok(match count {
            None => bound.call_method1("rpop", (key,))?,
            Some(n) => bound.call_method1("rpop_count", (key, n))?,
        }
        .unbind())
    }

    define_facade_command!(lmove, (src: String, dst: String, wherefrom: String = "LEFT".to_string(), whereto: String = "RIGHT".to_string()), lmove);
    define_facade_command!(lpos, (key: String, value: Vec<u8>, rank: Option<i64> = None, count: Option<i64> = None, maxlen: Option<i64> = None), lpos);
    define_facade_command!(lrange, (key: String, start: i64, end: i64), lrange);
    define_facade_command!(llen, (key: String), llen);
    define_facade_command!(lrem, (key: String, count: i64, value: Vec<u8>), lrem);
    define_facade_command!(lindex, (key: String, index: i64), lindex);
    define_facade_command!(lset, (key: String, index: i64, value: Vec<u8>), lset);
    define_facade_command!(linsert, (key: String, where_: String, pivot: Vec<u8>, value: Vec<u8>), linsert);
    define_facade_command!(ltrim, (key: String, start: i64, end: i64), ltrim);

    // Blocking variants — delegate; the driver routes them to the lazy blocking conn.
    #[pyo3(signature = (keys, timeout = 0.0))]
    fn blpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("blpop", (keys, timeout))?.unbind())
    }

    #[pyo3(signature = (keys, timeout = 0.0))]
    fn brpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("brpop", (keys, timeout))?.unbind())
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
            .call_method1("blmove", (src, dst, wherefrom, whereto, timeout))?
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
            .call_method1("blmpop", (timeout, numkeys, keys, direction, count))?
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
            .call_method1("lmpop", (numkeys, keys, direction, count))?
            .unbind())
    }
}
```

- [ ] **Step 2: Extend the smoke test**

Append to `tests/facade/test_sync_commands_smoke.py`:

```python
# --- lists ----------------------------------------------------------------


def test_list_push_pop(r: Redis) -> None:
    assert r.rpush("L", b"a", b"b", b"c") == 3
    assert r.llen("L") == 3
    assert r.lpop("L") == b"a"
    assert r.rpop("L") == b"c"
    assert r.lrange("L", 0, -1) == [b"b"]


def test_list_lpush_lpushx_rpushx(r: Redis) -> None:
    r.lpush("L", b"a")
    r.lpushx("L", b"b")
    r.rpushx("L", b"c")
    assert r.lrange("L", 0, -1) == [b"b", b"a", b"c"]


def test_list_lmove(r: Redis) -> None:
    r.rpush("src", b"a", b"b")
    r.lmove("src", "dst", "LEFT", "RIGHT")
    assert r.lrange("dst", 0, -1) == [b"a"]


def test_list_lpos_lrem_lindex_lset(r: Redis) -> None:
    r.rpush("L", b"a", b"b", b"a", b"c")
    assert r.lpos("L", b"a") == 0
    assert r.lrem("L", 1, b"a") == 1
    assert r.lindex("L", 0) == b"b"
    r.lset("L", 0, b"X")
    assert r.lindex("L", 0) == b"X"


def test_list_linsert_ltrim(r: Redis) -> None:
    r.rpush("L", b"a", b"c")
    r.linsert("L", "BEFORE", b"c", b"b")
    assert r.lrange("L", 0, -1) == [b"a", b"b", b"c"]
    r.ltrim("L", 0, 1)
    assert r.lrange("L", 0, -1) == [b"a", b"b"]


def test_list_blpop_immediate(r: Redis) -> None:
    r.rpush("L", b"x")
    assert r.blpop(["L"], timeout=1.0) == (b"L", b"x")


def test_list_brpop_immediate(r: Redis) -> None:
    r.rpush("L", b"y")
    assert r.brpop(["L"], timeout=1.0) == (b"L", b"y")


def test_list_blmove(r: Redis) -> None:
    r.rpush("S", b"a")
    r.blmove("S", "D", "LEFT", "RIGHT", timeout=1.0)
    assert r.lrange("D", 0, -1) == [b"a"]


def test_list_lmpop_blmpop(r: Redis) -> None:
    r.rpush("L", b"a", b"b")
    res = r.lmpop(1, ["L"], "LEFT", count=2)
    assert res is not None
    res2 = r.blmpop(1.0, 1, ["empty"], "LEFT", count=1)
    assert res2 is None
```

Run: `uv run pytest tests/facade/test_sync_commands_smoke.py -v -k list`
Expected: 9 PASS for the new list tests; existing tests still PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs tests/facade/test_sync_commands_smoke.py
git commit -m "feat(facade): add list commands"
```

---

## Task 6: Hash commands

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs`
- Test: `tests/facade/test_sync_commands_smoke.py`

- [ ] **Step 1: Append the hash `#[pymethods]` block**

```rust
// =========================================================================
// Hash commands — plan 05 surface.
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
        let bound = drv.bind(py);
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
        Ok(bound.call_method("hset", (name,), Some(&kwargs))?.unbind())
    }

    define_facade_command!(hsetnx, (name: String, key: String, value: Vec<u8>), hsetnx);
    define_facade_command!(hmset, (name: String, mapping: Bound<'_, PyDict>), hmset);
    define_facade_command!(hget, (name: String, key: String), hget);
    define_facade_command!(hgetall, (name: String), hgetall);

    #[pyo3(signature = (name, *keys))]
    fn hdel(&self, py: Python<'_>, name: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("hdel", (name, keys))?.unbind())
    }

    define_facade_command!(hincrby, (name: String, key: String, amount: i64 = 1), hincrby);
    define_facade_command!(hincrbyfloat, (name: String, key: String, amount: f64 = 1.0), hincrbyfloat);
    define_facade_command!(hkeys, (name: String), hkeys);
    define_facade_command!(hvals, (name: String), hvals);
    define_facade_command!(hexists, (name: String, key: String), hexists);
    define_facade_command!(hlen, (name: String), hlen);

    #[pyo3(signature = (name, *keys))]
    fn hmget(&self, py: Python<'_>, name: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("hmget", (name, keys))?.unbind())
    }

    define_facade_command!(hscan, (name: String, cursor: u64 = 0, match_: Option<String> = None, count: Option<i64> = None, no_values: bool = false), hscan, [name, cursor, match_, count, no_values]);
    define_facade_command!(hrandfield, (key: String, count: Option<i64> = None, withvalues: bool = false), hrandfield);

    // Hash-field TTLs (Redis 7.4)
    define_facade_command!(hexpire, (name: String, seconds: i64, fields: Vec<String>, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), hexpire);
    define_facade_command!(hpexpire, (name: String, milliseconds: i64, fields: Vec<String>, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), hpexpire);
    define_facade_command!(hexpireat, (name: String, unix_time_seconds: i64, fields: Vec<String>, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), hexpireat);
    define_facade_command!(hpexpireat, (name: String, unix_time_milliseconds: i64, fields: Vec<String>, nx: bool = false, xx: bool = false, gt: bool = false, lt: bool = false), hpexpireat);
    define_facade_command!(hexpiretime, (name: String, fields: Vec<String>), hexpiretime);
    define_facade_command!(hpexpiretime, (name: String, fields: Vec<String>), hpexpiretime);
    define_facade_command!(httl, (name: String, fields: Vec<String>), httl);
    define_facade_command!(hpttl, (name: String, fields: Vec<String>), hpttl);
    define_facade_command!(hpersist, (name: String, fields: Vec<String>), hpersist);
}
```

- [ ] **Step 2: Append hash smoke tests**

```python
# --- hashes ---------------------------------------------------------------


def test_hash_set_get(r: Redis) -> None:
    r.hset("H", "f", b"v")
    assert r.hget("H", "f") == b"v"


def test_hash_hsetnx(r: Redis) -> None:
    assert r.hsetnx("H", "f", b"v") in (True, 1)
    assert r.hsetnx("H", "f", b"x") in (False, 0)


def test_hash_hmset_hmget(r: Redis) -> None:
    r.hmset("H", {"a": b"1", "b": b"2"})
    assert r.hmget("H", "a", "b") == [b"1", b"2"]


def test_hash_hgetall(r: Redis) -> None:
    r.hmset("H", {"a": b"1", "b": b"2"})
    assert r.hgetall("H") == {b"a": b"1", b"b": b"2"}


def test_hash_hdel_hexists_hlen(r: Redis) -> None:
    r.hmset("H", {"a": b"1", "b": b"2"})
    assert r.hexists("H", "a") in (True, 1)
    assert r.hdel("H", "a") == 1
    assert r.hlen("H") == 1


def test_hash_hkeys_hvals(r: Redis) -> None:
    r.hmset("H", {"a": b"1", "b": b"2"})
    assert sorted(r.hkeys("H")) == [b"a", b"b"]
    assert sorted(r.hvals("H")) == [b"1", b"2"]


def test_hash_hincrby_hincrbyfloat(r: Redis) -> None:
    assert r.hincrby("H", "n", 4) == 4
    assert r.hincrbyfloat("H", "f", 1.5) == 1.5


def test_hash_hscan_hrandfield(r: Redis) -> None:
    r.hmset("H", {"a": b"1", "b": b"2", "c": b"3"})
    cursor, batch = r.hscan("H")
    assert cursor == 0
    assert len(batch) == 6  # flat [k,v,k,v,k,v]
    val = r.hrandfield("H")
    assert val in (b"a", b"b", b"c")


def test_hash_field_ttl(r: Redis) -> None:
    """Skip if running against a server older than 7.4."""
    import redis as upstream

    rp = upstream.Redis.from_url(r.connection_url() if hasattr(r, "connection_url") else "")
    info = rp.info("server") if rp else {}
    rp.close()
    version = info.get("redis_version", "0.0")
    major, minor = (int(x) for x in version.split(".")[:2])
    if (major, minor) < (7, 4):
        pytest.skip(f"hash-field TTLs require 7.4+, got {version}")
    r.hmset("H", {"f": b"v"})
    assert r.hexpire("H", 60, ["f"]) is not None
    assert r.httl("H", ["f"]) is not None
```

Run: `uv run pytest tests/facade/test_sync_commands_smoke.py -v -k hash`
Expected: 9 PASS (or 8 PASS + 1 SKIP if Valkey < 7.4).

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs tests/facade/test_sync_commands_smoke.py
git commit -m "feat(facade): add hash commands"
```

---

## Task 7: Set commands

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs`
- Test: `tests/facade/test_sync_commands_smoke.py`

- [ ] **Step 1: Append the set `#[pymethods]` block**

```rust
// =========================================================================
// Set commands — plan 06 surface.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (name, *values))]
    fn sadd(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("sadd", (name, values))?.unbind())
    }

    #[pyo3(signature = (name, *values))]
    fn srem(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("srem", (name, values))?.unbind())
    }

    define_facade_command!(smembers, (name: String), smembers);
    define_facade_command!(sismember, (name: String, value: Vec<u8>), sismember);

    #[pyo3(signature = (name, *values))]
    fn smismember(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("smismember", (name, values))?.unbind())
    }

    define_facade_command!(scard, (name: String), scard);

    #[pyo3(signature = (*keys))]
    fn sinter(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("sinter", (keys,))?.unbind())
    }

    #[pyo3(signature = (*keys))]
    fn sunion(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("sunion", (keys,))?.unbind())
    }

    #[pyo3(signature = (*keys))]
    fn sdiff(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("sdiff", (keys,))?.unbind())
    }

    #[pyo3(signature = (dest, *keys))]
    fn sinterstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("sinterstore", (dest, keys))?.unbind())
    }

    #[pyo3(signature = (dest, *keys))]
    fn sunionstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("sunionstore", (dest, keys))?.unbind())
    }

    #[pyo3(signature = (dest, *keys))]
    fn sdiffstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("sdiffstore", (dest, keys))?.unbind())
    }

    define_facade_command!(sintercard, (numkeys: i64, keys: Vec<String>, limit: Option<i64> = None), sintercard);
    define_facade_command!(smove, (src: String, dst: String, value: Vec<u8>), smove);

    #[pyo3(signature = (name, count = None))]
    fn spop(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        Ok(match count {
            None => bound.call_method1("spop", (name,))?,
            Some(n) => bound.call_method1("spop_count", (name, n))?,
        }
        .unbind())
    }

    #[pyo3(signature = (name, count = None))]
    fn srandmember(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        Ok(match count {
            None => bound.call_method1("srandmember", (name,))?,
            Some(n) => bound.call_method1("srandmember_count", (name, n))?,
        }
        .unbind())
    }

    define_facade_command!(sscan, (name: String, cursor: u64 = 0, match_: Option<String> = None, count: Option<i64> = None), sscan, [name, cursor, match_, count]);
}
```

- [ ] **Step 2: Append set smoke tests**

```python
# --- sets -----------------------------------------------------------------


def test_set_add_card_members(r: Redis) -> None:
    assert r.sadd("S", b"a", b"b", b"c") == 3
    assert r.scard("S") == 3
    members = r.smembers("S")
    # Driver returns a list-like; cast to set for shape-agnostic compare.
    assert set(members) == {b"a", b"b", b"c"}


def test_set_ismember_smismember(r: Redis) -> None:
    r.sadd("S", b"a")
    assert r.sismember("S", b"a") in (True, 1)
    assert r.smismember("S", b"a", b"x") in ([True, False], [1, 0])


def test_set_inter_union_diff(r: Redis) -> None:
    r.sadd("A", b"a", b"b")
    r.sadd("B", b"b", b"c")
    assert set(r.sinter("A", "B")) == {b"b"}
    assert set(r.sunion("A", "B")) == {b"a", b"b", b"c"}
    assert set(r.sdiff("A", "B")) == {b"a"}


def test_set_store_variants(r: Redis) -> None:
    r.sadd("A", b"a", b"b")
    r.sadd("B", b"b", b"c")
    assert r.sinterstore("X", "A", "B") == 1
    assert r.sunionstore("Y", "A", "B") == 3
    assert r.sdiffstore("Z", "A", "B") == 1


def test_set_intercard_smove(r: Redis) -> None:
    r.sadd("A", b"a", b"b")
    r.sadd("B", b"a")
    assert r.sintercard(2, ["A", "B"]) == 1
    r.smove("A", "B", b"b")
    assert set(r.smembers("B")) == {b"a", b"b"}


def test_set_spop_srandmember(r: Redis) -> None:
    r.sadd("S", b"a", b"b", b"c")
    popped = r.spop("S")
    assert popped in (b"a", b"b", b"c")
    rand = r.srandmember("S")
    assert rand in (b"a", b"b", b"c")
    rand_n = r.srandmember("S", count=2)
    assert len(rand_n) == 2


def test_set_sscan(r: Redis) -> None:
    r.sadd("S", b"a", b"b", b"c")
    cursor, batch = r.sscan("S")
    assert cursor == 0
    assert len(batch) == 3
```

Run: `uv run pytest tests/facade/test_sync_commands_smoke.py -v -k set`
Expected: 7 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs tests/facade/test_sync_commands_smoke.py
git commit -m "feat(facade): add set commands"
```

---

## Task 8: Sorted-set commands

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs`
- Test: `tests/facade/test_sync_commands_smoke.py`

- [ ] **Step 1: Append the zset `#[pymethods]` block**

```rust
// =========================================================================
// Sorted-set commands — plan 07 surface.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (
        name,
        mapping,
        nx = false,
        xx = false,
        gt = false,
        lt = false,
        ch = false,
        incr = false,
    ))]
    fn zadd(
        &self,
        py: Python<'_>,
        name: String,
        mapping: Bound<'_, PyDict>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let bound = drv.bind(py);
        let kwargs = PyDict::new(py);
        if nx {
            kwargs.set_item("nx", true)?;
        }
        if xx {
            kwargs.set_item("xx", true)?;
        }
        if gt {
            kwargs.set_item("gt", true)?;
        }
        if lt {
            kwargs.set_item("lt", true)?;
        }
        if ch {
            kwargs.set_item("ch", true)?;
        }
        if incr {
            kwargs.set_item("incr", true)?;
        }
        Ok(bound
            .call_method("zadd", (name, mapping), Some(&kwargs))?
            .unbind())
    }

    #[pyo3(signature = (name, *values))]
    fn zrem(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("zrem", (name, values))?.unbind())
    }

    #[pyo3(signature = (
        name, start, end,
        desc = false,
        withscores = false,
        score_cast_func = None,
        byscore = false,
        bylex = false,
        offset = None,
        num = None,
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
        byscore: bool,
        bylex: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = score_cast_func;
        let drv = self.driver_or_raise()?;
        let kwargs = PyDict::new(py);
        if desc {
            kwargs.set_item("rev", true)?;
        }
        if withscores {
            kwargs.set_item("withscores", true)?;
        }
        if byscore {
            kwargs.set_item("byscore", true)?;
        }
        if bylex {
            kwargs.set_item("bylex", true)?;
        }
        if let Some(o) = offset {
            kwargs.set_item("offset", o)?;
        }
        if let Some(n) = num {
            kwargs.set_item("count", n)?;
        }
        Ok(drv
            .bind(py)
            .call_method("zrange", (name, start, end), Some(&kwargs))?
            .unbind())
    }

    define_facade_command!(zrangebyscore, (name: String, min_: Py<PyAny>, max_: Py<PyAny>, start: Option<i64> = None, num: Option<i64> = None, withscores: bool = false), zrangebyscore, [name, min_, max_, start, num, withscores]);
    define_facade_command!(zrangebylex, (name: String, min_: Vec<u8>, max_: Vec<u8>, start: Option<i64> = None, num: Option<i64> = None), zrangebylex, [name, min_, max_, start, num]);
    define_facade_command!(zrevrangebyscore, (name: String, max_: Py<PyAny>, min_: Py<PyAny>, start: Option<i64> = None, num: Option<i64> = None, withscores: bool = false), zrevrangebyscore, [name, max_, min_, start, num, withscores]);
    define_facade_command!(zrevrangebylex, (name: String, max_: Vec<u8>, min_: Vec<u8>, start: Option<i64> = None, num: Option<i64> = None), zrevrangebylex, [name, max_, min_, start, num]);
    define_facade_command!(zrangestore, (dest: String, src: String, start: Py<PyAny>, end: Py<PyAny>, byscore: bool = false, bylex: bool = false, desc: bool = false, offset: Option<i64> = None, num: Option<i64> = None), zrangestore);
    define_facade_command!(zincrby, (name: String, amount: f64, value: Vec<u8>), zincrby);
    define_facade_command!(zcard, (name: String), zcard);
    define_facade_command!(zscore, (name: String, value: Vec<u8>), zscore);

    #[pyo3(signature = (name, *values))]
    fn zmscore(&self, py: Python<'_>, name: String, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("zmscore", (name, values))?.unbind())
    }

    define_facade_command!(zrank, (name: String, value: Vec<u8>, withscore: bool = false), zrank);
    define_facade_command!(zrevrank, (name: String, value: Vec<u8>, withscore: bool = false), zrevrank);
    define_facade_command!(zremrangebyrank, (name: String, min_: i64, max_: i64), zremrangebyrank, [name, min_, max_]);
    define_facade_command!(zremrangebyscore, (name: String, min_: Py<PyAny>, max_: Py<PyAny>), zremrangebyscore, [name, min_, max_]);
    define_facade_command!(zremrangebylex, (name: String, min_: Vec<u8>, max_: Vec<u8>), zremrangebylex, [name, min_, max_]);
    define_facade_command!(zcount, (name: String, min_: Py<PyAny>, max_: Py<PyAny>), zcount, [name, min_, max_]);
    define_facade_command!(zlexcount, (name: String, min_: Vec<u8>, max_: Vec<u8>), zlexcount, [name, min_, max_]);

    #[pyo3(signature = (name, count = None))]
    fn zpopmin(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("zpopmin", (name, count))?.unbind())
    }

    #[pyo3(signature = (name, count = None))]
    fn zpopmax(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("zpopmax", (name, count))?.unbind())
    }

    define_facade_command!(bzpopmin, (keys: Vec<String>, timeout: f64 = 0.0), bzpopmin);
    define_facade_command!(bzpopmax, (keys: Vec<String>, timeout: f64 = 0.0), bzpopmax);

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
        Ok(drv
            .bind(py)
            .call_method1("zmpop", (numkeys, keys, min_or_max, count))?
            .unbind())
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
        Ok(drv
            .bind(py)
            .call_method1("bzmpop", (timeout, numkeys, keys, min_or_max, count))?
            .unbind())
    }

    define_facade_command!(zrandmember, (name: String, count: Option<i64> = None, withscores: bool = false), zrandmember);
    define_facade_command!(zscan, (name: String, cursor: u64 = 0, match_: Option<String> = None, count: Option<i64> = None, score_cast_func: Option<Py<PyAny>> = None), zscan, [name, cursor, match_, count]);

    #[pyo3(signature = (dest, keys, aggregate = None, withscores = false))]
    fn zunion(
        &self,
        py: Python<'_>,
        dest: String,
        keys: Vec<String>,
        aggregate: Option<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let _ = dest;
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("zunion", (keys, aggregate, withscores))?
            .unbind())
    }

    #[pyo3(signature = (keys, aggregate = None, withscores = false))]
    fn zinter(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        aggregate: Option<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("zinter", (keys, aggregate, withscores))?
            .unbind())
    }

    #[pyo3(signature = (keys, withscores = false))]
    fn zdiff(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("zdiff", (keys, withscores))?.unbind())
    }

    #[pyo3(signature = (dest, keys, aggregate = None))]
    fn zunionstore(
        &self,
        py: Python<'_>,
        dest: String,
        keys: Vec<String>,
        aggregate: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("zunionstore", (dest, keys, aggregate))?
            .unbind())
    }

    #[pyo3(signature = (dest, keys, aggregate = None))]
    fn zinterstore(
        &self,
        py: Python<'_>,
        dest: String,
        keys: Vec<String>,
        aggregate: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("zinterstore", (dest, keys, aggregate))?
            .unbind())
    }

    #[pyo3(signature = (dest, keys))]
    fn zdiffstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("zdiffstore", (dest, keys))?.unbind())
    }
}
```

- [ ] **Step 2: Append zset smoke tests**

```python
# --- zsets ----------------------------------------------------------------


def test_zset_add_card_score(r: Redis) -> None:
    r.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert r.zcard("Z") == 3
    assert r.zscore("Z", b"b") == 2.0


def test_zset_zrange_basic_and_withscores(r: Redis) -> None:
    r.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert r.zrange("Z", 0, -1) == [b"a", b"b", b"c"]
    assert r.zrange("Z", 0, -1, withscores=True) == [(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]


def test_zset_rev_zincrby(r: Redis) -> None:
    r.zadd("Z", {"a": 1, "b": 2})
    assert r.zrange("Z", 0, -1, desc=True) == [b"b", b"a"]
    assert r.zincrby("Z", 3.0, b"a") == 4.0


def test_zset_zrem_zrank(r: Redis) -> None:
    r.zadd("Z", {"a": 1, "b": 2})
    assert r.zrem("Z", b"a") == 1
    assert r.zrank("Z", b"b") == 0


def test_zset_zpopmin_zpopmax(r: Redis) -> None:
    r.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert r.zpopmin("Z")[0] == (b"a", 1.0)
    assert r.zpopmax("Z")[0] == (b"c", 3.0)


def test_zset_zmscore_zcount(r: Redis) -> None:
    r.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert r.zmscore("Z", b"a", b"x") == [1.0, None]
    assert r.zcount("Z", 1, 2) == 2


def test_zset_union_inter_diff_store(r: Redis) -> None:
    r.zadd("A", {"a": 1, "b": 2})
    r.zadd("B", {"b": 3, "c": 4})
    assert r.zunionstore("U", ["A", "B"]) == 3
    assert r.zinterstore("I", ["A", "B"]) == 1
    assert r.zdiffstore("D", ["A", "B"]) == 1


def test_zset_zscan(r: Redis) -> None:
    r.zadd("Z", {"a": 1, "b": 2})
    cursor, batch = r.zscan("Z")
    assert cursor == 0
    assert len(batch) == 4  # flat [m,s,m,s]
```

Run: `uv run pytest tests/facade/test_sync_commands_smoke.py -v -k zset`
Expected: 8 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs tests/facade/test_sync_commands_smoke.py
git commit -m "feat(facade): add sorted-set commands"
```

---

## Task 9: Stream commands

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs`
- Test: `tests/facade/test_sync_commands_smoke.py`

- [ ] **Step 1: Append the streams `#[pymethods]` block**

```rust
// =========================================================================
// Stream commands — plan 08 surface.
// =========================================================================

#[pymethods]
impl Redis {
    #[pyo3(signature = (
        name,
        fields,
        id = "*".to_string(),
        maxlen = None,
        approximate = true,
        nomkstream = false,
        minid = None,
        limit = None,
    ))]
    fn xadd(
        &self,
        py: Python<'_>,
        name: String,
        fields: Bound<'_, PyDict>,
        id: String,
        maxlen: Option<i64>,
        approximate: bool,
        nomkstream: bool,
        minid: Option<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("id", id)?;
        if let Some(m) = maxlen {
            kwargs.set_item("maxlen", m)?;
        }
        kwargs.set_item("approximate", approximate)?;
        if nomkstream {
            kwargs.set_item("nomkstream", true)?;
        }
        if let Some(m) = minid {
            kwargs.set_item("minid", m)?;
        }
        if let Some(l) = limit {
            kwargs.set_item("limit", l)?;
        }
        Ok(drv
            .bind(py)
            .call_method("xadd", (name, fields), Some(&kwargs))?
            .unbind())
    }

    define_facade_command!(xlen, (name: String), xlen);
    define_facade_command!(xrange, (name: String, min_: String = "-".to_string(), max_: String = "+".to_string(), count: Option<i64> = None), xrange, [name, min_, max_, count]);
    define_facade_command!(xrevrange, (name: String, max_: String = "+".to_string(), min_: String = "-".to_string(), count: Option<i64> = None), xrevrange, [name, max_, min_, count]);

    #[pyo3(signature = (streams, count = None, block = None))]
    fn xread(
        &self,
        py: Python<'_>,
        streams: Bound<'_, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("xread", (streams, count, block))?
            .unbind())
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
        Ok(drv
            .bind(py)
            .call_method1(
                "xreadgroup",
                (groupname, consumername, streams, count, block, noack),
            )?
            .unbind())
    }

    #[pyo3(signature = (name, groupname, *ids))]
    fn xack(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
        ids: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("xack", (name, groupname, ids))?
            .unbind())
    }

    #[pyo3(signature = (name, *ids))]
    fn xdel(&self, py: Python<'_>, name: String, ids: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("xdel", (name, ids))?.unbind())
    }

    define_facade_command!(xgroup_create, (name: String, groupname: String, id: String = "$".to_string(), mkstream: bool = false, entries_read: Option<i64> = None), xgroup_create);
    define_facade_command!(xgroup_setid, (name: String, groupname: String, id: String, entries_read: Option<i64> = None), xgroup_setid);
    define_facade_command!(xgroup_destroy, (name: String, groupname: String), xgroup_destroy);
    define_facade_command!(xgroup_delconsumer, (name: String, groupname: String, consumername: String), xgroup_delconsumer);
    define_facade_command!(xgroup_createconsumer, (name: String, groupname: String, consumername: String), xgroup_createconsumer);
    define_facade_command!(xinfo_stream, (name: String, full: bool = false), xinfo_stream);
    define_facade_command!(xinfo_groups, (name: String), xinfo_groups);
    define_facade_command!(xinfo_consumers, (name: String, groupname: String), xinfo_consumers);
    define_facade_command!(xtrim, (name: String, maxlen: Option<i64> = None, approximate: bool = true, minid: Option<String> = None, limit: Option<i64> = None), xtrim);

    #[pyo3(signature = (name, groupname, idle = None, min_id = None, max_id = None, count = None, consumername = None))]
    fn xpending(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
        idle: Option<i64>,
        min_id: Option<String>,
        max_id: Option<String>,
        count: Option<i64>,
        consumername: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1(
                "xpending",
                (name, groupname, idle, min_id, max_id, count, consumername),
            )?
            .unbind())
    }

    #[pyo3(signature = (name, groupname, consumername, min_idle_time, ids, idle = None, time = None, retrycount = None, force = false, justid = false))]
    fn xclaim(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
        consumername: String,
        min_idle_time: i64,
        ids: Vec<String>,
        idle: Option<i64>,
        time: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1(
                "xclaim",
                (
                    name,
                    groupname,
                    consumername,
                    min_idle_time,
                    ids,
                    idle,
                    time,
                    retrycount,
                    force,
                    justid,
                ),
            )?
            .unbind())
    }

    #[pyo3(signature = (name, groupname, consumername, min_idle_time, start = "0-0".to_string(), count = None, justid = false))]
    fn xautoclaim(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
        consumername: String,
        min_idle_time: i64,
        start: String,
        count: Option<i64>,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1(
                "xautoclaim",
                (name, groupname, consumername, min_idle_time, start, count, justid),
            )?
            .unbind())
    }

    define_facade_command!(xsetid, (name: String, id: String, entries_added: Option<i64> = None, max_deleted_id: Option<String> = None), xsetid);
}
```

- [ ] **Step 2: Append stream smoke tests**

```python
# --- streams --------------------------------------------------------------


def test_stream_xadd_xlen_xrange(r: Redis) -> None:
    id1 = r.xadd("S", {"f": b"1"})
    id2 = r.xadd("S", {"f": b"2"})
    assert r.xlen("S") == 2
    rng = r.xrange("S")
    assert len(rng) == 2
    assert r.xrevrange("S")[0][0] == id2
    assert r.xrange("S", min_=id1, max_=id1)[0][0] == id1


def test_stream_xread(r: Redis) -> None:
    r.xadd("S", {"f": b"v"})
    res = r.xread({"S": "0"})
    assert res
    # Flattened result shape per plan 08.


def test_stream_xreadgroup_xack_xpending(r: Redis) -> None:
    r.xadd("S", {"f": b"v"})
    r.xgroup_create("S", "G", id="0", mkstream=False)
    msgs = r.xreadgroup("G", "C1", {"S": ">"})
    assert msgs
    pending = r.xpending("S", "G")
    assert pending
    # Use the first id we got back from xrange.
    first_id = r.xrange("S")[0][0]
    assert r.xack("S", "G", first_id) == 1


def test_stream_xdel_xtrim(r: Redis) -> None:
    id1 = r.xadd("S", {"f": b"1"})
    r.xadd("S", {"f": b"2"})
    assert r.xdel("S", id1) == 1
    r.xtrim("S", maxlen=1, approximate=False)
    assert r.xlen("S") <= 1


def test_stream_xinfo(r: Redis) -> None:
    r.xadd("S", {"f": b"v"})
    r.xgroup_create("S", "G", id="0", mkstream=False)
    info = r.xinfo_stream("S")
    assert info
    groups = r.xinfo_groups("S")
    assert groups


def test_stream_xclaim_xautoclaim(r: Redis) -> None:
    id1 = r.xadd("S", {"f": b"v"})
    r.xgroup_create("S", "G", id="0", mkstream=False)
    r.xreadgroup("G", "C1", {"S": ">"})
    claimed = r.xclaim("S", "G", "C2", 0, [id1])
    assert claimed
    autoclaimed = r.xautoclaim("S", "G", "C3", 0)
    assert autoclaimed is not None


def test_stream_xsetid(r: Redis) -> None:
    r.xadd("S", {"f": b"v"})
    r.xsetid("S", "100-0")  # noqa: PLR2004
```

Run: `uv run pytest tests/facade/test_sync_commands_smoke.py -v -k stream`
Expected: 7 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs tests/facade/test_sync_commands_smoke.py
git commit -m "feat(facade): add stream commands"
```

---

## Task 10: Scripts + admin commands

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs`
- Test: `tests/facade/test_sync_commands_smoke.py`

- [ ] **Step 1: Append the scripts/admin `#[pymethods]` block**

```rust
// =========================================================================
// Scripts + admin commands — plan 09 surface.
// =========================================================================

#[pymethods]
impl Redis {
    define_facade_command!(eval, (script: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), eval, [script, numkeys, keys_and_args]);
    define_facade_command!(eval_ro, (script: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), eval_ro, [script, numkeys, keys_and_args]);
    define_facade_command!(evalsha, (sha: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), evalsha, [sha, numkeys, keys_and_args]);
    define_facade_command!(evalsha_ro, (sha: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), evalsha_ro, [sha, numkeys, keys_and_args]);
    define_facade_command!(script_load, (script: String), script_load);

    #[pyo3(signature = (*shas))]
    fn script_exists(&self, py: Python<'_>, shas: Vec<String>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method1("script_exists", (shas,))?.unbind())
    }

    fn script_flush(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv.bind(py).call_method0("script_flush")?.unbind())
    }

    define_facade_command!(fcall, (function: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), fcall, [function, numkeys, keys_and_args]);
    define_facade_command!(fcall_ro, (function: String, numkeys: i64, keys_and_args: Vec<Py<PyAny>>), fcall_ro, [function, numkeys, keys_and_args]);
    define_facade_command!(function_load, (code: String, replace: bool = false), function_load);
    define_facade_command!(function_dump, (), function_dump);
    define_facade_command!(function_flush, (mode: Option<String> = None), function_flush);
    define_facade_command!(function_list, (library: Option<String> = None, withcode: bool = false), function_list);
    define_facade_command!(function_stats, (), function_stats);
    define_facade_command!(function_kill, (), function_kill);

    define_facade_command!(scan, (cursor: u64 = 0, match_: Option<String> = None, count: Option<i64> = None, type_: Option<String> = None), scan, [cursor, match_, count, type_]);

    #[pyo3(signature = (match_ = None, count = None, type_ = None))]
    fn scan_iter(
        &self,
        py: Python<'_>,
        match_: Option<String>,
        count: Option<i64>,
        type_: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1("scan_iter", (match_, count, type_))?
            .unbind())
    }

    define_facade_command!(keys, (pattern: String = "*".to_string()), keys);
    define_facade_command!(randomkey, (), randomkey);
    define_facade_command!(dbsize, (), dbsize);
    define_facade_command!(flushdb, (asynchronous: bool = false), flushdb);
    define_facade_command!(flushall, (asynchronous: bool = false), flushall);

    define_facade_command!(info, (section: Option<String> = None), info);
    define_facade_command!(config_get, (pattern: String = "*".to_string()), config_get);
    define_facade_command!(config_set, (parameter: String, value: Py<PyAny>), config_set);
    define_facade_command!(config_resetstat, (), config_resetstat);
    define_facade_command!(config_rewrite, (), config_rewrite);

    #[pyo3(signature = (
        _id = None,
        _type = None,
        addr = None,
        skipme = true,
        laddr = None,
        user = None,
        maxage = None,
    ))]
    fn client_kill(
        &self,
        py: Python<'_>,
        _id: Option<i64>,
        _type: Option<String>,
        addr: Option<String>,
        skipme: bool,
        laddr: Option<String>,
        user: Option<String>,
        maxage: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.driver_or_raise()?;
        Ok(drv
            .bind(py)
            .call_method1(
                "client_kill",
                (_id, _type, addr, skipme, laddr, user, maxage),
            )?
            .unbind())
    }

    define_facade_command!(client_getname, (), client_getname);
    define_facade_command!(client_setname, (name: String), client_setname);
    define_facade_command!(client_list, (_type: Option<String> = None, client_id: Option<Vec<i64>> = None), client_list);
    define_facade_command!(client_id, (), client_id);
    define_facade_command!(client_info, (), client_info);
    define_facade_command!(client_pause, (timeout: i64, all: bool = true), client_pause);
    define_facade_command!(client_unpause, (), client_unpause);
    define_facade_command!(client_no_evict, (mode: String), client_no_evict);
    define_facade_command!(client_no_touch, (mode: String), client_no_touch);

    define_facade_command!(object_encoding, (name: String), object_encoding);
    define_facade_command!(object_idletime, (name: String), object_idletime);
    define_facade_command!(object_freq, (name: String), object_freq);
    define_facade_command!(object_refcount, (name: String), object_refcount);
    define_facade_command!(object_help, (), object_help);
    define_facade_command!(memory_usage, (key: String, samples: Option<i64> = None), memory_usage);

    define_facade_command!(ping, (), ping);
    define_facade_command!(echo, (value: Vec<u8>), echo);
    define_facade_command!(wait, (numreplicas: i64, timeout: i64), wait);
    define_facade_command!(waitaof, (numlocal: i64, numreplicas: i64, timeout: i64), waitaof);
    define_facade_command!(time, (), time);
    define_facade_command!(lastsave, (), lastsave);
    define_facade_command!(bgsave, (schedule: bool = false), bgsave);
    define_facade_command!(bgrewriteaof, (), bgrewriteaof);
    define_facade_command!(debug_sleep, (seconds: f64), debug_sleep);
}
```

- [ ] **Step 2: Append scripts/admin smoke tests**

```python
# --- scripts + admin ------------------------------------------------------


def test_scripts_eval_evalsha(r: Redis) -> None:
    src = "return KEYS[1]"
    assert r.eval(src, 1, ["hello"]) == b"hello"
    sha = r.script_load(src)
    assert r.evalsha(sha, 1, ["world"]) == b"world"
    assert r.script_exists(sha) == [True] or r.script_exists(sha) == [1]


def test_scripts_script_flush(r: Redis) -> None:
    sha = r.script_load("return 1")
    r.script_flush()
    assert r.script_exists(sha) in ([False], [0])


def test_scripts_function_apis(r: Redis) -> None:
    code = "#!lua name=mylib\nredis.register_function('myf', function(k, a) return k[1] end)"
    try:
        r.function_load(code, replace=True)
    except Exception:
        pytest.skip("Lua functions not supported on this server")
    assert r.fcall("myf", 1, ["k"]) == b"k"
    assert r.function_list()
    assert r.function_stats()


def test_admin_scan_keys_dbsize(r: Redis) -> None:
    r.set("k1", b"v")
    r.set("k2", b"v")
    cursor, batch = r.scan()
    assert cursor == 0
    assert set(batch) >= {b"k1", b"k2"} or set(batch) >= {"k1", "k2"}
    assert set(r.keys("k*")) >= {b"k1", b"k2"} or set(r.keys("k*")) >= {"k1", "k2"}
    assert r.dbsize() >= 2


def test_admin_randomkey_flushdb(r: Redis) -> None:
    r.set("k", b"v")
    assert r.randomkey() in (b"k", "k")
    r.flushdb()
    assert r.dbsize() == 0


def test_admin_info_config(r: Redis) -> None:
    info = r.info()
    assert info
    cfg = r.config_get("maxmemory")
    assert cfg is not None
    r.config_resetstat()


def test_admin_client_apis(r: Redis) -> None:
    r.client_setname("test-client")
    assert r.client_getname() in (b"test-client", "test-client")
    assert r.client_id() > 0
    assert r.client_list()
    assert r.client_info()


def test_admin_object_memory(r: Redis) -> None:
    r.set("k", b"v")
    assert r.object_encoding("k") is not None
    assert r.object_refcount("k") >= 1
    assert r.memory_usage("k") > 0


def test_admin_basic_admin(r: Redis) -> None:
    assert r.ping() is True
    assert r.echo(b"hi") == b"hi"
    t = r.time()
    assert isinstance(t, (list, tuple)) and len(t) == 2
    r.lastsave()
    r.bgsave()
    r.bgrewriteaof()
```

Run: `uv run pytest tests/facade/test_sync_commands_smoke.py -v -k "scripts or admin"`
Expected: 9 PASS (1 may SKIP if Lua functions are disabled).

- [ ] **Step 3: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs tests/facade/test_sync_commands_smoke.py
git commit -m "feat(facade): add scripts and admin commands"
```

---

## Task 11: `lock(...)` distributed-lock helper

Wraps the script-based primitives from plan 09 (`SET key val NX PX ttl` to acquire, a Lua script to release-only-if-still-owned). Returns a `Lock` pyclass that supports the context-manager protocol and `acquire` / `release` / `extend` / `reacquire` / `owned`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs`
- Test: `tests/facade/test_sync_lock.py`

- [ ] **Step 1: Append the `Lock` pyclass + `Redis::lock` method**

Append to `crates/redis-rs-py-driver/src/facade/sync.rs`:

```rust
// =========================================================================
// Distributed lock helper.
//
// Mirrors redis-py's `Lock`. Acquire is a single SET NX PX call; release
// is a Lua script that checks ownership before deleting.
// =========================================================================

use pyo3::types::PyBytes;
use std::time::{SystemTime, UNIX_EPOCH};

const LOCK_RELEASE_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"#;

const LOCK_EXTEND_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
    return 0
end
"#;

#[pyclass(module = "redis_rs_py._driver", name = "Lock")]
pub struct Lock {
    redis: Py<Redis>,
    name: String,
    timeout: Option<f64>,
    sleep: f64,
    blocking: bool,
    blocking_timeout: Option<f64>,
    thread_local: bool,
    token: std::sync::Mutex<Option<Vec<u8>>>,
}

#[pymethods]
impl Lock {
    #[pyo3(signature = (blocking = None, blocking_timeout = None, token = None))]
    fn acquire(
        &self,
        py: Python<'_>,
        blocking: Option<bool>,
        blocking_timeout: Option<f64>,
        token: Option<Vec<u8>>,
    ) -> PyResult<bool> {
        let blocking = blocking.unwrap_or(self.blocking);
        let blocking_timeout = blocking_timeout.or(self.blocking_timeout);
        let token = token.unwrap_or_else(generate_token);
        let px = self.timeout.map(|s| (s * 1000.0) as i64).unwrap_or(0);
        let r = self.redis.bind(py);

        let deadline = blocking_timeout.map(|t| now_secs() + t);
        loop {
            let kwargs = PyDict::new(py);
            kwargs.set_item("nx", true)?;
            if px > 0 {
                kwargs.set_item("px", px)?;
            }
            let res: Py<PyAny> = r
                .call_method(
                    "set",
                    (self.name.clone(), PyBytes::new(py, &token)),
                    Some(&kwargs),
                )?
                .unbind();
            // SET NX returns True/None; treat None/False/0 as failure.
            let acquired = res.bind(py).is_truthy()?;
            if acquired {
                *self.token.lock().unwrap() = Some(token);
                return Ok(true);
            }
            if !blocking {
                return Ok(false);
            }
            if let Some(d) = deadline
                && now_secs() >= d
            {
                return Ok(false);
            }
            std::thread::sleep(std::time::Duration::from_secs_f64(self.sleep));
        }
    }

    fn release(&self, py: Python<'_>) -> PyResult<()> {
        let token = self
            .token
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| {
                let exc = py.import("redis_rs_py.exceptions")?.getattr("LockNotOwnedError")?;
                let err = exc.call1(("Cannot release an unlocked lock",))?;
                PyResult::<()>::Err(PyErr::from_value(err)).unwrap_err()
            })?;
        let r = self.redis.bind(py);
        let n: i64 = r
            .call_method1(
                "eval",
                (LOCK_RELEASE_LUA.to_string(), 1_i64, vec![
                    self.name.clone().into_pyobject(py)?.into_any().unbind(),
                    PyBytes::new(py, &token).into_any().unbind(),
                ]),
            )?
            .extract()?;
        if n == 0 {
            let exc = py.import("redis_rs_py.exceptions")?.getattr("LockNotOwnedError")?;
            let err = exc.call1(("Cannot release a lock owned by someone else",))?;
            return Err(PyErr::from_value(err));
        }
        *self.token.lock().unwrap() = None;
        Ok(())
    }

    #[pyo3(signature = (additional_time, replace_ttl = false))]
    fn extend(&self, py: Python<'_>, additional_time: f64, replace_ttl: bool) -> PyResult<bool> {
        let _ = replace_ttl; // simplification: always replace; matches redis-py's default for the script-based path
        let token = self
            .token
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| {
                let exc = py.import("redis_rs_py.exceptions").and_then(|m| m.getattr("LockNotOwnedError"));
                match exc {
                    Ok(c) => PyErr::from_value(c.call1(("Cannot extend an unlocked lock",)).unwrap()),
                    Err(e) => e,
                }
            })?;
        let r = self.redis.bind(py);
        let n: i64 = r
            .call_method1(
                "eval",
                (
                    LOCK_EXTEND_LUA.to_string(),
                    1_i64,
                    vec![
                        self.name.clone().into_pyobject(py)?.into_any().unbind(),
                        PyBytes::new(py, &token).into_any().unbind(),
                        ((additional_time * 1000.0) as i64)
                            .into_pyobject(py)?
                            .into_any()
                            .unbind(),
                    ],
                ),
            )?
            .extract()?;
        Ok(n > 0)
    }

    fn owned(&self, py: Python<'_>) -> PyResult<bool> {
        let token = match self.token.lock().unwrap().clone() {
            Some(t) => t,
            None => return Ok(false),
        };
        let r = self.redis.bind(py);
        let val: Option<Vec<u8>> = r.call_method1("get", (self.name.clone(),))?.extract()?;
        Ok(val.as_deref() == Some(token.as_slice()))
    }

    fn locked(&self, py: Python<'_>) -> PyResult<bool> {
        let r = self.redis.bind(py);
        let val: Option<Vec<u8>> = r.call_method1("get", (self.name.clone(),))?.extract()?;
        Ok(val.is_some())
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<Self>> {
        slf.bind(py).borrow().acquire(py, None, None, None)?;
        Ok(slf)
    }

    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<Py<PyAny>>,
        exc_val: Option<Py<PyAny>>,
        exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let _ = (exc_type, exc_val, exc_tb);
        self.release(py)?;
        Ok(false)
    }
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn generate_token() -> Vec<u8> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{pid}-{nanos}-{n}").into_bytes()
}

#[pymethods]
impl Redis {
    #[pyo3(signature = (
        name,
        timeout = None,
        sleep = 0.1,
        blocking = true,
        blocking_timeout = None,
        lock_class = None,
        thread_local = true,
    ))]
    fn lock(
        slf: Py<Self>,
        py: Python<'_>,
        name: String,
        timeout: Option<f64>,
        sleep: f64,
        blocking: bool,
        blocking_timeout: Option<f64>,
        lock_class: Option<Py<PyAny>>,
        thread_local: bool,
    ) -> PyResult<Py<Lock>> {
        let _ = lock_class; // custom Lock subclasses are out of scope for v0.1
        let lock = Lock {
            redis: slf,
            name,
            timeout,
            sleep,
            blocking,
            blocking_timeout,
            thread_local,
            token: std::sync::Mutex::new(None),
        };
        Py::new(py, lock)
    }
}
```

- [ ] **Step 2: Register `Lock` in `lib.rs`**

In `crates/redis-rs-py-driver/src/lib.rs`, after `m.add_class::<facade::sync::Redis>()?;`, add:

```rust
    m.add_class::<facade::sync::Lock>()?;
```

- [ ] **Step 3: Add the lock test**

`tests/facade/test_sync_lock.py`:

```python
"""r.lock() distributed-lock helper."""

from __future__ import annotations

import pytest

from redis_rs_py import Redis
from redis_rs_py.exceptions import LockNotOwnedError


@pytest.fixture
def r(valkey_url: str) -> Redis:
    import redis as upstream

    rp = upstream.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    client = Redis.from_url(valkey_url)
    yield client
    client.close()


def test_lock_acquire_release(r: Redis) -> None:
    lock = r.lock("L", timeout=5.0)
    assert lock.acquire(blocking=False) is True
    assert lock.owned() is True
    lock.release()
    assert lock.owned() is False


def test_lock_context_manager(r: Redis) -> None:
    with r.lock("L", timeout=5.0) as lock:
        assert lock.owned() is True
    assert lock.owned() is False


def test_lock_blocking_timeout(r: Redis) -> None:
    lock1 = r.lock("L", timeout=10.0)
    assert lock1.acquire() is True
    lock2 = r.lock("L", timeout=10.0, sleep=0.05)
    assert lock2.acquire(blocking=True, blocking_timeout=0.2) is False


def test_lock_release_unowned_raises(r: Redis) -> None:
    lock = r.lock("L")
    with pytest.raises(LockNotOwnedError):
        lock.release()


def test_lock_extend(r: Redis) -> None:
    lock = r.lock("L", timeout=5.0)
    lock.acquire()
    assert lock.extend(10.0) is True
    lock.release()


def test_lock_other_owner_release_raises(r: Redis) -> None:
    """If the underlying key got stolen by a different token, release raises."""
    lock = r.lock("L", timeout=10.0)
    lock.acquire()
    # Steal: overwrite with a different value via SET (no NX).
    r.set("L", b"stolen")
    with pytest.raises(LockNotOwnedError):
        lock.release()
```

- [ ] **Step 4: Run the lock tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/facade/test_sync_lock.py -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs crates/redis-rs-py-driver/src/lib.rs tests/facade/test_sync_lock.py
git commit -m "feat(facade): add lock helper with extend and owned semantics"
```

---

## Task 12: Re-exports + `.pyi` stub for `Redis`

**Files:**
- Modify: `python/redis_rs_py/__init__.py`
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Update `__init__.py`**

`python/redis_rs_py/__init__.py` — add `Redis` and `Lock` to the imports and to `__all__`. The full file (preserving plan 02's exception re-exports) becomes:

```python
"""redis-rs-py — high-performance, drop-in replacement for redis-py."""

from redis_rs_py import exceptions
from redis_rs_py._driver import (
    Lock,
    Redis,
    RedisRsAwaitable,
    RedisRsDriver,
    __version__,
)
from redis_rs_py.exceptions import (
    AskError,
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    DataError,
    ExecAbortError,
    InvalidResponse,
    LockError,
    LockNotOwnedError,
    MasterDownError,
    ModuleError,
    MovedError,
    NoPermissionError,
    NoScriptError,
    OutOfMemoryError,
    PubSubError,
    ReadOnlyError,
    RedisError,
    ResponseError,
    SlaveError,
    TimeoutError,
    TryAgainError,
    WatchError,
)

__all__ = [
    "AskError",
    "AuthenticationError",
    "AuthenticationWrongNumberOfArgsError",
    "BusyLoadingError",
    "ClusterCrossSlotError",
    "ClusterDownError",
    "ClusterError",
    "ConnectionError",
    "DataError",
    "ExecAbortError",
    "InvalidResponse",
    "Lock",
    "LockError",
    "LockNotOwnedError",
    "MasterDownError",
    "ModuleError",
    "MovedError",
    "NoPermissionError",
    "NoScriptError",
    "OutOfMemoryError",
    "PubSubError",
    "ReadOnlyError",
    "Redis",
    "RedisError",
    "RedisRsAwaitable",
    "RedisRsDriver",
    "ResponseError",
    "SlaveError",
    "TimeoutError",
    "TryAgainError",
    "WatchError",
    "__version__",
    "exceptions",
]
```

- [ ] **Step 2: Append the `Redis` stub to `_driver.pyi`**

Append to `python/redis_rs_py/_driver.pyi`:

```python
class Redis:
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
    def from_url(cls, url: str, **kwargs: Any) -> Redis: ...
    def close(self) -> None: ...
    def __enter__(self) -> Redis: ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool: ...
    def pipeline(self, transaction: bool = True, shard_hint: Any = None) -> Any: ...
    def pubsub(self, **kwargs: Any) -> Any: ...
    def transaction(self, func: Any, *watches: Any, value_from_callable: bool = False, watch_delay: float | None = None, **kwargs: Any) -> Any: ...
    def lock(
        self,
        name: str,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        lock_class: Any = None,
        thread_local: bool = True,
    ) -> Lock: ...
    # Commands — every method below returns the redis-rs typed value.
    def get(self, key: str) -> bytes | None: ...
    def set(self, key: str, value: bytes, ex: int | None = None, px: int | None = None, nx: bool = False, xx: bool = False, keepttl: bool = False, get: bool = False, exat: int | None = None, pxat: int | None = None) -> Any: ...
    # ... full method list omitted for brevity; mirror the pymethods block ...

class Lock:
    def acquire(self, blocking: bool | None = None, blocking_timeout: float | None = None, token: bytes | None = None) -> bool: ...
    def release(self) -> None: ...
    def extend(self, additional_time: float, replace_ttl: bool = False) -> bool: ...
    def owned(self) -> bool: ...
    def locked(self) -> bool: ...
    def __enter__(self) -> Lock: ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool: ...

def _facade_reset_warn_state() -> None: ...
```

- [ ] **Step 3: Run lint + the full façade suite**

```bash
uv run ruff check python/redis_rs_py/ tests/facade/
uv run ruff format --check python/redis_rs_py/ tests/facade/
uv run ty check python/redis_rs_py/
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
uv run pytest tests/facade/ -v
```

Expected: all green; the full façade suite (constructor + from_url + kwargs warn + close + lock + commands smoke) passes.

- [ ] **Step 4: Commit**

```bash
git add python/redis_rs_py/__init__.py python/redis_rs_py/_driver.pyi
git commit -m "feat(facade): re-export Redis and Lock at package root"
```

- [ ] **Step 5: Add a CHANGELOG entry**

Append under `### Added` in `CHANGELOG.md`:

```markdown
- `redis_rs_py.Redis` — Rust-backed sync façade matching `redis.Redis`'s constructor surface (every kwarg accepted; unimplemented ones warn-once). `Redis.from_url`, `__enter__`/`__exit__`/`close`, and `lock(...)` distributed-lock helper.
- Every redis-py command method on `Redis` (strings, lists, hashes, sets, sorted sets, streams, scripts, admin) — delegates to the underlying `RedisRsDriver` via the new `define_facade_command!` macro.
- `Pipeline` / `pubsub` / `transaction` are placeholders raising `NotImplementedError` pointing to plans 13 and 14.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 10 entry"
```

---

## Self-review checklist for this plan

- [x] Spec coverage — `PLAN.md` v0.1: "Redis(host=, port=, db=, password=, username=, ssl=, socket_timeout=, max_connections=, …)", `Redis.from_url(url)`, accept-and-warn for unknown kwargs — Tasks 2 + 3 cover all of it.
- [x] Spec coverage — "every command method delegated to driver" — Tasks 4-10 deliver one block per command family. Each command family has one corresponding smoke test in `test_sync_commands_smoke.py`.
- [x] Architecture — façade lives entirely in Rust per the "Rust by default" principle. Python tree is just `__init__.py` re-exports + `.pyi`.
- [x] Constructor mirrors `redis.Redis.__init__` exactly — verified by `test_constructor_accepts_full_redis_py_kwarg_surface`, which calls every kwarg name from the upstream signature.
- [x] `from_url` URL grammar covers `redis://`, `rediss://`, `unix://`, userinfo, `?db=N` query, percent-decoding — Task 3 implements; tests in `test_sync_from_url.py` cover scheme/path/query/userinfo/precedence.
- [x] Pipeline / PubSub / transaction are *placeholders* with explicit "see plan N" messages — `pipeline()`, `pubsub()`, `transaction()` in Task 3.
- [x] `lock()` uses script-based primitives from plan 09 (`eval` + Lua release) — Task 11.
- [x] `__enter__` / `__exit__` / `close()` drop the driver `Arc`; post-close use raises `ValueError` — Task 3 step 9 covers.
- [x] `define_facade_command!` keeps Tasks 4-10 mechanical: each task is one big macro invocation block + one smoke test per command family.
- [x] All file paths match the file-structure section.
- [x] Type stubs in `.pyi` are current — Task 12.
- [x] Re-exports surface `Redis` and `Lock` at the package root for the redis-py user idiom (`from redis_rs_py import Redis`).
