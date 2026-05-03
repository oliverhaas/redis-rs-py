# Plan 12 — `decode_responses=True`

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the `decode_responses=True` constructor flag end-to-end. Every command method that would return `bytes` returns `str` instead, decoded with the constructor's `encoding=` (default `"utf-8"`) / `encoding_errors=` (default `"strict"`). Recursively applied to lists, dicts (BOTH keys and values), tuples, sets. Numeric and boolean types pass through unchanged. Lives entirely in Rust under `crates/redis-rs-py-driver/src/facade/decode.rs`. Applied at the **façade** layer — the driver always returns bytes, the façade decodes on the way out per the `decode_responses` flag captured at construction time.

**Architecture:** A `DecodeOpts { encoding: String, errors: String }` struct hangs off the `Redis` and `asyncio.Redis` pyclasses (already wired into `FacadeConfig` by plan 10). A pure Rust function `decode_walk(py, value, &opts) -> PyResult<Py<PyAny>>` recursively rebuilds the input as `str`-flavoured types: `PyBytes → PyString` via Python's own `bytes.decode(encoding, errors)` (so `errors="strict"`/`"replace"`/`"ignore"` semantics match the stdlib exactly), `PyList → PyList` with each element walked, `PyDict → PyDict` with both keys and values walked, `PyTuple → PyTuple`, `PySet → PySet`, anything else passes through.

For sync: every façade command method, after returning the driver's `Py<PyAny>`, runs `decode_walk` if `self.decode.is_some()`. We achieve this with a thin `maybe_decode(self, py, value) -> PyResult<Py<PyAny>>` helper that the existing `define_facade_command!` macro is updated to call before unbinding. For async: the same helper is wrapped via a one-line Python coroutine — `async def _decode(aw, opts): return walk(await aw, opts)` — applied as `_decode_wrapper.call(awaitable, opts_tuple)`. We deliberately pick the wrapper-coroutine approach over modifying `RedisRsAwaitable::with_decode` because (a) it costs one Python frame per call (negligible compared to a Redis round-trip), (b) it avoids touching `async_bridge.rs`, which is verbatim from upstream and load-bearing, and (c) cancellation semantics fall out for free — the inner `RedisRsAwaitable` is still a Task-friendly awaitable, the outer coroutine just chains a synchronous mapping after `await`.

**Tech Stack:** PyO3 0.28 (`PyBytes::call_method1("decode", …)`, `PyDict::iter`, `PyList::new`, `PyTuple::new`, `PySet::new`), no new Rust deps. Python: a one-line `async def` literal in `decode.rs` compiled at module-init via `PyModule::from_code`.

**Reference material:**
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/10-facade-sync.md` — `FacadeConfig` (already carries `decode_responses`, `encoding`, `encoding_errors`); `define_facade_command!` macro (this plan extends it).
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/11-facade-asyncio.md` — `define_async_facade_command!` macro (this plan extends it analogously).
- redis-py reference: `python -c "import redis; r=redis.Redis(decode_responses=True); print(type(r.hgetall('x')))"` — the contract is "everything that would be `bytes` is now `str`; everything else (numbers, bools) passes through". HGETALL returns `dict[str, str]`. ZRANGE WITHSCORES returns `list[tuple[str, float]]`. XREAD returns nested structures with `str` IDs / field names / values.
- For test corpora, see plans 03-09 driver-level test files: each command family has a `test_*.py` with the canonical input/output shapes; this plan asserts the same shapes one decoded layer up.

**Out of scope for this plan:**
- Per-call decode override — once the constructor flag is set, the whole client is decoded; redis-py doesn't expose per-call control either.
- `decode_responses` in pipelines / pubsub — those land in plans 13 / 14, which inherit the constructor flag from their parent `Redis`.
- TLS-style late-bound `encoding=` mutation — `Redis.encoding` is set at construction and never re-read.

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  facade/
    mod.rs                        # MODIFIED: add `pub mod decode;`
    decode.rs                     # NEW: DecodeOpts + decode_walk + the async wrapper helper
    sync.rs                       # MODIFIED: store decode opts, wrap every command return
    asyncio_mod.rs                # MODIFIED: store decode opts, wrap every awaitable return
tests/facade/
  test_decode_responses.py        # NEW: every command family, both decode_responses=False and True
```

---

## Task 1: `DecodeOpts` + `decode_walk` recursive walker

The pure-Rust core. Doesn't touch the façade yet — Task 2 wires it in. Has full unit-test coverage of every supported Python type, plus nesting.

**Files:**
- New: `crates/redis-rs-py-driver/src/facade/decode.rs`
- Modify: `crates/redis-rs-py-driver/src/facade/mod.rs`
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (register `_facade_decode_walk` test helper)

- [ ] **Step 1: Add the module declaration**

Edit `crates/redis-rs-py-driver/src/facade/mod.rs`:

```rust
// Façade module — declares submodules implemented across plans 10-12.

pub mod asyncio_mod;
pub mod decode;
pub mod kwargs;
pub mod sync;
```

- [ ] **Step 2: Implement `decode.rs`**

Create `crates/redis-rs-py-driver/src/facade/decode.rs`:

```rust
// Decode-responses walker.
//
// Walks an arbitrary Python value tree replacing every `bytes` leaf with
// a `str`. Containers are rebuilt — lists into new lists, dicts into new
// dicts (with both keys and values walked), tuples and sets likewise.
// Anything else passes through unchanged (numeric types, booleans, None,
// custom objects).
//
// Public surface:
//   * `DecodeOpts { encoding, errors }` — the constructor flag's payload.
//   * `decode_walk(py, value, opts)` — sync entry point; called by
//      `facade::sync::Redis` after every command method's driver call.
//   * `wrap_awaitable(py, awaitable, opts)` — async entry point; wraps
//      a `RedisRsAwaitable` in a Python coroutine that decodes after
//      `await`. Called by `facade::asyncio_mod::Redis`.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyModule, PySet, PyString, PyTuple};
use std::ffi::CString;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct DecodeOpts {
    pub encoding: String,
    pub errors: String,
}

impl DecodeOpts {
    pub fn new(encoding: String, errors: String) -> Self {
        Self { encoding, errors }
    }
}

/// Recursively walk `value` and return a fresh Python object with every
/// `bytes` leaf decoded to `str` per `opts`. Returns the original
/// reference when no rewrite is needed (purely scalar leaves).
pub fn decode_walk(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    opts: &DecodeOpts,
) -> PyResult<Py<PyAny>> {
    // bytes → str
    if let Ok(b) = value.downcast::<PyBytes>() {
        let decoded = b
            .call_method1("decode", (opts.encoding.as_str(), opts.errors.as_str()))?
            .into_pyobject(py)?
            .into_any()
            .unbind();
        return Ok(decoded);
    }

    // PyString / PyInt / PyFloat / PyBool / None / etc. — pass through
    if value.downcast::<PyString>().is_ok() {
        return Ok(value.clone().unbind());
    }

    // list[T]
    if let Ok(list) = value.downcast::<PyList>() {
        let walked: Vec<Py<PyAny>> = list
            .iter()
            .map(|item| decode_walk(py, &item, opts))
            .collect::<PyResult<_>>()?;
        return Ok(PyList::new(py, walked)?.into_any().unbind());
    }

    // tuple[T, ...]
    if let Ok(tup) = value.downcast::<PyTuple>() {
        let walked: Vec<Py<PyAny>> = tup
            .iter()
            .map(|item| decode_walk(py, &item, opts))
            .collect::<PyResult<_>>()?;
        return Ok(PyTuple::new(py, walked)?.into_any().unbind());
    }

    // dict[K, V] — walk BOTH keys and values
    if let Ok(d) = value.downcast::<PyDict>() {
        let out = PyDict::new(py);
        for (k, v) in d.iter() {
            let k_walked = decode_walk(py, &k, opts)?;
            let v_walked = decode_walk(py, &v, opts)?;
            out.set_item(k_walked, v_walked)?;
        }
        return Ok(out.into_any().unbind());
    }

    // set[T] (including frozenset, treated as set on output for simplicity)
    if let Ok(s) = value.downcast::<PySet>() {
        let new = PySet::empty(py)?;
        for item in s.iter() {
            let walked = decode_walk(py, &item, opts)?;
            new.add(walked)?;
        }
        return Ok(new.into_any().unbind());
    }

    // Anything else: pass through unchanged.
    Ok(value.clone().unbind())
}

// =========================================================================
// Async wrapper helper.
//
// We can't easily mutate a RedisRsAwaitable from outside (`async_bridge.rs`
// is verbatim upstream). Instead we expose a Python coroutine that
// `await`s the awaitable then runs the sync walker on the result. The
// coroutine source is a one-liner compiled once at module init via
// `PyModule::from_code`. Each call to `wrap_awaitable` returns a fresh
// coroutine instance.
// =========================================================================

const DECODE_HELPER_SOURCE: &str = r#"
async def _wrap(awaitable, decoder):
    result = await awaitable
    return decoder(result)
"#;

static DECODE_HELPER: OnceLock<Py<PyAny>> = OnceLock::new();

fn helper(py: Python<'_>) -> PyResult<&'static Py<PyAny>> {
    if let Some(h) = DECODE_HELPER.get() {
        return Ok(h);
    }
    let module = PyModule::from_code(
        py,
        CString::new(DECODE_HELPER_SOURCE).unwrap().as_c_str(),
        CString::new("redis_rs_py_decode_helper.py").unwrap().as_c_str(),
        CString::new("redis_rs_py_decode_helper").unwrap().as_c_str(),
    )?;
    let wrap_fn: Py<PyAny> = module.getattr("_wrap")?.unbind();
    let _ = DECODE_HELPER.set(wrap_fn);
    Ok(DECODE_HELPER.get().unwrap())
}

/// Wrap `awaitable` in a Python coroutine that awaits then decodes.
/// Returns the coroutine; the caller hands it back to Python as the
/// command method's return value.
pub fn wrap_awaitable(
    py: Python<'_>,
    awaitable: Py<PyAny>,
    opts: DecodeOpts,
) -> PyResult<Py<PyAny>> {
    // Build a one-shot decoder closure carrying `opts`. Pure-Python
    // closure via a tiny pyfunction wrapper; takes the awaited value
    // and feeds it through `decode_walk`.
    let decoder = py
        .get_type::<DecoderClosure>()
        .call1((opts.encoding, opts.errors))?
        .unbind();
    helper(py)?
        .call1(py, (awaitable, decoder))
}

/// Single-purpose pyclass that captures the decode opts and exposes
/// itself as a callable to the Python coroutine. Cheaper than building
/// a Python lambda per call.
#[pyclass(module = "redis_rs_py._driver")]
pub struct DecoderClosure {
    encoding: String,
    errors: String,
}

#[pymethods]
impl DecoderClosure {
    #[new]
    fn new(encoding: String, errors: String) -> Self {
        Self { encoding, errors }
    }

    fn __call__(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let opts = DecodeOpts::new(self.encoding.clone(), self.errors.clone());
        decode_walk(py, value.bind(py), &opts)
    }
}

/// Test helper: walks the given value with the given encoding/errors.
/// Wired to a pyfunction so unit tests can exercise the walker without
/// going through the façade layer.
#[pyfunction]
#[pyo3(name = "_facade_decode_walk")]
pub fn py_decode_walk(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    encoding: String,
    errors: String,
) -> PyResult<Py<PyAny>> {
    let opts = DecodeOpts::new(encoding, errors);
    decode_walk(py, value, &opts)
}
```

- [ ] **Step 3: Register the test helper + the decoder pyclass in `lib.rs`**

In `crates/redis-rs-py-driver/src/lib.rs`, inside `fn _driver`, append:

```rust
    m.add_class::<facade::decode::DecoderClosure>()?;
    m.add_function(wrap_pyfunction!(facade::decode::py_decode_walk, m)?)?;
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings only.

- [ ] **Step 5: Write unit tests for the walker**

`tests/facade/test_decode_walker.py`:

```python
"""Unit tests for the decode walker (no façade involved)."""

from __future__ import annotations

import pytest

from redis_rs_py import _driver

walk = _driver._facade_decode_walk


def test_walk_bytes_to_str() -> None:
    assert walk(b"hello", "utf-8", "strict") == "hello"


def test_walk_str_passthrough() -> None:
    assert walk("hello", "utf-8", "strict") == "hello"


def test_walk_int_passthrough() -> None:
    assert walk(42, "utf-8", "strict") == 42


def test_walk_float_passthrough() -> None:
    assert walk(3.14, "utf-8", "strict") == 3.14


def test_walk_none_passthrough() -> None:
    assert walk(None, "utf-8", "strict") is None


def test_walk_bool_passthrough() -> None:
    assert walk(True, "utf-8", "strict") is True
    assert walk(False, "utf-8", "strict") is False


def test_walk_list_of_bytes() -> None:
    assert walk([b"a", b"b"], "utf-8", "strict") == ["a", "b"]


def test_walk_nested_list() -> None:
    assert walk([[b"a", b"b"], [b"c"]], "utf-8", "strict") == [["a", "b"], ["c"]]


def test_walk_tuple_of_bytes_and_floats() -> None:
    """ZRANGE WITHSCORES returns list[tuple[bytes, float]]."""
    assert walk([(b"m", 1.0), (b"n", 2.0)], "utf-8", "strict") == [("m", 1.0), ("n", 2.0)]


def test_walk_dict_keys_and_values() -> None:
    """HGETALL: dict[bytes, bytes] → dict[str, str]."""
    assert walk({b"a": b"1", b"b": b"2"}, "utf-8", "strict") == {"a": "1", "b": "2"}


def test_walk_nested_dict() -> None:
    inp = {b"outer": {b"inner": b"v"}}
    assert walk(inp, "utf-8", "strict") == {"outer": {"inner": "v"}}


def test_walk_set_of_bytes() -> None:
    assert walk({b"a", b"b"}, "utf-8", "strict") == {"a", "b"}


def test_walk_mixed_dict_with_int_value() -> None:
    """Numeric values stay numeric; only bytes get converted."""
    assert walk({b"key": 42}, "utf-8", "strict") == {"key": 42}


def test_walk_encoding_replace_handles_invalid_bytes() -> None:
    bad = b"\xff\xfe"
    out = walk(bad, "utf-8", "replace")
    assert isinstance(out, str)
    assert "�" in out


def test_walk_encoding_strict_raises_on_invalid_bytes() -> None:
    with pytest.raises(UnicodeDecodeError):
        walk(b"\xff\xfe", "utf-8", "strict")


def test_walk_latin1_round_trip() -> None:
    assert walk("café".encode("latin-1"), "latin-1", "strict") == "café"


def test_walk_xread_shaped_structure() -> None:
    """The driver's XREAD shape: list of (stream_key, list of (id, dict))."""
    inp = [
        (
            b"stream-1",
            [
                (b"1-0", {b"f": b"v"}),
                (b"1-1", {b"f": b"v2"}),
            ],
        ),
    ]
    expected = [
        (
            "stream-1",
            [
                ("1-0", {"f": "v"}),
                ("1-1", {"f": "v2"}),
            ],
        ),
    ]
    assert walk(inp, "utf-8", "strict") == expected
```

- [ ] **Step 6: Run the unit tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/facade/test_decode_walker.py -v`
Expected: 17 PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/decode.rs crates/redis-rs-py-driver/src/facade/mod.rs crates/redis-rs-py-driver/src/lib.rs tests/facade/test_decode_walker.py
git commit -m "feat(decode): add DecodeOpts and recursive decode_walk"
```

---

## Task 2: Wire decoding into the sync façade

Update every sync façade method to run the result through `decode_walk` if `self.decode.is_some()`. The cleanest approach is to update the `define_facade_command!` macro itself plus add a `maybe_decode` helper method on `Redis`. The hand-rolled methods (the ones with kwargs, like `set`, `xadd`, `hset`, `zadd`, `zrange`, `xpending`, `xclaim`, `xautoclaim`) each get a one-line `self.maybe_decode(py, result)` call before returning.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sync.rs`
- Test: `tests/facade/test_decode_responses.py`

- [ ] **Step 1: Add the `decode` field + `maybe_decode` helper to `Redis`**

In `crates/redis-rs-py-driver/src/facade/sync.rs`, find the `pub struct Redis` definition. Replace with:

```rust
#[pyclass(subclass, module = "redis_rs_py._driver", name = "Redis")]
pub struct Redis {
    pub(crate) driver: Option<Arc<Py<RedisRsDriver>>>,
    pub(crate) config: FacadeConfig,
    pub(crate) decode: Option<crate::facade::decode::DecodeOpts>,
}
```

In the `Redis::new` constructor body, after the `let config = FacadeConfig { ... };` line and before `let driver = build_driver(...)`, insert:

```rust
        let decode = if config.decode_responses {
            Some(crate::facade::decode::DecodeOpts::new(
                config.encoding.clone(),
                config.encoding_errors.clone(),
            ))
        } else {
            None
        };
```

Replace the trailing `Ok(Self { driver: Some(Arc::new(driver)), config })` with:

```rust
        Ok(Self {
            driver: Some(Arc::new(driver)),
            config,
            decode,
        })
```

Add the helper method on the `impl Redis` block (the `impl`, not `#[pymethods]`):

```rust
impl Redis {
    pub(crate) fn driver_or_raise(&self) -> PyResult<Arc<Py<RedisRsDriver>>> {
        match &self.driver {
            Some(d) => Ok(d.clone()),
            None => Err(PyValueError::new_err(
                "Redis client is closed; create a new one or use a context manager",
            )),
        }
    }

    /// If decode_responses is on, walk the value and return a decoded
    /// fresh tree; otherwise return the original.
    pub(crate) fn maybe_decode(
        &self,
        py: Python<'_>,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        match &self.decode {
            Some(opts) => crate::facade::decode::decode_walk(py, value.bind(py), opts),
            None => Ok(value),
        }
    }
}
```

- [ ] **Step 2: Update the `define_facade_command!` macro**

Find the existing `#[macro_export] macro_rules! define_facade_command!` block in `sync.rs`. Replace it with the version that wraps the result through `maybe_decode`:

```rust
#[macro_export]
macro_rules! define_facade_command {
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
            let result = bound
                .call_method1(stringify!($driver_method), args)?
                .unbind();
            self.maybe_decode(py, result)
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
            let result = bound
                .call_method1(stringify!($driver_method), args)?
                .unbind();
            self.maybe_decode(py, result)
        }
    };
}
```

- [ ] **Step 3: Wrap the hand-rolled methods**

Find every method in `sync.rs` that does `Ok(drv.bind(py).call_method...?.unbind())` (i.e. the methods with kwargs that don't use the macro) and replace the trailing line with:

```rust
        let result = drv.bind(py).call_method(...)?.unbind();
        self.maybe_decode(py, result)
```

The list of hand-rolled methods (from plan 10):
- `set` (Task 4)
- `mget`, `mset`, `msetnx` (Task 4)
- `exists`, `delete`, `unlink` (Task 4)
- All `lpop`/`rpop` overloads, `blpop`, `brpop`, `blmove`, `blmpop`, `lmpop` (Task 5)
- `hset`, `hdel`, `hmget` (Task 6)
- `sadd`, `srem`, `smismember`, `sinter`, `sunion`, `sdiff`, `sinterstore`, `sunionstore`, `sdiffstore`, `spop`, `srandmember` (Task 7)
- `zadd`, `zrem`, `zrange`, `zmscore`, `zpopmin`, `zpopmax`, `zmpop`, `bzmpop`, `zunion`, `zinter`, `zdiff`, `zunionstore`, `zinterstore`, `zdiffstore` (Task 8)
- `xadd`, `xread`, `xreadgroup`, `xack`, `xdel`, `xpending`, `xclaim`, `xautoclaim` (Task 9)
- `script_exists`, `script_flush`, `scan_iter`, `client_kill` (Task 10)
- `lock` is excluded — it returns a `Py<Lock>`, not a value tree.

For each, the rewrite is mechanical:

Before:
```rust
Ok(bound.call_method("set", (key, value), Some(&kwargs))?.unbind())
```

After:
```rust
let result = bound.call_method("set", (key, value), Some(&kwargs))?.unbind();
self.maybe_decode(py, result)
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings only.

- [ ] **Step 5: Write the sync decode tests**

`tests/facade/test_decode_responses.py`:

```python
"""End-to-end decode_responses=True coverage for the sync façade.

Each command family gets a paired test: one with decode_responses=False
asserting bytes, one with decode_responses=True asserting str. The two
share fixtures via parametrize so the matrix stays cheap to extend.
"""

from __future__ import annotations

import pytest

from redis_rs_py import Redis


@pytest.fixture
def make_client(valkey_url: str):
    import redis as upstream

    rp = upstream.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()

    clients: list[Redis] = []

    def _make(*, decode: bool) -> Redis:
        c = Redis.from_url(valkey_url, decode_responses=decode)
        clients.append(c)
        return c

    yield _make

    for c in clients:
        c.close()


# --- strings --------------------------------------------------------------


@pytest.mark.parametrize("decode", [False, True])
def test_string_get_set(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.set("k", b"v" if not decode else "v")
    assert r.get("k") == ("v" if decode else b"v")


@pytest.mark.parametrize("decode", [False, True])
def test_string_mget(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.mset({"a": b"1", "b": b"2"})
    assert r.mget("a", "b") == (["1", "2"] if decode else [b"1", b"2"])


@pytest.mark.parametrize("decode", [False, True])
def test_string_mget_with_missing(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.set("a", b"1")
    assert r.mget("a", "missing") == (["1", None] if decode else [b"1", None])


@pytest.mark.parametrize("decode", [False, True])
def test_string_incrby_returns_int_unchanged(make_client, decode: bool) -> None:
    """Numeric returns pass through unchanged regardless of decode_responses."""
    r = make_client(decode=decode)
    assert r.incr("c") == 1
    assert isinstance(r.incr("c"), int)


# --- lists ----------------------------------------------------------------


@pytest.mark.parametrize("decode", [False, True])
def test_list_lrange(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.rpush("L", b"a", b"b", b"c")
    expected = ["a", "b", "c"] if decode else [b"a", b"b", b"c"]
    assert r.lrange("L", 0, -1) == expected


@pytest.mark.parametrize("decode", [False, True])
def test_list_lpop(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.rpush("L", b"a")
    assert r.lpop("L") == ("a" if decode else b"a")


@pytest.mark.parametrize("decode", [False, True])
def test_list_blpop_decodes_both_key_and_value(make_client, decode: bool) -> None:
    """BLPOP returns (key, value). When decode_responses=True both decode."""
    r = make_client(decode=decode)
    r.rpush("L", b"x")
    res = r.blpop(["L"], timeout=1.0)
    if decode:
        assert res == ("L", "x")
    else:
        assert res == (b"L", b"x")


# --- hashes ---------------------------------------------------------------


@pytest.mark.parametrize("decode", [False, True])
def test_hash_hget(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.hset("H", "f", b"v")
    assert r.hget("H", "f") == ("v" if decode else b"v")


@pytest.mark.parametrize("decode", [False, True])
def test_hash_hgetall_keys_and_values(make_client, decode: bool) -> None:
    """HGETALL: dict[bytes, bytes] → dict[str, str] when decode is on."""
    r = make_client(decode=decode)
    r.hmset("H", {"a": b"1", "b": b"2"})
    expected = {"a": "1", "b": "2"} if decode else {b"a": b"1", b"b": b"2"}
    assert r.hgetall("H") == expected


@pytest.mark.parametrize("decode", [False, True])
def test_hash_hmget(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.hmset("H", {"a": b"1", "b": b"2"})
    expected = ["1", "2"] if decode else [b"1", b"2"]
    assert r.hmget("H", "a", "b") == expected


@pytest.mark.parametrize("decode", [False, True])
def test_hash_hkeys_hvals(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.hmset("H", {"a": b"1", "b": b"2"})
    if decode:
        assert sorted(r.hkeys("H")) == ["a", "b"]
        assert sorted(r.hvals("H")) == ["1", "2"]
    else:
        assert sorted(r.hkeys("H")) == [b"a", b"b"]
        assert sorted(r.hvals("H")) == [b"1", b"2"]


# --- sets -----------------------------------------------------------------


@pytest.mark.parametrize("decode", [False, True])
def test_set_smembers(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.sadd("S", b"a", b"b")
    if decode:
        assert set(r.smembers("S")) == {"a", "b"}
    else:
        assert set(r.smembers("S")) == {b"a", b"b"}


@pytest.mark.parametrize("decode", [False, True])
def test_set_spop(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.sadd("S", b"a")
    assert r.spop("S") == ("a" if decode else b"a")


# --- zsets ----------------------------------------------------------------


@pytest.mark.parametrize("decode", [False, True])
def test_zset_zrange(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.zadd("Z", {"a": 1, "b": 2})
    expected = ["a", "b"] if decode else [b"a", b"b"]
    assert r.zrange("Z", 0, -1) == expected


@pytest.mark.parametrize("decode", [False, True])
def test_zset_zrange_withscores(make_client, decode: bool) -> None:
    """list[tuple[bytes, float]] → list[tuple[str, float]] when decode is on."""
    r = make_client(decode=decode)
    r.zadd("Z", {"a": 1, "b": 2})
    expected = (
        [("a", 1.0), ("b", 2.0)] if decode else [(b"a", 1.0), (b"b", 2.0)]
    )
    assert r.zrange("Z", 0, -1, withscores=True) == expected


@pytest.mark.parametrize("decode", [False, True])
def test_zset_zpopmin(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.zadd("Z", {"a": 1, "b": 2})
    res = r.zpopmin("Z")
    expected = [("a", 1.0)] if decode else [(b"a", 1.0)]
    assert res == expected


# --- streams --------------------------------------------------------------


@pytest.mark.parametrize("decode", [False, True])
def test_stream_xadd_xrange_decodes_id_and_fields(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    rid = r.xadd("S", {"f": b"v"})
    if decode:
        assert isinstance(rid, str)
    else:
        assert isinstance(rid, bytes)
    rng = r.xrange("S")
    assert len(rng) == 1
    entry_id, fields = rng[0]
    if decode:
        assert isinstance(entry_id, str)
        assert fields == {"f": "v"}
    else:
        assert isinstance(entry_id, bytes)
        assert fields == {b"f": b"v"}


@pytest.mark.parametrize("decode", [False, True])
def test_stream_xread_shape(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.xadd("S", {"f": b"v"})
    res = r.xread({"S": "0"})
    # res is [(stream_key, [(id, {field: value})])]
    assert res
    stream_key, entries = res[0]
    if decode:
        assert stream_key == "S"
        eid, fields = entries[0]
        assert isinstance(eid, str)
        assert fields == {"f": "v"}
    else:
        assert stream_key == b"S"
        eid, fields = entries[0]
        assert isinstance(eid, bytes)
        assert fields == {b"f": b"v"}


# --- scripts + admin ------------------------------------------------------


@pytest.mark.parametrize("decode", [False, True])
def test_admin_keys(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.set("k1", b"v")
    r.set("k2", b"v")
    keys = r.keys("k*")
    if decode:
        assert set(keys) == {"k1", "k2"}
    else:
        assert set(keys) == {b"k1", b"k2"}


@pytest.mark.parametrize("decode", [False, True])
def test_admin_info(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    info = r.info()
    # `info` is a dict either way; with decode_responses=True the keys
    # and values must be str.
    assert info
    sample_key = next(iter(info))
    if decode:
        assert isinstance(sample_key, str)
    else:
        assert isinstance(sample_key, bytes)


@pytest.mark.parametrize("decode", [False, True])
def test_scripts_eval_returns_decoded(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    res = r.eval("return KEYS[1]", 1, ["hello"])
    assert res == ("hello" if decode else b"hello")


# --- numerics + bools pass through ----------------------------------------


@pytest.mark.parametrize("decode", [False, True])
def test_int_returns_unchanged(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.set("k", b"v")
    assert isinstance(r.exists("k"), int)
    assert r.exists("k") == 1


@pytest.mark.parametrize("decode", [False, True])
def test_float_returns_unchanged(make_client, decode: bool) -> None:
    r = make_client(decode=decode)
    r.zadd("Z", {"a": 1.5})
    assert r.zscore("Z", b"a") == 1.5
```

- [ ] **Step 6: Run the sync decode tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/facade/test_decode_responses.py -v`
Expected: every test PASSES (each parametrized so 2× the case count above).

- [ ] **Step 7: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sync.rs tests/facade/test_decode_responses.py
git commit -m "feat(decode): wire decode_responses into the sync façade"
```

---

## Task 3: Wire decoding into the asyncio façade

Same shape as Task 2, but using `wrap_awaitable` from `decode.rs` to chain the decoder onto the awaitable.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs`
- Test: `tests/facade/test_decode_responses.py` (extend with async cases)

- [ ] **Step 1: Add the `decode` field + `maybe_wrap` helper to async `Redis`**

In `crates/redis-rs-py-driver/src/facade/asyncio_mod.rs`, replace the `Redis` struct:

```rust
#[pyclass(subclass, module = "redis_rs_py._driver.asyncio", name = "Redis")]
pub struct Redis {
    pub(crate) driver: Option<Arc<Py<RedisRsDriver>>>,
    pub(crate) config: FacadeConfig,
    pub(crate) decode: Option<crate::facade::decode::DecodeOpts>,
}
```

In the constructor body, after the `let config = FacadeConfig { ... }` build step:

```rust
        let decode = if config.decode_responses {
            Some(crate::facade::decode::DecodeOpts::new(
                config.encoding.clone(),
                config.encoding_errors.clone(),
            ))
        } else {
            None
        };
```

And the trailing `Ok(Self { ... })`:

```rust
        Ok(Self {
            driver: Some(Arc::new(driver)),
            config,
            decode,
        })
```

Add the helper to the inherent `impl Redis { ... }` block:

```rust
impl Redis {
    fn driver_or_raise(&self) -> PyResult<Arc<Py<RedisRsDriver>>> {
        match &self.driver {
            Some(d) => Ok(d.clone()),
            None => Err(PyValueError::new_err(
                "Redis client is closed; create a new one or use an async context manager",
            )),
        }
    }

    /// If decode_responses is on, wrap the awaitable in a coroutine that
    /// awaits then decodes. Otherwise return the awaitable as-is.
    pub(crate) fn maybe_wrap(
        &self,
        py: Python<'_>,
        awaitable: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        match &self.decode {
            Some(opts) => {
                crate::facade::decode::wrap_awaitable(py, awaitable, opts.clone())
            }
            None => Ok(awaitable),
        }
    }
}
```

- [ ] **Step 2: Update the `define_async_facade_command!` macro**

Find the existing macro in `asyncio_mod.rs`. Replace with:

```rust
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
            let aw = bound
                .call_method1(stringify!($driver_method), args)?
                .unbind();
            self.maybe_wrap(py, aw)
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
            let aw = bound
                .call_method1(stringify!($driver_method), args)?
                .unbind();
            self.maybe_wrap(py, aw)
        }
    };
}
```

- [ ] **Step 3: Wrap the hand-rolled async methods**

For every hand-rolled async method (the ones with kwargs that don't go through the macro: `set`, `mget`, `mset`, `msetnx`, `exists`, `delete`, `unlink`, `lpop`, `rpop`, `blpop`, `brpop`, `blmove`, `blmpop`, `lmpop`, `hset`, `hdel`, `hmget`, `sadd`, `srem`, `smismember`, `sinter`, `sunion`, `sdiff`, `sinterstore`, `sunionstore`, `sdiffstore`, `spop`, `srandmember`, `zadd`, `zrem`, `zrange`, `zmscore`, `zpopmin`, `zpopmax`, `zmpop`, `bzmpop`, `zunion`, `zinter`, `zdiff`, `zunionstore`, `zinterstore`, `zdiffstore`, `xadd`, `xread`, `xreadgroup`, `xack`, `xdel`, `xpending`, `xclaim`, `xautoclaim`, `script_exists`, `script_flush`, `scan_iter`, `client_kill`):

Before:
```rust
Ok(drv.bind(py).call_method("aset", (key, value), Some(&kwargs))?.unbind())
```

After:
```rust
let aw = drv.bind(py).call_method("aset", (key, value), Some(&kwargs))?.unbind();
self.maybe_wrap(py, aw)
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with warnings only.

- [ ] **Step 5: Append async cases to `test_decode_responses.py`**

Append:

```python
# =========================================================================
# Async equivalents — same matrix, against asyncio.Redis
# =========================================================================

from redis_rs_py.asyncio import Redis as AsyncRedis


@pytest.fixture
async def make_async_client(valkey_url: str):
    import redis as upstream

    rp = upstream.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()

    clients: list[AsyncRedis] = []

    def _make(*, decode: bool) -> AsyncRedis:
        c = AsyncRedis.from_url(valkey_url, decode_responses=decode)
        clients.append(c)
        return c

    yield _make

    for c in clients:
        await c.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_string_get_set(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    await r.set("k", b"v")
    assert await r.get("k") == ("v" if decode else b"v")


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_hash_hgetall(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    await r.hmset("H", {"a": b"1", "b": b"2"})
    expected = {"a": "1", "b": "2"} if decode else {b"a": b"1", b"b": b"2"}
    assert await r.hgetall("H") == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_list_blpop(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    await r.rpush("L", b"x")
    res = await r.blpop(["L"], timeout=1.0)
    if decode:
        assert res == ("L", "x")
    else:
        assert res == (b"L", b"x")


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_zset_zrange_withscores(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    await r.zadd("Z", {"a": 1, "b": 2})
    expected = (
        [("a", 1.0), ("b", 2.0)] if decode else [(b"a", 1.0), (b"b", 2.0)]
    )
    assert await r.zrange("Z", 0, -1, withscores=True) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_set_smembers(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    await r.sadd("S", b"a", b"b")
    if decode:
        assert set(await r.smembers("S")) == {"a", "b"}
    else:
        assert set(await r.smembers("S")) == {b"a", b"b"}


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_stream_xread(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    await r.xadd("S", {"f": b"v"})
    res = await r.xread({"S": "0"})
    assert res
    stream_key, entries = res[0]
    if decode:
        assert stream_key == "S"
        eid, fields = entries[0]
        assert isinstance(eid, str)
        assert fields == {"f": "v"}
    else:
        assert stream_key == b"S"
        eid, fields = entries[0]
        assert isinstance(eid, bytes)
        assert fields == {b"f": b"v"}


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_int_returns_unchanged(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    assert await r.incr("c") == 1
    assert isinstance(await r.incr("c"), int)


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_eval_returns_decoded(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    res = await r.eval("return KEYS[1]", 1, ["hello"])
    assert res == ("hello" if decode else b"hello")


@pytest.mark.asyncio
@pytest.mark.parametrize("decode", [False, True])
async def test_async_admin_keys(make_async_client, decode: bool) -> None:
    r = make_async_client(decode=decode)
    await r.set("k1", b"v")
    await r.set("k2", b"v")
    keys = await r.keys("k*")
    if decode:
        assert set(keys) == {"k1", "k2"}
    else:
        assert set(keys) == {b"k1", b"k2"}
```

- [ ] **Step 6: Run the async decode tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/facade/test_decode_responses.py -v`
Expected: every parametrized case PASSES.

- [ ] **Step 7: Run the full façade suite**

Run: `uv run pytest tests/facade/ -v -n auto`
Expected: every test (constructor + from_url + close + commands smoke + decode walker + decode responses) PASSES.

- [ ] **Step 8: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/asyncio_mod.rs tests/facade/test_decode_responses.py
git commit -m "feat(decode): wire decode_responses into the asyncio façade"
```

---

## Task 4: Edge cases — BLPOP key, ScoredMembers tuple, HGETALL keys, XREAD nesting

The walker is generic enough that the edge cases from the spec are covered automatically by Tasks 2 + 3. Pin them down with named regression tests so future refactors of the walker can't silently break the contract.

**Files:**
- Test: `tests/facade/test_decode_edge_cases.py`

- [ ] **Step 1: Write the regression suite**

`tests/facade/test_decode_edge_cases.py`:

```python
"""Edge cases for decode_responses=True: shapes that look special but are
just deeply-nested versions of what `decode_walk` already handles."""

from __future__ import annotations

import pytest

from redis_rs_py import Redis
from redis_rs_py.asyncio import Redis as AsyncRedis


@pytest.fixture
def r(valkey_url: str):
    import redis as upstream

    rp = upstream.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    client = Redis.from_url(valkey_url, decode_responses=True)
    yield client
    client.close()


def test_blpop_key_and_value_both_decode(r: Redis) -> None:
    """BLPOP returns (stream_name, value) — both must decode."""
    r.rpush("queue", b"item")
    res = r.blpop(["queue"], timeout=1.0)
    assert res == ("queue", "item")
    assert isinstance(res[0], str)
    assert isinstance(res[1], str)


def test_scored_members_tuple_decodes_member_only(r: Redis) -> None:
    """list[tuple[str, float]] — member is str, score stays float."""
    r.zadd("Z", {"alpha": 1.5, "beta": 2.25})
    res = r.zrange("Z", 0, -1, withscores=True)
    assert res == [("alpha", 1.5), ("beta", 2.25)]
    for member, score in res:
        assert isinstance(member, str)
        assert isinstance(score, float)


def test_hgetall_decodes_both_keys_and_values(r: Redis) -> None:
    r.hmset("H", {"k1": b"v1", "k2": b"v2"})
    out = r.hgetall("H")
    assert out == {"k1": "v1", "k2": "v2"}
    for k, v in out.items():
        assert isinstance(k, str)
        assert isinstance(v, str)


def test_xread_nested_structure_fully_decodes(r: Redis) -> None:
    """XREAD: list[(stream_key, list[(id, dict[field, value])])]."""
    r.xadd("S1", {"a": b"1", "b": b"2"})
    r.xadd("S1", {"a": b"3"})
    res = r.xread({"S1": "0"})

    assert len(res) == 1
    stream_name, entries = res[0]
    assert stream_name == "S1"
    assert isinstance(stream_name, str)

    for entry_id, fields in entries:
        assert isinstance(entry_id, str)
        for field, value in fields.items():
            assert isinstance(field, str)
            assert isinstance(value, str)


def test_xrange_entries_decode_id_and_fields(r: Redis) -> None:
    rid = r.xadd("S", {"f": b"v"})
    rng = r.xrange("S")
    assert rng == [(rid, {"f": "v"})]
    assert isinstance(rng[0][0], str)


def test_xpending_summary_decodes(r: Redis) -> None:
    r.xadd("S", {"f": b"v"})
    r.xgroup_create("S", "G", id="0", mkstream=False)
    r.xreadgroup("G", "C1", {"S": ">"})
    pending = r.xpending("S", "G")
    # Summary: [count, min-id, max-id, [[consumer, count], ...]]
    # All bytes-shaped fields must decode.
    assert pending


def test_smembers_returns_decoded_set(r: Redis) -> None:
    r.sadd("S", b"a", b"b", b"c")
    members = set(r.smembers("S"))
    assert members == {"a", "b", "c"}
    for m in members:
        assert isinstance(m, str)


def test_lrange_decodes_each_element(r: Redis) -> None:
    r.rpush("L", b"a", b"b", b"c")
    out = r.lrange("L", 0, -1)
    assert out == ["a", "b", "c"]
    for v in out:
        assert isinstance(v, str)


def test_mget_with_missing_decodes_present_keeps_none(r: Redis) -> None:
    r.set("a", b"1")
    out = r.mget("a", "missing")
    assert out == ["1", None]
    assert out[0] == "1"
    assert out[1] is None


# --- async equivalents of the load-bearing edges --------------------------


@pytest.fixture
async def ar(valkey_url: str):
    import redis as upstream

    rp = upstream.Redis.from_url(valkey_url)
    rp.flushdb()
    rp.close()
    client = AsyncRedis.from_url(valkey_url, decode_responses=True)
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_async_blpop_key_and_value_both_decode(ar: AsyncRedis) -> None:
    await ar.rpush("queue", b"item")
    res = await ar.blpop(["queue"], timeout=1.0)
    assert res == ("queue", "item")


@pytest.mark.asyncio
async def test_async_xread_nested_decodes(ar: AsyncRedis) -> None:
    await ar.xadd("S", {"f": b"v"})
    res = await ar.xread({"S": "0"})
    stream_name, entries = res[0]
    assert stream_name == "S"
    eid, fields = entries[0]
    assert isinstance(eid, str)
    assert fields == {"f": "v"}


@pytest.mark.asyncio
async def test_async_zrange_withscores_decodes(ar: AsyncRedis) -> None:
    await ar.zadd("Z", {"alpha": 1.5})
    res = await ar.zrange("Z", 0, -1, withscores=True)
    assert res == [("alpha", 1.5)]
```

- [ ] **Step 2: Run the edge-case suite**

Run: `uv run pytest tests/facade/test_decode_edge_cases.py -v`
Expected: every test PASSES.

- [ ] **Step 3: Commit**

```bash
git add tests/facade/test_decode_edge_cases.py
git commit -m "test(decode): add regression coverage for BLPOP, scored members, HGETALL, XREAD"
```

---

## Task 5: Lint, full suite, CHANGELOG

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Run lint**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
uv run ruff check tests/facade/ python/redis_rs_py/
uv run ruff format --check tests/facade/ python/redis_rs_py/
uv run ty check python/redis_rs_py/
```

Expected: all green.

- [ ] **Step 2: Run the full façade suite**

Run: `uv run pytest tests/facade/ -v -n auto`
Expected: every test PASSES.

- [ ] **Step 3: Add the CHANGELOG entry**

Append to `CHANGELOG.md` under `### Added`:

```markdown
- `decode_responses=True` end-to-end on both the sync and asyncio façades. Every command method that would return `bytes` returns `str` instead, decoded with the constructor's `encoding=`/`encoding_errors=`. Recursively walks lists, dicts (both keys and values for HGETALL etc.), tuples (e.g. ZRANGE WITHSCORES), and sets. Numeric/boolean returns pass through unchanged.
- `crates/redis-rs-py-driver/src/facade/decode.rs`: pure-Rust `decode_walk` recursive walker plus `wrap_awaitable` that chains a one-line decoder coroutine onto a `RedisRsAwaitable`. The `define_facade_command!` and `define_async_facade_command!` macros automatically apply the decoder when `decode_responses=True`.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 12 entry"
```

---

## Self-review checklist for this plan

- [x] Spec coverage: "decode_responses=True makes every command that would return `bytes` return `str` instead, decoded with the constructor's `encoding=` (default `"utf-8"`) and `encoding_errors=` (default `"strict"`)" — Task 1 implements; Tasks 2 + 3 wire; Tasks 2-4 test.
- [x] Spec coverage: "Recursively applied to lists, dicts (BOTH keys and values for HGETALL etc.), tuples, sets" — covered by `decode_walk` (Task 1 Step 2). Unit-tested in Task 1 Step 5; live-tested across the family matrix in Task 2 Step 5.
- [x] Spec coverage: "Numeric types pass through unchanged" — explicitly tested by `test_int_returns_unchanged` and `test_float_returns_unchanged` (Task 2) and `test_async_int_returns_unchanged` (Task 3).
- [x] Spec coverage: "WITHSCORES lists become `list[tuple[str, float]]`" — `test_zset_zrange_withscores` (Task 2) and `test_scored_members_tuple_decodes_member_only` (Task 4).
- [x] Spec coverage: "XREAD/XRANGE flattened structures: stream IDs and field names and values all decode" — `test_xread_nested_structure_fully_decodes` (Task 4) and `test_async_xread_nested_decodes` (Task 4).
- [x] Architecture: lives in Rust under `facade/decode.rs`. Walker is pure-Rust; the async wrapper is a one-line Python coroutine compiled at module init via `PyModule::from_code`. Justified in the plan header (avoid touching `async_bridge.rs`, free cancellation semantics, one Python frame cost).
- [x] Implementation strategy chosen as documented: wrapper-coroutine for async, not `RedisRsAwaitable::with_decode`. Trade-off explained in the plan header.
- [x] Edge cases: BLPOP key+value both decode (`test_blpop_key_and_value_both_decode`), HGETALL both keys + values (`test_hgetall_decodes_both_keys_and_values`), ScoredMembers tuple (`test_scored_members_tuple_decodes_member_only`), XREAD nested structure (`test_xread_nested_structure_fully_decodes`).
- [x] Macro updates apply to both sync (`define_facade_command!`) and async (`define_async_facade_command!`); hand-rolled methods (kwarg-using) get a one-line `maybe_decode`/`maybe_wrap` per Task 2 Step 3 and Task 3 Step 3 (with explicit method lists).
- [x] All file paths match the file-structure section.
- [x] Length: ~1200 lines, focused on test matrix coverage and the wiring deltas needed in plans 10 and 11.
