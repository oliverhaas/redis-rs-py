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
//      `await`. Called by `facade::asyncio_mod::AsyncRedis`.

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
    if let Ok(b) = value.cast::<PyBytes>() {
        let decoded = b
            .call_method1("decode", (opts.encoding.as_str(), opts.errors.as_str()))?
            .into_pyobject(py)?
            .into_any()
            .unbind();
        return Ok(decoded);
    }

    // PyString / PyInt / PyFloat / PyBool / None / etc. — pass through
    if value.cast::<PyString>().is_ok() {
        return Ok(value.clone().unbind());
    }

    // list[T]
    if let Ok(list) = value.cast::<PyList>() {
        let walked: Vec<Py<PyAny>> = list
            .iter()
            .map(|item| decode_walk(py, &item, opts))
            .collect::<PyResult<_>>()?;
        return Ok(PyList::new(py, walked)?.into_any().unbind());
    }

    // tuple[T, ...]
    if let Ok(tup) = value.cast::<PyTuple>() {
        let walked: Vec<Py<PyAny>> = tup
            .iter()
            .map(|item| decode_walk(py, &item, opts))
            .collect::<PyResult<_>>()?;
        return Ok(PyTuple::new(py, walked)?.into_any().unbind());
    }

    // dict[K, V] — walk BOTH keys and values
    if let Ok(d) = value.cast::<PyDict>() {
        let out = PyDict::new(py);
        for (k, v) in d.iter() {
            let k_walked = decode_walk(py, &k, opts)?;
            let v_walked = decode_walk(py, &v, opts)?;
            out.set_item(k_walked, v_walked)?;
        }
        return Ok(out.into_any().unbind());
    }

    // set[T] (including frozenset, treated as set on output for simplicity)
    if let Ok(s) = value.cast::<PySet>() {
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
        CString::new("redis_rs_py_decode_helper.py")
            .unwrap()
            .as_c_str(),
        CString::new("redis_rs_py_decode_helper")
            .unwrap()
            .as_c_str(),
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
    opts: &DecodeOpts,
) -> PyResult<Py<PyAny>> {
    // Build a one-shot decoder closure carrying `opts`. Pure-Python
    // closure via a tiny pyfunction wrapper; takes the awaited value
    // and feeds it through `decode_walk`.
    let decoder = DecoderClosure {
        encoding: opts.encoding.clone(),
        errors: opts.errors.clone(),
    };
    let decoder_py = Py::new(py, decoder)?.into_pyobject(py)?.into_any().unbind();
    helper(py)?.call1(py, (awaitable, decoder_py))
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
