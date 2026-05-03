// Shared helpers: macros (async_op! / sync_op!) and Python conversion
// utilities (py_opt_bytes, py_int, etc.) used by every command family.
//
// These were previously defined in driver.rs. They are #[macro_export]
// so they remain importable via `crate::async_op!` / `crate::sync_op!`
// from every commands/* file.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySet, PyString, PyTuple};

// =========================================================================
// Macros: async_op! and sync_op!
// =========================================================================

/// Spawn an async block on the runtime, return a RedisRsAwaitable to Python.
/// `$body` must be an `async { ... }` block that evaluates to a `RawResult`.
/// Raises `ValueError("closed")` immediately if `self.closed` is true.
/// If `self.decode` is Some, the returned awaitable is wrapped in a Python
/// coroutine that decodes the result after awaiting.
#[macro_export]
macro_rules! async_op {
    ($self:expr, $py:expr, $conn:ident, $body:expr) => {{
        if $self.closed {
            return Err(::pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = $crate::async_bridge::RedisRsAwaitable::new(rx);
        let mut $conn = $self.connection.clone();
        $crate::runtime::get_runtime().spawn(async move {
            let result: $crate::async_bridge::RawResult = $body.await;
            let _ = tx.send(result);
        });
        let aw_py = awaitable.into_pyobject($py)?.into_any().unbind();
        $self.maybe_wrap($py, aw_py)
    }};
}

/// Block on the runtime in a GIL-released closure, return the inner Result.
/// `$body` must be an `async { ... }` block.
/// Raises `ValueError("closed")` immediately if `self.closed` is true.
#[macro_export]
macro_rules! sync_op {
    ($py:expr, $self:expr, $conn:ident, $body:expr) => {{
        if $self.closed {
            return Err(::pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut $conn = $self.connection.clone();
        $py.detach(|| $crate::runtime::get_runtime().block_on($body))
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

#[allow(dead_code)]
pub(crate) fn py_int(py: Python<'_>, v: i64) -> PyResult<Py<PyAny>> {
    Ok(v.into_pyobject(py)?.into_any().unbind())
}

#[allow(dead_code)]
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

#[allow(dead_code)]
pub(crate) fn py_set_of_bytes(py: Python<'_>, v: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
    let s = PySet::empty(py)?;
    for b in v {
        s.add(PyBytes::new(py, &b))?;
    }
    Ok(s.into_any().unbind())
}
