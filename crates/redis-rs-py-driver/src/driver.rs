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

use crate::async_bridge::RawResult;
use crate::connection::{ClientCacheOpts, TlsOpts, ValkeyConn, connect_standard, url_with_resp3};
use crate::errors::to_py_err;
use crate::raw_result::IntoRawResult;
use crate::runtime::get_runtime;
use crate::{conn_method, dispatch_cmd};

// =========================================================================
// Macros: async_op! and sync_op!
// =========================================================================

/// Spawn an async block on the runtime, return a RedisRsAwaitable to Python.
/// `$body` must be an `async { ... }` block that evaluates to a `RawResult`.
#[macro_export]
macro_rules! async_op {
    ($self:expr, $py:expr, $conn:ident, $body:expr) => {{
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = $crate::async_bridge::RedisRsAwaitable::new(rx);
        let mut $conn = $self.connection.clone();
        $crate::runtime::get_runtime().spawn(async move {
            let result: $crate::async_bridge::RawResult = $body.await;
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject($py)?.into_any().unbind())
    }};
}

/// Block on the runtime in a GIL-released closure, return the inner Result.
/// `$body` must be an `async { ... }` block.
#[macro_export]
macro_rules! sync_op {
    ($py:expr, $self:expr, $conn:ident, $body:expr) => {{
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
        let tls_opts = if ssl_ca_certs.is_some() || ssl_certfile.is_some() || ssl_keyfile.is_some()
        {
            Some(TlsOpts {
                root_cert: ssl_ca_certs,
                client_cert: ssl_certfile,
                client_key: ssl_keyfile,
            })
        } else {
            None
        };
        // Store the resp3-rewritten URL on the driver so the `connection_url`
        // getter reflects what the connection actually sees. `url_with_resp3`
        // is idempotent — `connect_standard` re-applies it internally without
        // double-rewriting because it short-circuits on `protocol=` already
        // present.
        let url = url_with_resp3(&url);
        let url_clone = url.clone();
        let conn = py.detach(|| {
            get_runtime()
                .block_on(async { connect_standard(&url_clone, cache_opts, tls_opts).await })
        });
        match conn {
            Ok(c) => Ok(RedisRsDriver { connection: c, url }),
            Err(e) => Err(crate::errors::to_py_err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "connect",
                e,
            )))),
        }
    }

    #[getter]
    fn connection_url(&self) -> &str {
        &self.url
    }

    fn cache_statistics(&self) -> Option<(usize, usize, usize)> {
        self.connection
            .cache_statistics()
            .map(|s| (s.hit, s.miss, s.invalidate))
    }

    // --- get / aget --------------------------------------------------------

    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: Result<Option<Vec<u8>>, _> = sync_op!(py, self, conn, async {
            conn_method!(&mut *conn, c, c.get(key))
        });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn aget(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Option<Vec<u8>>> = conn_method!(&mut *conn, c, c.get(&key));
            r.into_raw_result()
        })
    }

    // --- set / aset --------------------------------------------------------

    #[pyo3(signature = (key, value, ttl=None))]
    fn set(&self, py: Python<'_>, key: &str, value: &[u8], ttl: Option<u64>) -> PyResult<()> {
        let value = value.to_vec();
        let r: redis::RedisResult<()> = sync_op!(py, self, conn, async {
            match ttl {
                Some(s) => conn_method!(&mut *conn, c, c.set_ex::<_, _, ()>(key, value, s)),
                None => conn_method!(&mut *conn, c, c.set::<_, _, ()>(key, value)),
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
                Some(s) => conn_method!(&mut *conn, c, c.set_ex::<_, _, ()>(&key, value, s)),
                None => conn_method!(&mut *conn, c, c.set::<_, _, ()>(&key, value)),
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
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            conn_method!(&mut *conn, c, c.del(&keys))
        });
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (*keys))]
    fn adelete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if keys.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn_method!(&mut *conn, c, c.del(&keys));
            r.into_raw_result()
        })
    }

    // --- ping / aping ------------------------------------------------------

    fn ping(&self, py: Python<'_>) -> PyResult<bool> {
        let r: redis::RedisResult<String> = sync_op!(py, self, conn, async {
            dispatch_cmd!(&mut *conn, redis::cmd("PING"))
        });
        match r {
            Ok(s) => Ok(s == "PONG"),
            Err(e) => Err(to_py_err(e)),
        }
    }

    fn aping(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<String> = dispatch_cmd!(&mut *conn, redis::cmd("PING"));
            match r {
                Ok(s) => RawResult::Bool(s == "PONG"),
                Err(e) => crate::errors::classify(e),
            }
        })
    }
}
