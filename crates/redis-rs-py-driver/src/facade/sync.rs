// Sync façade: redis_rs_py.Redis.
//
// Mirrors redis-py's Redis class — same constructor kwargs, same method
// names. Implements the kwargs the driver actually uses, accepts-and-
// warns the rest via `crate::facade::kwargs`. Every command method
// delegates to an internal `RedisRsDriver` via Python-level dispatch.
//
// `decode_responses` is stored on the class; plan 12 wires the actual
// decoding step. Until plan 12 lands, the field is set but unused.

#![allow(clippy::too_many_arguments)]

use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple, PyType};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::driver::RedisRsDriver;
use crate::facade::kwargs::{IMPLEMENTED_KWARGS, accept_and_warn};

// =========================================================================
// Internal config struct
// =========================================================================

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
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
#[allow(dead_code)]
pub struct Redis {
    pub(crate) driver: Option<Arc<Py<RedisRsDriver>>>,
    pub(crate) config: FacadeConfig,
}

/// Convert a Python str, bytes, int, or float into a Python bytes object.
/// - `bytes` → returned as-is
/// - `str`   → UTF-8 encoded bytes
/// - `int` / `float` → str(n) encoded to UTF-8 bytes (redis-py behaviour)
/// - other types → returned as-is; the driver raises its own TypeError
fn to_bytes_obj<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    if obj.is_instance_of::<PyBytes>() {
        return Ok(obj.clone());
    }
    // str → utf-8 bytes
    if let Ok(s) = obj.extract::<String>() {
        return Ok(PyBytes::new(py, s.as_bytes()).into_any());
    }
    // int/float → str representation → bytes (redis-py encodes numbers as their string repr)
    if let Ok(n) = obj.extract::<i64>() {
        return Ok(PyBytes::new(py, n.to_string().as_bytes()).into_any());
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(PyBytes::new(py, f.to_string().as_bytes()).into_any());
    }
    Ok(obj.clone())
}

/// Convert a Python int/float/str to a Python str, for driver methods that take &str scores.
fn to_str_obj<'py>(_py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    if obj.is_instance_of::<pyo3::types::PyString>() {
        return Ok(obj.clone());
    }
    // Convert to str via Python str()
    let s = obj.str()?;
    Ok(s.into_any())
}

/// Build a new PyDict where every value has been converted to bytes.
/// This allows passing `{"key": "str_value"}` to driver methods that expect bytes values.
fn normalize_mapping_values<'py>(
    py: Python<'py>,
    mapping: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    for (k, v) in mapping.iter() {
        let bytes_v = to_bytes_obj(py, &v)?;
        out.set_item(k, bytes_v)?;
    }
    Ok(out)
}

/// Normalise the redis-py pattern where the first arg is either a list/tuple
/// of keys OR a single key string, followed by optional extra `*args` keys.
fn flatten_keys_arg(py: Python<'_>, keys: &Py<PyAny>, args: &[String]) -> PyResult<Vec<String>> {
    let bound = keys.bind(py);
    let mut out: Vec<String> = if let Ok(list) = bound.extract::<Vec<String>>() {
        list
    } else {
        vec![bound.extract::<String>()?]
    };
    out.extend_from_slice(args);
    Ok(out)
}

/// Build a PyTuple of (key, val1, val2, ...) for varargs driver methods.
/// Each value is converted via `to_bytes_obj` (str → bytes).
fn build_varargs_tuple<'py>(
    py: Python<'py>,
    key: &str,
    values: &[Py<PyAny>],
) -> PyResult<Bound<'py, PyTuple>> {
    let mut elems: Vec<Bound<'py, PyAny>> = Vec::with_capacity(1 + values.len());
    elems.push(key.into_pyobject(py)?.into_any());
    for v in values {
        elems.push(to_bytes_obj(py, v.bind(py))?);
    }
    PyTuple::new(py, elems)
}

impl Redis {
    pub(crate) fn driver_or_raise(&self) -> PyResult<Arc<Py<RedisRsDriver>>> {
        match &self.driver {
            Some(d) => Ok(d.clone()),
            None => Err(PyValueError::new_err(
                "Redis client is closed; create a new one or use a context manager",
            )),
        }
    }

    fn drv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RedisRsDriver>> {
        let arc = self.driver_or_raise()?;
        Ok(arc.bind(py).clone())
    }
}

#[pymethods]
impl Redis {
    #[new]
    #[pyo3(signature = (
        host = "localhost".to_string(),
        port = 6379,
        db = None,
        password = None,
        socket_timeout = None,
        encoding = "utf-8".to_string(),
        encoding_errors = "strict".to_string(),
        decode_responses = false,
        ssl = false,
        ssl_keyfile = None,
        ssl_certfile = None,
        ssl_ca_certs = None,
        max_connections = None,
        health_check_interval = 0,
        client_name = None,
        username = None,
        protocol = 2,
        **extra
    ))]
    fn new(
        py: Python<'_>,
        host: String,
        port: u16,
        db: Option<Py<PyAny>>,
        password: Option<String>,
        socket_timeout: Option<f64>,
        encoding: String,
        encoding_errors: String,
        decode_responses: bool,
        ssl: bool,
        ssl_keyfile: Option<String>,
        ssl_certfile: Option<String>,
        ssl_ca_certs: Option<String>,
        max_connections: Option<usize>,
        health_check_interval: u64,
        client_name: Option<String>,
        username: Option<String>,
        protocol: i64,
        extra: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        // db can be int or string (redis-py allows both)
        let db: i64 = match db {
            None => 0,
            Some(v) => {
                let b = v.bind(py);
                if let Ok(n) = b.extract::<i64>() {
                    n
                } else if let Ok(s) = b.extract::<String>() {
                    s.parse::<i64>().unwrap_or(0)
                } else {
                    0
                }
            }
        };
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
        merged_kwargs.set_item("host", &url_cfg.host)?;
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

    #[pyo3(signature = (transaction = true, shard_hint = None))]
    fn pipeline(&self, transaction: bool, shard_hint: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
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

    fn connection_url(&self, py: Python<'_>) -> PyResult<String> {
        let drv = self.drv(py)?;
        let url: String = drv.getattr("connection_url")?.extract()?;
        Ok(url)
    }

    // =========================================================================
    // Lock helper
    // =========================================================================

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
        let _ = lock_class;
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

    // =========================================================================
    // String commands — plan 03 surface.
    // =========================================================================

    fn get(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("get", (key,))?.unbind())
    }

    #[pyo3(signature = (
        key, value,
        ex = None, px = None,
        nx = false, xx = false,
        keepttl = false, get = false,
        exat = None, pxat = None,
    ))]
    fn set(
        &self,
        py: Python<'_>,
        key: String,
        value: Py<PyAny>,
        ex: Option<i64>,
        px: Option<i64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
        exat: Option<i64>,
        pxat: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.drv(py)?;
        let value = to_bytes_obj(py, value.bind(py))?;
        let kw = PyDict::new(py);
        if let Some(v) = ex {
            kw.set_item("ex", v)?;
        }
        if let Some(v) = px {
            kw.set_item("px", v)?;
        }
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if keepttl {
            kw.set_item("keepttl", true)?;
        }
        if get {
            kw.set_item("get", true)?;
        }
        if let Some(v) = exat {
            kw.set_item("exat", v)?;
        }
        if let Some(v) = pxat {
            kw.set_item("pxat", v)?;
        }
        Ok(drv.call_method("set", (key, value), Some(&kw))?.unbind())
    }

    #[pyo3(signature = (key, ex = None, px = None, exat = None, pxat = None, persist = false))]
    fn getex(
        &self,
        py: Python<'_>,
        key: String,
        ex: Option<i64>,
        px: Option<i64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        persist: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if let Some(v) = ex {
            kw.set_item("ex", v)?;
        }
        if let Some(v) = px {
            kw.set_item("px", v)?;
        }
        if let Some(v) = exat {
            kw.set_item("exat", v)?;
        }
        if let Some(v) = pxat {
            kw.set_item("pxat", v)?;
        }
        if persist {
            kw.set_item("persist", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("getex", (key,), Some(&kw))?
            .unbind())
    }

    fn getdel(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("getdel", (key,))?.unbind())
    }

    #[pyo3(signature = (source, destination, db = None, replace = false))]
    fn copy(
        &self,
        py: Python<'_>,
        source: String,
        destination: String,
        db: Option<i64>,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if let Some(d) = db {
            kw.set_item("db", d)?;
        }
        if replace {
            kw.set_item("replace", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("copy", (source, destination), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (key, amount = 1))]
    fn incr(&self, py: Python<'_>, key: String, amount: i64) -> PyResult<Py<PyAny>> {
        if amount == 1 {
            Ok(self.drv(py)?.call_method1("incr", (key,))?.unbind())
        } else {
            Ok(self
                .drv(py)?
                .call_method1("incrby", (key, amount))?
                .unbind())
        }
    }

    #[pyo3(signature = (key, amount = 1))]
    fn incrby(&self, py: Python<'_>, key: String, amount: i64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("incrby", (key, amount))?
            .unbind())
    }

    #[pyo3(signature = (key, amount = 1.0))]
    fn incrbyfloat(&self, py: Python<'_>, key: String, amount: f64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("incrbyfloat", (key, amount))?
            .unbind())
    }

    #[pyo3(signature = (key, amount = 1))]
    fn decr(&self, py: Python<'_>, key: String, amount: i64) -> PyResult<Py<PyAny>> {
        if amount == 1 {
            Ok(self.drv(py)?.call_method1("decr", (key,))?.unbind())
        } else {
            Ok(self
                .drv(py)?
                .call_method1("decrby", (key, amount))?
                .unbind())
        }
    }

    #[pyo3(signature = (key, amount = 1))]
    fn decrby(&self, py: Python<'_>, key: String, amount: i64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("decrby", (key, amount))?
            .unbind())
    }

    fn append(&self, py: Python<'_>, key: String, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self.drv(py)?.call_method1("append", (key, value))?.unbind())
    }

    fn strlen(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("strlen", (key,))?.unbind())
    }

    #[pyo3(signature = (keys, *args))]
    fn mget(&self, py: Python<'_>, keys: Py<PyAny>, args: Vec<String>) -> PyResult<Py<PyAny>> {
        let all_keys = flatten_keys_arg(py, &keys, &args)?;
        Ok(self.drv(py)?.call_method1("mget", (all_keys,))?.unbind())
    }

    fn mset(&self, py: Python<'_>, mapping: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let normalized = normalize_mapping_values(py, &mapping)?;
        Ok(self.drv(py)?.call_method1("mset", (normalized,))?.unbind())
    }

    fn msetnx(&self, py: Python<'_>, mapping: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let normalized = normalize_mapping_values(py, &mapping)?;
        Ok(self
            .drv(py)?
            .call_method1("msetnx", (normalized,))?
            .unbind())
    }

    fn setrange(
        &self,
        py: Python<'_>,
        key: String,
        offset: i64,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("setrange", (key, offset, value))?
            .unbind())
    }

    fn getrange(&self, py: Python<'_>, key: String, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("getrange", (key, start, end))?
            .unbind())
    }

    #[pyo3(signature = (*keys))]
    fn exists(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let pos_args = PyTuple::new(py, &keys)?;
        Ok(self
            .drv(py)?
            .call_method("exists", pos_args, None)?
            .unbind())
    }

    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let pos_args = PyTuple::new(py, &keys)?;
        Ok(self
            .drv(py)?
            .call_method("delete", pos_args, None)?
            .unbind())
    }

    #[pyo3(signature = (*keys))]
    fn unlink(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let pos_args = PyTuple::new(py, &keys)?;
        Ok(self
            .drv(py)?
            .call_method("unlink", pos_args, None)?
            .unbind())
    }

    #[pyo3(signature = (key, time, nx = false, xx = false, gt = false, lt = false))]
    fn expire(
        &self,
        py: Python<'_>,
        key: String,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("expire", (key, time), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (key, time, nx = false, xx = false, gt = false, lt = false))]
    fn pexpire(
        &self,
        py: Python<'_>,
        key: String,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("pexpire", (key, time), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (key, when, nx = false, xx = false, gt = false, lt = false))]
    fn expireat(
        &self,
        py: Python<'_>,
        key: String,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("expireat", (key, when), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (key, when, nx = false, xx = false, gt = false, lt = false))]
    fn pexpireat(
        &self,
        py: Python<'_>,
        key: String,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("pexpireat", (key, when), Some(&kw))?
            .unbind())
    }

    fn expiretime(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("expiretime", (key,))?.unbind())
    }

    fn pexpiretime(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("pexpiretime", (key,))?.unbind())
    }

    fn ttl(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("ttl", (key,))?.unbind())
    }

    fn pttl(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("pttl", (key,))?.unbind())
    }

    fn persist(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("persist", (key,))?.unbind())
    }

    fn rename(&self, py: Python<'_>, src: String, dst: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("rename", (src, dst))?.unbind())
    }

    fn renamenx(&self, py: Python<'_>, src: String, dst: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("renamenx", (src, dst))?.unbind())
    }

    #[pyo3(name = "type")]
    fn type_(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        let raw = self.drv(py)?.call_method1("type", (key,))?;
        // Driver returns a str; redis-py returns bytes.
        if let Ok(s) = raw.extract::<String>() {
            return Ok(PyBytes::new(py, s.as_bytes()).into_any().unbind());
        }
        Ok(raw.unbind())
    }

    fn dump(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("dump", (key,))?.unbind())
    }

    #[pyo3(signature = (key, ttl, value, replace = false, absttl = false, idletime = None, freq = None))]
    fn restore(
        &self,
        py: Python<'_>,
        key: String,
        ttl: i64,
        value: Py<PyAny>,
        replace: bool,
        absttl: bool,
        idletime: Option<i64>,
        freq: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        let kw = PyDict::new(py);
        if replace {
            kw.set_item("replace", true)?;
        }
        if absttl {
            kw.set_item("absttl", true)?;
        }
        if let Some(v) = idletime {
            kw.set_item("idletime", v)?;
        }
        if let Some(v) = freq {
            kw.set_item("frequency", v)?;
        }
        Ok(self
            .drv(py)?
            .call_method("restore", (key, ttl, value), Some(&kw))?
            .unbind())
    }

    // =========================================================================
    // List commands — plan 04 surface.
    // =========================================================================

    #[pyo3(signature = (key, *values))]
    fn lpush(&self, py: Python<'_>, key: String, values: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &key, &values)?;
        Ok(self.drv(py)?.call_method("lpush", args, None)?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn rpush(&self, py: Python<'_>, key: String, values: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &key, &values)?;
        Ok(self.drv(py)?.call_method("rpush", args, None)?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn lpushx(&self, py: Python<'_>, key: String, values: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &key, &values)?;
        Ok(self.drv(py)?.call_method("lpushx", args, None)?.unbind())
    }

    #[pyo3(signature = (key, *values))]
    fn rpushx(&self, py: Python<'_>, key: String, values: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &key, &values)?;
        Ok(self.drv(py)?.call_method("rpushx", args, None)?.unbind())
    }

    #[pyo3(signature = (key, count = None))]
    fn lpop(&self, py: Python<'_>, key: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        // Driver: lpop(name, count=None)
        let kw = PyDict::new(py);
        if let Some(n) = count {
            kw.set_item("count", n)?;
        }
        Ok(self
            .drv(py)?
            .call_method("lpop", (key,), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (key, count = None))]
    fn rpop(&self, py: Python<'_>, key: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        // Driver: rpop(name, count=None)
        let kw = PyDict::new(py);
        if let Some(n) = count {
            kw.set_item("count", n)?;
        }
        Ok(self
            .drv(py)?
            .call_method("rpop", (key,), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (src, dst, wherefrom = "LEFT".to_string(), whereto = "RIGHT".to_string()))]
    fn lmove(
        &self,
        py: Python<'_>,
        src: String,
        dst: String,
        wherefrom: String,
        whereto: String,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("lmove", (src, dst, wherefrom, whereto))?
            .unbind())
    }

    #[pyo3(signature = (key, value, rank = None, count = None, maxlen = None))]
    fn lpos(
        &self,
        py: Python<'_>,
        key: String,
        value: Py<PyAny>,
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        let kw = PyDict::new(py);
        if let Some(v) = rank {
            kw.set_item("rank", v)?;
        }
        if let Some(v) = count {
            kw.set_item("count", v)?;
        }
        if let Some(v) = maxlen {
            kw.set_item("maxlen", v)?;
        }
        Ok(self
            .drv(py)?
            .call_method("lpos", (key, value), Some(&kw))?
            .unbind())
    }

    fn lrange(&self, py: Python<'_>, key: String, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("lrange", (key, start, end))?
            .unbind())
    }

    fn llen(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("llen", (key,))?.unbind())
    }

    fn lrem(
        &self,
        py: Python<'_>,
        key: String,
        count: i64,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("lrem", (key, count, value))?
            .unbind())
    }

    fn lindex(&self, py: Python<'_>, key: String, index: i64) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("lindex", (key, index))?.unbind())
    }

    fn lset(
        &self,
        py: Python<'_>,
        key: String,
        index: i64,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("lset", (key, index, value))?
            .unbind())
    }

    fn linsert(
        &self,
        py: Python<'_>,
        key: String,
        where_: String,
        pivot: Py<PyAny>,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let pivot = to_bytes_obj(py, pivot.bind(py))?;
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("linsert", (key, where_, pivot, value))?
            .unbind())
    }

    fn ltrim(&self, py: Python<'_>, key: String, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("ltrim", (key, start, end))?
            .unbind())
    }

    #[pyo3(signature = (keys, timeout = 0.0))]
    fn blpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("blpop", (keys, timeout))?
            .unbind())
    }

    #[pyo3(signature = (keys, timeout = 0.0))]
    fn brpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("brpop", (keys, timeout))?
            .unbind())
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
        Ok(self
            .drv(py)?
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
        let _ = numkeys;
        // Driver signature: blmpop(*, timeout, keys, direction, count=1)
        let kw = PyDict::new(py);
        kw.set_item("timeout", timeout)?;
        kw.set_item("keys", keys)?;
        kw.set_item("direction", &direction)?;
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        Ok(self.drv(py)?.call_method("blmpop", (), Some(&kw))?.unbind())
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
        let _ = numkeys;
        // Driver signature: lmpop(keys, *, direction, count=1)
        let kw = PyDict::new(py);
        kw.set_item("direction", &direction)?;
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        Ok(self
            .drv(py)?
            .call_method("lmpop", (keys,), Some(&kw))?
            .unbind())
    }

    // =========================================================================
    // Hash commands — plan 05 surface.
    // =========================================================================

    // Driver: hset(key, *items, mapping=None)
    // redis-py: hset(name, key=None, value=None, mapping=None, items=None)
    // We accept the redis-py surface and map it to driver positional args.
    #[pyo3(signature = (name, key = None, value = None, mapping = None, items = None))]
    fn hset<'py>(
        &self,
        py: Python<'py>,
        name: String,
        key: Option<Py<PyAny>>,
        value: Option<Py<PyAny>>,
        mapping: Option<Bound<'py, PyDict>>,
        items: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<Py<PyAny>> {
        let drv = self.drv(py)?;
        // Build positional args: name, [field, value, ...from items...]
        let mut elems: Vec<Bound<'py, PyAny>> = Vec::new();
        elems.push(name.into_pyobject(py)?.into_any());
        // Single field/value pair
        if let Some(k) = key {
            elems.push(k.bind(py).clone());
            if let Some(v) = value {
                elems.push(to_bytes_obj(py, v.bind(py))?);
            }
        }
        // Flat items list [field1, val1, field2, val2, ...]
        if let Some(i) = items {
            for item in i {
                elems.push(item.bind(py).clone());
            }
        }
        let args = PyTuple::new(py, elems)?;
        // Pass mapping as keyword arg if provided
        if let Some(m) = mapping {
            let normalized = normalize_mapping_values(py, &m)?;
            let kw = PyDict::new(py);
            kw.set_item("mapping", normalized)?;
            Ok(drv.call_method("hset", args, Some(&kw))?.unbind())
        } else {
            Ok(drv.call_method("hset", args, None)?.unbind())
        }
    }

    fn hsetnx(
        &self,
        py: Python<'_>,
        name: String,
        key: String,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("hsetnx", (name, key, value))?
            .unbind())
    }

    fn hmset(
        &self,
        py: Python<'_>,
        name: String,
        mapping: Bound<'_, PyDict>,
    ) -> PyResult<Py<PyAny>> {
        let normalized = normalize_mapping_values(py, &mapping)?;
        Ok(self
            .drv(py)?
            .call_method1("hmset", (name, normalized))?
            .unbind())
    }

    fn hget(&self, py: Python<'_>, name: String, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("hget", (name, key))?.unbind())
    }

    fn hgetall(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("hgetall", (name,))?.unbind())
    }

    #[pyo3(signature = (name, *keys))]
    fn hdel(&self, py: Python<'_>, name: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        // Driver: hdel(key, *fields) — varargs
        let mut all = vec![name];
        all.extend(keys);
        let pos_args = PyTuple::new(py, &all)?;
        Ok(self.drv(py)?.call_method("hdel", pos_args, None)?.unbind())
    }

    #[pyo3(signature = (name, key, amount = 1))]
    fn hincrby(
        &self,
        py: Python<'_>,
        name: String,
        key: String,
        amount: i64,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("hincrby", (name, key, amount))?
            .unbind())
    }

    #[pyo3(signature = (name, key, amount = 1.0))]
    fn hincrbyfloat(
        &self,
        py: Python<'_>,
        name: String,
        key: String,
        amount: f64,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("hincrbyfloat", (name, key, amount))?
            .unbind())
    }

    fn hkeys(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("hkeys", (name,))?.unbind())
    }

    fn hvals(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("hvals", (name,))?.unbind())
    }

    fn hexists(&self, py: Python<'_>, name: String, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("hexists", (name, key))?.unbind())
    }

    fn hlen(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("hlen", (name,))?.unbind())
    }

    // redis-py: hmget(name, keys, *args) — keys can be a list or *varargs
    #[pyo3(signature = (name, keys, *args))]
    fn hmget(
        &self,
        py: Python<'_>,
        name: String,
        keys: Py<PyAny>,
        args: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: hmget(key, *fields) — varargs
        let all_fields = flatten_keys_arg(py, &keys, &args)?;
        let mut all = vec![name];
        all.extend(all_fields);
        let pos_args = PyTuple::new(py, &all)?;
        Ok(self.drv(py)?.call_method("hmget", pos_args, None)?.unbind())
    }

    #[pyo3(signature = (name, cursor = 0, match_ = None, count = None, no_values = false))]
    fn hscan(
        &self,
        py: Python<'_>,
        name: String,
        cursor: u64,
        match_: Option<String>,
        count: Option<i64>,
        no_values: bool,
    ) -> PyResult<Py<PyAny>> {
        // Driver: hscan(key, *, cursor=0, match=None, count=None, novalues=false)
        let kw = PyDict::new(py);
        kw.set_item("cursor", cursor)?;
        if let Some(m) = match_ {
            kw.set_item("match", m)?;
        }
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        if no_values {
            kw.set_item("novalues", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("hscan", (name,), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (key, count = None, withvalues = false))]
    fn hrandfield(
        &self,
        py: Python<'_>,
        key: String,
        count: Option<i64>,
        withvalues: bool,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("hrandfield", (key, count, withvalues))?
            .unbind())
    }

    #[pyo3(signature = (name, seconds, fields, nx = false, xx = false, gt = false, lt = false))]
    fn hexpire(
        &self,
        py: Python<'_>,
        name: String,
        seconds: i64,
        fields: Vec<String>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        // Driver: hexpire(key, fields, time, *, nx, xx, gt, lt)
        let kw = PyDict::new(py);
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("hexpire", (name, fields, seconds), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, milliseconds, fields, nx = false, xx = false, gt = false, lt = false))]
    fn hpexpire(
        &self,
        py: Python<'_>,
        name: String,
        milliseconds: i64,
        fields: Vec<String>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("hpexpire", (name, fields, milliseconds), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, unix_time_seconds, fields, nx = false, xx = false, gt = false, lt = false))]
    fn hexpireat(
        &self,
        py: Python<'_>,
        name: String,
        unix_time_seconds: i64,
        fields: Vec<String>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("hexpireat", (name, fields, unix_time_seconds), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, unix_time_milliseconds, fields, nx = false, xx = false, gt = false, lt = false))]
    fn hpexpireat(
        &self,
        py: Python<'_>,
        name: String,
        unix_time_milliseconds: i64,
        fields: Vec<String>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method(
                "hpexpireat",
                (name, fields, unix_time_milliseconds),
                Some(&kw),
            )?
            .unbind())
    }

    fn hexpiretime(
        &self,
        py: Python<'_>,
        name: String,
        fields: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("hexpiretime", (name, fields))?
            .unbind())
    }

    fn hpexpiretime(
        &self,
        py: Python<'_>,
        name: String,
        fields: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("hpexpiretime", (name, fields))?
            .unbind())
    }

    fn httl(&self, py: Python<'_>, name: String, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("httl", (name, fields))?.unbind())
    }

    fn hpttl(&self, py: Python<'_>, name: String, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("hpttl", (name, fields))?
            .unbind())
    }

    fn hpersist(&self, py: Python<'_>, name: String, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("hpersist", (name, fields))?
            .unbind())
    }

    // =========================================================================
    // Set commands — plan 06 surface.
    // =========================================================================

    #[pyo3(signature = (name, *values))]
    fn sadd(&self, py: Python<'_>, name: String, values: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &name, &values)?;
        Ok(self.drv(py)?.call_method("sadd", args, None)?.unbind())
    }

    #[pyo3(signature = (name, *values))]
    fn srem(&self, py: Python<'_>, name: String, values: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &name, &values)?;
        Ok(self.drv(py)?.call_method("srem", args, None)?.unbind())
    }

    fn smembers(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("smembers", (name,))?.unbind())
    }

    fn sismember(&self, py: Python<'_>, name: String, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("sismember", (name, value))?
            .unbind())
    }

    #[pyo3(signature = (name, *values))]
    fn smismember(
        &self,
        py: Python<'_>,
        name: String,
        values: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &name, &values)?;
        Ok(self
            .drv(py)?
            .call_method("smismember", args, None)?
            .unbind())
    }

    fn scard(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("scard", (name,))?.unbind())
    }

    // redis-py sunion/sinter/sdiff accept either a list or *varargs
    #[pyo3(signature = (keys, *args))]
    fn sinter(&self, py: Python<'_>, keys: Py<PyAny>, args: Vec<String>) -> PyResult<Py<PyAny>> {
        let all_keys = flatten_keys_arg(py, &keys, &args)?;
        let pos_args = PyTuple::new(py, &all_keys)?;
        Ok(self
            .drv(py)?
            .call_method("sinter", pos_args, None)?
            .unbind())
    }

    #[pyo3(signature = (keys, *args))]
    fn sunion(&self, py: Python<'_>, keys: Py<PyAny>, args: Vec<String>) -> PyResult<Py<PyAny>> {
        let all_keys = flatten_keys_arg(py, &keys, &args)?;
        let pos_args = PyTuple::new(py, &all_keys)?;
        Ok(self
            .drv(py)?
            .call_method("sunion", pos_args, None)?
            .unbind())
    }

    #[pyo3(signature = (keys, *args))]
    fn sdiff(&self, py: Python<'_>, keys: Py<PyAny>, args: Vec<String>) -> PyResult<Py<PyAny>> {
        let all_keys = flatten_keys_arg(py, &keys, &args)?;
        let pos_args = PyTuple::new(py, &all_keys)?;
        Ok(self.drv(py)?.call_method("sdiff", pos_args, None)?.unbind())
    }

    #[pyo3(signature = (dest, keys, *args))]
    fn sinterstore(
        &self,
        py: Python<'_>,
        dest: String,
        keys: Py<PyAny>,
        args: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let mut all_keys = vec![dest];
        all_keys.extend(flatten_keys_arg(py, &keys, &args)?);
        let pos_args = PyTuple::new(py, &all_keys)?;
        Ok(self
            .drv(py)?
            .call_method("sinterstore", pos_args, None)?
            .unbind())
    }

    #[pyo3(signature = (dest, keys, *args))]
    fn sunionstore(
        &self,
        py: Python<'_>,
        dest: String,
        keys: Py<PyAny>,
        args: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let mut all_keys = vec![dest];
        all_keys.extend(flatten_keys_arg(py, &keys, &args)?);
        let pos_args = PyTuple::new(py, &all_keys)?;
        Ok(self
            .drv(py)?
            .call_method("sunionstore", pos_args, None)?
            .unbind())
    }

    #[pyo3(signature = (dest, keys, *args))]
    fn sdiffstore(
        &self,
        py: Python<'_>,
        dest: String,
        keys: Py<PyAny>,
        args: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let mut all_keys = vec![dest];
        all_keys.extend(flatten_keys_arg(py, &keys, &args)?);
        let pos_args = PyTuple::new(py, &all_keys)?;
        Ok(self
            .drv(py)?
            .call_method("sdiffstore", pos_args, None)?
            .unbind())
    }

    #[pyo3(signature = (numkeys, keys, limit = None))]
    fn sintercard(
        &self,
        py: Python<'_>,
        numkeys: i64,
        keys: Vec<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = numkeys;
        // Driver: sintercard(*keys, limit=None) — keys spread, limit keyword-only
        let kw = PyDict::new(py);
        if let Some(l) = limit {
            kw.set_item("limit", l)?;
        }
        let pos_args = PyTuple::new(py, &keys)?;
        Ok(self
            .drv(py)?
            .call_method("sintercard", pos_args, Some(&kw))?
            .unbind())
    }

    fn smove(
        &self,
        py: Python<'_>,
        src: String,
        dst: String,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("smove", (src, dst, value))?
            .unbind())
    }

    #[pyo3(signature = (name, count = None))]
    fn spop(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let bound = self.drv(py)?;
        Ok(match count {
            None => bound.call_method1("spop", (name,))?,
            Some(n) => bound.call_method1("spop_count", (name, n))?,
        }
        .unbind())
    }

    #[pyo3(signature = (name, count = None))]
    fn srandmember(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let bound = self.drv(py)?;
        Ok(match count {
            None => bound.call_method1("srandmember", (name,))?,
            Some(n) => bound.call_method1("srandmember_count", (name, n))?,
        }
        .unbind())
    }

    #[pyo3(signature = (name, cursor = 0, match_ = None, count = None))]
    fn sscan(
        &self,
        py: Python<'_>,
        name: String,
        cursor: u64,
        match_: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: sscan(key, *, cursor=0, match=None, count=None) → returns (cursor, set)
        // redis-py returns (cursor, list)
        let kw = PyDict::new(py);
        kw.set_item("cursor", cursor)?;
        if let Some(m) = match_ {
            kw.set_item("match", m)?;
        }
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        let raw = self.drv(py)?.call_method("sscan", (name,), Some(&kw))?;
        // Convert (cursor, set) → (cursor, list)
        if let Ok((cur, members)) = raw.extract::<(Py<PyAny>, Py<PyAny>)>() {
            // Convert set/iterable to list using Python's list()
            let builtins = py.import("builtins")?;
            let py_list = builtins.call_method1("list", (members,))?;
            return Ok(PyTuple::new(py, [cur.bind(py).clone(), py_list])?
                .into_any()
                .unbind());
        }
        Ok(raw.unbind())
    }

    // =========================================================================
    // Sorted-set commands — plan 07 surface.
    // =========================================================================

    #[pyo3(signature = (name, mapping, nx = false, xx = false, gt = false, lt = false, ch = false, incr = false))]
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
        // Driver: zadd(key, *, mapping, nx, xx, gt, lt, ch, incr) — all keyword-only
        let drv = self.drv(py)?;
        let kw = PyDict::new(py);
        kw.set_item("mapping", &mapping)?;
        if nx {
            kw.set_item("nx", true)?;
        }
        if xx {
            kw.set_item("xx", true)?;
        }
        if gt {
            kw.set_item("gt", true)?;
        }
        if lt {
            kw.set_item("lt", true)?;
        }
        if ch {
            kw.set_item("ch", true)?;
        }
        if incr {
            kw.set_item("incr", true)?;
        }
        Ok(drv.call_method("zadd", (name,), Some(&kw))?.unbind())
    }

    #[pyo3(signature = (name, *values))]
    fn zrem(&self, py: Python<'_>, name: String, values: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &name, &values)?;
        Ok(self.drv(py)?.call_method("zrem", args, None)?.unbind())
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
        byscore: bool,
        bylex: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = score_cast_func;
        let drv = self.drv(py)?;
        let kw = PyDict::new(py);
        if desc {
            kw.set_item("rev", true)?;
        }
        if withscores {
            kw.set_item("withscores", true)?;
        }
        if byscore {
            kw.set_item("byscore", true)?;
        }
        if bylex {
            kw.set_item("bylex", true)?;
        }
        if let Some(o) = offset {
            kw.set_item("offset", o)?;
        }
        if let Some(n) = num {
            kw.set_item("count", n)?;
        }
        Ok(drv
            .call_method("zrange", (name, start, end), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, min_, max_, start = None, num = None, withscores = false))]
    fn zrangebyscore(
        &self,
        py: Python<'_>,
        name: String,
        min_: Py<PyAny>,
        max_: Py<PyAny>,
        start: Option<i64>,
        num: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        // Driver: zrangebyscore(key, min, max, *, withscores=false, offset=None, num=None)
        let min_ = to_str_obj(py, min_.bind(py))?;
        let max_ = to_str_obj(py, max_.bind(py))?;
        let kw = PyDict::new(py);
        if withscores {
            kw.set_item("withscores", true)?;
        }
        if let Some(s) = start {
            kw.set_item("offset", s)?;
        }
        if let Some(n) = num {
            kw.set_item("num", n)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zrangebyscore", (name, min_, max_), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, min_, max_, start = None, num = None))]
    fn zrangebylex(
        &self,
        py: Python<'_>,
        name: String,
        min_: Py<PyAny>,
        max_: Py<PyAny>,
        start: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: zrangebylex(key, min, max, *, offset=None, num=None)
        let min_ = to_bytes_obj(py, min_.bind(py))?;
        let max_ = to_bytes_obj(py, max_.bind(py))?;
        let kw = PyDict::new(py);
        if let Some(s) = start {
            kw.set_item("offset", s)?;
        }
        if let Some(n) = num {
            kw.set_item("num", n)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zrangebylex", (name, min_, max_), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, max_, min_, start = None, num = None, withscores = false))]
    fn zrevrangebyscore(
        &self,
        py: Python<'_>,
        name: String,
        max_: Py<PyAny>,
        min_: Py<PyAny>,
        start: Option<i64>,
        num: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        // Driver: zrevrangebyscore(key, max, min, *, withscores=false, offset=None, num=None)
        let max_ = to_str_obj(py, max_.bind(py))?;
        let min_ = to_str_obj(py, min_.bind(py))?;
        let kw = PyDict::new(py);
        if withscores {
            kw.set_item("withscores", true)?;
        }
        if let Some(s) = start {
            kw.set_item("offset", s)?;
        }
        if let Some(n) = num {
            kw.set_item("num", n)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zrevrangebyscore", (name, max_, min_), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, max_, min_, start = None, num = None))]
    fn zrevrangebylex(
        &self,
        py: Python<'_>,
        name: String,
        max_: Py<PyAny>,
        min_: Py<PyAny>,
        start: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: zrevrangebylex(key, max, min, *, offset=None, num=None)
        let max_ = to_bytes_obj(py, max_.bind(py))?;
        let min_ = to_bytes_obj(py, min_.bind(py))?;
        let kw = PyDict::new(py);
        if let Some(s) = start {
            kw.set_item("offset", s)?;
        }
        if let Some(n) = num {
            kw.set_item("num", n)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zrevrangebylex", (name, max_, min_), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (dest, src, start, end, byscore = false, bylex = false, desc = false, offset = None, num = None))]
    fn zrangestore(
        &self,
        py: Python<'_>,
        dest: String,
        src: String,
        start: Py<PyAny>,
        end: Py<PyAny>,
        byscore: bool,
        bylex: bool,
        desc: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: zrangestore(dest, src, start, stop, *, desc, byscore, bylex, offset, num)
        let kw = PyDict::new(py);
        if desc {
            kw.set_item("desc", true)?;
        }
        if byscore {
            kw.set_item("byscore", true)?;
        }
        if bylex {
            kw.set_item("bylex", true)?;
        }
        if let Some(o) = offset {
            kw.set_item("offset", o)?;
        }
        if let Some(n) = num {
            kw.set_item("num", n)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zrangestore", (dest, src, start, end), Some(&kw))?
            .unbind())
    }

    fn zincrby(
        &self,
        py: Python<'_>,
        name: String,
        amount: f64,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("zincrby", (name, amount, value))?
            .unbind())
    }

    fn zcard(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("zcard", (name,))?.unbind())
    }

    fn zscore(&self, py: Python<'_>, name: String, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("zscore", (name, value))?
            .unbind())
    }

    #[pyo3(signature = (name, *values))]
    fn zmscore(&self, py: Python<'_>, name: String, values: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let args = build_varargs_tuple(py, &name, &values)?;
        Ok(self.drv(py)?.call_method("zmscore", args, None)?.unbind())
    }

    #[pyo3(signature = (name, value, withscore = false))]
    fn zrank(
        &self,
        py: Python<'_>,
        name: String,
        value: Py<PyAny>,
        withscore: bool,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        // Driver: zrank(key, member, *, withscore=false)
        let kw = PyDict::new(py);
        if withscore {
            kw.set_item("withscore", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zrank", (name, value), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, value, withscore = false))]
    fn zrevrank(
        &self,
        py: Python<'_>,
        name: String,
        value: Py<PyAny>,
        withscore: bool,
    ) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        // Driver: zrevrank(key, member, *, withscore=false)
        let kw = PyDict::new(py);
        if withscore {
            kw.set_item("withscore", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zrevrank", (name, value), Some(&kw))?
            .unbind())
    }

    fn zremrangebyrank(
        &self,
        py: Python<'_>,
        name: String,
        min_: i64,
        max_: i64,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("zremrangebyrank", (name, min_, max_))?
            .unbind())
    }

    fn zremrangebyscore(
        &self,
        py: Python<'_>,
        name: String,
        min_: Py<PyAny>,
        max_: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("zremrangebyscore", (name, min_, max_))?
            .unbind())
    }

    fn zremrangebylex(
        &self,
        py: Python<'_>,
        name: String,
        min_: Py<PyAny>,
        max_: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let min_ = to_bytes_obj(py, min_.bind(py))?;
        let max_ = to_bytes_obj(py, max_.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("zremrangebylex", (name, min_, max_))?
            .unbind())
    }

    fn zcount(
        &self,
        py: Python<'_>,
        name: String,
        min_: Py<PyAny>,
        max_: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let min_ = to_str_obj(py, min_.bind(py))?;
        let max_ = to_str_obj(py, max_.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("zcount", (name, min_, max_))?
            .unbind())
    }

    fn zlexcount(
        &self,
        py: Python<'_>,
        name: String,
        min_: Py<PyAny>,
        max_: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let min_ = to_bytes_obj(py, min_.bind(py))?;
        let max_ = to_bytes_obj(py, max_.bind(py))?;
        Ok(self
            .drv(py)?
            .call_method1("zlexcount", (name, min_, max_))?
            .unbind())
    }

    #[pyo3(signature = (name, count = None))]
    fn zpopmin(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        // Driver: zpopmin(key, *, count=1) — count keyword-only
        let kw = PyDict::new(py);
        kw.set_item("count", count.unwrap_or(1))?;
        Ok(self
            .drv(py)?
            .call_method("zpopmin", (name,), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, count = None))]
    fn zpopmax(&self, py: Python<'_>, name: String, count: Option<i64>) -> PyResult<Py<PyAny>> {
        // Driver: zpopmax(key, *, count=1) — count keyword-only
        let kw = PyDict::new(py);
        kw.set_item("count", count.unwrap_or(1))?;
        Ok(self
            .drv(py)?
            .call_method("zpopmax", (name,), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (keys, timeout = 0.0))]
    fn bzpopmin(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        // Driver: bzpopmin(*keys, timeout) — timeout keyword-only, keys spread
        let kw = PyDict::new(py);
        kw.set_item("timeout", timeout)?;
        let pos_args = PyTuple::new(py, &keys)?;
        Ok(self
            .drv(py)?
            .call_method("bzpopmin", pos_args, Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (keys, timeout = 0.0))]
    fn bzpopmax(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        // Driver: bzpopmax(*keys, timeout) — timeout keyword-only, keys spread
        let kw = PyDict::new(py);
        kw.set_item("timeout", timeout)?;
        let pos_args = PyTuple::new(py, &keys)?;
        Ok(self
            .drv(py)?
            .call_method("bzpopmax", pos_args, Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (numkeys, keys, min_or_max, count = None))]
    fn zmpop(
        &self,
        py: Python<'_>,
        numkeys: i64,
        keys: Vec<String>,
        min_or_max: String,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let _ = numkeys;
        // Driver: zmpop(*keys, direction, count=1) — keyword-only after varargs keys
        let kw = PyDict::new(py);
        kw.set_item("direction", &min_or_max)?;
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        let pos_args = PyTuple::new(py, &keys)?;
        Ok(self
            .drv(py)?
            .call_method("zmpop", pos_args, Some(&kw))?
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
        let _ = numkeys;
        // Driver: bzmpop(*keys, direction, timeout, count=1) — keyword-only after varargs keys
        let kw = PyDict::new(py);
        kw.set_item("direction", &min_or_max)?;
        kw.set_item("timeout", timeout)?;
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        let pos_args = PyTuple::new(py, &keys)?;
        Ok(self
            .drv(py)?
            .call_method("bzmpop", pos_args, Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, count = None, withscores = false))]
    fn zrandmember(
        &self,
        py: Python<'_>,
        name: String,
        count: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("zrandmember", (name, count, withscores))?
            .unbind())
    }

    #[pyo3(signature = (name, cursor = 0, match_ = None, count = None, score_cast_func = None))]
    fn zscan(
        &self,
        py: Python<'_>,
        name: String,
        cursor: u64,
        match_: Option<String>,
        count: Option<i64>,
        score_cast_func: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = score_cast_func;
        // Driver: zscan(key, *, cursor=0, match=None, count=None)
        let kw = PyDict::new(py);
        kw.set_item("cursor", cursor)?;
        if let Some(m) = match_ {
            kw.set_item("match", m)?;
        }
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zscan", (name,), Some(&kw))?
            .unbind())
    }

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
        // Driver: zunion(*, keys, aggregate=None, withscores=false) — all keyword-only
        let kw = PyDict::new(py);
        kw.set_item("keys", &keys)?;
        if let Some(a) = aggregate {
            kw.set_item("aggregate", a)?;
        }
        if withscores {
            kw.set_item("withscores", true)?;
        }
        Ok(self.drv(py)?.call_method("zunion", (), Some(&kw))?.unbind())
    }

    #[pyo3(signature = (keys, aggregate = None, withscores = false))]
    fn zinter(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        aggregate: Option<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        // Driver: zinter(*, keys, aggregate=None, withscores=false) — all keyword-only
        let kw = PyDict::new(py);
        kw.set_item("keys", &keys)?;
        if let Some(a) = aggregate {
            kw.set_item("aggregate", a)?;
        }
        if withscores {
            kw.set_item("withscores", true)?;
        }
        Ok(self.drv(py)?.call_method("zinter", (), Some(&kw))?.unbind())
    }

    #[pyo3(signature = (keys, withscores = false))]
    fn zdiff(&self, py: Python<'_>, keys: Vec<String>, withscores: bool) -> PyResult<Py<PyAny>> {
        // Driver: zdiff(*, keys, withscores=false) — all keyword-only
        let kw = PyDict::new(py);
        kw.set_item("keys", &keys)?;
        if withscores {
            kw.set_item("withscores", true)?;
        }
        Ok(self.drv(py)?.call_method("zdiff", (), Some(&kw))?.unbind())
    }

    #[pyo3(signature = (dest, keys, aggregate = None))]
    fn zunionstore(
        &self,
        py: Python<'_>,
        dest: String,
        keys: Vec<String>,
        aggregate: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: zunionstore(destination, *, keys, aggregate=None) — keys keyword-only
        let kw = PyDict::new(py);
        kw.set_item("keys", &keys)?;
        if let Some(a) = aggregate {
            kw.set_item("aggregate", a)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zunionstore", (dest,), Some(&kw))?
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
        // Driver: zinterstore(destination, *, keys, aggregate=None) — keys keyword-only
        let kw = PyDict::new(py);
        kw.set_item("keys", &keys)?;
        if let Some(a) = aggregate {
            kw.set_item("aggregate", a)?;
        }
        Ok(self
            .drv(py)?
            .call_method("zinterstore", (dest,), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (dest, keys))]
    fn zdiffstore(&self, py: Python<'_>, dest: String, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        // Driver: zdiffstore(destination, *, keys) — keys keyword-only
        let kw = PyDict::new(py);
        kw.set_item("keys", &keys)?;
        Ok(self
            .drv(py)?
            .call_method("zdiffstore", (dest,), Some(&kw))?
            .unbind())
    }

    // =========================================================================
    // Stream commands — plan 08 surface.
    // =========================================================================

    #[pyo3(signature = (name, fields, id = "*".to_string(), maxlen = None, approximate = true, nomkstream = false, minid = None, limit = None))]
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
        // Driver: xadd(key, id, fields, *, nomkstream, maxlen, minid, approximate, limit)
        // Convert field dict values to bytes
        let normalized = normalize_mapping_values(py, &fields)?;
        // Build fields as list of (str, bytes) pairs
        let pairs: Vec<(String, Vec<u8>)> = normalized
            .iter()
            .map(|(k, v)| {
                let key_s: String = k.extract().unwrap_or_default();
                let val_b: Vec<u8> = v.extract().unwrap_or_default();
                (key_s, val_b)
            })
            .collect();
        let drv = self.drv(py)?;
        let kw = PyDict::new(py);
        kw.set_item("approximate", approximate)?;
        if nomkstream {
            kw.set_item("nomkstream", true)?;
        }
        if let Some(m) = maxlen {
            kw.set_item("maxlen", m)?;
        }
        if let Some(m) = minid {
            kw.set_item("minid", m)?;
        }
        if let Some(l) = limit {
            kw.set_item("limit", l)?;
        }
        Ok(drv
            .call_method("xadd", (name, id, pairs), Some(&kw))?
            .unbind())
    }

    fn xlen(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("xlen", (name,))?.unbind())
    }

    #[pyo3(signature = (name, min_ = "-".to_string(), max_ = "+".to_string(), count = None))]
    fn xrange(
        &self,
        py: Python<'_>,
        name: String,
        min_: String,
        max_: String,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: xrange(key, min, max, *, count=None)
        let kw = PyDict::new(py);
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xrange", (name, min_, max_), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, max_ = "+".to_string(), min_ = "-".to_string(), count = None))]
    fn xrevrange(
        &self,
        py: Python<'_>,
        name: String,
        max_: String,
        min_: String,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: xrevrange(key, max, min, *, count=None)
        let kw = PyDict::new(py);
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xrevrange", (name, max_, min_), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (streams, count = None, block = None))]
    fn xread(
        &self,
        py: Python<'_>,
        streams: Bound<'_, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: xread(streams, *, count=None, block=None)
        let kw = PyDict::new(py);
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        if let Some(b) = block {
            kw.set_item("block", b)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xread", (streams,), Some(&kw))?
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
        // Driver: xreadgroup(group, consumer, streams, *, count=None, block=None, noack=false)
        let kw = PyDict::new(py);
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        if let Some(b) = block {
            kw.set_item("block", b)?;
        }
        if noack {
            kw.set_item("noack", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xreadgroup", (groupname, consumername, streams), Some(&kw))?
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
        Ok(self
            .drv(py)?
            .call_method1("xack", (name, groupname, ids))?
            .unbind())
    }

    #[pyo3(signature = (name, *ids))]
    fn xdel(&self, py: Python<'_>, name: String, ids: Vec<String>) -> PyResult<Py<PyAny>> {
        // Driver: xdel(key, *ids) — varargs
        let mut all = vec![name];
        all.extend(ids);
        let pos_args = PyTuple::new(py, &all)?;
        Ok(self.drv(py)?.call_method("xdel", pos_args, None)?.unbind())
    }

    #[pyo3(signature = (name, groupname, id = "$".to_string(), mkstream = false, entries_read = None))]
    fn xgroup_create(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
        id: String,
        mkstream: bool,
        entries_read: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: xgroup_create(key, group, *, id="0", mkstream=false, entries_read=None)
        let kw = PyDict::new(py);
        kw.set_item("id", &id)?;
        if mkstream {
            kw.set_item("mkstream", true)?;
        }
        if let Some(e) = entries_read {
            kw.set_item("entries_read", e)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xgroup_create", (name, groupname), Some(&kw))?
            .unbind())
    }

    fn xgroup_setid(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
        id: String,
        entries_read: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: xgroup_setid(key, group, *, id, entries_read=None)
        let kw = PyDict::new(py);
        kw.set_item("id", &id)?;
        if let Some(e) = entries_read {
            kw.set_item("entries_read", e)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xgroup_setid", (name, groupname), Some(&kw))?
            .unbind())
    }

    fn xgroup_destroy(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("xgroup_destroy", (name, groupname))?
            .unbind())
    }

    fn xgroup_delconsumer(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
        consumername: String,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("xgroup_delconsumer", (name, groupname, consumername))?
            .unbind())
    }

    fn xgroup_createconsumer(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
        consumername: String,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("xgroup_createconsumer", (name, groupname, consumername))?
            .unbind())
    }

    #[pyo3(signature = (name, full = false))]
    fn xinfo_stream(&self, py: Python<'_>, name: String, full: bool) -> PyResult<Py<PyAny>> {
        // Driver: xinfo_stream(key, *, full=false)
        let kw = PyDict::new(py);
        if full {
            kw.set_item("full", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xinfo_stream", (name,), Some(&kw))?
            .unbind())
    }

    fn xinfo_groups(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("xinfo_groups", (name,))?
            .unbind())
    }

    fn xinfo_consumers(
        &self,
        py: Python<'_>,
        name: String,
        groupname: String,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("xinfo_consumers", (name, groupname))?
            .unbind())
    }

    #[pyo3(signature = (name, maxlen = None, approximate = true, minid = None, limit = None))]
    fn xtrim(
        &self,
        py: Python<'_>,
        name: String,
        maxlen: Option<i64>,
        approximate: bool,
        minid: Option<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: xtrim(key, *, maxlen=None, minid=None, approximate=true, limit=None)
        let kw = PyDict::new(py);
        if let Some(m) = maxlen {
            kw.set_item("maxlen", m)?;
        }
        if let Some(m) = minid {
            kw.set_item("minid", m)?;
        }
        kw.set_item("approximate", approximate)?;
        if let Some(l) = limit {
            kw.set_item("limit", l)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xtrim", (name,), Some(&kw))?
            .unbind())
    }

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
        // Driver: xpending(key, group, *, idle=None, min=None, max=None, count=None, consumer=None)
        let kw = PyDict::new(py);
        if let Some(i) = idle {
            kw.set_item("idle", i)?;
        }
        if let Some(m) = min_id {
            kw.set_item("min", m)?;
        }
        if let Some(m) = max_id {
            kw.set_item("max", m)?;
        }
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        if let Some(c) = consumername {
            kw.set_item("consumer", c)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xpending", (name, groupname), Some(&kw))?
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
        // Driver: xclaim(key, group, consumer, *, min_idle_time, message_ids, idle=None, time=None, retrycount=None, force=false, justid=false)
        let kw = PyDict::new(py);
        kw.set_item("min_idle_time", min_idle_time)?;
        kw.set_item("message_ids", &ids)?;
        if let Some(i) = idle {
            kw.set_item("idle", i)?;
        }
        if let Some(t) = time {
            kw.set_item("time", t)?;
        }
        if let Some(r) = retrycount {
            kw.set_item("retrycount", r)?;
        }
        if force {
            kw.set_item("force", true)?;
        }
        if justid {
            kw.set_item("justid", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xclaim", (name, groupname, consumername), Some(&kw))?
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
        // Driver: xautoclaim(key, group, consumer, *, min_idle_time, start_id="0-0", count=100, justid=false)
        let kw = PyDict::new(py);
        kw.set_item("min_idle_time", min_idle_time)?;
        kw.set_item("start_id", &start)?;
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        if justid {
            kw.set_item("justid", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xautoclaim", (name, groupname, consumername), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (name, id, entries_added = None, max_deleted_id = None))]
    fn xsetid(
        &self,
        py: Python<'_>,
        name: String,
        id: String,
        entries_added: Option<i64>,
        max_deleted_id: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: xsetid(key, id, *, entries_added=None, max_deleted_entry_id=None)
        let kw = PyDict::new(py);
        if let Some(e) = entries_added {
            kw.set_item("entries_added", e)?;
        }
        if let Some(m) = max_deleted_id {
            kw.set_item("max_deleted_entry_id", m)?;
        }
        Ok(self
            .drv(py)?
            .call_method("xsetid", (name, id), Some(&kw))?
            .unbind())
    }

    // =========================================================================
    // Scripts + admin commands — plan 09 surface.
    // =========================================================================

    #[pyo3(signature = (script, numkeys, *keys_and_args))]
    fn eval(
        &self,
        py: Python<'_>,
        script: String,
        numkeys: i64,
        keys_and_args: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: eval(script, keys: Vec<String>, args: Vec<Vec<u8>>)
        let n = numkeys.max(0) as usize;
        let keys: Vec<String> = keys_and_args[..n.min(keys_and_args.len())]
            .iter()
            .map(|o| o.bind(py).str().map(|s| s.to_string()).unwrap_or_default())
            .collect();
        let args: Vec<Vec<u8>> = keys_and_args[n.min(keys_and_args.len())..]
            .iter()
            .map(|o| {
                let b = o.bind(py);
                if let Ok(bytes) = b.cast::<PyBytes>() {
                    bytes.as_bytes().to_vec()
                } else {
                    b.str()
                        .map(|s| s.to_string().into_bytes())
                        .unwrap_or_default()
                }
            })
            .collect();
        Ok(self
            .drv(py)?
            .call_method1("eval", (script, keys, args))?
            .unbind())
    }

    #[pyo3(signature = (script, numkeys, *keys_and_args))]
    fn eval_ro(
        &self,
        py: Python<'_>,
        script: String,
        numkeys: i64,
        keys_and_args: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let n = numkeys.max(0) as usize;
        let keys: Vec<String> = keys_and_args[..n.min(keys_and_args.len())]
            .iter()
            .map(|o| o.bind(py).str().map(|s| s.to_string()).unwrap_or_default())
            .collect();
        let args: Vec<Vec<u8>> = keys_and_args[n.min(keys_and_args.len())..]
            .iter()
            .map(|o| {
                let b = o.bind(py);
                if let Ok(bytes) = b.cast::<PyBytes>() {
                    bytes.as_bytes().to_vec()
                } else {
                    b.str()
                        .map(|s| s.to_string().into_bytes())
                        .unwrap_or_default()
                }
            })
            .collect();
        Ok(self
            .drv(py)?
            .call_method1("eval_ro", (script, keys, args))?
            .unbind())
    }

    #[pyo3(signature = (sha, numkeys, *keys_and_args))]
    fn evalsha(
        &self,
        py: Python<'_>,
        sha: String,
        numkeys: i64,
        keys_and_args: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let n = numkeys.max(0) as usize;
        let keys: Vec<String> = keys_and_args[..n.min(keys_and_args.len())]
            .iter()
            .map(|o| o.bind(py).str().map(|s| s.to_string()).unwrap_or_default())
            .collect();
        let args: Vec<Vec<u8>> = keys_and_args[n.min(keys_and_args.len())..]
            .iter()
            .map(|o| {
                let b = o.bind(py);
                if let Ok(bytes) = b.cast::<PyBytes>() {
                    bytes.as_bytes().to_vec()
                } else {
                    b.str()
                        .map(|s| s.to_string().into_bytes())
                        .unwrap_or_default()
                }
            })
            .collect();
        Ok(self
            .drv(py)?
            .call_method1("evalsha", (sha, keys, args))?
            .unbind())
    }

    #[pyo3(signature = (sha, numkeys, *keys_and_args))]
    fn evalsha_ro(
        &self,
        py: Python<'_>,
        sha: String,
        numkeys: i64,
        keys_and_args: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let n = numkeys.max(0) as usize;
        let keys: Vec<String> = keys_and_args[..n.min(keys_and_args.len())]
            .iter()
            .map(|o| o.bind(py).str().map(|s| s.to_string()).unwrap_or_default())
            .collect();
        let args: Vec<Vec<u8>> = keys_and_args[n.min(keys_and_args.len())..]
            .iter()
            .map(|o| {
                let b = o.bind(py);
                if let Ok(bytes) = b.cast::<PyBytes>() {
                    bytes.as_bytes().to_vec()
                } else {
                    b.str()
                        .map(|s| s.to_string().into_bytes())
                        .unwrap_or_default()
                }
            })
            .collect();
        Ok(self
            .drv(py)?
            .call_method1("evalsha_ro", (sha, keys, args))?
            .unbind())
    }

    fn script_load(&self, py: Python<'_>, script: String) -> PyResult<Py<PyAny>> {
        let raw = self.drv(py)?.call_method1("script_load", (script,))?;
        // Driver returns str SHA; redis-py returns bytes.
        if let Ok(s) = raw.extract::<String>() {
            return Ok(PyBytes::new(py, s.as_bytes()).into_any().unbind());
        }
        Ok(raw.unbind())
    }

    #[pyo3(signature = (*shas))]
    fn script_exists(&self, py: Python<'_>, shas: Vec<String>) -> PyResult<Py<PyAny>> {
        let pos_args = PyTuple::new(py, &shas)?;
        Ok(self
            .drv(py)?
            .call_method("script_exists", pos_args, None)?
            .unbind())
    }

    fn script_flush(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("script_flush")?.unbind())
    }

    fn fcall(
        &self,
        py: Python<'_>,
        function: String,
        numkeys: i64,
        keys_and_args: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("fcall", (function, numkeys, keys_and_args))?
            .unbind())
    }

    fn fcall_ro(
        &self,
        py: Python<'_>,
        function: String,
        numkeys: i64,
        keys_and_args: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("fcall_ro", (function, numkeys, keys_and_args))?
            .unbind())
    }

    #[pyo3(signature = (code, replace = false))]
    fn function_load(&self, py: Python<'_>, code: String, replace: bool) -> PyResult<Py<PyAny>> {
        // Driver: function_load(code, *, replace=false)
        let kw = PyDict::new(py);
        if replace {
            kw.set_item("replace", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("function_load", (code,), Some(&kw))?
            .unbind())
    }

    fn function_dump(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("function_dump")?.unbind())
    }

    #[pyo3(signature = (mode = None))]
    fn function_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<Py<PyAny>> {
        // Driver: function_flush(*, mode=None)
        let kw = PyDict::new(py);
        if let Some(m) = mode {
            kw.set_item("mode", m)?;
        }
        Ok(self
            .drv(py)?
            .call_method("function_flush", (), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (library = None, withcode = false))]
    fn function_list(
        &self,
        py: Python<'_>,
        library: Option<String>,
        withcode: bool,
    ) -> PyResult<Py<PyAny>> {
        // Driver: function_list(*, library=None, withcode=false)
        let kw = PyDict::new(py);
        if let Some(l) = library {
            kw.set_item("library", l)?;
        }
        if withcode {
            kw.set_item("withcode", true)?;
        }
        Ok(self
            .drv(py)?
            .call_method("function_list", (), Some(&kw))?
            .unbind())
    }

    fn function_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("function_stats")?.unbind())
    }

    fn function_kill(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("function_kill")?.unbind())
    }

    #[pyo3(signature = (cursor = 0, match_ = None, count = None, type_ = None))]
    fn scan(
        &self,
        py: Python<'_>,
        cursor: u64,
        match_: Option<String>,
        count: Option<i64>,
        type_: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: scan(*, cursor=0, match=None, count=None, type=None) — all keyword-only
        let kw = PyDict::new(py);
        kw.set_item("cursor", cursor)?;
        if let Some(m) = match_ {
            kw.set_item("match", m)?;
        }
        if let Some(c) = count {
            kw.set_item("count", c)?;
        }
        if let Some(t) = type_ {
            kw.set_item("type", t)?;
        }
        Ok(self.drv(py)?.call_method("scan", (), Some(&kw))?.unbind())
    }

    #[pyo3(signature = (match_ = None, count = None, type_ = None))]
    fn scan_iter(
        &self,
        py: Python<'_>,
        match_: Option<String>,
        count: Option<i64>,
        type_: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("scan_iter", (match_, count, type_))?
            .unbind())
    }

    #[pyo3(signature = (pattern = "*".to_string()))]
    fn keys(&self, py: Python<'_>, pattern: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("keys", (pattern,))?.unbind())
    }

    fn randomkey(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("randomkey")?.unbind())
    }

    fn dbsize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("dbsize")?.unbind())
    }

    #[pyo3(signature = (asynchronous = false))]
    fn flushdb(&self, py: Python<'_>, asynchronous: bool) -> PyResult<Py<PyAny>> {
        // Driver: flushdb(*, asynchronous=false) — keyword-only
        let kw = PyDict::new(py);
        kw.set_item("asynchronous", asynchronous)?;
        Ok(self
            .drv(py)?
            .call_method("flushdb", (), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (asynchronous = false))]
    fn flushall(&self, py: Python<'_>, asynchronous: bool) -> PyResult<Py<PyAny>> {
        // Driver: flushall(*, asynchronous=false) — keyword-only
        let kw = PyDict::new(py);
        kw.set_item("asynchronous", asynchronous)?;
        Ok(self
            .drv(py)?
            .call_method("flushall", (), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (section = None))]
    fn info(&self, py: Python<'_>, section: Option<String>) -> PyResult<Py<PyAny>> {
        // Driver: info(*, section=None) — keyword-only
        let kw = PyDict::new(py);
        if let Some(s) = section {
            kw.set_item("section", s)?;
        }
        Ok(self.drv(py)?.call_method("info", (), Some(&kw))?.unbind())
    }

    #[pyo3(signature = (pattern = "*".to_string()))]
    fn config_get(&self, py: Python<'_>, pattern: String) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("config_get", (pattern,))?
            .unbind())
    }

    fn config_set(
        &self,
        py: Python<'_>,
        parameter: String,
        value: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("config_set", (parameter, value))?
            .unbind())
    }

    fn config_resetstat(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("config_resetstat")?.unbind())
    }

    fn config_rewrite(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("config_rewrite")?.unbind())
    }

    #[pyo3(signature = (_id = None, _type = None, addr = None, skipme = true, laddr = None, user = None, maxage = None))]
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
        // Driver: client_kill(*, addr, laddr, client_id, client_type, user, skipme, maxage) — all keyword-only
        let kw = PyDict::new(py);
        if let Some(v) = addr {
            kw.set_item("addr", v)?;
        }
        if let Some(v) = laddr {
            kw.set_item("laddr", v)?;
        }
        if let Some(v) = _id {
            kw.set_item("client_id", v)?;
        }
        if let Some(v) = _type {
            kw.set_item("client_type", v)?;
        }
        if let Some(v) = user {
            kw.set_item("user", v)?;
        }
        kw.set_item("skipme", skipme)?;
        if let Some(v) = maxage {
            kw.set_item("maxage", v)?;
        }
        Ok(self
            .drv(py)?
            .call_method("client_kill", (), Some(&kw))?
            .unbind())
    }

    fn client_getname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("client_getname")?.unbind())
    }

    fn client_setname(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("client_setname", (name,))?
            .unbind())
    }

    #[pyo3(signature = (_type = None, client_id = None))]
    fn client_list(
        &self,
        py: Python<'_>,
        _type: Option<String>,
        client_id: Option<Vec<i64>>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: client_list(*, client_type=None, client_id=None) — all keyword-only
        let kw = PyDict::new(py);
        if let Some(t) = _type {
            kw.set_item("client_type", t)?;
        }
        if let Some(ids) = client_id {
            kw.set_item("client_id", ids)?;
        }
        Ok(self
            .drv(py)?
            .call_method("client_list", (), Some(&kw))?
            .unbind())
    }

    fn client_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("client_id")?.unbind())
    }

    fn client_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("client_info")?.unbind())
    }

    #[pyo3(signature = (timeout, all = true))]
    fn client_pause(&self, py: Python<'_>, timeout: i64, all: bool) -> PyResult<Py<PyAny>> {
        // Driver: client_pause(timeout_ms, *, all=true) — all keyword-only
        let kw = PyDict::new(py);
        kw.set_item("all", all)?;
        Ok(self
            .drv(py)?
            .call_method("client_pause", (timeout,), Some(&kw))?
            .unbind())
    }

    fn client_unpause(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("client_unpause")?.unbind())
    }

    fn client_no_evict(&self, py: Python<'_>, mode: String) -> PyResult<Py<PyAny>> {
        // Driver: client_no_evict(*, mode) — keyword-only
        let kw = PyDict::new(py);
        kw.set_item("mode", &mode)?;
        Ok(self
            .drv(py)?
            .call_method("client_no_evict", (), Some(&kw))?
            .unbind())
    }

    fn client_no_touch(&self, py: Python<'_>, mode: String) -> PyResult<Py<PyAny>> {
        // Driver: client_no_touch(*, mode) — keyword-only
        let kw = PyDict::new(py);
        kw.set_item("mode", &mode)?;
        Ok(self
            .drv(py)?
            .call_method("client_no_touch", (), Some(&kw))?
            .unbind())
    }

    fn object_encoding(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("object_encoding", (name,))?
            .unbind())
    }

    fn object_idletime(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("object_idletime", (name,))?
            .unbind())
    }

    fn object_freq(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method1("object_freq", (name,))?.unbind())
    }

    fn object_refcount(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("object_refcount", (name,))?
            .unbind())
    }

    fn object_help(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("object_help")?.unbind())
    }

    #[pyo3(signature = (key, samples = None))]
    fn memory_usage(
        &self,
        py: Python<'_>,
        key: String,
        samples: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // Driver: memory_usage(key, *, samples=None) — samples keyword-only
        let kw = PyDict::new(py);
        if let Some(s) = samples {
            kw.set_item("samples", s)?;
        }
        Ok(self
            .drv(py)?
            .call_method("memory_usage", (key,), Some(&kw))?
            .unbind())
    }

    #[pyo3(signature = (message = None))]
    fn ping(&self, py: Python<'_>, message: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        match message {
            None => Ok(self.drv(py)?.call_method0("ping")?.unbind()),
            Some(msg) => {
                let msg = to_bytes_obj(py, msg.bind(py))?;
                Ok(self.drv(py)?.call_method1("echo", (msg,))?.unbind())
            }
        }
    }

    fn echo(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let value = to_bytes_obj(py, value.bind(py))?;
        Ok(self.drv(py)?.call_method1("echo", (value,))?.unbind())
    }

    fn wait(&self, py: Python<'_>, numreplicas: i64, timeout: i64) -> PyResult<Py<PyAny>> {
        // Driver: wait(*, numreplicas, timeout) — all keyword-only
        let kw = PyDict::new(py);
        kw.set_item("numreplicas", numreplicas)?;
        kw.set_item("timeout", timeout)?;
        Ok(self.drv(py)?.call_method("wait", (), Some(&kw))?.unbind())
    }

    fn waitaof(
        &self,
        py: Python<'_>,
        numlocal: i64,
        numreplicas: i64,
        timeout: i64,
    ) -> PyResult<Py<PyAny>> {
        // Driver: waitaof(*, numlocal, numreplicas, timeout) — all keyword-only
        let kw = PyDict::new(py);
        kw.set_item("numlocal", numlocal)?;
        kw.set_item("numreplicas", numreplicas)?;
        kw.set_item("timeout", timeout)?;
        Ok(self
            .drv(py)?
            .call_method("waitaof", (), Some(&kw))?
            .unbind())
    }

    fn time(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Driver returns (sec_str, usec_str); redis-py returns (int, int)
        let raw = self.drv(py)?.call_method0("time")?;
        let pair: Option<(Option<String>, Option<String>)> = raw.extract().ok();
        if let Some((Some(s), Some(us))) = pair {
            let sec: i64 = s.parse().unwrap_or(0);
            let usec: i64 = us.parse().unwrap_or(0);
            return Ok(PyTuple::new(py, [sec, usec])?.into_any().unbind());
        }
        Ok(raw.unbind())
    }

    fn lastsave(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("lastsave")?.unbind())
    }

    #[pyo3(signature = (schedule = false))]
    fn bgsave(&self, py: Python<'_>, schedule: bool) -> PyResult<Py<PyAny>> {
        // Driver: bgsave(*, schedule=false) — keyword-only
        let kw = PyDict::new(py);
        kw.set_item("schedule", schedule)?;
        Ok(self.drv(py)?.call_method("bgsave", (), Some(&kw))?.unbind())
    }

    fn bgrewriteaof(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.drv(py)?.call_method0("bgrewriteaof")?.unbind())
    }

    fn debug_sleep(&self, py: Python<'_>, seconds: f64) -> PyResult<Py<PyAny>> {
        Ok(self
            .drv(py)?
            .call_method1("debug_sleep", (seconds,))?
            .unbind())
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
    let (scheme, rest) = match input.split_once("://") {
        Some(s) => s,
        None => {
            return Err(PyValueError::new_err(format!(
                "Invalid Redis URL: {:?}; expected scheme://...",
                input
            )));
        }
    };
    let (ssl, is_unix) = match scheme {
        "redis" => (false, false),
        "rediss" => (true, false),
        "unix" => (false, true),
        other => {
            return Err(PyValueError::new_err(format!(
                "Invalid Redis URL scheme {:?}; expected redis://, rediss:// or unix://",
                other
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
        cfg.username = user.filter(|s| !s.is_empty()).map(percent_decode);
        cfg.password = pass.map(percent_decode);
    }

    if is_unix {
        cfg.host = host_port.to_string();
    } else if let Some((h, p)) = host_port.rsplit_once(':') {
        cfg.host = h.to_string();
        cfg.port = p.parse().map_err(|_| {
            PyValueError::new_err(format!("Invalid port in Redis URL: {:?}", input))
        })?;
    } else if !host_port.is_empty() {
        cfg.host = host_port.to_string();
    } else {
        cfg.host = "localhost".into();
    }

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
        kwargs.set_item(
            "ssl_ca_certs",
            std::fs::read(f)
                .map_err(|e| PyValueError::new_err(format!("Cannot read ssl_ca_certs {f}: {e}")))?,
        )?;
    }
    if let Some(ref f) = cfg.ssl_certfile {
        kwargs.set_item(
            "ssl_certfile",
            std::fs::read(f)
                .map_err(|e| PyValueError::new_err(format!("Cannot read ssl_certfile {f}: {e}")))?,
        )?;
    }
    if let Some(ref f) = cfg.ssl_keyfile {
        kwargs.set_item(
            "ssl_keyfile",
            std::fs::read(f)
                .map_err(|e| PyValueError::new_err(format!("Cannot read ssl_keyfile {f}: {e}")))?,
        )?;
    }
    let driver_cls = py.get_type::<RedisRsDriver>();
    let drv = driver_cls
        .call_method("connect_standard", (url,), Some(&kwargs))?
        .cast_into::<RedisRsDriver>()?
        .unbind();
    Ok(drv)
}

// =========================================================================
// Distributed lock helper.
// =========================================================================

const LOCK_RELEASE_LUA: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
";

const LOCK_EXTEND_LUA: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
    return 0
end
";

#[pyclass(module = "redis_rs_py._driver", name = "Lock")]
#[allow(dead_code)]
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
            let kw = PyDict::new(py);
            kw.set_item("nx", true)?;
            if px > 0 {
                kw.set_item("px", px)?;
            }
            let res: Py<PyAny> = r
                .call_method(
                    "set",
                    (self.name.clone(), PyBytes::new(py, &token)),
                    Some(&kw),
                )?
                .unbind();
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
        let token = {
            let guard = self.token.lock().unwrap();
            guard.clone()
        };
        let token = match token {
            Some(t) => t,
            None => {
                let exc = py
                    .import("redis_rs_py.exceptions")?
                    .getattr("LockNotOwnedError")?;
                let err = exc.call1(("Cannot release an unlocked lock",))?;
                return Err(PyErr::from_value(err));
            }
        };
        let r = self.redis.bind(py);
        let eval_args = PyTuple::new(
            py,
            [
                LOCK_RELEASE_LUA.into_pyobject(py)?.into_any(),
                1_i64.into_pyobject(py)?.into_any(),
                self.name.clone().into_pyobject(py)?.into_any(),
                PyBytes::new(py, &token).into_any(),
            ],
        )?;
        let n: i64 = r.call_method("eval", eval_args, None)?.extract()?;
        if n == 0 {
            let exc = py
                .import("redis_rs_py.exceptions")?
                .getattr("LockNotOwnedError")?;
            let err = exc.call1(("Cannot release a lock owned by someone else",))?;
            return Err(PyErr::from_value(err));
        }
        *self.token.lock().unwrap() = None;
        Ok(())
    }

    #[pyo3(signature = (additional_time, replace_ttl = false))]
    fn extend(&self, py: Python<'_>, additional_time: f64, replace_ttl: bool) -> PyResult<bool> {
        let _ = replace_ttl;
        let token = {
            let guard = self.token.lock().unwrap();
            guard.clone()
        };
        let token = match token {
            Some(t) => t,
            None => {
                let exc = py
                    .import("redis_rs_py.exceptions")
                    .and_then(|m| m.getattr("LockNotOwnedError"));
                return Err(match exc {
                    Ok(c) => {
                        PyErr::from_value(c.call1(("Cannot extend an unlocked lock",)).unwrap())
                    }
                    Err(e) => e,
                });
            }
        };
        let r = self.redis.bind(py);
        let eval_args = PyTuple::new(
            py,
            [
                LOCK_EXTEND_LUA.into_pyobject(py)?.into_any(),
                1_i64.into_pyobject(py)?.into_any(),
                self.name.clone().into_pyobject(py)?.into_any(),
                PyBytes::new(py, &token).into_any(),
                ((additional_time * 1000.0) as i64)
                    .into_pyobject(py)?
                    .into_any(),
            ],
        )?;
        let n: i64 = r.call_method("eval", eval_args, None)?.extract()?;
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
