// Sync façade: redis_rs_py.Redis.
//
// Mirrors redis-py's Redis class — same constructor kwargs, same method
// names. The struct owns a ValkeyConn directly (no Py-wrapped driver
// indirection). Command methods are added via `#[pymethods] impl Redis`
// blocks in each `commands/*.rs` file (PyO3 multiple-pymethods feature).

#![allow(clippy::too_many_arguments)]

use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::connection::{TlsOpts, ValkeyConn, connect_standard, url_with_resp3};
use crate::facade::kwargs::{IMPLEMENTED_KWARGS, accept_and_warn};

// =========================================================================
// Internal config struct (also used by asyncio_mod.rs)
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
    pub(crate) fn to_url(&self) -> String {
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
pub struct Redis {
    pub(crate) connection: ValkeyConn,
    pub(crate) url: String,
    pub(crate) closed: bool,
    pub(crate) decode: Option<crate::facade::decode::DecodeOpts>,
}

impl Redis {
    /// If decode_responses is on, walk the value and return a decoded
    /// fresh tree; otherwise return the original.
    pub(crate) fn maybe_decode(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        match &self.decode {
            Some(opts) => crate::facade::decode::decode_walk(py, value.bind(py), opts),
            None => Ok(value),
        }
    }
}

// =========================================================================
// Constructor, from_url, lifecycle, stubs
// =========================================================================

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
            socket_timeout: socket_timeout.or(None),
            max_connections,
            health_check_interval,
            client_name,
            protocol,
            decode_responses,
            encoding,
            encoding_errors,
        };

        build_connection(py, &config)
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
        // Mark the connection as closed so subsequent commands raise ValueError.
        self.closed = true;
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
    fn pipeline(
        &self,
        py: Python<'_>,
        transaction: bool,
        shard_hint: Option<Py<PyAny>>,
    ) -> PyResult<Py<crate::facade::pipeline::Pipeline>> {
        let _ = shard_hint;
        Py::new(
            py,
            crate::facade::pipeline::Pipeline::new(self.connection.clone(), transaction),
        )
    }

    #[pyo3(signature = (**kwargs))]
    fn pubsub(&self, kwargs: Option<Bound<'_, PyDict>>) -> PyResult<Py<PyAny>> {
        let _ = kwargs;
        Err(PyNotImplementedError::new_err(
            "PubSub is implemented by plan 14 (pubsub).",
        ))
    }

    #[pyo3(signature = (func, *watches, value_from_callable = false, watch_delay = None, **_kwargs))]
    fn transaction(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        watches: &Bound<'_, PyTuple>,
        value_from_callable: bool,
        watch_delay: Option<f64>,
        _kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let watch_keys: Vec<String> = watches
            .iter()
            .map(|k| k.extract::<String>())
            .collect::<PyResult<_>>()?;
        crate::facade::pipeline::transaction_helper(
            py,
            self.connection.clone(),
            func,
            watch_keys,
            value_from_callable,
            watch_delay,
        )
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
}

// =========================================================================
// URL parsing helpers
// =========================================================================

#[derive(Debug, Default)]
pub(crate) struct UrlConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) db: i64,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) ssl: bool,
}

pub(crate) fn parse_url(input: &str) -> PyResult<UrlConfig> {
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
// Connection factory (replaces build_driver)
// =========================================================================

pub(crate) fn build_connection(py: Python<'_>, cfg: &FacadeConfig) -> PyResult<Redis> {
    let url = cfg.to_url();
    let cache_opts = None; // TODO: wire cache_max_size / cache_ttl_secs from kwargs in a later plan
    let tls_opts =
        if cfg.ssl_ca_certs.is_some() || cfg.ssl_certfile.is_some() || cfg.ssl_keyfile.is_some() {
            let root_cert = cfg
                .ssl_ca_certs
                .as_ref()
                .map(|f| {
                    std::fs::read(f).map_err(|e| {
                        PyValueError::new_err(format!("Cannot read ssl_ca_certs {f}: {e}"))
                    })
                })
                .transpose()?;
            let client_cert = cfg
                .ssl_certfile
                .as_ref()
                .map(|f| {
                    std::fs::read(f).map_err(|e| {
                        PyValueError::new_err(format!("Cannot read ssl_certfile {f}: {e}"))
                    })
                })
                .transpose()?;
            let client_key = cfg
                .ssl_keyfile
                .as_ref()
                .map(|f| {
                    std::fs::read(f).map_err(|e| {
                        PyValueError::new_err(format!("Cannot read ssl_keyfile {f}: {e}"))
                    })
                })
                .transpose()?;
            Some(TlsOpts {
                root_cert,
                client_cert,
                client_key,
            })
        } else {
            None
        };

    let resp3_url = url_with_resp3(&url);
    let url_clone = resp3_url.clone();
    let conn = py.detach(|| {
        crate::runtime::get_runtime()
            .block_on(async { connect_standard(&url_clone, cache_opts, tls_opts).await })
    });
    let decode = if cfg.decode_responses {
        Some(crate::facade::decode::DecodeOpts::new(
            cfg.encoding.clone(),
            cfg.encoding_errors.clone(),
        ))
    } else {
        None
    };

    match conn {
        Ok(c) => Ok(Redis {
            connection: c,
            url: resp3_url,
            closed: false,
            decode,
        }),
        Err(e) => Err(crate::errors::to_py_err(redis::RedisError::from((
            redis::ErrorKind::Io,
            "connect",
            e,
        )))),
    }
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
        let r = self.redis.bind(py).borrow();
        let deadline = blocking_timeout.map(|t| now_secs() + t);
        loop {
            let res: Py<PyAny> = r.set(
                py,
                &self.name,
                token.as_slice(),
                /* ex */ None,
                /* px */ if px > 0 { Some(px as u64) } else { None },
                /* nx */ true,
                /* xx */ false,
                /* keepttl */ false,
                /* get */ false,
                /* exat */ None,
                /* pxat */ None,
            )?;
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
                    .import("redis_rs_py.exceptions")
                    .and_then(|m| m.getattr("LockNotOwnedError"));
                return Err(match exc {
                    Ok(c) => {
                        PyErr::from_value(c.call1(("Cannot release an unlocked lock",)).unwrap())
                    }
                    Err(e) => e,
                });
            }
        };
        let r = self.redis.bind(py).borrow();
        let result: Py<PyAny> =
            r.eval(py, LOCK_RELEASE_LUA, vec![self.name.clone()], vec![token])?;
        let n: i64 = result.extract(py)?;
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
        let r = self.redis.bind(py).borrow();
        let millis_str = format!("{}", (additional_time * 1000.0) as i64);
        let args = vec![token, millis_str.into_bytes()];
        let result: Py<PyAny> = r.eval(py, LOCK_EXTEND_LUA, vec![self.name.clone()], args)?;
        let n: i64 = result.extract(py)?;
        Ok(n > 0)
    }

    fn owned(&self, py: Python<'_>) -> PyResult<bool> {
        let token = match self.token.lock().unwrap().clone() {
            Some(t) => t,
            None => return Ok(false),
        };
        let r = self.redis.bind(py).borrow();
        let val: Option<Vec<u8>> = r.get(py, &self.name)?.extract(py)?;
        Ok(val.as_deref() == Some(token.as_slice()))
    }

    fn locked(&self, py: Python<'_>) -> PyResult<bool> {
        let r = self.redis.bind(py).borrow();
        let val: Option<Vec<u8>> = r.get(py, &self.name)?.extract(py)?;
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
