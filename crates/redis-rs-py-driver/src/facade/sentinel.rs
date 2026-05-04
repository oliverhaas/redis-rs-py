// Sentinel + AsyncSentinel pyclasses (Plan 16).
//
// Mirrors `redis.sentinel.Sentinel.__init__`:
//
//     Sentinel(sentinels=[(host, port), ...],
//              min_other_sentinels=0,
//              sentinel_kwargs=None,
//              force_master_ip=None,
//              **connection_kwargs)
//
// `master_for(service_name)` returns a `Redis` backed by
// `connect_sentinel(..., is_slave=False)`. `slave_for`
// uses `is_slave=True`. The discovery + admin commands open a transient
// sentinel-only connection.
//
// `connection_kwargs` flow through to the master/slave Redis instance.

#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::async_bridge::{RawResult, RedisRsAwaitable};
use crate::connection::{ClientCacheOpts, TlsOpts, ValkeyConn, connect_sentinel};
use crate::exceptions::{DataError, MasterDownError};
use crate::facade::kwargs::accept_and_warn;
use crate::facade::sync::Redis;
use crate::runtime::get_runtime;

// =========================================================================
// Accepted kwargs list for Sentinel constructor
// =========================================================================

const SENTINEL_KWARGS_ACCEPT: &[&str] = &[
    "sentinels",
    "min_other_sentinels",
    "sentinel_kwargs",
    "force_master_ip",
    // connection_kwargs forwarded to master_for/slave_for:
    "db",
    "username",
    "password",
    "decode_responses",
    "encoding",
    "encoding_errors",
    "ssl",
    "ssl_ca_certs",
    "ssl_certfile",
    "ssl_keyfile",
    "socket_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "client_name",
    "cache_max_size",
    "cache_ttl_secs",
];

// =========================================================================
// SentinelInner — shared manager struct
// =========================================================================

/// Internal manager: holds the sentinel URL list + the per-service
/// round-robin index used by `slave_for`.
#[derive(Clone)]
struct SentinelInner {
    urls: Arc<[String]>,
    /// Map of service_name → (last slave index used).
    slave_cursors: Arc<Mutex<HashMap<String, AtomicUsize>>>,
}

impl SentinelInner {
    fn next_slave_idx(&self, service: &str) -> usize {
        let mut guard = self.slave_cursors.lock().unwrap();
        let counter = guard
            .entry(service.to_string())
            .or_insert_with(|| AtomicUsize::new(0));
        counter.fetch_add(1, Ordering::Relaxed)
    }
}

// =========================================================================
// with_sentinel helper — open a transient sentinel connection
// =========================================================================

/// Open a transient connection to the first reachable sentinel and
/// hand it back to the caller closure. Returns the closure's result.
async fn with_sentinel<F, Fut, T>(urls: &[String], f: F) -> Result<T, redis::RedisError>
where
    F: Fn(redis::aio::ConnectionManager) -> Fut,
    Fut: std::future::Future<Output = Result<T, redis::RedisError>>,
{
    let mut last_err: Option<redis::RedisError> = None;
    for u in urls {
        let client = match redis::Client::open(u.as_str()) {
            Ok(c) => c,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
        use redis::aio::ConnectionManagerConfig;
        let cfg = ConnectionManagerConfig::new().set_response_timeout(Some(Duration::from_secs(5)));
        let conn = match redis::aio::ConnectionManager::new_with_config(client, cfg).await {
            Ok(c) => c,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
        match f(conn).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        redis::RedisError::from((redis::ErrorKind::Io, "no sentinels reachable"))
    }))
}

// =========================================================================
// Helper: parse (host, port) list from sentinel slave/sentinel responses
// =========================================================================

fn rows_to_dicts(py: Python<'_>, rows: Vec<Vec<(String, String)>>) -> PyResult<Vec<Py<PyDict>>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let d = PyDict::new(py);
        for (k, v) in row {
            d.set_item(k, v)?;
        }
        out.push(d.unbind());
    }
    Ok(out)
}

// =========================================================================
// build_redis_conn — create a ValkeyConn backed by sentinel for a service
// =========================================================================

fn build_redis_conn(
    py: Python<'_>,
    inner: &SentinelInner,
    connection_kwargs: &Py<PyDict>,
    service_name: String,
    is_slave: bool,
    per_call_kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Redis> {
    // Merge: connection_kwargs (constructor) + per_call_kwargs (call site).
    // Per-call wins on conflict.
    let merged = PyDict::new(py);
    let conn_kw = connection_kwargs.bind(py);
    for (k, v) in conn_kw.iter() {
        merged.set_item(k, v)?;
    }
    if let Some(kw) = per_call_kwargs {
        for (k, v) in kw.iter() {
            merged.set_item(k, v)?;
        }
    }

    let db: i64 = match merged.get_item("db")? {
        Some(v) => v.extract().unwrap_or(0),
        None => 0,
    };
    let cache_max_size: Option<usize> = merged
        .get_item("cache_max_size")?
        .and_then(|v| v.extract().ok());
    let cache_ttl_secs: Option<u64> = merged
        .get_item("cache_ttl_secs")?
        .and_then(|v| v.extract().ok());
    let ssl_ca_certs: Option<Vec<u8>> = merged
        .get_item("ssl_ca_certs")?
        .and_then(|v| v.extract().ok());
    let ssl_certfile: Option<Vec<u8>> = merged
        .get_item("ssl_certfile")?
        .and_then(|v| v.extract().ok());
    let ssl_keyfile: Option<Vec<u8>> = merged
        .get_item("ssl_keyfile")?
        .and_then(|v| v.extract().ok());

    let cache_opts = match (cache_max_size, cache_ttl_secs) {
        (None, None) => None,
        (max, ttl) => Some(ClientCacheOpts {
            max_size: max.unwrap_or(10_000),
            ttl_secs: ttl.unwrap_or(300),
        }),
    };
    let tls_opts = if ssl_ca_certs.is_some() || ssl_certfile.is_some() || ssl_keyfile.is_some() {
        Some(TlsOpts {
            root_cert: ssl_ca_certs,
            client_cert: ssl_certfile,
            client_key: ssl_keyfile,
        })
    } else {
        None
    };

    let urls: Vec<String> = inner.urls.iter().cloned().collect();
    let service_clone = service_name.clone();
    let conn: Result<ValkeyConn, String> = py.detach(|| {
        get_runtime().block_on(async {
            connect_sentinel(urls, &service_clone, db, is_slave, cache_opts, tls_opts).await
        })
    });
    match conn {
        Ok(c) => {
            // Build a representative URL for introspection (first sentinel URL).
            let url = inner
                .urls
                .first()
                .cloned()
                .unwrap_or_else(|| format!("sentinel://{service_name}"));
            Ok(Redis {
                connection: c,
                url,
                closed: false,
                decode: None,
            })
        }
        Err(e) => Err(crate::errors::to_py_err(redis::RedisError::from((
            redis::ErrorKind::Io,
            "connect_sentinel",
            e,
        )))),
    }
}

// =========================================================================
// Sentinel (sync façade)
// =========================================================================

#[pyclass(module = "redis_rs_py._driver.sentinel")]
pub struct Sentinel {
    inner: SentinelInner,
    #[pyo3(get)]
    sentinels: Py<PyList>,
    #[pyo3(get)]
    min_other_sentinels: u32,
    sentinel_kwargs: Py<PyDict>,
    connection_kwargs: Py<PyDict>,
}

#[pymethods]
impl Sentinel {
    #[new]
    #[pyo3(signature = (
        sentinels,
        min_other_sentinels = 0,
        sentinel_kwargs = None,
        force_master_ip = None,
        **connection_kwargs
    ))]
    fn new(
        py: Python<'_>,
        sentinels: &Bound<'_, PyList>,
        min_other_sentinels: u32,
        sentinel_kwargs: Option<&Bound<'_, PyDict>>,
        force_master_ip: Option<String>,
        connection_kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        if sentinels.is_empty() {
            return Err(PyErr::new::<DataError, _>(
                "Sentinel requires at least one (host, port) tuple",
            ));
        }
        let _ = force_master_ip; // accept-and-warn handled below

        let mut urls: Vec<String> = Vec::with_capacity(sentinels.len());
        for item in sentinels.iter() {
            let tup: &Bound<'_, PyTuple> = item.cast()?;
            if tup.len() != 2 {
                return Err(PyErr::new::<DataError, _>(
                    "each sentinel entry must be a (host, port) tuple",
                ));
            }
            let host: String = tup.get_item(0)?.extract()?;
            let port: u16 = tup.get_item(1)?.extract()?;
            urls.push(format!("redis://{host}:{port}"));
        }

        if let Some(kw) = connection_kwargs {
            accept_and_warn(py, SENTINEL_KWARGS_ACCEPT, Some(kw))?;
        }

        let connection_kwargs = match connection_kwargs {
            Some(d) => d.clone().unbind(),
            None => PyDict::new(py).unbind(),
        };
        let sentinel_kwargs = match sentinel_kwargs {
            Some(d) => d.clone().unbind(),
            None => PyDict::new(py).unbind(),
        };

        Ok(Sentinel {
            inner: SentinelInner {
                urls: Arc::from(urls),
                slave_cursors: Arc::new(Mutex::new(HashMap::new())),
            },
            sentinels: sentinels.clone().unbind(),
            min_other_sentinels,
            sentinel_kwargs,
            connection_kwargs,
        })
    }

    fn __repr__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<String> {
        let n = slf.sentinels.bind(py).len();
        Ok(format!(
            "<Sentinel(sentinels={n}, min_other_sentinels={})>",
            slf.min_other_sentinels
        ))
    }

    // =========================================================================
    // master_for / slave_for — return Redis instances
    // =========================================================================

    #[pyo3(signature = (
        service_name,
        redis_class = None,
        connection_pool_class = None,
        **kwargs
    ))]
    fn master_for(
        &self,
        py: Python<'_>,
        service_name: String,
        redis_class: Option<&Bound<'_, PyAny>>,
        connection_pool_class: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<Redis>> {
        let _ = (redis_class, connection_pool_class);
        let r = build_redis_conn(
            py,
            &self.inner,
            &self.connection_kwargs,
            service_name,
            false,
            kwargs,
        )?;
        Py::new(py, r)
    }

    #[pyo3(signature = (
        service_name,
        redis_class = None,
        connection_pool_class = None,
        **kwargs
    ))]
    fn slave_for(
        &self,
        py: Python<'_>,
        service_name: String,
        redis_class: Option<&Bound<'_, PyAny>>,
        connection_pool_class: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<Redis>> {
        let _ = (redis_class, connection_pool_class);
        // Bump the round-robin cursor.
        self.inner.next_slave_idx(&service_name);
        let r = build_redis_conn(
            py,
            &self.inner,
            &self.connection_kwargs,
            service_name,
            true,
            kwargs,
        )?;
        Py::new(py, r)
    }

    // =========================================================================
    // Discovery commands
    // =========================================================================

    fn discover_master(&self, py: Python<'_>, service_name: String) -> PyResult<(String, u16)> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let result = py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    async move {
                        let addr: Vec<String> = redis::cmd("SENTINEL")
                            .arg("get-master-addr-by-name")
                            .arg(&svc)
                            .query_async(&mut conn)
                            .await?;
                        if addr.len() != 2 {
                            return Err(redis::RedisError::from((
                                redis::ErrorKind::Server(redis::ServerErrorKind::MasterDown),
                                "no master found",
                            )));
                        }
                        let port: u16 = addr[1].parse().map_err(|_| {
                            redis::RedisError::from((
                                redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
                                "invalid port in sentinel response",
                            ))
                        })?;
                        Ok((addr[0].clone(), port))
                    }
                })
                .await
            })
        });
        result.map_err(|e| PyErr::new::<MasterDownError, _>(e.to_string()))
    }

    fn discover_slaves(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<Vec<(String, u16)>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let result = py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    async move {
                        let rows: Vec<Vec<(String, String)>> = redis::cmd("SENTINEL")
                            .arg("slaves")
                            .arg(&svc)
                            .query_async(&mut conn)
                            .await?;
                        let mut out = Vec::with_capacity(rows.len());
                        for row in rows {
                            let map: HashMap<_, _> = row.into_iter().collect();
                            let flags = map.get("flags").cloned().unwrap_or_default();
                            if flags.contains("disconnected")
                                || flags.contains("s_down")
                                || flags.contains("o_down")
                            {
                                continue;
                            }
                            let host = match map.get("ip") {
                                Some(h) => h.clone(),
                                None => continue,
                            };
                            let port: u16 = match map.get("port").and_then(|p| p.parse().ok()) {
                                Some(p) => p,
                                None => continue,
                            };
                            out.push((host, port));
                        }
                        Ok(out)
                    }
                })
                .await
            })
        });
        result.map_err(crate::errors::to_py_err)
    }

    fn sentinel_get_master_addr_by_name(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<(String, u16)> {
        self.discover_master(py, service_name)
    }

    // =========================================================================
    // Introspection commands
    // =========================================================================

    fn sentinel_masters(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let rows: Vec<Vec<(String, String)>> = py
            .detach(|| {
                get_runtime().block_on(async move {
                    with_sentinel(&urls, |mut conn| async move {
                        redis::cmd("SENTINEL")
                            .arg("masters")
                            .query_async(&mut conn)
                            .await
                    })
                    .await
                })
            })
            .map_err(crate::errors::to_py_err)?;
        let out = PyDict::new(py);
        for row in rows {
            let map: HashMap<_, _> = row.into_iter().collect();
            let name = match map.get("name") {
                Some(n) => n.clone(),
                None => continue,
            };
            let entry = PyDict::new(py);
            for (k, v) in map {
                entry.set_item(k, v)?;
            }
            out.set_item(name, entry)?;
        }
        Ok(out.unbind())
    }

    fn sentinel_master(&self, py: Python<'_>, service_name: String) -> PyResult<Py<PyDict>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let row: Vec<(String, String)> = py
            .detach(|| {
                get_runtime().block_on(async move {
                    with_sentinel(&urls, |mut conn| {
                        let svc = service_name.clone();
                        async move {
                            redis::cmd("SENTINEL")
                                .arg("master")
                                .arg(&svc)
                                .query_async(&mut conn)
                                .await
                        }
                    })
                    .await
                })
            })
            .map_err(crate::errors::to_py_err)?;
        let d = PyDict::new(py);
        for (k, v) in row {
            d.set_item(k, v)?;
        }
        Ok(d.unbind())
    }

    fn sentinel_slaves(&self, py: Python<'_>, service_name: String) -> PyResult<Vec<Py<PyDict>>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let rows: Vec<Vec<(String, String)>> = py
            .detach(|| {
                get_runtime().block_on(async move {
                    with_sentinel(&urls, |mut conn| {
                        let svc = service_name.clone();
                        async move {
                            redis::cmd("SENTINEL")
                                .arg("slaves")
                                .arg(&svc)
                                .query_async(&mut conn)
                                .await
                        }
                    })
                    .await
                })
            })
            .map_err(crate::errors::to_py_err)?;
        rows_to_dicts(py, rows)
    }

    fn sentinel_sentinels(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let rows: Vec<Vec<(String, String)>> = py
            .detach(|| {
                get_runtime().block_on(async move {
                    with_sentinel(&urls, |mut conn| {
                        let svc = service_name.clone();
                        async move {
                            redis::cmd("SENTINEL")
                                .arg("sentinels")
                                .arg(&svc)
                                .query_async(&mut conn)
                                .await
                        }
                    })
                    .await
                })
            })
            .map_err(crate::errors::to_py_err)?;
        rows_to_dicts(py, rows)
    }

    // =========================================================================
    // Admin commands
    // =========================================================================

    fn sentinel_failover(&self, py: Python<'_>, service_name: String) -> PyResult<()> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("failover")
                            .arg(&svc)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn sentinel_reset(&self, py: Python<'_>, pattern: String) -> PyResult<i64> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let pat = pattern.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("reset")
                            .arg(&pat)
                            .query_async(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn sentinel_set(
        &self,
        py: Python<'_>,
        service_name: String,
        option: String,
        value: String,
    ) -> PyResult<()> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    let opt = option.clone();
                    let val = value.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("set")
                            .arg(&svc)
                            .arg(&opt)
                            .arg(&val)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn sentinel_remove(&self, py: Python<'_>, service_name: String) -> PyResult<()> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("remove")
                            .arg(&svc)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn sentinel_monitor(
        &self,
        py: Python<'_>,
        service_name: String,
        ip: String,
        port: u16,
        quorum: u32,
    ) -> PyResult<()> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    let ip = ip.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("monitor")
                            .arg(&svc)
                            .arg(&ip)
                            .arg(port)
                            .arg(quorum)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }
}

// =========================================================================
// AsyncSentinel — async sibling
// =========================================================================

#[pyclass(module = "redis_rs_py._driver.asyncio.sentinel")]
pub struct AsyncSentinel {
    inner: SentinelInner,
    #[pyo3(get)]
    sentinels: Py<PyList>,
    #[pyo3(get)]
    min_other_sentinels: u32,
    #[allow(dead_code)]
    sentinel_kwargs: Py<PyDict>,
    connection_kwargs: Py<PyDict>,
}

#[pymethods]
impl AsyncSentinel {
    #[new]
    #[pyo3(signature = (
        sentinels,
        min_other_sentinels = 0,
        sentinel_kwargs = None,
        force_master_ip = None,
        **connection_kwargs
    ))]
    fn new(
        py: Python<'_>,
        sentinels: &Bound<'_, PyList>,
        min_other_sentinels: u32,
        sentinel_kwargs: Option<&Bound<'_, PyDict>>,
        force_master_ip: Option<String>,
        connection_kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let _ = force_master_ip;
        // Delegate to Sentinel::new for validation.
        let inst = Sentinel::new(
            py,
            sentinels,
            min_other_sentinels,
            sentinel_kwargs,
            None,
            connection_kwargs,
        )?;
        Ok(AsyncSentinel {
            inner: inst.inner,
            sentinels: inst.sentinels,
            min_other_sentinels: inst.min_other_sentinels,
            sentinel_kwargs: inst.sentinel_kwargs,
            connection_kwargs: inst.connection_kwargs,
        })
    }

    fn __repr__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<String> {
        let n = slf.sentinels.bind(py).len();
        Ok(format!(
            "<AsyncSentinel(sentinels={n}, min_other_sentinels={})>",
            slf.min_other_sentinels
        ))
    }

    // =========================================================================
    // master_for / slave_for — return async Redis instances
    // =========================================================================

    #[pyo3(signature = (service_name, redis_class = None, connection_pool_class = None, **kwargs))]
    fn master_for(
        &self,
        py: Python<'_>,
        service_name: String,
        redis_class: Option<&Bound<'_, PyAny>>,
        connection_pool_class: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<crate::facade::asyncio_mod::AsyncRedis>> {
        let _ = (redis_class, connection_pool_class);
        build_async_redis(
            py,
            &self.inner,
            &self.connection_kwargs,
            service_name,
            false,
            kwargs,
        )
    }

    #[pyo3(signature = (service_name, redis_class = None, connection_pool_class = None, **kwargs))]
    fn slave_for(
        &self,
        py: Python<'_>,
        service_name: String,
        redis_class: Option<&Bound<'_, PyAny>>,
        connection_pool_class: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<crate::facade::asyncio_mod::AsyncRedis>> {
        let _ = (redis_class, connection_pool_class);
        self.inner.next_slave_idx(&service_name);
        build_async_redis(
            py,
            &self.inner,
            &self.connection_kwargs,
            service_name,
            true,
            kwargs,
        )
    }

    // =========================================================================
    // Async discovery
    // =========================================================================

    fn discover_master(&self, py: Python<'_>, service_name: String) -> PyResult<Py<PyAny>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        get_runtime().spawn(async move {
            let r = with_sentinel(&urls, |mut conn| {
                let svc = service_name.clone();
                async move {
                    let addr: Vec<String> = redis::cmd("SENTINEL")
                        .arg("get-master-addr-by-name")
                        .arg(&svc)
                        .query_async(&mut conn)
                        .await?;
                    if addr.len() != 2 {
                        return Err(redis::RedisError::from((
                            redis::ErrorKind::Server(redis::ServerErrorKind::MasterDown),
                            "no master",
                        )));
                    }
                    Ok(addr)
                }
            })
            .await;
            let raw = match r {
                Ok(addr) => RawResult::StringList(addr),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(aw.into_pyobject(py)?.into_any().unbind())
    }

    fn sentinel_masters(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        get_runtime().spawn(async move {
            let r: Result<Vec<Vec<(String, String)>>, _> =
                with_sentinel(&urls, |mut conn| async move {
                    redis::cmd("SENTINEL")
                        .arg("masters")
                        .query_async(&mut conn)
                        .await
                })
                .await;
            let raw = match r {
                Ok(rows) => RawResult::Value(redis::Value::Array(
                    rows.into_iter()
                        .map(|row| {
                            redis::Value::Array(
                                row.into_iter()
                                    .flat_map(|(k, v)| {
                                        vec![
                                            redis::Value::BulkString(k.into_bytes()),
                                            redis::Value::BulkString(v.into_bytes()),
                                        ]
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                )),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(aw.into_pyobject(py)?.into_any().unbind())
    }
}

/// Build an AsyncRedis instance backed by a sentinel connection.
fn build_async_redis(
    py: Python<'_>,
    inner: &SentinelInner,
    connection_kwargs: &Py<PyDict>,
    service_name: String,
    is_slave: bool,
    per_call: Option<&Bound<'_, PyDict>>,
) -> PyResult<Py<crate::facade::asyncio_mod::AsyncRedis>> {
    let merged = PyDict::new(py);
    let conn_kw = connection_kwargs.bind(py);
    for (k, v) in conn_kw.iter() {
        merged.set_item(k, v)?;
    }
    if let Some(kw) = per_call {
        for (k, v) in kw.iter() {
            merged.set_item(k, v)?;
        }
    }
    let db: i64 = match merged.get_item("db")? {
        Some(v) => v.extract().unwrap_or(0),
        None => 0,
    };

    let urls: Vec<String> = inner.urls.iter().cloned().collect();
    let service_clone = service_name.clone();
    let conn: Result<ValkeyConn, String> = py.detach(|| {
        get_runtime().block_on(async {
            connect_sentinel(urls, &service_clone, db, is_slave, None, None).await
        })
    });
    match conn {
        Ok(c) => {
            let url = inner
                .urls
                .first()
                .cloned()
                .unwrap_or_else(|| format!("sentinel://{service_name}"));
            Py::new(
                py,
                crate::facade::asyncio_mod::AsyncRedis {
                    connection: c,
                    url,
                    closed: false,
                    decode: None,
                },
            )
        }
        Err(e) => Err(crate::errors::to_py_err(redis::RedisError::from((
            redis::ErrorKind::Io,
            "connect_sentinel",
            e,
        )))),
    }
}

// =========================================================================
// Module registration
// =========================================================================

/// Register `Sentinel` on `_driver.sentinel`.
pub fn register_sync(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "sentinel")?;
    m.add_class::<Sentinel>()?;
    parent.add_submodule(&m)?;

    let sys = py.import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("redis_rs_py._driver.sentinel", &m)?;

    Ok(())
}

/// Register `AsyncSentinel` on `_driver.asyncio.sentinel`.
pub fn register_async(py: Python<'_>, parent_asyncio: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "sentinel")?;
    m.add_class::<AsyncSentinel>()?;
    parent_asyncio.add_submodule(&m)?;

    let sys = py.import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("redis_rs_py._driver.asyncio.sentinel", &m)?;

    Ok(())
}
