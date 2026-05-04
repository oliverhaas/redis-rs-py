// RedisCluster + AsyncRedisCluster pyclasses (Plan 15).
//
// Mirrors `redis.cluster.RedisCluster` / `redis.asyncio.cluster.RedisCluster`.
// Both pyclasses hold a `ValkeyConn` with a `ValkeyConnInner::Cluster` arm.
// Single-key commands forward through the same `sync_op!`/`async_op!` macros
// used by `Redis`/`AsyncRedis` — redis-rs cluster_async routes MOVED/ASK
// transparently. Multi-key commands that may straddle slots fan out per-slot
// via `tokio::task::JoinSet`. Cluster admin commands wrap `CLUSTER <sub>`.

#![allow(clippy::too_many_arguments)]

use std::sync::OnceLock;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use crate::async_bridge::{RawResult, RedisRsAwaitable};
use crate::connection::{TlsOpts, ValkeyConn, connect_cluster};
use crate::exceptions::DataError;
use crate::facade::kwargs::accept_and_warn;
use crate::runtime::get_runtime;

// =========================================================================
// ClusterNode — mirrors redis.cluster.ClusterNode
// =========================================================================

#[pyclass(module = "redis_rs_py.cluster", frozen, from_py_object)]
#[derive(Clone)]
pub struct ClusterNode {
    #[pyo3(get)]
    host: String,
    #[pyo3(get)]
    port: u16,
}

#[pymethods]
impl ClusterNode {
    #[new]
    #[pyo3(signature = (host, port, server_type = None, redis_connection = None))]
    fn new(
        host: String,
        port: u16,
        server_type: Option<&str>,
        redis_connection: Option<&Bound<'_, PyAny>>,
    ) -> Self {
        let _ = (server_type, redis_connection);
        ClusterNode { host, port }
    }

    fn __repr__(&self) -> String {
        format!("ClusterNode(host={:?}, port={})", self.host, self.port)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.host == other.host && self.port == other.port
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        self.host.hash(&mut h);
        self.port.hash(&mut h);
        h.finish()
    }
}

// =========================================================================
// ClusterPubSub — stub (real wiring deferred until plan 14 bridge is ready)
// =========================================================================

#[pyclass(module = "redis_rs_py.cluster")]
pub struct ClusterPubSub {
    #[allow(dead_code)]
    master_urls: Vec<String>,
}

impl ClusterPubSub {
    pub fn new(master_urls: Vec<String>) -> Self {
        ClusterPubSub { master_urls }
    }
}

#[pymethods]
impl ClusterPubSub {
    fn subscribe(&self, _channels: Vec<String>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "ClusterPubSub.subscribe: cluster pub/sub support is deferred.",
        ))
    }

    fn psubscribe(&self, _patterns: Vec<String>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "ClusterPubSub.psubscribe: cluster pub/sub support is deferred.",
        ))
    }

    fn ssubscribe(&self, _channels: Vec<String>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "ClusterPubSub.ssubscribe: cluster pub/sub support is deferred.",
        ))
    }

    fn sunsubscribe(&self, _channels: Vec<String>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "ClusterPubSub.sunsubscribe: cluster pub/sub support is deferred.",
        ))
    }

    #[pyo3(signature = (timeout = None, ignore_subscribe_messages = false))]
    fn get_message(
        &self,
        timeout: Option<f64>,
        ignore_subscribe_messages: bool,
    ) -> PyResult<Py<PyAny>> {
        let _ = (timeout, ignore_subscribe_messages);
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "ClusterPubSub.get_message: cluster pub/sub support is deferred.",
        ))
    }

    fn close(&self) -> PyResult<()> {
        Ok(())
    }
}

// =========================================================================
// Cluster kwargs accept list
// =========================================================================

const CLUSTER_KWARGS_ACCEPT: &[&str] = &[
    "host",
    "port",
    "startup_nodes",
    "url",
    "username",
    "password",
    "db",
    "decode_responses",
    "encoding",
    "encoding_errors",
    "ssl",
    "ssl_ca_certs",
    "ssl_certfile",
    "ssl_keyfile",
    "ssl_cert_reqs",
    "ssl_check_hostname",
    "socket_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_keepalive_options",
    "max_connections_per_node",
    "client_name",
    "cluster_error_retry_attempts",
    "connection_error_retry_attempts",
    "read_from_replicas",
    "dynamic_startup_nodes",
    "require_full_coverage",
    "reinitialize_steps",
    "load_balancing_strategy",
    "address_remap",
    "cache_max_size",
    "cache_ttl_secs",
    "retry",
    "cache",
    "cache_config",
    "event_dispatcher",
];

static CACHE_ON_CLUSTER_WARNED: OnceLock<()> = OnceLock::new();

fn warn_cache_on_cluster(py: Python<'_>) -> PyResult<()> {
    if CACHE_ON_CLUSTER_WARNED.set(()).is_err() {
        return Ok(());
    }
    let warnings = py.import("warnings")?;
    warnings.call_method1(
        "warn",
        (
            "redis-rs-py: client-side caching is not supported on cluster connections \
             (redis-rs cluster_async has no CacheConfig hook). \
             cache_max_size / cache_ttl_secs are ignored.",
            py.get_type::<pyo3::exceptions::PyUserWarning>(),
        ),
    )?;
    Ok(())
}

// =========================================================================
// Connection builder helpers
// =========================================================================

/// Build the startup-URL list from any combination of (host/port, startup_nodes, url).
fn resolve_startup_urls(
    py: Python<'_>,
    host: Option<&str>,
    port: u16,
    startup_nodes: Option<&Bound<'_, PyList>>,
    url: Option<&str>,
) -> PyResult<Vec<String>> {
    if let Some(u) = url {
        return Ok(vec![u.to_string()]);
    }
    if let Some(nodes) = startup_nodes {
        let mut out = Vec::with_capacity(nodes.len());
        for item in nodes.iter() {
            let cn: PyRef<ClusterNode> = item.extract()?;
            out.push(format!("redis://{}:{}", cn.host, cn.port));
        }
        if !out.is_empty() {
            return Ok(out);
        }
    }
    if let Some(h) = host {
        return Ok(vec![format!("redis://{h}:{port}")]);
    }
    let _ = py;
    Err(PyErr::new::<DataError, _>(
        "RedisCluster requires one of: host=, url=, or startup_nodes=[ClusterNode(...)]",
    ))
}

fn inject_userinfo(url: &str, user: Option<&str>, pw: Option<&str>) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some(x) => x,
        None => return url.to_string(),
    };
    let userinfo = match (user, pw) {
        (Some(u), Some(p)) => format!("{u}:{p}@"),
        (Some(u), None) => format!("{u}@"),
        (None, Some(p)) => format!(":{p}@"),
        (None, None) => String::new(),
    };
    format!("{scheme}://{userinfo}{rest}")
}

fn build_cluster_conn(
    py: Python<'_>,
    host: Option<String>,
    port: u16,
    startup_nodes: Option<&Bound<'_, PyList>>,
    url: Option<String>,
    ssl_ca_certs: Option<Vec<u8>>,
    ssl_certfile: Option<Vec<u8>>,
    ssl_keyfile: Option<Vec<u8>>,
    username: Option<String>,
    password: Option<String>,
    cache_max_size: Option<usize>,
    cache_ttl_secs: Option<u64>,
    kwargs: Option<&Bound<'_, PyDict>>,
    cluster_error_retry_attempts: u32,
    connection_error_retry_attempts: u32,
    read_from_replicas: bool,
    dynamic_startup_nodes: bool,
    ssl: bool,
    socket_timeout: Option<f64>,
    socket_connect_timeout: Option<f64>,
    max_connections_per_node: Option<usize>,
    client_name: Option<String>,
) -> PyResult<(ValkeyConn, String)> {
    let _ = (
        cluster_error_retry_attempts,
        connection_error_retry_attempts,
        read_from_replicas,
        dynamic_startup_nodes,
        ssl,
        socket_timeout,
        socket_connect_timeout,
        max_connections_per_node,
        client_name,
    );

    if cache_max_size.is_some() || cache_ttl_secs.is_some() {
        warn_cache_on_cluster(py)?;
    }
    if let Some(extra) = kwargs {
        accept_and_warn(py, CLUSTER_KWARGS_ACCEPT, Some(extra))?;
    }

    let mut urls = resolve_startup_urls(py, host.as_deref(), port, startup_nodes, url.as_deref())?;

    if username.is_some() || password.is_some() {
        urls = urls
            .into_iter()
            .map(|u| inject_userinfo(&u, username.as_deref(), password.as_deref()))
            .collect();
    }

    let primary_url = urls.first().cloned().unwrap_or_default();

    let tls_opts = if ssl_ca_certs.is_some() || ssl_certfile.is_some() || ssl_keyfile.is_some() {
        Some(TlsOpts {
            root_cert: ssl_ca_certs,
            client_cert: ssl_certfile,
            client_key: ssl_keyfile,
        })
    } else {
        None
    };

    let conn =
        py.detach(|| get_runtime().block_on(async { connect_cluster(urls, tls_opts).await }));

    match conn {
        Ok(c) => Ok((c, primary_url)),
        Err(e) => Err(crate::errors::to_py_err(redis::RedisError::from((
            redis::ErrorKind::Io,
            "connect_cluster",
            e,
        )))),
    }
}

// =========================================================================
// CRC16 slot computation (Redis cluster spec)
// =========================================================================

/// Compute the Redis cluster slot for a key using CRC16-XMODEM, honouring
/// `{hashtag}` braces. This is the canonical algorithm from the Redis cluster
/// spec (https://redis.io/docs/reference/cluster-spec/).
fn cluster_slot(key: &[u8]) -> u16 {
    // Honour {hashtag} braces: if the key contains '{' followed later by '}',
    // only hash the content between them (if non-empty).
    let target: &[u8] = {
        let start = key.iter().position(|&b| b == b'{');
        let end = key.iter().position(|&b| b == b'}');
        match (start, end) {
            (Some(s), Some(e)) if e > s + 1 => &key[s + 1..e],
            _ => key,
        }
    };
    crc16_xmodem(target) % 16384
}

fn crc16_xmodem(data: &[u8]) -> u16 {
    const POLY: u16 = 0x1021;
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ POLY;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

// =========================================================================
// Cross-slot fan-out helpers
// =========================================================================

/// Group keys by slot. Returns a HashMap<slot, Vec<(original_index, key)>>.
fn group_by_slot(keys: &[String]) -> std::collections::HashMap<u16, Vec<(usize, String)>> {
    let mut map: std::collections::HashMap<u16, Vec<(usize, String)>> =
        std::collections::HashMap::new();
    for (idx, k) in keys.iter().enumerate() {
        let slot = cluster_slot(k.as_bytes());
        map.entry(slot).or_default().push((idx, k.clone()));
    }
    map
}

async fn fanout_mget(
    conn: ValkeyConn,
    keys: Vec<String>,
) -> redis::RedisResult<Vec<Option<Vec<u8>>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let groups = group_by_slot(&keys);
    let mut out: Vec<Option<Vec<u8>>> = vec![None; keys.len()];
    #[allow(clippy::type_complexity)]
    let mut set: tokio::task::JoinSet<redis::RedisResult<(Vec<usize>, Vec<Option<Vec<u8>>>)>> =
        tokio::task::JoinSet::new();
    for (_, entries) in groups {
        let mut conn = conn.clone();
        set.spawn(async move {
            let (indices, slot_keys): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
            let mut cmd = redis::cmd("MGET");
            for k in &slot_keys {
                cmd.arg(k);
            }
            let result: Vec<Option<Vec<u8>>> = crate::dispatch_cmd!(&mut *conn, cmd)?;
            Ok((indices, result))
        });
    }
    while let Some(joined) = set.join_next().await {
        let (indices, values) = joined
            .map_err(|e| {
                redis::RedisError::from((redis::ErrorKind::Io, "fanout join", e.to_string()))
            })
            .and_then(|r| r)?;
        for (i, v) in indices.into_iter().zip(values) {
            out[i] = v;
        }
    }
    Ok(out)
}

async fn fanout_mset(conn: ValkeyConn, pairs: Vec<(String, Vec<u8>)>) -> redis::RedisResult<()> {
    if pairs.is_empty() {
        return Ok(());
    }
    let mut groups: std::collections::HashMap<u16, Vec<(String, Vec<u8>)>> =
        std::collections::HashMap::new();
    for (k, v) in pairs {
        let slot = cluster_slot(k.as_bytes());
        groups.entry(slot).or_default().push((k, v));
    }
    let mut set: tokio::task::JoinSet<redis::RedisResult<()>> = tokio::task::JoinSet::new();
    for (_, group) in groups {
        let mut conn = conn.clone();
        set.spawn(async move {
            let mut cmd = redis::cmd("MSET");
            for (k, v) in &group {
                cmd.arg(k).arg(v.as_slice());
            }
            crate::dispatch_cmd!(&mut *conn, cmd)
        });
    }
    while let Some(joined) = set.join_next().await {
        joined
            .map_err(|e| {
                redis::RedisError::from((redis::ErrorKind::Io, "fanout join", e.to_string()))
            })
            .and_then(|r| r)?;
    }
    Ok(())
}

async fn fanout_del(conn: ValkeyConn, keys: Vec<String>, unlink: bool) -> redis::RedisResult<i64> {
    if keys.is_empty() {
        return Ok(0);
    }
    let groups = group_by_slot(&keys);
    let cmd_name = if unlink { "UNLINK" } else { "DEL" };
    let mut set: tokio::task::JoinSet<redis::RedisResult<i64>> = tokio::task::JoinSet::new();
    for (_, entries) in groups {
        let mut conn = conn.clone();
        let cmd_str = cmd_name;
        set.spawn(async move {
            let mut cmd = redis::cmd(cmd_str);
            for (_, k) in &entries {
                cmd.arg(k);
            }
            crate::dispatch_cmd!(&mut *conn, cmd)
        });
    }
    let mut total = 0i64;
    while let Some(joined) = set.join_next().await {
        let n = joined
            .map_err(|e| {
                redis::RedisError::from((redis::ErrorKind::Io, "fanout join", e.to_string()))
            })
            .and_then(|r| r)?;
        total += n;
    }
    Ok(total)
}

async fn fanout_exists(conn: ValkeyConn, keys: Vec<String>) -> redis::RedisResult<i64> {
    if keys.is_empty() {
        return Ok(0);
    }
    let groups = group_by_slot(&keys);
    let mut set: tokio::task::JoinSet<redis::RedisResult<i64>> = tokio::task::JoinSet::new();
    for (_, entries) in groups {
        let mut conn = conn.clone();
        set.spawn(async move {
            let mut cmd = redis::cmd("EXISTS");
            for (_, k) in &entries {
                cmd.arg(k);
            }
            crate::dispatch_cmd!(&mut *conn, cmd)
        });
    }
    let mut total = 0i64;
    while let Some(joined) = set.join_next().await {
        total += joined
            .map_err(|e| {
                redis::RedisError::from((redis::ErrorKind::Io, "fanout join", e.to_string()))
            })
            .and_then(|r| r)?;
    }
    Ok(total)
}

// =========================================================================
// RedisCluster — sync façade
// =========================================================================

#[pyclass(module = "redis_rs_py.cluster", name = "RedisCluster")]
pub struct RedisCluster {
    pub(crate) connection: ValkeyConn,
    pub(crate) url: String,
    pub(crate) closed: bool,
    pub(crate) decode: Option<crate::facade::decode::DecodeOpts>,
}

impl RedisCluster {
    pub(crate) fn maybe_decode(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        match &self.decode {
            Some(opts) => crate::facade::decode::decode_walk(py, value.bind(py), opts),
            None => Ok(value),
        }
    }
}

#[pymethods]
impl RedisCluster {
    #[new]
    #[pyo3(signature = (
        host = None,
        port = 6379,
        *,
        startup_nodes = None,
        url = None,
        cluster_error_retry_attempts = 3,
        connection_error_retry_attempts = 3,
        read_from_replicas = false,
        dynamic_startup_nodes = true,
        decode_responses = false,
        encoding = "utf-8".to_string(),
        encoding_errors = "strict".to_string(),
        ssl = false,
        ssl_ca_certs = None,
        ssl_certfile = None,
        ssl_keyfile = None,
        username = None,
        password = None,
        socket_timeout = None,
        socket_connect_timeout = None,
        max_connections_per_node = None,
        client_name = None,
        cache_max_size = None,
        cache_ttl_secs = None,
        **kwargs
    ))]
    fn new(
        py: Python<'_>,
        host: Option<String>,
        port: u16,
        startup_nodes: Option<&Bound<'_, PyList>>,
        url: Option<String>,
        cluster_error_retry_attempts: u32,
        connection_error_retry_attempts: u32,
        read_from_replicas: bool,
        dynamic_startup_nodes: bool,
        decode_responses: bool,
        encoding: String,
        encoding_errors: String,
        ssl: bool,
        ssl_ca_certs: Option<Vec<u8>>,
        ssl_certfile: Option<Vec<u8>>,
        ssl_keyfile: Option<Vec<u8>>,
        username: Option<String>,
        password: Option<String>,
        socket_timeout: Option<f64>,
        socket_connect_timeout: Option<f64>,
        max_connections_per_node: Option<usize>,
        client_name: Option<String>,
        cache_max_size: Option<usize>,
        cache_ttl_secs: Option<u64>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let (conn, primary_url) = build_cluster_conn(
            py,
            host,
            port,
            startup_nodes,
            url,
            ssl_ca_certs,
            ssl_certfile,
            ssl_keyfile,
            username,
            password,
            cache_max_size,
            cache_ttl_secs,
            kwargs,
            cluster_error_retry_attempts,
            connection_error_retry_attempts,
            read_from_replicas,
            dynamic_startup_nodes,
            ssl,
            socket_timeout,
            socket_connect_timeout,
            max_connections_per_node,
            client_name,
        )?;
        let decode = if decode_responses {
            Some(crate::facade::decode::DecodeOpts::new(
                encoding,
                encoding_errors,
            ))
        } else {
            None
        };
        Ok(RedisCluster {
            connection: conn,
            url: primary_url,
            closed: false,
            decode,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (url, **kwargs))]
    fn from_url(
        py: Python<'_>,
        url: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let combined = PyDict::new(py);
        combined.set_item("url", url)?;
        if let Some(extra) = kwargs {
            for (k, v) in extra.iter() {
                combined.set_item(k, v)?;
            }
        }
        let cls = py.get_type::<RedisCluster>();
        cls.call((), Some(&combined)).map(Bound::unbind)
    }

    fn close(&mut self) -> PyResult<()> {
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

    // =========================================================================
    // Core commands
    // =========================================================================

    fn ping(&self, py: Python<'_>) -> PyResult<bool> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<String> = py.detach(|| {
            get_runtime().block_on(async move {
                let cmd = redis::cmd("PING");
                crate::dispatch_cmd!(&mut *conn, cmd)
            })
        });
        result
            .map(|s| s == "PONG")
            .map_err(crate::errors::to_py_err)
    }

    fn get(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        use redis::AsyncCommands;
        let result: redis::RedisResult<Option<Vec<u8>>> = py.detach(|| {
            let mut conn = self.connection.clone();
            get_runtime()
                .block_on(async move { crate::conn_method!(&mut *conn, c, c.get(key.as_str())) })
        });
        let v = result.map_err(crate::errors::to_py_err)?;
        let py_val = crate::helpers::py_opt_bytes(py, v);
        self.maybe_decode(py, py_val)
    }

    #[pyo3(signature = (key, value, ex=None, px=None, exat=None, pxat=None, nx=false, xx=false, keepttl=false, get=false))]
    fn set(
        &self,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
    ) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<redis::Value> = py.detach(|| {
            get_runtime().block_on(async move {
                conn.set_full(&key, value, ex, px, exat, pxat, nx, xx, keepttl, get)
                    .await
            })
        });
        let raw = result.map_err(crate::errors::to_py_err)?;
        let py_val =
            crate::raw_result::IntoRawResult::into_raw_result(Ok::<_, redis::RedisError>(raw))
                .into_py(py)?;
        self.maybe_decode(py, py_val)
    }

    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { fanout_del(conn, keys, false).await }))
            .map_err(crate::errors::to_py_err)
    }

    #[pyo3(signature = (*keys))]
    fn exists(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { fanout_exists(conn, keys).await }))
            .map_err(crate::errors::to_py_err)
    }

    #[pyo3(signature = (*keys))]
    fn unlink(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { fanout_del(conn, keys, true).await }))
            .map_err(crate::errors::to_py_err)
    }

    /// Multi-key MGET with cross-slot **fan-out**: keys are grouped by
    /// cluster slot, one MGET round-trip is issued per unique slot in
    /// parallel via `tokio::task::JoinSet`, and results are re-ordered to
    /// match the input.
    ///
    /// This is **not** atomic across slots.
    fn mget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Option<Vec<u8>>>> =
            py.detach(|| get_runtime().block_on(async move { fanout_mget(conn, keys).await }));
        let items = result.map_err(crate::errors::to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|opt| crate::helpers::py_opt_bytes(py, opt))
            .collect();
        Ok(PyList::new(py, py_items)?.into_any().unbind())
    }

    /// Multi-key MSET with cross-slot **fan-out**: entries are grouped by
    /// cluster slot, one MSET per slot dispatched in parallel.
    fn mset(&self, py: Python<'_>, mapping: &Bound<'_, PyDict>) -> PyResult<()> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut pairs: Vec<(String, Vec<u8>)> = Vec::with_capacity(mapping.len());
        for (k, v) in mapping.iter() {
            let key: String = k.extract()?;
            let value: Vec<u8> = v.extract()?;
            pairs.push((key, value));
        }
        let conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { fanout_mset(conn, pairs).await }))
            .map_err(crate::errors::to_py_err)
    }

    // =========================================================================
    // String commands
    // =========================================================================

    fn incr(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.incr(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn incrby(&self, py: Python<'_>, key: String, amount: i64) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.incrby(&key, amount).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn incrbyfloat(&self, py: Python<'_>, key: String, amount: f64) -> PyResult<f64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.incrbyfloat(&key, amount).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn decr(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.decr(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn decrby(&self, py: Python<'_>, key: String, amount: i64) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.decrby(&key, amount).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn strlen(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.strlen(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn getdel(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Option<Vec<u8>>> =
            py.detach(|| get_runtime().block_on(async move { conn.getdel(&key).await }));
        let v = result.map_err(crate::errors::to_py_err)?;
        self.maybe_decode(py, crate::helpers::py_opt_bytes(py, v))
    }

    fn append(&self, py: Python<'_>, key: String, value: Vec<u8>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.append(&key, &value).await }))
            .map_err(crate::errors::to_py_err)
    }

    // =========================================================================
    // Key-space commands
    // =========================================================================

    #[pyo3(name = "ttl")]
    fn ttl_(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.ttl(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn pttl(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.pttl(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn expire(&self, py: Python<'_>, key: String, seconds: i64) -> PyResult<bool> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move {
                conn.expire_full(&key, seconds, false, false, false, false)
                    .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn pexpire(&self, py: Python<'_>, key: String, milliseconds: i64) -> PyResult<bool> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move {
                conn.pexpire_full(&key, milliseconds, false, false, false, false)
                    .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn persist(&self, py: Python<'_>, key: String) -> PyResult<bool> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.persist(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    #[pyo3(name = "type")]
    fn type_(&self, py: Python<'_>, key: String) -> PyResult<String> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.key_type(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn rename(&self, py: Python<'_>, src: String, dst: String) -> PyResult<()> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.rename(&src, &dst).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn keys(&self, py: Python<'_>, pattern: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Vec<u8>>> = py.detach(|| {
            get_runtime().block_on(async move {
                let mut cmd = redis::cmd("KEYS");
                cmd.arg(pattern.as_str());
                crate::dispatch_cmd!(&mut *conn, cmd)
            })
        });
        let items = result.map_err(crate::errors::to_py_err)?;
        crate::helpers::py_bytes_list(py, items)
    }

    // =========================================================================
    // Hash commands
    // =========================================================================

    fn hset(&self, py: Python<'_>, key: String, field: String, value: Vec<u8>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move { conn.hset_multiple(&key, &[(field, value)]).await })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn hget(&self, py: Python<'_>, key: String, field: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Option<Vec<u8>>> =
            py.detach(|| get_runtime().block_on(async move { conn.hget(&key, &field).await }));
        let v = result.map_err(crate::errors::to_py_err)?;
        self.maybe_decode(py, crate::helpers::py_opt_bytes(py, v))
    }

    fn hgetall(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<(Vec<u8>, Vec<u8>)>> =
            py.detach(|| get_runtime().block_on(async move { conn.hgetall(&key).await }));
        let pairs = result.map_err(crate::errors::to_py_err)?;
        crate::helpers::py_bytes_pairs(py, pairs)
    }

    fn hkeys(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Vec<u8>>> =
            py.detach(|| get_runtime().block_on(async move { conn.hkeys(&key).await }));
        crate::helpers::py_bytes_list(py, result.map_err(crate::errors::to_py_err)?)
    }

    fn hvals(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Vec<u8>>> =
            py.detach(|| get_runtime().block_on(async move { conn.hvals(&key).await }));
        crate::helpers::py_bytes_list(py, result.map_err(crate::errors::to_py_err)?)
    }

    fn hlen(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.hlen(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn hdel(&self, py: Python<'_>, key: String, fields: Vec<String>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.hdel(&key, &fields).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn hexists(&self, py: Python<'_>, key: String, field: String) -> PyResult<bool> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.hexists(&key, &field).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn hincrby(&self, py: Python<'_>, key: String, field: String, amount: i64) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move { conn.hincrby(&key, &field, amount).await })
        })
        .map_err(crate::errors::to_py_err)
    }

    // =========================================================================
    // List commands
    // =========================================================================

    #[pyo3(signature = (key, *values))]
    fn rpush(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.rpush(&key, &values).await }))
            .map_err(crate::errors::to_py_err)
    }

    #[pyo3(signature = (key, *values))]
    fn lpush(&self, py: Python<'_>, key: String, values: Vec<Vec<u8>>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.lpush(&key, &values).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn lrange(&self, py: Python<'_>, key: String, start: i64, stop: i64) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Vec<u8>>> = py
            .detach(|| get_runtime().block_on(async move { conn.lrange(&key, start, stop).await }));
        crate::helpers::py_bytes_list(py, result.map_err(crate::errors::to_py_err)?)
    }

    fn llen(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.llen(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn lpop(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Option<Vec<u8>>> =
            py.detach(|| get_runtime().block_on(async move { conn.lpop_one(&key).await }));
        self.maybe_decode(
            py,
            crate::helpers::py_opt_bytes(py, result.map_err(crate::errors::to_py_err)?),
        )
    }

    fn rpop(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Option<Vec<u8>>> =
            py.detach(|| get_runtime().block_on(async move { conn.rpop_one(&key).await }));
        self.maybe_decode(
            py,
            crate::helpers::py_opt_bytes(py, result.map_err(crate::errors::to_py_err)?),
        )
    }

    // =========================================================================
    // Set commands
    // =========================================================================

    fn sadd(&self, py: Python<'_>, key: String, members: Vec<Vec<u8>>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.sadd(&key, &members).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn srem(&self, py: Python<'_>, key: String, members: Vec<Vec<u8>>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.srem(&key, &members).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn smembers(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Vec<u8>>> =
            py.detach(|| get_runtime().block_on(async move { conn.smembers(&key).await }));
        crate::helpers::py_bytes_list(py, result.map_err(crate::errors::to_py_err)?)
    }

    fn scard(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.scard(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn sismember(&self, py: Python<'_>, key: String, member: Vec<u8>) -> PyResult<bool> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.sismember(&key, &member).await }))
            .map_err(crate::errors::to_py_err)
    }

    // =========================================================================
    // Sorted set commands
    // =========================================================================

    fn zadd(&self, py: Python<'_>, key: String, mapping: &Bound<'_, PyDict>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut pairs: Vec<(f64, Vec<u8>)> = Vec::with_capacity(mapping.len());
        for (member, score) in mapping.iter() {
            let s: f64 = score.extract()?;
            let m: Vec<u8> = member.extract()?;
            pairs.push((s, m));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move {
                if pairs.is_empty() {
                    return Ok(0i64);
                }
                let mut cmd = redis::cmd("ZADD");
                cmd.arg(key.as_str());
                for (score, member) in &pairs {
                    cmd.arg(score).arg(member.as_slice());
                }
                crate::dispatch_cmd!(&mut *conn, cmd)
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn zrange(&self, py: Python<'_>, key: String, start: i64, stop: i64) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Vec<u8>>> = py.detach(|| {
            get_runtime().block_on(async move {
                let mut cmd = redis::cmd("ZRANGE");
                cmd.arg(key.as_str()).arg(start).arg(stop);
                crate::dispatch_cmd!(&mut *conn, cmd)
            })
        });
        crate::helpers::py_bytes_list(py, result.map_err(crate::errors::to_py_err)?)
    }

    fn zcard(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move {
                let mut cmd = redis::cmd("ZCARD");
                cmd.arg(key.as_str());
                crate::dispatch_cmd!(&mut *conn, cmd)
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn zrem(&self, py: Python<'_>, key: String, members: Vec<Vec<u8>>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move {
                let mut cmd = redis::cmd("ZREM");
                cmd.arg(key.as_str());
                for m in &members {
                    cmd.arg(m.as_slice());
                }
                crate::dispatch_cmd!(&mut *conn, cmd)
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn zscore(&self, py: Python<'_>, key: String, member: Vec<u8>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Option<f64>> = py.detach(|| {
            get_runtime().block_on(async move {
                let mut cmd = redis::cmd("ZSCORE");
                cmd.arg(key.as_str()).arg(member.as_slice());
                crate::dispatch_cmd!(&mut *conn, cmd)
            })
        });
        let v = result.map_err(crate::errors::to_py_err)?;
        Ok(match v {
            Some(f) => f.into_pyobject(py)?.into_any().unbind(),
            None => py.None(),
        })
    }

    // =========================================================================
    // Pub/Sub
    // =========================================================================

    fn publish(&self, py: Python<'_>, channel: String, message: Vec<u8>) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move {
                let mut cmd = redis::cmd("PUBLISH");
                cmd.arg(channel.as_str()).arg(message.as_slice());
                crate::dispatch_cmd!(&mut *conn, cmd)
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn pubsub(&self, _py: Python<'_>) -> PyResult<ClusterPubSub> {
        Ok(ClusterPubSub::new(vec![self.url.clone()]))
    }

    // =========================================================================
    // Cluster admin commands
    // =========================================================================

    fn cluster_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<u8>> =
            py.detach(|| get_runtime().block_on(async move { conn.cluster_info().await }));
        let v = result.map_err(crate::errors::to_py_err)?;
        Ok(PyBytes::new(py, &v).into_any().unbind())
    }

    fn cluster_nodes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<u8>> =
            py.detach(|| get_runtime().block_on(async move { conn.cluster_nodes().await }));
        let v = result.map_err(crate::errors::to_py_err)?;
        Ok(PyBytes::new(py, &v).into_any().unbind())
    }

    fn cluster_slots(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<redis::Value> =
            py.detach(|| get_runtime().block_on(async move { conn.cluster_slots().await }));
        let v = result.map_err(crate::errors::to_py_err)?;
        RawResult::Value(v).into_py(py)
    }

    fn cluster_shards(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<redis::Value> =
            py.detach(|| get_runtime().block_on(async move { conn.cluster_shards().await }));
        let v = result.map_err(crate::errors::to_py_err)?;
        RawResult::Value(v).into_py(py)
    }

    fn cluster_myid(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<String> =
            py.detach(|| get_runtime().block_on(async move { conn.cluster_myid().await }));
        let s = result.map_err(crate::errors::to_py_err)?;
        Ok(PyBytes::new(py, s.as_bytes()).into_any().unbind())
    }

    fn cluster_keyslot(&self, py: Python<'_>, key: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.cluster_keyslot(&key).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn cluster_countkeysinslot(&self, py: Python<'_>, slot: u16) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move { conn.cluster_countkeysinslot(slot).await })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn cluster_getkeysinslot(&self, py: Python<'_>, slot: u16, count: u32) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Vec<u8>>> = py.detach(|| {
            get_runtime().block_on(async move { conn.cluster_getkeysinslot(slot, count).await })
        });
        crate::helpers::py_bytes_list(py, result.map_err(crate::errors::to_py_err)?)
    }

    fn cluster_meet(&self, py: Python<'_>, ip: String, port: u16) -> PyResult<()> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.cluster_meet(&ip, port).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn cluster_forget(&self, py: Python<'_>, node_id: String) -> PyResult<()> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.cluster_forget(&node_id).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn cluster_replicate(&self, py: Python<'_>, node_id: String) -> PyResult<()> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.cluster_replicate(&node_id).await }))
            .map_err(crate::errors::to_py_err)
    }

    #[pyo3(signature = (mode = "SOFT".to_string()))]
    fn cluster_reset(&self, py: Python<'_>, mode: String) -> PyResult<()> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let hard = mode.eq_ignore_ascii_case("HARD");
        let mut conn = self.connection.clone();
        py.detach(|| get_runtime().block_on(async move { conn.cluster_reset(hard).await }))
            .map_err(crate::errors::to_py_err)
    }

    fn cluster_count_failure_reports(&self, py: Python<'_>, node_id: String) -> PyResult<i64> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime()
                .block_on(async move { conn.cluster_count_failure_reports(&node_id).await })
        })
        .map_err(crate::errors::to_py_err)
    }

    #[pyo3(signature = (option = None))]
    fn cluster_failover(&self, py: Python<'_>, option: Option<String>) -> PyResult<()> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        py.detach(|| {
            get_runtime().block_on(async move { conn.cluster_failover(option.as_deref()).await })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn cluster_replicas(&self, py: Python<'_>, node_id: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<Vec<Vec<u8>>> = py.detach(|| {
            get_runtime().block_on(async move { conn.cluster_replicas(&node_id).await })
        });
        crate::helpers::py_bytes_list(py, result.map_err(crate::errors::to_py_err)?)
    }

    fn cluster_slaves(&self, py: Python<'_>, node_id: String) -> PyResult<Py<PyAny>> {
        self.cluster_replicas(py, node_id)
    }

    fn cluster_links(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<redis::Value> =
            py.detach(|| get_runtime().block_on(async move { conn.cluster_links().await }));
        RawResult::Value(result.map_err(crate::errors::to_py_err)?).into_py(py)
    }

    fn cluster_myshardid(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut conn = self.connection.clone();
        let result: redis::RedisResult<String> =
            py.detach(|| get_runtime().block_on(async move { conn.cluster_myshardid().await }));
        let s = result.map_err(crate::errors::to_py_err)?;
        Ok(PyBytes::new(py, s.as_bytes()).into_any().unbind())
    }

    // =========================================================================
    // Introspection
    // =========================================================================

    #[getter]
    fn connection_url(&self) -> &str {
        &self.url
    }
}

// =========================================================================
// AsyncRedisCluster — async sibling
// =========================================================================

#[pyclass(module = "redis_rs_py.asyncio.cluster", name = "AsyncRedisCluster")]
pub struct AsyncRedisCluster {
    pub(crate) connection: ValkeyConn,
    #[allow(dead_code)]
    pub(crate) url: String,
    pub(crate) closed: bool,
    pub(crate) decode: Option<crate::facade::decode::DecodeOpts>,
}

impl AsyncRedisCluster {
    pub(crate) fn maybe_wrap(&self, py: Python<'_>, awaitable: Py<PyAny>) -> PyResult<Py<PyAny>> {
        match &self.decode {
            Some(opts) => crate::facade::decode::wrap_awaitable(py, awaitable, opts),
            None => Ok(awaitable),
        }
    }
}

#[pymethods]
impl AsyncRedisCluster {
    #[new]
    #[pyo3(signature = (
        host = None,
        port = 6379,
        *,
        startup_nodes = None,
        url = None,
        cluster_error_retry_attempts = 3,
        connection_error_retry_attempts = 3,
        read_from_replicas = false,
        dynamic_startup_nodes = true,
        decode_responses = false,
        encoding = "utf-8".to_string(),
        encoding_errors = "strict".to_string(),
        ssl = false,
        ssl_ca_certs = None,
        ssl_certfile = None,
        ssl_keyfile = None,
        username = None,
        password = None,
        socket_timeout = None,
        socket_connect_timeout = None,
        max_connections_per_node = None,
        client_name = None,
        cache_max_size = None,
        cache_ttl_secs = None,
        **kwargs
    ))]
    fn new(
        py: Python<'_>,
        host: Option<String>,
        port: u16,
        startup_nodes: Option<&Bound<'_, PyList>>,
        url: Option<String>,
        cluster_error_retry_attempts: u32,
        connection_error_retry_attempts: u32,
        read_from_replicas: bool,
        dynamic_startup_nodes: bool,
        decode_responses: bool,
        encoding: String,
        encoding_errors: String,
        ssl: bool,
        ssl_ca_certs: Option<Vec<u8>>,
        ssl_certfile: Option<Vec<u8>>,
        ssl_keyfile: Option<Vec<u8>>,
        username: Option<String>,
        password: Option<String>,
        socket_timeout: Option<f64>,
        socket_connect_timeout: Option<f64>,
        max_connections_per_node: Option<usize>,
        client_name: Option<String>,
        cache_max_size: Option<usize>,
        cache_ttl_secs: Option<u64>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let (conn, primary_url) = build_cluster_conn(
            py,
            host,
            port,
            startup_nodes,
            url,
            ssl_ca_certs,
            ssl_certfile,
            ssl_keyfile,
            username,
            password,
            cache_max_size,
            cache_ttl_secs,
            kwargs,
            cluster_error_retry_attempts,
            connection_error_retry_attempts,
            read_from_replicas,
            dynamic_startup_nodes,
            ssl,
            socket_timeout,
            socket_connect_timeout,
            max_connections_per_node,
            client_name,
        )?;
        let decode = if decode_responses {
            Some(crate::facade::decode::DecodeOpts::new(
                encoding,
                encoding_errors,
            ))
        } else {
            None
        };
        Ok(AsyncRedisCluster {
            connection: conn,
            url: primary_url,
            closed: false,
            decode,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (url, **kwargs))]
    fn from_url(
        py: Python<'_>,
        url: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let combined = PyDict::new(py);
        combined.set_item("url", url)?;
        if let Some(extra) = kwargs {
            for (k, v) in extra.iter() {
                combined.set_item(k, v)?;
            }
        }
        let cls = py.get_type::<AsyncRedisCluster>();
        cls.call((), Some(&combined)).map(Bound::unbind)
    }

    fn aclose(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.closed = true;
        let asyncio = py.import("asyncio")?;
        let fut = asyncio.call_method0("Future")?;
        fut.call_method1("set_result", (py.None(),))?;
        Ok(fut.unbind())
    }

    fn __aenter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let asyncio = py.import("asyncio")?;
        let fut = asyncio.call_method0("Future")?;
        fut.call_method1("set_result", (slf,))?;
        Ok(fut.unbind())
    }

    fn __aexit__<'py>(
        &mut self,
        py: Python<'py>,
        _exc_type: &Bound<'py, PyAny>,
        _exc: &Bound<'py, PyAny>,
        _tb: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.closed = true;
        let asyncio = py.import("asyncio")?;
        let fut = asyncio.call_method0("Future")?;
        fut.call_method1("set_result", (py.None(),))?;
        Ok(fut.unbind())
    }

    // =========================================================================
    // Async core commands — return RedisRsAwaitable
    // =========================================================================

    fn ping(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result: redis::RedisResult<String> = {
                let cmd = redis::cmd("PING");
                crate::dispatch_cmd!(&mut *conn, cmd)
            };
            let raw = match result {
                Ok(s) => RawResult::Bool(s == "PONG"),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }

    fn get(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        use redis::AsyncCommands;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result: redis::RedisResult<Option<Vec<u8>>> = {
                let mut c = conn;
                crate::conn_method!(&mut *c, inner, inner.get(key.as_str()))
            };
            let raw = match result {
                Ok(v) => RawResult::OptBytes(v),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }

    #[pyo3(signature = (key, value, ex=None, px=None, exat=None, pxat=None, nx=false, xx=false, keepttl=false, get=false))]
    fn set(
        &self,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
    ) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result = conn
                .set_full(&key, value, ex, px, exat, pxat, nx, xx, keepttl, get)
                .await;
            use crate::raw_result::IntoRawResult;
            let raw = result.map(RawResult::Value).into_raw_result();
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }

    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result = fanout_del(conn, keys, false).await;
            let raw = match result {
                Ok(n) => RawResult::Int(n),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }

    #[pyo3(signature = (*keys))]
    fn exists(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result = fanout_exists(conn, keys).await;
            let raw = match result {
                Ok(n) => RawResult::Int(n),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }

    fn mget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result = fanout_mget(conn, keys).await;
            let raw = match result {
                Ok(items) => RawResult::OptBytesList(items),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }

    fn mset(&self, py: Python<'_>, mapping: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let mut pairs: Vec<(String, Vec<u8>)> = Vec::with_capacity(mapping.len());
        for (k, v) in mapping.iter() {
            let key: String = k.extract()?;
            let value: Vec<u8> = v.extract()?;
            pairs.push((key, value));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result = fanout_mset(conn, pairs).await;
            let raw = match result {
                Ok(()) => RawResult::Nil,
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }

    fn cluster_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result = conn.cluster_info().await;
            let raw = match result {
                Ok(v) => RawResult::OptBytes(Some(v)),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }

    fn cluster_keyslot(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyValueError::new_err("closed"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        get_runtime().spawn(async move {
            let result = conn.cluster_keyslot(&key).await;
            let raw = match result {
                Ok(n) => RawResult::Int(n),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        let aw_py = aw.into_pyobject(py)?.into_any().unbind();
        self.maybe_wrap(py, aw_py)
    }
}

// =========================================================================
// Module registration
// =========================================================================

/// Register cluster classes on `_driver.cluster` submodule.
pub fn register_sync(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "cluster")?;
    m.add_class::<ClusterNode>()?;
    m.add_class::<RedisCluster>()?;
    m.add_class::<ClusterPubSub>()?;
    parent.add_submodule(&m)?;

    let sys = py.import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("redis_rs_py._driver.cluster", &m)?;

    Ok(())
}

/// Register async cluster classes on `_driver.asyncio.cluster` submodule.
pub fn register_async(py: Python<'_>, asyncio_parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "cluster")?;
    m.add_class::<AsyncRedisCluster>()?;
    asyncio_parent.add_submodule(&m)?;

    let sys = py.import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("redis_rs_py._driver.asyncio.cluster", &m)?;

    Ok(())
}
