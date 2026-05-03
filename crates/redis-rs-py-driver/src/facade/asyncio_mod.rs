// Asyncio façade: redis_rs_py.asyncio.Redis.
//
// Mirrors `redis.asyncio.Redis` — same constructor as the sync façade,
// every command method returns a RedisRsAwaitable. The struct owns a
// ValkeyConn directly (no Py-wrapped driver indirection). Command methods
// are added via `#[pymethods] impl AsyncRedis` blocks in each
// `commands/*.rs` file (PyO3 multiple-pymethods feature).

#![allow(clippy::too_many_arguments)]

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};

use crate::connection::ValkeyConn;
use crate::facade::kwargs::{IMPLEMENTED_KWARGS, accept_and_warn};
use crate::facade::sync::{FacadeConfig, build_connection, parse_url};

// =========================================================================
// AsyncRedis pyclass
// =========================================================================

#[pyclass(subclass, module = "redis_rs_py._driver.asyncio", name = "Redis")]
pub struct AsyncRedis {
    pub(crate) connection: ValkeyConn,
    pub(crate) url: String,
    pub(crate) closed: bool,
    pub(crate) decode: Option<crate::facade::decode::DecodeOpts>,
}

impl AsyncRedis {
    /// If decode_responses is on, wrap the awaitable in a coroutine that
    /// awaits then decodes. Otherwise return the awaitable as-is.
    pub(crate) fn maybe_wrap(&self, py: Python<'_>, awaitable: Py<PyAny>) -> PyResult<Py<PyAny>> {
        match &self.decode {
            Some(opts) => crate::facade::decode::wrap_awaitable(py, awaitable, opts),
            None => Ok(awaitable),
        }
    }
}

// =========================================================================
// Constructor, from_url, lifecycle
// =========================================================================

#[pymethods]
impl AsyncRedis {
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

        let sync_redis = build_connection(py, &config)?;
        Ok(Self {
            connection: sync_redis.connection,
            url: sync_redis.url,
            closed: false,
            decode: sync_redis.decode,
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

    fn aclose<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Mark the connection as closed so subsequent calls raise ValueError.
        self.closed = true;
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
        // Mark closed so subsequent calls raise ValueError.
        self.closed = true;
        let asyncio = py.import("asyncio")?;
        let empty: Py<PyAny> = false.into_pyobject(py)?.to_owned().into_any().unbind();
        asyncio.call_method1("sleep", (0.0_f64, empty))
    }

    #[pyo3(signature = (transaction = true, shard_hint = None))]
    fn pipeline(
        &self,
        py: Python<'_>,
        transaction: bool,
        shard_hint: Option<Py<PyAny>>,
    ) -> PyResult<Py<crate::facade::pipeline::AsyncPipeline>> {
        let _ = shard_hint;
        Py::new(
            py,
            crate::facade::pipeline::AsyncPipeline::new(self.connection.clone(), transaction),
        )
    }

    #[pyo3(signature = (**kwargs))]
    fn pubsub(&self, kwargs: Option<Bound<'_, PyDict>>) -> PyResult<Py<PyAny>> {
        let _ = kwargs;
        Err(PyNotImplementedError::new_err(
            "Async PubSub is implemented by plan 14.",
        ))
    }

    #[pyo3(signature = (func, *watches, value_from_callable = false, watch_delay = None, **_kwargs))]
    fn atransaction(
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
        crate::facade::pipeline::atransaction_helper(
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
}

// =========================================================================
// Submodule registration entry point. Called by lib.rs when building
// the asyncio submodule.
// =========================================================================

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<AsyncRedis>()?;
    m.add_class::<crate::facade::pipeline::AsyncPipeline>()?;
    Ok(())
}
