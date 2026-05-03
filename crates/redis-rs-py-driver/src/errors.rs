// Error classification helpers.
//
// PLACEHOLDER: returns PyConnectionError / PyRuntimeError pairs (matching
// django-cachex). Plan 02 swaps these for the full redis.exceptions
// hierarchy (RedisError, ConnectionError, TimeoutError, ResponseError,
// BusyLoadingError, NoScriptError, ReadOnlyError, etc.).

use pyo3::PyErr;

use crate::async_bridge::RawResult;

pub fn is_connection_error(e: &redis::RedisError) -> bool {
    matches!(
        e.kind(),
        redis::ErrorKind::Io
            | redis::ErrorKind::Server(redis::ServerErrorKind::BusyLoading)
            | redis::ErrorKind::Server(redis::ServerErrorKind::TryAgain)
            | redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly)
    ) || e.is_connection_dropped()
        || e.is_connection_refusal()
        || e.is_timeout()
}

pub fn classify(e: redis::RedisError) -> RawResult {
    if is_connection_error(&e) {
        RawResult::Error(e.to_string())
    } else {
        RawResult::ServerError(e.to_string())
    }
}

pub fn to_py_err(e: redis::RedisError) -> PyErr {
    if is_connection_error(&e) {
        pyo3::exceptions::PyConnectionError::new_err(e.to_string())
    } else {
        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
    }
}
