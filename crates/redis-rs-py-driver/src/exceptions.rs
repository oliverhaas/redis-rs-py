// Full redis.exceptions hierarchy as PyO3 types.
//
// Names and inheritance mirror redis-py 5.x (verified against
// `python -c "import redis.exceptions"`). A few classes have multi-base
// inheritance (ClusterDownError, ClusterCrossSlotError); those are
// constructed manually because create_exception! only handles a single
// base.

use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::PyDict;

create_exception!(
    redis_rs_py._driver.exceptions,
    RedisError,
    pyo3::exceptions::PyException
);

create_exception!(redis_rs_py._driver.exceptions, ConnectionError, RedisError);
create_exception!(
    redis_rs_py._driver.exceptions,
    TimeoutError,
    ConnectionError
);
create_exception!(
    redis_rs_py._driver.exceptions,
    BusyLoadingError,
    ConnectionError
);
create_exception!(
    redis_rs_py._driver.exceptions,
    AuthenticationError,
    ConnectionError
);
create_exception!(
    redis_rs_py._driver.exceptions,
    AuthenticationWrongNumberOfArgsError,
    AuthenticationError
);
create_exception!(
    redis_rs_py._driver.exceptions,
    MasterDownError,
    ConnectionError
);

create_exception!(redis_rs_py._driver.exceptions, ResponseError, RedisError);
create_exception!(redis_rs_py._driver.exceptions, DataError, RedisError);
create_exception!(redis_rs_py._driver.exceptions, InvalidResponse, RedisError);

create_exception!(
    redis_rs_py._driver.exceptions,
    OutOfMemoryError,
    ResponseError
);
create_exception!(redis_rs_py._driver.exceptions, NoScriptError, ResponseError);
create_exception!(
    redis_rs_py._driver.exceptions,
    ExecAbortError,
    ResponseError
);
create_exception!(redis_rs_py._driver.exceptions, ReadOnlyError, ResponseError);
create_exception!(
    redis_rs_py._driver.exceptions,
    NoPermissionError,
    ResponseError
);
create_exception!(redis_rs_py._driver.exceptions, ModuleError, ResponseError);

create_exception!(redis_rs_py._driver.exceptions, LockError, RedisError);
create_exception!(redis_rs_py._driver.exceptions, LockNotOwnedError, LockError);
create_exception!(redis_rs_py._driver.exceptions, WatchError, RedisError);
create_exception!(redis_rs_py._driver.exceptions, PubSubError, RedisError);
create_exception!(redis_rs_py._driver.exceptions, SlaveError, RedisError);

create_exception!(redis_rs_py._driver.exceptions, ClusterError, RedisError);
create_exception!(redis_rs_py._driver.exceptions, MovedError, ClusterError);
create_exception!(redis_rs_py._driver.exceptions, AskError, ClusterError);
create_exception!(redis_rs_py._driver.exceptions, TryAgainError, ClusterError);

/// Discriminant carried through `RawResult::Error` so the async path can
/// raise the same exception class the sync path would.
#[derive(Clone, Copy, Debug)]
pub enum ExceptionClass {
    RedisError,
    ConnectionError,
    TimeoutError,
    BusyLoadingError,
    AuthenticationError,
    ResponseError,
    NoScriptError,
    ExecAbortError,
    ReadOnlyError,
    NoPermissionError,
    OutOfMemoryError,
    ModuleError,
    InvalidResponse,
    DataError,
    MasterDownError,
    ClusterDownError,
    ClusterCrossSlotError,
    MovedError,
    AskError,
    TryAgainError,
}

impl ExceptionClass {
    pub fn into_py_err(self, py: Python<'_>, msg: String) -> PyErr {
        match self {
            ExceptionClass::RedisError => PyErr::new::<RedisError, _>(msg),
            ExceptionClass::ConnectionError => PyErr::new::<ConnectionError, _>(msg),
            ExceptionClass::TimeoutError => PyErr::new::<TimeoutError, _>(msg),
            ExceptionClass::BusyLoadingError => PyErr::new::<BusyLoadingError, _>(msg),
            ExceptionClass::AuthenticationError => PyErr::new::<AuthenticationError, _>(msg),
            ExceptionClass::ResponseError => PyErr::new::<ResponseError, _>(msg),
            ExceptionClass::NoScriptError => PyErr::new::<NoScriptError, _>(msg),
            ExceptionClass::ExecAbortError => PyErr::new::<ExecAbortError, _>(msg),
            ExceptionClass::ReadOnlyError => PyErr::new::<ReadOnlyError, _>(msg),
            ExceptionClass::NoPermissionError => PyErr::new::<NoPermissionError, _>(msg),
            ExceptionClass::OutOfMemoryError => PyErr::new::<OutOfMemoryError, _>(msg),
            ExceptionClass::ModuleError => PyErr::new::<ModuleError, _>(msg),
            ExceptionClass::InvalidResponse => PyErr::new::<InvalidResponse, _>(msg),
            ExceptionClass::DataError => PyErr::new::<DataError, _>(msg),
            ExceptionClass::MasterDownError => PyErr::new::<MasterDownError, _>(msg),
            ExceptionClass::ClusterDownError => raise_clusterdown_error(py, msg),
            ExceptionClass::ClusterCrossSlotError => raise_clustercrossslot_error(py, msg),
            ExceptionClass::MovedError => PyErr::new::<MovedError, _>(msg),
            ExceptionClass::AskError => PyErr::new::<AskError, _>(msg),
            ExceptionClass::TryAgainError => PyErr::new::<TryAgainError, _>(msg),
        }
    }
}

/// Build a multi-base ClusterDownError dynamically. Used because
/// `create_exception!` only supports a single base, but ClusterDownError
/// must be `(ResponseError, ClusterError)` per redis-py.
fn raise_clusterdown_error(py: Python<'_>, msg: String) -> PyErr {
    let cls = py
        .import("redis_rs_py.exceptions")
        .and_then(|m| m.getattr("ClusterDownError"));
    match cls {
        Ok(c) => match c.call1((msg.clone(),)) {
            Ok(exc) => PyErr::from_value(exc),
            Err(_) => PyErr::new::<ResponseError, _>(msg),
        },
        Err(_) => PyErr::new::<ResponseError, _>(msg),
    }
}

fn raise_clustercrossslot_error(py: Python<'_>, msg: String) -> PyErr {
    let cls = py
        .import("redis_rs_py.exceptions")
        .and_then(|m| m.getattr("ClusterCrossSlotError"));
    match cls {
        Ok(c) => match c.call1((msg.clone(),)) {
            Ok(exc) => PyErr::from_value(exc),
            Err(_) => PyErr::new::<ResponseError, _>(msg),
        },
        Err(_) => PyErr::new::<ResponseError, _>(msg),
    }
}

/// Register every exception type into the `_driver.exceptions` submodule
/// AND into the parent `_driver` module so users have both
/// `from redis_rs_py.exceptions import RedisError` and
/// `from redis_rs_py import RedisError` working.
pub fn register(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "exceptions")?;
    m.add("RedisError", py.get_type::<RedisError>())?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;
    m.add("TimeoutError", py.get_type::<TimeoutError>())?;
    m.add("BusyLoadingError", py.get_type::<BusyLoadingError>())?;
    m.add("AuthenticationError", py.get_type::<AuthenticationError>())?;
    m.add(
        "AuthenticationWrongNumberOfArgsError",
        py.get_type::<AuthenticationWrongNumberOfArgsError>(),
    )?;
    m.add("MasterDownError", py.get_type::<MasterDownError>())?;
    m.add("ResponseError", py.get_type::<ResponseError>())?;
    m.add("DataError", py.get_type::<DataError>())?;
    m.add("InvalidResponse", py.get_type::<InvalidResponse>())?;
    m.add("OutOfMemoryError", py.get_type::<OutOfMemoryError>())?;
    m.add("NoScriptError", py.get_type::<NoScriptError>())?;
    m.add("ExecAbortError", py.get_type::<ExecAbortError>())?;
    m.add("ReadOnlyError", py.get_type::<ReadOnlyError>())?;
    m.add("NoPermissionError", py.get_type::<NoPermissionError>())?;
    m.add("ModuleError", py.get_type::<ModuleError>())?;
    m.add("LockError", py.get_type::<LockError>())?;
    m.add("LockNotOwnedError", py.get_type::<LockNotOwnedError>())?;
    m.add("WatchError", py.get_type::<WatchError>())?;
    m.add("PubSubError", py.get_type::<PubSubError>())?;
    m.add("SlaveError", py.get_type::<SlaveError>())?;
    m.add("ClusterError", py.get_type::<ClusterError>())?;
    m.add("MovedError", py.get_type::<MovedError>())?;
    m.add("AskError", py.get_type::<AskError>())?;
    m.add("TryAgainError", py.get_type::<TryAgainError>())?;

    // Multi-base classes built in pure Python (PyO3 create_exception! is
    // single-base). Subclass both ResponseError and ClusterError.
    let builtins: Bound<PyDict> = PyDict::new(py);
    builtins.set_item("ResponseError", py.get_type::<ResponseError>())?;
    builtins.set_item("ClusterError", py.get_type::<ClusterError>())?;

    let cluster_down = py.eval(
        std::ffi::CString::new("type('ClusterDownError', (ResponseError, ClusterError), {})")
            .unwrap()
            .as_c_str(),
        Some(&builtins),
        None,
    )?;
    let cluster_cross = py.eval(
        std::ffi::CString::new("type('ClusterCrossSlotError', (ResponseError, ClusterError), {})")
            .unwrap()
            .as_c_str(),
        Some(&builtins),
        None,
    )?;
    m.add("ClusterDownError", cluster_down)?;
    m.add("ClusterCrossSlotError", cluster_cross)?;

    // Also surface every name on the parent _driver module so the
    // Python re-export layer can do `from _driver import RedisError`.
    for name in [
        "RedisError",
        "ConnectionError",
        "TimeoutError",
        "BusyLoadingError",
        "AuthenticationError",
        "AuthenticationWrongNumberOfArgsError",
        "MasterDownError",
        "ResponseError",
        "DataError",
        "InvalidResponse",
        "OutOfMemoryError",
        "NoScriptError",
        "ExecAbortError",
        "ReadOnlyError",
        "NoPermissionError",
        "ModuleError",
        "LockError",
        "LockNotOwnedError",
        "WatchError",
        "PubSubError",
        "SlaveError",
        "ClusterError",
        "ClusterDownError",
        "ClusterCrossSlotError",
        "MovedError",
        "AskError",
        "TryAgainError",
    ] {
        parent.add(name, m.getattr(name)?)?;
    }

    parent.add_submodule(&m)?;
    Ok(())
}
