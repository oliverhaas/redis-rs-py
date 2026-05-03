// Boundary translator: redis::RedisError → redis-rs-py exception class.
//
// Logic, in order of preference:
//   1. Connection-class kinds (Io, dropped, refused, timeout) → ConnectionError or TimeoutError.
//   2. ServerErrorKind → its dedicated Exception class.
//   3. Code-prefix sniffing on the message (NOSCRIPT, OOM, MOVED, ASK, etc.).
//   4. Fallback: ResponseError.

use pyo3::PyErr;

use crate::async_bridge::RawResult;
use crate::exceptions::ExceptionClass;

pub fn classify_error(e: &redis::RedisError) -> ExceptionClass {
    // Layer 1: connection-class
    if e.is_timeout() {
        return ExceptionClass::TimeoutError;
    }
    if e.is_connection_dropped() || e.is_connection_refusal() {
        return ExceptionClass::ConnectionError;
    }
    if matches!(e.kind(), redis::ErrorKind::Io) {
        return ExceptionClass::ConnectionError;
    }

    // Layer 2: ServerErrorKind discriminants
    if let redis::ErrorKind::Server(sk) = e.kind() {
        match sk {
            redis::ServerErrorKind::BusyLoading => return ExceptionClass::BusyLoadingError,
            redis::ServerErrorKind::TryAgain => return ExceptionClass::TryAgainError,
            redis::ServerErrorKind::ReadOnly => return ExceptionClass::ReadOnlyError,
            redis::ServerErrorKind::NoScript => return ExceptionClass::NoScriptError,
            redis::ServerErrorKind::ExecAbort => return ExceptionClass::ExecAbortError,
            redis::ServerErrorKind::Moved => return ExceptionClass::MovedError,
            redis::ServerErrorKind::Ask => return ExceptionClass::AskError,
            redis::ServerErrorKind::ClusterDown => return ExceptionClass::ClusterDownError,
            redis::ServerErrorKind::CrossSlot => return ExceptionClass::ClusterCrossSlotError,
            redis::ServerErrorKind::MasterDown => return ExceptionClass::MasterDownError,
            redis::ServerErrorKind::NoPerm => return ExceptionClass::NoPermissionError,
            _ => {}
        }
    }

    // Layer 3: prefix sniffing on the textual message (covers servers /
    // codes redis-rs hasn't yet pulled into ServerErrorKind).
    let msg = e.to_string();
    let msg_upper = msg.to_ascii_uppercase();
    if msg_upper.starts_with("OOM") {
        return ExceptionClass::OutOfMemoryError;
    }
    if msg_upper.starts_with("WRONGPASS")
        || msg_upper.starts_with("NOAUTH")
        || msg_upper.contains("AUTHENTICATION")
    {
        return ExceptionClass::AuthenticationError;
    }
    if msg_upper.starts_with("MODULE") {
        return ExceptionClass::ModuleError;
    }
    if matches!(
        e.kind(),
        redis::ErrorKind::Parse | redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError)
    ) {
        return ExceptionClass::ResponseError;
    }

    // Layer 4: fallback
    ExceptionClass::RedisError
}

/// Used by sync command bodies.
pub fn to_py_err(e: redis::RedisError) -> PyErr {
    let class = classify_error(&e);
    let msg = e.to_string();
    Python::attach(|py| class.into_py_err(py, msg))
}

/// Used by async command bodies (via `IntoRawResult`).
pub fn classify(e: redis::RedisError) -> RawResult {
    let class = classify_error(&e);
    RawResult::Error(class, e.to_string())
}

use pyo3::Python;

#[cfg(test)]
mod tests {
    use super::*;

    fn make_err(
        kind: redis::ErrorKind,
        code: &'static str,
        msg: &'static str,
    ) -> redis::RedisError {
        // RedisError::from((kind, detail, source)) — the test-helper form
        // is RedisError::from((kind, code, msg)) which serialises as
        // `<code>: <msg>`.
        redis::RedisError::from((kind, code, msg.to_string()))
    }

    #[test]
    fn classifies_io_as_connection_error() {
        let e = make_err(redis::ErrorKind::Io, "io", "broken pipe");
        assert!(matches!(
            classify_error(&e),
            ExceptionClass::ConnectionError
        ));
    }

    #[test]
    fn classifies_busy_loading() {
        let e = make_err(
            redis::ErrorKind::Server(redis::ServerErrorKind::BusyLoading),
            "loading",
            "redis is loading the dataset",
        );
        assert!(matches!(
            classify_error(&e),
            ExceptionClass::BusyLoadingError
        ));
    }

    #[test]
    fn classifies_no_script() {
        let e = make_err(
            redis::ErrorKind::Server(redis::ServerErrorKind::NoScript),
            "noscript",
            "no matching script",
        );
        assert!(matches!(classify_error(&e), ExceptionClass::NoScriptError));
    }

    #[test]
    fn classifies_readonly() {
        let e = make_err(
            redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly),
            "readonly",
            "you can't write against a read only replica",
        );
        assert!(matches!(classify_error(&e), ExceptionClass::ReadOnlyError));
    }

    #[test]
    fn classifies_oom_via_prefix() {
        let e = make_err(
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "oom",
            "OOM command not allowed when used memory > 'maxmemory'",
        );
        assert!(matches!(
            classify_error(&e),
            ExceptionClass::OutOfMemoryError
        ));
    }

    #[test]
    fn classifies_auth_via_prefix() {
        // The desc field becomes the prefix of the Display string;
        // "wrongpass" uppercases to "WRONGPASS" which triggers the sniff.
        let e = make_err(
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "wrongpass",
            "invalid username-password pair",
        );
        assert!(matches!(
            classify_error(&e),
            ExceptionClass::AuthenticationError
        ));
    }

    #[test]
    fn classifies_module_via_prefix() {
        let e = make_err(
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "module",
            "MODULE no such module 'rejson'",
        );
        assert!(matches!(classify_error(&e), ExceptionClass::ModuleError));
    }

    #[test]
    fn classifies_response_error_default() {
        let e = make_err(
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "wrongtype",
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        );
        assert!(matches!(classify_error(&e), ExceptionClass::ResponseError));
    }

    #[test]
    fn classifies_unknown_kind_as_redis_error_fallback() {
        // Unknown ErrorKind shouldn't blow up — it should land in the fallback.
        let e = make_err(redis::ErrorKind::Extension, "ext", "unknown");
        assert!(matches!(classify_error(&e), ExceptionClass::RedisError));
    }
}
