// Boundary translator: redis::RedisError → redis-rs-py exception class.
//
// Logic, in order of preference:
//   1. Connection-class kinds (Io, dropped, refused, timeout) → ConnectionError or TimeoutError.
//   2. ServerErrorKind discriminants (BusyLoading, NoScript, ReadOnly, Moved, Ask, etc.).
//   3. Code-based sniffing for Extension server errors that redis-rs hasn't pulled into
//      ServerErrorKind yet (OOM, WRONGPASS, NOAUTH, MODULE, WRONGTYPE, etc.). Real server
//      replies arrive as `ErrorKind::Extension` carrying the raw code; the Display string
//      Debug-quotes the code (`"WRONGTYPE": ...`) so prefix-sniffing on `to_string()` is
//      unreliable. Use `e.code()` instead, which returns the unquoted code.
//   4. Generic ResponseError fallback for any remaining server-side error
//      (kind = Parse / Server(ResponseError) / Extension without a recognised code).
//   5. Final fallback: RedisError (base catch-all).

use pyo3::PyErr;
use pyo3::Python;

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

    // Layer 3: code-based sniffing for Extension server errors. The `code()` accessor
    // returns the raw code without the Debug-format quotes that `Display` adds, so it
    // works for both ErrorRepr::Server(Extension) (real wire errors) and ErrorRepr::General
    // (the test/synthetic constructor path).
    if let Some(code) = e.code() {
        let code_upper = code.to_ascii_uppercase();
        if code_upper == "OOM" {
            return ExceptionClass::OutOfMemoryError;
        }
        if code_upper == "WRONGPASS" || code_upper == "NOAUTH" {
            return ExceptionClass::AuthenticationError;
        }
        if code_upper.starts_with("MODULE") {
            return ExceptionClass::ModuleError;
        }
    }

    // Layer 4: any remaining server-side error → ResponseError. `Extension` covers the
    // catch-all "unknown server reply" path; without it WRONGTYPE etc. would fall through
    // to the base RedisError fallback below.
    if matches!(
        e.kind(),
        redis::ErrorKind::Parse
            | redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError)
            | redis::ErrorKind::Extension
    ) {
        return ExceptionClass::ResponseError;
    }

    // Layer 5: final fallback
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_err(
        kind: redis::ErrorKind,
        code: &'static str,
        msg: &'static str,
    ) -> redis::RedisError {
        // RedisError::from((kind, code, msg)) — produces ErrorRepr::General with the given
        // code surfaced through `e.code()`. Use this for non-Extension test cases.
        redis::RedisError::from((kind, code, msg.to_string()))
    }

    fn make_extension(code: &str, detail: &str) -> redis::RedisError {
        // Mirrors a real wire error: ErrorRepr::Server(Extension { code, detail }).
        // Use this for codes the redis crate doesn't have a ServerErrorKind for
        // (WRONGTYPE, OOM, WRONGPASS, MODULE, etc.).
        redis::make_extension_error(code.to_string(), Some(detail.to_string()))
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
    fn classifies_oom_extension() {
        // Real wire shape — `ErrorRepr::Server(Extension { code: "OOM", ... })`.
        let e = make_extension("OOM", "command not allowed when used memory > 'maxmemory'");
        assert!(matches!(
            classify_error(&e),
            ExceptionClass::OutOfMemoryError
        ));
    }

    #[test]
    fn classifies_wrongpass_extension() {
        let e = make_extension("WRONGPASS", "invalid username-password pair");
        assert!(matches!(
            classify_error(&e),
            ExceptionClass::AuthenticationError
        ));
    }

    #[test]
    fn classifies_noauth_extension() {
        let e = make_extension("NOAUTH", "Authentication required.");
        assert!(matches!(
            classify_error(&e),
            ExceptionClass::AuthenticationError
        ));
    }

    #[test]
    fn classifies_module_extension() {
        let e = make_extension("MODULE_LOAD_FAILED", "no such module 'rejson'");
        assert!(matches!(classify_error(&e), ExceptionClass::ModuleError));
    }

    #[test]
    fn classifies_wrongtype_extension_as_response_error() {
        // The bug this test guards: WRONGTYPE arrives as Extension, not Server(ResponseError).
        // Without `Extension` in the Layer 4 match, it would fall through to RedisError.
        let e = make_extension(
            "WRONGTYPE",
            "Operation against a key holding the wrong kind of value",
        );
        assert!(matches!(classify_error(&e), ExceptionClass::ResponseError));
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
        // Truly unknown kinds (not Server / Parse / Extension) fall through to RedisError.
        // Use a non-server kind that's not in any of the Layer 1-4 buckets.
        let e = make_err(
            redis::ErrorKind::UnexpectedReturnType,
            "weird",
            "unexpected reply",
        );
        assert!(matches!(classify_error(&e), ExceptionClass::RedisError));
    }
}
