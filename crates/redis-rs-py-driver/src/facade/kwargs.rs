// Accept-and-warn surface for redis-py constructor kwargs we don't yet
// implement. The redis-py contract is "every kwarg in `Redis.__init__`
// must be accepted without raising"; ours is "accepted, warn once per
// process per unknown name, then ignore".
//
// The `KNOWN_KWARGS` slice is the full redis-py 5.x kwarg surface
// (verified by `python -c "import redis, inspect; print(inspect.signature(redis.Redis.__init__))"`).
// Anything in this list is silently ignored if not in the
// `IMPLEMENTED_KWARGS` slice — but already-implemented names are
// extracted by the `Redis::__new__` constructor before we get here, so
// only the *unknown to us* names trigger a warning.
//
// Anything *not* in `KNOWN_KWARGS` (e.g. typos, future redis-py
// additions) gets a sharper warning that flags it as unrecognised.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

/// Every kwarg `redis.Redis.__init__` accepts (redis-py 5.x). Captured
/// verbatim from the upstream signature.
pub const KNOWN_KWARGS: &[&str] = &[
    "host",
    "port",
    "db",
    "password",
    "socket_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_keepalive_options",
    "connection_pool",
    "unix_socket_path",
    "encoding",
    "encoding_errors",
    "charset",
    "errors",
    "decode_responses",
    "retry_on_timeout",
    "retry_on_error",
    "ssl",
    "ssl_keyfile",
    "ssl_certfile",
    "ssl_cert_reqs",
    "ssl_ca_certs",
    "ssl_ca_path",
    "ssl_ca_data",
    "ssl_check_hostname",
    "ssl_password",
    "ssl_validate_ocsp",
    "ssl_validate_ocsp_stapled",
    "ssl_ocsp_context",
    "ssl_ocsp_expected_cert",
    "ssl_min_version",
    "ssl_ciphers",
    "max_connections",
    "single_connection_client",
    "health_check_interval",
    "client_name",
    "lib_name",
    "lib_version",
    "username",
    "retry",
    "redis_connect_func",
    "credential_provider",
    "protocol",
    "cache",
    "cache_config",
    "event_dispatcher",
];

/// Subset of `KNOWN_KWARGS` we wire to actual driver behaviour. This is
/// the contract the README's compatibility matrix advertises.
pub const IMPLEMENTED_KWARGS: &[&str] = &[
    "host",
    "port",
    "db",
    "password",
    "username",
    "ssl",
    "ssl_keyfile",
    "ssl_certfile",
    "ssl_ca_certs",
    "socket_timeout",
    "max_connections",
    "health_check_interval",
    "client_name",
    "protocol",
    "decode_responses",
    "encoding",
    "encoding_errors",
];

static SEEN_UNIMPLEMENTED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static SEEN_UNKNOWN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn seen_unimplemented() -> &'static Mutex<HashSet<String>> {
    SEEN_UNIMPLEMENTED.get_or_init(|| Mutex::new(HashSet::new()))
}

fn seen_unknown() -> &'static Mutex<HashSet<String>> {
    SEEN_UNKNOWN.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Iterate `kwargs` and warn (once per process per name) about each name
/// that is not in `implemented`. Distinguishes "redis-py kwarg we just
/// don't honour yet" (UserWarning, low severity) from "name we don't
/// recognise at all" (RuntimeWarning, higher severity).
///
/// Caller passes `implemented` as the names already extracted to typed
/// fields by the constructor.
pub fn accept_and_warn(
    py: Python<'_>,
    implemented: &[&str],
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let Some(kwargs) = kwargs else {
        return Ok(());
    };
    if kwargs.is_empty() {
        return Ok(());
    }

    let warnings_mod = py.import("warnings")?;
    let builtins = py.import("builtins")?;

    for (k, _v) in kwargs.iter() {
        let name: String = k.extract()?;
        if implemented.contains(&name.as_str()) {
            continue;
        }
        let is_redis_py = KNOWN_KWARGS.contains(&name.as_str());
        let (category_attr, msg) = if is_redis_py {
            (
                "UserWarning",
                format!(
                    "redis_rs_py.Redis: kwarg `{name}` is recognised by redis-py but not yet \
                     implemented in this driver — it has been accepted and ignored. \
                     See the compatibility matrix for status."
                ),
            )
        } else {
            (
                "RuntimeWarning",
                format!(
                    "redis_rs_py.Redis: kwarg `{name}` is not recognised by redis-py 5.x or this \
                     driver — it has been accepted and ignored. Check for a typo."
                ),
            )
        };

        // One-shot dedup by name.
        let map = if is_redis_py {
            seen_unimplemented()
        } else {
            seen_unknown()
        };
        {
            let mut g = map.lock().unwrap();
            if g.contains(&name) {
                continue;
            }
            g.insert(name.clone());
        }

        // UserWarning and RuntimeWarning are builtins, not in the warnings module.
        let category = builtins.getattr(category_attr)?;
        let stacklevel = 4_i64; // Skip into the user's frame: __init__ → __new__ → ours → user.
        let args = PyTuple::new(py, [msg.into_pyobject(py)?.into_any()])?;
        let kw = PyDict::new(py);
        kw.set_item("category", category)?;
        kw.set_item("stacklevel", stacklevel)?;
        warnings_mod.call_method("warn", args, Some(&kw))?;
    }

    Ok(())
}

/// Test-only: clear the warn-once dedup state so repeated test runs in a
/// single process all see the warning. Wired to a pyfunction in the
/// crate-level `_driver` module under `_facade_reset_warn_state`.
#[doc(hidden)]
pub fn reset_warn_state_for_tests() {
    if let Some(m) = SEEN_UNIMPLEMENTED.get() {
        m.lock().unwrap().clear();
    }
    if let Some(m) = SEEN_UNKNOWN.get() {
        m.lock().unwrap().clear();
    }
}

#[pyfunction]
#[pyo3(name = "_facade_reset_warn_state")]
pub fn py_reset_warn_state() {
    reset_warn_state_for_tests();
}
