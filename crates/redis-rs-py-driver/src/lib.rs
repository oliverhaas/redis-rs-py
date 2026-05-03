// redis-rs-py-driver — Rust I/O driver for the redis-rs-py Python package.
//
// The async bridge and the standard-connection wiring in this crate are
// derived from django-vcache (MIT, David Burke / GlitchTip), via the
// django-cachex-redis-rs prototype. Keep async_bridge.rs and the upstream
// half of connection.rs in lockstep with django-vcache; if you want to
// diverge, open a discussion first — the design is load-bearing.

mod async_bridge;
mod connection;
mod driver;
mod errors;
mod raw_result;
mod runtime;
mod test_helpers;

use pyo3::prelude::*;

#[pymodule]
fn _driver(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<async_bridge::RedisRsAwaitable>()?;
    // m.add_class::<driver::RedisRsDriver>()?;

    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_none, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_int, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_delayed_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_pending, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_dropped, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_error, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_server_error, m)?)?;

    Ok(())
}
