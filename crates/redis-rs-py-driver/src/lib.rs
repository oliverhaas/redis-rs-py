// redis-rs-py-driver — Rust I/O driver for the redis-rs-py Python package.
//
// The async bridge and the standard-connection wiring in this crate are
// derived from django-vcache (MIT, David Burke / GlitchTip), via the
// django-cachex-redis-rs prototype. Keep async_bridge.rs and the upstream
// half of connection.rs in lockstep with django-vcache; if you want to
// diverge, open a discussion first — the design is load-bearing.

mod async_bridge;
mod commands;
mod connection;
mod errors;
pub(crate) mod exceptions;
mod facade;
mod helpers;
mod raw_result;
mod runtime;
mod test_helpers;

use pyo3::prelude::*;

#[pymodule]
fn _driver(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    exceptions::register(m.py(), m)?;
    m.add_class::<async_bridge::RedisRsAwaitable>()?;
    m.add_class::<facade::sync::Redis>()?;
    m.add_class::<facade::sync::Lock>()?;
    m.add_class::<facade::pipeline::Pipeline>()?;
    m.add_class::<facade::pipeline::AsyncPipeline>()?;
    m.add_class::<facade::pubsub::PubSub>()?;
    m.add_class::<facade::pubsub::PubSubWorkerThread>()?;

    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_none, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_resolved_int, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_delayed_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_pending, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_dropped, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_error, m)?)?;
    m.add_function(wrap_pyfunction!(test_helpers::_test_server_error, m)?)?;
    m.add_function(wrap_pyfunction!(facade::kwargs::py_reset_warn_state, m)?)?;
    m.add_class::<facade::decode::DecoderClosure>()?;
    m.add_function(wrap_pyfunction!(facade::decode::py_decode_walk, m)?)?;

    // asyncio submodule — registered both as a PyO3 submodule and into
    // sys.modules so `import redis_rs_py._driver.asyncio` resolves.
    let asyncio_mod = PyModule::new(m.py(), "asyncio")?;
    facade::asyncio_mod::register(m.py(), &asyncio_mod)?;
    m.add_submodule(&asyncio_mod)?;

    // cluster submodule — redis_rs_py._driver.cluster
    facade::cluster::register_sync(m.py(), m)?;

    // async cluster submodule — redis_rs_py._driver.asyncio.cluster
    facade::cluster::register_async(m.py(), &asyncio_mod)?;

    // PyO3 0.28: submodules are NOT auto-added to sys.modules. Do it
    // manually so `from redis_rs_py._driver.asyncio import Redis` and
    // dotted import paths work.
    let sys = m.py().import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("redis_rs_py._driver.asyncio", &asyncio_mod)?;

    Ok(())
}
