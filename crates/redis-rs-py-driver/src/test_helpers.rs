// Test scaffolding for RedisRsAwaitable.
//
// Each function constructs a RedisRsAwaitable in a specific resolution
// state without going through the I/O surface, so the awaitable protocol
// can be exercised end-to-end in unit tests against the production class.
//
// Verbatim port of django-vcache's test_helpers.rs (MIT, David Burke /
// GlitchTip), via django-cachex-redis-rs.

use pyo3::prelude::*;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::async_bridge::{RawResult, RedisRsAwaitable};
use crate::runtime::get_runtime;

#[pyfunction]
pub fn _test_resolved_bytes(b: Vec<u8>) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::OptBytes(Some(b)));
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_resolved_none() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::OptBytes(None));
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_resolved_int(n: i64) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::Int(n));
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_delayed_bytes(b: Vec<u8>, delay_ms: u64) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    get_runtime().spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let _ = tx.send(RawResult::OptBytes(Some(b)));
    });
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_pending() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel::<RawResult>();
    // Leak the tx so the rx never closes. The awaitable is intentionally
    // never resolved — used to test cancellation paths.
    std::mem::forget(tx);
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_dropped() -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel::<RawResult>();
    drop(tx);
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_error(msg: String) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::Error(msg));
    RedisRsAwaitable::new(rx)
}

#[pyfunction]
pub fn _test_server_error(msg: String) -> RedisRsAwaitable {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(RawResult::ServerError(msg));
    RedisRsAwaitable::new(rx)
}
