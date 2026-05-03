// Process-global tokio runtime with fork-safe rebuild.
//
// Verbatim port of django-vcache's runtime singleton (MIT,
// David Burke / GlitchTip), via django-cachex-redis-rs.
//
// Fast path (~99.99% of calls): atomic PID check + OnceLock::get() →
// `&'static Runtime`, no locks, no allocations.
//
// Slow path: first call ever (OnceLock init) or first call after fork
// (Mutex-protected rebuild). After fork we leak the new runtime via
// `Box::leak` because dropping a tokio runtime that has dead worker
// threads from the parent process can hang.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static RUNTIME_PID: AtomicU32 = AtomicU32::new(0);
static FORK_RUNTIME: Mutex<Option<(u32, &'static Runtime)>> = Mutex::new(None);

#[inline]
pub fn get_runtime() -> &'static Runtime {
    let pid = std::process::id();
    if RUNTIME_PID.load(Ordering::Relaxed) == pid {
        return RUNTIME.get().unwrap();
    }
    init_or_fork_runtime(pid)
}

#[cold]
fn init_or_fork_runtime(pid: u32) -> &'static Runtime {
    let stored = RUNTIME_PID.load(Ordering::Relaxed);

    if stored == 0 {
        let rt = RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime")
        });
        RUNTIME_PID.store(pid, Ordering::Relaxed);
        return rt;
    }

    let mut guard = FORK_RUNTIME.lock().unwrap();
    if let Some((stored_pid, rt)) = *guard
        && stored_pid == pid
    {
        return rt;
    }
    let rt: &'static Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime"),
    ));
    *guard = Some((pid, rt));
    rt
}
