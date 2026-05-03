// Pipeline / AsyncPipeline pyclasses — buffered-then-flushed semantics
// matching redis-py's Pipeline contract.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyType};

use crate::async_bridge::RawResult;
use crate::connection::{ReservedConnection, ValkeyConn, WatchedExecResult};
use crate::errors::to_py_err;
use crate::exceptions::{ConnectionError, RedisError, WatchError};
use crate::runtime::get_runtime;

type BufferedCmd = (String, Vec<Vec<u8>>);

struct PipelineState {
    commands: Vec<BufferedCmd>,
    watched_keys: Vec<String>,
    transaction: bool,
    explicit_transaction: bool,
    watching: bool,
    reserved: Option<ReservedConnection>,
    closed: bool,
}

impl PipelineState {
    fn new(transaction: bool) -> Self {
        Self {
            commands: Vec::new(),
            watched_keys: Vec::new(),
            transaction,
            explicit_transaction: false,
            watching: false,
            reserved: None,
            closed: false,
        }
    }
}

fn str_arg(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}
fn i64_arg(n: i64) -> Vec<u8> {
    n.to_string().into_bytes()
}
fn f64_arg(n: f64) -> Vec<u8> {
    n.to_string().into_bytes()
}

// =========================================================================
// Pipeline pyclass (sync)
// =========================================================================

#[pyclass(module = "redis_rs_py._driver")]
pub struct Pipeline {
    conn: ValkeyConn,
    state: Mutex<PipelineState>,
}

impl Pipeline {
    pub fn new(conn: ValkeyConn, transaction: bool) -> Self {
        Self {
            conn,
            state: Mutex::new(PipelineState::new(transaction)),
        }
    }

    fn release_reserved(&self, py: Python<'_>) -> PyResult<()> {
        let reserved = {
            let mut state = self.state.lock().unwrap();
            state.watching = false;
            state.explicit_transaction = false;
            state.watched_keys.clear();
            state.commands.clear();
            state.reserved.take()
        };
        if let Some(mut r) = reserved {
            py.detach(|| {
                get_runtime().block_on(async {
                    let _ = r.unwatch_if_needed().await;
                })
            });
        }
        Ok(())
    }

    fn ensure_reserved(&self, py: Python<'_>) -> PyResult<()> {
        {
            let s = self.state.lock().unwrap();
            if s.reserved.is_some() {
                return Ok(());
            }
        }
        let conn = self.conn.clone();
        let reserved: Result<ReservedConnection, String> =
            py.detach(|| get_runtime().block_on(async move { conn.reserve_connection().await }));
        match reserved {
            Ok(r) => {
                self.state.lock().unwrap().reserved = Some(r);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<ConnectionError, _>(e)),
        }
    }

    fn dispatch_immediate_inner(
        &self,
        py: Python<'_>,
        name: &str,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let result: Result<redis::Value, _> = py.detach(|| {
            get_runtime().block_on(async {
                // Take the reserved connection out before awaiting to avoid
                // holding the MutexGuard across an await point.
                let mut taken = {
                    let mut s = self.state.lock().unwrap();
                    s.reserved.take().ok_or_else(|| {
                        redis::RedisError::from((
                            redis::ErrorKind::Client,
                            "internal error",
                            "immediate-mode dispatch with no reserved connection".to_string(),
                        ))
                    })?
                };
                let res = taken.dispatch_immediate(&name, &args).await;
                self.state.lock().unwrap().reserved = Some(taken);
                res
            })
        });
        let value = result.map_err(to_py_err)?;
        RawResult::Value(value).into_py(py)
    }

    fn buf_or_dispatch(
        slf: Py<Self>,
        py: Python<'_>,
        name: &str,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let this = slf.bind(py).borrow();
        {
            let s = this.state.lock().unwrap();
            if s.closed {
                return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
            }
        }
        let immediate = {
            let s = this.state.lock().unwrap();
            s.watching && !s.explicit_transaction
        };
        if immediate {
            return this.dispatch_immediate_inner(py, name, args);
        }
        this.state
            .lock()
            .unwrap()
            .commands
            .push((name.to_string(), args));
        drop(this);
        Ok(slf.into_any())
    }

    fn execute_watched(&self, py: Python<'_>, commands: Vec<BufferedCmd>) -> PyResult<Py<PyAny>> {
        let watched_keys: Vec<String> = self.state.lock().unwrap().watched_keys.clone();

        let result: Result<WatchedExecResult, _> = py.detach(|| {
            get_runtime().block_on(async {
                // Take the reserved connection out before awaiting to avoid
                // holding the MutexGuard across an await point.
                let mut taken = self.state.lock().unwrap().reserved.take().ok_or_else(|| {
                    redis::RedisError::from((
                        redis::ErrorKind::Client,
                        "internal error",
                        "execute_watched without a reserved connection".to_string(),
                    ))
                })?;
                taken.pipeline_exec_watched(&watched_keys, commands).await
            })
        });

        {
            let mut s = self.state.lock().unwrap();
            s.commands.clear();
            s.watched_keys.clear();
            s.watching = false;
            s.explicit_transaction = false;
            let _ = s.reserved.take();
        }

        match result {
            Ok(WatchedExecResult::Ok(items)) => {
                RawResult::Value(redis::Value::Array(items)).into_py(py)
            }
            Ok(WatchedExecResult::WatchAborted) => {
                Err(PyErr::new::<WatchError, _>("Watched variable changed."))
            }
            Err(e) => Err(to_py_err(e)),
        }
    }
}

#[pymethods]
impl Pipeline {
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.release_reserved(py)?;
        Ok(false)
    }

    fn __len__(&self) -> usize {
        self.state.lock().unwrap().commands.len()
    }

    fn __bool__(&self) -> bool {
        true
    }

    fn reset(&self, py: Python<'_>) -> PyResult<()> {
        self.release_reserved(py)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.release_reserved(py)?;
        self.state.lock().unwrap().closed = true;
        Ok(())
    }

    fn execute(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (commands, transaction, watching, explicit_transaction, closed) = {
            let s = self.state.lock().unwrap();
            (
                s.commands.clone(),
                s.transaction,
                s.watching,
                s.explicit_transaction,
                s.closed,
            )
        };
        if closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        if commands.is_empty() && !watching && !explicit_transaction {
            return Ok(PyList::empty(py).into_any().unbind());
        }
        // Only use the WATCH/EXEC path when a reserved connection was
        // actually acquired (i.e., watch() was called at least once).
        // If the user called multi() directly without any prior watch(),
        // there is no reserved connection — fall through to the plain
        // buffered pipeline_exec with transaction=true.
        let has_reserved = self.state.lock().unwrap().reserved.is_some();
        if watching || (explicit_transaction && has_reserved) {
            return self.execute_watched(py, commands);
        }

        // Plain buffered path: transaction flag comes from either the
        // constructor default OR an explicit multi() call.
        let run_as_transaction = transaction || explicit_transaction;
        let mut conn = self.conn.clone();
        let result: Result<Vec<redis::Value>, _> = py.detach(|| {
            get_runtime()
                .block_on(async move { conn.pipeline_exec(commands, run_as_transaction).await })
        });
        {
            let mut s = self.state.lock().unwrap();
            s.commands.clear();
            s.explicit_transaction = false;
        }
        let values = result.map_err(to_py_err)?;
        RawResult::Value(redis::Value::Array(values)).into_py(py)
    }

    #[pyo3(signature = (*keys))]
    fn watch(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        {
            let s = self.state.lock().unwrap();
            if s.closed {
                return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
            }
            if s.explicit_transaction {
                return Err(PyErr::new::<RedisError, _>(
                    "Cannot issue a WATCH after a MULTI",
                ));
            }
        }
        self.ensure_reserved(py)?;
        let result: Result<(), _> = py.detach(|| {
            get_runtime().block_on(async {
                // Take out the reserved connection to avoid holding the
                // MutexGuard across the await point.
                let mut taken = self.state.lock().unwrap().reserved.take().unwrap();
                let res = taken.watch(&keys).await;
                let mut s = self.state.lock().unwrap();
                s.reserved = Some(taken);
                if res.is_ok() {
                    s.watching = true;
                    s.watched_keys.extend(keys.iter().cloned());
                }
                res
            })
        });
        result.map_err(to_py_err)?;
        Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
    }

    fn unwatch(&self, py: Python<'_>) -> PyResult<bool> {
        let has_reserved = {
            let s = self.state.lock().unwrap();
            if s.closed {
                return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
            }
            s.reserved.is_some()
        };
        if has_reserved {
            let res: Result<(), _> = py.detach(|| {
                get_runtime().block_on(async {
                    // Take the reserved connection out before awaiting.
                    let mut taken = self.state.lock().unwrap().reserved.take().unwrap();
                    let res = taken.unwatch_if_needed().await;
                    self.state.lock().unwrap().reserved = Some(taken);
                    res
                })
            });
            res.map_err(to_py_err)?;
        }
        let mut s = self.state.lock().unwrap();
        s.watched_keys.clear();
        s.watching = false;
        Ok(true)
    }

    fn multi(&self) -> PyResult<()> {
        let mut s = self.state.lock().unwrap();
        if s.closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        if s.explicit_transaction {
            return Err(PyErr::new::<RedisError, _>(
                "Cannot issue nested calls to MULTI",
            ));
        }
        if !s.commands.is_empty() && !s.watching {
            return Err(PyErr::new::<RedisError, _>(
                "Commands without an initial WATCH have already been issued",
            ));
        }
        s.explicit_transaction = true;
        Ok(())
    }

    fn discard(&self, py: Python<'_>) -> PyResult<()> {
        let needs_unwatch = {
            let s = self.state.lock().unwrap();
            !s.closed && s.reserved.is_some()
        };
        if needs_unwatch {
            let res: Result<(), _> = py.detach(|| {
                get_runtime().block_on(async {
                    // Take out the reserved connection before awaiting.
                    let mut taken = self.state.lock().unwrap().reserved.take().unwrap();
                    let res = taken.unwatch_if_needed().await;
                    self.state.lock().unwrap().reserved = Some(taken);
                    res
                })
            });
            res.map_err(to_py_err)?;
        }
        let mut s = self.state.lock().unwrap();
        s.commands.clear();
        s.explicit_transaction = false;
        s.watched_keys.clear();
        s.watching = false;
        Ok(())
    }
}

// =========================================================================
// Pipeline buffered command methods — explicit (PyO3 0.28 bans macros in
// #[pymethods] blocks, so each method is written out individually).
// =========================================================================

#[pymethods]
impl Pipeline {
    // --- Strings ---
    fn set(slf: Py<Self>, py: Python<'_>, key: &str, value: Vec<u8>) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "SET", vec![str_arg(key), value])
    }
    fn get(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "GET", vec![str_arg(key)])
    }
    fn getdel(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "GETDEL", vec![str_arg(key)])
    }
    fn append(slf: Py<Self>, py: Python<'_>, key: &str, value: Vec<u8>) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "APPEND", vec![str_arg(key), value])
    }
    fn strlen(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "STRLEN", vec![str_arg(key)])
    }
    fn incr(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "INCR", vec![str_arg(key)])
    }
    fn incrby(slf: Py<Self>, py: Python<'_>, key: &str, by: i64) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "INCRBY", vec![str_arg(key), i64_arg(by)])
    }
    fn incrbyfloat(slf: Py<Self>, py: Python<'_>, key: &str, by: f64) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "INCRBYFLOAT", vec![str_arg(key), f64_arg(by)])
    }
    fn decr(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "DECR", vec![str_arg(key)])
    }
    fn decrby(slf: Py<Self>, py: Python<'_>, key: &str, by: i64) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "DECRBY", vec![str_arg(key), i64_arg(by)])
    }
    fn setrange(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        offset: i64,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(
            slf,
            py,
            "SETRANGE",
            vec![str_arg(key), i64_arg(offset), value],
        )
    }
    fn getrange(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        start: i64,
        end: i64,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(
            slf,
            py,
            "GETRANGE",
            vec![str_arg(key), i64_arg(start), i64_arg(end)],
        )
    }
    fn rename(slf: Py<Self>, py: Python<'_>, key: &str, new_key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "RENAME", vec![str_arg(key), str_arg(new_key)])
    }
    fn renamenx(slf: Py<Self>, py: Python<'_>, key: &str, new_key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "RENAMENX", vec![str_arg(key), str_arg(new_key)])
    }
    fn typ(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "TYPE", vec![str_arg(key)])
    }
    fn expire(slf: Py<Self>, py: Python<'_>, key: &str, seconds: i64) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "EXPIRE", vec![str_arg(key), i64_arg(seconds)])
    }
    fn pexpire(slf: Py<Self>, py: Python<'_>, key: &str, millis: i64) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "PEXPIRE", vec![str_arg(key), i64_arg(millis)])
    }
    fn ttl(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "TTL", vec![str_arg(key)])
    }
    fn pttl(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "PTTL", vec![str_arg(key)])
    }
    fn persist(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "PERSIST", vec![str_arg(key)])
    }

    // --- Key multi-forms ---
    #[pyo3(signature = (*keys))]
    fn delete(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let args: Vec<Vec<u8>> = keys.into_iter().map(|s| s.into_bytes()).collect();
        Pipeline::buf_or_dispatch(slf, py, "DEL", args)
    }
    #[pyo3(signature = (*keys))]
    fn unlink(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let args: Vec<Vec<u8>> = keys.into_iter().map(|s| s.into_bytes()).collect();
        Pipeline::buf_or_dispatch(slf, py, "UNLINK", args)
    }
    #[pyo3(signature = (*keys))]
    fn exists(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let args: Vec<Vec<u8>> = keys.into_iter().map(|s| s.into_bytes()).collect();
        Pipeline::buf_or_dispatch(slf, py, "EXISTS", args)
    }

    // --- Lists ---
    #[pyo3(signature = (key, *values))]
    fn lpush(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        values: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(values);
        Pipeline::buf_or_dispatch(slf, py, "LPUSH", args)
    }
    #[pyo3(signature = (key, *values))]
    fn rpush(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        values: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(values);
        Pipeline::buf_or_dispatch(slf, py, "RPUSH", args)
    }
    fn lpop(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "LPOP", vec![str_arg(key)])
    }
    fn rpop(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "RPOP", vec![str_arg(key)])
    }
    fn llen(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "LLEN", vec![str_arg(key)])
    }
    fn lrange(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(
            slf,
            py,
            "LRANGE",
            vec![str_arg(key), i64_arg(start), i64_arg(stop)],
        )
    }
    fn lindex(slf: Py<Self>, py: Python<'_>, key: &str, index: i64) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "LINDEX", vec![str_arg(key), i64_arg(index)])
    }
    fn lrem(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        count: i64,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "LREM", vec![str_arg(key), i64_arg(count), value])
    }
    fn ltrim(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(
            slf,
            py,
            "LTRIM",
            vec![str_arg(key), i64_arg(start), i64_arg(stop)],
        )
    }
    fn lset(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        index: i64,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "LSET", vec![str_arg(key), i64_arg(index), value])
    }

    // --- Hashes ---
    fn hset(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        field: &str,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "HSET", vec![str_arg(key), str_arg(field), value])
    }
    fn hget(slf: Py<Self>, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "HGET", vec![str_arg(key), str_arg(field)])
    }
    #[pyo3(signature = (key, *fields))]
    fn hdel(slf: Py<Self>, py: Python<'_>, key: &str, fields: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(fields);
        Pipeline::buf_or_dispatch(slf, py, "HDEL", args)
    }
    fn hgetall(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "HGETALL", vec![str_arg(key)])
    }
    fn hexists(slf: Py<Self>, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "HEXISTS", vec![str_arg(key), str_arg(field)])
    }
    fn hlen(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "HLEN", vec![str_arg(key)])
    }
    fn hincrby(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        field: &str,
        by: i64,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(
            slf,
            py,
            "HINCRBY",
            vec![str_arg(key), str_arg(field), i64_arg(by)],
        )
    }
    fn hincrbyfloat(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        field: &str,
        by: f64,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(
            slf,
            py,
            "HINCRBYFLOAT",
            vec![str_arg(key), str_arg(field), f64_arg(by)],
        )
    }

    // --- Sets ---
    #[pyo3(signature = (key, *members))]
    fn sadd(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(members);
        Pipeline::buf_or_dispatch(slf, py, "SADD", args)
    }
    #[pyo3(signature = (key, *members))]
    fn srem(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(members);
        Pipeline::buf_or_dispatch(slf, py, "SREM", args)
    }
    fn smembers(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "SMEMBERS", vec![str_arg(key)])
    }
    fn sismember(slf: Py<Self>, py: Python<'_>, key: &str, member: Vec<u8>) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "SISMEMBER", vec![str_arg(key), member])
    }
    fn scard(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "SCARD", vec![str_arg(key)])
    }

    // --- Sorted sets ---
    fn zincrby(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        by: f64,
        member: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "ZINCRBY", vec![str_arg(key), f64_arg(by), member])
    }
    fn zcard(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "ZCARD", vec![str_arg(key)])
    }
    fn zscore(slf: Py<Self>, py: Python<'_>, key: &str, member: Vec<u8>) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "ZSCORE", vec![str_arg(key), member])
    }

    // --- Admin ---
    fn ping(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "PING", vec![])
    }
    fn echo(slf: Py<Self>, py: Python<'_>, message: Vec<u8>) -> PyResult<Py<PyAny>> {
        Pipeline::buf_or_dispatch(slf, py, "ECHO", vec![message])
    }
}

// =========================================================================
// AsyncPipeline pyclass (async)
// =========================================================================

#[pyclass(module = "redis_rs_py._driver")]
pub struct AsyncPipeline {
    conn: ValkeyConn,
    state: Mutex<PipelineState>,
}

impl AsyncPipeline {
    pub fn new(conn: ValkeyConn, transaction: bool) -> Self {
        Self {
            conn,
            state: Mutex::new(PipelineState::new(transaction)),
        }
    }

    fn buffer_cmd_inner(
        slf: Py<Self>,
        py: Python<'_>,
        name: &str,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let this = slf.bind(py).borrow();
        {
            let s = this.state.lock().unwrap();
            if s.closed {
                return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
            }
        }
        this.state
            .lock()
            .unwrap()
            .commands
            .push((name.to_string(), args));
        drop(this);
        Ok(slf.into_any())
    }

    fn immediate_dispatch_inner(
        slf: Py<Self>,
        py: Python<'_>,
        cmd_name: &'static str,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let mut taken = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved.take()
            });
            let res = match taken.as_mut() {
                Some(r) => r.dispatch_immediate(cmd_name, &args).await,
                None => Err(redis::RedisError::from((
                    redis::ErrorKind::Client,
                    "internal error",
                    "immediate dispatch without a reservation; call awatch() first".to_string(),
                ))),
            };
            Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved = taken;
            });
            let raw = match res {
                Ok(v) => RawResult::Value(v),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }
}

#[pymethods]
impl AsyncPipeline {
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let asyncio = py.import("asyncio")?;
        asyncio.call_method1("sleep", (0.0_f64, slf.into_pyobject(py)?))
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__(
        slf: Py<Self>,
        py: Python<'_>,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::aclose(slf, py)
    }

    fn __len__(&self) -> usize {
        self.state.lock().unwrap().commands.len()
    }

    fn __bool__(&self) -> bool {
        true
    }

    fn aclose(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let reserved = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                let mut s = this.state.lock().unwrap();
                let r = s.reserved.take();
                s.commands.clear();
                s.watched_keys.clear();
                s.watching = false;
                s.explicit_transaction = false;
                s.closed = true;
                r
            });
            if let Some(mut r) = reserved {
                let _ = r.unwatch_if_needed().await;
            }
            let _ = tx.send(RawResult::Nil);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    fn reset(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        AsyncPipeline::aclose(slf, py)
    }

    #[pyo3(signature = (*keys))]
    fn awatch(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        let conn = {
            let this = slf.bind(py).borrow();
            this.conn.clone()
        };
        get_runtime().spawn(async move {
            let result: RawResult = async {
                let (closed, explicit_tx, need_reserve) = Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    let s = this.state.lock().unwrap();
                    (s.closed, s.explicit_transaction, s.reserved.is_none())
                });
                if closed {
                    return RawResult::Error(
                        crate::exceptions::ExceptionClass::RedisError,
                        "pipeline is closed".to_string(),
                    );
                }
                if explicit_tx {
                    return RawResult::Error(
                        crate::exceptions::ExceptionClass::RedisError,
                        "Cannot issue a WATCH after a MULTI".to_string(),
                    );
                }
                if need_reserve {
                    let r = match conn.reserve_connection().await {
                        Ok(r) => r,
                        Err(e) => {
                            return RawResult::Error(
                                crate::exceptions::ExceptionClass::ConnectionError,
                                e,
                            );
                        }
                    };
                    Python::attach(|py| {
                        let this = slf_clone.bind(py).borrow();
                        this.state.lock().unwrap().reserved = Some(r);
                    });
                }
                let mut taken = Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    this.state.lock().unwrap().reserved.take()
                })
                .expect("reserved must be present");
                let res = taken.watch(&keys).await;
                Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    this.state.lock().unwrap().reserved = Some(taken);
                });
                if let Err(e) = res {
                    return crate::errors::classify(e);
                }
                Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    let mut s = this.state.lock().unwrap();
                    s.watching = true;
                    s.watched_keys.extend(keys.iter().cloned());
                });
                RawResult::Bool(true)
            }
            .await;
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    fn aunwatch(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let mut taken = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved.take()
            });
            let result = if let Some(ref mut r) = taken {
                r.unwatch_if_needed().await.map(|_| ())
            } else {
                Ok(())
            };
            Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                let mut s = this.state.lock().unwrap();
                s.reserved = taken;
                s.watched_keys.clear();
                s.watching = false;
            });
            let raw = match result {
                Ok(()) => RawResult::Bool(true),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    fn multi(&self) -> PyResult<()> {
        let mut s = self.state.lock().unwrap();
        if s.closed {
            return Err(PyErr::new::<RedisError, _>("pipeline is closed"));
        }
        if s.explicit_transaction {
            return Err(PyErr::new::<RedisError, _>(
                "Cannot issue nested calls to MULTI",
            ));
        }
        if !s.commands.is_empty() && !s.watching {
            return Err(PyErr::new::<RedisError, _>(
                "Commands without an initial WATCH have already been issued",
            ));
        }
        s.explicit_transaction = true;
        Ok(())
    }

    fn adiscard(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        get_runtime().spawn(async move {
            let mut taken = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved.take()
            });
            if let Some(ref mut r) = taken {
                let _ = r.unwatch_if_needed().await;
            }
            Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                let mut s = this.state.lock().unwrap();
                s.reserved = taken;
                s.commands.clear();
                s.explicit_transaction = false;
                s.watched_keys.clear();
                s.watching = false;
            });
            let _ = tx.send(RawResult::Nil);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    fn aget_immediate(slf: Py<Self>, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        AsyncPipeline::immediate_dispatch_inner(slf, py, "GET", vec![key.into_bytes()])
    }

    fn aset_immediate(
        slf: Py<Self>,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::immediate_dispatch_inner(slf, py, "SET", vec![key.into_bytes(), value])
    }

    fn aexecute(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let slf_clone = slf.clone_ref(py);
        let conn = {
            let this = slf.bind(py).borrow();
            this.conn.clone()
        };
        get_runtime().spawn(async move {
            let (commands, transaction, watching, explicit_transaction, closed, watched_keys) =
                Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    let s = this.state.lock().unwrap();
                    (
                        s.commands.clone(),
                        s.transaction,
                        s.watching,
                        s.explicit_transaction,
                        s.closed,
                        s.watched_keys.clone(),
                    )
                });

            if closed {
                let _ = tx.send(RawResult::Error(
                    crate::exceptions::ExceptionClass::RedisError,
                    "pipeline is closed".to_string(),
                ));
                return;
            }
            if commands.is_empty() && !watching && !explicit_transaction {
                let _ = tx.send(RawResult::Value(redis::Value::Array(Vec::new())));
                return;
            }

            // Only use the WATCH/EXEC reserved-connection path when a
            // reservation was actually made (i.e., awatch() was called).
            let has_reserved = Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                this.state.lock().unwrap().reserved.is_some()
            });

            if watching || (explicit_transaction && has_reserved) {
                let mut taken = Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    this.state.lock().unwrap().reserved.take()
                });
                let res = match taken.as_mut() {
                    Some(r) => r.pipeline_exec_watched(&watched_keys, commands).await,
                    None => Err(redis::RedisError::from((
                        redis::ErrorKind::Client,
                        "internal error",
                        "aexecute without reservation".to_string(),
                    ))),
                };
                Python::attach(|py| {
                    let this = slf_clone.bind(py).borrow();
                    let mut s = this.state.lock().unwrap();
                    s.commands.clear();
                    s.watched_keys.clear();
                    s.watching = false;
                    s.explicit_transaction = false;
                    s.reserved = None;
                });
                let raw = match res {
                    Ok(WatchedExecResult::Ok(items)) => {
                        RawResult::Value(redis::Value::Array(items))
                    }
                    Ok(WatchedExecResult::WatchAborted) => RawResult::Error(
                        crate::exceptions::ExceptionClass::WatchError,
                        "Watched variable changed.".to_string(),
                    ),
                    Err(e) => crate::errors::classify(e),
                };
                let _ = tx.send(raw);
                return;
            }

            // Plain buffered path (no WATCH). transaction flag = constructor
            // default OR explicit multi() without a prior watch().
            let run_as_transaction = transaction || explicit_transaction;
            let mut c = conn;
            let res = c.pipeline_exec(commands, run_as_transaction).await;
            Python::attach(|py| {
                let this = slf_clone.bind(py).borrow();
                let mut s = this.state.lock().unwrap();
                s.commands.clear();
                s.explicit_transaction = false;
            });
            let raw = match res {
                Ok(items) => RawResult::Value(redis::Value::Array(items)),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }
}

// =========================================================================
// AsyncPipeline buffered command methods (explicit — PyO3 0.28 bans macros
// in #[pymethods] blocks).
// =========================================================================

#[pymethods]
impl AsyncPipeline {
    // --- Strings ---
    fn set(slf: Py<Self>, py: Python<'_>, key: &str, value: Vec<u8>) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "SET", vec![str_arg(key), value])
    }
    fn get(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "GET", vec![str_arg(key)])
    }
    fn getdel(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "GETDEL", vec![str_arg(key)])
    }
    fn append(slf: Py<Self>, py: Python<'_>, key: &str, value: Vec<u8>) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "APPEND", vec![str_arg(key), value])
    }
    fn strlen(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "STRLEN", vec![str_arg(key)])
    }
    fn incr(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "INCR", vec![str_arg(key)])
    }
    fn incrby(slf: Py<Self>, py: Python<'_>, key: &str, by: i64) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "INCRBY", vec![str_arg(key), i64_arg(by)])
    }
    fn incrbyfloat(slf: Py<Self>, py: Python<'_>, key: &str, by: f64) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "INCRBYFLOAT", vec![str_arg(key), f64_arg(by)])
    }
    fn decr(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "DECR", vec![str_arg(key)])
    }
    fn decrby(slf: Py<Self>, py: Python<'_>, key: &str, by: i64) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "DECRBY", vec![str_arg(key), i64_arg(by)])
    }
    fn setrange(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        offset: i64,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(
            slf,
            py,
            "SETRANGE",
            vec![str_arg(key), i64_arg(offset), value],
        )
    }
    fn getrange(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        start: i64,
        end: i64,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(
            slf,
            py,
            "GETRANGE",
            vec![str_arg(key), i64_arg(start), i64_arg(end)],
        )
    }
    fn rename(slf: Py<Self>, py: Python<'_>, key: &str, new_key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "RENAME", vec![str_arg(key), str_arg(new_key)])
    }
    fn renamenx(slf: Py<Self>, py: Python<'_>, key: &str, new_key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "RENAMENX", vec![str_arg(key), str_arg(new_key)])
    }
    fn typ(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "TYPE", vec![str_arg(key)])
    }
    fn expire(slf: Py<Self>, py: Python<'_>, key: &str, seconds: i64) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "EXPIRE", vec![str_arg(key), i64_arg(seconds)])
    }
    fn pexpire(slf: Py<Self>, py: Python<'_>, key: &str, millis: i64) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "PEXPIRE", vec![str_arg(key), i64_arg(millis)])
    }
    fn ttl(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "TTL", vec![str_arg(key)])
    }
    fn pttl(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "PTTL", vec![str_arg(key)])
    }
    fn persist(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "PERSIST", vec![str_arg(key)])
    }

    // --- Key multi-forms ---
    #[pyo3(signature = (*keys))]
    fn delete(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let args: Vec<Vec<u8>> = keys.into_iter().map(|s| s.into_bytes()).collect();
        AsyncPipeline::buffer_cmd_inner(slf, py, "DEL", args)
    }
    #[pyo3(signature = (*keys))]
    fn unlink(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let args: Vec<Vec<u8>> = keys.into_iter().map(|s| s.into_bytes()).collect();
        AsyncPipeline::buffer_cmd_inner(slf, py, "UNLINK", args)
    }
    #[pyo3(signature = (*keys))]
    fn exists(slf: Py<Self>, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let args: Vec<Vec<u8>> = keys.into_iter().map(|s| s.into_bytes()).collect();
        AsyncPipeline::buffer_cmd_inner(slf, py, "EXISTS", args)
    }

    // --- Lists ---
    #[pyo3(signature = (key, *values))]
    fn lpush(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        values: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(values);
        AsyncPipeline::buffer_cmd_inner(slf, py, "LPUSH", args)
    }
    #[pyo3(signature = (key, *values))]
    fn rpush(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        values: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(values);
        AsyncPipeline::buffer_cmd_inner(slf, py, "RPUSH", args)
    }
    fn lpop(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "LPOP", vec![str_arg(key)])
    }
    fn rpop(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "RPOP", vec![str_arg(key)])
    }
    fn llen(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "LLEN", vec![str_arg(key)])
    }
    fn lrange(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(
            slf,
            py,
            "LRANGE",
            vec![str_arg(key), i64_arg(start), i64_arg(stop)],
        )
    }
    fn lindex(slf: Py<Self>, py: Python<'_>, key: &str, index: i64) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "LINDEX", vec![str_arg(key), i64_arg(index)])
    }
    fn lrem(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        count: i64,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "LREM", vec![str_arg(key), i64_arg(count), value])
    }
    fn ltrim(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(
            slf,
            py,
            "LTRIM",
            vec![str_arg(key), i64_arg(start), i64_arg(stop)],
        )
    }
    fn lset(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        index: i64,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "LSET", vec![str_arg(key), i64_arg(index), value])
    }

    // --- Hashes ---
    fn hset(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        field: &str,
        value: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "HSET", vec![str_arg(key), str_arg(field), value])
    }
    fn hget(slf: Py<Self>, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "HGET", vec![str_arg(key), str_arg(field)])
    }
    #[pyo3(signature = (key, *fields))]
    fn hdel(slf: Py<Self>, py: Python<'_>, key: &str, fields: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(fields);
        AsyncPipeline::buffer_cmd_inner(slf, py, "HDEL", args)
    }
    fn hgetall(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "HGETALL", vec![str_arg(key)])
    }
    fn hexists(slf: Py<Self>, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "HEXISTS", vec![str_arg(key), str_arg(field)])
    }
    fn hlen(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "HLEN", vec![str_arg(key)])
    }
    fn hincrby(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        field: &str,
        by: i64,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(
            slf,
            py,
            "HINCRBY",
            vec![str_arg(key), str_arg(field), i64_arg(by)],
        )
    }
    fn hincrbyfloat(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        field: &str,
        by: f64,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(
            slf,
            py,
            "HINCRBYFLOAT",
            vec![str_arg(key), str_arg(field), f64_arg(by)],
        )
    }

    // --- Sets ---
    #[pyo3(signature = (key, *members))]
    fn sadd(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(members);
        AsyncPipeline::buffer_cmd_inner(slf, py, "SADD", args)
    }
    #[pyo3(signature = (key, *members))]
    fn srem(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let mut args = vec![str_arg(key)];
        args.extend(members);
        AsyncPipeline::buffer_cmd_inner(slf, py, "SREM", args)
    }
    fn smembers(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "SMEMBERS", vec![str_arg(key)])
    }
    fn sismember(slf: Py<Self>, py: Python<'_>, key: &str, member: Vec<u8>) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "SISMEMBER", vec![str_arg(key), member])
    }
    fn scard(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "SCARD", vec![str_arg(key)])
    }

    // --- Sorted sets ---
    fn zincrby(
        slf: Py<Self>,
        py: Python<'_>,
        key: &str,
        by: f64,
        member: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "ZINCRBY", vec![str_arg(key), f64_arg(by), member])
    }
    fn zcard(slf: Py<Self>, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "ZCARD", vec![str_arg(key)])
    }
    fn zscore(slf: Py<Self>, py: Python<'_>, key: &str, member: Vec<u8>) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "ZSCORE", vec![str_arg(key), member])
    }

    // --- Admin ---
    fn ping(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "PING", vec![])
    }
    fn echo(slf: Py<Self>, py: Python<'_>, message: Vec<u8>) -> PyResult<Py<PyAny>> {
        AsyncPipeline::buffer_cmd_inner(slf, py, "ECHO", vec![message])
    }
}

// =========================================================================
// transaction() / atransaction() helpers
// =========================================================================

/// Sync transaction helper — mirrors redis-py's Redis.transaction().
pub(crate) fn transaction_helper(
    py: Python<'_>,
    conn: ValkeyConn,
    func: Py<PyAny>,
    watches: Vec<String>,
    value_from_callable: bool,
    watch_delay: Option<f64>,
) -> PyResult<Py<PyAny>> {
    loop {
        let pipe = Py::new(py, Pipeline::new(conn.clone(), true))?;

        let res: PyResult<Py<PyAny>> = (|| {
            if !watches.is_empty() {
                pipe.bind(py).borrow().watch(py, watches.clone())?;
            }
            let func_value = func.call1(py, (pipe.clone_ref(py),))?;
            let exec_value = pipe.bind(py).borrow().execute(py)?;
            if value_from_callable {
                Ok(func_value)
            } else {
                Ok(exec_value)
            }
        })();

        let _ = pipe.bind(py).borrow().reset(py);

        match res {
            Ok(v) => return Ok(v),
            Err(e) => {
                if e.is_instance_of::<WatchError>(py) {
                    if let Some(d) = watch_delay
                        && d > 0.0
                    {
                        py.detach(|| std::thread::sleep(std::time::Duration::from_secs_f64(d)));
                    }
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// Async transaction helper — returns a coroutine that loops on WatchError.
pub(crate) fn atransaction_helper(
    py: Python<'_>,
    conn: ValkeyConn,
    func: Py<PyAny>,
    watches: Vec<String>,
    value_from_callable: bool,
    watch_delay: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let asyncio = py.import("asyncio")?;
    let watch_error_cls = py.get_type::<WatchError>();

    let pipe_factory = PipeFactory::new(conn);
    let pipe_factory_obj = Py::new(py, pipe_factory)?;

    let locals = pyo3::types::PyDict::new(py);
    locals.set_item("pipe_factory", pipe_factory_obj)?;
    locals.set_item("func", func.bind(py))?;
    locals.set_item("watches", watches)?;
    locals.set_item("value_from_callable", value_from_callable)?;
    locals.set_item("watch_delay", watch_delay)?;
    locals.set_item("WatchError", watch_error_cls)?;
    locals.set_item("asyncio", asyncio)?;
    locals.set_item("inspect", py.import("inspect")?)?;

    let src = std::ffi::CString::new(
        r#"
async def _go():
    while True:
        pipe = pipe_factory()
        try:
            if watches:
                await pipe.awatch(*watches)
            res = func(pipe)
            if inspect.iscoroutine(res):
                func_value = await res
            else:
                func_value = res
            exec_value = await pipe.aexecute()
            return func_value if value_from_callable else exec_value
        except WatchError:
            if watch_delay and watch_delay > 0:
                await asyncio.sleep(watch_delay)
            continue
        finally:
            await pipe.aclose()

_coro = _go()
"#,
    )
    .unwrap();

    // Pass `locals` as both globals and locals so that the async def's
    // `__globals__` is the same dict containing pipe_factory, func, etc.
    // If globals=None, CPython sets __globals__ to the current module dict,
    // which doesn't have those names → NameError inside the coroutine.
    py.run(src.as_c_str(), Some(&locals), Some(&locals))?;
    let coro = locals.get_item("_coro")?.unwrap();
    Ok(coro.into_any().unbind())
}

/// Helper pyclass that acts as a factory for `AsyncPipeline` instances,
/// capturing a `ValkeyConn` so it can be called from Python as `pipe_factory()`.
#[pyclass(module = "redis_rs_py._driver")]
struct PipeFactory {
    conn: ValkeyConn,
}

impl PipeFactory {
    fn new(conn: ValkeyConn) -> Self {
        Self { conn }
    }
}

#[pymethods]
impl PipeFactory {
    fn __call__(&self, py: Python<'_>) -> PyResult<Py<AsyncPipeline>> {
        Py::new(py, AsyncPipeline::new(self.conn.clone(), true))
    }
}
