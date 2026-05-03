// String / key commands — impl Redis (sync) + impl AsyncRedis (async).
//
// Sync methods keep their original names and bodies.
// Async methods: original `a<cmd>` bodies are exposed as `<cmd>` on AsyncRedis.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

use crate::async_bridge::RawResult;
use crate::errors::{classify_error, to_py_err};
use crate::exceptions::DataError;
use crate::facade::asyncio_mod::AsyncRedis;
use crate::facade::sync::Redis;
use crate::helpers::py_opt_bytes;
use crate::raw_result::IntoRawResult;
use crate::{async_op, sync_op};

/// Normalise the variadic `*keys` tuple that `mget` receives.
///
/// Redis-py supports two call styles:
///   `mget("a", "b")` — multiple positional strings
///   `mget(["a", "b"])` — a single list argument
///
/// PyO3's `*keys` collects all positional args into a `PyTuple`. If that
/// tuple contains exactly one element that is itself a list or tuple, we
/// unwrap it so both call styles produce the same `Vec<String>`.
fn flatten_mget_keys(keys: &Bound<'_, PyTuple>) -> PyResult<Vec<String>> {
    if keys.len() == 1 {
        let first = keys.get_item(0)?;
        if first.is_instance_of::<PyList>() || first.is_instance_of::<PyTuple>() {
            return first
                .try_iter()?
                .map(|item| item?.extract::<String>())
                .collect();
        }
    }
    keys.iter().map(|k| k.extract::<String>()).collect()
}

// =========================================================================
// Validation helpers shared by sync + async SET paths.
// =========================================================================

fn validate_set_kwargs(
    ex: Option<u64>,
    px: Option<u64>,
    exat: Option<i64>,
    pxat: Option<i64>,
    nx: bool,
    xx: bool,
    keepttl: bool,
) -> PyResult<()> {
    if nx && xx {
        return Err(PyErr::new::<DataError, _>(
            "nx and xx options are mutually exclusive",
        ));
    }
    let ttl_set = [ex.is_some(), px.is_some(), exat.is_some(), pxat.is_some()]
        .into_iter()
        .filter(|b| *b)
        .count();
    if ttl_set > 1 {
        return Err(PyErr::new::<DataError, _>(
            "only one of ex, px, exat, pxat may be set",
        ));
    }
    if keepttl && ttl_set > 0 {
        return Err(PyErr::new::<DataError, _>(
            "keepttl is mutually exclusive with ex/px/exat/pxat",
        ));
    }
    Ok(())
}

fn validate_expire_flags(nx: bool, xx: bool, gt: bool, lt: bool) -> PyResult<()> {
    let count = [nx, xx, gt, lt].into_iter().filter(|b| *b).count();
    if count > 1 {
        return Err(PyErr::new::<DataError, _>(
            "at most one of nx, xx, gt, lt may be set on EXPIRE-family commands",
        ));
    }
    Ok(())
}

fn set_value_to_py(py: Python<'_>, v: redis::Value, get: bool) -> PyResult<Py<PyAny>> {
    match v {
        redis::Value::Okay => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::SimpleString(s) if s == "OK" => {
            Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
        }
        redis::Value::Nil => Ok(py.None()),
        redis::Value::BulkString(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        _ if get => Ok(py.None()),
        _ => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
    }
}

// =========================================================================
// Sync — redis_rs_py.Redis
// =========================================================================

#[pymethods]
impl Redis {
    // ----- GET / SET -------------------------------------------------------

    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        use redis::AsyncCommands;
        let r: Result<Option<Vec<u8>>, _> = sync_op!(py, self, conn, async {
            crate::conn_method!(&mut *conn, c, c.get(key))
        });
        Ok(py_opt_bytes(py, r.map_err(crate::errors::to_py_err)?))
    }

    #[pyo3(signature = (
        name,
        value,
        *,
        ex = None,
        px = None,
        nx = false,
        xx = false,
        keepttl = false,
        get = false,
        exat = None,
        pxat = None,
    ))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn set(
        &self,
        py: Python<'_>,
        name: &str,
        value: &[u8],
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
        exat: Option<i64>,
        pxat: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        validate_set_kwargs(ex, px, exat, pxat, nx, xx, keepttl)?;
        let value = value.to_vec();
        let r: redis::RedisResult<redis::Value> = sync_op!(py, self, conn, async {
            conn.set_full(name, value, ex, px, exat, pxat, nx, xx, keepttl, get)
                .await
        });
        let v = r.map_err(to_py_err)?;
        set_value_to_py(py, v, get)
    }

    // ----- DELETE (varargs) ------------------------------------------------

    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<i64> {
        use redis::AsyncCommands;
        if keys.is_empty() {
            return Ok(0);
        }
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            crate::conn_method!(&mut *conn, c, c.del(&keys))
        });
        r.map_err(to_py_err)
    }

    // ----- PING ------------------------------------------------------------

    #[pyo3(signature = (*, message=None))]
    fn ping(&self, py: Python<'_>, message: Option<String>) -> PyResult<Py<PyAny>> {
        use crate::helpers::py_bool;
        match message {
            None => {
                let r: redis::RedisResult<String> = sync_op!(py, self, conn, async {
                    crate::dispatch_cmd!(&mut *conn, redis::cmd("PING"))
                });
                match r {
                    Ok(s) => py_bool(py, s == "PONG"),
                    Err(e) => Err(to_py_err(e)),
                }
            }
            Some(msg) => {
                let mut cmd = redis::cmd("PING");
                cmd.arg(msg);
                let r: redis::RedisResult<Vec<u8>> = sync_op!(py, self, conn, async {
                    crate::dispatch_cmd!(&mut *conn, cmd)
                });
                let bytes = r.map_err(to_py_err)?;
                Ok(PyBytes::new(py, &bytes).into_any().unbind())
            }
        }
    }

    // ----- GETEX -----------------------------------------------------------

    #[pyo3(signature = (
        name,
        *,
        ex = None,
        px = None,
        exat = None,
        pxat = None,
        persist = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn getex(
        &self,
        py: Python<'_>,
        name: &str,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        persist: bool,
    ) -> PyResult<Py<PyAny>> {
        let ttl_set = [ex.is_some(), px.is_some(), exat.is_some(), pxat.is_some()]
            .into_iter()
            .filter(|b| *b)
            .count();
        if ttl_set > 1 {
            return Err(PyErr::new::<DataError, _>(
                "only one of ex, px, exat, pxat may be set",
            ));
        }
        if persist && ttl_set > 0 {
            return Err(PyErr::new::<DataError, _>(
                "persist is mutually exclusive with ex/px/exat/pxat",
            ));
        }
        let r: redis::RedisResult<Option<Vec<u8>>> = sync_op!(py, self, conn, async {
            conn.getex(name, ex, px, exat, pxat, persist).await
        });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    // ----- GETDEL ----------------------------------------------------------

    fn getdel(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.getdel(name).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    // ----- GETRANGE --------------------------------------------------------

    fn getrange(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<u8>> = sync_op!(py, self, conn, async {
            conn.getrange(name, start, end).await
        });
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    // ----- SETRANGE --------------------------------------------------------

    fn setrange(&self, py: Python<'_>, name: &str, offset: i64, value: &[u8]) -> PyResult<i64> {
        sync_op!(py, self, conn, async {
            conn.setrange(name, offset, value).await
        })
        .map_err(to_py_err)
    }

    // ----- STRLEN ----------------------------------------------------------

    fn strlen(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.strlen(name).await }).map_err(to_py_err)
    }

    // ----- APPEND ----------------------------------------------------------

    fn append(&self, py: Python<'_>, name: &str, value: &[u8]) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.append(name, value).await }).map_err(to_py_err)
    }

    // ----- MGET ------------------------------------------------------------

    #[pyo3(signature = (*keys))]
    fn mget<'py>(&self, py: Python<'py>, keys: &Bound<'py, PyTuple>) -> PyResult<Py<PyAny>> {
        let key_strs = flatten_mget_keys(keys)?;
        let r: redis::RedisResult<Vec<Option<Vec<u8>>>> =
            sync_op!(py, self, conn, async { conn.mget(&key_strs).await });
        let v = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = v
            .into_iter()
            .map(|o| match o {
                Some(b) => PyBytes::new(py, &b).into_any().unbind(),
                None => py.None(),
            })
            .collect();
        Ok(pyo3::types::PyList::new(py, py_items)?.into_any().unbind())
    }

    // ----- MSET ------------------------------------------------------------

    fn mset(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<()> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        sync_op!(py, self, conn, async { conn.mset(&entries).await }).map_err(to_py_err)
    }

    // ----- MSETNX ----------------------------------------------------------

    fn msetnx(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<bool> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        sync_op!(py, self, conn, async { conn.msetnx(&entries).await }).map_err(to_py_err)
    }

    // ----- INCR / INCRBY / INCRBYFLOAT ------------------------------------

    fn incr(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.incr(name).await }).map_err(to_py_err)
    }

    fn incrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.incrby(name, amount).await }).map_err(to_py_err)
    }

    fn incrbyfloat(&self, py: Python<'_>, name: &str, amount: f64) -> PyResult<f64> {
        sync_op!(py, self, conn, async {
            conn.incrbyfloat(name, amount).await
        })
        .map_err(to_py_err)
    }

    // ----- DECR / DECRBY --------------------------------------------------

    fn decr(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.decr(name).await }).map_err(to_py_err)
    }

    fn decrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.decrby(name, amount).await }).map_err(to_py_err)
    }

    // ----- EXISTS (variadic) -----------------------------------------------

    #[pyo3(signature = (*names))]
    fn exists(&self, py: Python<'_>, names: Vec<String>) -> PyResult<i64> {
        if names.is_empty() {
            return Ok(0);
        }
        sync_op!(py, self, conn, async { conn.exists_many(&names).await }).map_err(to_py_err)
    }

    // ----- UNLINK (variadic) -----------------------------------------------

    #[pyo3(signature = (*names))]
    fn unlink(&self, py: Python<'_>, names: Vec<String>) -> PyResult<i64> {
        if names.is_empty() {
            return Ok(0);
        }
        sync_op!(py, self, conn, async { conn.unlink_many(&names).await }).map_err(to_py_err)
    }

    // ----- EXPIRE family --------------------------------------------------

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn expire(
        &self,
        py: Python<'_>,
        name: &str,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.expire_full(name, time, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn pexpire(
        &self,
        py: Python<'_>,
        name: &str,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.pexpire_full(name, time, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn expireat(
        &self,
        py: Python<'_>,
        name: &str,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.expireat_full(name, when, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn pexpireat(
        &self,
        py: Python<'_>,
        name: &str,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.pexpireat_full(name, when, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    // ----- TTL family -----------------------------------------------------

    fn ttl(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.ttl(name).await }).map_err(to_py_err)
    }

    fn pttl(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.pttl(name).await }).map_err(to_py_err)
    }

    fn expiretime(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.expiretime(name).await }).map_err(to_py_err)
    }

    fn pexpiretime(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.pexpiretime(name).await }).map_err(to_py_err)
    }

    fn persist(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, async { conn.persist(name).await }).map_err(to_py_err)
    }

    // ----- RENAME ---------------------------------------------------------

    fn rename(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        sync_op!(py, self, conn, async { conn.rename(src, dst).await }).map_err(to_py_err)
    }

    fn renamenx(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, async { conn.renamenx(src, dst).await }).map_err(to_py_err)
    }

    // ----- TYPE -----------------------------------------------------------

    #[pyo3(name = "type")]
    fn type_(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        sync_op!(py, self, conn, async { conn.key_type(name).await }).map_err(to_py_err)
    }

    fn key_type(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        sync_op!(py, self, conn, async { conn.key_type(name).await }).map_err(to_py_err)
    }

    // ----- COPY -----------------------------------------------------------

    #[pyo3(signature = (source, destination, *, db = None, replace = false))]
    fn copy(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        db: Option<i64>,
        replace: bool,
    ) -> PyResult<bool> {
        sync_op!(py, self, conn, async {
            conn.copy(source, destination, db, replace).await
        })
        .map_err(to_py_err)
    }

    // ----- DUMP -----------------------------------------------------------

    fn dump(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.dump(name).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    // ----- RESTORE --------------------------------------------------------

    #[pyo3(signature = (
        name,
        ttl,
        value,
        *,
        replace = false,
        absttl = false,
        idletime = None,
        frequency = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn restore(
        &self,
        py: Python<'_>,
        name: &str,
        ttl: i64,
        value: &[u8],
        replace: bool,
        absttl: bool,
        idletime: Option<u64>,
        frequency: Option<u64>,
    ) -> PyResult<bool> {
        sync_op!(py, self, conn, async {
            conn.restore(name, ttl, value, replace, absttl, idletime, frequency)
                .await
        })
        .map_err(to_py_err)?;
        Ok(true)
    }
}

// =========================================================================
// Async — redis_rs_py.asyncio.Redis
// =========================================================================

#[pymethods]
impl AsyncRedis {
    // ----- GET / SET -------------------------------------------------------

    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            use redis::AsyncCommands;
            let r: redis::RedisResult<Option<Vec<u8>>> =
                crate::conn_method!(&mut *conn, c, c.get(&key));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (
        name,
        value,
        *,
        ex = None,
        px = None,
        nx = false,
        xx = false,
        keepttl = false,
        get = false,
        exat = None,
        pxat = None,
    ))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn set(
        &self,
        py: Python<'_>,
        name: &str,
        value: &[u8],
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
        exat: Option<i64>,
        pxat: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        validate_set_kwargs(ex, px, exat, pxat, nx, xx, keepttl)?;
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<redis::Value> = conn
                .set_full(&name, value, ex, px, exat, pxat, nx, xx, keepttl, get)
                .await;
            match r {
                Ok(redis::Value::Okay) => RawResult::Bool(true),
                Ok(redis::Value::SimpleString(ref s)) if s == "OK" => RawResult::Bool(true),
                Ok(redis::Value::Nil) => RawResult::Nil,
                Ok(redis::Value::BulkString(b)) => RawResult::OptBytes(Some(b)),
                Ok(_) if get => RawResult::Nil,
                Ok(_) => RawResult::Bool(true),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            }
        })
    }

    // ----- DELETE (varargs) ------------------------------------------------

    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if keys.is_empty() {
                return RawResult::Int(0);
            }
            use redis::AsyncCommands;
            let r: redis::RedisResult<i64> = crate::conn_method!(&mut *conn, c, c.del(&keys));
            r.into_raw_result()
        })
    }

    // ----- PING ------------------------------------------------------------

    #[pyo3(signature = (*, message=None))]
    fn ping(&self, py: Python<'_>, message: Option<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            match message {
                None => {
                    let r: redis::RedisResult<String> =
                        crate::dispatch_cmd!(&mut *conn, redis::cmd("PING"));
                    match r {
                        Ok(s) => RawResult::Bool(s == "PONG"),
                        Err(e) => crate::errors::classify(e),
                    }
                }
                Some(msg) => {
                    let mut cmd = redis::cmd("PING");
                    cmd.arg(&msg);
                    let r: redis::RedisResult<Vec<u8>> = crate::dispatch_cmd!(&mut *conn, cmd);
                    match r {
                        Ok(b) => RawResult::OptBytes(Some(b)),
                        Err(e) => crate::errors::classify(e),
                    }
                }
            }
        })
    }

    // ----- GETEX -----------------------------------------------------------

    #[pyo3(signature = (
        name,
        *,
        ex = None,
        px = None,
        exat = None,
        pxat = None,
        persist = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn getex(
        &self,
        py: Python<'_>,
        name: &str,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        persist: bool,
    ) -> PyResult<Py<PyAny>> {
        let ttl_set = [ex.is_some(), px.is_some(), exat.is_some(), pxat.is_some()]
            .into_iter()
            .filter(|b| *b)
            .count();
        if ttl_set > 1 {
            return Err(PyErr::new::<DataError, _>(
                "only one of ex, px, exat, pxat may be set",
            ));
        }
        if persist && ttl_set > 0 {
            return Err(PyErr::new::<DataError, _>(
                "persist is mutually exclusive with ex/px/exat/pxat",
            ));
        }
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.getex(&name, ex, px, exat, pxat, persist)
                .await
                .into_raw_result()
        })
    }

    // ----- GETDEL ----------------------------------------------------------

    fn getdel(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.getdel(&name).await.into_raw_result()
        })
    }

    // ----- GETRANGE --------------------------------------------------------

    fn getrange(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            match conn.getrange(&name, start, end).await {
                Ok(b) => RawResult::OptBytes(Some(b)),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            }
        })
    }

    // ----- SETRANGE --------------------------------------------------------

    fn setrange(
        &self,
        py: Python<'_>,
        name: &str,
        offset: i64,
        value: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.setrange(&name, offset, &value).await.into_raw_result()
        })
    }

    // ----- STRLEN ----------------------------------------------------------

    fn strlen(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.strlen(&name).await.into_raw_result()
        })
    }

    // ----- APPEND ----------------------------------------------------------

    fn append(&self, py: Python<'_>, name: &str, value: &[u8]) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.append(&name, &value).await.into_raw_result()
        })
    }

    // ----- MGET ------------------------------------------------------------

    #[pyo3(signature = (*keys))]
    fn mget<'py>(&self, py: Python<'py>, keys: &Bound<'py, PyTuple>) -> PyResult<Py<PyAny>> {
        let key_strs = flatten_mget_keys(keys)?;
        async_op!(self, py, conn, async {
            conn.mget(&key_strs).await.into_raw_result()
        })
    }

    // ----- MSET ------------------------------------------------------------

    fn mset(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        async_op!(self, py, conn, async {
            conn.mset(&entries).await.into_raw_result()
        })
    }

    // ----- MSETNX ----------------------------------------------------------

    fn msetnx(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        async_op!(self, py, conn, async {
            conn.msetnx(&entries).await.into_raw_result()
        })
    }

    // ----- INCR / INCRBY / INCRBYFLOAT ------------------------------------

    fn incr(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incr(&name).await.into_raw_result()
        })
    }

    fn incrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incrby(&name, amount).await.into_raw_result()
        })
    }

    fn incrbyfloat(&self, py: Python<'_>, name: &str, amount: f64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incrbyfloat(&name, amount).await.into_raw_result()
        })
    }

    // ----- DECR / DECRBY --------------------------------------------------

    fn decr(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.decr(&name).await.into_raw_result()
        })
    }

    fn decrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.decrby(&name, amount).await.into_raw_result()
        })
    }

    // ----- EXISTS (variadic) -----------------------------------------------

    #[pyo3(signature = (*names))]
    fn exists(&self, py: Python<'_>, names: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if names.is_empty() {
                return RawResult::Int(0);
            }
            conn.exists_many(&names).await.into_raw_result()
        })
    }

    // ----- UNLINK (variadic) -----------------------------------------------

    #[pyo3(signature = (*names))]
    fn unlink(&self, py: Python<'_>, names: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if names.is_empty() {
                return RawResult::Int(0);
            }
            conn.unlink_many(&names).await.into_raw_result()
        })
    }

    // ----- EXPIRE family --------------------------------------------------

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn expire(
        &self,
        py: Python<'_>,
        name: &str,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        validate_expire_flags(nx, xx, gt, lt)?;
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.expire_full(&name, time, nx, xx, gt, lt)
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn pexpire(
        &self,
        py: Python<'_>,
        name: &str,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        validate_expire_flags(nx, xx, gt, lt)?;
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pexpire_full(&name, time, nx, xx, gt, lt)
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn expireat(
        &self,
        py: Python<'_>,
        name: &str,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        validate_expire_flags(nx, xx, gt, lt)?;
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.expireat_full(&name, when, nx, xx, gt, lt)
                .await
                .into_raw_result()
        })
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn pexpireat(
        &self,
        py: Python<'_>,
        name: &str,
        when: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        validate_expire_flags(nx, xx, gt, lt)?;
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pexpireat_full(&name, when, nx, xx, gt, lt)
                .await
                .into_raw_result()
        })
    }

    // ----- TTL family -----------------------------------------------------

    fn ttl(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.ttl(&name).await.into_raw_result()
        })
    }

    fn pttl(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pttl(&name).await.into_raw_result()
        })
    }

    fn expiretime(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.expiretime(&name).await.into_raw_result()
        })
    }

    fn pexpiretime(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pexpiretime(&name).await.into_raw_result()
        })
    }

    fn persist(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.persist(&name).await.into_raw_result()
        })
    }

    // ----- RENAME ---------------------------------------------------------

    fn rename(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<Py<PyAny>> {
        let src = src.to_string();
        let dst = dst.to_string();
        async_op!(self, py, conn, async {
            conn.rename(&src, &dst).await.into_raw_result()
        })
    }

    fn renamenx(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<Py<PyAny>> {
        let src = src.to_string();
        let dst = dst.to_string();
        async_op!(self, py, conn, async {
            conn.renamenx(&src, &dst).await.into_raw_result()
        })
    }

    // ----- TYPE -----------------------------------------------------------

    #[pyo3(name = "type")]
    fn type_(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.key_type(&name).await.into_raw_result()
        })
    }

    fn key_type(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.key_type(&name).await.into_raw_result()
        })
    }

    // ----- COPY -----------------------------------------------------------

    #[pyo3(signature = (source, destination, *, db = None, replace = false))]
    fn copy(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        db: Option<i64>,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        let source = source.to_string();
        let destination = destination.to_string();
        async_op!(self, py, conn, async {
            conn.copy(&source, &destination, db, replace)
                .await
                .into_raw_result()
        })
    }

    // ----- DUMP -----------------------------------------------------------

    fn dump(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.dump(&name).await.into_raw_result()
        })
    }

    // ----- RESTORE --------------------------------------------------------

    #[pyo3(signature = (
        name,
        ttl,
        value,
        *,
        replace = false,
        absttl = false,
        idletime = None,
        frequency = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn restore(
        &self,
        py: Python<'_>,
        name: &str,
        ttl: i64,
        value: &[u8],
        replace: bool,
        absttl: bool,
        idletime: Option<u64>,
        frequency: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            match conn
                .restore(&name, ttl, &value, replace, absttl, idletime, frequency)
                .await
            {
                Ok(()) => RawResult::Bool(true),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            }
        })
    }
}
