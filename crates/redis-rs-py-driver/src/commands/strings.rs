// String / key commands.
//
// Every method exists as a sync + async pair:
//   * `<cmd>(...)` — sync; releases the GIL via py.detach.
//   * `a<cmd>(...)` — async; returns a RedisRsAwaitable.
//
// Shared helpers live in driver.rs (macros) and connection.rs
// (per-command async fns on ValkeyConnInner).

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::async_bridge::RawResult;
use crate::driver::{RedisRsDriver, py_opt_bytes};
use crate::errors::{classify_error, to_py_err};
use crate::exceptions::DataError;
use crate::raw_result::IntoRawResult;
use crate::{async_op, sync_op};

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
        // Plain SET succeeded — `OK` / `SimpleString("OK")`.
        redis::Value::Okay => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::SimpleString(s) if s == "OK" => {
            Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
        }
        // Either the NX/XX predicate failed, or GET on a missing key.
        redis::Value::Nil => Ok(py.None()),
        // GET=true and key existed → previous bytes.
        redis::Value::BulkString(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        // Defensive: any other shape we treat as "OK" if we expected a write,
        // or "None" if we expected a GET. Should not happen with current Redis.
        _ if get => Ok(py.None()),
        _ => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
    }
}

#[pymethods]
impl RedisRsDriver {
    // ----- SET / aset -----------------------------------------------------

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
    fn aset(
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

    // ----- GETEX / aGETEX -------------------------------------------------

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
    fn agetex(
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

    // ----- GETDEL / aGETDEL ----------------------------------------------

    fn getdel(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.getdel(name).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn agetdel(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.getdel(&name).await.into_raw_result()
        })
    }

    // ----- GETRANGE / aGETRANGE ------------------------------------------

    fn getrange(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<u8>> = sync_op!(py, self, conn, async {
            conn.getrange(name, start, end).await
        });
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    fn agetrange(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            match conn.getrange(&name, start, end).await {
                Ok(b) => RawResult::OptBytes(Some(b)),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            }
        })
    }

    // ----- SETRANGE / aSETRANGE ------------------------------------------

    fn setrange(&self, py: Python<'_>, name: &str, offset: i64, value: &[u8]) -> PyResult<i64> {
        sync_op!(py, self, conn, async {
            conn.setrange(name, offset, value).await
        })
        .map_err(to_py_err)
    }

    fn asetrange(
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

    // ----- STRLEN / aSTRLEN ----------------------------------------------

    fn strlen(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.strlen(name).await }).map_err(to_py_err)
    }

    fn astrlen(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.strlen(&name).await.into_raw_result()
        })
    }

    // ----- APPEND / aAPPEND ----------------------------------------------

    fn append(&self, py: Python<'_>, name: &str, value: &[u8]) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.append(name, value).await }).map_err(to_py_err)
    }

    fn aappend(&self, py: Python<'_>, name: &str, value: &[u8]) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.append(&name, &value).await.into_raw_result()
        })
    }

    // ----- MGET / aMGET --------------------------------------------------

    fn mget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Option<Vec<u8>>>> =
            sync_op!(py, self, conn, async { conn.mget(&keys).await });
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

    fn amget(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            conn.mget(&keys).await.into_raw_result()
        })
    }

    // ----- MSET / aMSET --------------------------------------------------

    fn mset(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<()> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        sync_op!(py, self, conn, async { conn.mset(&entries).await }).map_err(to_py_err)
    }

    fn amset(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        async_op!(self, py, conn, async {
            conn.mset(&entries).await.into_raw_result()
        })
    }

    // ----- MSETNX / aMSETNX ----------------------------------------------

    fn msetnx(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<bool> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        sync_op!(py, self, conn, async { conn.msetnx(&entries).await }).map_err(to_py_err)
    }

    fn amsetnx(
        &self,
        py: Python<'_>,
        mapping: std::collections::HashMap<String, Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let entries: Vec<(String, Vec<u8>)> = mapping.into_iter().collect();
        async_op!(self, py, conn, async {
            conn.msetnx(&entries).await.into_raw_result()
        })
    }

    // ----- INCR / aINCR --------------------------------------------------

    fn incr(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.incrby(name, 1).await }).map_err(to_py_err)
    }

    fn aincr(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incrby(&name, 1).await.into_raw_result()
        })
    }

    // ----- INCRBY / aINCRBY ----------------------------------------------

    fn incrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.incrby(name, amount).await }).map_err(to_py_err)
    }

    fn aincrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incrby(&name, amount).await.into_raw_result()
        })
    }

    // ----- INCRBYFLOAT / aINCRBYFLOAT ------------------------------------

    fn incrbyfloat(&self, py: Python<'_>, name: &str, amount: f64) -> PyResult<f64> {
        sync_op!(py, self, conn, async {
            conn.incrbyfloat(name, amount).await
        })
        .map_err(to_py_err)
    }

    fn aincrbyfloat(&self, py: Python<'_>, name: &str, amount: f64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.incrbyfloat(&name, amount).await.into_raw_result()
        })
    }

    // ----- DECR / aDECR --------------------------------------------------

    fn decr(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.decrby(name, 1).await }).map_err(to_py_err)
    }

    fn adecr(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.decrby(&name, 1).await.into_raw_result()
        })
    }

    // ----- DECRBY / aDECRBY ----------------------------------------------

    fn decrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.decrby(name, amount).await }).map_err(to_py_err)
    }

    fn adecrby(&self, py: Python<'_>, name: &str, amount: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.decrby(&name, amount).await.into_raw_result()
        })
    }

    // ----- EXISTS / aEXISTS (variadic) -----------------------------------

    #[pyo3(signature = (*names))]
    fn exists(&self, py: Python<'_>, names: Vec<String>) -> PyResult<i64> {
        if names.is_empty() {
            return Ok(0);
        }
        sync_op!(py, self, conn, async { conn.exists_many(&names).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (*names))]
    fn aexists(&self, py: Python<'_>, names: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if names.is_empty() {
                return RawResult::Int(0);
            }
            conn.exists_many(&names).await.into_raw_result()
        })
    }

    // ----- UNLINK / aUNLINK (variadic) -----------------------------------

    #[pyo3(signature = (*names))]
    fn unlink(&self, py: Python<'_>, names: Vec<String>) -> PyResult<i64> {
        if names.is_empty() {
            return Ok(0);
        }
        sync_op!(py, self, conn, async { conn.unlink_many(&names).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (*names))]
    fn aunlink(&self, py: Python<'_>, names: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if names.is_empty() {
                return RawResult::Int(0);
            }
            conn.unlink_many(&names).await.into_raw_result()
        })
    }

    // ----- EXPIRE family -------------------------------------------------

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
    fn aexpire(
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
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.pexpire_full(name, time, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, time, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn apexpire(
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
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.expireat_full(name, when, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn aexpireat(
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
    ) -> PyResult<bool> {
        validate_expire_flags(nx, xx, gt, lt)?;
        sync_op!(py, self, conn, async {
            conn.pexpireat_full(name, when, nx, xx, gt, lt).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (name, when, *, nx = false, xx = false, gt = false, lt = false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn apexpireat(
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

    // ----- TTL / aTTL ----------------------------------------------------

    fn ttl(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.ttl(name).await }).map_err(to_py_err)
    }

    fn attl(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.ttl(&name).await.into_raw_result()
        })
    }

    fn pttl(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.pttl(name).await }).map_err(to_py_err)
    }

    fn apttl(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pttl(&name).await.into_raw_result()
        })
    }

    fn expiretime(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.expiretime(name).await }).map_err(to_py_err)
    }

    fn aexpiretime(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.expiretime(&name).await.into_raw_result()
        })
    }

    fn pexpiretime(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.pexpiretime(name).await }).map_err(to_py_err)
    }

    fn apexpiretime(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.pexpiretime(&name).await.into_raw_result()
        })
    }

    fn persist(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, async { conn.persist(name).await }).map_err(to_py_err)
    }

    fn apersist(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.persist(&name).await.into_raw_result()
        })
    }

    // ----- RENAME / aRENAME ----------------------------------------------

    fn rename(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        sync_op!(py, self, conn, async { conn.rename(src, dst).await }).map_err(to_py_err)
    }

    fn arename(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<Py<PyAny>> {
        let src = src.to_string();
        let dst = dst.to_string();
        async_op!(self, py, conn, async {
            conn.rename(&src, &dst).await.into_raw_result()
        })
    }

    fn renamenx(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<bool> {
        sync_op!(py, self, conn, async { conn.renamenx(src, dst).await }).map_err(to_py_err)
    }

    fn arenamenx(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<Py<PyAny>> {
        let src = src.to_string();
        let dst = dst.to_string();
        async_op!(self, py, conn, async {
            conn.renamenx(&src, &dst).await.into_raw_result()
        })
    }

    // ----- TYPE / aTYPE --------------------------------------------------
    //
    // `type` is a Python keyword (and a Rust keyword). We expose two names:
    //   * `type` — matches redis-py exactly (call via getattr in Python).
    //   * `key_type` — convenience alias for codebases that prefer not to
    //     use getattr.

    #[pyo3(name = "type")]
    fn type_(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        sync_op!(py, self, conn, async { conn.key_type(name).await }).map_err(to_py_err)
    }

    fn key_type(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        sync_op!(py, self, conn, async { conn.key_type(name).await }).map_err(to_py_err)
    }

    #[pyo3(name = "atype")]
    fn atype_(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.key_type(&name).await.into_raw_result()
        })
    }

    fn akey_type(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.key_type(&name).await.into_raw_result()
        })
    }

    // ----- COPY / aCOPY --------------------------------------------------

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

    #[pyo3(signature = (source, destination, *, db = None, replace = false))]
    fn acopy(
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

    // ----- DUMP / aDUMP --------------------------------------------------

    fn dump(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.dump(name).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn adump(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.dump(&name).await.into_raw_result()
        })
    }

    // ----- RESTORE / aRESTORE --------------------------------------------
    //
    // Returns True on OK to keep parity with the boolean return shape used
    // throughout the driver — redis-py returns the literal string "OK".

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
    fn arestore(
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
