// List commands.
//
// Every method exists as a sync + async pair:
//   * `<cmd>(...)` — sync; releases the GIL via py.detach.
//   * `a<cmd>(...)` — async; returns a RedisRsAwaitable.
//
// Non-blocking commands ride ValkeyConnInner (the multiplexed pipeline
// connection). Blocking commands (BLPOP/BRPOP/BLMOVE/BLMPOP) ride the
// lazy-allocated second connection via the inherent `blocking_*` methods
// on ValkeyConn — see Task 7 in the plan.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString, PyTuple};

use crate::async_bridge::RawResult;
use crate::driver::{RedisRsDriver, py_bytes_list, py_int, py_opt_bytes};
use crate::errors::{classify_error, to_py_err};
use crate::raw_result::IntoRawResult;
use crate::{async_op, sync_op};

// =========================================================================
// Helpers
// =========================================================================

fn opt_bytes_list_to_py(py: Python<'_>, v: Option<Vec<Vec<u8>>>) -> PyResult<Py<PyAny>> {
    match v {
        None => Ok(py.None()),
        Some(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .iter()
                .map(|b| PyBytes::new(py, b).into_any().unbind())
                .collect();
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
    }
}

fn validate_pop_direction(direction: &str) -> PyResult<()> {
    let d = direction.to_ascii_uppercase();
    if d != "LEFT" && d != "RIGHT" {
        return Err(PyErr::new::<crate::exceptions::DataError, _>(
            "direction must be 'LEFT' or 'RIGHT'",
        ));
    }
    Ok(())
}

fn opt_key_and_bytes_list_to_py(
    py: Python<'_>,
    v: Option<(String, Vec<Vec<u8>>)>,
) -> PyResult<Py<PyAny>> {
    match v {
        None => Ok(py.None()),
        Some((key, elements)) => {
            let py_key = PyString::new(py, &key).into_any().unbind();
            let py_elements: Vec<Py<PyAny>> = elements
                .iter()
                .map(|b| PyBytes::new(py, b).into_any().unbind())
                .collect();
            let py_list = PyList::new(py, py_elements)?.into_any().unbind();
            Ok(PyTuple::new(py, [py_key, py_list])?.into_any().unbind())
        }
    }
}

fn opt_key_and_bytes_to_py(py: Python<'_>, v: Option<(String, Vec<u8>)>) -> PyResult<Py<PyAny>> {
    match v {
        None => Ok(py.None()),
        Some((key, value)) => {
            let py_key = PyString::new(py, &key).into_any().unbind();
            let py_value = PyBytes::new(py, &value).into_any().unbind();
            Ok(PyTuple::new(py, [py_key, py_value])?.into_any().unbind())
        }
    }
}

// =========================================================================
// PyMethods block
// =========================================================================

#[pymethods]
impl RedisRsDriver {
    // ----- LPUSH / aLPUSH (variadic) -------------------------------------

    #[pyo3(signature = (name, *values))]
    fn lpush(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.lpush(name, &values).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (name, *values))]
    fn alpush(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.lpush(&name, &values).await.into_raw_result()
        })
    }

    // ----- RPUSH / aRPUSH (variadic) -------------------------------------

    #[pyo3(signature = (name, *values))]
    fn rpush(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.rpush(name, &values).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (name, *values))]
    fn arpush(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.rpush(&name, &values).await.into_raw_result()
        })
    }

    // ----- LPUSHX / aLPUSHX ----------------------------------------------

    #[pyo3(signature = (name, *values))]
    fn lpushx(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.lpushx(name, &values).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (name, *values))]
    fn alpushx(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.lpushx(&name, &values).await.into_raw_result()
        })
    }

    // ----- RPUSHX / aRPUSHX ----------------------------------------------

    #[pyo3(signature = (name, *values))]
    fn rpushx(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.rpushx(name, &values).await }).map_err(to_py_err)
    }

    #[pyo3(signature = (name, *values))]
    fn arpushx(&self, py: Python<'_>, name: &str, values: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.rpushx(&name, &values).await.into_raw_result()
        })
    }

    // ----- LPOP / aLPOP --------------------------------------------------

    #[pyo3(signature = (name, count = None))]
    fn lpop(&self, py: Python<'_>, name: &str, count: Option<u64>) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<Vec<u8>>> =
                    sync_op!(py, self, conn, async { conn.lpop_one(name).await });
                Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
            }
            Some(c) => {
                let r: redis::RedisResult<Option<Vec<Vec<u8>>>> =
                    sync_op!(py, self, conn, async { conn.lpop_count(name, c).await });
                opt_bytes_list_to_py(py, r.map_err(to_py_err)?)
            }
        }
    }

    #[pyo3(signature = (name, count = None))]
    fn alpop(&self, py: Python<'_>, name: &str, count: Option<u64>) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            match count {
                None => match conn.lpop_one(&name).await {
                    Ok(Some(b)) => RawResult::OptBytes(Some(b)),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
                Some(c) => match conn.lpop_count(&name, c).await {
                    Ok(Some(items)) => RawResult::BytesList(items),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
            }
        })
    }

    // ----- RPOP / aRPOP --------------------------------------------------

    #[pyo3(signature = (name, count = None))]
    fn rpop(&self, py: Python<'_>, name: &str, count: Option<u64>) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<Vec<u8>>> =
                    sync_op!(py, self, conn, async { conn.rpop_one(name).await });
                Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
            }
            Some(c) => {
                let r: redis::RedisResult<Option<Vec<Vec<u8>>>> =
                    sync_op!(py, self, conn, async { conn.rpop_count(name, c).await });
                opt_bytes_list_to_py(py, r.map_err(to_py_err)?)
            }
        }
    }

    #[pyo3(signature = (name, count = None))]
    fn arpop(&self, py: Python<'_>, name: &str, count: Option<u64>) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            match count {
                None => match conn.rpop_one(&name).await {
                    Ok(Some(b)) => RawResult::OptBytes(Some(b)),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
                Some(c) => match conn.rpop_count(&name, c).await {
                    Ok(Some(items)) => RawResult::BytesList(items),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
            }
        })
    }

    // ----- LRANGE / aLRANGE ----------------------------------------------

    fn lrange(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Vec<u8>>> = sync_op!(py, self, conn, async {
            conn.lrange(name, start, end).await
        });
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    fn alrange(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.lrange(&name, start, end).await.into_raw_result()
        })
    }

    // ----- LLEN / aLLEN --------------------------------------------------

    fn llen(&self, py: Python<'_>, name: &str) -> PyResult<i64> {
        sync_op!(py, self, conn, async { conn.llen(name).await }).map_err(to_py_err)
    }

    fn allen(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.llen(&name).await.into_raw_result()
        })
    }

    // ----- LMOVE / aLMOVE ------------------------------------------------

    fn lmove(
        &self,
        py: Python<'_>,
        first_list: &str,
        second_list: &str,
        src: &str,
        dest: &str,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> = sync_op!(py, self, conn, async {
            conn.lmove(first_list, second_list, src, dest).await
        });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn almove(
        &self,
        py: Python<'_>,
        first_list: &str,
        second_list: &str,
        src: &str,
        dest: &str,
    ) -> PyResult<Py<PyAny>> {
        let first_list = first_list.to_string();
        let second_list = second_list.to_string();
        let src = src.to_string();
        let dest = dest.to_string();
        async_op!(self, py, conn, async {
            conn.lmove(&first_list, &second_list, &src, &dest)
                .await
                .into_raw_result()
        })
    }

    // ----- LPOS / aLPOS --------------------------------------------------

    #[pyo3(signature = (name, value, *, rank = None, count = None, maxlen = None))]
    fn lpos(
        &self,
        py: Python<'_>,
        name: &str,
        value: &[u8],
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<i64>> = sync_op!(py, self, conn, async {
                    conn.lpos_single(name, value, rank, maxlen).await
                });
                match r.map_err(to_py_err)? {
                    Some(i) => py_int(py, i),
                    None => Ok(py.None()),
                }
            }
            Some(c) => {
                let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
                    conn.lpos_count(name, value, rank, c, maxlen).await
                });
                let items = r.map_err(to_py_err)?;
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|i| i.into_pyobject(py).unwrap().into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
        }
    }

    #[pyo3(signature = (name, value, *, rank = None, count = None, maxlen = None))]
    fn alpos(
        &self,
        py: Python<'_>,
        name: &str,
        value: &[u8],
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            match count {
                None => match conn.lpos_single(&name, &value, rank, maxlen).await {
                    Ok(Some(i)) => RawResult::Int(i),
                    Ok(None) => RawResult::Nil,
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
                Some(c) => match conn.lpos_count(&name, &value, rank, c, maxlen).await {
                    Ok(items) => RawResult::Value(redis::Value::Array(
                        items.into_iter().map(redis::Value::Int).collect(),
                    )),
                    Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
                },
            }
        })
    }

    // ----- LREM / aLREM --------------------------------------------------

    fn lrem(&self, py: Python<'_>, name: &str, count: i64, value: &[u8]) -> PyResult<i64> {
        sync_op!(py, self, conn, async {
            conn.lrem(name, count, value).await
        })
        .map_err(to_py_err)
    }

    fn alrem(&self, py: Python<'_>, name: &str, count: i64, value: &[u8]) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.lrem(&name, count, &value).await.into_raw_result()
        })
    }

    // ----- LINDEX / aLINDEX ----------------------------------------------

    fn lindex(&self, py: Python<'_>, name: &str, index: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.lindex(name, index).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn alindex(&self, py: Python<'_>, name: &str, index: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.lindex(&name, index).await.into_raw_result()
        })
    }

    // ----- LSET / aLSET --------------------------------------------------

    fn lset(&self, py: Python<'_>, name: &str, index: i64, value: &[u8]) -> PyResult<()> {
        sync_op!(py, self, conn, async {
            conn.lset(name, index, value).await
        })
        .map_err(to_py_err)
    }

    fn alset(&self, py: Python<'_>, name: &str, index: i64, value: &[u8]) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.lset(&name, index, &value).await.into_raw_result()
        })
    }

    // ----- LINSERT / aLINSERT --------------------------------------------

    fn linsert(
        &self,
        py: Python<'_>,
        name: &str,
        where_: &str,
        refvalue: &[u8],
        value: &[u8],
    ) -> PyResult<i64> {
        let before = match where_.to_ascii_uppercase().as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => {
                return Err(PyErr::new::<crate::exceptions::DataError, _>(
                    "where argument must be 'BEFORE' or 'AFTER'",
                ));
            }
        };
        sync_op!(py, self, conn, async {
            conn.linsert(name, before, refvalue, value).await
        })
        .map_err(to_py_err)
    }

    fn alinsert(
        &self,
        py: Python<'_>,
        name: &str,
        where_: &str,
        refvalue: &[u8],
        value: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let before = match where_.to_ascii_uppercase().as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => {
                return Err(PyErr::new::<crate::exceptions::DataError, _>(
                    "where argument must be 'BEFORE' or 'AFTER'",
                ));
            }
        };
        let name = name.to_string();
        let refvalue = refvalue.to_vec();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            conn.linsert(&name, before, &refvalue, &value)
                .await
                .into_raw_result()
        })
    }

    // ----- LTRIM / aLTRIM ------------------------------------------------

    fn ltrim(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<()> {
        sync_op!(py, self, conn, async { conn.ltrim(name, start, end).await }).map_err(to_py_err)
    }

    fn altrim(&self, py: Python<'_>, name: &str, start: i64, end: i64) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            conn.ltrim(&name, start, end).await.into_raw_result()
        })
    }

    // ----- LMPOP / aLMPOP ------------------------------------------------

    #[pyo3(signature = (keys, *, direction, count = 1))]
    fn lmpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        validate_pop_direction(direction)?;
        let r: redis::RedisResult<Option<(String, Vec<Vec<u8>>)>> =
            sync_op!(py, self, conn, async {
                conn.lmpop(&keys, direction, count).await
            });
        opt_key_and_bytes_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (keys, *, direction, count = 1))]
    fn almpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        validate_pop_direction(direction)?;
        let direction = direction.to_string();
        async_op!(self, py, conn, async {
            conn.lmpop(&keys, &direction, count).await.into_raw_result()
        })
    }

    // ----- BLPOP / aBLPOP ------------------------------------------------
    //
    // Routed through the lazy blocking connection: the body calls
    // `self.connection.blpop(...)` (the inherent ValkeyConn method),
    // not `conn.blpop(...)` on the Deref'd ValkeyConnInner. This is
    // load-bearing — see Task 7.

    fn blpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let r: redis::RedisResult<Option<(String, Vec<u8>)>> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async { conn.blpop(&keys, timeout).await })
        });
        opt_key_and_bytes_to_py(py, r.map_err(to_py_err)?)
    }

    fn ablpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        crate::runtime::get_runtime().spawn(async move {
            let result = match conn.blpop(&keys, timeout).await {
                Ok(v) => RawResult::OptKeyAndBytes(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            };
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // ----- BRPOP / aBRPOP ------------------------------------------------

    fn brpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let r: redis::RedisResult<Option<(String, Vec<u8>)>> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async { conn.brpop(&keys, timeout).await })
        });
        opt_key_and_bytes_to_py(py, r.map_err(to_py_err)?)
    }

    fn abrpop(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        crate::runtime::get_runtime().spawn(async move {
            let result = match conn.brpop(&keys, timeout).await {
                Ok(v) => RawResult::OptKeyAndBytes(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            };
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // ----- BLMOVE / aBLMOVE ----------------------------------------------

    fn blmove(
        &self,
        py: Python<'_>,
        first_list: &str,
        second_list: &str,
        src: &str,
        dest: &str,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let first_list = first_list.to_string();
        let second_list = second_list.to_string();
        let src = src.to_string();
        let dest = dest.to_string();
        let r: redis::RedisResult<Option<Vec<u8>>> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async {
                conn.blmove(&first_list, &second_list, &src, &dest, timeout)
                    .await
            })
        });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    fn ablmove(
        &self,
        py: Python<'_>,
        first_list: &str,
        second_list: &str,
        src: &str,
        dest: &str,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let conn = self.connection.clone();
        let first_list = first_list.to_string();
        let second_list = second_list.to_string();
        let src = src.to_string();
        let dest = dest.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        crate::runtime::get_runtime().spawn(async move {
            let result = match conn
                .blmove(&first_list, &second_list, &src, &dest, timeout)
                .await
            {
                Ok(v) => RawResult::OptBytes(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            };
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // ----- BLMPOP / aBLMPOP ----------------------------------------------

    #[pyo3(signature = (*, timeout, keys, direction, count = 1))]
    fn blmpop(
        &self,
        py: Python<'_>,
        timeout: f64,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        validate_pop_direction(direction)?;
        let conn = self.connection.clone();
        let direction_owned = direction.to_string();
        let r: redis::RedisResult<Option<(String, Vec<Vec<u8>>)>> = py.detach(|| {
            crate::runtime::get_runtime()
                .block_on(async { conn.blmpop(timeout, &keys, &direction_owned, count).await })
        });
        opt_key_and_bytes_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*, timeout, keys, direction, count = 1))]
    fn ablmpop(
        &self,
        py: Python<'_>,
        timeout: f64,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        validate_pop_direction(direction)?;
        let conn = self.connection.clone();
        let direction_owned = direction.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        crate::runtime::get_runtime().spawn(async move {
            let result = match conn.blmpop(timeout, &keys, &direction_owned, count).await {
                Ok(v) => RawResult::OptKeyAndBytesList(v),
                Err(e) => RawResult::Error(classify_error(&e), e.to_string()),
            };
            let _ = tx.send(result);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }
}
