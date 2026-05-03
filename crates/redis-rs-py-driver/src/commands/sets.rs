// Set commands.
//
// Every method exists as a sync + async pair:
//   * `<cmd>(...)` on Redis — sync; releases the GIL via py.detach.
//   * `<cmd>(...)` on AsyncRedis — async; returns a RedisRsAwaitable.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

use crate::async_bridge::RawResult;
use crate::errors::to_py_err;
use crate::facade::asyncio_mod::AsyncRedis;
use crate::facade::sync::Redis;
use crate::helpers::{py_bool, py_int, py_opt_bytes, py_set_of_bytes};
use crate::raw_result::IntoRawResult;

// =========================================================================
// Module-private helpers
// =========================================================================

/// Parse the `[cursor, [member, ...]]` reply that SSCAN returns.
fn parse_sscan_reply(value: redis::Value) -> PyResult<(u64, Vec<Vec<u8>>)> {
    if let redis::Value::Array(items) = value
        && items.len() == 2
    {
        let mut iter = items.into_iter();
        let cursor_v = iter.next().unwrap();
        let payload = iter.next().unwrap();
        let cursor: u64 = match cursor_v {
            redis::Value::BulkString(b) => std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            redis::Value::Int(n) => n as u64,
            _ => 0,
        };
        let members = match payload {
            redis::Value::Array(items) => items
                .into_iter()
                .filter_map(|item| match item {
                    redis::Value::BulkString(b) => Some(b),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        return Ok((cursor, members));
    }
    Err(pyo3::exceptions::PyValueError::new_err(
        "SSCAN reply did not match the [cursor, members] shape",
    ))
}

// =========================================================================
// Sync Redis methods
// =========================================================================

#[pymethods]
impl Redis {
    // =====================================================================
    // (a) SADD / SREM
    // =====================================================================

    #[pyo3(signature = (key, *members))]
    fn sadd(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return py_int(py, 0);
        }
        let r: redis::RedisResult<i64> =
            crate::sync_op!(py, self, conn, async { conn.sadd(key, &members).await });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *members))]
    fn srem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return py_int(py, 0);
        }
        let r: redis::RedisResult<i64> =
            crate::sync_op!(py, self, conn, async { conn.srem(key, &members).await });
        py_int(py, r.map_err(to_py_err)?)
    }

    // =====================================================================
    // (b) SMEMBERS / SCARD
    // =====================================================================

    #[pyo3(signature = (key))]
    fn smembers(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            crate::sync_op!(py, self, conn, async { conn.smembers(key).await });
        py_set_of_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn scard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> =
            crate::sync_op!(py, self, conn, async { conn.scard(key).await });
        py_int(py, r.map_err(to_py_err)?)
    }

    // =====================================================================
    // (c) SISMEMBER / SMISMEMBER
    // =====================================================================

    #[pyo3(signature = (key, member))]
    fn sismember(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<bool> =
            crate::sync_op!(py, self, conn, async { conn.sismember(key, member).await });
        py_bool(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *members))]
    fn smismember(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return Ok(PyList::empty(py).into_any().unbind());
        }
        let r: redis::RedisResult<Vec<bool>> = crate::sync_op!(py, self, conn, async {
            conn.smismember(key, &members).await
        });
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|b| b.into_pyobject(py).unwrap().to_owned().into_any().unbind())
            .collect();
        Ok(PyList::new(py, py_items)?.into_any().unbind())
    }

    // =====================================================================
    // (d) SPOP / SRANDMEMBER
    // =====================================================================

    #[pyo3(signature = (key, count=None))]
    fn spop(&self, py: Python<'_>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<Vec<u8>>> =
                    crate::sync_op!(py, self, conn, async { conn.spop_one(key).await });
                Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
            }
            Some(n) => {
                let r: redis::RedisResult<Vec<Vec<u8>>> =
                    crate::sync_op!(py, self, conn, async { conn.spop_count(key, n).await });
                py_set_of_bytes(py, r.map_err(to_py_err)?)
            }
        }
    }

    #[pyo3(signature = (key, count=None))]
    fn srandmember(&self, py: Python<'_>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        match count {
            None => {
                let r: redis::RedisResult<Option<Vec<u8>>> =
                    crate::sync_op!(py, self, conn, async { conn.srandmember_one(key).await });
                Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
            }
            Some(n) => {
                let r: redis::RedisResult<Vec<Vec<u8>>> = crate::sync_op!(py, self, conn, async {
                    conn.srandmember_count(key, n).await
                });
                let items = r.map_err(to_py_err)?;
                if n < 0 {
                    // Negative count → list (repeats allowed).
                    let py_items: Vec<Py<PyAny>> = items
                        .iter()
                        .map(|b| PyBytes::new(py, b).into_any().unbind())
                        .collect();
                    Ok(PyList::new(py, py_items)?.into_any().unbind())
                } else {
                    // Non-negative → distinct → set.
                    py_set_of_bytes(py, items)
                }
            }
        }
    }

    // =====================================================================
    // (e) SINTER / SUNION / SDIFF + STORE variants + SINTERCARD
    // =====================================================================

    #[pyo3(signature = (*keys))]
    fn sinter(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            crate::sync_op!(py, self, conn, async { conn.sinter(&keys).await });
        py_set_of_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*keys))]
    fn sunion(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            crate::sync_op!(py, self, conn, async { conn.sunion(&keys).await });
        py_set_of_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*keys))]
    fn sdiff(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            crate::sync_op!(py, self, conn, async { conn.sdiff(&keys).await });
        py_set_of_bytes(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (destination, *keys))]
    fn sinterstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = crate::sync_op!(py, self, conn, async {
            conn.sinterstore(destination, &keys).await
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (destination, *keys))]
    fn sunionstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = crate::sync_op!(py, self, conn, async {
            conn.sunionstore(destination, &keys).await
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (destination, *keys))]
    fn sdiffstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = crate::sync_op!(py, self, conn, async {
            conn.sdiffstore(destination, &keys).await
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*keys, limit=None))]
    fn sintercard(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = crate::sync_op!(py, self, conn, async {
            conn.sintercard(&keys, limit).await
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    // =====================================================================
    // (f) SMOVE
    // =====================================================================

    #[pyo3(signature = (source, destination, member))]
    fn smove(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        member: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<bool> = crate::sync_op!(py, self, conn, async {
            conn.smove(source, destination, member).await
        });
        py_bool(py, r.map_err(to_py_err)?)
    }

    // =====================================================================
    // (g) SSCAN
    // =====================================================================

    #[pyo3(signature = (key, *, cursor=0, r#match=None, count=None))]
    fn sscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<redis::Value> = crate::sync_op!(py, self, conn, async {
            conn.sscan_raw(key, cursor, r#match.as_deref(), count).await
        });
        let value = r.map_err(to_py_err)?;
        let (next_cursor, members) = parse_sscan_reply(value)?;
        let cursor_py = next_cursor.into_pyobject(py)?.into_any().unbind();
        let py_set = py_set_of_bytes(py, members)?;
        Ok(PyTuple::new(py, [cursor_py, py_set])?.into_any().unbind())
    }
}

// =========================================================================
// Async AsyncRedis methods
// =========================================================================

#[pymethods]
impl AsyncRedis {
    // =====================================================================
    // (a) SADD / SREM
    // =====================================================================

    #[pyo3(signature = (key, *members))]
    fn sadd(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        crate::async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn.sadd(&key, &members).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn srem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        crate::async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn.srem(&key, &members).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (b) SMEMBERS / SCARD
    // =====================================================================

    #[pyo3(signature = (key))]
    fn smembers(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = conn.smembers(&key).await;
            match r {
                Ok(v) => RawResult::SetOfBytes(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (key))]
    fn scard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn.scard(&key).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (c) SISMEMBER / SMISMEMBER
    // =====================================================================

    #[pyo3(signature = (key, member))]
    fn sismember(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<bool> = conn.sismember(&key, &member).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn smismember(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        crate::async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::BoolList(Vec::new());
            }
            let r: redis::RedisResult<Vec<bool>> = conn.smismember(&key, &members).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (d) SPOP / SRANDMEMBER
    // =====================================================================

    #[pyo3(signature = (key, count=None))]
    fn spop(&self, py: Python<'_>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        crate::async_op!(self, py, conn, async {
            match count {
                None => {
                    let r: redis::RedisResult<Option<Vec<u8>>> = conn.spop_one(&key).await;
                    r.into_raw_result()
                }
                Some(n) => {
                    let r: redis::RedisResult<Vec<Vec<u8>>> = conn.spop_count(&key, n).await;
                    match r {
                        Ok(v) => RawResult::SetOfBytes(v),
                        Err(e) => crate::errors::classify(e),
                    }
                }
            }
        })
    }

    #[pyo3(signature = (key, count=None))]
    fn srandmember(&self, py: Python<'_>, key: &str, count: Option<i64>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        crate::async_op!(self, py, conn, async {
            match count {
                None => {
                    let r: redis::RedisResult<Option<Vec<u8>>> = conn.srandmember_one(&key).await;
                    r.into_raw_result()
                }
                Some(n) => {
                    let r: redis::RedisResult<Vec<Vec<u8>>> = conn.srandmember_count(&key, n).await;
                    match r {
                        Ok(v) if n < 0 => RawResult::BytesList(v),
                        Ok(v) => RawResult::SetOfBytes(v),
                        Err(e) => crate::errors::classify(e),
                    }
                }
            }
        })
    }

    // =====================================================================
    // (e) SINTER / SUNION / SDIFF + STORE variants + SINTERCARD
    // =====================================================================

    #[pyo3(signature = (*keys))]
    fn sinter(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = conn.sinter(&keys).await;
            match r {
                Ok(v) => RawResult::SetOfBytes(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (*keys))]
    fn sunion(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = conn.sunion(&keys).await;
            match r {
                Ok(v) => RawResult::SetOfBytes(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (*keys))]
    fn sdiff(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = conn.sdiff(&keys).await;
            match r {
                Ok(v) => RawResult::SetOfBytes(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (destination, *keys))]
    fn sinterstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let destination = destination.to_string();
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn.sinterstore(&destination, &keys).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (destination, *keys))]
    fn sunionstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let destination = destination.to_string();
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn.sunionstore(&destination, &keys).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (destination, *keys))]
    fn sdiffstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let destination = destination.to_string();
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn.sdiffstore(&destination, &keys).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*keys, limit=None))]
    fn sintercard(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn.sintercard(&keys, limit).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (f) SMOVE
    // =====================================================================

    #[pyo3(signature = (source, destination, member))]
    fn smove(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        member: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let source = source.to_string();
        let destination = destination.to_string();
        let member = member.to_vec();
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<bool> = conn.smove(&source, &destination, &member).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (g) SSCAN
    // =====================================================================

    #[pyo3(signature = (key, *, cursor=0, r#match=None, count=None))]
    fn sscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        crate::async_op!(self, py, conn, async {
            let r: redis::RedisResult<redis::Value> = conn
                .sscan_raw(&key, cursor, r#match.as_deref(), count)
                .await;
            match r {
                Ok(v) => match parse_sscan_reply(v) {
                    Ok((cursor, members)) => RawResult::SScan { cursor, members },
                    Err(e) => RawResult::Error(
                        crate::exceptions::ExceptionClass::ResponseError,
                        e.to_string(),
                    ),
                },
                Err(e) => crate::errors::classify(e),
            }
        })
    }
}
