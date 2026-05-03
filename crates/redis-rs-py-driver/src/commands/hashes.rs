// Hash commands on RedisRsDriver.
//
// Every method exists as a sync + async pair:
//   * `<cmd>(...)` — sync; releases the GIL via py.detach.
//   * `a<cmd>(...)` — async; returns a RedisRsAwaitable.
//
// The hash-field TTL family (HEXPIRE / HPEXPIRE / ...) requires Redis/Valkey
// >= 7.4. The tests gate these via a version probe; the Rust code sends the
// raw commands unconditionally and surfaces any WRONGTYPE / unknown-command
// errors through the standard exception classifier.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyTuple};

use crate::async_bridge::RawResult;
use crate::driver::{RedisRsDriver, py_bool, py_bytes_list, py_bytes_pairs, py_int, py_opt_bytes};
use crate::errors::to_py_err;
use crate::exceptions::{DataError, ExceptionClass};
use crate::raw_result::IntoRawResult;
use crate::{async_op, sync_op};

// =========================================================================
// Module-private helpers
// =========================================================================

/// Flatten redis-py-style positional pairs + `mapping` kwarg into a
/// `Vec<(String, Vec<u8>)>` for HSET / HMSET.
fn collect_field_value_pairs(
    items: &Bound<'_, PyTuple>,
    mapping: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<(String, Vec<u8>)>> {
    let mut pairs: Vec<(String, Vec<u8>)> = Vec::new();
    if items.len() % 2 != 0 {
        return Err(PyErr::new::<DataError, _>(
            "HSET items must be an even number of (field, value) positional args",
        ));
    }
    let mut i = 0;
    while i < items.len() {
        let field: String = items.get_item(i)?.extract()?;
        let value: Vec<u8> = items.get_item(i + 1)?.extract()?;
        pairs.push((field, value));
        i += 2;
    }
    if let Some(m) = mapping {
        for (k, v) in m.iter() {
            let field: String = k.extract()?;
            let value: Vec<u8> = v.extract()?;
            pairs.push((field, value));
        }
    }
    if pairs.is_empty() {
        return Err(PyErr::new::<DataError, _>(
            "HSET requires at least one (field, value) pair or a non-empty mapping=",
        ));
    }
    Ok(pairs)
}

fn mapping_to_pairs(mapping: &Bound<'_, PyDict>) -> PyResult<Vec<(String, Vec<u8>)>> {
    if mapping.is_empty() {
        return Err(PyErr::new::<DataError, _>(
            "HMSET requires a non-empty mapping",
        ));
    }
    let mut out = Vec::with_capacity(mapping.len());
    for (k, v) in mapping.iter() {
        let field: String = k.extract()?;
        let value: Vec<u8> = v.extract()?;
        out.push((field, value));
    }
    Ok(out)
}

fn warn_hmset_deprecated(py: Python<'_>) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    warnings.call_method1(
        "warn",
        (
            "HMSET is deprecated. Use HSET instead.",
            py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
        ),
    )?;
    Ok(())
}

fn validate_ttl_modifiers(
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
) -> PyResult<Option<&'static str>> {
    let modifier_count = [nx, xx, gt, lt].iter().filter(|x| **x).count();
    if modifier_count > 1 {
        return Err(PyErr::new::<DataError, _>(
            "Only one of NX, XX, GT, LT can be set at a time",
        ));
    }
    Ok(if nx {
        Some("NX")
    } else if xx {
        Some("XX")
    } else if gt {
        Some("GT")
    } else if lt {
        Some("LT")
    } else {
        None
    })
}

/// Convert a Vec<i64> TTL reply into a Python list[int].
fn int_list_to_py(py: Python<'_>, items: Vec<i64>) -> PyResult<Py<PyAny>> {
    let py_items: Vec<Py<PyAny>> = items
        .into_iter()
        .map(|n| n.into_pyobject(py).unwrap().into_any().unbind())
        .collect();
    Ok(PyList::new(py, py_items)?.into_any().unbind())
}

fn split_scan_reply(value: redis::Value) -> PyResult<(u64, redis::Value)> {
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
        return Ok((cursor, payload));
    }
    Err(PyErr::new::<DataError, _>(
        "HSCAN reply did not match the [cursor, items] shape",
    ))
}

pub(crate) fn render_hscan(
    py: Python<'_>,
    cursor: u64,
    value: redis::Value,
    novalues: bool,
) -> PyResult<Py<PyAny>> {
    let cursor_py = cursor.into_pyobject(py).unwrap().into_any().unbind();
    let payload = match value {
        redis::Value::Array(items) if novalues => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| match item {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    _ => py.None(),
                })
                .collect();
            PyList::new(py, py_items)?.into_any().unbind()
        }
        redis::Value::Array(items) => {
            let dict = PyDict::new(py);
            let mut iter = items.into_iter();
            while let (Some(k_v), Some(v_v)) = (iter.next(), iter.next()) {
                let k = match k_v {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    _ => py.None(),
                };
                let v = match v_v {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    _ => py.None(),
                };
                dict.set_item(k, v)?;
            }
            dict.into_any().unbind()
        }
        _ => PyList::empty(py).into_any().unbind(),
    };
    Ok(PyTuple::new(py, [cursor_py, payload])?.into_any().unbind())
}

pub(crate) fn render_hrandfield(
    py: Python<'_>,
    value: redis::Value,
    count: Option<i64>,
    withvalues: bool,
) -> PyResult<Py<PyAny>> {
    match (count, value) {
        // No count → single bytes or None.
        (None, redis::Value::Nil) => Ok(py.None()),
        (None, redis::Value::BulkString(b)) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        // count without WITHVALUES → flat list[bytes].
        (Some(_), redis::Value::Array(items)) if !withvalues => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| match item {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    redis::Value::Nil => py.None(),
                    other => PyBytes::new(py, format!("{other:?}").as_bytes())
                        .into_any()
                        .unbind(),
                })
                .collect();
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        // count + WITHVALUES → list[tuple[bytes, bytes]].
        // RESP2 returns a flat array [field, value, field, value, ...];
        // RESP3 returns an array of two-element arrays. Handle both.
        (Some(_), redis::Value::Array(items)) if withvalues => {
            let mut pairs: Vec<Py<PyAny>> = Vec::new();
            // Detect shape from first item.
            let nested = items
                .first()
                .map(|first| matches!(first, redis::Value::Array(_)))
                .unwrap_or(false);
            if nested {
                for item in items {
                    if let redis::Value::Array(inner) = item
                        && inner.len() == 2
                    {
                        let field = match &inner[0] {
                            redis::Value::BulkString(b) => PyBytes::new(py, b).into_any().unbind(),
                            _ => py.None(),
                        };
                        let val = match &inner[1] {
                            redis::Value::BulkString(b) => PyBytes::new(py, b).into_any().unbind(),
                            _ => py.None(),
                        };
                        pairs.push(PyTuple::new(py, [field, val])?.into_any().unbind());
                    }
                }
            } else {
                let mut iter = items.into_iter();
                while let (Some(field_v), Some(val_v)) = (iter.next(), iter.next()) {
                    let field = match field_v {
                        redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                        _ => py.None(),
                    };
                    let val = match val_v {
                        redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                        _ => py.None(),
                    };
                    pairs.push(PyTuple::new(py, [field, val])?.into_any().unbind());
                }
            }
            Ok(PyList::new(py, pairs)?.into_any().unbind())
        }
        (_, redis::Value::Nil) => Ok(py.None()),
        (_, other) => Ok(pyo3::types::PyString::new(py, &format!("{other:?}"))
            .into_any()
            .unbind()),
    }
}

// =========================================================================
// #[pymethods] impl block
// =========================================================================

#[pymethods]
impl RedisRsDriver {
    // =====================================================================
    // (a) HGET / HSET / HSETNX
    // =====================================================================

    #[pyo3(signature = (key, field))]
    fn hget(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.hget(key, field).await });
        Ok(py_opt_bytes(py, r.map_err(to_py_err)?))
    }

    #[pyo3(signature = (key, field))]
    fn ahget(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Option<Vec<u8>>> = conn.hget(&key, &field).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *items, mapping=None))]
    fn hset(
        &self,
        py: Python<'_>,
        key: &str,
        items: &Bound<'_, PyTuple>,
        mapping: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let pairs = collect_field_value_pairs(items, mapping)?;
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            conn.hset_multiple(key, &pairs).await
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *items, mapping=None))]
    fn ahset(
        &self,
        py: Python<'_>,
        key: &str,
        items: &Bound<'_, PyTuple>,
        mapping: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let pairs = collect_field_value_pairs(items, mapping)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn.hset_multiple(&key, &pairs).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, field, value))]
    fn hsetnx(&self, py: Python<'_>, key: &str, field: &str, value: &[u8]) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<bool> = sync_op!(py, self, conn, async {
            conn.hset_nx(key, field, value).await
        });
        py_bool(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, field, value))]
    fn ahsetnx(&self, py: Python<'_>, key: &str, field: &str, value: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        let value = value.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<bool> = conn.hset_nx(&key, &field, &value).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (b) HGETALL / HMGET / HMSET
    // =====================================================================

    #[pyo3(signature = (key))]
    fn hgetall(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<(Vec<u8>, Vec<u8>)>> =
            sync_op!(py, self, conn, async { conn.hgetall(key).await });
        py_bytes_pairs(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahgetall(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<(Vec<u8>, Vec<u8>)>> = conn.hgetall(&key).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *fields))]
    fn hmget(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        if fields.is_empty() {
            return Ok(PyList::empty(py).into_any().unbind());
        }
        let r: redis::RedisResult<Vec<Option<Vec<u8>>>> = sync_op!(py, self, conn, async {
            conn.hget_multiple(key, &fields).await
        });
        let raw = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = raw
            .into_iter()
            .map(|opt| match opt {
                Some(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                None => py.None(),
            })
            .collect();
        Ok(PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, *fields))]
    fn ahmget(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if fields.is_empty() {
                return RawResult::OptBytesList(Vec::new());
            }
            let r: redis::RedisResult<Vec<Option<Vec<u8>>>> =
                conn.hget_multiple(&key, &fields).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, mapping))]
    fn hmset(&self, py: Python<'_>, key: &str, mapping: &Bound<'_, PyDict>) -> PyResult<()> {
        warn_hmset_deprecated(py)?;
        let pairs = mapping_to_pairs(mapping)?;
        let r: redis::RedisResult<()> = sync_op!(py, self, conn, async {
            conn.hset_multiple_void(key, &pairs).await
        });
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (key, mapping))]
    fn ahmset(
        &self,
        py: Python<'_>,
        key: &str,
        mapping: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyAny>> {
        warn_hmset_deprecated(py)?;
        let pairs = mapping_to_pairs(mapping)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<()> = conn.hset_multiple_void(&key, &pairs).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (c) HDEL / HEXISTS / HLEN
    // =====================================================================

    #[pyo3(signature = (key, *fields))]
    fn hdel(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        if fields.is_empty() {
            return py_int(py, 0);
        }
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { conn.hdel(key, &fields).await });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *fields))]
    fn ahdel(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if fields.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn.hdel(&key, &fields).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, field))]
    fn hexists(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<bool> =
            sync_op!(py, self, conn, async { conn.hexists(key, field).await });
        py_bool(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, field))]
    fn ahexists(&self, py: Python<'_>, key: &str, field: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<bool> = conn.hexists(&key, &field).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key))]
    fn hlen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async { conn.hlen(key).await });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahlen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn.hlen(&key).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (d) HKEYS / HVALS / HRANDFIELD
    // =====================================================================

    #[pyo3(signature = (key))]
    fn hkeys(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.hkeys(key).await });
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahkeys(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = conn.hkeys(&key).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key))]
    fn hvals(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            sync_op!(py, self, conn, async { conn.hvals(key).await });
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn ahvals(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = conn.hvals(&key).await;
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, count=None, withvalues=false))]
    fn hrandfield(
        &self,
        py: Python<'_>,
        key: &str,
        count: Option<i64>,
        withvalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<redis::Value> = sync_op!(py, self, conn, async {
            conn.hrandfield_raw(key, count, withvalues).await
        });
        let value = r.map_err(to_py_err)?;
        render_hrandfield(py, value, count, withvalues)
    }

    #[pyo3(signature = (key, count=None, withvalues=false))]
    fn ahrandfield(
        &self,
        py: Python<'_>,
        key: &str,
        count: Option<i64>,
        withvalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        crate::runtime::get_runtime().spawn(async move {
            let r: redis::RedisResult<redis::Value> =
                conn.hrandfield_raw(&key, count, withvalues).await;
            let raw = match r {
                Ok(v) => RawResult::HRandfield(v, count, withvalues),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // =====================================================================
    // (e) HINCRBY / HINCRBYFLOAT
    // =====================================================================

    #[pyo3(signature = (key, field, amount))]
    fn hincrby(&self, py: Python<'_>, key: &str, field: &str, amount: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            conn.hincrby(key, field, amount).await
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, field, amount))]
    fn ahincrby(&self, py: Python<'_>, key: &str, field: &str, amount: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn.hincrby(&key, &field, amount).await;
            r.into_raw_result()
        })
    }

    /// HINCRBYFLOAT — returns f64 directly (no String round-trip).
    #[pyo3(signature = (key, field, amount))]
    fn hincrbyfloat(&self, py: Python<'_>, key: &str, field: &str, amount: f64) -> PyResult<f64> {
        sync_op!(py, self, conn, async {
            conn.hincrbyfloat(key, field, amount).await
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (key, field, amount))]
    fn ahincrbyfloat(
        &self,
        py: Python<'_>,
        key: &str,
        field: &str,
        amount: f64,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let field = field.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<f64> = conn.hincrbyfloat(&key, &field, amount).await;
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (f) HSCAN
    // =====================================================================

    #[pyo3(signature = (key, *, cursor=0, r#match=None, count=None, novalues=false))]
    fn hscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
        novalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<redis::Value> = sync_op!(py, self, conn, async {
            conn.hscan_raw(key, cursor, r#match.as_deref(), count, novalues)
                .await
        });
        let value = r.map_err(to_py_err)?;
        let (cur, payload) = split_scan_reply(value)?;
        render_hscan(py, cur, payload, novalues)
    }

    #[pyo3(signature = (key, *, cursor=0, r#match=None, count=None, novalues=false))]
    fn ahscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
        novalues: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        crate::runtime::get_runtime().spawn(async move {
            let r: redis::RedisResult<redis::Value> = conn
                .hscan_raw(&key, cursor, r#match.as_deref(), count, novalues)
                .await;
            let raw = match r {
                Ok(v) => match split_scan_reply(v) {
                    Ok((cur, payload)) => RawResult::HScan {
                        cursor: cur,
                        value: payload,
                        novalues,
                    },
                    Err(_) => RawResult::Error(
                        ExceptionClass::ResponseError,
                        "HSCAN reply did not match the [cursor, items] shape".to_string(),
                    ),
                },
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // =====================================================================
    // (g) Hash-field TTL family (Redis 7.4+)
    // =====================================================================

    // --- HEXPIRE -----------------------------------------------------------

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn hexpire(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.hexpire_family("HEXPIRE", key, &fields, time, modifier)
                .await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn ahexpire(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> = conn
                .hexpire_family("HEXPIRE", &key, &fields, time, modifier)
                .await;
            r.into_raw_result()
        })
    }

    // --- HPEXPIRE ----------------------------------------------------------

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn hpexpire(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.hexpire_family("HPEXPIRE", key, &fields, time, modifier)
                .await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn ahpexpire(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> = conn
                .hexpire_family("HPEXPIRE", &key, &fields, time, modifier)
                .await;
            r.into_raw_result()
        })
    }

    // --- HEXPIREAT ---------------------------------------------------------

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn hexpireat(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.hexpire_family("HEXPIREAT", key, &fields, time, modifier)
                .await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn ahexpireat(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> = conn
                .hexpire_family("HEXPIREAT", &key, &fields, time, modifier)
                .await;
            r.into_raw_result()
        })
    }

    // --- HPEXPIREAT --------------------------------------------------------

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn hpexpireat(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.hexpire_family("HPEXPIREAT", key, &fields, time, modifier)
                .await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields, time, *, nx=false, xx=false, gt=false, lt=false))]
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    fn ahpexpireat(
        &self,
        py: Python<'_>,
        key: &str,
        fields: Vec<String>,
        time: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> PyResult<Py<PyAny>> {
        let modifier = validate_ttl_modifiers(nx, xx, gt, lt)?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> = conn
                .hexpire_family("HPEXPIREAT", &key, &fields, time, modifier)
                .await;
            r.into_raw_result()
        })
    }

    // --- HEXPIRETIME -------------------------------------------------------

    #[pyo3(signature = (key, fields))]
    fn hexpiretime(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.httl_family("HEXPIRETIME", key, &fields).await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields))]
    fn ahexpiretime(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> =
                conn.httl_family("HEXPIRETIME", &key, &fields).await;
            r.into_raw_result()
        })
    }

    // --- HPEXPIRETIME ------------------------------------------------------

    #[pyo3(signature = (key, fields))]
    fn hpexpiretime(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.httl_family("HPEXPIRETIME", key, &fields).await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields))]
    fn ahpexpiretime(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> =
                conn.httl_family("HPEXPIRETIME", &key, &fields).await;
            r.into_raw_result()
        })
    }

    // --- HTTL --------------------------------------------------------------

    #[pyo3(signature = (key, fields))]
    fn httl(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.httl_family("HTTL", key, &fields).await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields))]
    fn ahttl(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> = conn.httl_family("HTTL", &key, &fields).await;
            r.into_raw_result()
        })
    }

    // --- HPTTL -------------------------------------------------------------

    #[pyo3(signature = (key, fields))]
    fn hpttl(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.httl_family("HPTTL", key, &fields).await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields))]
    fn ahpttl(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> = conn.httl_family("HPTTL", &key, &fields).await;
            r.into_raw_result()
        })
    }

    // --- HPERSIST ----------------------------------------------------------

    #[pyo3(signature = (key, fields))]
    fn hpersist(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<i64>> = sync_op!(py, self, conn, async {
            conn.httl_family("HPERSIST", key, &fields).await
        });
        int_list_to_py(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, fields))]
    fn ahpersist(&self, py: Python<'_>, key: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<i64>> = conn.httl_family("HPERSIST", &key, &fields).await;
            r.into_raw_result()
        })
    }
}
