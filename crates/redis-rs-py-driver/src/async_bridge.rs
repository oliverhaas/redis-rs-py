// RawResult typed boundary + recursive redis::Value → Python conversion.
//
// Variants are kept wide on day one so the command-family plans (03–09)
// can return without back-editing this file. New variants can be added
// freely as commands need them.
//
// Lifted from django-vcache (MIT, David Burke / GlitchTip) via
// django-cachex-redis-rs. The RedisRsAwaitable half lives below in
// the second region of this file and is also a verbatim port.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySet, PyString, PyTuple};

#[allow(clippy::type_complexity)]
pub enum RawResult {
    Nil,
    OptBytes(Option<Vec<u8>>),
    Bool(bool),
    Int(i64),
    OptInt(Option<i64>),
    F64(f64),
    OptF64(Option<f64>),
    Str(String),
    OptStr(Option<String>),
    OptBytesList(Vec<Option<Vec<u8>>>),
    BytesList(Vec<Vec<u8>>),
    StringList(Vec<String>),
    BytesPairs(Vec<(Vec<u8>, Vec<u8>)>),
    ScoredMembers(Vec<(Vec<u8>, f64)>),
    OptKeyAndBytesList(Option<(String, Vec<Vec<u8>>)>),
    OptKeyAndBytes(Option<(String, Vec<u8>)>),
    CursorAndStrings(u64, Vec<String>),
    IntList(Vec<i64>),
    HRandfield(redis::Value, Option<i64>, bool),
    HScan {
        cursor: u64,
        value: redis::Value,
        novalues: bool,
    },
    /// Python `set[bytes]` — used by SMEMBERS, SINTER, SUNION, SDIFF, SPOP(count), SRANDMEMBER(count>=0).
    SetOfBytes(Vec<Vec<u8>>),
    /// Python `list[bool]` — used by SMISMEMBER.
    BoolList(Vec<bool>),
    /// `(cursor: int, set[bytes])` — used by SSCAN.
    SScan {
        cursor: u64,
        members: Vec<Vec<u8>>,
    },
    // ZADD INCR mode → float | None
    OptScore(Option<f64>),
    // ZRANK WITHSCORE → (rank, score) | None
    OptRankAndScore(Option<(i64, f64)>),
    // ZMPOP / BZMPOP → (key, [(member, score), ...]) | None
    OptKeyAndScoredMembers(Option<(String, Vec<(Vec<u8>, f64)>)>),
    // BZPOPMIN / BZPOPMAX → (key, member, score) | None
    OptKeyMemberScore(Option<(Vec<u8>, Vec<u8>, f64)>),
    // ZRANDMEMBER with count/withscores
    ZRandmember {
        value: redis::Value,
        count: Option<i64>,
        withscores: bool,
    },
    // ZSCAN → (cursor, list[tuple[bytes, float]])
    ZScan {
        cursor: u64,
        items: Vec<(Vec<u8>, f64)>,
    },
    Value(redis::Value),
    Error(crate::exceptions::ExceptionClass, String),
    // Stream variants (Plan 08)
    #[allow(clippy::type_complexity)]
    StreamEntries(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>),
    #[allow(clippy::type_complexity)]
    StreamReadEntries(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)>),
    StreamPendingSummary(Option<(i64, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, i64)>)>),
    StreamPendingRange(Vec<(Vec<u8>, Vec<u8>, i64, i64)>),
    #[allow(clippy::type_complexity)]
    StreamClaim(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>),
    StreamClaimJustIds(Vec<Vec<u8>>),
    #[allow(clippy::type_complexity)]
    StreamAutoclaim(
        (
            Vec<u8>,
            Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>,
            Vec<Vec<u8>>,
        ),
    ),
    StreamAutoclaimJustIds((Vec<u8>, Vec<Vec<u8>>, Vec<Vec<u8>>)),
    StreamInfoStream(Vec<(Vec<u8>, redis::Value)>),
    StreamInfoGroups(Vec<Vec<(Vec<u8>, redis::Value)>>),
    StreamInfoConsumers(Vec<Vec<(Vec<u8>, redis::Value)>>),
    // Admin variants (Plan 09)
    /// `(seconds, microseconds)` string pair from TIME.
    OptStrPair(Option<(String, String)>),
    /// `(seconds, microseconds)` integer pair from TIME.
    IntPair(Option<(i64, i64)>),
    /// `list[dict[bytes, bytes]]` — used by CLIENT LIST.
    BytesPairsList(Vec<Vec<(Vec<u8>, Vec<u8>)>>),
}

fn redis_value_to_py(py: Python<'_>, v: redis::Value) -> PyResult<Py<PyAny>> {
    match v {
        redis::Value::Nil => Ok(py.None()),
        redis::Value::Int(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        redis::Value::BulkString(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        redis::Value::SimpleString(s) => Ok(PyBytes::new(py, s.as_bytes()).into_any().unbind()),
        redis::Value::Boolean(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::Double(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        redis::Value::Okay => Ok(true.into_pyobject(py)?.to_owned().into_any().unbind()),
        redis::Value::Array(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::Map(pairs) => {
            let dict = PyDict::new(py);
            for (k, val) in pairs {
                let k_py = redis_value_to_py(py, k)?;
                let v_py = redis_value_to_py(py, val)?;
                dict.set_item(k_py, v_py)?;
            }
            Ok(dict.into_any().unbind())
        }
        redis::Value::Set(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::Attribute { data, .. } => redis_value_to_py(py, *data),
        redis::Value::Push { kind: _, data } => {
            let py_items: Vec<Py<PyAny>> = data
                .into_iter()
                .map(|item| redis_value_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        redis::Value::BigNumber(n) => Ok(PyString::new(py, &n.to_string()).into_any().unbind()),
        redis::Value::VerbatimString { text, .. } => {
            Ok(PyBytes::new(py, text.as_bytes()).into_any().unbind())
        }
        redis::Value::ServerError(e) => {
            Err(pyo3::exceptions::PyRuntimeError::new_err(format!("{e:?}")))
        }
        other => Ok(PyString::new(py, &format!("{other:?}")).into_any().unbind()),
    }
}

impl RawResult {
    pub fn into_py(self, py: Python<'_>) -> Result<Py<PyAny>, PyErr> {
        match self {
            RawResult::Nil => Ok(py.None()),
            RawResult::OptBytes(Some(b)) => Ok(PyBytes::new(py, &b).into_any().unbind()),
            RawResult::OptBytes(None) => Ok(py.None()),
            RawResult::Bool(b) => Ok(b.into_pyobject(py).unwrap().to_owned().into_any().unbind()),
            RawResult::Int(n) => Ok(n.into_pyobject(py).unwrap().into_any().unbind()),
            RawResult::Str(s) => Ok(PyString::new(py, &s).into_any().unbind()),
            RawResult::OptStr(Some(s)) => Ok(PyString::new(py, &s).into_any().unbind()),
            RawResult::OptStr(None) => Ok(py.None()),
            RawResult::OptBytesList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|r| match r {
                        Some(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                        None => py.None(),
                    })
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::BytesList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .iter()
                    .map(|b| PyBytes::new(py, b).into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::StringList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .iter()
                    .map(|s| PyString::new(py, s).into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::OptKeyAndBytesList(Some((key, values))) => {
                let py_values: Vec<Py<PyAny>> = values
                    .iter()
                    .map(|b| PyBytes::new(py, b).into_any().unbind())
                    .collect();
                let py_key = PyString::new(py, &key).into_any().unbind();
                let py_list = PyList::new(py, py_values)?.into_any().unbind();
                Ok(PyTuple::new(py, [py_key, py_list])?.into_any().unbind())
            }
            RawResult::OptKeyAndBytesList(None) => Ok(py.None()),
            RawResult::OptKeyAndBytes(Some((key, value))) => {
                let py_key = PyBytes::new(py, key.as_bytes()).into_any().unbind();
                let py_value = PyBytes::new(py, &value).into_any().unbind();
                Ok(PyTuple::new(py, [py_key, py_value])?.into_any().unbind())
            }
            RawResult::OptKeyAndBytes(None) => Ok(py.None()),
            RawResult::CursorAndStrings(cursor, keys) => {
                let py_cursor = cursor.into_pyobject(py)?.into_any().unbind();
                let py_items: Vec<Py<PyAny>> = keys
                    .iter()
                    .map(|s| PyString::new(py, s).into_any().unbind())
                    .collect();
                let py_list = PyList::new(py, py_items)?.into_any().unbind();
                Ok(PyTuple::new(py, [py_cursor, py_list])?.into_any().unbind())
            }
            RawResult::OptInt(Some(n)) => Ok(n.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptInt(None) => Ok(py.None()),
            RawResult::F64(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptF64(Some(f)) => Ok(f.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptF64(None) => Ok(py.None()),
            RawResult::BytesPairs(pairs) => {
                let dict = PyDict::new(py);
                for (k, v) in pairs {
                    let k_py = PyBytes::new(py, &k).into_any().unbind();
                    let v_py = PyBytes::new(py, &v).into_any().unbind();
                    dict.set_item(k_py, v_py)?;
                }
                Ok(dict.into_any().unbind())
            }
            RawResult::ScoredMembers(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|(member, score)| {
                        let m_py = PyBytes::new(py, &member).into_any().unbind();
                        let s_py = score.into_pyobject(py)?.into_any().unbind();
                        Ok(PyTuple::new(py, [m_py, s_py])?.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::IntList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|n| n.into_pyobject(py).unwrap().into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::HRandfield(v, count, withvalues) => {
                crate::commands::hashes::render_hrandfield(py, v, count, withvalues)
            }
            RawResult::HScan {
                cursor,
                value,
                novalues,
            } => crate::commands::hashes::render_hscan(py, cursor, value, novalues),
            RawResult::SetOfBytes(items) => {
                let py_set = PySet::empty(py)?;
                for b in items {
                    py_set.add(PyBytes::new(py, &b))?;
                }
                Ok(py_set.into_any().unbind())
            }
            RawResult::BoolList(items) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|b| b.into_pyobject(py).unwrap().to_owned().into_any().unbind())
                    .collect();
                Ok(PyList::new(py, py_items)?.into_any().unbind())
            }
            RawResult::SScan { cursor, members } => {
                let cursor_py = cursor.into_pyobject(py)?.into_any().unbind();
                let py_set = PySet::empty(py)?;
                for b in members {
                    py_set.add(PyBytes::new(py, &b))?;
                }
                Ok(PyTuple::new(py, [cursor_py, py_set.into_any().unbind()])?
                    .into_any()
                    .unbind())
            }
            RawResult::OptScore(Some(f)) => Ok(f.into_pyobject(py)?.into_any().unbind()),
            RawResult::OptScore(None) => Ok(py.None()),
            RawResult::OptRankAndScore(Some((rank, score))) => {
                let r = rank.into_pyobject(py)?.into_any().unbind();
                let s = score.into_pyobject(py)?.into_any().unbind();
                Ok(PyTuple::new(py, [r, s])?.into_any().unbind())
            }
            RawResult::OptRankAndScore(None) => Ok(py.None()),
            RawResult::OptKeyAndScoredMembers(Some((key, items))) => {
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|(m, s)| {
                        let m_py = PyBytes::new(py, &m).into_any().unbind();
                        let s_py = s.into_pyobject(py)?.into_any().unbind();
                        Ok(PyTuple::new(py, [m_py, s_py])?.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                let key_py = PyString::new(py, &key).into_any().unbind();
                let list_py = PyList::new(py, py_items)?.into_any().unbind();
                Ok(PyTuple::new(py, [key_py, list_py])?.into_any().unbind())
            }
            RawResult::OptKeyAndScoredMembers(None) => Ok(py.None()),
            RawResult::OptKeyMemberScore(Some((k, m, s))) => {
                let k_py = PyBytes::new(py, &k).into_any().unbind();
                let m_py = PyBytes::new(py, &m).into_any().unbind();
                let s_py = s.into_pyobject(py)?.into_any().unbind();
                Ok(PyTuple::new(py, [k_py, m_py, s_py])?.into_any().unbind())
            }
            RawResult::OptKeyMemberScore(None) => Ok(py.None()),
            RawResult::ZRandmember {
                value,
                count,
                withscores,
            } => crate::commands::zsets::render_zrandmember(py, value, count, withscores),
            RawResult::ZScan { cursor, items } => {
                let cursor_py = cursor.into_pyobject(py)?.into_any().unbind();
                let py_items: Vec<Py<PyAny>> = items
                    .into_iter()
                    .map(|(m, s)| {
                        let m_py = PyBytes::new(py, &m).into_any().unbind();
                        let s_py = s.into_pyobject(py)?.into_any().unbind();
                        Ok(PyTuple::new(py, [m_py, s_py])?.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                let list_py = PyList::new(py, py_items)?.into_any().unbind();
                Ok(PyTuple::new(py, [cursor_py, list_py])?.into_any().unbind())
            }
            RawResult::OptStrPair(None) => Ok(py.None()),
            RawResult::OptStrPair(Some((a, b))) => {
                let a_py = PyString::new(py, &a).into_any().unbind();
                let b_py = PyString::new(py, &b).into_any().unbind();
                Ok(PyTuple::new(py, [a_py, b_py])?.into_any().unbind())
            }
            RawResult::IntPair(None) => Ok(py.None()),
            RawResult::IntPair(Some((a, b))) => {
                let a_py = a.into_pyobject(py)?.into_any().unbind();
                let b_py = b.into_pyobject(py)?.into_any().unbind();
                Ok(PyTuple::new(py, [a_py, b_py])?.into_any().unbind())
            }
            RawResult::BytesPairsList(rows) => {
                let mut items: Vec<Py<PyAny>> = Vec::with_capacity(rows.len());
                for row in rows {
                    let dict = PyDict::new(py);
                    for (k, v) in row {
                        dict.set_item(PyBytes::new(py, &k), PyBytes::new(py, &v))?;
                    }
                    items.push(dict.into_any().unbind());
                }
                Ok(PyList::new(py, items)?.into_any().unbind())
            }
            RawResult::Value(v) => redis_value_to_py(py, v),
            RawResult::Error(class, e) => Err(class.into_py_err(py, e)),
            // --- Stream variants (Plan 08) ---
            RawResult::StreamEntries(entries) => {
                let py_entries = build_stream_entries(py, entries)?;
                Ok(py_entries.into_any().unbind())
            }
            RawResult::StreamReadEntries(streams) => {
                if streams.is_empty() {
                    return Ok(py.None());
                }
                let dict = PyDict::new(py);
                for (key, entries) in streams {
                    let key_py = PyBytes::new(py, &key).into_any().unbind();
                    let entries_py = build_stream_entries(py, entries)?;
                    dict.set_item(key_py, entries_py)?;
                }
                Ok(dict.into_any().unbind())
            }
            RawResult::StreamPendingSummary(None) => {
                let zero = 0_i64.into_pyobject(py)?.into_any().unbind();
                let none = py.None();
                let empty_list = PyList::empty(py).into_any().unbind();
                Ok(
                    PyTuple::new(py, [zero, none.clone_ref(py), none, empty_list])?
                        .into_any()
                        .unbind(),
                )
            }
            RawResult::StreamPendingSummary(Some((count, min_id, max_id, consumers))) => {
                let count_py = count.into_pyobject(py)?.into_any().unbind();
                let min_py = PyBytes::new(py, &min_id).into_any().unbind();
                let max_py = PyBytes::new(py, &max_id).into_any().unbind();
                let consumers_py: Vec<Py<PyAny>> = consumers
                    .into_iter()
                    .map(|(name, n)| {
                        let name_py = PyBytes::new(py, &name).into_any().unbind();
                        let n_py = n.into_pyobject(py)?.into_any().unbind();
                        PyTuple::new(py, [name_py, n_py]).map(|t| t.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                let consumers_list = PyList::new(py, consumers_py)?.into_any().unbind();
                Ok(
                    PyTuple::new(py, [count_py, min_py, max_py, consumers_list])?
                        .into_any()
                        .unbind(),
                )
            }
            RawResult::StreamPendingRange(rows) => {
                let items: Vec<Py<PyAny>> = rows
                    .into_iter()
                    .map(|(id, consumer, idle, deliveries)| {
                        let d = PyDict::new(py);
                        d.set_item(b"message_id" as &[u8], PyBytes::new(py, &id))?;
                        d.set_item(b"consumer" as &[u8], PyBytes::new(py, &consumer))?;
                        d.set_item(b"time_since_delivered" as &[u8], idle)?;
                        d.set_item(b"times_delivered" as &[u8], deliveries)?;
                        Ok::<_, PyErr>(d.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                Ok(PyList::new(py, items)?.into_any().unbind())
            }
            RawResult::StreamClaim(entries) => {
                Ok(build_stream_entries(py, entries)?.into_any().unbind())
            }
            RawResult::StreamClaimJustIds(ids) => {
                let items: Vec<Py<PyAny>> = ids
                    .into_iter()
                    .map(|id| PyBytes::new(py, &id).into_any().unbind())
                    .collect();
                Ok(PyList::new(py, items)?.into_any().unbind())
            }
            RawResult::StreamAutoclaim((next_id, entries, deleted)) => {
                let next_id_py = PyBytes::new(py, &next_id).into_any().unbind();
                let entries_py = build_stream_entries(py, entries)?.into_any().unbind();
                let deleted_py: Vec<Py<PyAny>> = deleted
                    .into_iter()
                    .map(|id| PyBytes::new(py, &id).into_any().unbind())
                    .collect();
                let deleted_list = PyList::new(py, deleted_py)?.into_any().unbind();
                Ok(PyTuple::new(py, [next_id_py, entries_py, deleted_list])?
                    .into_any()
                    .unbind())
            }
            RawResult::StreamAutoclaimJustIds((next_id, ids, deleted)) => {
                let next_id_py = PyBytes::new(py, &next_id).into_any().unbind();
                let ids_py: Vec<Py<PyAny>> = ids
                    .into_iter()
                    .map(|id| PyBytes::new(py, &id).into_any().unbind())
                    .collect();
                let ids_list = PyList::new(py, ids_py)?.into_any().unbind();
                let deleted_py: Vec<Py<PyAny>> = deleted
                    .into_iter()
                    .map(|id| PyBytes::new(py, &id).into_any().unbind())
                    .collect();
                let deleted_list = PyList::new(py, deleted_py)?.into_any().unbind();
                Ok(PyTuple::new(py, [next_id_py, ids_list, deleted_list])?
                    .into_any()
                    .unbind())
            }
            RawResult::StreamInfoStream(pairs) => {
                let dict = PyDict::new(py);
                for (k, v) in pairs {
                    let v_py = redis_value_to_py(py, v)?;
                    dict.set_item(PyBytes::new(py, &k), v_py)?;
                }
                Ok(dict.into_any().unbind())
            }
            RawResult::StreamInfoGroups(rows) => {
                let mut items: Vec<Py<PyAny>> = Vec::with_capacity(rows.len());
                for row in rows {
                    let d = PyDict::new(py);
                    for (k, v) in row {
                        let v_py = redis_value_to_py(py, v)?;
                        d.set_item(PyBytes::new(py, &k), v_py)?;
                    }
                    items.push(d.into_any().unbind());
                }
                Ok(PyList::new(py, items)?.into_any().unbind())
            }
            RawResult::StreamInfoConsumers(rows) => {
                let mut items: Vec<Py<PyAny>> = Vec::with_capacity(rows.len());
                for row in rows {
                    let d = PyDict::new(py);
                    for (k, v) in row {
                        let v_py = redis_value_to_py(py, v)?;
                        d.set_item(PyBytes::new(py, &k), v_py)?;
                    }
                    items.push(d.into_any().unbind());
                }
                Ok(PyList::new(py, items)?.into_any().unbind())
            }
        }
    }
}

#[allow(clippy::type_complexity)]
fn build_stream_entries(
    py: Python<'_>,
    entries: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>,
) -> PyResult<Bound<'_, PyList>> {
    let mut items: Vec<Py<PyAny>> = Vec::with_capacity(entries.len());
    for (id, fields) in entries {
        let id_py = PyBytes::new(py, &id).into_any().unbind();
        let dict = PyDict::new(py);
        for (k, v) in fields {
            dict.set_item(PyBytes::new(py, &k), PyBytes::new(py, &v))?;
        }
        let tuple = PyTuple::new(py, [id_py, dict.into_any().unbind()])?;
        items.push(tuple.into_any().unbind());
    }
    PyList::new(py, items)
}

// =========================================================================
// RedisRsAwaitable — deferred-callback async bridge.
//
// Verbatim port of django-vcache's RedisRsAwaitable (MIT, David Burke /
// GlitchTip), via django-cachex-redis-rs. Keep this region in lockstep
// with upstream — the design (5-poll busy-yield, callback fallback,
// done-callbacks with optional contextvars.Context, cancel that wakes
// callbacks) is load-bearing.
// =========================================================================

use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::runtime::get_runtime;

struct DoneCallback {
    callback: Py<PyAny>,
    context: Option<Py<PyAny>>,
}

struct CallbackState {
    event_loop: Py<PyAny>,
    callbacks: Vec<DoneCallback>,
    result_slot: Arc<Mutex<Option<Result<RawResult, ()>>>>,
}

#[pyclass]
pub struct RedisRsAwaitable {
    rx: Option<oneshot::Receiver<RawResult>>,
    value: Option<Py<PyAny>>,
    error: Option<Py<PyAny>>,
    resolved: bool,
    cancelled: bool,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
    polls: u8,
    cb: Option<Box<CallbackState>>,
}

fn cancelled_error(py: Python<'_>) -> PyErr {
    if let Ok(asyncio) = py.import("asyncio")
        && let Ok(cls) = asyncio.getattr("CancelledError")
        && let Ok(exc) = cls.call0()
    {
        return PyErr::from_value(exc.into_any());
    }
    pyo3::exceptions::PyRuntimeError::new_err("cancelled")
}

fn deliver_value(
    this: &mut RedisRsAwaitable,
    py: Python<'_>,
    val: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    this.resolved = true;
    this.value = Some(val.clone_ref(py));
    let stop = py
        .get_type::<pyo3::exceptions::PyStopIteration>()
        .call1((val,))?;
    Err(PyErr::from_value(stop.into_any()))
}

fn deliver_error(this: &mut RedisRsAwaitable, py: Python<'_>, err: PyErr) -> PyResult<Py<PyAny>> {
    this.resolved = true;
    this.error = Some(err.value(py).clone().into_any().unbind());
    Err(err)
}

#[pymethods]
impl RedisRsAwaitable {
    fn __await__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[getter]
    fn _loop(&self) -> Option<&Py<PyAny>> {
        self.cb.as_ref().map(|cb| &cb.event_loop)
    }

    fn __next__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut this = slf.borrow_mut(py);

        if this.cancelled {
            return Err(cancelled_error(py));
        }

        if this.resolved {
            if let Some(ref exc) = this.error {
                return Err(PyErr::from_value(exc.bind(py).clone()));
            }
            if let Some(ref value) = this.value {
                let stop = py
                    .get_type::<pyo3::exceptions::PyStopIteration>()
                    .call1((value,))?;
                return Err(PyErr::from_value(stop.into_any()));
            }
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "awaitable already consumed",
            ));
        }

        if let Some(ref cb) = this.cb {
            let maybe = cb.result_slot.lock().unwrap().take();
            if let Some(raw_result) = maybe {
                this.cb = None;
                return match raw_result {
                    Ok(raw) => match raw.into_py(py) {
                        Ok(val) => deliver_value(&mut this, py, val),
                        Err(e) => deliver_error(&mut this, py, e),
                    },
                    Err(()) => deliver_error(
                        &mut this,
                        py,
                        pyo3::exceptions::PyRuntimeError::new_err("operation was dropped"),
                    ),
                };
            }
        }

        if let Some(rx) = this.rx.as_mut() {
            match rx.try_recv() {
                Ok(raw) => {
                    this.rx = None;
                    return match raw.into_py(py) {
                        Ok(val) => deliver_value(&mut this, py, val),
                        Err(e) => deliver_error(&mut this, py, e),
                    };
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    this.rx = None;
                    return deliver_error(
                        &mut this,
                        py,
                        pyo3::exceptions::PyRuntimeError::new_err("operation was dropped"),
                    );
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
        } else if this.resolved {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "awaitable already consumed",
            ));
        }

        this.polls += 1;

        if this.polls <= 5 {
            drop(this);
            return Ok(py.None());
        }

        let rx = this.rx.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("awaitable already consumed")
        })?;

        let asyncio = py.import("asyncio")?;
        let event_loop = asyncio.call_method0("get_running_loop")?;
        this._asyncio_future_blocking = true;

        let event_loop_ref = event_loop.clone().into_any().unbind();
        let awaitable_ref = slf.clone_ref(py).into_any();
        let result_slot = Arc::new(Mutex::new(None));
        this.cb = Some(Box::new(CallbackState {
            event_loop: event_loop.into_any().unbind(),
            callbacks: Vec::new(),
            result_slot: result_slot.clone(),
        }));
        get_runtime().spawn(async move {
            let raw = rx.await;
            let raw_result = match raw {
                Ok(r) => Ok(r),
                Err(_) => Err(()),
            };
            *result_slot.lock().unwrap() = Some(raw_result);
            tokio::task::spawn_blocking(move || {
                Python::try_attach(|py| {
                    if let Ok(wake) = awaitable_ref.getattr(py, "_wake") {
                        let _ = event_loop_ref.call_method1(py, "call_soon_threadsafe", (wake,));
                    }
                });
            });
        });

        drop(this);
        Ok(slf.into_any())
    }

    fn _wake(slf: Py<Self>, py: Python<'_>) {
        let callbacks = {
            let mut this = slf.borrow_mut(py);
            this.cb
                .as_mut()
                .map(|cb| std::mem::take(&mut cb.callbacks))
                .unwrap_or_default()
        };
        for done_cb in callbacks {
            if let Some(ref ctx) = done_cb.context {
                let _ = ctx.call_method1(py, "run", (&done_cb.callback, &slf));
            } else {
                let _ = done_cb.callback.call1(py, (&slf,));
            }
        }
    }

    #[pyo3(signature = (fn_cb, *, context=None))]
    fn add_done_callback(&mut self, fn_cb: Py<PyAny>, context: Option<Py<PyAny>>) {
        if let Some(ref mut cb) = self.cb {
            cb.callbacks.push(DoneCallback {
                callback: fn_cb,
                context,
            });
        }
    }

    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.cancelled {
            return Err(cancelled_error(py));
        }
        if let Some(ref exc) = self.error {
            return Err(PyErr::from_value(exc.bind(py).clone()));
        }
        match &self.value {
            Some(v) => Ok(v.clone_ref(py)),
            None => Ok(py.None()),
        }
    }

    fn exception(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.cancelled {
            let asyncio = py.import("asyncio")?;
            let exc = asyncio.getattr("CancelledError")?.call0()?;
            return Ok(exc.into_any().unbind());
        }
        match &self.error {
            Some(exc) => Ok(exc.clone_ref(py)),
            None => Ok(py.None()),
        }
    }

    #[pyo3(signature = (msg=None))]
    fn cancel(slf: Py<Self>, py: Python<'_>, msg: Option<Py<PyAny>>) -> bool {
        let mut this = slf.borrow_mut(py);
        let _ = msg;
        if this.resolved || this.cancelled {
            return false;
        }
        this.cancelled = true;
        this.rx = None;
        let cb_state = this.cb.take();
        drop(this);
        if let Some(cb) = cb_state {
            for done_cb in cb.callbacks {
                let kwargs = pyo3::types::PyDict::new(py);
                if let Some(ref ctx) = done_cb.context {
                    let _ = kwargs.set_item("context", ctx);
                }
                let _ = cb.event_loop.call_method(
                    py,
                    "call_soon",
                    (&done_cb.callback, slf.bind(py)),
                    Some(&kwargs),
                );
            }
        }
        true
    }

    fn cancelled(&self) -> bool {
        self.cancelled
    }

    fn done(&self) -> bool {
        self.resolved || self.cancelled
    }
}

impl RedisRsAwaitable {
    pub fn new(rx: oneshot::Receiver<RawResult>) -> Self {
        RedisRsAwaitable {
            rx: Some(rx),
            value: None,
            error: None,
            resolved: false,
            cancelled: false,
            _asyncio_future_blocking: false,
            polls: 0,
            cb: None,
        }
    }
}
