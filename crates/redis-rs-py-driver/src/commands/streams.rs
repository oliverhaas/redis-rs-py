// Stream commands.
//
// Architectural note: every command flattens the reply in Rust to match
// redis-py's output shape exactly (see Plan 08, Architecture section).
// The flatten_* helpers below are private to this file.
//
// Sync variants live in `#[pymethods] impl Redis`.
// Async variants live in `#[pymethods] impl AsyncRedis` (a-prefix dropped).

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::async_bridge::RawResult;
use crate::errors::{classify, to_py_err};
use crate::exceptions::DataError;
use crate::facade::asyncio_mod::AsyncRedis;
use crate::facade::sync::Redis;
use crate::raw_result::IntoRawResult;
use crate::runtime::get_runtime;
use crate::{async_op, dispatch_cmd, sync_op};

// =========================================================================
// Python → Rust conversion helpers
// =========================================================================

/// Convert a Python `dict[str, str]` to `Vec<(String, String)>` in insertion order.
/// Used by XREAD and XREADGROUP which accept `streams` as a dict.
fn dict_to_stream_pairs(streams: &Bound<'_, PyDict>) -> PyResult<Vec<(String, String)>> {
    let mut out = Vec::with_capacity(streams.len());
    for (k, v) in streams.iter() {
        let key: String = k.extract()?;
        let id: String = v.extract()?;
        out.push((key, id));
    }
    Ok(out)
}

/// Coerce a Python value (str, bytes, int, float) to Vec<u8> for stream field values.
fn coerce_field_value(v: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = v.extract::<Vec<u8>>() {
        return Ok(b);
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(s.into_bytes());
    }
    if let Ok(n) = v.extract::<i64>() {
        return Ok(n.to_string().into_bytes());
    }
    if let Ok(f) = v.extract::<f64>() {
        return Ok(f.to_string().into_bytes());
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "stream field value must be str, bytes, int, or float",
    ))
}

/// Parse stream fields from either:
///   - dict[str, Any] → key=field-name, value=field-value
///   - list/tuple of (str, Any) pairs
fn parse_fields(obj: &Bound<'_, PyAny>) -> PyResult<Vec<(String, Vec<u8>)>> {
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut out = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            let val = coerce_field_value(&v)?;
            out.push((key, val));
        }
        return Ok(out);
    }
    // Assume iterable of (key, value) pairs
    let seq: Vec<(String, Bound<'_, PyAny>)> = obj
        .try_iter()?
        .map(|item| {
            let item = item?;
            let (k, v): (String, Bound<'_, PyAny>) = if let Ok(t) = item.cast::<PyTuple>() {
                (t.get_item(0)?.extract()?, t.get_item(1)?)
            } else if let Ok(l) = item.cast::<PyList>() {
                (l.get_item(0)?.extract()?, l.get_item(1)?)
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "each field entry must be a (key, value) tuple or list",
                ));
            };
            Ok((k, v))
        })
        .collect::<PyResult<Vec<_>>>()?;
    seq.into_iter()
        .map(|(k, v)| Ok((k, coerce_field_value(&v)?)))
        .collect()
}

// =========================================================================
// Argument-encoding helpers (cmd_*)
// =========================================================================

#[allow(clippy::too_many_arguments)]
fn cmd_xadd(
    key: &str,
    id: &str,
    fields: &[(String, Vec<u8>)],
    nomkstream: bool,
    maxlen: Option<i64>,
    minid: Option<&str>,
    approximate: bool,
    limit: Option<i64>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(key);
    if nomkstream {
        cmd.arg("NOMKSTREAM");
    }
    if let Some(n) = maxlen {
        cmd.arg("MAXLEN");
        cmd.arg(if approximate { "~" } else { "=" });
        cmd.arg(n);
        if let Some(lim) = limit {
            cmd.arg("LIMIT").arg(lim);
        }
    } else if let Some(min_id) = minid {
        cmd.arg("MINID");
        cmd.arg(if approximate { "~" } else { "=" });
        cmd.arg(min_id);
        if let Some(lim) = limit {
            cmd.arg("LIMIT").arg(lim);
        }
    }
    cmd.arg(id);
    for (f, v) in fields {
        cmd.arg(f.as_str()).arg(v.as_slice());
    }
    cmd
}

fn cmd_xlen(key: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XLEN");
    cmd.arg(key);
    cmd
}

fn cmd_xdel(key: &str, ids: &[String]) -> redis::Cmd {
    let mut cmd = redis::cmd("XDEL");
    cmd.arg(key);
    for id in ids {
        cmd.arg(id.as_str());
    }
    cmd
}

fn cmd_xack(key: &str, group: &str, ids: &[String]) -> redis::Cmd {
    let mut cmd = redis::cmd("XACK");
    cmd.arg(key).arg(group);
    for id in ids {
        cmd.arg(id.as_str());
    }
    cmd
}

fn cmd_xrange(key: &str, min: &str, max: &str, count: Option<i64>) -> redis::Cmd {
    let mut cmd = redis::cmd("XRANGE");
    cmd.arg(key).arg(min).arg(max);
    if let Some(n) = count {
        cmd.arg("COUNT").arg(n);
    }
    cmd
}

fn cmd_xrevrange(key: &str, max: &str, min: &str, count: Option<i64>) -> redis::Cmd {
    let mut cmd = redis::cmd("XREVRANGE");
    cmd.arg(key).arg(max).arg(min);
    if let Some(n) = count {
        cmd.arg("COUNT").arg(n);
    }
    cmd
}

fn cmd_xread(
    streams: &[(String, String)],
    count: Option<i64>,
    block_ms: Option<i64>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XREAD");
    if let Some(c) = count {
        cmd.arg("COUNT").arg(c);
    }
    if let Some(b) = block_ms {
        cmd.arg("BLOCK").arg(b);
    }
    cmd.arg("STREAMS");
    for (k, _) in streams {
        cmd.arg(k.as_str());
    }
    for (_, id) in streams {
        cmd.arg(id.as_str());
    }
    cmd
}

#[allow(clippy::too_many_arguments)]
fn cmd_xreadgroup(
    group: &str,
    consumer: &str,
    streams: &[(String, String)],
    count: Option<i64>,
    block_ms: Option<i64>,
    noack: bool,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XREADGROUP");
    cmd.arg("GROUP").arg(group).arg(consumer);
    if let Some(c) = count {
        cmd.arg("COUNT").arg(c);
    }
    if let Some(b) = block_ms {
        cmd.arg("BLOCK").arg(b);
    }
    if noack {
        cmd.arg("NOACK");
    }
    cmd.arg("STREAMS");
    for (k, _) in streams {
        cmd.arg(k.as_str());
    }
    for (_, id) in streams {
        cmd.arg(id.as_str());
    }
    cmd
}

fn cmd_xgroup_create(
    key: &str,
    group: &str,
    id: &str,
    mkstream: bool,
    entries_read: Option<i64>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XGROUP");
    cmd.arg("CREATE").arg(key).arg(group).arg(id);
    if mkstream {
        cmd.arg("MKSTREAM");
    }
    if let Some(n) = entries_read {
        cmd.arg("ENTRIESREAD").arg(n);
    }
    cmd
}

fn cmd_xgroup_setid(key: &str, group: &str, id: &str, entries_read: Option<i64>) -> redis::Cmd {
    let mut cmd = redis::cmd("XGROUP");
    cmd.arg("SETID").arg(key).arg(group).arg(id);
    if let Some(n) = entries_read {
        cmd.arg("ENTRIESREAD").arg(n);
    }
    cmd
}

fn cmd_xgroup_destroy(key: &str, group: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XGROUP");
    cmd.arg("DESTROY").arg(key).arg(group);
    cmd
}

fn cmd_xgroup_createconsumer(key: &str, group: &str, consumer: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XGROUP");
    cmd.arg("CREATECONSUMER").arg(key).arg(group).arg(consumer);
    cmd
}

fn cmd_xgroup_delconsumer(key: &str, group: &str, consumer: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XGROUP");
    cmd.arg("DELCONSUMER").arg(key).arg(group).arg(consumer);
    cmd
}

fn cmd_xinfo_stream(key: &str, full: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("XINFO");
    cmd.arg("STREAM").arg(key);
    if full {
        cmd.arg("FULL");
    }
    cmd
}

fn cmd_xinfo_groups(key: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XINFO");
    cmd.arg("GROUPS").arg(key);
    cmd
}

fn cmd_xinfo_consumers(key: &str, group: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XINFO");
    cmd.arg("CONSUMERS").arg(key).arg(group);
    cmd
}

fn cmd_xtrim(
    key: &str,
    maxlen: Option<i64>,
    minid: Option<&str>,
    approximate: bool,
    limit: Option<i64>,
) -> Option<redis::Cmd> {
    if maxlen.is_none() && minid.is_none() {
        return None;
    }
    let mut cmd = redis::cmd("XTRIM");
    cmd.arg(key);
    if let Some(n) = maxlen {
        cmd.arg("MAXLEN");
        cmd.arg(if approximate { "~" } else { "=" });
        cmd.arg(n);
    } else if let Some(min) = minid {
        cmd.arg("MINID");
        cmd.arg(if approximate { "~" } else { "=" });
        cmd.arg(min);
    }
    if let Some(lim) = limit {
        cmd.arg("LIMIT").arg(lim);
    }
    Some(cmd)
}

fn cmd_xpending_summary(key: &str, group: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XPENDING");
    cmd.arg(key).arg(group);
    cmd
}

#[allow(clippy::too_many_arguments)]
fn cmd_xpending_range(
    key: &str,
    group: &str,
    idle: Option<i64>,
    min: &str,
    max: &str,
    count: i64,
    consumer: Option<&str>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XPENDING");
    cmd.arg(key).arg(group);
    if let Some(ms) = idle {
        cmd.arg("IDLE").arg(ms);
    }
    cmd.arg(min).arg(max).arg(count);
    if let Some(c) = consumer {
        cmd.arg(c);
    }
    cmd
}

#[allow(clippy::too_many_arguments)]
fn cmd_xclaim(
    key: &str,
    group: &str,
    consumer: &str,
    min_idle_time: i64,
    message_ids: &[String],
    idle: Option<i64>,
    time_ms: Option<i64>,
    retrycount: Option<i64>,
    force: bool,
    justid: bool,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XCLAIM");
    cmd.arg(key).arg(group).arg(consumer).arg(min_idle_time);
    for id in message_ids {
        cmd.arg(id.as_str());
    }
    if let Some(v) = idle {
        cmd.arg("IDLE").arg(v);
    }
    if let Some(v) = time_ms {
        cmd.arg("TIME").arg(v);
    }
    if let Some(v) = retrycount {
        cmd.arg("RETRYCOUNT").arg(v);
    }
    if force {
        cmd.arg("FORCE");
    }
    if justid {
        cmd.arg("JUSTID");
    }
    cmd
}

fn cmd_xautoclaim(
    key: &str,
    group: &str,
    consumer: &str,
    min_idle_time: i64,
    start_id: &str,
    count: Option<i64>,
    justid: bool,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XAUTOCLAIM");
    cmd.arg(key)
        .arg(group)
        .arg(consumer)
        .arg(min_idle_time)
        .arg(start_id);
    if let Some(c) = count {
        cmd.arg("COUNT").arg(c);
    }
    if justid {
        cmd.arg("JUSTID");
    }
    cmd
}

fn cmd_xsetid(
    key: &str,
    id: &str,
    entries_added: Option<i64>,
    max_deleted_entry_id: Option<&str>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XSETID");
    cmd.arg(key).arg(id);
    if let Some(n) = entries_added {
        cmd.arg("ENTRIESADDED").arg(n);
    }
    if let Some(mdid) = max_deleted_entry_id {
        cmd.arg("MAXDELETEDID").arg(mdid);
    }
    cmd
}

// =========================================================================
// Reply-flattening helpers (flatten_*)
// =========================================================================

/// Coerce a `redis::Value` to bytes for the limited set of types that
/// stream commands return as keys/values/ids. Returns None if the value
/// shape is unexpected.
fn value_to_bytes(v: redis::Value) -> Option<Vec<u8>> {
    match v {
        redis::Value::BulkString(b) => Some(b),
        redis::Value::SimpleString(s) => Some(s.into_bytes()),
        redis::Value::VerbatimString { text, .. } => Some(text.into_bytes()),
        redis::Value::Int(n) => Some(n.to_string().into_bytes()),
        redis::Value::BigNumber(n) => Some(n.to_string().into_bytes()),
        redis::Value::Nil => None,
        _ => None,
    }
}

/// Convert a flat `[k, v, k, v, ...]` `Vec<Value>` into `Vec<(k_bytes, v_bytes)>`.
fn pairs_from_flat(flat: Vec<redis::Value>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::with_capacity(flat.len() / 2);
    let mut iter = flat.into_iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        if let (Some(k), Some(v)) = (value_to_bytes(k), value_to_bytes(v)) {
            out.push((k, v));
        }
    }
    out
}

/// Flatten an XRANGE/XREVRANGE/XCLAIM reply.
#[allow(clippy::type_complexity)]
fn flatten_xrange_reply(value: redis::Value) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let entries = match value {
        redis::Value::Array(items) => items,
        redis::Value::Nil => return Vec::new(),
        other => vec![other],
    };

    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let pair = match entry {
            redis::Value::Array(items) if items.len() == 2 => items,
            _ => continue,
        };
        let mut iter = pair.into_iter();
        let id = iter.next().and_then(value_to_bytes).unwrap_or_default();
        let fields_raw = iter.next();
        let fields = match fields_raw {
            Some(redis::Value::Array(flat)) => pairs_from_flat(flat),
            Some(redis::Value::Map(map_pairs)) => {
                let mut v = Vec::with_capacity(map_pairs.len());
                for (k, val) in map_pairs {
                    if let (Some(k), Some(val)) = (value_to_bytes(k), value_to_bytes(val)) {
                        v.push((k, val));
                    }
                }
                v
            }
            _ => Vec::new(),
        };
        out.push((id, fields));
    }
    out
}

/// Flatten an XREAD/XREADGROUP reply.
#[allow(clippy::type_complexity)]
fn flatten_xread_reply(
    value: redis::Value,
) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)> {
    match value {
        redis::Value::Nil => Vec::new(),
        redis::Value::Map(pairs) => {
            let mut out = Vec::with_capacity(pairs.len());
            for (k, v) in pairs {
                let key = match value_to_bytes(k) {
                    Some(b) => b,
                    None => continue,
                };
                let entries = flatten_xrange_reply(v);
                out.push((key, entries));
            }
            out
        }
        redis::Value::Array(streams) => {
            if streams.is_empty() {
                return Vec::new();
            }
            let mut out = Vec::with_capacity(streams.len());
            for stream in streams {
                let pair = match stream {
                    redis::Value::Array(items) if items.len() == 2 => items,
                    _ => continue,
                };
                let mut iter = pair.into_iter();
                let key = iter.next().and_then(value_to_bytes).unwrap_or_default();
                let entries = iter.next().map(flatten_xrange_reply).unwrap_or_default();
                out.push((key, entries));
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Flatten an XPENDING summary reply.
#[allow(clippy::type_complexity)]
fn flatten_xpending_summary(
    value: redis::Value,
) -> Option<(i64, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, i64)>)> {
    let items = match value {
        redis::Value::Array(items) if items.len() == 4 => items,
        _ => return None,
    };
    let mut iter = items.into_iter();
    let count = match iter.next() {
        Some(redis::Value::Int(n)) => n,
        _ => 0,
    };
    let min_id = iter.next().and_then(value_to_bytes);
    let max_id = iter.next().and_then(value_to_bytes);
    let consumers_raw = iter.next();

    if count == 0 && min_id.is_none() && max_id.is_none() {
        return None;
    }

    let consumers = match consumers_raw {
        Some(redis::Value::Array(rows)) => rows
            .into_iter()
            .filter_map(|row| match row {
                redis::Value::Array(parts) if parts.len() == 2 => {
                    let mut p = parts.into_iter();
                    let name = value_to_bytes(p.next().unwrap())?;
                    let n_raw = p.next().unwrap();
                    let n: i64 = match n_raw {
                        redis::Value::Int(n) => n,
                        redis::Value::BulkString(b) => {
                            std::str::from_utf8(&b).ok()?.parse().ok()?
                        }
                        redis::Value::SimpleString(s) => s.parse().ok()?,
                        _ => return None,
                    };
                    Some((name, n))
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };

    Some((
        count,
        min_id.unwrap_or_default(),
        max_id.unwrap_or_default(),
        consumers,
    ))
}

/// Flatten an XPENDING range reply.
fn flatten_xpending_range(value: redis::Value) -> Vec<(Vec<u8>, Vec<u8>, i64, i64)> {
    let rows = match value {
        redis::Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let parts = match row {
            redis::Value::Array(p) if p.len() == 4 => p,
            _ => continue,
        };
        let mut iter = parts.into_iter();
        let id = match iter.next().and_then(value_to_bytes) {
            Some(b) => b,
            None => continue,
        };
        let consumer = match iter.next().and_then(value_to_bytes) {
            Some(b) => b,
            None => continue,
        };
        let idle = match iter.next() {
            Some(redis::Value::Int(n)) => n,
            _ => 0,
        };
        let deliveries = match iter.next() {
            Some(redis::Value::Int(n)) => n,
            _ => 0,
        };
        out.push((id, consumer, idle, deliveries));
    }
    out
}

/// Flatten an XINFO STREAM reply (single map of bytes → value).
fn flatten_xinfo_stream(value: redis::Value) -> Vec<(Vec<u8>, redis::Value)> {
    map_pairs_from_value(value)
}

/// Flatten an XINFO GROUPS / XINFO CONSUMERS reply (list of maps).
fn flatten_xinfo_list(value: redis::Value) -> Vec<Vec<(Vec<u8>, redis::Value)>> {
    let rows = match value {
        redis::Value::Array(items) => items,
        redis::Value::Nil => return Vec::new(),
        _ => return Vec::new(),
    };
    rows.into_iter().map(map_pairs_from_value).collect()
}

/// Convert either a flat-array `[k, v, k, v, ...]` or a Map into
/// `Vec<(k_bytes, v_value)>`.
fn map_pairs_from_value(value: redis::Value) -> Vec<(Vec<u8>, redis::Value)> {
    match value {
        redis::Value::Map(pairs) => pairs
            .into_iter()
            .filter_map(|(k, v)| value_to_bytes(k).map(|kb| (kb, v)))
            .collect(),
        redis::Value::Array(flat) => {
            let mut out = Vec::with_capacity(flat.len() / 2);
            let mut iter = flat.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                if let Some(kb) = value_to_bytes(k) {
                    out.push((kb, v));
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Split an XAUTOCLAIM reply into its three parts.
fn split_xautoclaim_reply(value: redis::Value) -> (Vec<u8>, redis::Value, Vec<Vec<u8>>) {
    let parts = match value {
        redis::Value::Array(items) if items.len() >= 2 => items,
        _ => return (Vec::new(), redis::Value::Nil, Vec::new()),
    };
    let mut iter = parts.into_iter();
    let next_id = iter.next().and_then(value_to_bytes).unwrap_or_default();
    let middle = iter.next().unwrap_or(redis::Value::Nil);
    let deleted = match iter.next() {
        Some(redis::Value::Array(ids)) => ids.into_iter().filter_map(value_to_bytes).collect(),
        _ => Vec::new(),
    };
    (next_id, middle, deleted)
}

// =========================================================================
// Sync impl (Redis)
// =========================================================================

#[pymethods]
impl Redis {
    // ----- XADD -----

    /// XADD — accepts two call styles matching redis-py:
    ///   `xadd(key, id, fields)` — explicit entry-ID (old style, used by driver tests)
    ///   `xadd(key, fields, id="*")` — redis-py compatible, where `fields` is
    ///       a `dict[str, Any]` or list of `(str, Any)` pairs (new facade style)
    ///
    /// Disambiguation: if the second positional arg (`id_or_fields`) is a str we
    /// treat it as an explicit ID and require the third positional arg as fields.
    /// If it is a dict / list we treat it as the fields and use the keyword `id`
    /// (default `"*"`).
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, id_or_fields, fields = None, *,
        id = "*",
        nomkstream = false,
        maxlen = None,
        minid = None,
        approximate = true,
        limit = None,
    ))]
    fn xadd(
        &self,
        py: Python<'_>,
        key: &str,
        id_or_fields: Bound<'_, PyAny>,
        fields: Option<Bound<'_, PyAny>>,
        id: &str,
        nomkstream: bool,
        maxlen: Option<i64>,
        minid: Option<String>,
        approximate: bool,
        limit: Option<i64>,
    ) -> PyResult<Option<String>> {
        // Resolve (entry_id, fields_vec)
        let (entry_id, fields_vec) = if let Ok(s) = id_or_fields.extract::<String>() {
            // Old style: xadd(key, id_str, fields_list)
            let f = fields.ok_or_else(|| {
                pyo3::exceptions::PyTypeError::new_err(
                    "xadd(key, id, fields): 'fields' is required when second arg is a string",
                )
            })?;
            (s, parse_fields(&f)?)
        } else {
            // New style: xadd(key, fields_dict_or_list, *, id="*")
            (id.to_string(), parse_fields(&id_or_fields)?)
        };
        let cmd = cmd_xadd(
            key,
            &entry_id,
            &fields_vec,
            nomkstream,
            maxlen,
            minid.as_deref(),
            approximate,
            limit,
        );
        let r: redis::RedisResult<Option<String>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    // ----- XLEN -----

    #[pyo3(signature = (key))]
    fn xlen(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        let cmd = cmd_xlen(key);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    // ----- XDEL -----

    #[pyo3(signature = (key, *ids))]
    fn xdel(&self, py: Python<'_>, key: &str, ids: Vec<String>) -> PyResult<i64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let cmd = cmd_xdel(key, &ids);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    // ----- XACK -----

    #[pyo3(signature = (key, group, *ids))]
    fn xack(&self, py: Python<'_>, key: &str, group: &str, ids: Vec<String>) -> PyResult<i64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let cmd = cmd_xack(key, group, &ids);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    // ----- XRANGE -----

    #[pyo3(signature = (key, min = "-", max = "+", *, count = None))]
    fn xrange(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xrange(key, min, max, count);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let entries = flatten_xrange_reply(r.map_err(to_py_err)?);
        RawResult::StreamEntries(entries).into_py(py)
    }

    // ----- XREVRANGE -----

    #[pyo3(signature = (key, max = "+", min = "-", *, count = None))]
    fn xrevrange(
        &self,
        py: Python<'_>,
        key: &str,
        max: &str,
        min: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xrevrange(key, max, min, count);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let entries = flatten_xrange_reply(r.map_err(to_py_err)?);
        RawResult::StreamEntries(entries).into_py(py)
    }

    // ----- XREAD -----

    #[pyo3(signature = (streams, *, count = None, block = None))]
    fn xread(
        &self,
        py: Python<'_>,
        streams: &Bound<'_, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let streams = dict_to_stream_pairs(streams)?;
        let cmd = cmd_xread(&streams, count, block);
        let r: redis::RedisResult<redis::Value> = py.detach(|| {
            get_runtime().block_on(async {
                let mut conn = self.connection.clone();
                if block.is_some() {
                    let mut blocking_inner = conn.get_blocking().await?;
                    dispatch_cmd!(&mut blocking_inner, cmd)
                } else {
                    dispatch_cmd!(&mut *conn, cmd)
                }
            })
        });
        let entries = flatten_xread_reply(r.map_err(to_py_err)?);
        RawResult::StreamReadEntries(entries).into_py(py)
    }

    // ----- XREADGROUP -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group, consumer, streams, *, count = None, block = None, noack = false))]
    fn xreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        streams: &Bound<'_, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
        noack: bool,
    ) -> PyResult<Py<PyAny>> {
        let streams = dict_to_stream_pairs(streams)?;
        let cmd = cmd_xreadgroup(group, consumer, &streams, count, block, noack);
        let r: redis::RedisResult<redis::Value> = py.detach(|| {
            get_runtime().block_on(async {
                let mut conn = self.connection.clone();
                if block.is_some() {
                    let mut blocking_inner = conn.get_blocking().await?;
                    dispatch_cmd!(&mut blocking_inner, cmd)
                } else {
                    dispatch_cmd!(&mut *conn, cmd)
                }
            })
        });
        let entries = flatten_xread_reply(r.map_err(to_py_err)?);
        RawResult::StreamReadEntries(entries).into_py(py)
    }

    // ----- XGROUP CREATE -----

    #[pyo3(signature = (key, group, id = "0", *, mkstream = false, entries_read = None))]
    fn xgroup_create(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
        entries_read: Option<i64>,
    ) -> PyResult<()> {
        let cmd = cmd_xgroup_create(key, group, id, mkstream, entries_read);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    // ----- XGROUP SETID -----

    #[pyo3(signature = (key, group, *, id, entries_read = None))]
    fn xgroup_setid(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        id: &str,
        entries_read: Option<i64>,
    ) -> PyResult<()> {
        let cmd = cmd_xgroup_setid(key, group, id, entries_read);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    // ----- XGROUP DESTROY -----

    fn xgroup_destroy(&self, py: Python<'_>, key: &str, group: &str) -> PyResult<i64> {
        let cmd = cmd_xgroup_destroy(key, group);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    // ----- XGROUP CREATECONSUMER -----

    fn xgroup_createconsumer(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> PyResult<i64> {
        let cmd = cmd_xgroup_createconsumer(key, group, consumer);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    // ----- XGROUP DELCONSUMER -----

    fn xgroup_delconsumer(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> PyResult<i64> {
        let cmd = cmd_xgroup_delconsumer(key, group, consumer);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    // ----- XINFO STREAM -----

    #[pyo3(signature = (key, *, full = false))]
    fn xinfo_stream(&self, py: Python<'_>, key: &str, full: bool) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xinfo_stream(key, full);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let pairs = flatten_xinfo_stream(r.map_err(to_py_err)?);
        RawResult::StreamInfoStream(pairs).into_py(py)
    }

    // ----- XINFO GROUPS -----

    fn xinfo_groups(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xinfo_groups(key);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::StreamInfoGroups(flatten_xinfo_list(r.map_err(to_py_err)?)).into_py(py)
    }

    // ----- XINFO CONSUMERS -----

    fn xinfo_consumers(&self, py: Python<'_>, key: &str, group: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xinfo_consumers(key, group);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::StreamInfoConsumers(flatten_xinfo_list(r.map_err(to_py_err)?)).into_py(py)
    }

    // ----- XTRIM -----

    #[pyo3(signature = (key, *, maxlen = None, minid = None, approximate = true, limit = None))]
    fn xtrim(
        &self,
        py: Python<'_>,
        key: &str,
        maxlen: Option<i64>,
        minid: Option<String>,
        approximate: bool,
        limit: Option<i64>,
    ) -> PyResult<i64> {
        let cmd = cmd_xtrim(key, maxlen, minid.as_deref(), approximate, limit)
            .ok_or_else(|| PyErr::new::<DataError, _>("xtrim requires maxlen or minid"))?;
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    // ----- XPENDING -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, *,
        idle = None,
        min = None,
        max = None,
        count = None,
        consumer = None,
    ))]
    fn xpending(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        idle: Option<i64>,
        min: Option<String>,
        max: Option<String>,
        count: Option<i64>,
        consumer: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let is_range = min.is_some() || max.is_some() || count.is_some();
        let cmd = if is_range {
            let min_s = min.as_deref().unwrap_or("-");
            let max_s = max.as_deref().unwrap_or("+");
            let cnt = count.unwrap_or(10);
            cmd_xpending_range(key, group, idle, min_s, max_s, cnt, consumer.as_deref())
        } else {
            cmd_xpending_summary(key, group)
        };
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let value = r.map_err(to_py_err)?;
        if is_range {
            RawResult::StreamPendingRange(flatten_xpending_range(value)).into_py(py)
        } else {
            RawResult::StreamPendingSummary(flatten_xpending_summary(value)).into_py(py)
        }
    }

    // ----- XCLAIM -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, consumer, *,
        min_idle_time,
        message_ids,
        idle = None,
        time = None,
        retrycount = None,
        force = false,
        justid = false,
    ))]
    fn xclaim(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        message_ids: Vec<String>,
        idle: Option<i64>,
        time: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xclaim(
            key,
            group,
            consumer,
            min_idle_time,
            &message_ids,
            idle,
            time,
            retrycount,
            force,
            justid,
        );
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let value = r.map_err(to_py_err)?;
        if justid {
            let ids = match value {
                redis::Value::Array(items) => {
                    items.into_iter().filter_map(value_to_bytes).collect()
                }
                _ => Vec::new(),
            };
            RawResult::StreamClaimJustIds(ids).into_py(py)
        } else {
            let entries = flatten_xrange_reply(value);
            RawResult::StreamClaim(entries).into_py(py)
        }
    }

    // ----- XAUTOCLAIM -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, consumer, *,
        min_idle_time,
        start_id = "0-0",
        count = 100,
        justid = false,
    ))]
    fn xautoclaim(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        start_id: &str,
        count: i64,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xautoclaim(
            key,
            group,
            consumer,
            min_idle_time,
            start_id,
            Some(count),
            justid,
        );
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let (next_id, middle, deleted) = split_xautoclaim_reply(r.map_err(to_py_err)?);
        if justid {
            let ids = match middle {
                redis::Value::Array(items) => {
                    items.into_iter().filter_map(value_to_bytes).collect()
                }
                _ => Vec::new(),
            };
            RawResult::StreamAutoclaimJustIds((next_id, ids, deleted)).into_py(py)
        } else {
            let entries = flatten_xrange_reply(middle);
            RawResult::StreamAutoclaim((next_id, entries, deleted)).into_py(py)
        }
    }

    // ----- XSETID -----

    #[pyo3(signature = (key, id, *, entries_added = None, max_deleted_entry_id = None))]
    fn xsetid(
        &self,
        py: Python<'_>,
        key: &str,
        id: &str,
        entries_added: Option<i64>,
        max_deleted_entry_id: Option<String>,
    ) -> PyResult<()> {
        let cmd = cmd_xsetid(key, id, entries_added, max_deleted_entry_id.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }
}

// =========================================================================
// Async impl (AsyncRedis)
// =========================================================================

#[pymethods]
impl AsyncRedis {
    // ----- XADD -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, id_or_fields, fields = None, *,
        id = "*",
        nomkstream = false,
        maxlen = None,
        minid = None,
        approximate = true,
        limit = None,
    ))]
    fn xadd(
        &self,
        py: Python<'_>,
        key: &str,
        id_or_fields: Bound<'_, PyAny>,
        fields: Option<Bound<'_, PyAny>>,
        id: &str,
        nomkstream: bool,
        maxlen: Option<i64>,
        minid: Option<String>,
        approximate: bool,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let (entry_id, fields_vec) = if let Ok(s) = id_or_fields.extract::<String>() {
            let f = fields.ok_or_else(|| {
                pyo3::exceptions::PyTypeError::new_err(
                    "xadd(key, id, fields): 'fields' is required when second arg is a string",
                )
            })?;
            (s, parse_fields(&f)?)
        } else {
            (id.to_string(), parse_fields(&id_or_fields)?)
        };
        async_op!(self, py, conn, async {
            let cmd = cmd_xadd(
                &key,
                &entry_id,
                &fields_vec,
                nomkstream,
                maxlen,
                minid.as_deref(),
                approximate,
                limit,
            );
            let r: redis::RedisResult<Option<String>> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(opt) => RawResult::OptStr(opt),
                Err(e) => classify(e),
            }
        })
    }

    // ----- XLEN -----

    #[pyo3(signature = (key))]
    fn xlen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xlen(&key);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // ----- XDEL -----

    #[pyo3(signature = (key, *ids))]
    fn xdel(&self, py: Python<'_>, key: &str, ids: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if ids.is_empty() {
                return RawResult::Int(0);
            }
            let cmd = cmd_xdel(&key, &ids);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // ----- XACK -----

    #[pyo3(signature = (key, group, *ids))]
    fn xack(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        ids: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        async_op!(self, py, conn, async {
            if ids.is_empty() {
                return RawResult::Int(0);
            }
            let cmd = cmd_xack(&key, &group, &ids);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // ----- XRANGE -----

    #[pyo3(signature = (key, min = "-", max = "+", *, count = None))]
    fn xrange(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let min = min.to_string();
        let max = max.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xrange(&key, &min, &max, count);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::StreamEntries(flatten_xrange_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    // ----- XREVRANGE -----

    #[pyo3(signature = (key, max = "+", min = "-", *, count = None))]
    fn xrevrange(
        &self,
        py: Python<'_>,
        key: &str,
        max: &str,
        min: &str,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let max = max.to_string();
        let min = min.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xrevrange(&key, &max, &min, count);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::StreamEntries(flatten_xrange_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    // ----- XREAD -----

    #[pyo3(signature = (streams, *, count = None, block = None))]
    fn xread(
        &self,
        py: Python<'_>,
        streams: &Bound<'_, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let streams = dict_to_stream_pairs(streams)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_xread(&streams, count, block);
            let r: redis::RedisResult<redis::Value> = if block.is_some() {
                match conn.get_blocking().await {
                    Ok(mut blocking_inner) => dispatch_cmd!(&mut blocking_inner, cmd),
                    Err(e) => Err(e),
                }
            } else {
                dispatch_cmd!(&mut *conn, cmd)
            };
            match r {
                Ok(v) => RawResult::StreamReadEntries(flatten_xread_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    // ----- XREADGROUP -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group, consumer, streams, *, count = None, block = None, noack = false))]
    fn xreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        streams: &Bound<'_, PyDict>,
        count: Option<i64>,
        block: Option<i64>,
        noack: bool,
    ) -> PyResult<Py<PyAny>> {
        let group = group.to_string();
        let consumer = consumer.to_string();
        let streams = dict_to_stream_pairs(streams)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_xreadgroup(&group, &consumer, &streams, count, block, noack);
            let r: redis::RedisResult<redis::Value> = if block.is_some() {
                match conn.get_blocking().await {
                    Ok(mut blocking_inner) => dispatch_cmd!(&mut blocking_inner, cmd),
                    Err(e) => Err(e),
                }
            } else {
                dispatch_cmd!(&mut *conn, cmd)
            };
            match r {
                Ok(v) => RawResult::StreamReadEntries(flatten_xread_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    // ----- XGROUP CREATE -----

    #[pyo3(signature = (key, group, id = "0", *, mkstream = false, entries_read = None))]
    fn xgroup_create(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
        entries_read: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let id = id.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xgroup_create(&key, &group, &id, mkstream, entries_read);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    // ----- XGROUP SETID -----

    #[pyo3(signature = (key, group, *, id, entries_read = None))]
    fn xgroup_setid(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        id: &str,
        entries_read: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let id = id.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xgroup_setid(&key, &group, &id, entries_read);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    // ----- XGROUP DESTROY -----

    fn xgroup_destroy(&self, py: Python<'_>, key: &str, group: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xgroup_destroy(&key, &group);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // ----- XGROUP CREATECONSUMER -----

    fn xgroup_createconsumer(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xgroup_createconsumer(&key, &group, &consumer);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // ----- XGROUP DELCONSUMER -----

    fn xgroup_delconsumer(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xgroup_delconsumer(&key, &group, &consumer);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // ----- XINFO STREAM -----

    #[pyo3(signature = (key, *, full = false))]
    fn xinfo_stream(&self, py: Python<'_>, key: &str, full: bool) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xinfo_stream(&key, full);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::StreamInfoStream(flatten_xinfo_stream(v)),
                Err(e) => classify(e),
            }
        })
    }

    // ----- XINFO GROUPS -----

    fn xinfo_groups(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xinfo_groups(&key);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::StreamInfoGroups(flatten_xinfo_list(v)),
                Err(e) => classify(e),
            }
        })
    }

    // ----- XINFO CONSUMERS -----

    fn xinfo_consumers(&self, py: Python<'_>, key: &str, group: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xinfo_consumers(&key, &group);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::StreamInfoConsumers(flatten_xinfo_list(v)),
                Err(e) => classify(e),
            }
        })
    }

    // ----- XTRIM -----

    #[pyo3(signature = (key, *, maxlen = None, minid = None, approximate = true, limit = None))]
    fn xtrim(
        &self,
        py: Python<'_>,
        key: &str,
        maxlen: Option<i64>,
        minid: Option<String>,
        approximate: bool,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let cmd = cmd_xtrim(&key, maxlen, minid.as_deref(), approximate, limit)
            .ok_or_else(|| PyErr::new::<DataError, _>("xtrim requires maxlen or minid"))?;
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // ----- XPENDING -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, *,
        idle = None,
        min = None,
        max = None,
        count = None,
        consumer = None,
    ))]
    fn xpending(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        idle: Option<i64>,
        min: Option<String>,
        max: Option<String>,
        count: Option<i64>,
        consumer: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let is_range = min.is_some() || max.is_some() || count.is_some();
        async_op!(self, py, conn, async {
            let cmd = if is_range {
                let min_s: String = min.unwrap_or_else(|| "-".to_string());
                let max_s: String = max.unwrap_or_else(|| "+".to_string());
                let cnt = count.unwrap_or(10);
                cmd_xpending_range(&key, &group, idle, &min_s, &max_s, cnt, consumer.as_deref())
            } else {
                cmd_xpending_summary(&key, &group)
            };
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => {
                    if is_range {
                        RawResult::StreamPendingRange(flatten_xpending_range(v))
                    } else {
                        RawResult::StreamPendingSummary(flatten_xpending_summary(v))
                    }
                }
                Err(e) => classify(e),
            }
        })
    }

    // ----- XCLAIM -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, consumer, *,
        min_idle_time,
        message_ids,
        idle = None,
        time = None,
        retrycount = None,
        force = false,
        justid = false,
    ))]
    fn xclaim(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        message_ids: Vec<String>,
        idle: Option<i64>,
        time: Option<i64>,
        retrycount: Option<i64>,
        force: bool,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xclaim(
                &key,
                &group,
                &consumer,
                min_idle_time,
                &message_ids,
                idle,
                time,
                retrycount,
                force,
                justid,
            );
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(value) => {
                    if justid {
                        let ids = match value {
                            redis::Value::Array(items) => {
                                items.into_iter().filter_map(value_to_bytes).collect()
                            }
                            _ => Vec::new(),
                        };
                        RawResult::StreamClaimJustIds(ids)
                    } else {
                        RawResult::StreamClaim(flatten_xrange_reply(value))
                    }
                }
                Err(e) => classify(e),
            }
        })
    }

    // ----- XAUTOCLAIM -----

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, consumer, *,
        min_idle_time,
        start_id = "0-0",
        count = 100,
        justid = false,
    ))]
    fn xautoclaim(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        start_id: &str,
        count: i64,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        let consumer = consumer.to_string();
        let start_id = start_id.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xautoclaim(
                &key,
                &group,
                &consumer,
                min_idle_time,
                &start_id,
                Some(count),
                justid,
            );
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(value) => {
                    let (next_id, middle, deleted) = split_xautoclaim_reply(value);
                    if justid {
                        let ids = match middle {
                            redis::Value::Array(items) => {
                                items.into_iter().filter_map(value_to_bytes).collect()
                            }
                            _ => Vec::new(),
                        };
                        RawResult::StreamAutoclaimJustIds((next_id, ids, deleted))
                    } else {
                        RawResult::StreamAutoclaim((next_id, flatten_xrange_reply(middle), deleted))
                    }
                }
                Err(e) => classify(e),
            }
        })
    }

    // ----- XSETID -----

    #[pyo3(signature = (key, id, *, entries_added = None, max_deleted_entry_id = None))]
    fn xsetid(
        &self,
        py: Python<'_>,
        key: &str,
        id: &str,
        entries_added: Option<i64>,
        max_deleted_entry_id: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let id = id.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xsetid(&key, &id, entries_added, max_deleted_entry_id.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
}
