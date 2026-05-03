// Admin / introspection commands for RedisRsDriver.
//
// SCAN family + KEYS/RANDOMKEY + DBSIZE/FLUSHDB/FLUSHALL/SELECT +
// INFO/CONFIG */CLIENT */OBJECT */MEMORY USAGE +
// PING/ECHO/WAIT/WAITAOF/TIME/LASTSAVE/BGSAVE/BGREWRITEAOF/DEBUG SLEEP.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::async_bridge::RawResult;
use crate::driver::RedisRsDriver;
use crate::errors::{classify, to_py_err};
use crate::raw_result::IntoRawResult;
use crate::{async_op, dispatch_cmd, sync_op};

// =========================================================================
// Module-private helpers
// =========================================================================

fn warn_keys_use(py: Python<'_>) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    let _ = warnings.call_method1(
        "warn",
        (
            "KEYS scans the entire keyspace and blocks the server. Use scan_iter() instead.",
            py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
        ),
    );
    Ok(())
}

/// CONFIG SET kwarg coercion: accept either `(name: str, value: str)` or
/// `(mapping: dict[str, str], value=None)`. Returns the flat list of pairs.
fn config_set_extract_pairs(
    name_or_mapping: &Bound<'_, pyo3::PyAny>,
    value: Option<String>,
) -> PyResult<Vec<(String, String)>> {
    if let Ok(s) = name_or_mapping.extract::<String>() {
        let v = value.ok_or_else(|| {
            pyo3::PyErr::new::<crate::exceptions::DataError, _>(
                "config_set(name, value) requires a value when name is a string",
            )
        })?;
        return Ok(vec![(s, v)]);
    }
    // Accept a dict[str, str] mapping.
    if let Ok(dict) = name_or_mapping.cast::<PyDict>() {
        let mut out = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            out.push((k.extract::<String>()?, v.extract::<String>()?));
        }
        return Ok(out);
    }
    // Fall back to sequence-of-tuples.
    let seq: Vec<(String, String)> = name_or_mapping.extract()?;
    Ok(seq)
}

// =========================================================================
// Argument-encoding helpers (cmd_*)
// =========================================================================

fn cmd_scan(
    cursor: u64,
    match_pattern: Option<&str>,
    count: Option<i64>,
    type_filter: Option<&str>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("SCAN");
    cmd.arg(cursor);
    if let Some(p) = match_pattern {
        cmd.arg("MATCH").arg(p);
    }
    if let Some(c) = count {
        cmd.arg("COUNT").arg(c);
    }
    if let Some(t) = type_filter {
        cmd.arg("TYPE").arg(t);
    }
    cmd
}

fn cmd_keys(pattern: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("KEYS");
    cmd.arg(pattern);
    cmd
}

fn cmd_randomkey() -> redis::Cmd {
    redis::cmd("RANDOMKEY")
}

/// Convert a SCAN reply (cursor as string|int, then array of bytes-keys)
/// into the typed pair our Python users expect.
fn parse_scan_reply(value: redis::Value) -> (u64, Vec<Vec<u8>>) {
    let parts = match value {
        redis::Value::Array(items) if items.len() == 2 => items,
        _ => return (0, Vec::new()),
    };
    let mut iter = parts.into_iter();
    let cursor = match iter.next() {
        Some(redis::Value::BulkString(b)) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        Some(redis::Value::Int(n)) => n.max(0) as u64,
        Some(redis::Value::SimpleString(s)) => s.parse().unwrap_or(0),
        _ => 0,
    };
    let keys = match iter.next() {
        Some(redis::Value::Array(items)) => items
            .into_iter()
            .filter_map(|v| match v {
                redis::Value::BulkString(b) => Some(b),
                redis::Value::SimpleString(s) => Some(s.into_bytes()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    (cursor, keys)
}

fn cmd_dbsize() -> redis::Cmd {
    redis::cmd("DBSIZE")
}

fn cmd_flushdb(asynchronous: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("FLUSHDB");
    if asynchronous {
        cmd.arg("ASYNC");
    } else {
        cmd.arg("SYNC");
    }
    cmd
}

fn cmd_flushall(asynchronous: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("FLUSHALL");
    if asynchronous {
        cmd.arg("ASYNC");
    } else {
        cmd.arg("SYNC");
    }
    cmd
}

/// Extract the `/<db>` segment from a redis URL (defaults to 0 if missing
/// or unparseable). Used by SELECT to validate the requested db matches
/// the connected db.
fn url_db_index(url: &str) -> u8 {
    let after_scheme = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => return 0,
    };
    let after_host = match after_scheme.split_once('/') {
        Some((_, path)) => path,
        None => return 0,
    };
    let path_segment = after_host.split(['?', '#']).next().unwrap_or("");
    path_segment.parse().unwrap_or(0)
}

/// Extract a byte vector from a redis::Value that may be BulkString,
/// SimpleString, or VerbatimString (the last is sent by Valkey/Redis >= 7
/// over RESP3 for INFO, CLIENT INFO, CLIENT LIST).
fn value_to_bytes(v: redis::Value) -> Vec<u8> {
    match v {
        redis::Value::BulkString(b) => b,
        redis::Value::SimpleString(s) => s.into_bytes(),
        redis::Value::VerbatimString { text, .. } => text.into_bytes(),
        _ => Vec::new(),
    }
}

fn cmd_info(section: Option<&str>) -> redis::Cmd {
    let mut cmd = redis::cmd("INFO");
    if let Some(s) = section {
        cmd.arg(s);
    }
    cmd
}

fn cmd_config_get(parameter: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("CONFIG");
    cmd.arg("GET").arg(parameter);
    cmd
}

fn cmd_config_set(pairs: &[(String, String)]) -> redis::Cmd {
    let mut cmd = redis::cmd("CONFIG");
    cmd.arg("SET");
    for (k, v) in pairs {
        cmd.arg(k.as_str()).arg(v.as_str());
    }
    cmd
}

fn cmd_config_resetstat() -> redis::Cmd {
    let mut cmd = redis::cmd("CONFIG");
    cmd.arg("RESETSTAT");
    cmd
}

fn cmd_config_rewrite() -> redis::Cmd {
    let mut cmd = redis::cmd("CONFIG");
    cmd.arg("REWRITE");
    cmd
}

/// Flatten a CONFIG GET reply (Map or flat-Array of key/value pairs)
/// into the typed pair-list.
fn parse_config_get_reply(value: redis::Value) -> Vec<(Vec<u8>, Vec<u8>)> {
    match value {
        redis::Value::Map(pairs) => pairs
            .into_iter()
            .filter_map(|(k, v)| match (k, v) {
                (redis::Value::BulkString(kb), redis::Value::BulkString(vb)) => Some((kb, vb)),
                (redis::Value::SimpleString(ks), redis::Value::BulkString(vb)) => {
                    Some((ks.into_bytes(), vb))
                }
                (redis::Value::BulkString(kb), redis::Value::SimpleString(vs)) => {
                    Some((kb, vs.into_bytes()))
                }
                (redis::Value::SimpleString(ks), redis::Value::SimpleString(vs)) => {
                    Some((ks.into_bytes(), vs.into_bytes()))
                }
                _ => None,
            })
            .collect(),
        redis::Value::Array(flat) => {
            let mut out = Vec::with_capacity(flat.len() / 2);
            let mut iter = flat.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                match (k, v) {
                    (redis::Value::BulkString(kb), redis::Value::BulkString(vb)) => {
                        out.push((kb, vb));
                    }
                    (redis::Value::SimpleString(ks), redis::Value::BulkString(vb)) => {
                        out.push((ks.into_bytes(), vb));
                    }
                    (redis::Value::BulkString(kb), redis::Value::SimpleString(vs)) => {
                        out.push((kb, vs.into_bytes()));
                    }
                    (redis::Value::SimpleString(ks), redis::Value::SimpleString(vs)) => {
                        out.push((ks.into_bytes(), vs.into_bytes()));
                    }
                    _ => {}
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

fn cmd_client_id() -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("ID");
    cmd
}

fn cmd_client_getname() -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("GETNAME");
    cmd
}

fn cmd_client_setname(name: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("SETNAME").arg(name);
    cmd
}

fn cmd_client_info() -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("INFO");
    cmd
}

fn cmd_client_list(client_type: Option<&str>, client_ids: &[i64]) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("LIST");
    if let Some(t) = client_type {
        cmd.arg("TYPE").arg(t);
    }
    if !client_ids.is_empty() {
        cmd.arg("ID");
        for id in client_ids {
            cmd.arg(*id);
        }
    }
    cmd
}

/// Parse a `CLIENT LIST` text reply (newline-separated lines of
/// `key=value key=value ...`) into a list of dict[bytes, bytes].
fn parse_client_list_reply(text: &[u8]) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for line in text.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let mut row = Vec::new();
        for pair in line.split(|&b| b == b' ') {
            if let Some(eq_pos) = pair.iter().position(|&b| b == b'=') {
                let (k, v) = pair.split_at(eq_pos);
                row.push((k.to_vec(), v[1..].to_vec()));
            }
        }
        if !row.is_empty() {
            out.push(row);
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn cmd_client_kill(
    addr: Option<&str>,
    laddr: Option<&str>,
    client_id: Option<i64>,
    client_type: Option<&str>,
    user: Option<&str>,
    skipme: Option<bool>,
    maxage: Option<i64>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("KILL");
    if let Some(a) = addr {
        cmd.arg("ADDR").arg(a);
    }
    if let Some(la) = laddr {
        cmd.arg("LADDR").arg(la);
    }
    if let Some(id) = client_id {
        cmd.arg("ID").arg(id);
    }
    if let Some(t) = client_type {
        cmd.arg("TYPE").arg(t);
    }
    if let Some(u) = user {
        cmd.arg("USER").arg(u);
    }
    if let Some(skip) = skipme {
        cmd.arg("SKIPME").arg(if skip { "yes" } else { "no" });
    }
    if let Some(age) = maxage {
        cmd.arg("MAXAGE").arg(age);
    }
    cmd
}

fn cmd_client_pause(timeout_ms: i64, all: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("PAUSE").arg(timeout_ms);
    cmd.arg(if all { "ALL" } else { "WRITE" });
    cmd
}

fn cmd_client_unpause() -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("UNPAUSE");
    cmd
}

fn cmd_client_no_evict(mode: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("NO-EVICT").arg(mode);
    cmd
}

fn cmd_client_no_touch(mode: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("CLIENT");
    cmd.arg("NO-TOUCH").arg(mode);
    cmd
}

fn validate_on_off_mode(mode: &str) -> PyResult<()> {
    match mode.to_ascii_uppercase().as_str() {
        "ON" | "OFF" => Ok(()),
        _ => Err(pyo3::PyErr::new::<crate::exceptions::DataError, _>(
            format!("mode must be ON or OFF, got {mode}"),
        )),
    }
}

/// OBJECT HELP (and similar) can return either an Array of bulk-strings
/// (RESP2 / older Valkey) or a SimpleString / VerbatimString (RESP3 Valkey ≥ 8).
/// Normalise to a list of byte lines in all cases.
fn parse_help_reply(v: redis::Value) -> Vec<Vec<u8>> {
    match v {
        redis::Value::Array(items) => items
            .into_iter()
            .filter_map(|item| match item {
                redis::Value::BulkString(b) => Some(b),
                redis::Value::SimpleString(s) => Some(s.into_bytes()),
                redis::Value::VerbatimString { text, .. } => Some(text.into_bytes()),
                _ => None,
            })
            .collect(),
        redis::Value::BulkString(b) => b
            .split(|&c| c == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| l.to_vec())
            .collect(),
        redis::Value::SimpleString(s) => s
            .as_bytes()
            .split(|&c| c == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| l.to_vec())
            .collect(),
        redis::Value::VerbatimString { text, .. } => text
            .as_bytes()
            .split(|&c| c == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| l.to_vec())
            .collect(),
        _ => Vec::new(),
    }
}

fn cmd_object_subcmd(subcmd: &str, key: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("OBJECT");
    cmd.arg(subcmd).arg(key);
    cmd
}

fn cmd_object_help() -> redis::Cmd {
    let mut cmd = redis::cmd("OBJECT");
    cmd.arg("HELP");
    cmd
}

fn cmd_memory_usage(key: &str, samples: Option<i64>) -> redis::Cmd {
    let mut cmd = redis::cmd("MEMORY");
    cmd.arg("USAGE").arg(key);
    if let Some(s) = samples {
        cmd.arg("SAMPLES").arg(s);
    }
    cmd
}

fn cmd_echo(message: &[u8]) -> redis::Cmd {
    let mut cmd = redis::cmd("ECHO");
    cmd.arg(message);
    cmd
}

fn cmd_wait(numreplicas: i64, timeout_ms: i64) -> redis::Cmd {
    let mut cmd = redis::cmd("WAIT");
    cmd.arg(numreplicas).arg(timeout_ms);
    cmd
}

fn cmd_waitaof(numlocal: i64, numreplicas: i64, timeout_ms: i64) -> redis::Cmd {
    let mut cmd = redis::cmd("WAITAOF");
    cmd.arg(numlocal).arg(numreplicas).arg(timeout_ms);
    cmd
}

fn cmd_time() -> redis::Cmd {
    redis::cmd("TIME")
}

fn cmd_lastsave() -> redis::Cmd {
    redis::cmd("LASTSAVE")
}

fn cmd_bgsave(schedule: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("BGSAVE");
    if schedule {
        cmd.arg("SCHEDULE");
    }
    cmd
}

fn cmd_bgrewriteaof() -> redis::Cmd {
    redis::cmd("BGREWRITEAOF")
}

fn cmd_debug_sleep(seconds: f64) -> redis::Cmd {
    let mut cmd = redis::cmd("DEBUG");
    cmd.arg("SLEEP").arg(format!("{seconds:.6}"));
    cmd
}

/// Parse a TIME reply: Array(vec![BulkString("seconds"), BulkString("microseconds")]).
fn parse_time_reply(value: redis::Value) -> Option<(String, String)> {
    let parts = match value {
        redis::Value::Array(items) if items.len() == 2 => items,
        _ => return None,
    };
    let mut iter = parts.into_iter();
    let secs = match iter.next()? {
        redis::Value::BulkString(b) => String::from_utf8(b).ok()?,
        redis::Value::SimpleString(s) => s,
        _ => return None,
    };
    let usecs = match iter.next()? {
        redis::Value::BulkString(b) => String::from_utf8(b).ok()?,
        redis::Value::SimpleString(s) => s,
        _ => return None,
    };
    Some((secs, usecs))
}

// =========================================================================
// RedisRsDriver method impls
// =========================================================================

#[pymethods]
impl RedisRsDriver {
    // --- SCAN / KEYS / RANDOMKEY ---

    #[pyo3(signature = (*, cursor=0, r#match=None, count=None, r#type=None))]
    fn scan(
        &self,
        py: Python<'_>,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
        r#type: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_scan(cursor, r#match.as_deref(), count, r#type.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let (next_cursor, keys) = parse_scan_reply(r.map_err(to_py_err)?);
        let cursor_py = next_cursor.into_pyobject(py)?.into_any().unbind();
        let keys_py: Vec<Py<PyAny>> = keys
            .into_iter()
            .map(|k| PyBytes::new(py, &k).into_any().unbind())
            .collect();
        let keys_list = PyList::new(py, keys_py)?.into_any().unbind();
        Ok(PyTuple::new(py, [cursor_py, keys_list])?
            .into_any()
            .unbind())
    }

    #[pyo3(signature = (*, cursor=0, r#match=None, count=None, r#type=None))]
    fn ascan(
        &self,
        py: Python<'_>,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
        r#type: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_scan(cursor, r#match.as_deref(), count, r#type.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => {
                    let (next_cursor, keys) = parse_scan_reply(v);
                    RawResult::Value(redis::Value::Array(vec![
                        redis::Value::Int(next_cursor as i64),
                        redis::Value::Array(
                            keys.into_iter().map(redis::Value::BulkString).collect(),
                        ),
                    ]))
                }
                Err(e) => classify(e),
            }
        })
    }

    fn keys(&self, py: Python<'_>, pattern: &str) -> PyResult<Py<PyAny>> {
        warn_keys_use(py)?;
        let cmd = cmd_keys(pattern);
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::BytesList(r.map_err(to_py_err)?).into_py(py)
    }

    fn akeys(&self, py: Python<'_>, pattern: &str) -> PyResult<Py<PyAny>> {
        let pattern = pattern.to_string();
        warn_keys_use(py)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_keys(&pattern);
            let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn randomkey(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_randomkey();
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::OptBytes(r.map_err(to_py_err)?).into_py(py)
    }

    fn arandomkey(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_randomkey();
            let r: redis::RedisResult<Option<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // --- DBSIZE / FLUSHDB / FLUSHALL / SELECT ---

    fn dbsize(&self, py: Python<'_>) -> PyResult<i64> {
        let cmd = cmd_dbsize();
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    fn adbsize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_dbsize();
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, asynchronous=false))]
    fn flushdb(&self, py: Python<'_>, asynchronous: bool) -> PyResult<()> {
        let cmd = cmd_flushdb(asynchronous);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, asynchronous=false))]
    fn aflushdb(&self, py: Python<'_>, asynchronous: bool) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_flushdb(asynchronous);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, asynchronous=false))]
    fn flushall(&self, py: Python<'_>, asynchronous: bool) -> PyResult<()> {
        let cmd = cmd_flushall(asynchronous);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, asynchronous=false))]
    fn aflushall(&self, py: Python<'_>, asynchronous: bool) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_flushall(asynchronous);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    /// Per-Redis-instance database — set at connect time via the URL's
    /// `/<db>` segment. We accept SELECT for compatibility but only
    /// succeed when `db_index` matches the connected db; raising
    /// otherwise is preferable to silently drifting state under a
    /// multiplexed pool.
    fn select(&self, db_index: u8) -> PyResult<bool> {
        let connected = url_db_index(&self.url);
        if db_index == connected {
            Ok(true)
        } else {
            Err(pyo3::exceptions::PyNotImplementedError::new_err(format!(
                "SELECT to a different db is not supported under the multiplexed \
                 connection model (connected to db {connected}, requested {db_index}). \
                 Construct a new RedisRsDriver with the desired db in the URL instead."
            )))
        }
    }

    // --- INFO ---

    #[pyo3(signature = (*, section=None))]
    fn info(&self, py: Python<'_>, section: Option<String>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_info(section.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let bytes = value_to_bytes(r.map_err(to_py_err)?);
        Ok(PyBytes::new(py, &bytes).into_any().unbind())
    }

    #[pyo3(signature = (*, section=None))]
    fn ainfo(&self, py: Python<'_>, section: Option<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_info(section.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::OptBytes(Some(value_to_bytes(v))),
                Err(e) => classify(e),
            }
        })
    }

    // --- CONFIG GET / SET / RESETSTAT / REWRITE ---

    fn config_get(&self, py: Python<'_>, parameter: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_config_get(parameter);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::BytesPairs(parse_config_get_reply(r.map_err(to_py_err)?)).into_py(py)
    }

    fn aconfig_get(&self, py: Python<'_>, parameter: &str) -> PyResult<Py<PyAny>> {
        let parameter = parameter.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_config_get(&parameter);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::BytesPairs(parse_config_get_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    /// CONFIG SET — accepts either `(name, value)` positional args, or a
    /// single `mapping={name: value, ...}` kwarg. Mirrors redis-py.
    #[pyo3(signature = (name_or_mapping, value=None))]
    fn config_set(
        &self,
        py: Python<'_>,
        name_or_mapping: Bound<'_, PyAny>,
        value: Option<String>,
    ) -> PyResult<()> {
        let pairs = config_set_extract_pairs(&name_or_mapping, value)?;
        let cmd = cmd_config_set(&pairs);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (name_or_mapping, value=None))]
    fn aconfig_set(
        &self,
        py: Python<'_>,
        name_or_mapping: Bound<'_, PyAny>,
        value: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let pairs = config_set_extract_pairs(&name_or_mapping, value)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_config_set(&pairs);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn config_resetstat(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_config_resetstat();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    fn aconfig_resetstat(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_config_resetstat();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn config_rewrite(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_config_rewrite();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    fn aconfig_rewrite(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_config_rewrite();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    // --- CLIENT ID / GETNAME / SETNAME / INFO / LIST ---

    fn client_id(&self, py: Python<'_>) -> PyResult<i64> {
        let cmd = cmd_client_id();
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    fn aclient_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_id();
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn client_getname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_client_getname();
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::OptBytes(r.map_err(to_py_err)?).into_py(py)
    }

    fn aclient_getname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_getname();
            let r: redis::RedisResult<Option<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn client_setname(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let cmd = cmd_client_setname(name);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    fn aclient_setname(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let name = name.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_client_setname(&name);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn client_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_client_info();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let bytes = value_to_bytes(r.map_err(to_py_err)?);
        Ok(PyBytes::new(py, &bytes).into_any().unbind())
    }

    fn aclient_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_info();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::OptBytes(Some(value_to_bytes(v))),
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, client_type=None, client_id=None))]
    fn client_list(
        &self,
        py: Python<'_>,
        client_type: Option<String>,
        client_id: Option<Vec<i64>>,
    ) -> PyResult<Py<PyAny>> {
        let ids = client_id.unwrap_or_default();
        let cmd = cmd_client_list(client_type.as_deref(), &ids);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        let text = value_to_bytes(r.map_err(to_py_err)?);
        let rows = parse_client_list_reply(&text);
        RawResult::BytesPairsList(rows).into_py(py)
    }

    #[pyo3(signature = (*, client_type=None, client_id=None))]
    fn aclient_list(
        &self,
        py: Python<'_>,
        client_type: Option<String>,
        client_id: Option<Vec<i64>>,
    ) -> PyResult<Py<PyAny>> {
        let ids = client_id.unwrap_or_default();
        async_op!(self, py, conn, async {
            let cmd = cmd_client_list(client_type.as_deref(), &ids);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::BytesPairsList(parse_client_list_reply(&value_to_bytes(v))),
                Err(e) => classify(e),
            }
        })
    }

    // --- CLIENT KILL / PAUSE / UNPAUSE / NO-EVICT / NO-TOUCH ---

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        addr=None,
        laddr=None,
        client_id=None,
        client_type=None,
        user=None,
        skipme=None,
        maxage=None,
    ))]
    fn client_kill(
        &self,
        py: Python<'_>,
        addr: Option<String>,
        laddr: Option<String>,
        client_id: Option<i64>,
        client_type: Option<String>,
        user: Option<String>,
        skipme: Option<bool>,
        maxage: Option<i64>,
    ) -> PyResult<i64> {
        let cmd = cmd_client_kill(
            addr.as_deref(),
            laddr.as_deref(),
            client_id,
            client_type.as_deref(),
            user.as_deref(),
            skipme,
            maxage,
        );
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        addr=None,
        laddr=None,
        client_id=None,
        client_type=None,
        user=None,
        skipme=None,
        maxage=None,
    ))]
    fn aclient_kill(
        &self,
        py: Python<'_>,
        addr: Option<String>,
        laddr: Option<String>,
        client_id: Option<i64>,
        client_type: Option<String>,
        user: Option<String>,
        skipme: Option<bool>,
        maxage: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_kill(
                addr.as_deref(),
                laddr.as_deref(),
                client_id,
                client_type.as_deref(),
                user.as_deref(),
                skipme,
                maxage,
            );
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (timeout_ms, *, all=true))]
    fn client_pause(&self, py: Python<'_>, timeout_ms: i64, all: bool) -> PyResult<()> {
        let cmd = cmd_client_pause(timeout_ms, all);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (timeout_ms, *, all=true))]
    fn aclient_pause(&self, py: Python<'_>, timeout_ms: i64, all: bool) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_pause(timeout_ms, all);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn client_unpause(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_client_unpause();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    fn aclient_unpause(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_client_unpause();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, mode))]
    fn client_no_evict(&self, py: Python<'_>, mode: String) -> PyResult<()> {
        validate_on_off_mode(&mode)?;
        let cmd = cmd_client_no_evict(&mode.to_ascii_uppercase());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, mode))]
    fn aclient_no_evict(&self, py: Python<'_>, mode: String) -> PyResult<Py<PyAny>> {
        validate_on_off_mode(&mode)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_client_no_evict(&mode.to_ascii_uppercase());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, mode))]
    fn client_no_touch(&self, py: Python<'_>, mode: String) -> PyResult<()> {
        validate_on_off_mode(&mode)?;
        let cmd = cmd_client_no_touch(&mode.to_ascii_uppercase());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, mode))]
    fn aclient_no_touch(&self, py: Python<'_>, mode: String) -> PyResult<Py<PyAny>> {
        validate_on_off_mode(&mode)?;
        async_op!(self, py, conn, async {
            let cmd = cmd_client_no_touch(&mode.to_ascii_uppercase());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    // --- OBJECT ENCODING / IDLETIME / FREQ / REFCOUNT / HELP ---

    fn object_encoding(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_subcmd("ENCODING", key);
        let r: redis::RedisResult<Option<Vec<u8>>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::OptBytes(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_encoding(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_object_subcmd("ENCODING", &key);
            let r: redis::RedisResult<Option<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn object_idletime(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_subcmd("IDLETIME", key);
        let r: redis::RedisResult<Option<i64>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::OptInt(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_idletime(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_object_subcmd("IDLETIME", &key);
            let r: redis::RedisResult<Option<i64>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn object_freq(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_subcmd("FREQ", key);
        let r: redis::RedisResult<Option<i64>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::OptInt(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_freq(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_object_subcmd("FREQ", &key);
            let r: redis::RedisResult<Option<i64>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn object_refcount(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_subcmd("REFCOUNT", key);
        let r: redis::RedisResult<Option<i64>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::OptInt(r.map_err(to_py_err)?).into_py(py)
    }

    fn aobject_refcount(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_object_subcmd("REFCOUNT", &key);
            let r: redis::RedisResult<Option<i64>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn object_help(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_object_help();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::BytesList(parse_help_reply(r.map_err(to_py_err)?)).into_py(py)
    }

    fn aobject_help(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_object_help();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::BytesList(parse_help_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    // --- MEMORY USAGE ---

    #[pyo3(signature = (key, *, samples=None))]
    fn memory_usage(&self, py: Python<'_>, key: &str, samples: Option<i64>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_memory_usage(key, samples);
        let r: redis::RedisResult<Option<i64>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::OptInt(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (key, *, samples=None))]
    fn amemory_usage(
        &self,
        py: Python<'_>,
        key: &str,
        samples: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_memory_usage(&key, samples);
            let r: redis::RedisResult<Option<i64>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // --- ECHO ---

    #[pyo3(signature = (message))]
    fn echo(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let bytes: Vec<u8> = if let Ok(s) = message.extract::<String>() {
            s.into_bytes()
        } else {
            message.extract::<Vec<u8>>()?
        };
        let cmd = cmd_echo(&bytes);
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    #[pyo3(signature = (message))]
    fn aecho(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let bytes: Vec<u8> = if let Ok(s) = message.extract::<String>() {
            s.into_bytes()
        } else {
            message.extract::<Vec<u8>>()?
        };
        async_op!(self, py, conn, async {
            let cmd = cmd_echo(&bytes);
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // --- WAIT / WAITAOF ---

    #[pyo3(signature = (*, numreplicas, timeout))]
    fn wait(&self, py: Python<'_>, numreplicas: i64, timeout: i64) -> PyResult<i64> {
        let cmd = cmd_wait(numreplicas, timeout);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    /// Async name `await_` to avoid keyword collision with Python's `await`.
    #[pyo3(name = "await_", signature = (*, numreplicas, timeout))]
    fn r_await(&self, py: Python<'_>, numreplicas: i64, timeout: i64) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_wait(numreplicas, timeout);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, numlocal, numreplicas, timeout))]
    fn waitaof(
        &self,
        py: Python<'_>,
        numlocal: i64,
        numreplicas: i64,
        timeout: i64,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_waitaof(numlocal, numreplicas, timeout);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::Value(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (*, numlocal, numreplicas, timeout))]
    fn awaitaof(
        &self,
        py: Python<'_>,
        numlocal: i64,
        numreplicas: i64,
        timeout: i64,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_waitaof(numlocal, numreplicas, timeout);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // --- TIME ---

    fn time(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_time();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::OptStrPair(parse_time_reply(r.map_err(to_py_err)?)).into_py(py)
    }

    fn atime(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_time();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::OptStrPair(parse_time_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    // --- LASTSAVE / BGSAVE / BGREWRITEAOF ---

    fn lastsave(&self, py: Python<'_>) -> PyResult<i64> {
        let cmd = cmd_lastsave();
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    fn alastsave(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_lastsave();
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, schedule=false))]
    fn bgsave(&self, py: Python<'_>, schedule: bool) -> PyResult<Py<PyAny>> {
        let cmd = cmd_bgsave(schedule);
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    #[pyo3(signature = (*, schedule=false))]
    fn abgsave(&self, py: Python<'_>, schedule: bool) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_bgsave(schedule);
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn bgrewriteaof(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_bgrewriteaof();
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        Ok(PyBytes::new(py, &r.map_err(to_py_err)?).into_any().unbind())
    }

    fn abgrewriteaof(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_bgrewriteaof();
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // --- DEBUG SLEEP (test-only) ---

    /// Test-only — DO NOT call this from production code. Blocks the
    /// server (and our connection) for `seconds`. Used in blocking-cmd
    /// tests to simulate a slow-server scenario.
    fn debug_sleep(&self, py: Python<'_>, seconds: f64) -> PyResult<()> {
        let cmd = cmd_debug_sleep(seconds);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    fn adebug_sleep(&self, py: Python<'_>, seconds: f64) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_debug_sleep(seconds);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
}
