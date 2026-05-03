// Sorted-set commands on RedisRsDriver.
//
// Every method exists as a sync + async pair:
//   * `<cmd>(...)` — sync; releases the GIL via py.detach.
//   * `a<cmd>(...)` — async; returns a RedisRsAwaitable.
//
// WITHSCORES return shape: list[tuple[bytes, float]] everywhere.
// Float-returning commands (ZINCRBY, ZSCORE, ZADD+INCR) return f64 directly —
// no String round-trip.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyString, PyTuple};
use redis::AsyncCommands;

use crate::async_bridge::RawResult;
use crate::driver::{RedisRsDriver, py_bytes_list, py_int};
use crate::errors::to_py_err;
use crate::exceptions::{DataError, ExceptionClass};
use crate::raw_result::IntoRawResult;
use crate::{async_op, conn_method, dispatch_cmd, sync_op};

// =========================================================================
// Private type aliases to keep clippy::type_complexity quiet.
// =========================================================================

/// `(member, score)` pair returned by scored-range commands.
type ScoredPair = (Vec<u8>, f64);
/// Return of ZMPOP / BZMPOP: `(key, [(member, score), ...]) | nil`.
type ZMPopResult = Option<(String, Vec<ScoredPair>)>;
/// Return of BZPOPMIN / BZPOPMAX: `(key, member, score) | nil`.
type BZPopResult = Option<(Vec<u8>, Vec<u8>, f64)>;

// =========================================================================
// ZADD flag-matrix helpers
// =========================================================================

#[derive(Clone, Copy)]
struct ZAddFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
    incr: bool,
}

fn validate_zadd_flags(f: ZAddFlags, pair_count: usize) -> PyResult<()> {
    if f.nx && f.xx {
        return Err(PyErr::new::<DataError, _>(
            "ZADD: NX and XX options are mutually exclusive",
        ));
    }
    if f.gt && f.lt {
        return Err(PyErr::new::<DataError, _>(
            "ZADD: GT and LT options are mutually exclusive",
        ));
    }
    if f.nx && (f.gt || f.lt) {
        return Err(PyErr::new::<DataError, _>(
            "ZADD: NX cannot be combined with GT or LT",
        ));
    }
    if f.incr && pair_count != 1 {
        return Err(PyErr::new::<DataError, _>(
            "ZADD: INCR option supports a single member-score pair only",
        ));
    }
    Ok(())
}

fn collect_zadd_pairs(mapping: &Bound<'_, PyDict>) -> PyResult<Vec<(Vec<u8>, f64)>> {
    if mapping.is_empty() {
        return Err(PyErr::new::<DataError, _>(
            "ZADD: mapping is empty; provide at least one (member, score) pair",
        ));
    }
    let mut out = Vec::with_capacity(mapping.len());
    for (k, v) in mapping.iter() {
        // Members may be bytes or str.
        let member: Vec<u8> = if let Ok(b) = k.extract::<Vec<u8>>() {
            b
        } else {
            let s: String = k.extract()?;
            s.into_bytes()
        };
        let score: f64 = v.extract()?;
        out.push((member, score));
    }
    Ok(out)
}

fn build_zadd_cmd(key: &str, pairs: &[(Vec<u8>, f64)], f: ZAddFlags) -> redis::Cmd {
    let mut cmd = redis::cmd("ZADD");
    cmd.arg(key);
    if f.nx {
        cmd.arg("NX");
    }
    if f.xx {
        cmd.arg("XX");
    }
    if f.gt {
        cmd.arg("GT");
    }
    if f.lt {
        cmd.arg("LT");
    }
    if f.ch {
        cmd.arg("CH");
    }
    if f.incr {
        cmd.arg("INCR");
    }
    for (m, s) in pairs {
        cmd.arg(*s).arg(m.as_slice());
    }
    cmd
}

// =========================================================================
// ZRANGE / ZRANGESTORE helpers
// =========================================================================

#[allow(clippy::too_many_arguments)]
fn build_zrange_cmd(
    name: &'static str,
    leading_args: &[&str],
    start: &str,
    stop: &str,
    byscore: bool,
    bylex: bool,
    desc: bool,
    offset: Option<i64>,
    num: Option<i64>,
    withscores: bool,
) -> PyResult<redis::Cmd> {
    if byscore && bylex {
        return Err(PyErr::new::<DataError, _>(
            "ZRANGE: BYSCORE and BYLEX are mutually exclusive",
        ));
    }
    if (offset.is_some() || num.is_some()) && !(byscore || bylex) {
        return Err(PyErr::new::<DataError, _>(
            "ZRANGE: LIMIT (offset/num) requires BYSCORE or BYLEX",
        ));
    }
    if withscores && bylex {
        return Err(PyErr::new::<DataError, _>(
            "ZRANGE: WITHSCORES is not allowed with BYLEX",
        ));
    }
    let mut cmd = redis::cmd(name);
    for arg in leading_args {
        cmd.arg(*arg);
    }
    cmd.arg(start).arg(stop);
    if byscore {
        cmd.arg("BYSCORE");
    }
    if bylex {
        cmd.arg("BYLEX");
    }
    if desc {
        cmd.arg("REV");
    }
    if let (Some(o), Some(n)) = (offset, num) {
        cmd.arg("LIMIT").arg(o).arg(n);
    } else if offset.is_some() || num.is_some() {
        return Err(PyErr::new::<DataError, _>(
            "ZRANGE: both `offset` and `num` are required for LIMIT",
        ));
    }
    if withscores {
        cmd.arg("WITHSCORES");
    }
    Ok(cmd)
}

fn pyany_to_zrange_arg(v: &Bound<'_, PyAny>) -> PyResult<String> {
    // ZRANGE accepts ints (rank), floats / "(N" / "-inf" / "+inf" (score),
    // or "[member" / "(member" / "-" / "+" (lex). Coerce to string.
    if let Ok(i) = v.extract::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(s);
    }
    if let Ok(b) = v.extract::<Vec<u8>>() {
        return Ok(String::from_utf8_lossy(&b).into_owned());
    }
    Err(PyErr::new::<DataError, _>(
        "ZRANGE start/stop must be int, str, or bytes",
    ))
}

// =========================================================================
// Simple range command helper (ZRANGEBYSCORE etc.)
// =========================================================================

fn build_simple_range_cmd(
    name: &'static str,
    key: &str,
    a: &str,
    b: &str,
    withscores: bool,
    offset: Option<i64>,
    num: Option<i64>,
) -> PyResult<redis::Cmd> {
    let mut cmd = redis::cmd(name);
    cmd.arg(key).arg(a).arg(b);
    if withscores {
        cmd.arg("WITHSCORES");
    }
    if let (Some(o), Some(n)) = (offset, num) {
        cmd.arg("LIMIT").arg(o).arg(n);
    } else if offset.is_some() || num.is_some() {
        return Err(PyErr::new::<DataError, _>(
            "LIMIT requires both offset and num",
        ));
    }
    Ok(cmd)
}

// =========================================================================
// ZRANK / ZREVRANK helpers
// =========================================================================

fn rank_impl(
    py: Python<'_>,
    driver: &RedisRsDriver,
    name: &'static str,
    key: &str,
    member: &[u8],
    withscore: bool,
) -> PyResult<Py<PyAny>> {
    let r: Result<redis::Value, _> = sync_op!(py, driver, conn, async {
        let mut cmd = redis::cmd(name);
        cmd.arg(key).arg(member);
        if withscore {
            cmd.arg("WITHSCORE");
        }
        dispatch_cmd!(&mut *conn, cmd)
    });
    let value = r.map_err(to_py_err)?;
    parse_rank_reply(py, value, withscore)
}

fn arank_impl(
    py: Python<'_>,
    driver: &RedisRsDriver,
    name: &'static str,
    key: &str,
    member: &[u8],
    withscore: bool,
) -> PyResult<Py<PyAny>> {
    let key = key.to_string();
    let member = member.to_vec();
    async_op!(driver, py, conn, async {
        let mut cmd = redis::cmd(name);
        cmd.arg(&key).arg(&member);
        if withscore {
            cmd.arg("WITHSCORE");
        }
        let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
        match r {
            Ok(v) => parse_rank_reply_to_rawresult(v, withscore),
            Err(e) => crate::errors::classify(e),
        }
    })
}

fn parse_rank_reply(py: Python<'_>, value: redis::Value, withscore: bool) -> PyResult<Py<PyAny>> {
    if !withscore {
        return Ok(match value {
            redis::Value::Nil => py.None(),
            redis::Value::Int(n) => n.into_pyobject(py)?.into_any().unbind(),
            _ => py.None(),
        });
    }
    // WITHSCORE → [rank, score] or nil
    match value {
        redis::Value::Nil => Ok(py.None()),
        redis::Value::Array(items) if items.len() == 2 => {
            let mut iter = items.into_iter();
            let rank = match iter.next().unwrap() {
                redis::Value::Int(n) => n,
                _ => 0,
            };
            let score = match iter.next().unwrap() {
                redis::Value::Double(f) => f,
                redis::Value::BulkString(b) => std::str::from_utf8(&b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                _ => 0.0,
            };
            let r_py = rank.into_pyobject(py)?.into_any().unbind();
            let s_py = score.into_pyobject(py)?.into_any().unbind();
            Ok(PyTuple::new(py, [r_py, s_py])?.into_any().unbind())
        }
        _ => Ok(py.None()),
    }
}

fn parse_rank_reply_to_rawresult(value: redis::Value, withscore: bool) -> RawResult {
    if !withscore {
        return match value {
            redis::Value::Nil => RawResult::OptInt(None),
            redis::Value::Int(n) => RawResult::OptInt(Some(n)),
            _ => RawResult::OptInt(None),
        };
    }
    match value {
        redis::Value::Nil => RawResult::OptRankAndScore(None),
        redis::Value::Array(items) if items.len() == 2 => {
            let mut iter = items.into_iter();
            let rank = match iter.next().unwrap() {
                redis::Value::Int(n) => n,
                _ => 0,
            };
            let score = match iter.next().unwrap() {
                redis::Value::Double(f) => f,
                redis::Value::BulkString(b) => std::str::from_utf8(&b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                _ => 0.0,
            };
            RawResult::OptRankAndScore(Some((rank, score)))
        }
        _ => RawResult::OptRankAndScore(None),
    }
}

// =========================================================================
// ZMPOP / BZMPOP helpers
// =========================================================================

fn validate_zmpop_direction(d: &str) -> PyResult<&'static str> {
    match d.to_ascii_uppercase().as_str() {
        "MIN" => Ok("MIN"),
        "MAX" => Ok("MAX"),
        _ => Err(PyErr::new::<DataError, _>(
            "ZMPOP/BZMPOP: direction must be MIN or MAX",
        )),
    }
}

fn parse_zmpop_value(value: redis::Value) -> ZMPopResult {
    let items = match value {
        redis::Value::Array(items) if items.len() == 2 => items,
        _ => return None,
    };
    let mut iter = items.into_iter();
    let key = match iter.next().unwrap() {
        redis::Value::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        redis::Value::SimpleString(s) => s,
        _ => return None,
    };
    let pairs_v = iter.next().unwrap();
    let pairs = match pairs_v {
        redis::Value::Array(a) => a,
        _ => return None,
    };
    let mut out: Vec<(Vec<u8>, f64)> = Vec::with_capacity(pairs.len());
    for entry in pairs {
        if let redis::Value::Array(inner) = entry
            && inner.len() == 2
        {
            let mut it = inner.into_iter();
            let m = match it.next().unwrap() {
                redis::Value::BulkString(b) => b,
                _ => continue,
            };
            let s = match it.next().unwrap() {
                redis::Value::Double(f) => f,
                redis::Value::BulkString(b) => std::str::from_utf8(&b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                _ => 0.0,
            };
            out.push((m, s));
        }
    }
    Some((key, out))
}

fn render_zmpop_reply(py: Python<'_>, value: redis::Value) -> PyResult<Py<PyAny>> {
    match parse_zmpop_value(value) {
        None => Ok(py.None()),
        Some((key, items)) => {
            let key_py = PyString::new(py, &key).into_any().unbind();
            let pairs_py: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|(m, s)| {
                    let m_py = PyBytes::new(py, &m).into_any().unbind();
                    let s_py = s.into_pyobject(py)?.into_any().unbind();
                    Ok(PyTuple::new(py, [m_py, s_py])?.into_any().unbind())
                })
                .collect::<PyResult<_>>()?;
            let list_py = PyList::new(py, pairs_py)?.into_any().unbind();
            Ok(PyTuple::new(py, [key_py, list_py])?.into_any().unbind())
        }
    }
}

fn parse_zmpop_to_rawresult(value: redis::Value) -> RawResult {
    RawResult::OptKeyAndScoredMembers(parse_zmpop_value(value))
}

fn render_bzpop_reply(
    py: Python<'_>,
    value: Option<(Vec<u8>, Vec<u8>, f64)>,
) -> PyResult<Py<PyAny>> {
    match value {
        None => Ok(py.None()),
        Some((k, m, s)) => {
            let k_py = PyBytes::new(py, &k).into_any().unbind();
            let m_py = PyBytes::new(py, &m).into_any().unbind();
            let s_py = s.into_pyobject(py)?.into_any().unbind();
            Ok(PyTuple::new(py, [k_py, m_py, s_py])?.into_any().unbind())
        }
    }
}

// =========================================================================
// ZRANDMEMBER renderer
// =========================================================================

pub(crate) fn render_zrandmember(
    py: Python<'_>,
    value: redis::Value,
    count: Option<i64>,
    withscores: bool,
) -> PyResult<Py<PyAny>> {
    match (count, value) {
        (None, redis::Value::Nil) => Ok(py.None()),
        (None, redis::Value::BulkString(b)) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        (Some(_), redis::Value::Array(items)) if !withscores => {
            let py_items: Vec<Py<PyAny>> = items
                .into_iter()
                .map(|item| match item {
                    redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                    _ => py.None(),
                })
                .collect();
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        (Some(_), redis::Value::Array(items)) if withscores => {
            // RESP3 returns nested [[m, s], [m, s], ...]; RESP2 returns flat [m, s, m, s, ...]
            let nested = items
                .first()
                .map(|f| matches!(f, redis::Value::Array(_)))
                .unwrap_or(false);
            let mut pairs: Vec<Py<PyAny>> = Vec::new();
            if nested {
                for item in items {
                    if let redis::Value::Array(inner) = item
                        && inner.len() == 2
                    {
                        let m = match &inner[0] {
                            redis::Value::BulkString(b) => PyBytes::new(py, b).into_any().unbind(),
                            _ => py.None(),
                        };
                        let s = parse_score(&inner[1]);
                        let s_py = s.into_pyobject(py)?.into_any().unbind();
                        pairs.push(PyTuple::new(py, [m, s_py])?.into_any().unbind());
                    }
                }
            } else {
                let mut iter = items.into_iter();
                while let (Some(m_v), Some(s_v)) = (iter.next(), iter.next()) {
                    let m = match m_v {
                        redis::Value::BulkString(b) => PyBytes::new(py, &b).into_any().unbind(),
                        _ => py.None(),
                    };
                    let s = parse_score(&s_v);
                    let s_py = s.into_pyobject(py)?.into_any().unbind();
                    pairs.push(PyTuple::new(py, [m, s_py])?.into_any().unbind());
                }
            }
            Ok(PyList::new(py, pairs)?.into_any().unbind())
        }
        (_, redis::Value::Nil) => Ok(py.None()),
        _ => Ok(PyList::empty(py).into_any().unbind()),
    }
}

fn parse_score(v: &redis::Value) -> f64 {
    match v {
        redis::Value::Double(f) => *f,
        redis::Value::BulkString(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        redis::Value::Int(n) => *n as f64,
        _ => 0.0,
    }
}

// =========================================================================
// ZSCAN helpers
// =========================================================================

fn parse_zscan_reply(value: redis::Value) -> PyResult<(u64, Vec<ScoredPair>)> {
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
        let mut out: Vec<(Vec<u8>, f64)> = Vec::new();
        if let redis::Value::Array(items) = payload {
            let mut it = items.into_iter();
            while let (Some(m_v), Some(s_v)) = (it.next(), it.next()) {
                let m = match m_v {
                    redis::Value::BulkString(b) => b,
                    _ => continue,
                };
                let s = parse_score(&s_v);
                out.push((m, s));
            }
        }
        return Ok((cursor, out));
    }
    Err(PyErr::new::<DataError, _>(
        "ZSCAN reply did not match the [cursor, items] shape",
    ))
}

// =========================================================================
// ZUNION/ZINTER/ZDIFF helpers
// =========================================================================

fn validate_zset_op_args(
    keys: &[String],
    weights: &Option<Vec<f64>>,
    aggregate: &Option<String>,
) -> PyResult<Option<&'static str>> {
    if keys.is_empty() {
        return Err(PyErr::new::<DataError, _>(
            "keys= must contain at least one key",
        ));
    }
    if let Some(w) = weights
        && w.len() != keys.len()
    {
        return Err(PyErr::new::<DataError, _>(
            "weights= must have the same length as keys=",
        ));
    }
    let agg = match aggregate.as_deref().map(|s| s.to_ascii_uppercase()) {
        None => None,
        Some(ref s) if s == "SUM" => Some("SUM"),
        Some(ref s) if s == "MIN" => Some("MIN"),
        Some(ref s) if s == "MAX" => Some("MAX"),
        Some(_) => {
            return Err(PyErr::new::<DataError, _>(
                "AGGREGATE must be one of SUM, MIN, MAX",
            ));
        }
    };
    Ok(agg)
}

fn build_zset_op_cmd(
    name: &'static str,
    leading_args: &[&str],
    keys: &[String],
    weights: &Option<Vec<f64>>,
    aggregate: Option<&'static str>,
    withscores: bool,
) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    for arg in leading_args {
        cmd.arg(*arg);
    }
    cmd.arg(keys.len());
    for k in keys {
        cmd.arg(k);
    }
    if let Some(w) = weights {
        cmd.arg("WEIGHTS");
        for v in w {
            cmd.arg(*v);
        }
    }
    if let Some(a) = aggregate {
        cmd.arg("AGGREGATE").arg(a);
    }
    if withscores {
        cmd.arg("WITHSCORES");
    }
    cmd
}

// =========================================================================
// Shared render helper for WITHSCORES
// =========================================================================

pub(crate) fn render_scored_members(
    py: Python<'_>,
    items: Vec<(Vec<u8>, f64)>,
) -> PyResult<Py<PyAny>> {
    let py_items: Vec<Py<PyAny>> = items
        .into_iter()
        .map(|(m, s)| {
            let m_py = PyBytes::new(py, &m).into_any().unbind();
            let s_py = s.into_pyobject(py)?.into_any().unbind();
            Ok(PyTuple::new(py, [m_py, s_py])?.into_any().unbind())
        })
        .collect::<PyResult<_>>()?;
    Ok(PyList::new(py, py_items)?.into_any().unbind())
}

// =========================================================================
// #[pymethods] impl block — all sorted-set commands
// =========================================================================

#[pymethods]
impl RedisRsDriver {
    // =====================================================================
    // (a) ZADD with full NX/XX/GT/LT/CH/INCR flag matrix
    // =====================================================================

    #[pyo3(signature = (
        key,
        *,
        mapping,
        nx = false,
        xx = false,
        gt = false,
        lt = false,
        ch = false,
        incr = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn zadd(
        &self,
        py: Python<'_>,
        key: &str,
        mapping: &Bound<'_, PyDict>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
    ) -> PyResult<Py<PyAny>> {
        let flags = ZAddFlags {
            nx,
            xx,
            gt,
            lt,
            ch,
            incr,
        };
        let pairs = collect_zadd_pairs(mapping)?;
        validate_zadd_flags(flags, pairs.len())?;
        let cmd = build_zadd_cmd(key, &pairs, flags);
        if incr {
            let r: redis::RedisResult<Option<f64>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            match r.map_err(to_py_err)? {
                Some(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
                None => Ok(py.None()),
            }
        } else {
            let r: redis::RedisResult<i64> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            py_int(py, r.map_err(to_py_err)?)
        }
    }

    #[pyo3(signature = (
        key,
        *,
        mapping,
        nx = false,
        xx = false,
        gt = false,
        lt = false,
        ch = false,
        incr = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn azadd(
        &self,
        py: Python<'_>,
        key: &str,
        mapping: &Bound<'_, PyDict>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
    ) -> PyResult<Py<PyAny>> {
        let flags = ZAddFlags {
            nx,
            xx,
            gt,
            lt,
            ch,
            incr,
        };
        let pairs = collect_zadd_pairs(mapping)?;
        validate_zadd_flags(flags, pairs.len())?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_zadd_cmd(&key, &pairs, flags);
            if flags.incr {
                let r: redis::RedisResult<Option<f64>> = dispatch_cmd!(&mut *conn, cmd);
                match r {
                    Ok(v) => RawResult::OptScore(v),
                    Err(e) => crate::errors::classify(e),
                }
            } else {
                let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            }
        })
    }

    // =====================================================================
    // ZREM (variadic)
    // =====================================================================

    #[pyo3(signature = (key, *members))]
    fn zrem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return py_int(py, 0);
        }
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            conn_method!(&mut *conn, c, c.zrem(key, &members))
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *members))]
    fn azrem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn_method!(&mut *conn, c, c.zrem(&key, &members));
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (b) ZRANGE / ZRANGESTORE
    // =====================================================================

    #[pyo3(signature = (
        key,
        start,
        stop,
        *,
        desc = false,
        byscore = false,
        bylex = false,
        withscores = false,
        offset = None,
        num = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn zrange(
        &self,
        py: Python<'_>,
        key: &str,
        start: &Bound<'_, PyAny>,
        stop: &Bound<'_, PyAny>,
        desc: bool,
        byscore: bool,
        bylex: bool,
        withscores: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let start_s = pyany_to_zrange_arg(start)?;
        let stop_s = pyany_to_zrange_arg(stop)?;
        let cmd = build_zrange_cmd(
            "ZRANGE",
            &[key],
            &start_s,
            &stop_s,
            byscore,
            bylex,
            desc,
            offset,
            num,
            withscores,
        )?;
        if withscores {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

    #[pyo3(signature = (
        key,
        start,
        stop,
        *,
        desc = false,
        byscore = false,
        bylex = false,
        withscores = false,
        offset = None,
        num = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn azrange(
        &self,
        py: Python<'_>,
        key: &str,
        start: &Bound<'_, PyAny>,
        stop: &Bound<'_, PyAny>,
        desc: bool,
        byscore: bool,
        bylex: bool,
        withscores: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let start_s = pyany_to_zrange_arg(start)?;
        let stop_s = pyany_to_zrange_arg(stop)?;
        let cmd = build_zrange_cmd(
            "ZRANGE",
            &[key],
            &start_s,
            &stop_s,
            byscore,
            bylex,
            desc,
            offset,
            num,
            withscores,
        )?;
        async_op!(self, py, conn, async {
            if withscores {
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            }
        })
    }

    #[pyo3(signature = (
        destination,
        source,
        start,
        stop,
        *,
        desc = false,
        byscore = false,
        bylex = false,
        offset = None,
        num = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn zrangestore(
        &self,
        py: Python<'_>,
        destination: &str,
        source: &str,
        start: &Bound<'_, PyAny>,
        stop: &Bound<'_, PyAny>,
        desc: bool,
        byscore: bool,
        bylex: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let start_s = pyany_to_zrange_arg(start)?;
        let stop_s = pyany_to_zrange_arg(stop)?;
        let cmd = build_zrange_cmd(
            "ZRANGESTORE",
            &[destination, source],
            &start_s,
            &stop_s,
            byscore,
            bylex,
            desc,
            offset,
            num,
            false,
        )?;
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (
        destination,
        source,
        start,
        stop,
        *,
        desc = false,
        byscore = false,
        bylex = false,
        offset = None,
        num = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn azrangestore(
        &self,
        py: Python<'_>,
        destination: &str,
        source: &str,
        start: &Bound<'_, PyAny>,
        stop: &Bound<'_, PyAny>,
        desc: bool,
        byscore: bool,
        bylex: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let start_s = pyany_to_zrange_arg(start)?;
        let stop_s = pyany_to_zrange_arg(stop)?;
        let cmd = build_zrange_cmd(
            "ZRANGESTORE",
            &[destination, source],
            &start_s,
            &stop_s,
            byscore,
            bylex,
            desc,
            offset,
            num,
            false,
        )?;
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (c) ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGEBYLEX
    // =====================================================================

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (key, min, max, *, withscores=false, offset=None, num=None))]
    fn zrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        withscores: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = build_simple_range_cmd("ZRANGEBYSCORE", key, min, max, withscores, offset, num)?;
        if withscores {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (key, min, max, *, withscores=false, offset=None, num=None))]
    fn azrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        withscores: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = build_simple_range_cmd("ZRANGEBYSCORE", key, min, max, withscores, offset, num)?;
        async_op!(self, py, conn, async {
            if withscores {
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (key, max, min, *, withscores=false, offset=None, num=None))]
    fn zrevrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        max: &str,
        min: &str,
        withscores: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd =
            build_simple_range_cmd("ZREVRANGEBYSCORE", key, max, min, withscores, offset, num)?;
        if withscores {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (key, max, min, *, withscores=false, offset=None, num=None))]
    fn azrevrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        max: &str,
        min: &str,
        withscores: bool,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd =
            build_simple_range_cmd("ZREVRANGEBYSCORE", key, max, min, withscores, offset, num)?;
        async_op!(self, py, conn, async {
            if withscores {
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            }
        })
    }

    #[pyo3(signature = (key, min, max, *, offset=None, num=None))]
    fn zrangebylex(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = build_simple_range_cmd("ZRANGEBYLEX", key, min, max, false, offset, num)?;
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, min, max, *, offset=None, num=None))]
    fn azrangebylex(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = build_simple_range_cmd("ZRANGEBYLEX", key, min, max, false, offset, num)?;
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, max, min, *, offset=None, num=None))]
    fn zrevrangebylex(
        &self,
        py: Python<'_>,
        key: &str,
        max: &str,
        min: &str,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = build_simple_range_cmd("ZREVRANGEBYLEX", key, max, min, false, offset, num)?;
        let r: redis::RedisResult<Vec<Vec<u8>>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        py_bytes_list(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, max, min, *, offset=None, num=None))]
    fn azrevrangebylex(
        &self,
        py: Python<'_>,
        key: &str,
        max: &str,
        min: &str,
        offset: Option<i64>,
        num: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = build_simple_range_cmd("ZREVRANGEBYLEX", key, max, min, false, offset, num)?;
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (d) ZINCRBY / ZCARD / ZSCORE / ZMSCORE
    // =====================================================================

    #[pyo3(signature = (key, amount, member))]
    fn zincrby(&self, py: Python<'_>, key: &str, amount: f64, member: &[u8]) -> PyResult<f64> {
        sync_op!(py, self, conn, async {
            conn_method!(&mut *conn, c, c.zincr(key, member, amount))
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (key, amount, member))]
    fn azincrby(
        &self,
        py: Python<'_>,
        key: &str,
        amount: f64,
        member: &[u8],
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<f64> =
                conn_method!(&mut *conn, c, c.zincr(&key, &member, amount));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key))]
    fn zcard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            conn_method!(&mut *conn, c, c.zcard(key))
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn azcard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn_method!(&mut *conn, c, c.zcard(&key));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, member))]
    fn zscore(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Option<f64>> {
        sync_op!(py, self, conn, async {
            conn_method!(&mut *conn, c, c.zscore(key, member))
        })
        .map_err(to_py_err)
    }

    #[pyo3(signature = (key, member))]
    fn azscore(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Option<f64>> =
                conn_method!(&mut *conn, c, c.zscore(&key, &member));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn zmscore(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return Ok(PyList::empty(py).into_any().unbind());
        }
        let r: redis::RedisResult<Vec<Option<f64>>> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZMSCORE");
            cmd.arg(key);
            for m in &members {
                cmd.arg(m.as_slice());
            }
            dispatch_cmd!(&mut *conn, cmd)
        });
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|opt| match opt {
                Some(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
                None => Ok(py.None()),
            })
            .collect::<PyResult<_>>()?;
        Ok(PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, *members))]
    fn azmscore(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::Value(redis::Value::Array(Vec::new()));
            }
            let mut cmd = redis::cmd("ZMSCORE");
            cmd.arg(&key);
            for m in &members {
                cmd.arg(m.as_slice());
            }
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => RawResult::Value(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    // =====================================================================
    // (e) ZRANK / ZREVRANK with WITHSCORE (Redis 7.2+)
    // =====================================================================

    #[pyo3(signature = (key, member, *, withscore=false))]
    fn zrank(
        &self,
        py: Python<'_>,
        key: &str,
        member: &[u8],
        withscore: bool,
    ) -> PyResult<Py<PyAny>> {
        rank_impl(py, self, "ZRANK", key, member, withscore)
    }

    #[pyo3(signature = (key, member, *, withscore=false))]
    fn azrank(
        &self,
        py: Python<'_>,
        key: &str,
        member: &[u8],
        withscore: bool,
    ) -> PyResult<Py<PyAny>> {
        arank_impl(py, self, "ZRANK", key, member, withscore)
    }

    #[pyo3(signature = (key, member, *, withscore=false))]
    fn zrevrank(
        &self,
        py: Python<'_>,
        key: &str,
        member: &[u8],
        withscore: bool,
    ) -> PyResult<Py<PyAny>> {
        rank_impl(py, self, "ZREVRANK", key, member, withscore)
    }

    #[pyo3(signature = (key, member, *, withscore=false))]
    fn azrevrank(
        &self,
        py: Python<'_>,
        key: &str,
        member: &[u8],
        withscore: bool,
    ) -> PyResult<Py<PyAny>> {
        arank_impl(py, self, "ZREVRANK", key, member, withscore)
    }

    // =====================================================================
    // (f) ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREMRANGEBYLEX
    // =====================================================================

    #[pyo3(signature = (key, start, stop))]
    fn zremrangebyrank(
        &self,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            conn_method!(
                &mut *conn,
                c,
                c.zremrangebyrank(key, start as isize, stop as isize)
            )
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, start, stop))]
    fn azremrangebyrank(
        &self,
        py: Python<'_>,
        key: &str,
        start: i64,
        stop: i64,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn_method!(
                &mut *conn,
                c,
                c.zremrangebyrank(&key, start as isize, stop as isize)
            );
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, min, max))]
    fn zremrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZREMRANGEBYSCORE");
            cmd.arg(key).arg(min).arg(max);
            dispatch_cmd!(&mut *conn, cmd)
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, min, max))]
    fn azremrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let min = min.to_string();
        let max = max.to_string();
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("ZREMRANGEBYSCORE");
            cmd.arg(&key).arg(&min).arg(&max);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, min, max))]
    fn zremrangebylex(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
    ) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZREMRANGEBYLEX");
            cmd.arg(key).arg(min).arg(max);
            dispatch_cmd!(&mut *conn, cmd)
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, min, max))]
    fn azremrangebylex(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let min = min.to_string();
        let max = max.to_string();
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("ZREMRANGEBYLEX");
            cmd.arg(&key).arg(&min).arg(&max);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (g) ZCOUNT / ZLEXCOUNT
    // =====================================================================

    #[pyo3(signature = (key, min, max))]
    fn zcount(&self, py: Python<'_>, key: &str, min: &str, max: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZCOUNT");
            cmd.arg(key).arg(min).arg(max);
            dispatch_cmd!(&mut *conn, cmd)
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, min, max))]
    fn azcount(&self, py: Python<'_>, key: &str, min: &str, max: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let min = min.to_string();
        let max = max.to_string();
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("ZCOUNT");
            cmd.arg(&key).arg(&min).arg(&max);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, min, max))]
    fn zlexcount(&self, py: Python<'_>, key: &str, min: &str, max: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZLEXCOUNT");
            cmd.arg(key).arg(min).arg(max);
            dispatch_cmd!(&mut *conn, cmd)
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, min, max))]
    fn azlexcount(&self, py: Python<'_>, key: &str, min: &str, max: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let min = min.to_string();
        let max = max.to_string();
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("ZLEXCOUNT");
            cmd.arg(&key).arg(&min).arg(&max);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // =====================================================================
    // (h) ZPOPMIN / ZPOPMAX / ZMPOP / BZPOPMIN / BZPOPMAX / BZMPOP
    // =====================================================================

    #[pyo3(signature = (key, *, count=1))]
    fn zpopmin(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let count = count as isize;
        let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = sync_op!(py, self, conn, async {
            conn_method!(&mut *conn, c, c.zpopmin(key, count))
        });
        render_scored_members(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *, count=1))]
    fn azpopmin(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let count = count as isize;
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                conn_method!(&mut *conn, c, c.zpopmin(&key, count));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *, count=1))]
    fn zpopmax(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let count = count as isize;
        let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = sync_op!(py, self, conn, async {
            conn_method!(&mut *conn, c, c.zpopmax(key, count))
        });
        render_scored_members(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *, count=1))]
    fn azpopmax(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let count = count as isize;
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                conn_method!(&mut *conn, c, c.zpopmax(&key, count));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*keys, direction, count=1))]
    fn zmpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        let direction = validate_zmpop_direction(direction)?;
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZMPOP");
            cmd.arg(keys.len());
            for k in &keys {
                cmd.arg(k);
            }
            cmd.arg(direction).arg("COUNT").arg(count);
            dispatch_cmd!(&mut *conn, cmd)
        });
        let value = r.map_err(to_py_err)?;
        render_zmpop_reply(py, value)
    }

    #[pyo3(signature = (*keys, direction, count=1))]
    fn azmpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        direction: &str,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        let direction = validate_zmpop_direction(direction)?;
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("ZMPOP");
            cmd.arg(keys.len());
            for k in &keys {
                cmd.arg(k);
            }
            cmd.arg(direction).arg("COUNT").arg(count);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => parse_zmpop_to_rawresult(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (*keys, timeout))]
    fn bzpopmin(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let r: Result<BZPopResult, _> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async {
                let mut conn = self
                    .connection
                    .get_blocking()
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;
                let mut cmd = redis::cmd("BZPOPMIN");
                for k in &keys {
                    cmd.arg(k);
                }
                cmd.arg(timeout);
                let r: redis::RedisResult<BZPopResult> = dispatch_cmd!(&mut conn, cmd);
                r.map_err(to_py_err)
            })
        });
        render_bzpop_reply(py, r?)
    }

    #[pyo3(signature = (*keys, timeout))]
    fn abzpopmin(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let conn_handle = self.connection.clone();
        crate::runtime::get_runtime().spawn(async move {
            let raw = async {
                let mut conn = match conn_handle.get_blocking().await {
                    Ok(c) => c,
                    Err(e) => return crate::errors::classify(e),
                };
                let mut cmd = redis::cmd("BZPOPMIN");
                for k in &keys {
                    cmd.arg(k);
                }
                cmd.arg(timeout);
                let r: redis::RedisResult<BZPopResult> = dispatch_cmd!(&mut conn, cmd);
                match r {
                    Ok(v) => RawResult::OptKeyMemberScore(v),
                    Err(e) => crate::errors::classify(e),
                }
            }
            .await;
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    #[pyo3(signature = (*keys, timeout))]
    fn bzpopmax(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let r: Result<BZPopResult, _> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async {
                let mut conn = self
                    .connection
                    .get_blocking()
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;
                let mut cmd = redis::cmd("BZPOPMAX");
                for k in &keys {
                    cmd.arg(k);
                }
                cmd.arg(timeout);
                let r: redis::RedisResult<BZPopResult> = dispatch_cmd!(&mut conn, cmd);
                r.map_err(to_py_err)
            })
        });
        render_bzpop_reply(py, r?)
    }

    #[pyo3(signature = (*keys, timeout))]
    fn abzpopmax(&self, py: Python<'_>, keys: Vec<String>, timeout: f64) -> PyResult<Py<PyAny>> {
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let conn_handle = self.connection.clone();
        crate::runtime::get_runtime().spawn(async move {
            let raw = async {
                let mut conn = match conn_handle.get_blocking().await {
                    Ok(c) => c,
                    Err(e) => return crate::errors::classify(e),
                };
                let mut cmd = redis::cmd("BZPOPMAX");
                for k in &keys {
                    cmd.arg(k);
                }
                cmd.arg(timeout);
                let r: redis::RedisResult<BZPopResult> = dispatch_cmd!(&mut conn, cmd);
                match r {
                    Ok(v) => RawResult::OptKeyMemberScore(v),
                    Err(e) => crate::errors::classify(e),
                }
            }
            .await;
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    #[pyo3(signature = (*keys, direction, timeout, count=1))]
    fn bzmpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        direction: &str,
        timeout: f64,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        let direction = validate_zmpop_direction(direction)?;
        let r: Result<redis::Value, _> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async {
                let mut conn = self
                    .connection
                    .get_blocking()
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;
                let mut cmd = redis::cmd("BZMPOP");
                cmd.arg(timeout).arg(keys.len());
                for k in &keys {
                    cmd.arg(k);
                }
                cmd.arg(direction).arg("COUNT").arg(count);
                let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
                r.map_err(to_py_err)
            })
        });
        render_zmpop_reply(py, r?)
    }

    #[pyo3(signature = (*keys, direction, timeout, count=1))]
    fn abzmpop(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        direction: &str,
        timeout: f64,
        count: i64,
    ) -> PyResult<Py<PyAny>> {
        let direction = validate_zmpop_direction(direction)?;
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let conn_handle = self.connection.clone();
        crate::runtime::get_runtime().spawn(async move {
            let raw = async {
                let mut conn = match conn_handle.get_blocking().await {
                    Ok(c) => c,
                    Err(e) => return crate::errors::classify(e),
                };
                let mut cmd = redis::cmd("BZMPOP");
                cmd.arg(timeout).arg(keys.len());
                for k in &keys {
                    cmd.arg(k);
                }
                cmd.arg(direction).arg("COUNT").arg(count);
                let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
                match r {
                    Ok(v) => parse_zmpop_to_rawresult(v),
                    Err(e) => crate::errors::classify(e),
                }
            }
            .await;
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // =====================================================================
    // (i) ZRANDMEMBER
    // =====================================================================

    #[pyo3(signature = (key, count=None, withscores=false))]
    fn zrandmember(
        &self,
        py: Python<'_>,
        key: &str,
        count: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZRANDMEMBER");
            cmd.arg(key);
            if let Some(c) = count {
                cmd.arg(c);
                if withscores {
                    cmd.arg("WITHSCORES");
                }
            }
            dispatch_cmd!(&mut *conn, cmd)
        });
        render_zrandmember(py, r.map_err(to_py_err)?, count, withscores)
    }

    #[pyo3(signature = (key, count=None, withscores=false))]
    fn azrandmember(
        &self,
        py: Python<'_>,
        key: &str,
        count: Option<i64>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let awaitable = crate::async_bridge::RedisRsAwaitable::new(rx);
        let mut conn = self.connection.clone();
        crate::runtime::get_runtime().spawn(async move {
            let mut cmd = redis::cmd("ZRANDMEMBER");
            cmd.arg(&key);
            if let Some(c) = count {
                cmd.arg(c);
                if withscores {
                    cmd.arg("WITHSCORES");
                }
            }
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            let raw = match r {
                Ok(v) => RawResult::ZRandmember {
                    value: v,
                    count,
                    withscores,
                },
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    // =====================================================================
    // (j) ZSCAN
    // =====================================================================

    #[pyo3(signature = (key, *, cursor=0, r#match=None, count=None))]
    #[allow(non_snake_case)]
    fn zscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<redis::Value, _> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZSCAN");
            cmd.arg(key).arg(cursor);
            if let Some(ref p) = r#match {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            dispatch_cmd!(&mut *conn, cmd)
        });
        let value = r.map_err(to_py_err)?;
        let (cursor_out, items) = parse_zscan_reply(value)?;
        let cursor_py = cursor_out.into_pyobject(py)?.into_any().unbind();
        let pairs: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|(m, s)| {
                let m_py = PyBytes::new(py, &m).into_any().unbind();
                let s_py = s.into_pyobject(py)?.into_any().unbind();
                Ok(PyTuple::new(py, [m_py, s_py])?.into_any().unbind())
            })
            .collect::<PyResult<_>>()?;
        let list_py = PyList::new(py, pairs)?.into_any().unbind();
        Ok(PyTuple::new(py, [cursor_py, list_py])?.into_any().unbind())
    }

    #[pyo3(signature = (key, *, cursor=0, r#match=None, count=None))]
    #[allow(non_snake_case)]
    fn azscan(
        &self,
        py: Python<'_>,
        key: &str,
        cursor: u64,
        r#match: Option<String>,
        count: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let mut cmd = redis::cmd("ZSCAN");
            cmd.arg(&key).arg(cursor);
            if let Some(ref p) = r#match {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(v) => match parse_zscan_reply(v) {
                    Ok((cursor, items)) => RawResult::ZScan { cursor, items },
                    Err(_) => RawResult::Error(
                        ExceptionClass::ResponseError,
                        "ZSCAN reply did not match expected shape".to_string(),
                    ),
                },
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    // =====================================================================
    // (k) ZUNION / ZINTER / ZDIFF + STORE variants + ZINTERCARD
    // =====================================================================

    #[pyo3(signature = (
        *,
        keys,
        weights = None,
        aggregate = None,
        withscores = false,
    ))]
    fn zunion(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let agg = validate_zset_op_args(&keys, &weights, &aggregate)?;
        let cmd = build_zset_op_cmd("ZUNION", &[], &keys, &weights, agg, withscores);
        if withscores {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

    #[pyo3(signature = (
        *,
        keys,
        weights = None,
        aggregate = None,
        withscores = false,
    ))]
    fn azunion(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let agg = validate_zset_op_args(&keys, &weights, &aggregate)?;
        let cmd = build_zset_op_cmd("ZUNION", &[], &keys, &weights, agg, withscores);
        async_op!(self, py, conn, async {
            if withscores {
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            }
        })
    }

    #[pyo3(signature = (
        *,
        keys,
        weights = None,
        aggregate = None,
        withscores = false,
    ))]
    fn zinter(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let agg = validate_zset_op_args(&keys, &weights, &aggregate)?;
        let cmd = build_zset_op_cmd("ZINTER", &[], &keys, &weights, agg, withscores);
        if withscores {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

    #[pyo3(signature = (
        *,
        keys,
        weights = None,
        aggregate = None,
        withscores = false,
    ))]
    fn azinter(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
        let agg = validate_zset_op_args(&keys, &weights, &aggregate)?;
        let cmd = build_zset_op_cmd("ZINTER", &[], &keys, &weights, agg, withscores);
        async_op!(self, py, conn, async {
            if withscores {
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            }
        })
    }

    #[pyo3(signature = (*, keys, withscores = false))]
    fn zdiff(&self, py: Python<'_>, keys: Vec<String>, withscores: bool) -> PyResult<Py<PyAny>> {
        if keys.is_empty() {
            return Err(PyErr::new::<DataError, _>(
                "keys= must contain at least one key",
            ));
        }
        let mut cmd = redis::cmd("ZDIFF");
        cmd.arg(keys.len());
        for k in &keys {
            cmd.arg(k);
        }
        if withscores {
            cmd.arg("WITHSCORES");
        }
        if withscores {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

    #[pyo3(signature = (*, keys, withscores = false))]
    fn azdiff(&self, py: Python<'_>, keys: Vec<String>, withscores: bool) -> PyResult<Py<PyAny>> {
        if keys.is_empty() {
            return Err(PyErr::new::<DataError, _>(
                "keys= must contain at least one key",
            ));
        }
        let mut cmd = redis::cmd("ZDIFF");
        cmd.arg(keys.len());
        for k in &keys {
            cmd.arg(k);
        }
        if withscores {
            cmd.arg("WITHSCORES");
        }
        async_op!(self, py, conn, async {
            if withscores {
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut *conn, cmd);
                r.into_raw_result()
            }
        })
    }

    #[pyo3(signature = (
        destination,
        *,
        keys,
        weights = None,
        aggregate = None,
    ))]
    fn zunionstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let agg = validate_zset_op_args(&keys, &weights, &aggregate)?;
        let cmd = build_zset_op_cmd("ZUNIONSTORE", &[destination], &keys, &weights, agg, false);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (
        destination,
        *,
        keys,
        weights = None,
        aggregate = None,
    ))]
    fn azunionstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let agg = validate_zset_op_args(&keys, &weights, &aggregate)?;
        let cmd = build_zset_op_cmd("ZUNIONSTORE", &[destination], &keys, &weights, agg, false);
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (
        destination,
        *,
        keys,
        weights = None,
        aggregate = None,
    ))]
    fn zinterstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let agg = validate_zset_op_args(&keys, &weights, &aggregate)?;
        let cmd = build_zset_op_cmd("ZINTERSTORE", &[destination], &keys, &weights, agg, false);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (
        destination,
        *,
        keys,
        weights = None,
        aggregate = None,
    ))]
    fn azinterstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let agg = validate_zset_op_args(&keys, &weights, &aggregate)?;
        let cmd = build_zset_op_cmd("ZINTERSTORE", &[destination], &keys, &weights, agg, false);
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (destination, *, keys))]
    fn zdiffstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        if keys.is_empty() {
            return Err(PyErr::new::<DataError, _>(
                "keys= must contain at least one key",
            ));
        }
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZDIFFSTORE");
            cmd.arg(destination).arg(keys.len());
            for k in &keys {
                cmd.arg(k);
            }
            dispatch_cmd!(&mut *conn, cmd)
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (destination, *, keys))]
    fn azdiffstore(
        &self,
        py: Python<'_>,
        destination: &str,
        keys: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let destination = destination.to_string();
        async_op!(self, py, conn, async {
            if keys.is_empty() {
                return RawResult::Error(
                    ExceptionClass::DataError,
                    "keys= must contain at least one key".to_string(),
                );
            }
            let mut cmd = redis::cmd("ZDIFFSTORE");
            cmd.arg(&destination).arg(keys.len());
            for k in &keys {
                cmd.arg(k);
            }
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, keys, limit = None))]
    fn zintercard(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        if keys.is_empty() {
            return Err(PyErr::new::<DataError, _>(
                "keys= must contain at least one key",
            ));
        }
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZINTERCARD");
            cmd.arg(keys.len());
            for k in &keys {
                cmd.arg(k);
            }
            if let Some(lim) = limit {
                cmd.arg("LIMIT").arg(lim);
            }
            dispatch_cmd!(&mut *conn, cmd)
        });
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (*, keys, limit = None))]
    fn azintercard(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            if keys.is_empty() {
                return RawResult::Error(
                    ExceptionClass::DataError,
                    "keys= must contain at least one key".to_string(),
                );
            }
            let mut cmd = redis::cmd("ZINTERCARD");
            cmd.arg(keys.len());
            for k in &keys {
                cmd.arg(k);
            }
            if let Some(lim) = limit {
                cmd.arg("LIMIT").arg(lim);
            }
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }
}
