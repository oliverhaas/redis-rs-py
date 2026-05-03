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
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

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
    Value(redis::Value),
    Error(String),
    ServerError(String),
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
                let py_key = PyString::new(py, &key).into_any().unbind();
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
            RawResult::Value(v) => redis_value_to_py(py, v),
            RawResult::Error(e) => Err(pyo3::exceptions::PyConnectionError::new_err(e)),
            RawResult::ServerError(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}
