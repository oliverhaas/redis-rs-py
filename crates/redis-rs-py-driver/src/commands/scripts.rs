// Server-side scripting commands.
//
// EVAL/EVALSHA/EVAL_RO/EVALSHA_RO + SCRIPT LOAD/EXISTS/FLUSH/KILL +
// FCALL/FCALL_RO + FUNCTION LOAD/DUMP/FLUSH/LIST/STATS/KILL/RESTORE/DELETE.
//
// Sync variants live in `#[pymethods] impl Redis`.
// Async variants live in `#[pymethods] impl AsyncRedis` (a-prefix dropped).

use pyo3::prelude::*;

use crate::async_bridge::RawResult;
use crate::errors::{classify, to_py_err};
use crate::facade::asyncio_mod::AsyncRedis;
use crate::facade::sync::Redis;
use crate::raw_result::IntoRawResult;
use crate::{async_op, dispatch_cmd, sync_op};

// =========================================================================
// Argument-encoding helpers (cmd_*)
// =========================================================================

fn cmd_eval(name: &str, script_or_sha: &str, keys: &[String], args: &[Vec<u8>]) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    cmd.arg(script_or_sha).arg(keys.len());
    for k in keys {
        cmd.arg(k.as_str());
    }
    for a in args {
        cmd.arg(a.as_slice());
    }
    cmd
}

fn cmd_script_load(script: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("SCRIPT");
    cmd.arg("LOAD").arg(script);
    cmd
}

fn cmd_script_exists(shas: &[String]) -> redis::Cmd {
    let mut cmd = redis::cmd("SCRIPT");
    cmd.arg("EXISTS");
    for s in shas {
        cmd.arg(s.as_str());
    }
    cmd
}

fn cmd_script_flush(mode: Option<&str>) -> redis::Cmd {
    let mut cmd = redis::cmd("SCRIPT");
    cmd.arg("FLUSH");
    if let Some(m) = mode {
        cmd.arg(m);
    }
    cmd
}

fn cmd_script_kill() -> redis::Cmd {
    let mut cmd = redis::cmd("SCRIPT");
    cmd.arg("KILL");
    cmd
}

fn validate_flush_mode(mode: &str) -> PyResult<()> {
    match mode.to_ascii_uppercase().as_str() {
        "ASYNC" | "SYNC" => Ok(()),
        _ => Err(pyo3::PyErr::new::<crate::exceptions::DataError, _>(
            format!("flush mode must be ASYNC or SYNC, got {mode}"),
        )),
    }
}

fn cmd_fcall(name: &str, function: &str, keys: &[String], args: &[Vec<u8>]) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    cmd.arg(function).arg(keys.len());
    for k in keys {
        cmd.arg(k.as_str());
    }
    for a in args {
        cmd.arg(a.as_slice());
    }
    cmd
}

fn cmd_function_load(code: &str, replace: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("LOAD");
    if replace {
        cmd.arg("REPLACE");
    }
    cmd.arg(code);
    cmd
}

fn cmd_function_delete(library: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("DELETE").arg(library);
    cmd
}

fn cmd_function_dump() -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("DUMP");
    cmd
}

fn cmd_function_flush(mode: Option<&str>) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("FLUSH");
    if let Some(m) = mode {
        cmd.arg(m);
    }
    cmd
}

fn cmd_function_list(library: Option<&str>, withcode: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("LIST");
    if let Some(lib) = library {
        cmd.arg("LIBRARYNAME").arg(lib);
    }
    if withcode {
        cmd.arg("WITHCODE");
    }
    cmd
}

fn cmd_function_stats() -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("STATS");
    cmd
}

fn cmd_function_kill() -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("KILL");
    cmd
}

fn cmd_function_restore(dump: &[u8], policy: Option<&str>) -> redis::Cmd {
    let mut cmd = redis::cmd("FUNCTION");
    cmd.arg("RESTORE").arg(dump);
    if let Some(p) = policy {
        cmd.arg(p);
    }
    cmd
}

fn validate_restore_policy(policy: &str) -> PyResult<()> {
    match policy.to_ascii_uppercase().as_str() {
        "FLUSH" | "APPEND" | "REPLACE" => Ok(()),
        _ => Err(pyo3::PyErr::new::<crate::exceptions::DataError, _>(
            format!("restore policy must be FLUSH, APPEND, or REPLACE, got {policy}"),
        )),
    }
}

// =========================================================================
// Sync impl (Redis)
// =========================================================================

#[pymethods]
impl Redis {
    // --- EVAL / EVALSHA / EVAL_RO / EVALSHA_RO ---

    #[pyo3(signature = (script, keys, args))]
    pub(crate) fn eval(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_eval("EVAL", script, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(py, RawResult::Value(r.map_err(to_py_err)?).into_py(py)?)
    }

    #[pyo3(signature = (sha, keys, args))]
    fn evalsha(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_eval("EVALSHA", sha, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(py, RawResult::Value(r.map_err(to_py_err)?).into_py(py)?)
    }

    #[pyo3(signature = (script, keys, args))]
    fn eval_ro(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_eval("EVAL_RO", script, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(py, RawResult::Value(r.map_err(to_py_err)?).into_py(py)?)
    }

    #[pyo3(signature = (sha, keys, args))]
    fn evalsha_ro(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_eval("EVALSHA_RO", sha, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(py, RawResult::Value(r.map_err(to_py_err)?).into_py(py)?)
    }

    // --- SCRIPT LOAD / EXISTS / FLUSH / KILL ---

    fn script_load(&self, py: Python<'_>, script: &str) -> PyResult<String> {
        let cmd = cmd_script_load(script);
        let r: redis::RedisResult<String> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (*shas))]
    fn script_exists(&self, py: Python<'_>, shas: Vec<String>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_script_exists(&shas);
        let r: redis::RedisResult<Vec<bool>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        RawResult::BoolList(r.map_err(to_py_err)?).into_py(py)
    }

    #[pyo3(signature = (*, mode=None))]
    fn script_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<()> {
        if let Some(ref m) = mode {
            validate_flush_mode(m)?;
        }
        let cmd = cmd_script_flush(mode.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    fn script_kill(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_script_kill();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    // --- FCALL / FCALL_RO ---

    #[pyo3(signature = (function, keys, args))]
    fn fcall(
        &self,
        py: Python<'_>,
        function: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_fcall("FCALL", function, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(py, RawResult::Value(r.map_err(to_py_err)?).into_py(py)?)
    }

    #[pyo3(signature = (function, keys, args))]
    fn fcall_ro(
        &self,
        py: Python<'_>,
        function: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_fcall("FCALL_RO", function, &keys, &args);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(py, RawResult::Value(r.map_err(to_py_err)?).into_py(py)?)
    }

    // --- FUNCTION LOAD / DELETE / DUMP / FLUSH / LIST / STATS / KILL / RESTORE ---

    #[pyo3(signature = (code, *, replace=false))]
    fn function_load(&self, py: Python<'_>, code: &str, replace: bool) -> PyResult<String> {
        let cmd = cmd_function_load(code, replace);
        let r: redis::RedisResult<String> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map_err(to_py_err)
    }

    fn function_delete(&self, py: Python<'_>, library: &str) -> PyResult<()> {
        let cmd = cmd_function_delete(library);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    fn function_dump(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_function_dump();
        let r: redis::RedisResult<Vec<u8>> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(
            py,
            pyo3::types::PyBytes::new(py, &r.map_err(to_py_err)?)
                .into_any()
                .unbind(),
        )
    }

    #[pyo3(signature = (*, mode=None))]
    fn function_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<()> {
        if let Some(ref m) = mode {
            validate_flush_mode(m)?;
        }
        let cmd = cmd_function_flush(mode.as_deref());
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (*, library=None, withcode=false))]
    fn function_list(
        &self,
        py: Python<'_>,
        library: Option<String>,
        withcode: bool,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_function_list(library.as_deref(), withcode);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(py, RawResult::Value(r.map_err(to_py_err)?).into_py(py)?)
    }

    fn function_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmd = cmd_function_stats();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        self.maybe_decode(py, RawResult::Value(r.map_err(to_py_err)?).into_py(py)?)
    }

    fn function_kill(&self, py: Python<'_>) -> PyResult<()> {
        let cmd = cmd_function_kill();
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, async { dispatch_cmd!(&mut *conn, cmd) });
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (dump, *, policy=None))]
    fn function_restore(
        &self,
        py: Python<'_>,
        dump: &[u8],
        policy: Option<String>,
    ) -> PyResult<()> {
        if let Some(ref p) = policy {
            validate_restore_policy(p)?;
        }
        let cmd = cmd_function_restore(dump, policy.as_deref());
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
    // --- EVAL / EVALSHA / EVAL_RO / EVALSHA_RO ---

    #[pyo3(signature = (script, keys, args))]
    fn eval(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let script = script.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_eval("EVAL", &script, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (sha, keys, args))]
    fn evalsha(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let sha = sha.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_eval("EVALSHA", &sha, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (script, keys, args))]
    fn eval_ro(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let script = script.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_eval("EVAL_RO", &script, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (sha, keys, args))]
    fn evalsha_ro(
        &self,
        py: Python<'_>,
        sha: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let sha = sha.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_eval("EVALSHA_RO", &sha, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // --- SCRIPT LOAD / EXISTS / FLUSH / KILL ---

    fn script_load(&self, py: Python<'_>, script: &str) -> PyResult<Py<PyAny>> {
        let script = script.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_script_load(&script);
            let r: redis::RedisResult<String> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*shas))]
    fn script_exists(&self, py: Python<'_>, shas: Vec<String>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_script_exists(&shas);
            let r: redis::RedisResult<Vec<bool>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, mode=None))]
    fn script_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<Py<PyAny>> {
        if let Some(ref m) = mode {
            validate_flush_mode(m)?;
        }
        async_op!(self, py, conn, async {
            let cmd = cmd_script_flush(mode.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn script_kill(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_script_kill();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    // --- FCALL / FCALL_RO ---

    #[pyo3(signature = (function, keys, args))]
    fn fcall(
        &self,
        py: Python<'_>,
        function: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let function = function.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_fcall("FCALL", &function, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (function, keys, args))]
    fn fcall_ro(
        &self,
        py: Python<'_>,
        function: &str,
        keys: Vec<String>,
        args: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        let function = function.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_fcall("FCALL_RO", &function, &keys, &args);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    // --- FUNCTION LOAD / DELETE / DUMP / FLUSH / LIST / STATS / KILL / RESTORE ---

    #[pyo3(signature = (code, *, replace=false))]
    fn function_load(&self, py: Python<'_>, code: &str, replace: bool) -> PyResult<Py<PyAny>> {
        let code = code.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_function_load(&code, replace);
            let r: redis::RedisResult<String> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn function_delete(&self, py: Python<'_>, library: &str) -> PyResult<Py<PyAny>> {
        let library = library.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_function_delete(&library);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn function_dump(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_function_dump();
            let r: redis::RedisResult<Vec<u8>> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (*, mode=None))]
    fn function_flush(&self, py: Python<'_>, mode: Option<String>) -> PyResult<Py<PyAny>> {
        if let Some(ref m) = mode {
            validate_flush_mode(m)?;
        }
        async_op!(self, py, conn, async {
            let cmd = cmd_function_flush(mode.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (*, library=None, withcode=false))]
    fn function_list(
        &self,
        py: Python<'_>,
        library: Option<String>,
        withcode: bool,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_function_list(library.as_deref(), withcode);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn function_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_function_stats();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            r.into_raw_result()
        })
    }

    fn function_kill(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_function_kill();
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (dump, *, policy=None))]
    fn function_restore(
        &self,
        py: Python<'_>,
        dump: &[u8],
        policy: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        if let Some(ref p) = policy {
            validate_restore_policy(p)?;
        }
        let dump = dump.to_vec();
        async_op!(self, py, conn, async {
            let cmd = cmd_function_restore(&dump, policy.as_deref());
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut *conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
}
