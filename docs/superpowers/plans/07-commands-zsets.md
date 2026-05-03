# Plan 07 — Sorted-set commands

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the full v0.1 sorted-set surface on `RedisRsDriver`. Sorted sets are the largest and most option-laden command family in Redis, with subtle return-shape variations (`WITHSCORES`, single-vs-count `ZPOPMIN`/`ZPOPMAX`, `BYSCORE`/`BYLEX`/`REV`/`LIMIT` modifiers on `ZRANGE`, the `NX`/`XX`/`GT`/`LT`/`CH`/`INCR` matrix on `ZADD`, the multi-key blocking `BZMPOP`). Each command ships as a sync (`zxxx`) + async (`azxxx`) pair backed by a live Valkey via testcontainers.

**Architecture:** Per the Plan-01 file-structure invariants, each command family lives in its own file. This plan creates `crates/redis-rs-py-driver/src/commands/zsets.rs` with one `impl RedisRsDriver` block. Slots into the `commands` module created by Plan 05 (`commands/mod.rs::pub mod zsets;`). The `WITHSCORES`-shaped reply (`list[tuple[bytes, float]]`) is built via the existing `RawResult::ScoredMembers` variant from Plan 01. The blocking `BZPOPMIN`/`BZPOPMAX`/`BZMPOP` commands use the lazy blocking connection wired up by Plan 04 (`ValkeyConn::get_blocking().await`) so they don't head-of-line-block the multiplexed pipeline. Most commands have such a rich option matrix that bodies build `redis::Cmd` by hand and dispatch via `dispatch_cmd!` rather than calling typed `AsyncCommands` methods.

**WITHSCORES return shape (load-bearing):** every "...WITHSCORES" command returns `list[tuple[bytes, float]]` — a flat list of `(member, score)` pairs, *not* a dict. This matches redis-py exactly. The Rust side decodes via `RawResult::ScoredMembers(Vec<(Vec<u8>, f64)>)`.

**Tech Stack:** PyO3 0.28, tokio 1.x, redis 1.x (`AsyncCommands`, `Cmd`), testcontainers (Valkey 8.0). Python 3.14 + 3.14t.

**Reference material:**
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/01-foundation-async-bridge.md` — defines `async_op!`, `sync_op!`, `conn_method!`, `dispatch_cmd!`, `IntoRawResult`, `RawResult::{ScoredMembers, OptKeyAndBytesList}`, and `py_*` helpers.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/04-commands-lists.md` — exposes `ValkeyConn::get_blocking().await` which `BZPOPMIN`/`BZPOPMAX`/`BZMPOP` consume.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/05-commands-hashes.md` — establishes `commands/` module path convention.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/06-commands-sets.md` — establishes the `RawResult::SetOfBytes` and `BoolList` variants we re-use; introduces the `dispatch_cmd!`-with-explicit-arg pattern this plan extends.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs:1921-2278` — cachex's `zadd`/`zrem`/`zrange`/`zrangebyscore`/`zrevrange`/`zincrby`/`zcard`/`zscore`/`zrank`/`zrevrank`/`zmscore`/`zremrangebyrank`/`zremrangebyscore`/`zrevrangebyscore`/`zcount`/`zpopmin`/`zpopmax`. We widen the option matrices substantially (cachex's `zadd` is bare; this plan adds the full flag matrix).
- redis-py `redis/commands/core.py::SortedSetCommands` — canonical kwarg shapes for `zadd(name, mapping, nx=False, xx=False, ch=False, incr=False, gt=False, lt=False)`, `zrange(name, start, end, desc=False, withscores=False, score_cast_func=float, byscore=False, bylex=False, offset=None, num=None)`, `zrank(name, value, withscore=False)`.
- Redis docs:
  - `ZADD`: https://redis.io/commands/zadd/ — return is `int` for normal, `float|None` for `INCR` mode, `int` (changed count) for `CH`.
  - `ZRANGE`: https://redis.io/commands/zrange/ — `BYSCORE`/`BYLEX` modify the meaning of `start`/`stop`; `LIMIT offset count` requires `BYSCORE` or `BYLEX`.
  - `ZRANK` 7.2+: `WITHSCORE` returns `[rank, score]` array or nil.
  - `ZMPOP`/`BZMPOP`: https://redis.io/commands/zmpop/ — return is `[key, [[member, score], ...]]` or nil.

**Out of scope for this plan:**
- The high-level `Redis` façade — plan 10. Only low-level `RedisRsDriver` here.
- Pipelines/transactions — plan 13.
- `decode_responses=True` — plan 12.
- A redis-py-shaped `ZSCAN` async iterator — plan 10.

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  commands/
    mod.rs                     # MODIFIED: add `pub mod zsets;`
    zsets.rs                   # NEW: every sorted-set command on RedisRsDriver
  raw_result.rs                # MODIFIED: add From for OptF64Or i64 unions where needed
  async_bridge.rs              # MODIFIED: add ZPopReply / ZRankWithScore / ZMPop variants
python/
  redis_rs_py/
    _driver.pyi                # MODIFIED: add zset-command method stubs
tests/
  driver/
    test_commands_zsets.py     # NEW: end-to-end coverage of every zset command
```

---

## Task 1: Add the `commands::zsets` module path + new RawResult variants

Wire the new module file and the `RawResult` variants the rich option matrices need.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/mod.rs`
- Create: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs`

- [ ] **Step 1: Add `pub mod zsets;` to `commands/mod.rs`**

Edit `crates/redis-rs-py-driver/src/commands/mod.rs`:

```rust
// Per-family command modules.

pub mod hashes;
pub mod sets;
pub mod zsets;
```

- [ ] **Step 2: Stub `commands/zsets.rs`**

Create `crates/redis-rs-py-driver/src/commands/zsets.rs`:

```rust
// Sorted-set commands on RedisRsDriver.
//
// Filled in by Plan 07 — for now an empty pyclass-extension block so the
// `mod zsets;` declaration compiles.

use crate::driver::RedisRsDriver;
use pyo3::prelude::*;

#[pymethods]
impl RedisRsDriver {}
```

- [ ] **Step 3: Add three new variants to `RawResult`**

Edit `crates/redis-rs-py-driver/src/async_bridge.rs`. In the enum, add:

```rust
    // ZADD INCR mode → float | None
    OptScore(Option<f64>),
    // ZRANK WITHSCORE → (rank, score) | None
    OptRankAndScore(Option<(i64, f64)>),
    // ZMPOP / BZMPOP → (key, [(member, score), ...]) | None
    OptKeyAndScoredMembers(Option<(String, Vec<(Vec<u8>, f64)>)>),
```

In `into_py`, add the matching arms:

```rust
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
```

- [ ] **Step 4: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: `Finished` with unused-warnings only.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/ crates/redis-rs-py-driver/src/async_bridge.rs
git commit -m "feat(zsets): scaffold commands/zsets.rs and ZADD/ZRANK/ZMPOP variants"
```

---

## Task 2: ZADD with the full flag matrix

Sub-task (a). `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]`. Mutually-exclusive groups: `NX`/`XX`; `GT`/`LT`/`neither`. With `INCR`, only one (score, member) pair allowed and the return is `float|None` (None when the NX/XX/GT/LT condition isn't met). Without `INCR`, return is `int` (newly added or, with `CH`, changed).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Test: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Write the failing test for the (a) sub-task**

Create `tests/driver/test_commands_zsets.py`:

```python
"""Sorted-set command coverage on RedisRsDriver — sub-task (a): ZADD."""

from __future__ import annotations

import pytest


# --- ZADD basic ---------------------------------------------------------

def test_zadd_basic_returns_added_count(driver) -> None:
    assert driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0}) == 3


def test_zadd_existing_member_returns_zero(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    # Updating an existing member does not bump the count (without CH).
    assert driver.zadd("z", mapping={"a": 5.0}) == 0


def test_zadd_with_ch_returns_changed_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    # Both `a` (rescored) and `c` (added) → 2 changed.
    assert driver.zadd("z", mapping={"a": 10.0, "c": 3.0}, ch=True) == 2


# --- NX / XX -------------------------------------------------------------

def test_zadd_nx_only_inserts_new(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    n = driver.zadd("z", mapping={"a": 99.0, "b": 2.0}, nx=True)
    assert n == 1  # only `b` newly added
    assert driver.zscore("z", b"a") == 1.0  # not overwritten


def test_zadd_xx_only_updates_existing(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    n = driver.zadd("z", mapping={"a": 5.0, "b": 2.0}, xx=True)
    # `b` is rejected by XX; `a` updated but not new → 0 added.
    assert n == 0
    assert driver.zscore("z", b"a") == 5.0
    assert driver.zscore("z", b"b") is None


def test_zadd_nx_and_xx_together_raises_data_error(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="NX and XX"):
        driver.zadd("z", mapping={"a": 1.0}, nx=True, xx=True)


# --- GT / LT -------------------------------------------------------------

def test_zadd_gt_only_updates_when_new_score_higher(driver) -> None:
    driver.zadd("z", mapping={"a": 5.0})
    driver.zadd("z", mapping={"a": 3.0}, gt=True)
    assert driver.zscore("z", b"a") == 5.0  # not lowered
    driver.zadd("z", mapping={"a": 10.0}, gt=True)
    assert driver.zscore("z", b"a") == 10.0


def test_zadd_lt_only_updates_when_new_score_lower(driver) -> None:
    driver.zadd("z", mapping={"a": 5.0})
    driver.zadd("z", mapping={"a": 10.0}, lt=True)
    assert driver.zscore("z", b"a") == 5.0
    driver.zadd("z", mapping={"a": 1.0}, lt=True)
    assert driver.zscore("z", b"a") == 1.0


def test_zadd_gt_and_lt_together_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="GT and LT"):
        driver.zadd("z", mapping={"a": 1.0}, gt=True, lt=True)


def test_zadd_nx_and_gt_together_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="NX"):
        driver.zadd("z", mapping={"a": 1.0}, nx=True, gt=True)


# --- INCR ---------------------------------------------------------------

def test_zadd_incr_returns_new_score(driver) -> None:
    got = driver.zadd("z", mapping={"a": 5.0}, incr=True)
    assert got == 5.0
    got2 = driver.zadd("z", mapping={"a": 3.0}, incr=True)
    assert got2 == 8.0


def test_zadd_incr_blocked_by_nx_returns_none(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    got = driver.zadd("z", mapping={"a": 5.0}, incr=True, nx=True)
    assert got is None


def test_zadd_incr_with_multiple_pairs_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="INCR.*single"):
        driver.zadd("z", mapping={"a": 1.0, "b": 2.0}, incr=True)


def test_zadd_empty_mapping_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="empty"):
        driver.zadd("z", mapping={})


# --- async --------------------------------------------------------------

@pytest.mark.asyncio
async def test_azadd_basic(driver) -> None:
    assert await driver.azadd("z", mapping={"a": 1.0, "b": 2.0}) == 2


@pytest.mark.asyncio
async def test_azadd_incr_returns_score(driver) -> None:
    assert await driver.azadd("z", mapping={"a": 3.5}, incr=True) == 3.5


@pytest.mark.asyncio
async def test_azadd_incr_nx_blocked_returns_none(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azadd("z", mapping={"a": 5.0}, incr=True, nx=True) is None
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v`
Expected: every test FAILS with `AttributeError: 'builtins.RedisRsDriver' object has no attribute 'zadd'`.

- [ ] **Step 3: Implement ZADD**

Replace `crates/redis-rs-py-driver/src/commands/zsets.rs`:

```rust
// Sorted-set commands on RedisRsDriver.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyTuple};
use redis::AsyncCommands;

use crate::async_bridge::RawResult;
use crate::driver::{
    py_bool, py_bytes_list, py_int, py_opt_bytes, RedisRsDriver,
};
use crate::errors::to_py_err;
use crate::exceptions::{DataError, ExceptionClass};
use crate::raw_result::IntoRawResult;
use crate::{async_op, conn_method, dispatch_cmd, sync_op};

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

fn collect_zadd_pairs(
    mapping: &Bound<'_, PyDict>,
) -> PyResult<Vec<(Vec<u8>, f64)>> {
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

fn build_zadd_cmd(
    key: &str,
    pairs: &[(Vec<u8>, f64)],
    f: ZAddFlags,
) -> redis::Cmd {
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

#[pymethods]
impl RedisRsDriver {
    // =====================================================================
    // (a) ZADD
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
        let flags = ZAddFlags { nx, xx, gt, lt, ch, incr };
        let pairs = collect_zadd_pairs(mapping)?;
        validate_zadd_flags(flags, pairs.len())?;
        let cmd = build_zadd_cmd(key, &pairs, flags);
        if incr {
            let r: redis::RedisResult<Option<f64>> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            match r.map_err(to_py_err)? {
                Some(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
                None => Ok(py.None()),
            }
        } else {
            let r: redis::RedisResult<i64> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
        let flags = ZAddFlags { nx, xx, gt, lt, ch, incr };
        let pairs = collect_zadd_pairs(mapping)?;
        validate_zadd_flags(flags, pairs.len())?;
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = build_zadd_cmd(&key, &pairs, flags);
            if flags.incr {
                let r: redis::RedisResult<Option<f64>> = dispatch_cmd!(&mut conn, cmd);
                match r {
                    Ok(v) => RawResult::OptScore(v),
                    Err(e) => crate::errors::classify(e),
                }
            } else {
                let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            }
        })
    }
}
```

Note the `mapping` is **keyword-only** to keep the explicit shape `zadd("z", mapping={...}, nx=True)`.

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v`
Expected: 17 PASS (the 17 tests in Step 1).

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZADD with full NX/XX/GT/LT/CH/INCR flag matrix"
```

---

## Task 3: ZREM (variadic)

Quick supporting command. `ZREM key member [member ...]` returns count of members actually removed.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the ZREM tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZREM ---------------------------------------------------------------

def test_zrem_returns_removed_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrem("z", b"a", b"missing", b"c") == 2
    assert driver.zcard("z") == 1


def test_zrem_missing_key_returns_zero(driver) -> None:
    assert driver.zrem("missing", b"a") == 0


def test_zrem_no_members_returns_zero(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zrem("z") == 0


@pytest.mark.asyncio
async def test_azrem(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azrem("z", b"a") == 1
```

(`zcard` is implemented in Task 5 — these tests use it; the implementation is already in cachex's pattern, but we still add it formally in Task 5. For now, the assertion `driver.zcard("z") == 1` will fail until Task 5. Comment that line if running in isolation — re-enable it once Task 5 lands.)

- [ ] **Step 2: Run the failing test**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k zrem`
Expected: FAIL with `AttributeError: ... 'zrem'`.

- [ ] **Step 3: Implement ZREM**

Append inside the `#[pymethods]` block of `commands/zsets.rs`:

```rust
    // =====================================================================
    // ZREM
    // =====================================================================

    #[pyo3(signature = (key, *members))]
    fn zrem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return py_int(py, 0);
        }
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.zrem(key, &members)));
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *members))]
    fn azrem(&self, py: Python<'_>, key: &str, members: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if members.is_empty() {
                return RawResult::Int(0);
            }
            let r: redis::RedisResult<i64> = conn_method!(&mut conn, c, c.zrem(&key, &members));
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run (the zcard-dependent assertion still fails — that's OK, Task 5 fixes it)**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k zrem`
Expected: 3 PASS plus the one that depends on zcard FAILS (`AttributeError`). That's expected — the failing case will go green after Task 5.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZREM variadic"
```

---

## Task 4: ZRANGE family + ZRANGESTORE

Sub-task (b). `ZRANGE` is the workhorse — `start`, `end`, plus `byscore`/`bylex`/`desc`/`withscores`/`offset`/`num` modifiers. The `LIMIT offset count` modifier (`offset`/`num`) is only valid with `BYSCORE` or `BYLEX`. With `WITHSCORES`, returns `list[tuple[bytes, float]]`. Without, returns `list[bytes]`. `ZRANGESTORE` writes the slice into a destination key and returns the cardinality.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (b) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZRANGE -------------------------------------------------------------

def test_zrange_basic(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrange("z", 0, -1) == [b"a", b"b", b"c"]


def test_zrange_with_scores(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    got = driver.zrange("z", 0, -1, withscores=True)
    assert got == [(b"a", 1.0), (b"b", 2.0)]


def test_zrange_desc(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrange("z", 0, -1, desc=True) == [b"c", b"b", b"a"]


def test_zrange_byscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert driver.zrange("z", "2", "3", byscore=True) == [b"b", b"c"]


def test_zrange_byscore_with_limit(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    got = driver.zrange("z", "1", "10", byscore=True, offset=1, num=2)
    assert got == [b"b", b"c"]


def test_zrange_bylex(driver) -> None:
    # All same score for BYLEX ordering.
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0})
    assert driver.zrange("z", "[a", "[b", bylex=True) == [b"a", b"b"]


def test_zrange_byscore_and_bylex_together_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="BYSCORE and BYLEX"):
        driver.zrange("z", "0", "10", byscore=True, bylex=True)


def test_zrange_limit_without_byscore_or_bylex_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="LIMIT"):
        driver.zrange("z", 0, -1, offset=0, num=5)


def test_zrange_withscores_and_bylex_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="WITHSCORES"):
        driver.zrange("z", "[a", "[z", bylex=True, withscores=True)


@pytest.mark.asyncio
async def test_azrange(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azrange("z", 0, -1) == [b"a", b"b"]


@pytest.mark.asyncio
async def test_azrange_withscores(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrange("z", 0, -1, withscores=True) == [(b"a", 1.0)]


# --- ZRANGESTORE --------------------------------------------------------

def test_zrangestore_basic(driver) -> None:
    driver.zadd("src", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    n = driver.zrangestore("dst", "src", 0, 1)
    assert n == 2
    assert driver.zrange("dst", 0, -1) == [b"a", b"b"]


def test_zrangestore_byscore(driver) -> None:
    driver.zadd("src", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    n = driver.zrangestore("dst", "src", "2", "3", byscore=True)
    assert n == 2


@pytest.mark.asyncio
async def test_azrangestore(driver) -> None:
    await driver.azadd("src", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azrangestore("dst", "src", 0, -1) == 2
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k "zrange and not bylex" -k zrangestore`
Expected: every new test FAILS.

- [ ] **Step 3: Implement ZRANGE + ZRANGESTORE**

Add a helper at the bottom of `commands/zsets.rs` (outside `#[pymethods]`):

```rust
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
```

Inside `#[pymethods]`, append the ZRANGE pair:

```rust
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
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
        let key = key.to_string();
        let _ = key; // shadow for ownership in async block
        async_op!(self, py, conn, async {
            if withscores {
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

Outside `#[pymethods]`, add the renderer + arg-coercion helpers:

```rust
fn pyany_to_zrange_arg(v: &Bound<'_, PyAny>) -> PyResult<String> {
    // ZRANGE accepts ints (rank), floats / "(N" / "-inf" / "+inf" (score),
    // or "[member" / "(member" / "-" / "+" (lex). All hit Redis as a
    // single text token, so coerce to string regardless of the Python
    // type — preserve int formatting (no trailing .0).
    if let Ok(i) = v.extract::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(f) = v.extract::<f64>() {
        return Ok(f.to_string());
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(s);
    }
    if let Ok(b) = v.extract::<Vec<u8>>() {
        return Ok(String::from_utf8_lossy(&b).into_owned());
    }
    Err(PyErr::new::<DataError, _>(
        "ZRANGE start/stop must be int, float, str, or bytes",
    ))
}

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
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k "zrange or zrangestore"`
Expected: 14 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZRANGE family + ZRANGESTORE with full modifier matrix"
```

---

## Task 5: ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGEBYLEX

Sub-task (c). Older-style range commands kept for redis-py parity. Each accepts `min`/`max` (or reversed for REV variants), `withscores` (BYSCORE only), and `LIMIT offset count` (`offset`/`num` kwargs).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (c) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZRANGEBYSCORE / ZREVRANGEBYSCORE -----------------------------------

def test_zrangebyscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrangebyscore("z", "-inf", "2") == [b"a", b"b"]


def test_zrangebyscore_with_scores(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    assert driver.zrangebyscore("z", "1", "2", withscores=True) == [
        (b"a", 1.0),
        (b"b", 2.0),
    ]


def test_zrangebyscore_with_limit(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    got = driver.zrangebyscore("z", "1", "10", offset=1, num=2)
    assert got == [b"b", b"c"]


def test_zrevrangebyscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    # Note: max comes BEFORE min for REV.
    assert driver.zrevrangebyscore("z", "3", "1") == [b"c", b"b", b"a"]


def test_zrevrangebyscore_with_limit(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrevrangebyscore("z", "3", "1", offset=0, num=2) == [b"c", b"b"]


@pytest.mark.asyncio
async def test_azrangebyscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrangebyscore("z", "-inf", "+inf") == [b"a"]


# --- ZRANGEBYLEX / ZREVRANGEBYLEX ---------------------------------------

def test_zrangebylex(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
    assert driver.zrangebylex("z", "[a", "[c") == [b"a", b"b", b"c"]


def test_zrangebylex_exclusive(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0})
    assert driver.zrangebylex("z", "(a", "(c") == [b"b"]


def test_zrangebylex_with_limit(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
    assert driver.zrangebylex("z", "-", "+", offset=1, num=2) == [b"b", b"c"]


def test_zrevrangebylex(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0})
    # Note: max comes BEFORE min for REV.
    assert driver.zrevrangebylex("z", "[c", "[a") == [b"c", b"b", b"a"]


@pytest.mark.asyncio
async def test_azrangebylex(driver) -> None:
    await driver.azadd("z", mapping={"a": 0.0, "b": 0.0})
    assert await driver.azrangebylex("z", "[a", "[b") == [b"a", b"b"]
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k "zrangebyscore or zrevrangebyscore or zrangebylex or zrevrangebylex"`
Expected: 11 FAIL.

- [ ] **Step 3: Implement the four `*range_by*` commands**

Outside `#[pymethods]` in `commands/zsets.rs`, add a helper:

```rust
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
```

Inside `#[pymethods]`, append:

```rust
    // =====================================================================
    // (c) ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGEBYLEX
    // =====================================================================

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
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

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
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            }
        })
    }

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
        let cmd = build_simple_range_cmd("ZREVRANGEBYSCORE", key, max, min, withscores, offset, num)?;
        if withscores {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

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
        let cmd = build_simple_range_cmd("ZREVRANGEBYSCORE", key, max, min, withscores, offset, num)?;
        async_op!(self, py, conn, async {
            if withscores {
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
            let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
            let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k "zrangebyscore or zrevrangebyscore or zrangebylex or zrevrangebylex"`
Expected: 11 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZRANGEBYSCORE/BYLEX + REV variants"
```

---

## Task 6: ZINCRBY / ZCARD / ZSCORE / ZMSCORE

Sub-task (d). Counters and score lookups. `ZSCORE` returns `float|None`. `ZMSCORE` is variadic and returns `list[float|None]` preserving input order.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (d) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZINCRBY ------------------------------------------------------------

def test_zincrby_creates_member_at_delta(driver) -> None:
    assert driver.zincrby("z", 5.5, b"a") == 5.5
    assert driver.zscore("z", b"a") == 5.5


def test_zincrby_increments_existing(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zincrby("z", 2.5, b"a") == 3.5
    assert driver.zincrby("z", -1.0, b"a") == 2.5


@pytest.mark.asyncio
async def test_azincrby(driver) -> None:
    assert await driver.azincrby("z", 1.0, b"a") == 1.0


# --- ZCARD --------------------------------------------------------------

def test_zcard(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zcard("z") == 3


def test_zcard_missing_is_zero(driver) -> None:
    assert driver.zcard("missing") == 0


@pytest.mark.asyncio
async def test_azcard(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azcard("z") == 1


# --- ZSCORE -------------------------------------------------------------

def test_zscore_present(driver) -> None:
    driver.zadd("z", mapping={"a": 3.5})
    assert driver.zscore("z", b"a") == 3.5


def test_zscore_absent_returns_none(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zscore("z", b"missing") is None
    assert driver.zscore("missing-key", b"a") is None


@pytest.mark.asyncio
async def test_azscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 2.0})
    assert await driver.azscore("z", b"a") == 2.0
    assert await driver.azscore("z", b"x") is None


# --- ZMSCORE ------------------------------------------------------------

def test_zmscore_preserves_order(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "c": 3.0})
    assert driver.zmscore("z", b"a", b"b", b"c") == [1.0, None, 3.0]


def test_zmscore_missing_key(driver) -> None:
    assert driver.zmscore("missing", b"a", b"b") == [None, None]


def test_zmscore_no_members_returns_empty(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zmscore("z") == []


@pytest.mark.asyncio
async def test_azmscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azmscore("z", b"a", b"b") == [1.0, None]
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k "zincrby or zcard or zscore or zmscore"`
Expected: 13 FAIL.

- [ ] **Step 3: Implement ZINCRBY / ZCARD / ZSCORE / ZMSCORE**

Inside `#[pymethods]`, append:

```rust
    // =====================================================================
    // (d) ZINCRBY / ZCARD / ZSCORE / ZMSCORE
    // =====================================================================

    #[pyo3(signature = (key, amount, member))]
    fn zincrby(
        &self,
        py: Python<'_>,
        key: &str,
        amount: f64,
        member: &[u8],
    ) -> PyResult<f64> {
        sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.zincr(key, member, amount))
        )
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
                conn_method!(&mut conn, c, c.zincr(&key, &member, amount));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key))]
    fn zcard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.zcard(key)));
        py_int(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key))]
    fn azcard(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = conn_method!(&mut conn, c, c.zcard(&key));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, member))]
    fn zscore(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Option<f64>> {
        sync_op!(py, self, conn, conn_method!(&mut conn, c, c.zscore(key, member)))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (key, member))]
    fn azscore(&self, py: Python<'_>, key: &str, member: &[u8]) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let member = member.to_vec();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Option<f64>> =
                conn_method!(&mut conn, c, c.zscore(&key, &member));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *members))]
    fn zmscore(
        &self,
        py: Python<'_>,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
        if members.is_empty() {
            return Ok(PyList::empty(py).into_any().unbind());
        }
        let r: redis::RedisResult<Vec<Option<f64>>> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZMSCORE");
            cmd.arg(key);
            for m in &members {
                cmd.arg(m.as_slice());
            }
            dispatch_cmd!(&mut conn, cmd)
        });
        let items = r.map_err(to_py_err)?;
        let py_items: Vec<Py<PyAny>> = items
            .into_iter()
            .map(|opt| match opt {
                Some(f) => f.into_pyobject(py).map(|v| v.into_any().unbind()),
                None => Ok(py.None()),
            })
            .collect::<PyResult<_>>()?;
        Ok(PyList::new(py, py_items)?.into_any().unbind())
    }

    #[pyo3(signature = (key, *members))]
    fn azmscore(
        &self,
        py: Python<'_>,
        key: &str,
        members: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyAny>> {
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::Value(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k "zincrby or zcard or zscore or zmscore"`
Expected: 13 PASS. (And the previously-failing zrem assertion that depended on `zcard` now passes too.)

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZINCRBY/ZCARD/ZSCORE/ZMSCORE"
```

---

## Task 7: ZRANK / ZREVRANK with WITHSCORE

Sub-task (e). Returns `int|None`, or `tuple[int, float]|None` with `withscore=True` (Redis 7.2+).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (e) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZRANK / ZREVRANK ---------------------------------------------------

def test_zrank(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrank("z", b"a") == 0
    assert driver.zrank("z", b"c") == 2
    assert driver.zrank("z", b"missing") is None


def test_zrevrank(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zrevrank("z", b"c") == 0
    assert driver.zrevrank("z", b"a") == 2


def test_zrank_withscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.5})
    got = driver.zrank("z", b"b", withscore=True)
    assert got == (1, 2.5)


def test_zrank_withscore_missing_returns_none(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    assert driver.zrank("z", b"missing", withscore=True) is None


def test_zrevrank_withscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    assert driver.zrevrank("z", b"a", withscore=True) == (1, 1.0)


@pytest.mark.asyncio
async def test_azrank(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrank("z", b"a") == 0


@pytest.mark.asyncio
async def test_azrank_withscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrank("z", b"a", withscore=True) == (0, 1.0)
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k "zrank or zrevrank"`
Expected: 7 FAIL.

- [ ] **Step 3: Implement ZRANK / ZREVRANK**

Inside `#[pymethods]`, append:

```rust
    // =====================================================================
    // (e) ZRANK / ZREVRANK with WITHSCORE
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
```

Outside `#[pymethods]`, add the shared implementations:

```rust
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
        dispatch_cmd!(&mut conn, cmd)
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
        let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
        match r {
            Ok(v) => parse_rank_reply_to_rawresult(v, withscore),
            Err(e) => crate::errors::classify(e),
        }
    })
}

fn parse_rank_reply(
    py: Python<'_>,
    value: redis::Value,
    withscore: bool,
) -> PyResult<Py<PyAny>> {
    if !withscore {
        return Ok(match value {
            redis::Value::Nil => py.None(),
            redis::Value::Int(n) => n.into_pyobject(py)?.into_any().unbind(),
            other => format!("{other:?}").into_pyobject(py)?.into_any().unbind(),
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
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k "zrank or zrevrank"`
Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZRANK/ZREVRANK with WITHSCORE (Redis 7.2+)"
```

---

## Task 8: ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREMRANGEBYLEX

Sub-task (f). Three pruners. Each returns `int` (count removed).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (f) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZREMRANGEBYRANK ----------------------------------------------------

def test_zremrangebyrank(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert driver.zremrangebyrank("z", 1, 2) == 2
    assert driver.zrange("z", 0, -1) == [b"a", b"d"]


def test_zremrangebyrank_missing_key(driver) -> None:
    assert driver.zremrangebyrank("missing", 0, -1) == 0


@pytest.mark.asyncio
async def test_azremrangebyrank(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azremrangebyrank("z", 0, 0) == 1


# --- ZREMRANGEBYSCORE ---------------------------------------------------

def test_zremrangebyscore(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert driver.zremrangebyscore("z", "2", "3") == 2
    assert driver.zrange("z", 0, -1) == [b"a", b"d"]


@pytest.mark.asyncio
async def test_azremrangebyscore(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    assert await driver.azremrangebyscore("z", "1", "1") == 1


# --- ZREMRANGEBYLEX -----------------------------------------------------

def test_zremrangebylex(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0, "d": 0.0})
    assert driver.zremrangebylex("z", "[b", "[c") == 2
    assert driver.zrange("z", 0, -1) == [b"a", b"d"]


@pytest.mark.asyncio
async def test_azremrangebylex(driver) -> None:
    await driver.azadd("z", mapping={"a": 0.0, "b": 0.0})
    assert await driver.azremrangebylex("z", "[a", "[a") == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k "zremrange"`
Expected: 8 FAIL.

- [ ] **Step 3: Implement the three remrange commands**

Inside `#[pymethods]`, append:

```rust
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
        let r: redis::RedisResult<i64> = sync_op!(
            py,
            self,
            conn,
            conn_method!(&mut conn, c, c.zremrangebyrank(key, start, stop))
        );
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
            let r: redis::RedisResult<i64> =
                conn_method!(&mut conn, c, c.zremrangebyrank(&key, start, stop));
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
            dispatch_cmd!(&mut conn, cmd)
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
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
            dispatch_cmd!(&mut conn, cmd)
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k "zremrange"`
Expected: 8 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZREMRANGEBYRANK/BYSCORE/BYLEX pruners"
```

---

## Task 9: ZCOUNT / ZLEXCOUNT

Sub-task (g). Cardinality of a score or lex range.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (g) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZCOUNT / ZLEXCOUNT -------------------------------------------------

def test_zcount(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zcount("z", "1", "2") == 2
    assert driver.zcount("z", "-inf", "+inf") == 3


def test_zcount_with_exclusive_bounds(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zcount("z", "(1", "(3") == 1


@pytest.mark.asyncio
async def test_azcount(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azcount("z", "0", "5") == 1


def test_zlexcount(driver) -> None:
    driver.zadd("z", mapping={"a": 0.0, "b": 0.0, "c": 0.0})
    assert driver.zlexcount("z", "[a", "[c") == 3
    assert driver.zlexcount("z", "-", "+") == 3
    assert driver.zlexcount("z", "(a", "(c") == 1


@pytest.mark.asyncio
async def test_azlexcount(driver) -> None:
    await driver.azadd("z", mapping={"a": 0.0, "b": 0.0})
    assert await driver.azlexcount("z", "[a", "[b") == 2
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k "zcount or zlexcount"`
Expected: 5 FAIL.

- [ ] **Step 3: Implement ZCOUNT / ZLEXCOUNT**

Inside `#[pymethods]`, append:

```rust
    // =====================================================================
    // (g) ZCOUNT / ZLEXCOUNT
    // =====================================================================

    #[pyo3(signature = (key, min, max))]
    fn zcount(&self, py: Python<'_>, key: &str, min: &str, max: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZCOUNT");
            cmd.arg(key).arg(min).arg(max);
            dispatch_cmd!(&mut conn, cmd)
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, min, max))]
    fn zlexcount(&self, py: Python<'_>, key: &str, min: &str, max: &str) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<i64> = sync_op!(py, self, conn, async {
            let mut cmd = redis::cmd("ZLEXCOUNT");
            cmd.arg(key).arg(min).arg(max);
            dispatch_cmd!(&mut conn, cmd)
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k "zcount or zlexcount"`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZCOUNT and ZLEXCOUNT"
```

---

## Task 10: ZPOPMIN / ZPOPMAX + ZMPOP + BZPOPMIN/MAX + BZMPOP

Sub-task (h). Pop highest/lowest by score. `ZPOPMIN`/`ZPOPMAX` with optional `count` return `list[tuple[bytes, float]]` (or empty list if missing). `ZMPOP` is multi-key with direction (`MIN`/`MAX`) and returns `tuple[bytes, list[tuple[bytes, float]]] | None`. The B-prefix variants block on the dedicated blocking connection from Plan 04.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (h) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZPOPMIN / ZPOPMAX --------------------------------------------------

def test_zpopmin(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zpopmin("z") == [(b"a", 1.0)]
    assert driver.zcard("z") == 2


def test_zpopmin_with_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zpopmin("z", count=2) == [(b"a", 1.0), (b"b", 2.0)]


def test_zpopmin_missing_returns_empty(driver) -> None:
    assert driver.zpopmin("missing") == []


def test_zpopmax(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zpopmax("z") == [(b"c", 3.0)]


def test_zpopmax_with_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    assert driver.zpopmax("z", count=2) == [(b"c", 3.0), (b"b", 2.0)]


@pytest.mark.asyncio
async def test_azpopmin(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azpopmin("z") == [(b"a", 1.0)]


# --- ZMPOP --------------------------------------------------------------

def test_zmpop_min(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    got = driver.zmpop("z", direction="MIN")
    assert got == ("z", [(b"a", 1.0)])


def test_zmpop_max_with_count(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    got = driver.zmpop("z", direction="MAX", count=2)
    assert got == ("z", [(b"c", 3.0), (b"b", 2.0)])


def test_zmpop_no_match_returns_none(driver) -> None:
    assert driver.zmpop("missing-1", "missing-2", direction="MIN") is None


def test_zmpop_picks_first_non_empty(driver) -> None:
    driver.zadd("b", mapping={"x": 1.0})
    got = driver.zmpop("a", "b", direction="MIN")
    assert got == ("b", [(b"x", 1.0)])


def test_zmpop_invalid_direction_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="MIN or MAX"):
        driver.zmpop("z", direction="UP")


@pytest.mark.asyncio
async def test_azmpop(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azmpop("z", direction="MIN") == ("z", [(b"a", 1.0)])


# --- BZPOPMIN / BZPOPMAX ------------------------------------------------

def test_bzpopmin_immediate(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    got = driver.bzpopmin("z", timeout=1.0)
    # Returns (key, member, score) tuple per redis docs.
    assert got == (b"z", b"a", 1.0)


def test_bzpopmin_timeout_returns_none(driver) -> None:
    assert driver.bzpopmin("missing", timeout=0.1) is None


def test_bzpopmax_immediate(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    got = driver.bzpopmax("z", timeout=1.0)
    assert got == (b"z", b"b", 2.0)


@pytest.mark.asyncio
async def test_abzpopmin(driver) -> None:
    await driver.azadd("z", mapping={"x": 5.0})
    got = await driver.abzpopmin("z", timeout=1.0)
    assert got == (b"z", b"x", 5.0)


# --- BZMPOP -------------------------------------------------------------

def test_bzmpop_immediate(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0})
    got = driver.bzmpop("z", direction="MIN", timeout=1.0)
    assert got == ("z", [(b"a", 1.0)])


def test_bzmpop_timeout_returns_none(driver) -> None:
    assert driver.bzmpop("missing", direction="MIN", timeout=0.1) is None


@pytest.mark.asyncio
async def test_abzmpop(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    got = await driver.abzmpop("z", direction="MIN", timeout=1.0)
    assert got == ("z", [(b"a", 1.0)])
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k "zpopmin or zpopmax or zmpop or bzpop or bzmpop"`
Expected: every new test FAILS.

- [ ] **Step 3: Add a `BKeyMemberScore` variant for `BZPOPMIN`/`BZPOPMAX`**

`BZPOPMIN`/`BZPOPMAX` return `(key: bytes, member: bytes, score: float) | None` — a flat 3-tuple, not nested. Add the variant in `crates/redis-rs-py-driver/src/async_bridge.rs`:

```rust
    OptKeyMemberScore(Option<(Vec<u8>, Vec<u8>, f64)>),
```

In `into_py`:

```rust
            RawResult::OptKeyMemberScore(Some((k, m, s))) => {
                let k_py = PyBytes::new(py, &k).into_any().unbind();
                let m_py = PyBytes::new(py, &m).into_any().unbind();
                let s_py = s.into_pyobject(py)?.into_any().unbind();
                Ok(PyTuple::new(py, [k_py, m_py, s_py])?.into_any().unbind())
            }
            RawResult::OptKeyMemberScore(None) => Ok(py.None()),
```

- [ ] **Step 4: Implement the (h) commands in `commands/zsets.rs`**

Inside `#[pymethods]`, append:

```rust
    // =====================================================================
    // (h) ZPOPMIN / ZPOPMAX / ZMPOP / BZPOPMIN / BZPOPMAX / BZMPOP
    // =====================================================================

    #[pyo3(signature = (key, *, count=1))]
    fn zpopmin(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.zpopmin(key, count)));
        render_scored_members(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *, count=1))]
    fn azpopmin(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                conn_method!(&mut conn, c, c.zpopmin(&key, count));
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *, count=1))]
    fn zpopmax(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
            sync_op!(py, self, conn, conn_method!(&mut conn, c, c.zpopmax(key, count)));
        render_scored_members(py, r.map_err(to_py_err)?)
    }

    #[pyo3(signature = (key, *, count=1))]
    fn azpopmax(&self, py: Python<'_>, key: &str, count: i64) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> =
                conn_method!(&mut conn, c, c.zpopmax(&key, count));
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
            dispatch_cmd!(&mut conn, cmd)
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => parse_zmpop_to_rawresult(v),
                Err(e) => crate::errors::classify(e),
            }
        })
    }

    #[pyo3(signature = (*keys, timeout))]
    fn bzpopmin(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Option<(Vec<u8>, Vec<u8>, f64)>, _> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async {
                let mut conn = self.connection.get_blocking().await.map_err(|e| {
                    pyo3::exceptions::PyConnectionError::new_err(e.to_string())
                })?;
                let mut cmd = redis::cmd("BZPOPMIN");
                for k in &keys {
                    cmd.arg(k);
                }
                cmd.arg(timeout);
                let r: redis::RedisResult<Option<(Vec<u8>, Vec<u8>, f64)>> =
                    dispatch_cmd!(&mut conn, cmd);
                r.map_err(to_py_err)
            })
        });
        let value = r?;
        render_bzpop_reply(py, value)
    }

    #[pyo3(signature = (*keys, timeout))]
    fn abzpopmin(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
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
                let r: redis::RedisResult<Option<(Vec<u8>, Vec<u8>, f64)>> =
                    dispatch_cmd!(&mut conn, cmd);
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
    fn bzpopmax(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
        let r: Result<Option<(Vec<u8>, Vec<u8>, f64)>, _> = py.detach(|| {
            crate::runtime::get_runtime().block_on(async {
                let mut conn = self.connection.get_blocking().await.map_err(|e| {
                    pyo3::exceptions::PyConnectionError::new_err(e.to_string())
                })?;
                let mut cmd = redis::cmd("BZPOPMAX");
                for k in &keys {
                    cmd.arg(k);
                }
                cmd.arg(timeout);
                let r: redis::RedisResult<Option<(Vec<u8>, Vec<u8>, f64)>> =
                    dispatch_cmd!(&mut conn, cmd);
                r.map_err(to_py_err)
            })
        });
        let value = r?;
        render_bzpop_reply(py, value)
    }

    #[pyo3(signature = (*keys, timeout))]
    fn abzpopmax(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        timeout: f64,
    ) -> PyResult<Py<PyAny>> {
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
                let r: redis::RedisResult<Option<(Vec<u8>, Vec<u8>, f64)>> =
                    dispatch_cmd!(&mut conn, cmd);
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
                let mut conn = self.connection.get_blocking().await.map_err(|e| {
                    pyo3::exceptions::PyConnectionError::new_err(e.to_string())
                })?;
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
```

Outside `#[pymethods]`, add the helpers:

```rust
fn validate_zmpop_direction(d: &str) -> PyResult<&'static str> {
    match d.to_ascii_uppercase().as_str() {
        "MIN" => Ok("MIN"),
        "MAX" => Ok("MAX"),
        _ => Err(PyErr::new::<DataError, _>(
            "ZMPOP/BZMPOP: direction must be MIN or MAX",
        )),
    }
}

fn parse_zmpop_value(value: redis::Value) -> Option<(String, Vec<(Vec<u8>, f64)>)> {
    let items = match value {
        redis::Value::Array(items) if items.len() == 2 => items,
        _ => return None,
    };
    let mut iter = items.into_iter();
    let key = match iter.next().unwrap() {
        redis::Value::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
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
            let key_py = pyo3::types::PyString::new(py, &key).into_any().unbind();
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
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k "zpopmin or zpopmax or zmpop or bzpop or bzmpop"`
Expected: 17 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs crates/redis-rs-py-driver/src/async_bridge.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZPOPMIN/MAX + ZMPOP + BZPOPMIN/MAX + BZMPOP"
```

---

## Task 11: ZRANDMEMBER

Sub-task (i). Tri-mode like SRANDMEMBER but with optional `withscores=True`. No-count → single bytes (or None); positive count → distinct list; negative count → list with replacement; `withscores=True` → list[tuple[bytes, float]].

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (i) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZRANDMEMBER --------------------------------------------------------

def test_zrandmember_no_count(driver) -> None:
    driver.zadd("z", mapping={"only": 1.0})
    assert driver.zrandmember("z") == b"only"


def test_zrandmember_missing_returns_none(driver) -> None:
    assert driver.zrandmember("missing") is None


def test_zrandmember_positive_count_distinct(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0, "c": 3.0})
    got = driver.zrandmember("z", count=2)
    assert isinstance(got, list)
    assert len(got) == 2
    assert len(set(got)) == 2


def test_zrandmember_negative_count_with_repeats(driver) -> None:
    driver.zadd("z", mapping={"only": 1.0})
    got = driver.zrandmember("z", count=-3)
    assert got == [b"only", b"only", b"only"]


def test_zrandmember_withscores(driver) -> None:
    driver.zadd("z", mapping={"a": 1.0, "b": 2.0})
    got = driver.zrandmember("z", count=2, withscores=True)
    assert isinstance(got, list)
    assert all(isinstance(item, tuple) and len(item) == 2 for item in got)


@pytest.mark.asyncio
async def test_azrandmember(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0})
    assert await driver.azrandmember("z") == b"a"
    assert await driver.azrandmember("z", count=1) == [b"a"]
    assert await driver.azrandmember("z", count=1, withscores=True) == [(b"a", 1.0)]
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k zrandmember`
Expected: 7 FAIL.

- [ ] **Step 3: Add a `ZRandmember` variant for the async path**

In `crates/redis-rs-py-driver/src/async_bridge.rs`, add:

```rust
    ZRandmember { value: redis::Value, count: Option<i64>, withscores: bool },
```

In `into_py`:

```rust
            RawResult::ZRandmember { value, count, withscores } => {
                crate::commands::zsets::render_zrandmember(py, value, count, withscores)
            }
```

- [ ] **Step 4: Implement ZRANDMEMBER**

Inside `#[pymethods]`, append:

```rust
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
            dispatch_cmd!(&mut conn, cmd)
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
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
```

Outside `#[pymethods]`, add the renderer (mirrors the HRANDFIELD renderer, with score parsing):

```rust
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
            // RESP2: flat [m, s, m, s, ...]; RESP3: nested [[m, s], [m, s], ...]
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
        (_, _) => Ok(PyList::empty(py).into_any().unbind()),
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
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k zrandmember`
Expected: 7 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs crates/redis-rs-py-driver/src/async_bridge.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZRANDMEMBER with count and withscores"
```

---

## Task 12: ZSCAN

Sub-task (j). Cursor-based iteration. Returns `(cursor, list[tuple[bytes, float]])`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (j) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZSCAN --------------------------------------------------------------

def test_zscan_full_iteration(driver) -> None:
    expected = {f"m{i}".encode(): float(i) for i in range(20)}
    driver.zadd("z", mapping={k.decode(): v for k, v in expected.items()})

    seen: dict[bytes, float] = {}
    cursor = 0
    while True:
        cursor, batch = driver.zscan("z", cursor=cursor)
        assert isinstance(batch, list)
        assert all(isinstance(p, tuple) and len(p) == 2 for p in batch)
        for member, score in batch:
            seen[member] = score
        if cursor == 0:
            break
    assert seen == expected


def test_zscan_with_match(driver) -> None:
    driver.zadd("z", mapping={"foo:1": 1.0, "foo:2": 2.0, "bar:1": 3.0})
    cursor = 0
    seen: dict[bytes, float] = {}
    while True:
        cursor, batch = driver.zscan("z", cursor=cursor, match="foo:*")
        for m, s in batch:
            seen[m] = s
        if cursor == 0:
            break
    assert seen == {b"foo:1": 1.0, b"foo:2": 2.0}


def test_zscan_with_count(driver) -> None:
    driver.zadd("z", mapping={f"k{i}": float(i) for i in range(40)})
    cursor, batch = driver.zscan("z", cursor=0, count=10)
    seen: dict[bytes, float] = dict(batch)
    while cursor != 0:
        cursor, batch = driver.zscan("z", cursor=cursor, count=10)
        seen.update(batch)
    assert len(seen) == 40


@pytest.mark.asyncio
async def test_azscan(driver) -> None:
    await driver.azadd("z", mapping={"a": 1.0, "b": 2.0})
    cursor, batch = await driver.azscan("z", cursor=0)
    seen = dict(batch)
    assert seen == {b"a": 1.0, b"b": 2.0}
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k zscan`
Expected: 4 FAIL.

- [ ] **Step 3: Add a `ZScan` variant**

In `crates/redis-rs-py-driver/src/async_bridge.rs`, add:

```rust
    ZScan { cursor: u64, items: Vec<(Vec<u8>, f64)> },
```

In `into_py`:

```rust
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
```

- [ ] **Step 4: Implement ZSCAN**

Inside `#[pymethods]`, append:

```rust
    // =====================================================================
    // (j) ZSCAN
    // =====================================================================

    #[pyo3(signature = (key, *, cursor=0, match=None, count=None))]
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
            if let Some(p) = &r#match {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            dispatch_cmd!(&mut conn, cmd)
        });
        let value = r.map_err(to_py_err)?;
        let (cursor, items) = parse_zscan_reply(value)?;
        let cursor_py = cursor.into_pyobject(py)?.into_any().unbind();
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

    #[pyo3(signature = (key, *, cursor=0, match=None, count=None))]
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
            if let Some(p) = &r#match {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => match parse_zscan_reply(v) {
                    Ok((cursor, items)) => RawResult::ZScan { cursor, items },
                    Err(e) => RawResult::Error(
                        ExceptionClass::ResponseError,
                        e.to_string(),
                    ),
                },
                Err(e) => crate::errors::classify(e),
            }
        })
    }
```

Outside `#[pymethods]`, add the parser:

```rust
fn parse_zscan_reply(value: redis::Value) -> PyResult<(u64, Vec<(Vec<u8>, f64)>)> {
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
```

- [ ] **Step 5: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k zscan`
Expected: 4 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs crates/redis-rs-py-driver/src/async_bridge.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZSCAN with MATCH/COUNT"
```

---

## Task 13: ZUNION / ZINTER / ZDIFF + STORE variants + ZINTERCARD

Sub-task (k). Multi-key set algebra over sorted sets. The read variants accept variadic `keys=`, optional `weights=` (one float per key), `aggregate=` (`SUM`/`MIN`/`MAX`), and `withscores=`. The STORE variants take a destination key first. `ZINTERCARD` is a count-only variant with optional `limit=`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/zsets.rs`
- Modify: `tests/driver/test_commands_zsets.py`

- [ ] **Step 1: Append the (k) tests**

Append to `tests/driver/test_commands_zsets.py`:

```python
# --- ZUNION / ZINTER / ZDIFF (read) -------------------------------------

def test_zunion_basic(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 3.0, "z": 4.0})
    got = driver.zunion(keys=["a", "b"])
    assert sorted(got) == [b"x", b"y", b"z"]


def test_zunion_with_scores(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 3.0, "z": 4.0})
    got = driver.zunion(keys=["a", "b"], withscores=True)
    # Default aggregate is SUM: y=2+3=5
    as_dict = dict(got)
    assert as_dict[b"y"] == 5.0


def test_zunion_with_weights_and_aggregate_max(driver) -> None:
    driver.zadd("a", mapping={"y": 1.0})
    driver.zadd("b", mapping={"y": 2.0})
    got = driver.zunion(
        keys=["a", "b"], weights=[10.0, 1.0], aggregate="MAX", withscores=True
    )
    # weights → a:y=10, b:y=2; MAX → 10
    assert got == [(b"y", 10.0)]


def test_zinter_basic(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 3.0, "z": 4.0})
    assert driver.zinter(keys=["a", "b"]) == [b"y"]


def test_zinter_with_scores(driver) -> None:
    driver.zadd("a", mapping={"y": 2.0})
    driver.zadd("b", mapping={"y": 3.0})
    assert driver.zinter(keys=["a", "b"], withscores=True) == [(b"y", 5.0)]


def test_zdiff_basic(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 1.0})
    assert driver.zdiff(keys=["a", "b"]) == [b"x"]


def test_zdiff_with_scores(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0})
    driver.zadd("b", mapping={"y": 1.0})
    assert driver.zdiff(keys=["a", "b"], withscores=True) == [(b"x", 1.0)]


def test_zunion_weights_count_mismatch_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="weights"):
        driver.zunion(keys=["a", "b"], weights=[1.0])


def test_zunion_invalid_aggregate_raises(driver) -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError, match="AGGREGATE"):
        driver.zunion(keys=["a"], aggregate="AVERAGE")


@pytest.mark.asyncio
async def test_azunion(driver) -> None:
    await driver.azadd("a", mapping={"x": 1.0})
    await driver.azadd("b", mapping={"x": 2.0})
    got = await driver.azunion(keys=["a", "b"], withscores=True)
    assert got == [(b"x", 3.0)]


# --- ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE -----------------------------

def test_zunionstore(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 3.0, "z": 4.0})
    n = driver.zunionstore("dst", keys=["a", "b"])
    assert n == 3
    assert driver.zscore("dst", b"y") == 5.0


def test_zinterstore_with_weights(driver) -> None:
    driver.zadd("a", mapping={"y": 1.0})
    driver.zadd("b", mapping={"y": 2.0})
    n = driver.zinterstore("dst", keys=["a", "b"], weights=[2.0, 1.0])
    assert n == 1
    # 1*2 + 2*1 = 4
    assert driver.zscore("dst", b"y") == 4.0


def test_zdiffstore(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0})
    driver.zadd("b", mapping={"y": 1.0})
    n = driver.zdiffstore("dst", keys=["a", "b"])
    assert n == 1


@pytest.mark.asyncio
async def test_azunionstore(driver) -> None:
    await driver.azadd("a", mapping={"x": 1.0})
    assert await driver.azunionstore("dst", keys=["a"]) == 1


# --- ZINTERCARD ---------------------------------------------------------

def test_zintercard(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0, "z": 3.0})
    driver.zadd("b", mapping={"y": 1.0, "z": 1.0, "w": 1.0})
    assert driver.zintercard(keys=["a", "b"]) == 2


def test_zintercard_with_limit(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 2.0, "z": 3.0})
    driver.zadd("b", mapping={"x": 1.0, "y": 1.0, "z": 1.0})
    assert driver.zintercard(keys=["a", "b"], limit=2) == 2


def test_zintercard_limit_zero_unlimited(driver) -> None:
    driver.zadd("a", mapping={"x": 1.0, "y": 1.0})
    driver.zadd("b", mapping={"x": 1.0, "y": 1.0})
    assert driver.zintercard(keys=["a", "b"], limit=0) == 2


@pytest.mark.asyncio
async def test_azintercard(driver) -> None:
    await driver.azadd("a", mapping={"x": 1.0})
    await driver.azadd("b", mapping={"x": 1.0})
    assert await driver.azintercard(keys=["a", "b"]) == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_zsets.py -v -k "zunion or zinter or zdiff"`
Expected: every new test FAILS.

- [ ] **Step 3: Implement the (k) commands**

Outside `#[pymethods]` in `commands/zsets.rs`, add helpers:

```rust
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
        Some(s) if s == "SUM" => Some("SUM"),
        Some(s) if s == "MIN" => Some("MIN"),
        Some(s) if s == "MAX" => Some("MAX"),
        Some(_) => {
            return Err(PyErr::new::<DataError, _>(
                "AGGREGATE must be one of SUM, MIN, MAX",
            ))
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
```

Inside `#[pymethods]`, append the eleven methods (read + store + intercard for union/inter/diff and intercard):

```rust
    // =====================================================================
    // (k) ZUNION / ZINTER / ZDIFF + STORE + ZINTERCARD
    // =====================================================================

    // ZDIFF doesn't support WEIGHTS or AGGREGATE per Redis docs, but accept
    // them in the signature for shape parity and ignore-with-error if set.
    // (Tests don't pass them; users following redis-py's signature won't either.)

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
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
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
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            }
        })
    }

    #[pyo3(signature = (*, keys, withscores = false))]
    fn zdiff(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
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
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            render_scored_members(py, r.map_err(to_py_err)?)
        } else {
            let r: redis::RedisResult<Vec<Vec<u8>>> =
                sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
            py_bytes_list(py, r.map_err(to_py_err)?)
        }
    }

    #[pyo3(signature = (*, keys, withscores = false))]
    fn azdiff(
        &self,
        py: Python<'_>,
        keys: Vec<String>,
        withscores: bool,
    ) -> PyResult<Py<PyAny>> {
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
                let r: redis::RedisResult<Vec<(Vec<u8>, f64)>> = dispatch_cmd!(&mut conn, cmd);
                r.into_raw_result()
            } else {
                let r: redis::RedisResult<Vec<Vec<u8>>> = dispatch_cmd!(&mut conn, cmd);
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
        let cmd = build_zset_op_cmd(
            "ZUNIONSTORE",
            &[destination],
            &keys,
            &weights,
            agg,
            false,
        );
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
        let cmd = build_zset_op_cmd(
            "ZUNIONSTORE",
            &[destination],
            &keys,
            &weights,
            agg,
            false,
        );
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
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
        let cmd = build_zset_op_cmd(
            "ZINTERSTORE",
            &[destination],
            &keys,
            &weights,
            agg,
            false,
        );
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
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
        let cmd = build_zset_op_cmd(
            "ZINTERSTORE",
            &[destination],
            &keys,
            &weights,
            agg,
            false,
        );
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
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
            dispatch_cmd!(&mut conn, cmd)
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
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
            dispatch_cmd!(&mut conn, cmd)
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_zsets.py -v -k "zunion or zinter or zdiff"`
Expected: 19 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/zsets.rs tests/driver/test_commands_zsets.py
git commit -m "feat(zsets): add ZUNION/ZINTER/ZDIFF families plus ZINTERCARD"
```

---

## Task 14: Update `_driver.pyi` stubs for every zset command

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Append the zset-command stubs**

Append to the `class RedisRsDriver:` block in `python/redis_rs_py/_driver.pyi`:

```python
    # --- Sorted sets (Plan 07) -------------------------------------------
    def zadd(
        self,
        key: str,
        *,
        mapping: dict[str | bytes, float],
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
        ch: bool = ...,
        incr: bool = ...,
    ) -> int | float | None: ...
    def azadd(
        self,
        key: str,
        *,
        mapping: dict[str | bytes, float],
        nx: bool = ...,
        xx: bool = ...,
        gt: bool = ...,
        lt: bool = ...,
        ch: bool = ...,
        incr: bool = ...,
    ) -> Awaitable[int | float | None]: ...
    def zrem(self, key: str, *members: bytes) -> int: ...
    def azrem(self, key: str, *members: bytes) -> Awaitable[int]: ...
    def zrange(
        self,
        key: str,
        start: int | str | bytes,
        stop: int | str | bytes,
        *,
        desc: bool = ...,
        byscore: bool = ...,
        bylex: bool = ...,
        withscores: bool = ...,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> list[bytes] | list[tuple[bytes, float]]: ...
    def azrange(
        self,
        key: str,
        start: int | str | bytes,
        stop: int | str | bytes,
        *,
        desc: bool = ...,
        byscore: bool = ...,
        bylex: bool = ...,
        withscores: bool = ...,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> Awaitable[list[bytes] | list[tuple[bytes, float]]]: ...
    def zrangestore(
        self,
        destination: str,
        source: str,
        start: int | str | bytes,
        stop: int | str | bytes,
        *,
        desc: bool = ...,
        byscore: bool = ...,
        bylex: bool = ...,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> int: ...
    def azrangestore(
        self,
        destination: str,
        source: str,
        start: int | str | bytes,
        stop: int | str | bytes,
        *,
        desc: bool = ...,
        byscore: bool = ...,
        bylex: bool = ...,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> Awaitable[int]: ...
    def zrangebyscore(
        self,
        key: str,
        min: str,
        max: str,
        *,
        withscores: bool = ...,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> list[bytes] | list[tuple[bytes, float]]: ...
    def azrangebyscore(
        self,
        key: str,
        min: str,
        max: str,
        *,
        withscores: bool = ...,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> Awaitable[list[bytes] | list[tuple[bytes, float]]]: ...
    def zrevrangebyscore(
        self,
        key: str,
        max: str,
        min: str,
        *,
        withscores: bool = ...,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> list[bytes] | list[tuple[bytes, float]]: ...
    def azrevrangebyscore(
        self,
        key: str,
        max: str,
        min: str,
        *,
        withscores: bool = ...,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> Awaitable[list[bytes] | list[tuple[bytes, float]]]: ...
    def zrangebylex(
        self,
        key: str,
        min: str,
        max: str,
        *,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> list[bytes]: ...
    def azrangebylex(
        self,
        key: str,
        min: str,
        max: str,
        *,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> Awaitable[list[bytes]]: ...
    def zrevrangebylex(
        self,
        key: str,
        max: str,
        min: str,
        *,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> list[bytes]: ...
    def azrevrangebylex(
        self,
        key: str,
        max: str,
        min: str,
        *,
        offset: int | None = ...,
        num: int | None = ...,
    ) -> Awaitable[list[bytes]]: ...
    def zincrby(self, key: str, amount: float, member: bytes) -> float: ...
    def azincrby(
        self, key: str, amount: float, member: bytes
    ) -> Awaitable[float]: ...
    def zcard(self, key: str) -> int: ...
    def azcard(self, key: str) -> Awaitable[int]: ...
    def zscore(self, key: str, member: bytes) -> float | None: ...
    def azscore(self, key: str, member: bytes) -> Awaitable[float | None]: ...
    def zmscore(self, key: str, *members: bytes) -> list[float | None]: ...
    def azmscore(
        self, key: str, *members: bytes
    ) -> Awaitable[list[float | None]]: ...
    def zrank(
        self, key: str, member: bytes, *, withscore: bool = ...
    ) -> int | tuple[int, float] | None: ...
    def azrank(
        self, key: str, member: bytes, *, withscore: bool = ...
    ) -> Awaitable[int | tuple[int, float] | None]: ...
    def zrevrank(
        self, key: str, member: bytes, *, withscore: bool = ...
    ) -> int | tuple[int, float] | None: ...
    def azrevrank(
        self, key: str, member: bytes, *, withscore: bool = ...
    ) -> Awaitable[int | tuple[int, float] | None]: ...
    def zremrangebyrank(self, key: str, start: int, stop: int) -> int: ...
    def azremrangebyrank(
        self, key: str, start: int, stop: int
    ) -> Awaitable[int]: ...
    def zremrangebyscore(self, key: str, min: str, max: str) -> int: ...
    def azremrangebyscore(
        self, key: str, min: str, max: str
    ) -> Awaitable[int]: ...
    def zremrangebylex(self, key: str, min: str, max: str) -> int: ...
    def azremrangebylex(self, key: str, min: str, max: str) -> Awaitable[int]: ...
    def zcount(self, key: str, min: str, max: str) -> int: ...
    def azcount(self, key: str, min: str, max: str) -> Awaitable[int]: ...
    def zlexcount(self, key: str, min: str, max: str) -> int: ...
    def azlexcount(self, key: str, min: str, max: str) -> Awaitable[int]: ...
    def zpopmin(
        self, key: str, *, count: int = ...
    ) -> list[tuple[bytes, float]]: ...
    def azpopmin(
        self, key: str, *, count: int = ...
    ) -> Awaitable[list[tuple[bytes, float]]]: ...
    def zpopmax(
        self, key: str, *, count: int = ...
    ) -> list[tuple[bytes, float]]: ...
    def azpopmax(
        self, key: str, *, count: int = ...
    ) -> Awaitable[list[tuple[bytes, float]]]: ...
    def zmpop(
        self, *keys: str, direction: str, count: int = ...
    ) -> tuple[str, list[tuple[bytes, float]]] | None: ...
    def azmpop(
        self, *keys: str, direction: str, count: int = ...
    ) -> Awaitable[tuple[str, list[tuple[bytes, float]]] | None]: ...
    def bzpopmin(
        self, *keys: str, timeout: float
    ) -> tuple[bytes, bytes, float] | None: ...
    def abzpopmin(
        self, *keys: str, timeout: float
    ) -> Awaitable[tuple[bytes, bytes, float] | None]: ...
    def bzpopmax(
        self, *keys: str, timeout: float
    ) -> tuple[bytes, bytes, float] | None: ...
    def abzpopmax(
        self, *keys: str, timeout: float
    ) -> Awaitable[tuple[bytes, bytes, float] | None]: ...
    def bzmpop(
        self, *keys: str, direction: str, timeout: float, count: int = ...
    ) -> tuple[str, list[tuple[bytes, float]]] | None: ...
    def abzmpop(
        self, *keys: str, direction: str, timeout: float, count: int = ...
    ) -> Awaitable[tuple[str, list[tuple[bytes, float]]] | None]: ...
    def zrandmember(
        self, key: str, count: int | None = ..., withscores: bool = ...
    ) -> bytes | list[bytes] | list[tuple[bytes, float]] | None: ...
    def azrandmember(
        self, key: str, count: int | None = ..., withscores: bool = ...
    ) -> Awaitable[
        bytes | list[bytes] | list[tuple[bytes, float]] | None
    ]: ...
    def zscan(
        self,
        key: str,
        *,
        cursor: int = ...,
        match: str | None = ...,
        count: int | None = ...,
    ) -> tuple[int, list[tuple[bytes, float]]]: ...
    def azscan(
        self,
        key: str,
        *,
        cursor: int = ...,
        match: str | None = ...,
        count: int | None = ...,
    ) -> Awaitable[tuple[int, list[tuple[bytes, float]]]]: ...
    def zunion(
        self,
        *,
        keys: list[str],
        weights: list[float] | None = ...,
        aggregate: str | None = ...,
        withscores: bool = ...,
    ) -> list[bytes] | list[tuple[bytes, float]]: ...
    def azunion(
        self,
        *,
        keys: list[str],
        weights: list[float] | None = ...,
        aggregate: str | None = ...,
        withscores: bool = ...,
    ) -> Awaitable[list[bytes] | list[tuple[bytes, float]]]: ...
    def zinter(
        self,
        *,
        keys: list[str],
        weights: list[float] | None = ...,
        aggregate: str | None = ...,
        withscores: bool = ...,
    ) -> list[bytes] | list[tuple[bytes, float]]: ...
    def azinter(
        self,
        *,
        keys: list[str],
        weights: list[float] | None = ...,
        aggregate: str | None = ...,
        withscores: bool = ...,
    ) -> Awaitable[list[bytes] | list[tuple[bytes, float]]]: ...
    def zdiff(
        self, *, keys: list[str], withscores: bool = ...
    ) -> list[bytes] | list[tuple[bytes, float]]: ...
    def azdiff(
        self, *, keys: list[str], withscores: bool = ...
    ) -> Awaitable[list[bytes] | list[tuple[bytes, float]]]: ...
    def zunionstore(
        self,
        destination: str,
        *,
        keys: list[str],
        weights: list[float] | None = ...,
        aggregate: str | None = ...,
    ) -> int: ...
    def azunionstore(
        self,
        destination: str,
        *,
        keys: list[str],
        weights: list[float] | None = ...,
        aggregate: str | None = ...,
    ) -> Awaitable[int]: ...
    def zinterstore(
        self,
        destination: str,
        *,
        keys: list[str],
        weights: list[float] | None = ...,
        aggregate: str | None = ...,
    ) -> int: ...
    def azinterstore(
        self,
        destination: str,
        *,
        keys: list[str],
        weights: list[float] | None = ...,
        aggregate: str | None = ...,
    ) -> Awaitable[int]: ...
    def zdiffstore(self, destination: str, *, keys: list[str]) -> int: ...
    def azdiffstore(
        self, destination: str, *, keys: list[str]
    ) -> Awaitable[int]: ...
    def zintercard(
        self, *, keys: list[str], limit: int | None = ...
    ) -> int: ...
    def azintercard(
        self, *, keys: list[str], limit: int | None = ...
    ) -> Awaitable[int]: ...
```

- [ ] **Step 2: Run ty + ruff**

```bash
uv run ty check python/redis_rs_py/
uv run ruff check
uv run ruff format --check
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add python/redis_rs_py/_driver.pyi
git commit -m "feat(zsets): add type stubs for all sorted-set commands"
```

---

## Task 15: Final lint pass + free-threaded smoke + CHANGELOG

**Files:** none modified — verification + CHANGELOG.

- [ ] **Step 1: Run linters**

```bash
uv run ruff check
uv run ruff format --check
uv run ty check python/redis_rs_py/
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: all green.

- [ ] **Step 2: Run the full zsets test file**

```bash
uv run pytest tests/driver/test_commands_zsets.py -v
```

Expected: every test PASSES — count should be 100+ across all sub-tasks.

- [ ] **Step 3: Run the suite under cp314t**

```bash
.venv-ft/bin/uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
.venv-ft/bin/uv run pytest tests/driver/test_commands_zsets.py -n auto
```

Expected: same green.

- [ ] **Step 4: Add CHANGELOG entry**

Append under `### Added` in `CHANGELOG.md`:

```markdown
- Sorted-set commands: `ZADD` (full `NX`/`XX`/`GT`/`LT`/`CH`/`INCR` flag matrix; `INCR` returns `float|None`), `ZREM`, `ZRANGE` (with `desc`/`byscore`/`bylex`/`withscores`/`offset`/`num`), `ZRANGESTORE`, `ZRANGEBYSCORE`/`ZREVRANGEBYSCORE`/`ZRANGEBYLEX`/`ZREVRANGEBYLEX` (all with `LIMIT offset count`), `ZINCRBY`, `ZCARD`, `ZSCORE` (returns `float|None`), `ZMSCORE` (returns `list[float|None]`), `ZRANK`/`ZREVRANK` with `withscore=` (Redis 7.2+), `ZREMRANGEBYRANK`/`BYSCORE`/`BYLEX`, `ZCOUNT`, `ZLEXCOUNT`, `ZPOPMIN`/`ZPOPMAX` (with `count=`), `ZMPOP`, `BZPOPMIN`/`BZPOPMAX` (returns `(key, member, score)|None`), `BZMPOP`, `ZRANDMEMBER` (with `count=`/`withscores=`), `ZSCAN`, `ZUNION`/`ZINTER`/`ZDIFF` (with `keys=`/`weights=`/`aggregate=`/`withscores=`), `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE`, `ZINTERCARD` (with `limit=`).
- WITHSCORES return shape standardized as `list[tuple[bytes, float]]` across every command that supports it.
- Blocking sorted-set commands (`BZPOPMIN`/`BZPOPMAX`/`BZMPOP`) use the lazy blocking connection from Plan 04.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 07 entry"
```

---

## Self-review checklist for this plan

- [x] Spec coverage — every command in the assignment block has a sub-task: ZADD (full NX/XX/GT/LT/CH/INCR matrix), ZREM (variadic), ZRANGE (desc/byscore/bylex/withscores/offset/num/score_cast_func), ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANGEBYLEX/ZREVRANGEBYLEX (all with LIMIT), ZRANGESTORE, ZINCRBY, ZCARD, ZSCORE (float|None), ZMSCORE (list[float|None]), ZRANK/ZREVRANK with WITHSCORE, ZREMRANGEBYRANK/BYSCORE/BYLEX, ZCOUNT, ZLEXCOUNT, ZPOPMIN/ZPOPMAX (with count), ZMPOP, BZPOPMIN/BZPOPMAX, BZMPOP, ZRANDMEMBER (with count/withscores), ZSCAN, ZUNION/ZINTER/ZDIFF (with keys/aggregate/weights/withscores), ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE, ZINTERCARD (with limit).
- [x] No placeholders: every step ships actual code, every test step ships an explicit pass/fail expectation.
- [x] Type consistency: Rust signatures, `.pyi` stubs, and test usage all line up. `withscores=True` everywhere returns `list[tuple[bytes, float]]`; ZADD INCR returns `float|None`; ZRANK with `withscore=True` returns `tuple[int, float]|None`.
- [x] WITHSCORES return shape (`list[tuple[bytes, float]]`) prominently documented in the architecture header AND consistently surfaced via `RawResult::ScoredMembers` / `render_scored_members`.
- [x] ZADD flag matrix: NX/XX mutual exclusion, GT/LT mutual exclusion, NX with GT/LT forbidden, INCR requires single pair — all four conditions tested and validated by `validate_zadd_flags`.
- [x] ZRANGE option matrix: BYSCORE/BYLEX mutual exclusion, LIMIT requires BYSCORE or BYLEX, WITHSCORES not allowed with BYLEX — all three validated by `build_zrange_cmd`.
- [x] BZPOPMIN/BZPOPMAX/BZMPOP correctly route through `ValkeyConn::get_blocking().await` (the lazy second connection from Plan 04) — no head-of-line-blocking on the multiplexed pipeline.
- [x] Sub-task grouping matches the assignment: (a) ZADD, (b) ZRANGE family + ZRANGESTORE, (c) ZRANGEBYSCORE/LEX + REV, (d) ZINCRBY/ZCARD/ZSCORE/ZMSCORE, (e) ZRANK/ZREVRANK + WITHSCORE, (f) ZREMRANGEBY*, (g) ZCOUNT/ZLEXCOUNT, (h) ZPOPMIN/MAX + ZMPOP + BZPOPMIN/MAX + BZMPOP, (i) ZRANDMEMBER, (j) ZSCAN, (k) ZUNION/INTER/DIFF + STORE + ZINTERCARD.
- [x] All file paths absolute or repo-relative-from-root.
- [x] Frequent commits: 15 across 15 tasks, each independently revertable, each with conventional-commit `feat(zsets):` / `docs(changelog):` prefixes.
- [x] `commands/zsets.rs` module path declared from `commands/mod.rs::pub mod zsets;`.
- [x] Out-of-scope items deferred to later plans: façade bindings (10), pipelines (13), `decode_responses=True` (12), `score_cast_func=` Python parameter — we always return `float`; the façade in plan 10 applies the user-supplied cast function.
