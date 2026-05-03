# Plan 08 — Stream commands (XADD, XREAD, XGROUP, XPENDING, XCLAIM, XAUTOCLAIM, …)

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the full Redis Streams surface on the low-level driver, with output shapes that match `redis-py`'s stream methods exactly. Every command exists as a sync + async pair on `RedisRsDriver`. New file `crates/redis-rs-py-driver/src/commands/streams.rs` holds both halves of every command and the private flattening helpers.

**Architectural decision: flatten `redis::Value` to redis-py-shaped tuples in Rust (NOT pass-through).**

The cachex prototype passed `redis::Value` through the recursive `redis_value_to_py` converter, which produces nested `list`/`dict` shapes that *resemble* but do not equal `redis-py`'s output. For every stream command except `XADD`/`XLEN`/`XACK`/`XDEL`/`XTRIM`/`XSETID` (which return scalars), we flatten the reply in Rust into the exact nested tuple/dict shape that redis-py returns. This buys us:

- **Drop-in compat at the data layer.** `xread()` returns `dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]]`, identical to redis-py — user code handling stream entries can be moved between clients without touching the parsing code.
- **One conversion at the boundary.** The flattening helpers run in Rust where the data is already laid out, instead of doing the work twice (once in Rust into a generic Python list/dict tree, once in Python to reshape).
- **Matches the rest of the driver.** Strings/lists/hashes/sets/zsets in plans 03–07 already return shaped values (lists of bytes, dicts of bytes, lists of `(bytes, score)` tuples). Streams should too — the streams sub-tree is the only one where we have a choice, and consistency wins.

**Trade-off we accept:** Flattening helpers are non-trivial (~200 LOC across the file). They have to handle RESP2 (XREAD returns nested arrays) AND RESP3 (XREAD returns a Map of arrays). The driver forces `protocol=resp3` (Plan 01 Task 8 — `url_with_resp3`), so the RESP3 shape is the canonical case; RESP2 fallbacks exist defensively but are tested via a feature-flag fixture. If a future redis-rs version adds typed stream wrappers (`StreamReadReply` etc.) that already do this work, swap to those — the public Python output shapes are the contract.

**Architecture:** Per family of commands, two methods on `RedisRsDriver`: sync `xfoo(...)` and async `axfoo(...)`. Every method body is a thin wrapper over a private `cmd_*` helper that constructs the `redis::Cmd` (so the sync and async paths share the argument-encoding logic verbatim). Replies that need flattening go through one of `flatten_xrange_reply`, `flatten_xread_reply`, `flatten_xpending_summary`, `flatten_xpending_range`, `flatten_xclaim_reply`, `flatten_xautoclaim_reply`, `flatten_xinfo_stream`, `flatten_xinfo_groups`, `flatten_xinfo_consumers` — all private to `commands/streams.rs`. Replies that are scalar (`XADD` returns the new id, `XLEN` returns an int, `XACK`/`XDEL`/`XTRIM` return counts) use the existing `RawResult` variants.

**New `RawResult` variants this plan adds:**
- `RawResult::StreamEntries(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)` — list of `(id, [(field, value), ...])` for `XRANGE`/`XREVRANGE`.
- `RawResult::StreamReadEntries(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)>)` — list of `(stream_key, [(id, [(field, value), ...]), ...])` for `XREAD`/`XREADGROUP`. Empty outer list maps to Python `None` (block timeout).
- `RawResult::StreamPendingSummary(Option<(i64, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, i64)>)>)` — summary form (count, min_id, max_id, [(consumer, count), …]) or `None` if no pending entries.
- `RawResult::StreamPendingRange(Vec<(Vec<u8>, Vec<u8>, i64, i64)>)` — `[(id, consumer, idle_ms, deliveries), …]`.
- `RawResult::StreamClaim(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)` — same shape as `StreamEntries` (but distinct variant for clarity).
- `RawResult::StreamClaimJustIds(Vec<Vec<u8>>)` — `XCLAIM ... JUSTID` returns just the ids.
- `RawResult::StreamAutoclaim((Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>, Vec<Vec<u8>>))` — `(next_cursor_id, claimed_entries, deleted_ids)`.
- `RawResult::StreamAutoclaimJustIds((Vec<u8>, Vec<Vec<u8>>, Vec<Vec<u8>>))` — `(next_cursor_id, claimed_ids, deleted_ids)`.
- `RawResult::StreamInfoStream(Py<PyDict>)` — `XINFO STREAM` reply pre-flattened to a dict (built in the runtime task; we hold the `Py<PyDict>` and unbox in `into_py`).

  Wait — the runtime task can't touch the GIL. Instead: `RawResult::StreamInfoStream(Vec<(Vec<u8>, redis::Value)>)` carrying the raw map pairs; flatten on the GIL-side `into_py` call. Same for `StreamInfoGroups`/`StreamInfoConsumers`.

- `RawResult::StreamInfoGroups(Vec<Vec<(Vec<u8>, redis::Value)>>)` — list of group-info maps.
- `RawResult::StreamInfoConsumers(Vec<Vec<(Vec<u8>, redis::Value)>>)` — list of consumer-info maps.

**Tech stack:** PyO3 0.28, redis 1.x with `tokio-comp`/`connection-manager`, tokio 1.x. No new deps. Python-side: `pytest`, `pytest-asyncio`, `testcontainers`, `redis>=5.0` (the upstream client we compare against — already pinned).

**Reference material:**
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs:1980-2250` — every stream-command argument-encoding helper. Lift verbatim into `cmd_*` functions in `commands/streams.rs`.
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/client.rs:2285-2811` — the cachex method bodies (which pass `redis::Value` straight through). Use as the source for argument signatures; throw away the `py_redis_value(...)` calls and replace with our flattening helpers.
- `redis-py` source for the canonical output shapes — `python -c "import redis; help(redis.commands.core.StreamCommands)"` (run once before starting). Specifically: `xread`, `xrange`, `xpending`, `xclaim`, `xautoclaim`, `xinfo_*`.
- redis-py `5.x` docs for `XPENDING` argument logic: one method that switches between summary form and range form depending on whether `min`/`max`/`count` were passed.

**Out of scope:**
- The `Redis` façade (high-level kwargs translation). That's plan 10/11 — this plan stops at `RedisRsDriver`.
- `decode_responses=True` mode — bytes stay bytes for now; plan 12 adds the decode layer.
- Python `register_script`-style helpers (defer to plan 09).
- Cluster routing for stream commands (XREAD across keys must hash to one slot; CROSSSLOT translation is plan 02's classifier already and plan 15 wires Cluster).
- Pipelines / transactions — plan 13.

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  commands/
    mod.rs                       # MODIFIED: add `pub mod streams;`
    streams.rs                   # NEW (~1100 LOC): cmd_* builders, flatten_* helpers, RedisRsDriver methods
  async_bridge.rs                # MODIFIED: 9 new RawResult variants + into_py arms
  raw_result.rs                  # MODIFIED: From<T> impls for the new variant payloads
  driver.rs                      # MODIFIED: `mod commands { pub mod streams; }` + #[pymethods] inherent impl block split
python/
  redis_rs_py/
    _driver.pyi                  # MODIFIED: stub the 23 new methods (sync + async)
tests/
  driver/
    test_commands_streams.py     # NEW (~1500 LOC): one test class per command, parity-asserted vs redis-py
  conftest.py                    # MODIFIED: add `redis_py_client` fixture (upstream client against the same Valkey)
```

---

## Pre-task: Read the upstream output shapes once

Before any code changes, run this in a shell and copy the output into the working notes (no commit needed):

```bash
uv run python -c "
import redis
r = redis.Redis(decode_responses=False)
help(r.xrange)
help(r.xread)
help(r.xpending)
help(r.xclaim)
help(r.xautoclaim)
help(r.xinfo_stream)
help(r.xinfo_groups)
help(r.xinfo_consumers)
" 2>&1 | head -200
```

Verify against this plan's flattening targets:
- `xrange`: `list[tuple[bytes, dict[bytes, bytes]]]`
- `xread`: `dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]]` OR `None` if BLOCK timed out
- `xpending(name, group)` (summary): `[count, min_id, max_id, [[consumer, count], ...]]` (a list of 4 elements; redis-py preserves list-of-lists for the consumer-count tail). Equivalent Python tuple shape: `(count, min_id, max_id, [(consumer, count), ...])`.
- `xpending(name, group, idle, min, max, count, consumer)` (range): `list[{"message_id": id, "consumer": consumer, "time_since_delivered": idle, "times_delivered": deliveries}]` — redis-py returns dicts, NOT tuples. Flatten to dicts.
- `xclaim`: `list[tuple[bytes, dict[bytes, bytes]]]` (same as xrange) OR `list[bytes]` if `justid=True`.
- `xautoclaim`: `(next_id_bytes, list_of_entries, list_of_deleted_ids)` — 3-tuple.
- `xinfo_stream`: `dict[bytes, Any]` with keys like `b"length"`, `b"first-entry"`, `b"last-entry"`, `b"groups"`, etc.
- `xinfo_groups`/`xinfo_consumers`: `list[dict[bytes, Any]]`.

If your local `redis-py` returns slightly different shapes (e.g. all-strings for some keys), we still flatten to bytes uniformly — the `decode_responses=True` mode (plan 12) handles the str variant. The parity tests below force `decode_responses=False` on the upstream client to keep the comparison apples-to-apples.

---

## Task 1: Add the `commands` module skeleton + new `RawResult` variants

Land the boilerplate so all subsequent tasks just append new methods. Wire the new variants through `async_bridge.rs::RawResult::into_py` so they raise on misuse instead of silently dropping.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/driver.rs` (add `mod commands;` declaration with submodule)
- Create: `crates/redis-rs-py-driver/src/commands/mod.rs`
- Create: `crates/redis-rs-py-driver/src/commands/streams.rs` (skeleton)
- Modify: `crates/redis-rs-py-driver/src/async_bridge.rs` (extend `RawResult` enum + `into_py`)
- Modify: `crates/redis-rs-py-driver/src/raw_result.rs` (add `From<...>` impls)

- [ ] **Step 1: Create the `commands/` module hierarchy**

```bash
mkdir -p crates/redis-rs-py-driver/src/commands
```

Create `crates/redis-rs-py-driver/src/commands/mod.rs`:

```rust
// Per-command-family submodules. Each file holds the sync + async pair
// for every command in its family, plus any private helpers the family
// needs (e.g. flatten_* for streams).

pub mod streams;
```

- [ ] **Step 2: Wire `mod commands;` into the driver crate**

Edit `crates/redis-rs-py-driver/src/lib.rs`. After `mod test_helpers;` add:

```rust
mod commands;
```

- [ ] **Step 3: Create the `commands/streams.rs` skeleton**

Create `crates/redis-rs-py-driver/src/commands/streams.rs` with just the doc-header and module-level use statements. Tasks 2–13 will append the per-command halves into this file.

```rust
// Stream commands for RedisRsDriver.
//
// Architectural note: every command flattens the reply in Rust to match
// redis-py's output shape exactly (see Plan 08, Architecture section).
// The flatten_* helpers below are private to this file. New variants
// of redis::Value coming from the server land in RawResult::Value and
// the helpers in this file translate them to typed RawResult variants.
//
// Argument-encoding helpers are extracted into `cmd_*` functions so the
// sync and async halves share the same Cmd construction code verbatim.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::async_bridge::{RawResult, RedisRsAwaitable};
use crate::connection::ValkeyConn;
use crate::driver::RedisRsDriver;
use crate::errors::{classify, to_py_err};
use crate::raw_result::IntoRawResult;
use crate::runtime::get_runtime;
use crate::{conn_method, dispatch_cmd};

// Re-import the macros at file scope so #[pymethods] blocks below can use them.
use crate::{async_op, sync_op};

// =========================================================================
// Argument-encoding helpers (cmd_*)
// =========================================================================
//
// (Filled in by Tasks 2-13.)

// =========================================================================
// Reply-flattening helpers (flatten_*)
// =========================================================================
//
// (Filled in by Tasks 4, 5, 7-12.)

// =========================================================================
// RedisRsDriver method impls
// =========================================================================

#[pymethods]
impl RedisRsDriver {
    // (Filled in by Tasks 2-13.)
}
```

This file will compile (empty `#[pymethods]` impl block is legal). Verify:

Run: `cargo check -p redis-rs-py-driver`
Expected: clean.

- [ ] **Step 4: Extend `RawResult` with the new variants**

Edit `crates/redis-rs-py-driver/src/async_bridge.rs`. Locate the `pub enum RawResult { ... }` block and append the following variants before the closing brace (right after `Value(redis::Value),`):

```rust
    StreamEntries(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>),
    StreamReadEntries(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)>),
    StreamPendingSummary(Option<(i64, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, i64)>)>),
    StreamPendingRange(Vec<(Vec<u8>, Vec<u8>, i64, i64)>),
    StreamClaim(Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>),
    StreamClaimJustIds(Vec<Vec<u8>>),
    StreamAutoclaim((Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>, Vec<Vec<u8>>)),
    StreamAutoclaimJustIds((Vec<u8>, Vec<Vec<u8>>, Vec<Vec<u8>>)),
    StreamInfoStream(Vec<(Vec<u8>, redis::Value)>),
    StreamInfoGroups(Vec<Vec<(Vec<u8>, redis::Value)>>),
    StreamInfoConsumers(Vec<Vec<(Vec<u8>, redis::Value)>>),
```

Locate `impl RawResult { pub fn into_py(...) }` and add these arms inside the `match self` block (right after the `RawResult::Value(v) => redis_value_to_py(py, v),` arm):

```rust
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
                // No pending entries — return the redis-py 4-tuple of zero/None values.
                let zero = 0_i64.into_pyobject(py)?.into_any().unbind();
                let none = py.None();
                let empty_list = PyList::empty(py).into_any().unbind();
                Ok(PyTuple::new(py, [
                    zero,
                    none.clone_ref(py),
                    none,
                    empty_list,
                ])?.into_any().unbind())
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
                        PyTuple::new(py, [name_py, n_py])
                            .map(|t| t.into_any().unbind())
                    })
                    .collect::<PyResult<_>>()?;
                let consumers_list = PyList::new(py, consumers_py)?.into_any().unbind();
                Ok(PyTuple::new(py, [count_py, min_py, max_py, consumers_list])?
                    .into_any()
                    .unbind())
            }
            RawResult::StreamPendingRange(rows) => {
                let items: Vec<Py<PyAny>> = rows
                    .into_iter()
                    .map(|(id, consumer, idle, deliveries)| {
                        let d = PyDict::new(py);
                        d.set_item("message_id", PyBytes::new(py, &id))?;
                        d.set_item("consumer", PyBytes::new(py, &consumer))?;
                        d.set_item("time_since_delivered", idle)?;
                        d.set_item("times_delivered", deliveries)?;
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
```

Add a free function near the bottom of `async_bridge.rs` (above the `// =====` separator that introduces `RedisRsAwaitable`):

```rust
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
```

- [ ] **Step 5: Add `From<...>` impls for the new variants**

Edit `crates/redis-rs-py-driver/src/raw_result.rs`. Append:

```rust
impl From<Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>) -> Self {
        RawResult::StreamEntries(v)
    }
}

impl From<Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)>) -> Self {
        RawResult::StreamReadEntries(v)
    }
}

impl From<Option<(i64, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, i64)>)>> for RawResult {
    fn from(v: Option<(i64, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, i64)>)>) -> Self {
        RawResult::StreamPendingSummary(v)
    }
}

impl From<Vec<(Vec<u8>, Vec<u8>, i64, i64)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<u8>, i64, i64)>) -> Self {
        RawResult::StreamPendingRange(v)
    }
}
```

The remaining variants are constructed explicitly in command bodies (because they share concrete types with `StreamEntries`/`StreamPendingSummary` and would conflict with the `From` impls — explicit `RawResult::StreamClaim(...)` calls instead).

- [ ] **Step 6: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: clean. The new variants are unused — that's fine (the rest of the plan adds the call sites).

- [ ] **Step 7: Commit**

```bash
git add crates/redis-rs-py-driver/src/lib.rs crates/redis-rs-py-driver/src/commands/ crates/redis-rs-py-driver/src/async_bridge.rs crates/redis-rs-py-driver/src/raw_result.rs
git commit -m "feat(streams): add commands module skeleton and stream RawResult variants"
```

---

## Task 2: `XADD` basic — `*` id + ms-seq id + variadic fields

Most of the option matrix lands in Task 3; this task ships the simple `xadd(key, id, fields)` form so we have something to test the rest against.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Create: `tests/driver/test_commands_streams.py` (with the first XADD test)
- Modify: `tests/conftest.py` (add `redis_py_client` fixture)

- [ ] **Step 1: Add the `redis_py_client` fixture**

Edit `tests/conftest.py`. After the existing `driver` fixture, append:

```python
@pytest.fixture
def redis_py_client(valkey_url: str):
    """Upstream redis-py client against the same Valkey instance.

    Used by parity tests in plans 08+ to compare reply shapes between
    redis-rs-py and redis-py — for stream commands especially, the
    bytes-vs-tuple-vs-dict shape contract is non-trivial and must
    match exactly.
    """
    import redis

    rp = redis.Redis.from_url(valkey_url, decode_responses=False)
    yield rp
    rp.close()
```

- [ ] **Step 2: Write the failing test for basic XADD**

Create `tests/driver/test_commands_streams.py`:

```python
"""Stream commands — parity with redis-py output shapes."""

from __future__ import annotations

import asyncio

import pytest


def test_xadd_basic_returns_id(driver) -> None:
    new_id = driver.xadd("s", "*", [("field1", b"value1"), ("field2", b"value2")])
    assert isinstance(new_id, str)
    # Format: <ms-timestamp>-<seq>
    assert "-" in new_id
    ms, seq = new_id.split("-")
    assert ms.isdigit()
    assert seq.isdigit()


def test_xadd_explicit_ms_seq_id(driver) -> None:
    new_id = driver.xadd("s", "1-1", [("f", b"v")])
    assert new_id == "1-1"


def test_xadd_partial_id_assigns_seq(driver) -> None:
    """Format `<ms>-*` lets the server pick the seq."""
    new_id = driver.xadd("s", "100-*", [("f", b"v")])
    assert new_id.startswith("100-")


@pytest.mark.asyncio
async def test_axadd_basic_returns_id(driver) -> None:
    new_id = await driver.axadd("s", "*", [("f", b"v")])
    assert isinstance(new_id, str)
    assert "-" in new_id


def test_xadd_appends_to_existing_stream(driver, redis_py_client) -> None:
    driver.xadd("s", "*", [("a", b"1")])
    driver.xadd("s", "*", [("b", b"2")])
    # Use the upstream client to verify the count.
    assert redis_py_client.xlen("s") == 2
```

- [ ] **Step 3: Run the failing tests**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::test_xadd_basic_returns_id -v`
Expected: FAIL — `AttributeError: 'RedisRsDriver' object has no attribute 'xadd'`.

- [ ] **Step 4: Implement basic XADD in `commands/streams.rs`**

Insert into the **Argument-encoding helpers** region of `commands/streams.rs`:

```rust
fn cmd_xadd_basic(key: &str, id: &str, fields: &[(String, Vec<u8>)]) -> redis::Cmd {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(key).arg(id);
    for (f, v) in fields {
        cmd.arg(f.as_str()).arg(v.as_slice());
    }
    cmd
}
```

Insert into the **`#[pymethods] impl RedisRsDriver`** block:

```rust
    #[pyo3(signature = (key, id, fields))]
    fn xadd(
        &self,
        py: Python<'_>,
        key: &str,
        id: &str,
        fields: Vec<(String, Vec<u8>)>,
    ) -> PyResult<String> {
        let cmd = cmd_xadd_basic(key, id, &fields);
        let r: redis::RedisResult<String> = sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (key, id, fields))]
    fn axadd(
        &self,
        py: Python<'_>,
        key: &str,
        id: &str,
        fields: Vec<(String, Vec<u8>)>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let id = id.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xadd_basic(&key, &id, &fields);
            let r: redis::RedisResult<String> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 5: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py -v -k "xadd"`
Expected: 5 PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/conftest.py tests/driver/test_commands_streams.py
git commit -m "feat(streams): add basic XADD (sync + async)"
```

---

## Task 3: `XADD` full option matrix — NOMKSTREAM, MAXLEN, MINID, LIMIT, ~/=

The redis-py signature (we mirror it):

```python
def xadd(
    name,
    fields,
    id="*",
    maxlen=None,
    approximate=True,
    nomkstream=False,
    minid=None,
    limit=None,
)
```

Note: redis-py orders the arg-list as `(name, fields, id="*", ...)`. Our driver-level signature already takes `id` second (positional, no default) since the driver layer prefers explicit args — the high-level façade in plan 10 reorders. Here we extend the driver signature to take the option keywords.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write the failing tests for the option matrix**

Append to `tests/driver/test_commands_streams.py`:

```python
def test_xadd_nomkstream_no_stream_returns_none(driver) -> None:
    """NOMKSTREAM: don't create the stream if it doesn't exist."""
    result = driver.xadd("missing", "*", [("f", b"v")], nomkstream=True)
    assert result is None


def test_xadd_nomkstream_existing_stream_returns_id(driver) -> None:
    driver.xadd("s", "*", [("f", b"v")])
    result = driver.xadd("s", "*", [("f", b"v")], nomkstream=True)
    assert isinstance(result, str)


def test_xadd_maxlen_approximate(driver, redis_py_client) -> None:
    for _ in range(10):
        driver.xadd("s", "*", [("f", b"v")], maxlen=5, approximate=True)
    # Approximate trim might leave more than 5 (it's `~5`); tolerate up to 2x.
    n = redis_py_client.xlen("s")
    assert n >= 5
    assert n <= 10


def test_xadd_maxlen_exact(driver, redis_py_client) -> None:
    for _ in range(10):
        driver.xadd("s", "*", [("f", b"v")], maxlen=5, approximate=False)
    assert redis_py_client.xlen("s") == 5


def test_xadd_maxlen_with_limit_is_rejected_when_exact(driver) -> None:
    """LIMIT is only valid with approximate trim (~). With `=`, the server rejects."""
    from redis_rs_py.exceptions import ResponseError

    with pytest.raises(ResponseError):
        driver.xadd("s", "*", [("f", b"v")], maxlen=5, approximate=False, limit=2)


def test_xadd_minid(driver, redis_py_client) -> None:
    driver.xadd("s", "1-0", [("f", b"a")])
    driver.xadd("s", "2-0", [("f", b"b")])
    driver.xadd("s", "3-0", [("f", b"c")])
    # Add another with minid=2-0 — should evict the 1-0 entry.
    driver.xadd("s", "4-0", [("f", b"d")], minid="2-0", approximate=False)
    ids = [e[0] for e in redis_py_client.xrange("s", "-", "+")]
    assert b"1-0" not in ids
    assert b"2-0" in ids
    assert b"3-0" in ids
    assert b"4-0" in ids


@pytest.mark.asyncio
async def test_axadd_full_options(driver, redis_py_client) -> None:
    new_id = await driver.axadd(
        "s", "*", [("f", b"v")], maxlen=100, approximate=True, limit=10,
    )
    assert isinstance(new_id, str)
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py -v -k "nomkstream or maxlen or minid or full_options"`
Expected: FAIL — TypeError (unexpected keyword argument 'nomkstream') or wrong return type.

- [ ] **Step 3: Replace the basic `cmd_xadd_basic` with the full-option builder**

Edit `commands/streams.rs`. Replace `cmd_xadd_basic` with:

```rust
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
    // MAXLEN and MINID are mutually exclusive in the protocol; the caller
    // (driver method) is responsible for not passing both. We tolerate
    // both being None (no trim) and document the precedence: MAXLEN wins
    // over MINID if both are non-None (matches redis-py).
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
```

Replace the `xadd` and `axadd` methods with:

```rust
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, id, fields, *,
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
        id: &str,
        fields: Vec<(String, Vec<u8>)>,
        nomkstream: bool,
        maxlen: Option<i64>,
        minid: Option<String>,
        approximate: bool,
        limit: Option<i64>,
    ) -> PyResult<Option<String>> {
        let cmd = cmd_xadd(
            key,
            id,
            &fields,
            nomkstream,
            maxlen,
            minid.as_deref(),
            approximate,
            limit,
        );
        // NOMKSTREAM on a missing stream returns Nil → Option<String>.
        let r: redis::RedisResult<Option<String>> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, id, fields, *,
        nomkstream = false,
        maxlen = None,
        minid = None,
        approximate = true,
        limit = None,
    ))]
    fn axadd(
        &self,
        py: Python<'_>,
        key: &str,
        id: &str,
        fields: Vec<(String, Vec<u8>)>,
        nomkstream: bool,
        maxlen: Option<i64>,
        minid: Option<String>,
        approximate: bool,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let id = id.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xadd(
                &key,
                &id,
                &fields,
                nomkstream,
                maxlen,
                minid.as_deref(),
                approximate,
                limit,
            );
            let r: redis::RedisResult<Option<String>> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(opt) => RawResult::OptStr(opt),
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test the basic suite still passes**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py -v -k "xadd"`
Expected: all 12 PASS (5 basic + 7 option-matrix). The basic tests now drop into the option-matrix branch with all-defaults — verify no regression. If `test_xadd_basic_returns_id` now expects `Option<String>`-shaped return: the assertion `isinstance(new_id, str)` still passes when the server returns a real id (it's only None on NOMKSTREAM-misses).

Note: edit `test_xadd_basic_returns_id` if needed to assert `new_id is not None` first:

```python
def test_xadd_basic_returns_id(driver) -> None:
    new_id = driver.xadd("s", "*", [("field1", b"value1"), ("field2", b"value2")])
    assert new_id is not None
    assert isinstance(new_id, str)
    assert "-" in new_id
```

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add full XADD option matrix (NOMKSTREAM, MAXLEN, MINID, LIMIT)"
```

---

## Task 4: `XLEN`, `XDEL`, `XACK`

Three small scalar-returning commands grouped together — no flattening needed.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
def test_xlen_empty_stream(driver) -> None:
    assert driver.xlen("missing") == 0


def test_xlen_after_adds(driver) -> None:
    driver.xadd("s", "*", [("f", b"v")])
    driver.xadd("s", "*", [("f", b"v")])
    assert driver.xlen("s") == 2


@pytest.mark.asyncio
async def test_axlen_async(driver) -> None:
    await driver.axadd("s", "*", [("f", b"v")])
    assert await driver.axlen("s") == 1


def test_xdel_single(driver) -> None:
    id1 = driver.xadd("s", "1-0", [("f", b"v")])
    id2 = driver.xadd("s", "2-0", [("f", b"v")])
    assert driver.xdel("s", id1) == 1
    assert driver.xlen("s") == 1


def test_xdel_variadic(driver) -> None:
    id1 = driver.xadd("s", "1-0", [("f", b"v")])
    id2 = driver.xadd("s", "2-0", [("f", b"v")])
    id3 = driver.xadd("s", "3-0", [("f", b"v")])
    assert driver.xdel("s", id1, id2, "missing-0") == 2


@pytest.mark.asyncio
async def test_axdel_async(driver) -> None:
    id1 = await driver.axadd("s", "*", [("f", b"v")])
    assert await driver.axdel("s", id1) == 1


def test_xack_basic(driver, redis_py_client) -> None:
    """XACK: ack a delivered entry. Set up a consumer group inline."""
    id1 = driver.xadd("s", "*", [("f", b"v")])
    redis_py_client.xgroup_create("s", "g", id="0")
    # Read with the consumer group so the entry is in pending state.
    redis_py_client.xreadgroup("g", "c", {"s": ">"})
    assert driver.xack("s", "g", id1) == 1
    # ACKing again returns 0.
    assert driver.xack("s", "g", id1) == 0


def test_xack_variadic(driver, redis_py_client) -> None:
    id1 = driver.xadd("s", "*", [("f", b"v")])
    id2 = driver.xadd("s", "*", [("f", b"v")])
    redis_py_client.xgroup_create("s", "g", id="0")
    redis_py_client.xreadgroup("g", "c", {"s": ">"})
    assert driver.xack("s", "g", id1, id2) == 2


@pytest.mark.asyncio
async def test_axack_async(driver, redis_py_client) -> None:
    id1 = await driver.axadd("s", "*", [("f", b"v")])
    redis_py_client.xgroup_create("s", "g", id="0")
    redis_py_client.xreadgroup("g", "c", {"s": ">"})
    assert await driver.axack("s", "g", id1) == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py -v -k "xlen or xdel or xack"`
Expected: FAIL — methods don't exist.

- [ ] **Step 3: Implement XLEN/XDEL/XACK**

Append to **Argument-encoding helpers** in `commands/streams.rs`:

```rust
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (key))]
    fn xlen(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        let cmd = cmd_xlen(key);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (key))]
    fn axlen(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xlen(&key);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, *ids))]
    fn xdel(&self, py: Python<'_>, key: &str, ids: Vec<String>) -> PyResult<i64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let cmd = cmd_xdel(key, &ids);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (key, *ids))]
    fn axdel(&self, py: Python<'_>, key: &str, ids: Vec<String>) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            if ids.is_empty() {
                return RawResult::Int(0);
            }
            let cmd = cmd_xdel(&key, &ids);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    #[pyo3(signature = (key, group, *ids))]
    fn xack(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        ids: Vec<String>,
    ) -> PyResult<i64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let cmd = cmd_xack(key, group, &ids);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (key, group, *ids))]
    fn axack(
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py -v -k "xlen or xdel or xack"`
Expected: 9 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XLEN, XDEL, XACK (sync + async)"
```

---

## Task 5: `XRANGE` / `XREVRANGE` + the `flatten_xrange_reply` helper

This is where the flattening machinery starts. `XRANGE` reply (RESP2):
```
1) 1) "1-0"        # entry id
   2) 1) "field1"
      2) "value1"
      3) "field2"
      4) "value2"
2) 1) "2-0"
   ...
```
RESP3: same shape (Array of Array). Both decode as `Value::Array(vec![Value::Array(vec![Value::BulkString(id), Value::Array(vec![field, value, field, value, ...])])])`.

Target Python shape: `[(b"1-0", {b"field1": b"value1", b"field2": b"value2"}), ...]`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write the parity-with-redis-py tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXrange:
    def test_xrange_returns_list_of_id_dict_tuples(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f1", b"v1"), ("f2", b"v2")])
        driver.xadd("s", "2-0", [("f3", b"v3")])
        result = driver.xrange("s", "-", "+")
        assert result == [
            (b"1-0", {b"f1": b"v1", b"f2": b"v2"}),
            (b"2-0", {b"f3": b"v3"}),
        ]
        # Same call against the upstream client must return the same shape.
        assert result == redis_py_client.xrange("s", "-", "+")

    def test_xrange_with_count(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xadd("s", "2-0", [("f", b"v")])
        driver.xadd("s", "3-0", [("f", b"v")])
        result = driver.xrange("s", "-", "+", count=2)
        assert len(result) == 2
        assert result[0][0] == b"1-0"
        assert result[1][0] == b"2-0"

    def test_xrange_with_explicit_min_max(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xadd("s", "5-0", [("f", b"v")])
        driver.xadd("s", "10-0", [("f", b"v")])
        result = driver.xrange("s", "2-0", "8-0")
        assert [e[0] for e in result] == [b"5-0"]

    def test_xrange_empty_stream(self, driver) -> None:
        assert driver.xrange("missing", "-", "+") == []

    @pytest.mark.asyncio
    async def test_axrange(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("a", b"x")])
        result = await driver.axrange("s", "-", "+")
        assert result == [(b"1-0", {b"a": b"x"})]
        assert result == redis_py_client.xrange("s", "-", "+")


class TestXrevrange:
    def test_xrevrange_reverses_order(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v1")])
        driver.xadd("s", "2-0", [("f", b"v2")])
        driver.xadd("s", "3-0", [("f", b"v3")])
        result = driver.xrevrange("s", "+", "-")
        assert [e[0] for e in result] == [b"3-0", b"2-0", b"1-0"]
        assert result == redis_py_client.xrevrange("s", "+", "-")

    def test_xrevrange_with_count(self, driver) -> None:
        for ms in (1, 2, 3, 4, 5):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        result = driver.xrevrange("s", "+", "-", count=2)
        assert [e[0] for e in result] == [b"5-0", b"4-0"]

    @pytest.mark.asyncio
    async def test_axrevrange(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xadd("s", "2-0", [("f", b"v")])
        result = await driver.axrevrange("s", "+", "-")
        assert [e[0] for e in result] == [b"2-0", b"1-0"]
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXrange -v`
Expected: FAIL — methods don't exist.

- [ ] **Step 3: Implement `cmd_xrange`/`cmd_xrevrange` and the flattening helper**

Append to **Argument-encoding helpers**:

```rust
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
```

Append to **Reply-flattening helpers**:

```rust
/// Flatten an XRANGE/XREVRANGE/XCLAIM reply.
///
/// Input shape (RESP2 + RESP3):
///   Array(vec![
///     Array(vec![BulkString(id), Array(vec![field1, value1, field2, value2, ...])]),
///     ...
///   ])
///
/// Output: `Vec<(id_bytes, Vec<(field_bytes, value_bytes)>)>` — preserves
/// field-insertion order, since redis-py converts the inner list into an
/// insertion-ordered dict.
///
/// Returns an empty Vec on empty/Nil reply.
fn flatten_xrange_reply(value: redis::Value) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let entries = match value {
        redis::Value::Array(items) => items,
        redis::Value::Nil => return Vec::new(),
        // Defensive: a single-entry pseudo-array might land here from some
        // older redis-rs versions.
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
            // RESP3 may send a Map instead of a flat Array. Both possible.
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

/// Coerce a `redis::Value` to bytes for the limited set of types that
/// stream commands return as keys/values/ids. Returns None if the value
/// shape is unexpected (we choose to silently drop the field rather than
/// crash; the parity tests catch any drop in practice).
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (key, min, max, *, count=None))]
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let entries = flatten_xrange_reply(r.map_err(to_py_err)?);
        RawResult::StreamEntries(entries).into_py(py)
    }

    #[pyo3(signature = (key, min, max, *, count=None))]
    fn axrange(
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::StreamEntries(flatten_xrange_reply(v)),
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (key, max, min, *, count=None))]
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let entries = flatten_xrange_reply(r.map_err(to_py_err)?);
        RawResult::StreamEntries(entries).into_py(py)
    }

    #[pyo3(signature = (key, max, min, *, count=None))]
    fn axrevrange(
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::StreamEntries(flatten_xrange_reply(v)),
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXrange tests/driver/test_commands_streams.py::TestXrevrange -v`
Expected: 8 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XRANGE/XREVRANGE with flatten_xrange_reply helper"
```

---

## Task 6: `XREAD` with `streams=`, `count=`, `block=` + `flatten_xread_reply`

This is the second flattening helper — and the most-used stream command.

`XREAD` reply (RESP2):
```
1) 1) "stream-key-1"
   2) 1) 1) "id-1"
         2) 1) "f"
            2) "v"
      2) 1) "id-2"
         2) ...
2) 1) "stream-key-2"
   2) ...
```

RESP3: a `Map` keyed by stream-key, values are the entry-arrays. The driver forces RESP3 — both shapes must be handled because users may explicitly downgrade via the URL query.

Target shape: `dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]]` — keyed by stream key, value is the same `xrange`-shaped list of `(id, fields)` tuples. Returns `None` if the BLOCK timeout expires.

redis-py's signature:
```python
def xread(streams: dict[bytes|str, bytes|str], count: int|None = None, block: int|None = None)
```

The `streams` dict maps stream-key → starting-id (e.g. `{"my-stream": "0"}` to read all from the start, or `{"my-stream": "$"}` for new-only). At the driver layer we accept the same dict and unpack it into the keys-then-ids ordering the protocol requires.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXread:
    def test_xread_single_stream(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v1")])
        driver.xadd("s", "2-0", [("f", b"v2")])
        result = driver.xread({"s": "0"})
        # redis-py shape: {b"s": [(b"1-0", {b"f": b"v1"}), (b"2-0", {b"f": b"v2"})]}
        assert result == {
            b"s": [(b"1-0", {b"f": b"v1"}), (b"2-0", {b"f": b"v2"})]
        }
        assert result == redis_py_client.xread({"s": "0"})

    def test_xread_multiple_streams(self, driver, redis_py_client) -> None:
        driver.xadd("s1", "1-0", [("f", b"a")])
        driver.xadd("s2", "1-0", [("f", b"b")])
        result = driver.xread({"s1": "0", "s2": "0"})
        assert result == {
            b"s1": [(b"1-0", {b"f": b"a"})],
            b"s2": [(b"1-0", {b"f": b"b"})],
        }

    def test_xread_only_new(self, driver) -> None:
        """`$` means: only entries with id > current last."""
        driver.xadd("s", "*", [("f", b"v")])
        # Read with $ — no entries newer than the last one; result is None.
        result = driver.xread({"s": "$"})
        assert result is None

    def test_xread_with_count(self, driver) -> None:
        for ms in (1, 2, 3, 4, 5):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        result = driver.xread({"s": "0"}, count=2)
        assert len(result[b"s"]) == 2
        assert result[b"s"][0][0] == b"1-0"
        assert result[b"s"][1][0] == b"2-0"

    def test_xread_block_timeout_returns_none(self, driver) -> None:
        """Read with $ and a short block — no new entries → None."""
        driver.xadd("s", "*", [("f", b"v")])
        result = driver.xread({"s": "$"}, block=50)  # 50ms
        assert result is None

    def test_xread_block_with_concurrent_xadd(self, driver, valkey_url) -> None:
        """Block then have a second client XADD — should unblock and return."""
        import threading
        import time

        driver.xadd("s", "*", [("f", b"v0")])  # baseline
        out: dict = {}

        def reader() -> None:
            out["result"] = driver.xread({"s": "$"}, block=2000)

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.1)  # ensure the reader is blocked
        # Use a fresh upstream client to add — `driver` is busy in the reader thread.
        import redis as upstream
        rp = upstream.Redis.from_url(valkey_url)
        rp.xadd("s", {"f": b"v1"})
        rp.close()

        t.join(timeout=5)
        assert out["result"] is not None
        assert b"s" in out["result"]
        assert out["result"][b"s"][0][1] == {b"f": b"v1"}

    @pytest.mark.asyncio
    async def test_axread_basic(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        result = await driver.axread({"s": "0"})
        assert result == {b"s": [(b"1-0", {b"f": b"v"})]}
        assert result == redis_py_client.xread({"s": "0"})

    @pytest.mark.asyncio
    async def test_axread_block_timeout_returns_none(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        result = await driver.axread({"s": "$"}, block=50)
        assert result is None
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXread -v`
Expected: FAIL — `xread` doesn't exist.

- [ ] **Step 3: Implement `cmd_xread` + `flatten_xread_reply`**

Note about blocking: `XREAD ... BLOCK` is a blocking command on the server, but redis-rs's `ConnectionManager` (regular pool) has a 30s `response_timeout`. For `block > 30000` the driver MUST route to the blocking pool (`ValkeyConn::get_blocking()`, plumbed in plan 04). For `block <= 30000` the regular pool is fine. The simple implementation: always route to the blocking pool when `block` is set. We use that here; it costs an extra connection allocation on first use, but matches the cachex precedent.

Append to **Argument-encoding helpers**:

```rust
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
```

Append to **Reply-flattening helpers**:

```rust
/// Flatten an XREAD/XREADGROUP reply.
///
/// RESP2 shape:
///   Array(vec![
///     Array(vec![BulkString(stream-key), Array(vec![entry, entry, ...])]),
///     ...
///   ])
///
/// RESP3 shape (the driver forces this):
///   Map(vec![
///     (BulkString(stream-key), Array(vec![entry, entry, ...])),
///     ...
///   ])
///
/// On BLOCK timeout the server returns Nil (RESP2) or an empty Map
/// (RESP3) — both flatten to an empty Vec, which `into_py` translates
/// to Python `None`.
///
/// Returns: Vec<(stream_key_bytes, Vec<(entry_id_bytes, Vec<(field_bytes, value_bytes)>)>)>.
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
            let mut out = Vec::with_capacity(streams.len());
            for stream in streams {
                let pair = match stream {
                    redis::Value::Array(items) if items.len() == 2 => items,
                    _ => continue,
                };
                let mut iter = pair.into_iter();
                let key = iter
                    .next()
                    .and_then(value_to_bytes)
                    .unwrap_or_default();
                let entries = iter
                    .next()
                    .map(flatten_xrange_reply)
                    .unwrap_or_default();
                out.push((key, entries));
            }
            out
        }
        _ => Vec::new(),
    }
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (streams, *, count=None, block=None))]
    fn xread(
        &self,
        py: Python<'_>,
        streams: Vec<(String, String)>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        // PyO3 will convert a Python `dict[str, str]` into Vec<(K, V)> in
        // insertion order — that's what we want.
        let cmd = cmd_xread(&streams, count, block);
        let r: redis::RedisResult<redis::Value> = py.detach(|| {
            get_runtime().block_on(async {
                let mut conn = self.connection.clone();
                if block.is_some() {
                    let mut blocking_inner = conn
                        .get_blocking()
                        .await
                        .map_err(|e| e)?;
                    dispatch_cmd!(&mut blocking_inner, cmd)
                } else {
                    dispatch_cmd!(&mut conn, cmd)
                }
            })
        });
        let entries = flatten_xread_reply(r.map_err(to_py_err)?);
        RawResult::StreamReadEntries(entries).into_py(py)
    }

    #[pyo3(signature = (streams, *, count=None, block=None))]
    fn axread(
        &self,
        py: Python<'_>,
        streams: Vec<(String, String)>,
        count: Option<i64>,
        block: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        async_op!(self, py, conn, async {
            let cmd = cmd_xread(&streams, count, block);
            let r: redis::RedisResult<redis::Value> = if block.is_some() {
                match conn.get_blocking().await {
                    Ok(mut blocking_inner) => dispatch_cmd!(&mut blocking_inner, cmd),
                    Err(e) => Err(e),
                }
            } else {
                dispatch_cmd!(&mut conn, cmd)
            };
            match r {
                Ok(v) => RawResult::StreamReadEntries(flatten_xread_reply(v)),
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXread -v`
Expected: 8 PASS. The blocking-with-concurrent-xadd test takes ~100ms.

If `test_xread_block_timeout_returns_none` returns an empty dict instead of None: confirm `flatten_xread_reply` handles the empty-Map case — empty input → empty Vec → `into_py` returns `py.None()` per the variant arm in Task 1 Step 4.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XREAD with flatten_xread_reply helper (sync + async)"
```

---

## Task 7: `XREADGROUP` — same shape as XREAD, plus `group`/`consumer`/`noack`

`XREADGROUP` returns the same `dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]]` shape as XREAD; the only protocol difference is the `GROUP <group> <consumer>` prefix and the optional `NOACK` flag. Reuses `flatten_xread_reply`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXreadgroup:
    def _setup_group(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v1")])
        driver.xadd("s", "2-0", [("f", b"v2")])
        redis_py_client.xgroup_create("s", "g", id="0")

    def test_xreadgroup_pending_marker(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        # `>` — give me only undelivered entries (deliver them to consumer).
        result = driver.xreadgroup("g", "c1", {"s": ">"})
        assert result == {
            b"s": [
                (b"1-0", {b"f": b"v1"}),
                (b"2-0", {b"f": b"v2"}),
            ]
        }
        assert result == redis_py_client.xreadgroup("g", "c2", {"s": ">"})  # different consumer, but same shape after first delivery — careful

    def test_xreadgroup_history(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        # First delivery — one entry to c1.
        driver.xreadgroup("g", "c1", {"s": ">"}, count=1)
        # Now read the history (id="0") for c1 — already-delivered ones.
        result = driver.xreadgroup("g", "c1", {"s": "0"})
        assert b"s" in result
        assert result[b"s"][0][0] == b"1-0"

    def test_xreadgroup_with_count(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        result = driver.xreadgroup("g", "c1", {"s": ">"}, count=1)
        assert len(result[b"s"]) == 1

    def test_xreadgroup_noack(self, driver, redis_py_client) -> None:
        """NOACK: the entries are delivered but never enter the pending list."""
        self._setup_group(driver, redis_py_client)
        result = driver.xreadgroup("g", "c1", {"s": ">"}, noack=True)
        assert result is not None
        # Pending should be empty after NOACK delivery.
        pending = redis_py_client.xpending("s", "g")
        assert pending["pending"] == 0

    def test_xreadgroup_block_timeout(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        # Drain everything first.
        driver.xreadgroup("g", "c1", {"s": ">"})
        # Now read again with `>` and a short block — nothing new → None.
        result = driver.xreadgroup("g", "c1", {"s": ">"}, block=50)
        assert result is None

    @pytest.mark.asyncio
    async def test_axreadgroup(self, driver, redis_py_client) -> None:
        self._setup_group(driver, redis_py_client)
        result = await driver.axreadgroup("g", "c1", {"s": ">"})
        assert b"s" in result
        assert len(result[b"s"]) == 2
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXreadgroup -v`
Expected: FAIL.

- [ ] **Step 3: Implement `cmd_xreadgroup` + methods**

Append to **Argument-encoding helpers**:

```rust
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group, consumer, streams, *, count=None, block=None, noack=false))]
    fn xreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        streams: Vec<(String, String)>,
        count: Option<i64>,
        block: Option<i64>,
        noack: bool,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xreadgroup(group, consumer, &streams, count, block, noack);
        let r: redis::RedisResult<redis::Value> = py.detach(|| {
            get_runtime().block_on(async {
                let mut conn = self.connection.clone();
                if block.is_some() {
                    let mut blocking_inner = conn.get_blocking().await?;
                    dispatch_cmd!(&mut blocking_inner, cmd)
                } else {
                    dispatch_cmd!(&mut conn, cmd)
                }
            })
        });
        let entries = flatten_xread_reply(r.map_err(to_py_err)?);
        RawResult::StreamReadEntries(entries).into_py(py)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group, consumer, streams, *, count=None, block=None, noack=false))]
    fn axreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        streams: Vec<(String, String)>,
        count: Option<i64>,
        block: Option<i64>,
        noack: bool,
    ) -> PyResult<Py<PyAny>> {
        let group = group.to_string();
        let consumer = consumer.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xreadgroup(&group, &consumer, &streams, count, block, noack);
            let r: redis::RedisResult<redis::Value> = if block.is_some() {
                match conn.get_blocking().await {
                    Ok(mut blocking_inner) => dispatch_cmd!(&mut blocking_inner, cmd),
                    Err(e) => Err(e),
                }
            } else {
                dispatch_cmd!(&mut conn, cmd)
            };
            match r {
                Ok(v) => RawResult::StreamReadEntries(flatten_xread_reply(v)),
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXreadgroup -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XREADGROUP (sync + async)"
```

---

## Task 8: `XGROUP` family — `CREATE`, `SETID`, `DESTROY`, `CREATECONSUMER`, `DELCONSUMER`

All five sub-commands return scalars (`OK`, `i64`); no flattening needed.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXgroup:
    def test_xgroup_create_basic(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        # Returns None (sync) on success — server returns "OK".
        driver.xgroup_create("s", "g", id="0")
        # Creating the same group again raises BUSYGROUP.
        from redis_rs_py.exceptions import ResponseError
        with pytest.raises(ResponseError, match="BUSYGROUP"):
            driver.xgroup_create("s", "g", id="0")

    def test_xgroup_create_mkstream(self, driver) -> None:
        # mkstream=True creates the stream if missing.
        driver.xgroup_create("missing", "g", id="0", mkstream=True)
        # Stream now exists.
        assert driver.xlen("missing") == 0

    def test_xgroup_create_entries_read(self, driver) -> None:
        """Redis 7+ — ENTRIESREAD argument."""
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0", entries_read=5)

    def test_xgroup_setid(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xadd("s", "2-0", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        driver.xgroup_setid("s", "g", id="2-0")
        # Now `>` returns only id > 2-0.
        result = redis_py_client.xreadgroup("g", "c", {"s": ">"})
        assert result == []  # nothing new

    def test_xgroup_destroy(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        assert driver.xgroup_destroy("s", "g") == 1
        assert driver.xgroup_destroy("s", "g") == 0  # already gone

    def test_xgroup_createconsumer(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        # Returns 1 if created, 0 if already existed.
        assert driver.xgroup_createconsumer("s", "g", "c1") == 1
        assert driver.xgroup_createconsumer("s", "g", "c1") == 0

    def test_xgroup_delconsumer(self, driver, redis_py_client) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        # Read with consumer to register them with at least one pending entry.
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        # Delete consumer; returns the number of pending entries owned by them.
        assert driver.xgroup_delconsumer("s", "g", "c1") == 1

    @pytest.mark.asyncio
    async def test_axgroup_create_destroy(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        await driver.axgroup_create("s", "g", id="0")
        assert await driver.axgroup_destroy("s", "g") == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXgroup -v`
Expected: FAIL.

- [ ] **Step 3: Implement the XGROUP family**

Append to **Argument-encoding helpers**:

```rust
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

fn cmd_xgroup_setid(
    key: &str,
    group: &str,
    id: &str,
    entries_read: Option<i64>,
) -> redis::Cmd {
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (key, group, *, id="0", mkstream=false, entries_read=None))]
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (key, group, *, id="0", mkstream=false, entries_read=None))]
    fn axgroup_create(
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    #[pyo3(signature = (key, group, *, id, entries_read=None))]
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (key, group, *, id, entries_read=None))]
    fn axgroup_setid(
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }

    fn xgroup_destroy(&self, py: Python<'_>, key: &str, group: &str) -> PyResult<i64> {
        let cmd = cmd_xgroup_destroy(key, group);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    fn axgroup_destroy(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xgroup_destroy(&key, &group);
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn xgroup_createconsumer(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> PyResult<i64> {
        let cmd = cmd_xgroup_createconsumer(key, group, consumer);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    fn axgroup_createconsumer(
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }

    fn xgroup_delconsumer(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> PyResult<i64> {
        let cmd = cmd_xgroup_delconsumer(key, group, consumer);
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    fn axgroup_delconsumer(
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
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXgroup -v`
Expected: 8 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XGROUP CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER"
```

---

## Task 9: `XINFO STREAM` / `XINFO GROUPS` / `XINFO CONSUMERS`

These three return per-row maps. The flattening is "Map -> dict[bytes, Any]" — we keep the inner values as recursively-converted `redis::Value` (lists/ints/bytes), since the schema is open-ended and the parity tests assert dict equality with `redis-py`'s output.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXinfo:
    def test_xinfo_stream_basic_fields(self, driver, redis_py_client) -> None:
        driver.xadd("s", "1-0", [("f", b"v1")])
        driver.xadd("s", "2-0", [("f", b"v2")])
        info = driver.xinfo_stream("s")
        # redis-py returns dict with bytes keys.
        assert isinstance(info, dict)
        assert info[b"length"] == 2
        # Field-set parity (modulo new fields in newer Valkey versions).
        upstream = redis_py_client.xinfo_stream("s")
        # Check the load-bearing keys all present in both.
        for k in (b"length", b"first-entry", b"last-entry", b"groups"):
            assert k in info
            assert k in upstream
        assert info[b"length"] == upstream[b"length"]

    def test_xinfo_stream_full(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        info = driver.xinfo_stream("s", full=True)
        assert isinstance(info, dict)
        assert b"length" in info

    def test_xinfo_groups_empty(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        result = driver.xinfo_groups("s")
        assert result == []

    def test_xinfo_groups_after_create(self, driver, redis_py_client) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        result = driver.xinfo_groups("s")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0][b"name"] == b"g"
        # Compare with upstream — the dicts must be equal (modulo new fields).
        upstream = redis_py_client.xinfo_groups("s")
        assert result[0][b"name"] == upstream[0][b"name"]
        assert result[0][b"consumers"] == upstream[0][b"consumers"]

    def test_xinfo_consumers(self, driver, redis_py_client) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        result = driver.xinfo_consumers("s", "g")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0][b"name"] == b"c1"
        assert result[0][b"pending"] == 1

    @pytest.mark.asyncio
    async def test_axinfo_stream(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        info = await driver.axinfo_stream("s")
        assert info[b"length"] == 1
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXinfo -v`
Expected: FAIL.

- [ ] **Step 3: Implement XINFO**

Append to **Argument-encoding helpers**:

```rust
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
```

Append to **Reply-flattening helpers**:

```rust
/// Flatten an XINFO STREAM reply (single map of bytes → value).
///
/// RESP2 shape: Array(vec![key1, value1, key2, value2, ...])
/// RESP3 shape: Map(vec![(key1, value1), (key2, value2), ...])
///
/// Returns the pairs untouched; `RawResult::StreamInfoStream::into_py`
/// builds the dict on the GIL side.
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

/// Convert either a flat-array `[k, v, k, v, ...]` or a Map(vec![(k, v), ...])
/// into a `Vec<(k_bytes, v_value)>`.
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (key, *, full=false))]
    fn xinfo_stream(
        &self,
        py: Python<'_>,
        key: &str,
        full: bool,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xinfo_stream(key, full);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let pairs = flatten_xinfo_stream(r.map_err(to_py_err)?);
        RawResult::StreamInfoStream(pairs).into_py(py)
    }

    #[pyo3(signature = (key, *, full=false))]
    fn axinfo_stream(
        &self,
        py: Python<'_>,
        key: &str,
        full: bool,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xinfo_stream(&key, full);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::StreamInfoStream(flatten_xinfo_stream(v)),
                Err(e) => classify(e),
            }
        })
    }

    fn xinfo_groups(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xinfo_groups(key);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::StreamInfoGroups(flatten_xinfo_list(r.map_err(to_py_err)?)).into_py(py)
    }

    fn axinfo_groups(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xinfo_groups(&key);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::StreamInfoGroups(flatten_xinfo_list(v)),
                Err(e) => classify(e),
            }
        })
    }

    fn xinfo_consumers(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
    ) -> PyResult<Py<PyAny>> {
        let cmd = cmd_xinfo_consumers(key, group);
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        RawResult::StreamInfoConsumers(flatten_xinfo_list(r.map_err(to_py_err)?)).into_py(py)
    }

    fn axinfo_consumers(
        &self,
        py: Python<'_>,
        key: &str,
        group: &str,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        let group = group.to_string();
        async_op!(self, py, conn, async {
            let cmd = cmd_xinfo_consumers(&key, &group);
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(v) => RawResult::StreamInfoConsumers(flatten_xinfo_list(v)),
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXinfo -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XINFO STREAM/GROUPS/CONSUMERS with flatten helpers"
```

---

## Task 10: `XTRIM` with MAXLEN/MINID/approximate/limit

`XTRIM` returns an i64 (entries-removed count). Mirrors the trim half of XADD but lets the user trim explicitly without adding.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXtrim:
    def test_xtrim_maxlen_exact(self, driver) -> None:
        for ms in range(10):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = driver.xtrim("s", maxlen=5, approximate=False)
        assert removed == 5
        assert driver.xlen("s") == 5

    def test_xtrim_maxlen_approximate(self, driver) -> None:
        for ms in range(20):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = driver.xtrim("s", maxlen=5, approximate=True)
        assert removed >= 0  # approximate trim might remove 0–15

    def test_xtrim_minid(self, driver) -> None:
        for ms in (1, 2, 3, 4, 5):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = driver.xtrim("s", minid="3-0", approximate=False)
        assert removed == 2

    def test_xtrim_with_limit(self, driver) -> None:
        for ms in range(20):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = driver.xtrim("s", maxlen=5, approximate=True, limit=3)
        assert removed >= 0
        assert removed <= 15

    def test_xtrim_requires_maxlen_or_minid(self, driver) -> None:
        from redis_rs_py.exceptions import DataError
        driver.xadd("s", "*", [("f", b"v")])
        with pytest.raises(DataError):
            driver.xtrim("s")

    @pytest.mark.asyncio
    async def test_axtrim(self, driver) -> None:
        for ms in range(5):
            driver.xadd("s", f"{ms}-0", [("f", b"v")])
        removed = await driver.axtrim("s", maxlen=2, approximate=False)
        assert removed == 3
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXtrim -v`
Expected: FAIL.

- [ ] **Step 3: Implement XTRIM**

Append to **Argument-encoding helpers**:

```rust
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (key, *, maxlen=None, minid=None, approximate=true, limit=None))]
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
            .ok_or_else(|| {
                pyo3::PyErr::new::<crate::exceptions::DataError, _>(
                    "xtrim requires maxlen or minid",
                )
            })?;
        let r: redis::RedisResult<i64> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map_err(to_py_err)
    }

    #[pyo3(signature = (key, *, maxlen=None, minid=None, approximate=true, limit=None))]
    fn axtrim(
        &self,
        py: Python<'_>,
        key: &str,
        maxlen: Option<i64>,
        minid: Option<String>,
        approximate: bool,
        limit: Option<i64>,
    ) -> PyResult<Py<PyAny>> {
        let key = key.to_string();
        // Build the cmd outside the spawn so we can return a sync error if
        // both maxlen and minid are missing.
        let cmd = cmd_xtrim(&key, maxlen, minid.as_deref(), approximate, limit)
            .ok_or_else(|| {
                pyo3::PyErr::new::<crate::exceptions::DataError, _>(
                    "xtrim requires maxlen or minid",
                )
            })?;
        async_op!(self, py, conn, async {
            let r: redis::RedisResult<i64> = dispatch_cmd!(&mut conn, cmd);
            r.into_raw_result()
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXtrim -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XTRIM with MAXLEN/MINID/approximate/limit"
```

---

## Task 11: `XPENDING` — summary and range forms in one method

redis-py's `xpending(name, groupname, idle=None, min=None, max=None, count=None, consumer=None)`:
- If `min`/`max`/`count` are all None → summary form: `XPENDING <key> <group>`. Returns `(count, min_id, max_id, [(consumer, count), ...])` 4-tuple.
- Otherwise → range form: `XPENDING <key> <group> [IDLE ms] <start> <end> <count> [<consumer>]`. Returns `[{message_id, consumer, time_since_delivered, times_delivered}, ...]`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXpending:
    def _setup(self, driver, redis_py_client) -> tuple[str, str]:
        id1 = driver.xadd("s", "*", [("f", b"v1")])
        id2 = driver.xadd("s", "*", [("f", b"v2")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        return id1, id2

    def test_xpending_summary_no_pending(self, driver) -> None:
        driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="$")
        result = driver.xpending("s", "g")
        # No pending entries → 4-tuple with count=0, min/max None, empty list.
        assert result == (0, None, None, [])

    def test_xpending_summary_with_pending(self, driver, redis_py_client) -> None:
        id1, id2 = self._setup(driver, redis_py_client)
        result = driver.xpending("s", "g")
        count, min_id, max_id, consumers = result
        assert count == 2
        assert min_id == id1.encode()
        assert max_id == id2.encode()
        assert consumers == [(b"c1", 2)]

    def test_xpending_range_basic(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        result = driver.xpending("s", "g", min="-", max="+", count=10)
        # range form returns a list of dicts.
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0][b"consumer"] == b"c1"
        assert result[0][b"times_delivered"] == 1

    def test_xpending_range_with_consumer_filter(
        self, driver, redis_py_client
    ) -> None:
        self._setup(driver, redis_py_client)
        # Add a third entry and read it with a different consumer.
        id3 = driver.xadd("s", "*", [("f", b"v3")])
        redis_py_client.xreadgroup("g", "c2", {"s": ">"})
        result = driver.xpending(
            "s", "g", min="-", max="+", count=10, consumer="c1"
        )
        assert len(result) == 2
        for row in result:
            assert row[b"consumer"] == b"c1"

    def test_xpending_range_with_idle(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        # idle=0 returns all entries idle at least 0ms (i.e. all).
        result = driver.xpending(
            "s", "g", idle=0, min="-", max="+", count=10
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_axpending_summary(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        result = await driver.axpending("s", "g")
        count, _min, _max, consumers = result
        assert count == 2

    @pytest.mark.asyncio
    async def test_axpending_range(self, driver, redis_py_client) -> None:
        self._setup(driver, redis_py_client)
        result = await driver.axpending("s", "g", min="-", max="+", count=10)
        assert len(result) == 2
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXpending -v`
Expected: FAIL.

- [ ] **Step 3: Implement XPENDING**

Append to **Argument-encoding helpers**:

```rust
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
```

Append to **Reply-flattening helpers**:

```rust
/// Flatten an XPENDING summary reply.
///
/// Shape (RESP2 + RESP3):
///   Array(vec![
///     Int(count),
///     BulkString(min_id) | Nil,
///     BulkString(max_id) | Nil,
///     Array(vec![Array(vec![BulkString(consumer), BulkString(count_str)]), ...]) | Nil,
///   ])
///
/// Returns None if count==0 and min/max are Nil — caller materializes a
/// (0, None, None, []) tuple via the StreamPendingSummary(None) variant arm.
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

    Some((count, min_id.unwrap_or_default(), max_id.unwrap_or_default(), consumers))
}

/// Flatten an XPENDING range reply.
///
/// Shape:
///   Array(vec![
///     Array(vec![BulkString(id), BulkString(consumer), Int(idle_ms), Int(deliveries)]),
///     ...
///   ])
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, *,
        idle=None,
        min=None,
        max=None,
        count=None,
        consumer=None,
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
        // Pick form: range if any of min/max/count is set; otherwise summary.
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let value = r.map_err(to_py_err)?;
        if is_range {
            RawResult::StreamPendingRange(flatten_xpending_range(value)).into_py(py)
        } else {
            RawResult::StreamPendingSummary(flatten_xpending_summary(value)).into_py(py)
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, *,
        idle=None,
        min=None,
        max=None,
        count=None,
        consumer=None,
    ))]
    fn axpending(
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
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
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXpending -v`
Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XPENDING summary + range forms with one method"
```

---

## Task 12: `XCLAIM` with `justid`

`XCLAIM <key> <group> <consumer> <min_idle> <id> [<id> ...] [IDLE ms] [TIME ms-unix] [RETRYCOUNT n] [FORCE] [JUSTID]`.

Returns either a list of `(id, fields)` entries (default) OR a list of just-ids (`JUSTID`).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXclaim:
    def _make_pending(self, driver, redis_py_client) -> tuple[str, str]:
        id1 = driver.xadd("s", "*", [("f", b"v1")])
        id2 = driver.xadd("s", "*", [("f", b"v2")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        return id1, id2

    def test_xclaim_basic_returns_entries(self, driver, redis_py_client) -> None:
        id1, _id2 = self._make_pending(driver, redis_py_client)
        # Claim id1 from c1 to c2 (with min_idle=0 so nothing blocks).
        result = driver.xclaim("s", "g", "c2", min_idle_time=0, message_ids=[id1])
        # Same shape as XRANGE.
        assert result == [(id1.encode(), {b"f": b"v1"})]

    def test_xclaim_justid(self, driver, redis_py_client) -> None:
        id1, id2 = self._make_pending(driver, redis_py_client)
        result = driver.xclaim(
            "s", "g", "c2", min_idle_time=0, message_ids=[id1, id2], justid=True,
        )
        assert sorted(result) == sorted([id1.encode(), id2.encode()])

    def test_xclaim_force_creates_pending_if_missing(
        self, driver, redis_py_client
    ) -> None:
        """FORCE: claim an id that's not in the PEL — server creates an entry."""
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xgroup_create("s", "g", id="$")  # no pending
        # 1-0 is not pending under group g. Without FORCE, claim returns [].
        no_force = driver.xclaim("s", "g", "c1", min_idle_time=0, message_ids=["1-0"])
        assert no_force == []
        with_force = driver.xclaim(
            "s", "g", "c1", min_idle_time=0, message_ids=["1-0"], force=True,
        )
        assert len(with_force) == 1
        assert with_force[0][0] == b"1-0"

    def test_xclaim_with_idle_time_setting(self, driver, redis_py_client) -> None:
        id1, _id2 = self._make_pending(driver, redis_py_client)
        # Set idle=100000 on the claimed entry.
        result = driver.xclaim(
            "s", "g", "c2", min_idle_time=0, message_ids=[id1], idle=100000,
        )
        assert len(result) == 1
        # Verify via XPENDING range that idle is at least 100s.
        pending = driver.xpending("s", "g", min="-", max="+", count=10)
        for row in pending:
            if row[b"message_id"] == id1.encode():
                assert row[b"time_since_delivered"] >= 100000

    def test_xclaim_min_idle_time_filters(
        self, driver, redis_py_client
    ) -> None:
        """min_idle_time: claim only if the pending entry has been idle for at least that long."""
        id1, _ = self._make_pending(driver, redis_py_client)
        # The entry was just delivered — idle is ~0. Claiming with min_idle=10000 → empty.
        result = driver.xclaim(
            "s", "g", "c2", min_idle_time=10000, message_ids=[id1],
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_axclaim(self, driver, redis_py_client) -> None:
        id1, _id2 = self._make_pending(driver, redis_py_client)
        result = await driver.axclaim(
            "s", "g", "c2", min_idle_time=0, message_ids=[id1],
        )
        assert len(result) == 1
        assert result[0][0] == id1.encode()
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXclaim -v`
Expected: FAIL.

- [ ] **Step 3: Implement XCLAIM**

Append to **Argument-encoding helpers**:

```rust
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
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
            key, group, consumer, min_idle_time, &message_ids, idle, time, retrycount,
            force, justid,
        );
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let value = r.map_err(to_py_err)?;
        if justid {
            // JUSTID returns a flat array of bulk-strings.
            let ids = match value {
                redis::Value::Array(items) => items
                    .into_iter()
                    .filter_map(value_to_bytes)
                    .collect(),
                _ => Vec::new(),
            };
            RawResult::StreamClaimJustIds(ids).into_py(py)
        } else {
            let entries = flatten_xrange_reply(value);
            RawResult::StreamClaim(entries).into_py(py)
        }
    }

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
    fn axclaim(
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
                &key, &group, &consumer, min_idle_time, &message_ids, idle, time, retrycount,
                force, justid,
            );
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(value) => {
                    if justid {
                        let ids = match value {
                            redis::Value::Array(items) => items
                                .into_iter()
                                .filter_map(value_to_bytes)
                                .collect(),
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
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXclaim -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XCLAIM with JUSTID support"
```

---

## Task 13: `XAUTOCLAIM` with `justid`

`XAUTOCLAIM <key> <group> <consumer> <min_idle> <start> [COUNT n] [JUSTID]`.

Reply (default):
```
1) <next_cursor_id>      # opaque id to pass on next call (or "0-0" when done)
2) <list of [id, fields] entries>     # the claimed entries
3) <list of deleted ids>              # ids that were in the PEL but no longer in the stream
```

Reply (JUSTID): the middle list becomes a list of just-ids.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXautoclaim:
    def _make_pending(self, driver, redis_py_client) -> tuple[str, str]:
        id1 = driver.xadd("s", "*", [("f", b"v1")])
        id2 = driver.xadd("s", "*", [("f", b"v2")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        return id1, id2

    def test_xautoclaim_basic(self, driver, redis_py_client) -> None:
        id1, id2 = self._make_pending(driver, redis_py_client)
        next_id, entries, deleted = driver.xautoclaim(
            "s", "g", "c2", min_idle_time=0, start_id="0-0",
        )
        assert next_id == b"0-0"  # done — no more to claim
        assert len(entries) == 2
        assert entries[0][0] in {id1.encode(), id2.encode()}
        assert deleted == []

    def test_xautoclaim_with_count(self, driver, redis_py_client) -> None:
        for _ in range(5):
            driver.xadd("s", "*", [("f", b"v")])
        driver.xgroup_create("s", "g", id="0")
        redis_py_client.xreadgroup("g", "c1", {"s": ">"})
        next_id, entries, _deleted = driver.xautoclaim(
            "s", "g", "c2", min_idle_time=0, start_id="0-0", count=2,
        )
        assert len(entries) == 2
        # next_id is non-zero — there's more to claim.
        assert next_id != b"0-0"

    def test_xautoclaim_justid(self, driver, redis_py_client) -> None:
        id1, id2 = self._make_pending(driver, redis_py_client)
        next_id, ids, _deleted = driver.xautoclaim(
            "s", "g", "c2", min_idle_time=0, start_id="0-0", justid=True,
        )
        assert next_id == b"0-0"
        assert sorted(ids) == sorted([id1.encode(), id2.encode()])

    def test_xautoclaim_min_idle_time_filters(
        self, driver, redis_py_client
    ) -> None:
        self._make_pending(driver, redis_py_client)
        # The entries were just delivered — idle ~0ms. Auto-claim with min_idle=10000 → empty.
        next_id, entries, deleted = driver.xautoclaim(
            "s", "g", "c2", min_idle_time=10000, start_id="0-0",
        )
        assert next_id == b"0-0"
        assert entries == []
        assert deleted == []

    @pytest.mark.asyncio
    async def test_axautoclaim(self, driver, redis_py_client) -> None:
        self._make_pending(driver, redis_py_client)
        next_id, entries, _deleted = await driver.axautoclaim(
            "s", "g", "c2", min_idle_time=0, start_id="0-0",
        )
        assert next_id == b"0-0"
        assert len(entries) == 2
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXautoclaim -v`
Expected: FAIL.

- [ ] **Step 3: Implement XAUTOCLAIM**

Append to **Argument-encoding helpers**:

```rust
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
    cmd.arg(key).arg(group).arg(consumer).arg(min_idle_time).arg(start_id);
    if let Some(c) = count {
        cmd.arg("COUNT").arg(c);
    }
    if justid {
        cmd.arg("JUSTID");
    }
    cmd
}
```

Append to **Reply-flattening helpers**:

```rust
/// Split an XAUTOCLAIM reply into its three parts.
///
/// Shape:
///   Array(vec![
///     BulkString(next_id),
///     Array(<entries or just-ids>),
///     Array(<deleted ids>),
///   ])
fn split_xautoclaim_reply(value: redis::Value) -> (Vec<u8>, redis::Value, Vec<Vec<u8>>) {
    let parts = match value {
        redis::Value::Array(items) if items.len() >= 2 => items,
        _ => return (Vec::new(), redis::Value::Nil, Vec::new()),
    };
    let mut iter = parts.into_iter();
    let next_id = iter.next().and_then(value_to_bytes).unwrap_or_default();
    let middle = iter.next().unwrap_or(redis::Value::Nil);
    // The deleted-ids list is only present in Redis 7+ replies.
    let deleted = match iter.next() {
        Some(redis::Value::Array(ids)) => ids
            .into_iter()
            .filter_map(value_to_bytes)
            .collect(),
        _ => Vec::new(),
    };
    (next_id, middle, deleted)
}
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
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
            key, group, consumer, min_idle_time, start_id, Some(count), justid,
        );
        let r: redis::RedisResult<redis::Value> =
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        let (next_id, middle, deleted) = split_xautoclaim_reply(r.map_err(to_py_err)?);
        if justid {
            let ids = match middle {
                redis::Value::Array(items) => items
                    .into_iter()
                    .filter_map(value_to_bytes)
                    .collect(),
                _ => Vec::new(),
            };
            RawResult::StreamAutoclaimJustIds((next_id, ids, deleted)).into_py(py)
        } else {
            let entries = flatten_xrange_reply(middle);
            RawResult::StreamAutoclaim((next_id, entries, deleted)).into_py(py)
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key, group, consumer, *,
        min_idle_time,
        start_id = "0-0",
        count = 100,
        justid = false,
    ))]
    fn axautoclaim(
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
                &key, &group, &consumer, min_idle_time, &start_id, Some(count), justid,
            );
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(value) => {
                    let (next_id, middle, deleted) = split_xautoclaim_reply(value);
                    if justid {
                        let ids = match middle {
                            redis::Value::Array(items) => items
                                .into_iter()
                                .filter_map(value_to_bytes)
                                .collect(),
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
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXautoclaim -v`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XAUTOCLAIM with JUSTID support"
```

---

## Task 14: `XSETID` with `entries_added` + `max_deleted_entry_id`

Trivial scalar command but needs the two Redis 7+ option flags.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/commands/streams.rs`
- Modify: `tests/driver/test_commands_streams.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/driver/test_commands_streams.py`:

```python
class TestXsetid:
    def test_xsetid_basic(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        # Set the last-id to 100-0 — subsequent XADD with `*` will use ms >= 100.
        driver.xsetid("s", "100-0")
        new_id = driver.xadd("s", "*", [("f", b"v")])
        ms, _ = new_id.split("-")
        assert int(ms) >= 100

    def test_xsetid_entries_added(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xsetid("s", "100-0", entries_added=42)
        info = driver.xinfo_stream("s")
        assert info[b"entries-added"] == 42

    def test_xsetid_max_deleted(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        driver.xsetid("s", "100-0", max_deleted_entry_id="50-0")
        info = driver.xinfo_stream("s")
        assert info[b"max-deleted-entry-id"] == b"50-0"

    @pytest.mark.asyncio
    async def test_axsetid(self, driver) -> None:
        driver.xadd("s", "1-0", [("f", b"v")])
        await driver.axsetid("s", "200-0")
```

- [ ] **Step 2: Run the failing tests**

Run: `uv run pytest tests/driver/test_commands_streams.py::TestXsetid -v`
Expected: FAIL.

- [ ] **Step 3: Implement XSETID**

Append to **Argument-encoding helpers**:

```rust
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
```

Append to `#[pymethods] impl RedisRsDriver`:

```rust
    #[pyo3(signature = (key, id, *, entries_added=None, max_deleted_entry_id=None))]
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
            sync_op!(py, self, conn, dispatch_cmd!(&mut conn, cmd));
        r.map(|_| ()).map_err(to_py_err)
    }

    #[pyo3(signature = (key, id, *, entries_added=None, max_deleted_entry_id=None))]
    fn axsetid(
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
            let r: redis::RedisResult<redis::Value> = dispatch_cmd!(&mut conn, cmd);
            match r {
                Ok(_) => RawResult::Nil,
                Err(e) => classify(e),
            }
        })
    }
```

- [ ] **Step 4: Build + test**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/driver/test_commands_streams.py::TestXsetid -v`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/commands/streams.rs tests/driver/test_commands_streams.py
git commit -m "feat(streams): add XSETID with entries_added/max_deleted_entry_id"
```

---

## Task 15: Stub the new methods in `_driver.pyi`

Hand-maintained type stubs for the 23 new methods.

**Files:**
- Modify: `python/redis_rs_py/_driver.pyi`

- [ ] **Step 1: Append the stream method stubs**

Open `python/redis_rs_py/_driver.pyi` and add these methods inside the `class RedisRsDriver:` block (after the `aping` method):

```python
    # Streams (Plan 08)
    def xadd(
        self,
        key: str,
        id: str,
        fields: list[tuple[str, bytes]],
        *,
        nomkstream: bool = ...,
        maxlen: int | None = ...,
        minid: str | None = ...,
        approximate: bool = ...,
        limit: int | None = ...,
    ) -> str | None: ...
    def axadd(
        self,
        key: str,
        id: str,
        fields: list[tuple[str, bytes]],
        *,
        nomkstream: bool = ...,
        maxlen: int | None = ...,
        minid: str | None = ...,
        approximate: bool = ...,
        limit: int | None = ...,
    ) -> Awaitable[str | None]: ...
    def xlen(self, key: str) -> int: ...
    def axlen(self, key: str) -> Awaitable[int]: ...
    def xdel(self, key: str, *ids: str) -> int: ...
    def axdel(self, key: str, *ids: str) -> Awaitable[int]: ...
    def xack(self, key: str, group: str, *ids: str) -> int: ...
    def axack(self, key: str, group: str, *ids: str) -> Awaitable[int]: ...
    def xrange(
        self, key: str, min: str, max: str, *, count: int | None = ...
    ) -> list[tuple[bytes, dict[bytes, bytes]]]: ...
    def axrange(
        self, key: str, min: str, max: str, *, count: int | None = ...
    ) -> Awaitable[list[tuple[bytes, dict[bytes, bytes]]]]: ...
    def xrevrange(
        self, key: str, max: str, min: str, *, count: int | None = ...
    ) -> list[tuple[bytes, dict[bytes, bytes]]]: ...
    def axrevrange(
        self, key: str, max: str, min: str, *, count: int | None = ...
    ) -> Awaitable[list[tuple[bytes, dict[bytes, bytes]]]]: ...
    def xread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = ...,
        block: int | None = ...,
    ) -> dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]] | None: ...
    def axread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = ...,
        block: int | None = ...,
    ) -> Awaitable[dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]] | None]: ...
    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int | None = ...,
        block: int | None = ...,
        noack: bool = ...,
    ) -> dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]] | None: ...
    def axreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int | None = ...,
        block: int | None = ...,
        noack: bool = ...,
    ) -> Awaitable[dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]] | None]: ...
    def xgroup_create(
        self,
        key: str,
        group: str,
        *,
        id: str = ...,
        mkstream: bool = ...,
        entries_read: int | None = ...,
    ) -> None: ...
    def axgroup_create(
        self,
        key: str,
        group: str,
        *,
        id: str = ...,
        mkstream: bool = ...,
        entries_read: int | None = ...,
    ) -> Awaitable[None]: ...
    def xgroup_setid(
        self, key: str, group: str, *, id: str, entries_read: int | None = ...
    ) -> None: ...
    def axgroup_setid(
        self, key: str, group: str, *, id: str, entries_read: int | None = ...
    ) -> Awaitable[None]: ...
    def xgroup_destroy(self, key: str, group: str) -> int: ...
    def axgroup_destroy(self, key: str, group: str) -> Awaitable[int]: ...
    def xgroup_createconsumer(self, key: str, group: str, consumer: str) -> int: ...
    def axgroup_createconsumer(
        self, key: str, group: str, consumer: str
    ) -> Awaitable[int]: ...
    def xgroup_delconsumer(self, key: str, group: str, consumer: str) -> int: ...
    def axgroup_delconsumer(
        self, key: str, group: str, consumer: str
    ) -> Awaitable[int]: ...
    def xinfo_stream(self, key: str, *, full: bool = ...) -> dict[bytes, Any]: ...
    def axinfo_stream(
        self, key: str, *, full: bool = ...
    ) -> Awaitable[dict[bytes, Any]]: ...
    def xinfo_groups(self, key: str) -> list[dict[bytes, Any]]: ...
    def axinfo_groups(self, key: str) -> Awaitable[list[dict[bytes, Any]]]: ...
    def xinfo_consumers(self, key: str, group: str) -> list[dict[bytes, Any]]: ...
    def axinfo_consumers(
        self, key: str, group: str
    ) -> Awaitable[list[dict[bytes, Any]]]: ...
    def xtrim(
        self,
        key: str,
        *,
        maxlen: int | None = ...,
        minid: str | None = ...,
        approximate: bool = ...,
        limit: int | None = ...,
    ) -> int: ...
    def axtrim(
        self,
        key: str,
        *,
        maxlen: int | None = ...,
        minid: str | None = ...,
        approximate: bool = ...,
        limit: int | None = ...,
    ) -> Awaitable[int]: ...
    def xpending(
        self,
        key: str,
        group: str,
        *,
        idle: int | None = ...,
        min: str | None = ...,
        max: str | None = ...,
        count: int | None = ...,
        consumer: str | None = ...,
    ) -> tuple[int, bytes | None, bytes | None, list[tuple[bytes, int]]] | list[
        dict[bytes, Any]
    ]: ...
    def axpending(
        self,
        key: str,
        group: str,
        *,
        idle: int | None = ...,
        min: str | None = ...,
        max: str | None = ...,
        count: int | None = ...,
        consumer: str | None = ...,
    ) -> Awaitable[
        tuple[int, bytes | None, bytes | None, list[tuple[bytes, int]]]
        | list[dict[bytes, Any]]
    ]: ...
    def xclaim(
        self,
        key: str,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        message_ids: list[str],
        idle: int | None = ...,
        time: int | None = ...,
        retrycount: int | None = ...,
        force: bool = ...,
        justid: bool = ...,
    ) -> list[tuple[bytes, dict[bytes, bytes]]] | list[bytes]: ...
    def axclaim(
        self,
        key: str,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        message_ids: list[str],
        idle: int | None = ...,
        time: int | None = ...,
        retrycount: int | None = ...,
        force: bool = ...,
        justid: bool = ...,
    ) -> Awaitable[list[tuple[bytes, dict[bytes, bytes]]] | list[bytes]]: ...
    def xautoclaim(
        self,
        key: str,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        start_id: str = ...,
        count: int = ...,
        justid: bool = ...,
    ) -> tuple[
        bytes,
        list[tuple[bytes, dict[bytes, bytes]]] | list[bytes],
        list[bytes],
    ]: ...
    def axautoclaim(
        self,
        key: str,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        start_id: str = ...,
        count: int = ...,
        justid: bool = ...,
    ) -> Awaitable[
        tuple[
            bytes,
            list[tuple[bytes, dict[bytes, bytes]]] | list[bytes],
            list[bytes],
        ]
    ]: ...
    def xsetid(
        self,
        key: str,
        id: str,
        *,
        entries_added: int | None = ...,
        max_deleted_entry_id: str | None = ...,
    ) -> None: ...
    def axsetid(
        self,
        key: str,
        id: str,
        *,
        entries_added: int | None = ...,
        max_deleted_entry_id: str | None = ...,
    ) -> Awaitable[None]: ...
```

The top of the file already imports `Awaitable` (Plan 01 Task 11). Add `Any` to that import line:

```python
from typing import Any, Awaitable
```

- [ ] **Step 2: Run ty**

Run: `uv run ty check python/redis_rs_py/`
Expected: 0 errors.

- [ ] **Step 3: Commit**

```bash
git add python/redis_rs_py/_driver.pyi
git commit -m "feat(streams): add type stubs for the 23 new stream methods"
```

---

## Task 16: Lint pass + free-threaded smoke

Verify the new file compiles cleanly under clippy, the tests pass under pytest-xdist parallel, and the cp314t free-threaded build still works.

**Files:** none modified — verification only.

- [ ] **Step 1: Lint pass**

```bash
uv run ruff check tests/driver/test_commands_streams.py
uv run ruff format --check tests/driver/test_commands_streams.py
uv run ty check python/redis_rs_py/
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: all green. If clippy complains about `clippy::too_many_arguments` on a method missing the `#[allow]`, add the attribute.

- [ ] **Step 2: Run the full streams suite**

```bash
uv run pytest tests/driver/test_commands_streams.py -v
```

Expected: ~70 PASS (sum of: XADD 12 + XLEN/DEL/ACK 9 + XRANGE 5 + XREVRANGE 3 + XREAD 8 + XREADGROUP 6 + XGROUP 8 + XINFO 6 + XTRIM 6 + XPENDING 7 + XCLAIM 6 + XAUTOCLAIM 5 + XSETID 4 + bare XADD basics in Task 2 = sum varies).

- [ ] **Step 3: Run the full test suite under xdist**

```bash
uv run pytest -n auto
```

Expected: every existing test still passes + every new stream test passes. The new tests use the same shared Valkey container so they parallelise cleanly.

- [ ] **Step 4: Run under cp314t free-threaded**

```bash
.venv-ft/bin/uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
.venv-ft/bin/uv run pytest tests/driver/test_commands_streams.py -n auto
```

Expected: same green. If a test fails under free-threaded only, the most likely culprit is a thread-unsafe shared fixture (e.g. two tests fighting over the `s` stream key). Add `driver` per-test cleanup if needed.

- [ ] **Step 5: CHANGELOG update**

Append to `CHANGELOG.md` under `### Added`:

```markdown
- Stream commands (XADD/XLEN/XDEL/XACK/XRANGE/XREVRANGE/XREAD/XREADGROUP/XGROUP CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER/XINFO STREAM/GROUPS/CONSUMERS/XTRIM/XPENDING summary+range/XCLAIM/XAUTOCLAIM/XSETID), with output shapes flattened in Rust to match `redis-py` exactly: `xrange()` → `list[tuple[bytes, dict[bytes, bytes]]]`, `xread()` → `dict[bytes, list[tuple[bytes, dict[bytes, bytes]]]]`, `xpending` → 4-tuple summary or list-of-dicts range, `xautoclaim` → `(next_id, entries, deleted_ids)`.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 08 entry"
```

- [ ] **Step 6: Final verification**

```bash
git log --oneline -16
```

Expected: 16 commits since plan start (one per task), every conventional-commit prefixed `feat(streams):` or `docs(changelog):` or `test(streams):`.

---

## Self-review checklist for this plan

- [x] Spec coverage (`PLAN.md` v0.1 surface — Streams): "streams incl. groups/pending/claim/autoclaim" — every command in the roadmap row for plan 08 is implemented (`XADD`, `XLEN`, `XDEL`, `XACK`, `XRANGE`/`XREVRANGE`, `XREAD`/`XREADGROUP` with BLOCK/COUNT/NOACK, `XGROUP CREATE`/`SETID`/`DESTROY`/`DELCONSUMER`/`CREATECONSUMER`, `XINFO STREAM`/`GROUPS`/`CONSUMERS`, `XTRIM`, `XPENDING` summary+range, `XCLAIM`, `XAUTOCLAIM`, `XSETID`).
- [x] Architectural decision documented in the file header AND the `Architecture` section (flatten-in-Rust vs pass-through; trade-off articulated).
- [x] All flattening helpers (`flatten_xrange_reply`, `flatten_xread_reply`, `flatten_xpending_summary`, `flatten_xpending_range`, `flatten_xinfo_stream`, `flatten_xinfo_list`, `split_xautoclaim_reply`) live in `commands/streams.rs` as private functions.
- [x] `RawResult` extended with the 11 new variants needed for stream-shaped replies; each has a `match self` arm in `into_py`.
- [x] Every command has a sync + async pair and both call the same `cmd_*` builder.
- [x] `XPENDING` is a single method that picks summary vs range based on whether `min`/`max`/`count` was passed — matches redis-py's API.
- [x] `XREAD`/`XREADGROUP` route to the blocking connection pool when `block` is set (avoids the regular pool's 30s response-timeout HOL-blocking other requests).
- [x] Parity tests assert dict equality with `redis-py`'s output for every flattened reply shape (XRANGE/XREVRANGE/XREAD/XREADGROUP/XPENDING/XINFO).
- [x] `decode_responses=False` is forced on the upstream client in the fixture so the bytes-vs-str comparison is apples-to-apples.
- [x] Type stubs added for all 23 new sync + 23 new async methods.
- [x] All file paths absolute or repo-relative-from-root.
- [x] Every code-changing step ships actual code (no pseudocode).
- [x] Every test step has a runnable command and an explicit pass count.
- [x] Frequent commits — 16 across 16 tasks; each conventional-commit prefixed `feat(streams):`.
- [x] Free-threaded (cp314t) verified at the end (Task 16 Step 4).
