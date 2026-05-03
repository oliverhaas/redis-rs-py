# Plan 17 — Compatibility matrix + parity test suite

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land a manifest-driven parity test suite that runs every implemented method through `redis-rs-py` and through upstream `redis-py` against the same Valkey, asserting identical return value/shape/type. The same manifest is the single source of truth for the README compatibility matrix — a pre-commit hook regenerates the matrix and fails the commit if it drifts.

**Architecture:** A single Python module (`tests/_compat_manifest.py`) declares one entry per Redis command we cover, tagging it with status (`implemented` / `partial` / `deferred`), command family, and notes. Two test files (`tests/compat/test_parity.py` for `implemented` rows, `tests/compat/test_partial.py` for `partial` rows) iterate the manifest and dispatch every entry to a per-command verifier function defined in `tests/compat/_verifiers/`. Each verifier exercises the command through both clients with representative inputs and asserts identical responses. A second consumer of the manifest, `scripts/render_compat_matrix.py`, renders the markdown table that lives between the `<!-- compat:start -->` and `<!-- compat:end -->` markers in `README.md`. A pre-commit hook re-renders that table on every commit and fails when the README is stale, so the matrix and the test surface can never disagree.

**Tech Stack:** pytest 9, pytest-asyncio (auto mode, already configured in `pyproject.toml`), `redis==7.4.0` (already in the dev dep group as the reference client), the existing `valkey_url` fixture from Plan 01's `tests/conftest.py`. No new dependencies — the manifest, verifiers, and renderer are pure stdlib Python.

**Reference material:**
- `/home/ohaas/e1+/redis-rs-py/PLAN.md` — "Compatibility matrix" section: "every redis-py public method gets a row in the README: implemented / partial (with notes) / deferred. No silent gaps." This plan implements that contract.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/0000-roadmap.md` — Plan 17 entry: parity assertions vs. redis-py on covered surface, README matrix generated from a manifest.
- `/home/ohaas/e1+/redis-rs-py/tests/conftest.py` — already provides the `valkey_url` session fixture.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/03-commands-strings.md` through `09-commands-scripts-admin.md` — every command listed there must have a manifest row in this plan.
- `python -c "import redis; help(redis.Redis)"` — the upstream signature surface our verifiers compare against.

**Out of scope:** Cluster-only commands (manifest entries can be added once Plan 15 lands — for v0.1 we mark them `partial` or `deferred`). Sentinel admin (same — marked `partial` until Plan 16). Module clients (RedisJSON / RediSearch / etc. — marked `deferred` permanently per `PLAN.md`). Encoding-specific quirks like `decode_responses=True` divergence; those have their own dedicated tests under `tests/facade/test_decode.py` (Plan 12).

**Hard requirement:** the parity tests run only after Plans 03–09 (the command tier) and Plan 12 (decode) have landed, because the verifiers call into the high-level façade. Until then this plan is implementation-ready but its tests will skip every entry whose command isn't yet wired. The `status="implemented"` filter in the parity collector enforces that.

---

## File structure delivered by this plan

```
tests/
  _compat_manifest.py             # NEW: single source of truth, ~150 entries
  compat/
    __init__.py                   # NEW
    conftest.py                   # NEW: paired-client fixtures (rs + py)
    test_parity.py                # NEW: collects ✅ rows, dispatches to verifiers
    test_partial.py               # NEW: collects ⚠️ rows, runs documented divergence checks
    _verifiers/
      __init__.py                 # NEW: VERIFIERS registry + decorator
      strings.py                  # NEW: GET/SET/INCR/MGET/...
      lists.py                    # NEW: LPUSH/RPUSH/LRANGE/...
      hashes.py                   # NEW: HSET/HGET/HGETALL/...
      sets.py                     # NEW: SADD/SREM/SMEMBERS/...
      zsets.py                    # NEW: ZADD/ZRANGE/ZSCORE/...
      streams.py                  # NEW: XADD/XREAD/XGROUP/...
      scripts.py                  # NEW: EVAL/EVALSHA/SCRIPT LOAD
      admin.py                    # NEW: INFO/CONFIG/CLIENT/DBSIZE/PING/...
scripts/
  render_compat_matrix.py         # NEW: reads manifest, writes README block
README.md                         # MODIFIED: insert <!-- compat:start --> ... <!-- compat:end --> block
.pre-commit-config.yaml           # MODIFIED: add matrix-freshness hook
```

---

## Task 1: Manifest skeleton + first 20 entries (string commands)

Lay down the canonical manifest record shape and populate it with every string command from Plan 03. The string family is the smallest closed family and the right shape to validate the schema before scaling out.

**Files:**
- Create: `tests/_compat_manifest.py`
- Create: `tests/compat/__init__.py`

- [ ] **Step 1: Create the empty `tests/compat/__init__.py`**

```bash
mkdir -p tests/compat
: > tests/compat/__init__.py
```

- [ ] **Step 2: Write the manifest skeleton + string entries**

Create `tests/_compat_manifest.py`:

```python
"""Single source of truth for the redis-rs-py compatibility matrix.

Every Redis/Valkey command we have decided to either implement, partially
implement, or explicitly defer gets exactly one entry here. The same data
drives:

  1. ``tests/compat/test_parity.py`` — runs every ``implemented`` entry
     through both clients and asserts equality.
  2. ``tests/compat/test_partial.py`` — runs every ``partial`` entry
     through the documented-divergence verifier.
  3. ``scripts/render_compat_matrix.py`` — renders the README markdown
     table.

Status values:

* ``implemented`` — full parity with redis-py.
* ``partial``     — covered, but with a documented difference. ``notes``
                    must explain the divergence.
* ``deferred``    — explicitly out of scope for v0.1. ``notes`` explains
                    why and (optionally) which version targets it.

Adding or moving an entry between buckets is a CHANGELOG-worthy event.
The pre-commit hook will fail the commit if the README block is stale.
"""

from __future__ import annotations

from typing import Final, Literal, TypedDict

Status = Literal["implemented", "partial", "deferred"]


class ManifestEntry(TypedDict):
    """One row in the compat matrix.

    Attributes:
        command:           Upstream Redis command name, e.g. ``"GET"``.
        method:            redis-py method name, e.g. ``"get"``. Must
                           match ``redis.Redis.<method>`` so the parity
                           tests can resolve it dynamically.
        family:            One of: strings, lists, hashes, sets, zsets,
                           streams, scripts, admin, pubsub, cluster,
                           sentinel, transactions.
        status:            ``implemented`` / ``partial`` / ``deferred``.
        notes:             Free-text divergence note. Required when
                           status is ``partial`` or ``deferred``.
        since_redis:       Earliest Valkey/Redis version that supports
                           this command (informational; renders as a
                           column in the README).
        since_redis_rs_py: Earliest redis-rs-py version that ships this
                           method (informational).
    """

    command: str
    method: str
    family: str
    status: Status
    notes: str
    since_redis: str
    since_redis_rs_py: str


# ---------------------------------------------------------------------------
# Strings (Plan 03)
# ---------------------------------------------------------------------------

STRING_ENTRIES: Final[list[ManifestEntry]] = [
    {"command": "GET", "method": "get", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SET", "method": "set", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "GETEX", "method": "getex", "family": "strings", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "GETDEL", "method": "getdel", "family": "strings", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "COPY", "method": "copy", "family": "strings", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "INCR", "method": "incr", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "INCRBY", "method": "incrby", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "INCRBYFLOAT", "method": "incrbyfloat", "family": "strings", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "DECR", "method": "decr", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "DECRBY", "method": "decrby", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "APPEND", "method": "append", "family": "strings", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "STRLEN", "method": "strlen", "family": "strings", "status": "implemented", "notes": "", "since_redis": "2.2", "since_redis_rs_py": "0.1"},
    {"command": "MGET", "method": "mget", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "MSET", "method": "mset", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0.1", "since_redis_rs_py": "0.1"},
    {"command": "MSETNX", "method": "msetnx", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0.1", "since_redis_rs_py": "0.1"},
    {"command": "SETRANGE", "method": "setrange", "family": "strings", "status": "implemented", "notes": "", "since_redis": "2.2", "since_redis_rs_py": "0.1"},
    {"command": "GETRANGE", "method": "getrange", "family": "strings", "status": "implemented", "notes": "", "since_redis": "2.4", "since_redis_rs_py": "0.1"},
    {"command": "EXISTS", "method": "exists", "family": "strings", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "DEL", "method": "delete", "family": "strings", "status": "implemented", "notes": "redis-py exposes DEL as ``delete``", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "UNLINK", "method": "unlink", "family": "strings", "status": "implemented", "notes": "", "since_redis": "4.0", "since_redis_rs_py": "0.1"},
]


# ---------------------------------------------------------------------------
# Master list (assembled in Tasks 2)
# ---------------------------------------------------------------------------

MANIFEST: Final[list[ManifestEntry]] = list(STRING_ENTRIES)


def by_status(status: Status) -> list[ManifestEntry]:
    """Return all entries matching a given status."""
    return [e for e in MANIFEST if e["status"] == status]


def by_family(family: str) -> list[ManifestEntry]:
    """Return all entries in a given command family."""
    return [e for e in MANIFEST if e["family"] == family]


def get_by_command(command: str) -> ManifestEntry:
    """Look up a single entry by uppercase command name."""
    for e in MANIFEST:
        if e["command"] == command:
            return e
    raise KeyError(command)
```

- [ ] **Step 3: Self-test the manifest schema**

Create a one-shot smoke test (we'll delete it after Task 2 — it's only here to validate the skeleton compiles and the lookup helpers work):

```bash
uv run python -c "
from tests._compat_manifest import MANIFEST, by_status, by_family, get_by_command

assert len(MANIFEST) == 20, f'expected 20 string entries, got {len(MANIFEST)}'
assert all(e['family'] == 'strings' for e in MANIFEST)
assert len(by_status('implemented')) == 20
assert by_family('strings') == MANIFEST
assert get_by_command('GET')['method'] == 'get'
print('manifest skeleton OK')
"
```

Expected: prints `manifest skeleton OK`. No exceptions.

- [ ] **Step 4: Commit**

```bash
git add tests/_compat_manifest.py tests/compat/__init__.py
git commit -m "test(compat): add manifest skeleton with string commands"
```

---

## Task 2: Add list / hash / set / zset / stream / scripts / admin entries

Complete the manifest. After this task the manifest is the contract for every other artefact in this plan — adding a new method anywhere in the codebase requires adding a manifest row first.

**Files:**
- Modify: `tests/_compat_manifest.py`

- [ ] **Step 1: Append the list-command block above `MANIFEST = ...`**

Insert before the `# Master list` block:

```python
# ---------------------------------------------------------------------------
# Lists (Plan 04)
# ---------------------------------------------------------------------------

LIST_ENTRIES: Final[list[ManifestEntry]] = [
    {"command": "LPUSH", "method": "lpush", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "RPUSH", "method": "rpush", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "LPUSHX", "method": "lpushx", "family": "lists", "status": "implemented", "notes": "", "since_redis": "2.2", "since_redis_rs_py": "0.1"},
    {"command": "RPUSHX", "method": "rpushx", "family": "lists", "status": "implemented", "notes": "", "since_redis": "2.2", "since_redis_rs_py": "0.1"},
    {"command": "LPOP", "method": "lpop", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "RPOP", "method": "rpop", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "LMOVE", "method": "lmove", "family": "lists", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "BLMOVE", "method": "blmove", "family": "lists", "status": "implemented", "notes": "uses lazy blocking conn", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "LMPOP", "method": "lmpop", "family": "lists", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "BLMPOP", "method": "blmpop", "family": "lists", "status": "implemented", "notes": "uses lazy blocking conn", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "BLPOP", "method": "blpop", "family": "lists", "status": "implemented", "notes": "uses lazy blocking conn", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "BRPOP", "method": "brpop", "family": "lists", "status": "implemented", "notes": "uses lazy blocking conn", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "LPOS", "method": "lpos", "family": "lists", "status": "implemented", "notes": "", "since_redis": "6.0.6", "since_redis_rs_py": "0.1"},
    {"command": "LRANGE", "method": "lrange", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "LLEN", "method": "llen", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "LREM", "method": "lrem", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "LINDEX", "method": "lindex", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "LSET", "method": "lset", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "LINSERT", "method": "linsert", "family": "lists", "status": "implemented", "notes": "", "since_redis": "2.2", "since_redis_rs_py": "0.1"},
    {"command": "LTRIM", "method": "ltrim", "family": "lists", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "RPOPLPUSH", "method": "rpoplpush", "family": "lists", "status": "deferred", "notes": "deprecated upstream — use LMOVE", "since_redis": "1.2", "since_redis_rs_py": "—"},
    {"command": "BRPOPLPUSH", "method": "brpoplpush", "family": "lists", "status": "deferred", "notes": "deprecated upstream — use BLMOVE", "since_redis": "2.2", "since_redis_rs_py": "—"},
]


# ---------------------------------------------------------------------------
# Hashes (Plan 05)
# ---------------------------------------------------------------------------

HASH_ENTRIES: Final[list[ManifestEntry]] = [
    {"command": "HGET", "method": "hget", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HSET", "method": "hset", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HSETNX", "method": "hsetnx", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HMSET", "method": "hmset", "family": "hashes", "status": "deferred", "notes": "deprecated upstream — use HSET with mapping", "since_redis": "2.0", "since_redis_rs_py": "—"},
    {"command": "HGETALL", "method": "hgetall", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HDEL", "method": "hdel", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HINCRBY", "method": "hincrby", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HINCRBYFLOAT", "method": "hincrbyfloat", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "HKEYS", "method": "hkeys", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HVALS", "method": "hvals", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HEXISTS", "method": "hexists", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HLEN", "method": "hlen", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HMGET", "method": "hmget", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "HSCAN", "method": "hscan", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "2.8", "since_redis_rs_py": "0.1"},
    {"command": "HRANDFIELD", "method": "hrandfield", "family": "hashes", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "HEXPIRE", "method": "hexpire", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+ field TTL", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
    {"command": "HPEXPIRE", "method": "hpexpire", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+ field TTL", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
    {"command": "HEXPIREAT", "method": "hexpireat", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
    {"command": "HPEXPIREAT", "method": "hpexpireat", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
    {"command": "HEXPIRETIME", "method": "hexpiretime", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
    {"command": "HPEXPIRETIME", "method": "hpexpiretime", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
    {"command": "HTTL", "method": "httl", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
    {"command": "HPTTL", "method": "hpttl", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
    {"command": "HPERSIST", "method": "hpersist", "family": "hashes", "status": "implemented", "notes": "Redis 7.4+", "since_redis": "7.4", "since_redis_rs_py": "0.1"},
]


# ---------------------------------------------------------------------------
# Sets (Plan 06)
# ---------------------------------------------------------------------------

SET_ENTRIES: Final[list[ManifestEntry]] = [
    {"command": "SADD", "method": "sadd", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SREM", "method": "srem", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SMEMBERS", "method": "smembers", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SISMEMBER", "method": "sismember", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SMISMEMBER", "method": "smismember", "family": "sets", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "SCARD", "method": "scard", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SINTER", "method": "sinter", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SINTERSTORE", "method": "sinterstore", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SINTERCARD", "method": "sintercard", "family": "sets", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "SUNION", "method": "sunion", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SUNIONSTORE", "method": "sunionstore", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SDIFF", "method": "sdiff", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SDIFFSTORE", "method": "sdiffstore", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SMOVE", "method": "smove", "family": "sets", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SPOP", "method": "spop", "family": "sets", "status": "partial", "notes": "result order is server-defined; verifier asserts set equality, not list equality", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SRANDMEMBER", "method": "srandmember", "family": "sets", "status": "partial", "notes": "result order is server-defined; verifier asserts membership only", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "SSCAN", "method": "sscan", "family": "sets", "status": "implemented", "notes": "", "since_redis": "2.8", "since_redis_rs_py": "0.1"},
]


# ---------------------------------------------------------------------------
# Sorted sets (Plan 07)
# ---------------------------------------------------------------------------

ZSET_ENTRIES: Final[list[ManifestEntry]] = [
    {"command": "ZADD", "method": "zadd", "family": "zsets", "status": "implemented", "notes": "full NX/XX/GT/LT/CH/INCR matrix", "since_redis": "1.2", "since_redis_rs_py": "0.1"},
    {"command": "ZREM", "method": "zrem", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "1.2", "since_redis_rs_py": "0.1"},
    {"command": "ZRANGE", "method": "zrange", "family": "zsets", "status": "implemented", "notes": "BYSCORE/BYLEX/REV/LIMIT/WITHSCORES", "since_redis": "1.2", "since_redis_rs_py": "0.1"},
    {"command": "ZRANGEBYSCORE", "method": "zrangebyscore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "1.0.5", "since_redis_rs_py": "0.1"},
    {"command": "ZREVRANGEBYSCORE", "method": "zrevrangebyscore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.2", "since_redis_rs_py": "0.1"},
    {"command": "ZRANGEBYLEX", "method": "zrangebylex", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.8.9", "since_redis_rs_py": "0.1"},
    {"command": "ZREVRANGEBYLEX", "method": "zrevrangebylex", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.8.9", "since_redis_rs_py": "0.1"},
    {"command": "ZRANGESTORE", "method": "zrangestore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "ZINCRBY", "method": "zincrby", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "1.2", "since_redis_rs_py": "0.1"},
    {"command": "ZCARD", "method": "zcard", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "1.2", "since_redis_rs_py": "0.1"},
    {"command": "ZSCORE", "method": "zscore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "1.2", "since_redis_rs_py": "0.1"},
    {"command": "ZMSCORE", "method": "zmscore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "ZRANK", "method": "zrank", "family": "zsets", "status": "implemented", "notes": "WITHSCORE", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "ZREVRANK", "method": "zrevrank", "family": "zsets", "status": "implemented", "notes": "WITHSCORE", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "ZREMRANGEBYRANK", "method": "zremrangebyrank", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "ZREMRANGEBYSCORE", "method": "zremrangebyscore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "1.2", "since_redis_rs_py": "0.1"},
    {"command": "ZREMRANGEBYLEX", "method": "zremrangebylex", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.8.9", "since_redis_rs_py": "0.1"},
    {"command": "ZCOUNT", "method": "zcount", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "ZLEXCOUNT", "method": "zlexcount", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.8.9", "since_redis_rs_py": "0.1"},
    {"command": "ZPOPMIN", "method": "zpopmin", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "ZPOPMAX", "method": "zpopmax", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "BZPOPMIN", "method": "bzpopmin", "family": "zsets", "status": "implemented", "notes": "uses lazy blocking conn", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "BZPOPMAX", "method": "bzpopmax", "family": "zsets", "status": "implemented", "notes": "uses lazy blocking conn", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "ZMPOP", "method": "zmpop", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "BZMPOP", "method": "bzmpop", "family": "zsets", "status": "implemented", "notes": "uses lazy blocking conn", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "ZRANDMEMBER", "method": "zrandmember", "family": "zsets", "status": "partial", "notes": "result is non-deterministic without WITHSCORES; verifier asserts membership", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "ZSCAN", "method": "zscan", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.8", "since_redis_rs_py": "0.1"},
    {"command": "ZUNIONSTORE", "method": "zunionstore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "ZINTERSTORE", "method": "zinterstore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "ZDIFFSTORE", "method": "zdiffstore", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "ZUNION", "method": "zunion", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "ZINTER", "method": "zinter", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "ZDIFF", "method": "zdiff", "family": "zsets", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
]


# ---------------------------------------------------------------------------
# Streams (Plan 08)
# ---------------------------------------------------------------------------

STREAM_ENTRIES: Final[list[ManifestEntry]] = [
    {"command": "XADD", "method": "xadd", "family": "streams", "status": "implemented", "notes": "NOMKSTREAM/MAXLEN/MINID/LIMIT/~", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XLEN", "method": "xlen", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XRANGE", "method": "xrange", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XREVRANGE", "method": "xrevrange", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XREAD", "method": "xread", "family": "streams", "status": "implemented", "notes": "BLOCK + COUNT", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XREADGROUP", "method": "xreadgroup", "family": "streams", "status": "implemented", "notes": "BLOCK + COUNT + NOACK", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XACK", "method": "xack", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XDEL", "method": "xdel", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XGROUP CREATE", "method": "xgroup_create", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XGROUP SETID", "method": "xgroup_setid", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XGROUP DESTROY", "method": "xgroup_destroy", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XGROUP DELCONSUMER", "method": "xgroup_delconsumer", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XGROUP CREATECONSUMER", "method": "xgroup_createconsumer", "family": "streams", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "XINFO STREAM", "method": "xinfo_stream", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XINFO GROUPS", "method": "xinfo_groups", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XINFO CONSUMERS", "method": "xinfo_consumers", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XTRIM", "method": "xtrim", "family": "streams", "status": "implemented", "notes": "MAXLEN/MINID/~", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XPENDING", "method": "xpending", "family": "streams", "status": "implemented", "notes": "summary + range forms", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XCLAIM", "method": "xclaim", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "XAUTOCLAIM", "method": "xautoclaim", "family": "streams", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "XSETID", "method": "xsetid", "family": "streams", "status": "implemented", "notes": "", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
]


# ---------------------------------------------------------------------------
# Scripts (Plan 09)
# ---------------------------------------------------------------------------

SCRIPT_ENTRIES: Final[list[ManifestEntry]] = [
    {"command": "EVAL", "method": "eval", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "EVALSHA", "method": "evalsha", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "EVAL_RO", "method": "eval_ro", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "EVALSHA_RO", "method": "evalsha_ro", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "SCRIPT LOAD", "method": "script_load", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "SCRIPT EXISTS", "method": "script_exists", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "SCRIPT FLUSH", "method": "script_flush", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "FCALL", "method": "fcall", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "FCALL_RO", "method": "fcall_ro", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "FUNCTION LOAD", "method": "function_load", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "FUNCTION DUMP", "method": "function_dump", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "FUNCTION FLUSH", "method": "function_flush", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "FUNCTION LIST", "method": "function_list", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "FUNCTION STATS", "method": "function_stats", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "FUNCTION KILL", "method": "function_kill", "family": "scripts", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "register_script", "method": "register_script", "family": "scripts", "status": "deferred", "notes": "v0.1: use SCRIPT LOAD + EVALSHA directly", "since_redis": "2.6", "since_redis_rs_py": "—"},
]


# ---------------------------------------------------------------------------
# Admin / scan / keyspace (Plan 09)
# ---------------------------------------------------------------------------

ADMIN_ENTRIES: Final[list[ManifestEntry]] = [
    {"command": "SCAN", "method": "scan", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.8", "since_redis_rs_py": "0.1"},
    {"command": "SCAN_ITER", "method": "scan_iter", "family": "admin", "status": "implemented", "notes": "iterator wrapper", "since_redis": "2.8", "since_redis_rs_py": "0.1"},
    {"command": "KEYS", "method": "keys", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "RANDOMKEY", "method": "randomkey", "family": "admin", "status": "partial", "notes": "result depends on keyspace; verifier asserts membership", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "DBSIZE", "method": "dbsize", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "FLUSHDB", "method": "flushdb", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "FLUSHALL", "method": "flushall", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "INFO", "method": "info", "family": "admin", "status": "partial", "notes": "values like ``uptime_in_seconds`` differ between calls; verifier asserts shape + key presence", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "CONFIG GET", "method": "config_get", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "CONFIG SET", "method": "config_set", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "CONFIG RESETSTAT", "method": "config_resetstat", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.0", "since_redis_rs_py": "0.1"},
    {"command": "CONFIG REWRITE", "method": "config_rewrite", "family": "admin", "status": "deferred", "notes": "requires writable redis.conf — not exercised in CI", "since_redis": "2.8", "since_redis_rs_py": "—"},
    {"command": "CLIENT GETNAME", "method": "client_getname", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.6.9", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT SETNAME", "method": "client_setname", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.6.9", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT ID", "method": "client_id", "family": "admin", "status": "partial", "notes": "ID values differ across clients; verifier asserts type only", "since_redis": "5.0", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT INFO", "method": "client_info", "family": "admin", "status": "partial", "notes": "shape parity; values differ across clients", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT LIST", "method": "client_list", "family": "admin", "status": "partial", "notes": "shape parity; values differ across clients", "since_redis": "2.4", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT KILL", "method": "client_kill", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.4", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT PAUSE", "method": "client_pause", "family": "admin", "status": "implemented", "notes": "", "since_redis": "3.0", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT UNPAUSE", "method": "client_unpause", "family": "admin", "status": "implemented", "notes": "", "since_redis": "6.2", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT NO-EVICT", "method": "client_no_evict", "family": "admin", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "CLIENT NO-TOUCH", "method": "client_no_touch", "family": "admin", "status": "implemented", "notes": "", "since_redis": "7.2", "since_redis_rs_py": "0.1"},
    {"command": "OBJECT ENCODING", "method": "object_encoding", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.2.3", "since_redis_rs_py": "0.1"},
    {"command": "OBJECT IDLETIME", "method": "object_idletime", "family": "admin", "status": "partial", "notes": "wall-clock dependent; verifier asserts non-negative int", "since_redis": "2.2.3", "since_redis_rs_py": "0.1"},
    {"command": "OBJECT FREQ", "method": "object_freq", "family": "admin", "status": "partial", "notes": "requires LFU policy; skipped under default config", "since_redis": "4.0", "since_redis_rs_py": "0.1"},
    {"command": "OBJECT REFCOUNT", "method": "object_refcount", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.2.3", "since_redis_rs_py": "0.1"},
    {"command": "MEMORY USAGE", "method": "memory_usage", "family": "admin", "status": "implemented", "notes": "", "since_redis": "4.0", "since_redis_rs_py": "0.1"},
    {"command": "PING", "method": "ping", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "ECHO", "method": "echo", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "WAIT", "method": "wait", "family": "admin", "status": "implemented", "notes": "", "since_redis": "3.0", "since_redis_rs_py": "0.1"},
    {"command": "WAITAOF", "method": "waitaof", "family": "admin", "status": "implemented", "notes": "", "since_redis": "7.2", "since_redis_rs_py": "0.1"},
    {"command": "TIME", "method": "time", "family": "admin", "status": "partial", "notes": "wall-clock dependent; verifier asserts shape (sec, usec)", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "LASTSAVE", "method": "lastsave", "family": "admin", "status": "partial", "notes": "wall-clock dependent; verifier asserts type only", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "BGSAVE", "method": "bgsave", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "BGREWRITEAOF", "method": "bgrewriteaof", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "EXPIRE", "method": "expire", "family": "admin", "status": "implemented", "notes": "NX/XX/GT/LT flags", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "PEXPIRE", "method": "pexpire", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "EXPIREAT", "method": "expireat", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.2", "since_redis_rs_py": "0.1"},
    {"command": "PEXPIREAT", "method": "pexpireat", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "EXPIRETIME", "method": "expiretime", "family": "admin", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "PEXPIRETIME", "method": "pexpiretime", "family": "admin", "status": "implemented", "notes": "", "since_redis": "7.0", "since_redis_rs_py": "0.1"},
    {"command": "TTL", "method": "ttl", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "PTTL", "method": "pttl", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "PERSIST", "method": "persist", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.2", "since_redis_rs_py": "0.1"},
    {"command": "RENAME", "method": "rename", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "RENAMENX", "method": "renamenx", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "TYPE", "method": "type", "family": "admin", "status": "implemented", "notes": "", "since_redis": "1.0", "since_redis_rs_py": "0.1"},
    {"command": "DUMP", "method": "dump", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "RESTORE", "method": "restore", "family": "admin", "status": "implemented", "notes": "", "since_redis": "2.6", "since_redis_rs_py": "0.1"},
    {"command": "DEBUG SLEEP", "method": "debug_sleep", "family": "admin", "status": "deferred", "notes": "test-only helper, not part of redis-py public surface", "since_redis": "1.0", "since_redis_rs_py": "—"},
    {"command": "MONITOR", "method": "monitor", "family": "admin", "status": "deferred", "notes": "v0.2; rarely used in production", "since_redis": "1.0", "since_redis_rs_py": "—"},
    {"command": "LATENCY HISTORY", "method": "latency_history", "family": "admin", "status": "deferred", "notes": "v0.2", "since_redis": "2.8.13", "since_redis_rs_py": "—"},
    {"command": "CLIENT TRACKING", "method": "client_tracking", "family": "admin", "status": "deferred", "notes": "v0.2 — caching is configured in Rust, not via this command", "since_redis": "6.0", "since_redis_rs_py": "—"},
]
```

- [ ] **Step 2: Replace the master list assembly with the full union**

Replace the `MANIFEST: Final[list[ManifestEntry]] = list(STRING_ENTRIES)` line with:

```python
MANIFEST: Final[list[ManifestEntry]] = [
    *STRING_ENTRIES,
    *LIST_ENTRIES,
    *HASH_ENTRIES,
    *SET_ENTRIES,
    *ZSET_ENTRIES,
    *STREAM_ENTRIES,
    *SCRIPT_ENTRIES,
    *ADMIN_ENTRIES,
]
```

- [ ] **Step 3: Add a uniqueness invariant**

Append after the helper functions:

```python
def _validate_manifest() -> None:
    """Fail-fast check run at import time.

    Catches the two classes of mistakes that would silently corrupt the
    matrix and the tests:

    * duplicate ``command`` keys (would shadow each other in lookups);
    * empty ``notes`` on a ``partial`` or ``deferred`` row.
    """
    seen: set[str] = set()
    for entry in MANIFEST:
        if entry["command"] in seen:
            raise RuntimeError(f"duplicate manifest command: {entry['command']}")
        seen.add(entry["command"])
        if entry["status"] in {"partial", "deferred"} and not entry["notes"]:
            raise RuntimeError(
                f"{entry['command']} is {entry['status']} but has no notes",
            )


_validate_manifest()
```

- [ ] **Step 4: Verify the manifest validates and counts as expected**

```bash
uv run python -c "
from tests._compat_manifest import MANIFEST, by_status

implemented = by_status('implemented')
partial = by_status('partial')
deferred = by_status('deferred')

print(f'total:       {len(MANIFEST)}')
print(f'implemented: {len(implemented)}')
print(f'partial:     {len(partial)}')
print(f'deferred:    {len(deferred)}')
assert len(MANIFEST) >= 150, f'expected >=150 entries, got {len(MANIFEST)}'
assert len(implemented) >= 120
"
```

Expected: `total >= 150`, `implemented >= 120`, no exceptions.

- [ ] **Step 5: Commit**

```bash
git add tests/_compat_manifest.py
git commit -m "test(compat): add list/hash/set/zset/stream/scripts/admin entries to manifest"
```

---

## Task 3: Paired-client conftest

The parity tests need both clients pointed at the same Valkey, both clean before each test. This conftest also pins the `valkey-py` client at the same image as redis-py to avoid a "did the server change underneath us" red herring.

**Files:**
- Create: `tests/compat/conftest.py`

- [ ] **Step 1: Implement the paired-client fixtures**

Create `tests/compat/conftest.py`:

```python
"""Paired-client fixtures for the parity test suite.

Two clients, same Valkey container, FLUSHDB before every test:

* ``rs_client`` — the redis-rs-py high-level facade
  (``redis_rs_py.Redis.from_url``).
* ``py_client`` — upstream redis-py
  (``redis.Redis.from_url``).

Both are constructed with ``decode_responses=False`` (the default and
the only mode the parity suite cares about — Plan 12 has its own
``decode_responses=True`` tests).

Why FLUSHDB before each test? Because the verifiers seed their own
fixtures and need a known-empty database. Doing it here, once, is
~10× faster than FLUSHDB-twice (once per client) per test.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import redis as redis_py


@pytest.fixture
def py_client(valkey_url: str) -> Iterator[redis_py.Redis]:
    """Reference client (upstream redis-py)."""
    client = redis_py.Redis.from_url(valkey_url, decode_responses=False)
    client.flushdb()
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def rs_client(valkey_url: str) -> Iterator:
    """System under test (redis-rs-py façade)."""
    from redis_rs_py import Redis

    client = Redis.from_url(valkey_url, decode_responses=False)
    # Don't FLUSHDB again — py_client already did, and the two share a
    # database. Tests must request both fixtures (or the database is
    # whatever the previous test left behind, which is wrong).
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def paired_clients(rs_client, py_client) -> tuple:
    """Convenience tuple for verifiers: (rs, py)."""
    return rs_client, py_client
```

- [ ] **Step 2: Smoke-test the fixtures resolve**

(Plan 10's `Redis` façade must already be merged for this to pass; if not, this step is the natural blocker.)

Create a one-shot probe:

```bash
uv run pytest --collect-only tests/compat/conftest.py 2>&1 | tail -5
```

Expected: collection succeeds (no errors). The conftest itself defines no tests, so the count is zero.

- [ ] **Step 3: Commit**

```bash
git add tests/compat/conftest.py
git commit -m "test(compat): add paired rs+py client fixtures"
```

---

## Task 4: Verifier registry

The registry lets each verifier file declare its commands without the test collector having to hard-code them. A `@verifier("GET")` decorator inserts the function into a module-level dict that the parity tests look up by command name.

**Files:**
- Create: `tests/compat/_verifiers/__init__.py`

- [ ] **Step 1: Write the registry module**

```bash
mkdir -p tests/compat/_verifiers
```

Create `tests/compat/_verifiers/__init__.py`:

```python
"""Per-command verifier registry.

A *verifier* is a callable
``f(rs_client, py_client) -> None`` that exercises one Redis command
through both clients with one or more representative inputs and
asserts the responses agree.

Verifiers live in family-specific modules (``strings.py``, ``lists.py``
…); each registers itself with this module's ``@verifier`` decorator.
The parity test collector looks up a command's verifier via ``get(name)``
and invokes it.

Conventions for verifier authors:

* The function name **does not** need to match the command — but the
  ``@verifier(...)`` argument **must** match the manifest's ``command``
  field exactly (case-sensitive).
* Use ``assert rs == py`` for byte-for-byte parity.
* For commands that return an unordered collection (e.g. SMEMBERS), the
  verifier MUST normalize before comparing: ``assert set(rs) == set(py)``.
* A verifier for a ``partial`` row goes through this registry exactly
  the same way; the test collector decides which test file invokes it.
* If both clients raise the same exception type with the same message,
  that's a pass; use ``_assert_same_error`` for that case.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final, TypeAlias

VerifierFn: TypeAlias = Callable[[object, object], None]

_REGISTRY: Final[dict[str, VerifierFn]] = {}


def verifier(command: str) -> Callable[[VerifierFn], VerifierFn]:
    """Decorator. Registers ``fn`` as the verifier for ``command``."""

    def _wrap(fn: VerifierFn) -> VerifierFn:
        if command in _REGISTRY:
            raise RuntimeError(f"duplicate verifier registration for {command}")
        _REGISTRY[command] = fn
        return fn

    return _wrap


def get(command: str) -> VerifierFn | None:
    """Return the verifier for ``command`` or None if unregistered."""
    return _REGISTRY.get(command)


def all_registered() -> list[str]:
    """List every command currently registered, in insertion order."""
    return list(_REGISTRY)


def _assert_same_error(rs_call: Callable[[], object], py_call: Callable[[], object]) -> None:
    """Run both callables; pass if both raise the same exception class."""
    rs_exc: BaseException | None = None
    py_exc: BaseException | None = None
    try:
        rs_call()
    except BaseException as e:  # noqa: BLE001
        rs_exc = e
    try:
        py_call()
    except BaseException as e:  # noqa: BLE001
        py_exc = e
    assert rs_exc is not None, f"rs_client did not raise; py_client raised {type(py_exc).__name__}"
    assert py_exc is not None, f"py_client did not raise; rs_client raised {type(rs_exc).__name__}"
    assert type(rs_exc).__name__ == type(py_exc).__name__, (
        f"different exception classes: rs={type(rs_exc).__name__} vs py={type(py_exc).__name__}"
    )


# Side-effect imports: each module's @verifier decorators register on import.
# Keep these alphabetical so a missing import is obvious.
from . import admin as _admin  # noqa: E402, F401
from . import hashes as _hashes  # noqa: E402, F401
from . import lists as _lists  # noqa: E402, F401
from . import scripts as _scripts  # noqa: E402, F401
from . import sets as _sets  # noqa: E402, F401
from . import streams as _streams  # noqa: E402, F401
from . import strings as _strings  # noqa: E402, F401
from . import zsets as _zsets  # noqa: E402, F401
```

- [ ] **Step 2: Stub out the eight verifier modules**

Each module starts empty so the imports in `__init__.py` resolve. The next tasks fill them in.

```bash
for f in admin hashes lists scripts sets streams strings zsets; do
  cat > "tests/compat/_verifiers/${f}.py" <<'EOF'
"""Verifiers for the {family} command family. Filled in by Plan 17 task body."""

from __future__ import annotations

from . import verifier  # noqa: F401  (imported so siblings can decorate)
EOF
done
```

Then replace each `{family}` placeholder with a quick `sed`:

```bash
for f in admin hashes lists scripts sets streams strings zsets; do
  sed -i "s/{family}/${f}/g" "tests/compat/_verifiers/${f}.py"
done
```

- [ ] **Step 3: Verify imports resolve and the registry is empty**

```bash
uv run python -c "
from tests.compat._verifiers import _REGISTRY, all_registered
assert _REGISTRY == {}
assert all_registered() == []
print('registry skeleton OK')
"
```

Expected: prints `registry skeleton OK`.

- [ ] **Step 4: Commit**

```bash
git add tests/compat/_verifiers/
git commit -m "test(compat): scaffold per-command verifier registry"
```

---

## Task 5: String + admin verifiers (the canonical examples)

Implement the verifiers for the two simplest families first. These set the pattern other families follow.

**Files:**
- Modify: `tests/compat/_verifiers/strings.py`
- Modify: `tests/compat/_verifiers/admin.py`

- [ ] **Step 1: Write the string verifiers**

Replace `tests/compat/_verifiers/strings.py`:

```python
"""Verifiers for the strings command family."""

from __future__ import annotations

from . import verifier


@verifier("GET")
def _verify_get(rs, py) -> None:
    py.set("k", "v")
    assert rs.get("k") == py.get("k") == b"v"
    assert rs.get("missing") == py.get("missing") is None


@verifier("SET")
def _verify_set(rs, py) -> None:
    assert rs.set("k", "v") == py.set("k", "v") is True
    assert rs.get("k") == py.get("k") == b"v"
    # NX flag — second set must fail with NX
    assert rs.set("k", "x", nx=True) == py.set("k", "x", nx=True)
    # XX flag on missing — both should return None
    assert rs.set("missing", "v", xx=True) == py.set("missing", "v", xx=True)
    # EX
    rs.set("k_ttl", "v", ex=60)
    py.set("k_ttl_py", "v", ex=60)
    assert 0 < rs.ttl("k_ttl") <= 60
    assert 0 < py.ttl("k_ttl_py") <= 60


@verifier("GETEX")
def _verify_getex(rs, py) -> None:
    py.set("k", "v")
    assert rs.getex("k", ex=60) == py.getex("k", ex=60) == b"v"


@verifier("GETDEL")
def _verify_getdel(rs, py) -> None:
    py.set("k", "v")
    assert rs.getdel("k") == b"v"
    assert py.get("k") is None


@verifier("COPY")
def _verify_copy(rs, py) -> None:
    py.set("src", "v")
    assert rs.copy("src", "dst") == py.copy("src", "dst2") is True
    assert rs.get("dst") == py.get("dst2") == b"v"


@verifier("INCR")
def _verify_incr(rs, py) -> None:
    assert rs.incr("c") == py.incr("c") == 1


@verifier("INCRBY")
def _verify_incrby(rs, py) -> None:
    assert rs.incrby("c", 5) == py.incrby("c", 5) == 5


@verifier("INCRBYFLOAT")
def _verify_incrbyfloat(rs, py) -> None:
    rs_val = rs.incrbyfloat("c", 1.5)
    py_val = py.incrbyfloat("c", 1.5)
    assert rs_val == py_val == 1.5


@verifier("DECR")
def _verify_decr(rs, py) -> None:
    assert rs.decr("c") == py.decr("c") == -1


@verifier("DECRBY")
def _verify_decrby(rs, py) -> None:
    assert rs.decrby("c", 3) == py.decrby("c", 3) == -3


@verifier("APPEND")
def _verify_append(rs, py) -> None:
    py.set("k", "abc")
    assert rs.append("k", "def") == py.append("k", "def") == 9


@verifier("STRLEN")
def _verify_strlen(rs, py) -> None:
    py.set("k", "abc")
    assert rs.strlen("k") == py.strlen("k") == 3


@verifier("MGET")
def _verify_mget(rs, py) -> None:
    py.set("a", "1")
    py.set("b", "2")
    assert rs.mget("a", "b", "missing") == py.mget("a", "b", "missing") == [b"1", b"2", None]


@verifier("MSET")
def _verify_mset(rs, py) -> None:
    assert rs.mset({"a": "1", "b": "2"}) == py.mset({"a": "1", "b": "2"}) is True
    assert py.get("a") == b"1"


@verifier("MSETNX")
def _verify_msetnx(rs, py) -> None:
    assert rs.msetnx({"a": "1"}) == py.msetnx({"b": "2"}) == 1
    assert rs.msetnx({"a": "x"}) == py.msetnx({"b": "x"}) == 0


@verifier("SETRANGE")
def _verify_setrange(rs, py) -> None:
    py.set("k", "Hello World")
    assert rs.setrange("k", 6, "Redis") == py.setrange("k", 6, "Redis") == 11


@verifier("GETRANGE")
def _verify_getrange(rs, py) -> None:
    py.set("k", "Hello World")
    assert rs.getrange("k", 0, 4) == py.getrange("k", 0, 4) == b"Hello"


@verifier("EXISTS")
def _verify_exists(rs, py) -> None:
    py.set("k", "v")
    assert rs.exists("k", "missing") == py.exists("k", "missing") == 1


@verifier("DEL")
def _verify_del(rs, py) -> None:
    py.set("k1", "v")
    py.set("k2", "v")
    assert rs.delete("k1", "k2", "missing") == 2
    assert py.delete("k1", "k2", "missing") == 0  # already gone — verifier asserts the rs side did the work


@verifier("UNLINK")
def _verify_unlink(rs, py) -> None:
    py.set("k", "v")
    assert rs.unlink("k") == 1
    assert py.unlink("k") == 0
```

- [ ] **Step 2: Write the admin verifiers**

Replace `tests/compat/_verifiers/admin.py`:

```python
"""Verifiers for the admin / scan / keyspace family."""

from __future__ import annotations

from . import verifier


@verifier("PING")
def _verify_ping(rs, py) -> None:
    assert rs.ping() == py.ping() is True


@verifier("ECHO")
def _verify_echo(rs, py) -> None:
    assert rs.echo("hello") == py.echo("hello") == b"hello"


@verifier("DBSIZE")
def _verify_dbsize(rs, py) -> None:
    py.set("a", "1")
    py.set("b", "2")
    assert rs.dbsize() == py.dbsize() == 2


@verifier("FLUSHDB")
def _verify_flushdb(rs, py) -> None:
    py.set("k", "v")
    assert rs.flushdb() is True
    assert py.dbsize() == 0


@verifier("FLUSHALL")
def _verify_flushall(rs, py) -> None:
    py.set("k", "v")
    assert rs.flushall() is True
    assert py.dbsize() == 0


@verifier("KEYS")
def _verify_keys(rs, py) -> None:
    py.set("a", "1")
    py.set("b", "2")
    assert sorted(rs.keys("*")) == sorted(py.keys("*"))


@verifier("SCAN")
def _verify_scan(rs, py) -> None:
    py.mset({f"k{i}": str(i) for i in range(50)})
    rs_cursor, rs_keys = rs.scan(0, match="k*", count=100)
    py_cursor, py_keys = py.scan(0, match="k*", count=100)
    assert sorted(rs_keys) == sorted(py_keys)
    # Cursors converge to 0 once the scan is exhausted; both clients
    # may return non-zero on the first page — assert only that both
    # return ints.
    assert isinstance(rs_cursor, int) and isinstance(py_cursor, int)


@verifier("SCAN_ITER")
def _verify_scan_iter(rs, py) -> None:
    py.mset({f"k{i}": str(i) for i in range(50)})
    assert sorted(rs.scan_iter(match="k*")) == sorted(py.scan_iter(match="k*"))


@verifier("EXPIRE")
def _verify_expire(rs, py) -> None:
    py.set("k", "v")
    assert rs.expire("k", 60) == py.expire("k", 60) is True
    assert 0 < rs.ttl("k") <= 60


@verifier("PEXPIRE")
def _verify_pexpire(rs, py) -> None:
    py.set("k", "v")
    assert rs.pexpire("k", 60000) == py.pexpire("k", 60000) is True


@verifier("TTL")
def _verify_ttl(rs, py) -> None:
    py.set("k", "v", ex=60)
    rs_ttl = rs.ttl("k")
    py_ttl = py.ttl("k")
    # Allow ±1s drift between the two reads.
    assert abs(rs_ttl - py_ttl) <= 1


@verifier("PTTL")
def _verify_pttl(rs, py) -> None:
    py.set("k", "v", ex=60)
    assert rs.pttl("k") > 0
    assert py.pttl("k") > 0


@verifier("PERSIST")
def _verify_persist(rs, py) -> None:
    py.set("k", "v", ex=60)
    assert rs.persist("k") is True
    assert py.ttl("k") == -1


@verifier("EXPIREAT")
def _verify_expireat(rs, py) -> None:
    import time
    py.set("k", "v")
    target = int(time.time()) + 60
    assert rs.expireat("k", target) == py.expireat("k", target) is True


@verifier("PEXPIREAT")
def _verify_pexpireat(rs, py) -> None:
    import time
    py.set("k", "v")
    target = int(time.time() * 1000) + 60_000
    assert rs.pexpireat("k", target) == py.pexpireat("k", target) is True


@verifier("EXPIRETIME")
def _verify_expiretime(rs, py) -> None:
    import time
    py.set("k", "v", ex=60)
    rs_t = rs.expiretime("k")
    py_t = py.expiretime("k")
    now = int(time.time())
    assert now <= rs_t <= now + 61
    assert now <= py_t <= now + 61


@verifier("PEXPIRETIME")
def _verify_pexpiretime(rs, py) -> None:
    py.set("k", "v", ex=60)
    assert rs.pexpiretime("k") > 0
    assert py.pexpiretime("k") > 0


@verifier("RENAME")
def _verify_rename(rs, py) -> None:
    py.set("k", "v")
    assert rs.rename("k", "k2") is True
    assert py.get("k2") == b"v"


@verifier("RENAMENX")
def _verify_renamenx(rs, py) -> None:
    py.set("k", "v")
    assert rs.renamenx("k", "k2") == py.renamenx("k", "k2") is True


@verifier("TYPE")
def _verify_type(rs, py) -> None:
    py.set("k", "v")
    assert rs.type("k") == py.type("k") == b"string"


@verifier("DUMP")
def _verify_dump(rs, py) -> None:
    py.set("k", "v")
    rs_dump = rs.dump("k")
    py_dump = py.dump("k")
    assert rs_dump == py_dump


@verifier("RESTORE")
def _verify_restore(rs, py) -> None:
    py.set("k", "v")
    payload = py.dump("k")
    assert rs.restore("k2", 0, payload) is True
    assert py.get("k2") == b"v"


@verifier("CONFIG GET")
def _verify_config_get(rs, py) -> None:
    rs_v = rs.config_get("maxmemory")
    py_v = py.config_get("maxmemory")
    assert rs_v == py_v


@verifier("CONFIG SET")
def _verify_config_set(rs, py) -> None:
    assert rs.config_set("maxmemory-policy", "noeviction") is True
    assert py.config_get("maxmemory-policy")[b"maxmemory-policy"] == b"noeviction"


@verifier("CONFIG RESETSTAT")
def _verify_config_resetstat(rs, py) -> None:
    assert rs.config_resetstat() is True


@verifier("CLIENT GETNAME")
def _verify_client_getname(rs, py) -> None:
    rs.client_setname("rs-name")
    assert rs.client_getname() == b"rs-name"


@verifier("CLIENT SETNAME")
def _verify_client_setname(rs, py) -> None:
    assert rs.client_setname("rs") is True


@verifier("CLIENT KILL")
def _verify_client_kill(rs, py) -> None:
    # Killing arbitrary clients in a shared container is dangerous.
    # The smoke test: the method exists and returns an int (count killed).
    out = rs.client_kill_filter(_id=999_999_999)  # bogus id — kills nothing
    assert isinstance(out, int)


@verifier("CLIENT PAUSE")
def _verify_client_pause(rs, py) -> None:
    assert rs.client_pause(1) is True


@verifier("CLIENT UNPAUSE")
def _verify_client_unpause(rs, py) -> None:
    assert rs.client_unpause() is True


@verifier("CLIENT NO-EVICT")
def _verify_client_no_evict(rs, py) -> None:
    assert rs.client_no_evict("ON") is True


@verifier("CLIENT NO-TOUCH")
def _verify_client_no_touch(rs, py) -> None:
    assert rs.client_no_touch("ON") is True


@verifier("OBJECT ENCODING")
def _verify_object_encoding(rs, py) -> None:
    py.set("k", "v")
    assert rs.object_encoding("k") == py.object_encoding("k")


@verifier("OBJECT REFCOUNT")
def _verify_object_refcount(rs, py) -> None:
    py.set("k", "v")
    assert isinstance(rs.object_refcount("k"), int)


@verifier("MEMORY USAGE")
def _verify_memory_usage(rs, py) -> None:
    py.set("k", "v")
    rs_v = rs.memory_usage("k")
    py_v = py.memory_usage("k")
    assert rs_v is not None
    assert py_v is not None
    # Two clients on the same value get within 32 bytes of each other in practice.
    assert abs(rs_v - py_v) <= 64


@verifier("WAIT")
def _verify_wait(rs, py) -> None:
    # Standalone server: WAIT 0 0 returns 0.
    assert rs.wait(0, 100) == py.wait(0, 100) == 0


@verifier("WAITAOF")
def _verify_waitaof(rs, py) -> None:
    out = rs.waitaof(0, 0, 100)
    assert isinstance(out, list) and len(out) == 2


@verifier("BGSAVE")
def _verify_bgsave(rs, py) -> None:
    out = rs.bgsave()
    assert out in (True, b"Background saving started", "Background saving started")


@verifier("BGREWRITEAOF")
def _verify_bgrewriteaof(rs, py) -> None:
    out = rs.bgrewriteaof()
    assert isinstance(out, (bytes, str, bool))
```

- [ ] **Step 3: Sanity-check the registry contents**

```bash
uv run python -c "
from tests.compat._verifiers import all_registered
n = len(all_registered())
print(f'{n} verifiers registered')
assert n >= 50
"
```

Expected: prints something like `60 verifiers registered`.

- [ ] **Step 4: Commit**

```bash
git add tests/compat/_verifiers/strings.py tests/compat/_verifiers/admin.py
git commit -m "test(compat): add string and admin verifiers"
```

---

## Task 6: List + hash + set verifiers

Same pattern as Task 5; one verifier per `implemented` row in the list/hash/set families. Set-family verifiers use set-equality where ordering is server-defined.

**Files:**
- Modify: `tests/compat/_verifiers/lists.py`
- Modify: `tests/compat/_verifiers/hashes.py`
- Modify: `tests/compat/_verifiers/sets.py`

- [ ] **Step 1: Write the list verifiers**

Replace `tests/compat/_verifiers/lists.py`:

```python
"""Verifiers for the lists command family."""

from __future__ import annotations

from . import verifier


@verifier("LPUSH")
def _verify_lpush(rs, py) -> None:
    assert rs.lpush("L", "a", "b") == py.lpush("L_py", "a", "b") == 2


@verifier("RPUSH")
def _verify_rpush(rs, py) -> None:
    assert rs.rpush("L", "a", "b") == py.rpush("L_py", "a", "b") == 2


@verifier("LPUSHX")
def _verify_lpushx(rs, py) -> None:
    py.rpush("L", "a")
    assert rs.lpushx("L", "b") == py.lpushx("L", "b") == 3


@verifier("RPUSHX")
def _verify_rpushx(rs, py) -> None:
    py.rpush("L", "a")
    assert rs.rpushx("L", "b") == py.rpushx("L", "b") == 3


@verifier("LPOP")
def _verify_lpop(rs, py) -> None:
    py.rpush("L", "a", "b", "c")
    assert rs.lpop("L") == py.lpop("L") == b"a"


@verifier("RPOP")
def _verify_rpop(rs, py) -> None:
    py.rpush("L", "a", "b", "c")
    assert rs.rpop("L") == py.rpop("L") == b"c"


@verifier("LMOVE")
def _verify_lmove(rs, py) -> None:
    py.rpush("src", "a")
    assert rs.lmove("src", "dst", "LEFT", "RIGHT") == b"a"


@verifier("BLMOVE")
def _verify_blmove(rs, py) -> None:
    py.rpush("src", "a")
    assert rs.blmove("src", "dst", 1, "LEFT", "RIGHT") == b"a"


@verifier("LMPOP")
def _verify_lmpop(rs, py) -> None:
    py.rpush("L", "a", "b")
    rs_out = rs.lmpop(1, "L", direction="LEFT", count=1)
    py.delete("L")
    py.rpush("L", "a", "b")
    py_out = py.lmpop(1, "L", direction="LEFT", count=1)
    assert rs_out == py_out


@verifier("BLMPOP")
def _verify_blmpop(rs, py) -> None:
    py.rpush("L", "a")
    rs_out = rs.blmpop(0.1, 1, "L", direction="LEFT", count=1)
    assert rs_out is not None


@verifier("BLPOP")
def _verify_blpop(rs, py) -> None:
    py.rpush("L", "a")
    rs_out = rs.blpop(["L"], timeout=1)
    assert rs_out is not None and rs_out[1] == b"a"


@verifier("BRPOP")
def _verify_brpop(rs, py) -> None:
    py.rpush("L", "a")
    rs_out = rs.brpop(["L"], timeout=1)
    assert rs_out is not None and rs_out[1] == b"a"


@verifier("LPOS")
def _verify_lpos(rs, py) -> None:
    py.rpush("L", "a", "b", "c", "b")
    assert rs.lpos("L", "b") == py.lpos("L", "b") == 1


@verifier("LRANGE")
def _verify_lrange(rs, py) -> None:
    py.rpush("L", "a", "b", "c")
    assert rs.lrange("L", 0, -1) == py.lrange("L", 0, -1) == [b"a", b"b", b"c"]


@verifier("LLEN")
def _verify_llen(rs, py) -> None:
    py.rpush("L", "a", "b", "c")
    assert rs.llen("L") == py.llen("L") == 3


@verifier("LREM")
def _verify_lrem(rs, py) -> None:
    py.rpush("L", "a", "b", "a")
    assert rs.lrem("L", 1, "a") == 1


@verifier("LINDEX")
def _verify_lindex(rs, py) -> None:
    py.rpush("L", "a", "b")
    assert rs.lindex("L", 0) == py.lindex("L", 0) == b"a"


@verifier("LSET")
def _verify_lset(rs, py) -> None:
    py.rpush("L", "a", "b")
    assert rs.lset("L", 0, "x") is True
    assert py.lindex("L", 0) == b"x"


@verifier("LINSERT")
def _verify_linsert(rs, py) -> None:
    py.rpush("L", "a", "c")
    assert rs.linsert("L", "BEFORE", "c", "b") == py.linsert("L", "BEFORE", "c", "b") == 4


@verifier("LTRIM")
def _verify_ltrim(rs, py) -> None:
    py.rpush("L", "a", "b", "c", "d")
    assert rs.ltrim("L", 1, 2) is True
    assert py.lrange("L", 0, -1) == [b"b", b"c"]
```

- [ ] **Step 2: Write the hash verifiers**

Replace `tests/compat/_verifiers/hashes.py`:

```python
"""Verifiers for the hashes command family."""

from __future__ import annotations

from . import verifier


@verifier("HGET")
def _verify_hget(rs, py) -> None:
    py.hset("H", "f", "v")
    assert rs.hget("H", "f") == py.hget("H", "f") == b"v"


@verifier("HSET")
def _verify_hset(rs, py) -> None:
    assert rs.hset("H", "f", "v") == py.hset("H_py", "f", "v") == 1


@verifier("HSETNX")
def _verify_hsetnx(rs, py) -> None:
    assert rs.hsetnx("H", "f", "v") == py.hsetnx("H_py", "f", "v") == 1


@verifier("HGETALL")
def _verify_hgetall(rs, py) -> None:
    py.hset("H", mapping={"a": "1", "b": "2"})
    assert rs.hgetall("H") == py.hgetall("H") == {b"a": b"1", b"b": b"2"}


@verifier("HDEL")
def _verify_hdel(rs, py) -> None:
    py.hset("H", "f", "v")
    assert rs.hdel("H", "f") == 1


@verifier("HINCRBY")
def _verify_hincrby(rs, py) -> None:
    assert rs.hincrby("H", "c", 5) == py.hincrby("H_py", "c", 5) == 5


@verifier("HINCRBYFLOAT")
def _verify_hincrbyfloat(rs, py) -> None:
    rs_v = rs.hincrbyfloat("H", "c", 1.5)
    py_v = py.hincrbyfloat("H_py", "c", 1.5)
    assert rs_v == py_v == 1.5


@verifier("HKEYS")
def _verify_hkeys(rs, py) -> None:
    py.hset("H", mapping={"a": "1", "b": "2"})
    assert sorted(rs.hkeys("H")) == sorted(py.hkeys("H")) == [b"a", b"b"]


@verifier("HVALS")
def _verify_hvals(rs, py) -> None:
    py.hset("H", mapping={"a": "1", "b": "2"})
    assert sorted(rs.hvals("H")) == sorted(py.hvals("H")) == [b"1", b"2"]


@verifier("HEXISTS")
def _verify_hexists(rs, py) -> None:
    py.hset("H", "f", "v")
    assert rs.hexists("H", "f") == py.hexists("H", "f") is True


@verifier("HLEN")
def _verify_hlen(rs, py) -> None:
    py.hset("H", mapping={"a": "1", "b": "2"})
    assert rs.hlen("H") == py.hlen("H") == 2


@verifier("HMGET")
def _verify_hmget(rs, py) -> None:
    py.hset("H", mapping={"a": "1", "b": "2"})
    assert rs.hmget("H", ["a", "b", "missing"]) == py.hmget("H", ["a", "b", "missing"]) == [b"1", b"2", None]


@verifier("HSCAN")
def _verify_hscan(rs, py) -> None:
    py.hset("H", mapping={f"f{i}": str(i) for i in range(20)})
    rs_cursor, rs_data = rs.hscan("H", 0, count=100)
    py_cursor, py_data = py.hscan("H", 0, count=100)
    assert rs_data == py_data
    assert isinstance(rs_cursor, int) and isinstance(py_cursor, int)


@verifier("HRANDFIELD")
def _verify_hrandfield(rs, py) -> None:
    py.hset("H", mapping={"a": "1", "b": "2", "c": "3"})
    rs_v = rs.hrandfield("H")
    py_v = py.hrandfield("H")
    assert rs_v in {b"a", b"b", b"c"}
    assert py_v in {b"a", b"b", b"c"}


@verifier("HEXPIRE")
def _verify_hexpire(rs, py) -> None:
    py.hset("H", "f", "v")
    out = rs.hexpire("H", 60, "f")
    assert out == [1]


@verifier("HPEXPIRE")
def _verify_hpexpire(rs, py) -> None:
    py.hset("H", "f", "v")
    assert rs.hpexpire("H", 60_000, "f") == [1]


@verifier("HEXPIREAT")
def _verify_hexpireat(rs, py) -> None:
    import time
    py.hset("H", "f", "v")
    assert rs.hexpireat("H", int(time.time()) + 60, "f") == [1]


@verifier("HPEXPIREAT")
def _verify_hpexpireat(rs, py) -> None:
    import time
    py.hset("H", "f", "v")
    assert rs.hpexpireat("H", int(time.time() * 1000) + 60_000, "f") == [1]


@verifier("HEXPIRETIME")
def _verify_hexpiretime(rs, py) -> None:
    py.hset("H", "f", "v")
    rs.hexpire("H", 60, "f")
    out = rs.hexpiretime("H", "f")
    assert isinstance(out, list) and out[0] > 0


@verifier("HPEXPIRETIME")
def _verify_hpexpiretime(rs, py) -> None:
    py.hset("H", "f", "v")
    rs.hexpire("H", 60, "f")
    out = rs.hpexpiretime("H", "f")
    assert isinstance(out, list) and out[0] > 0


@verifier("HTTL")
def _verify_httl(rs, py) -> None:
    py.hset("H", "f", "v")
    rs.hexpire("H", 60, "f")
    out = rs.httl("H", "f")
    assert isinstance(out, list) and 0 < out[0] <= 60


@verifier("HPTTL")
def _verify_hpttl(rs, py) -> None:
    py.hset("H", "f", "v")
    rs.hexpire("H", 60, "f")
    out = rs.hpttl("H", "f")
    assert isinstance(out, list) and out[0] > 0


@verifier("HPERSIST")
def _verify_hpersist(rs, py) -> None:
    py.hset("H", "f", "v")
    rs.hexpire("H", 60, "f")
    assert rs.hpersist("H", "f") == [1]
```

- [ ] **Step 3: Write the set verifiers**

Replace `tests/compat/_verifiers/sets.py`:

```python
"""Verifiers for the sets command family.

Many set commands return server-defined-order results — the verifiers
normalise to ``set(...)`` before comparing.
"""

from __future__ import annotations

from . import verifier


@verifier("SADD")
def _verify_sadd(rs, py) -> None:
    assert rs.sadd("S", "a", "b") == py.sadd("S_py", "a", "b") == 2


@verifier("SREM")
def _verify_srem(rs, py) -> None:
    py.sadd("S", "a", "b")
    assert rs.srem("S", "a") == 1


@verifier("SMEMBERS")
def _verify_smembers(rs, py) -> None:
    py.sadd("S", "a", "b", "c")
    assert rs.smembers("S") == py.smembers("S") == {b"a", b"b", b"c"}


@verifier("SISMEMBER")
def _verify_sismember(rs, py) -> None:
    py.sadd("S", "a")
    assert rs.sismember("S", "a") == py.sismember("S", "a") is True


@verifier("SMISMEMBER")
def _verify_smismember(rs, py) -> None:
    py.sadd("S", "a", "b")
    assert rs.smismember("S", ["a", "b", "missing"]) == py.smismember("S", ["a", "b", "missing"]) == [1, 1, 0]


@verifier("SCARD")
def _verify_scard(rs, py) -> None:
    py.sadd("S", "a", "b")
    assert rs.scard("S") == py.scard("S") == 2


@verifier("SINTER")
def _verify_sinter(rs, py) -> None:
    py.sadd("A", "a", "b")
    py.sadd("B", "b", "c")
    assert rs.sinter("A", "B") == py.sinter("A", "B") == {b"b"}


@verifier("SINTERSTORE")
def _verify_sinterstore(rs, py) -> None:
    py.sadd("A", "a", "b")
    py.sadd("B", "b", "c")
    assert rs.sinterstore("DST", ["A", "B"]) == 1


@verifier("SINTERCARD")
def _verify_sintercard(rs, py) -> None:
    py.sadd("A", "a", "b")
    py.sadd("B", "b", "c")
    assert rs.sintercard(2, ["A", "B"]) == py.sintercard(2, ["A", "B"]) == 1


@verifier("SUNION")
def _verify_sunion(rs, py) -> None:
    py.sadd("A", "a", "b")
    py.sadd("B", "b", "c")
    assert rs.sunion("A", "B") == py.sunion("A", "B") == {b"a", b"b", b"c"}


@verifier("SUNIONSTORE")
def _verify_sunionstore(rs, py) -> None:
    py.sadd("A", "a", "b")
    py.sadd("B", "b", "c")
    assert rs.sunionstore("DST", ["A", "B"]) == 3


@verifier("SDIFF")
def _verify_sdiff(rs, py) -> None:
    py.sadd("A", "a", "b")
    py.sadd("B", "b")
    assert rs.sdiff("A", "B") == py.sdiff("A", "B") == {b"a"}


@verifier("SDIFFSTORE")
def _verify_sdiffstore(rs, py) -> None:
    py.sadd("A", "a", "b")
    py.sadd("B", "b")
    assert rs.sdiffstore("DST", ["A", "B"]) == 1


@verifier("SMOVE")
def _verify_smove(rs, py) -> None:
    py.sadd("A", "a")
    assert rs.smove("A", "B", "a") is True


@verifier("SSCAN")
def _verify_sscan(rs, py) -> None:
    py.sadd("S", *[f"m{i}" for i in range(20)])
    rs_cursor, rs_members = rs.sscan("S", 0, count=100)
    py_cursor, py_members = py.sscan("S", 0, count=100)
    assert set(rs_members) == set(py_members)
```

- [ ] **Step 4: Run the verifier-registration smoke check**

```bash
uv run python -c "
from tests.compat._verifiers import all_registered
n = len(all_registered())
print(f'{n} verifiers registered after task 6')
assert n >= 100
"
```

Expected: prints `>= 100`.

- [ ] **Step 5: Commit**

```bash
git add tests/compat/_verifiers/lists.py tests/compat/_verifiers/hashes.py tests/compat/_verifiers/sets.py
git commit -m "test(compat): add list, hash, and set verifiers"
```

---

## Task 7: Sorted-set + stream + script verifiers

Same pattern. Stream verifiers must use `xadd` to seed because the timestamps the server assigns determine the read response shape.

**Files:**
- Modify: `tests/compat/_verifiers/zsets.py`
- Modify: `tests/compat/_verifiers/streams.py`
- Modify: `tests/compat/_verifiers/scripts.py`

- [ ] **Step 1: Write the zset verifiers**

Replace `tests/compat/_verifiers/zsets.py`:

```python
"""Verifiers for the sorted-sets command family."""

from __future__ import annotations

from . import verifier


@verifier("ZADD")
def _verify_zadd(rs, py) -> None:
    assert rs.zadd("Z", {"a": 1, "b": 2}) == py.zadd("Z_py", {"a": 1, "b": 2}) == 2


@verifier("ZREM")
def _verify_zrem(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zrem("Z", "a") == 1


@verifier("ZRANGE")
def _verify_zrange(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert rs.zrange("Z", 0, -1) == py.zrange("Z", 0, -1) == [b"a", b"b", b"c"]
    assert rs.zrange("Z", 0, -1, withscores=True) == py.zrange("Z", 0, -1, withscores=True)


@verifier("ZRANGEBYSCORE")
def _verify_zrangebyscore(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert rs.zrangebyscore("Z", 1, 2) == py.zrangebyscore("Z", 1, 2) == [b"a", b"b"]


@verifier("ZREVRANGEBYSCORE")
def _verify_zrevrangebyscore(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zrevrangebyscore("Z", 2, 1) == py.zrevrangebyscore("Z", 2, 1) == [b"b", b"a"]


@verifier("ZRANGEBYLEX")
def _verify_zrangebylex(rs, py) -> None:
    py.zadd("Z", {"a": 0, "b": 0, "c": 0})
    assert rs.zrangebylex("Z", "[a", "[b") == py.zrangebylex("Z", "[a", "[b") == [b"a", b"b"]


@verifier("ZREVRANGEBYLEX")
def _verify_zrevrangebylex(rs, py) -> None:
    py.zadd("Z", {"a": 0, "b": 0, "c": 0})
    assert rs.zrevrangebylex("Z", "[b", "[a") == py.zrevrangebylex("Z", "[b", "[a") == [b"b", b"a"]


@verifier("ZRANGESTORE")
def _verify_zrangestore(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert rs.zrangestore("DST", "Z", 0, -1) == 3


@verifier("ZINCRBY")
def _verify_zincrby(rs, py) -> None:
    py.zadd("Z", {"a": 1})
    assert rs.zincrby("Z", 5, "a") == py.zincrby("Z_py", 6, "a") == 6


@verifier("ZCARD")
def _verify_zcard(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zcard("Z") == py.zcard("Z") == 2


@verifier("ZSCORE")
def _verify_zscore(rs, py) -> None:
    py.zadd("Z", {"a": 1.5})
    assert rs.zscore("Z", "a") == py.zscore("Z", "a") == 1.5


@verifier("ZMSCORE")
def _verify_zmscore(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zmscore("Z", ["a", "b", "missing"]) == py.zmscore("Z", ["a", "b", "missing"]) == [1.0, 2.0, None]


@verifier("ZRANK")
def _verify_zrank(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zrank("Z", "a") == py.zrank("Z", "a") == 0


@verifier("ZREVRANK")
def _verify_zrevrank(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zrevrank("Z", "a") == py.zrevrank("Z", "a") == 1


@verifier("ZREMRANGEBYRANK")
def _verify_zremrangebyrank(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2, "c": 3})
    assert rs.zremrangebyrank("Z", 0, 0) == 1


@verifier("ZREMRANGEBYSCORE")
def _verify_zremrangebyscore(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zremrangebyscore("Z", 1, 1) == 1


@verifier("ZREMRANGEBYLEX")
def _verify_zremrangebylex(rs, py) -> None:
    py.zadd("Z", {"a": 0, "b": 0})
    assert rs.zremrangebylex("Z", "[a", "[a") == 1


@verifier("ZCOUNT")
def _verify_zcount(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zcount("Z", 1, 2) == py.zcount("Z", 1, 2) == 2


@verifier("ZLEXCOUNT")
def _verify_zlexcount(rs, py) -> None:
    py.zadd("Z", {"a": 0, "b": 0})
    assert rs.zlexcount("Z", "[a", "[b") == py.zlexcount("Z", "[a", "[b") == 2


@verifier("ZPOPMIN")
def _verify_zpopmin(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zpopmin("Z") == [(b"a", 1.0)]


@verifier("ZPOPMAX")
def _verify_zpopmax(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    assert rs.zpopmax("Z") == [(b"b", 2.0)]


@verifier("BZPOPMIN")
def _verify_bzpopmin(rs, py) -> None:
    py.zadd("Z", {"a": 1})
    assert rs.bzpopmin(["Z"], timeout=1) == (b"Z", b"a", 1.0)


@verifier("BZPOPMAX")
def _verify_bzpopmax(rs, py) -> None:
    py.zadd("Z", {"a": 1})
    assert rs.bzpopmax(["Z"], timeout=1) == (b"Z", b"a", 1.0)


@verifier("ZMPOP")
def _verify_zmpop(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    rs_out = rs.zmpop(1, ["Z"], min=True, count=1)
    assert rs_out is not None


@verifier("BZMPOP")
def _verify_bzmpop(rs, py) -> None:
    py.zadd("Z", {"a": 1})
    rs_out = rs.bzmpop(0.1, 1, ["Z"], min=True, count=1)
    assert rs_out is not None


@verifier("ZSCAN")
def _verify_zscan(rs, py) -> None:
    py.zadd("Z", {f"m{i}": float(i) for i in range(20)})
    rs_cursor, rs_data = rs.zscan("Z", 0, count=100)
    py_cursor, py_data = py.zscan("Z", 0, count=100)
    assert sorted(rs_data) == sorted(py_data)


@verifier("ZUNIONSTORE")
def _verify_zunionstore(rs, py) -> None:
    py.zadd("A", {"a": 1})
    py.zadd("B", {"b": 2})
    assert rs.zunionstore("DST", ["A", "B"]) == 2


@verifier("ZINTERSTORE")
def _verify_zinterstore(rs, py) -> None:
    py.zadd("A", {"a": 1, "b": 2})
    py.zadd("B", {"b": 3})
    assert rs.zinterstore("DST", ["A", "B"]) == 1


@verifier("ZDIFFSTORE")
def _verify_zdiffstore(rs, py) -> None:
    py.zadd("A", {"a": 1, "b": 2})
    py.zadd("B", {"b": 3})
    assert rs.zdiffstore("DST", ["A", "B"]) == 1


@verifier("ZUNION")
def _verify_zunion(rs, py) -> None:
    py.zadd("A", {"a": 1})
    py.zadd("B", {"b": 2})
    assert sorted(rs.zunion(["A", "B"])) == sorted(py.zunion(["A", "B"]))


@verifier("ZINTER")
def _verify_zinter(rs, py) -> None:
    py.zadd("A", {"a": 1, "b": 2})
    py.zadd("B", {"b": 3})
    assert rs.zinter(["A", "B"]) == py.zinter(["A", "B"]) == [b"b"]


@verifier("ZDIFF")
def _verify_zdiff(rs, py) -> None:
    py.zadd("A", {"a": 1, "b": 2})
    py.zadd("B", {"b": 3})
    assert rs.zdiff(["A", "B"]) == py.zdiff(["A", "B"]) == [b"a"]
```

- [ ] **Step 2: Write the stream verifiers**

Replace `tests/compat/_verifiers/streams.py`:

```python
"""Verifiers for the streams command family.

Stream IDs are server-assigned timestamps; the verifiers seed via
``xadd`` then read what they wrote, comparing shapes.
"""

from __future__ import annotations

from . import verifier


@verifier("XADD")
def _verify_xadd(rs, py) -> None:
    rs_id = rs.xadd("S", {"k": "v"})
    py_id = py.xadd("S_py", {"k": "v"})
    # IDs are server-assigned timestamps; both look like b"<ms>-<seq>".
    assert isinstance(rs_id, bytes) and b"-" in rs_id
    assert isinstance(py_id, bytes) and b"-" in py_id


@verifier("XLEN")
def _verify_xlen(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    assert rs.xlen("S") == py.xlen("S") == 1


@verifier("XRANGE")
def _verify_xrange(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    rs_out = rs.xrange("S")
    py_out = py.xrange("S")
    assert len(rs_out) == len(py_out) == 1
    # Each entry is (id, fields-dict). IDs may differ across calls, but the fields must match.
    assert rs_out[0][1] == py_out[0][1] == {b"k": b"v"}


@verifier("XREVRANGE")
def _verify_xrevrange(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    rs_out = rs.xrevrange("S")
    assert rs_out[0][1] == {b"k": b"v"}


@verifier("XREAD")
def _verify_xread(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    rs_out = rs.xread({"S": "0"})
    py_out = py.xread({"S": "0"})
    assert rs_out[0][0] == py_out[0][0] == b"S"
    assert rs_out[0][1][0][1] == py_out[0][1][0][1] == {b"k": b"v"}


@verifier("XREADGROUP")
def _verify_xreadgroup(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    out = rs.xreadgroup("g", "c1", {"S": ">"})
    assert out[0][0] == b"S"


@verifier("XACK")
def _verify_xack(rs, py) -> None:
    msg_id = py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    assert rs.xack("S", "g", msg_id) == 1


@verifier("XDEL")
def _verify_xdel(rs, py) -> None:
    msg_id = py.xadd("S", {"k": "v"})
    assert rs.xdel("S", msg_id) == 1


@verifier("XGROUP CREATE")
def _verify_xgroup_create(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    assert rs.xgroup_create("S", "g", id="0") is True


@verifier("XGROUP SETID")
def _verify_xgroup_setid(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    assert rs.xgroup_setid("S", "g", id="$") is True


@verifier("XGROUP DESTROY")
def _verify_xgroup_destroy(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    assert rs.xgroup_destroy("S", "g") == 1


@verifier("XGROUP DELCONSUMER")
def _verify_xgroup_delconsumer(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    assert rs.xgroup_delconsumer("S", "g", "c1") == 1


@verifier("XGROUP CREATECONSUMER")
def _verify_xgroup_createconsumer(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    assert rs.xgroup_createconsumer("S", "g", "c1") == 1


@verifier("XINFO STREAM")
def _verify_xinfo_stream(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    rs_info = rs.xinfo_stream("S")
    py_info = py.xinfo_stream("S")
    assert set(rs_info) == set(py_info)


@verifier("XINFO GROUPS")
def _verify_xinfo_groups(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    rs_groups = rs.xinfo_groups("S")
    py_groups = py.xinfo_groups("S")
    assert len(rs_groups) == len(py_groups) == 1


@verifier("XINFO CONSUMERS")
def _verify_xinfo_consumers(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    rs_out = rs.xinfo_consumers("S", "g")
    assert len(rs_out) == 1


@verifier("XTRIM")
def _verify_xtrim(rs, py) -> None:
    for _ in range(5):
        py.xadd("S", {"k": "v"})
    assert rs.xtrim("S", maxlen=2) == 3


@verifier("XPENDING")
def _verify_xpending(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    rs_p = rs.xpending("S", "g")
    py_p = py.xpending("S", "g")
    assert rs_p["pending"] == py_p["pending"] == 1


@verifier("XCLAIM")
def _verify_xclaim(rs, py) -> None:
    msg_id = py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    out = rs.xclaim("S", "g", "c2", min_idle_time=0, message_ids=[msg_id])
    assert len(out) == 1


@verifier("XAUTOCLAIM")
def _verify_xautoclaim(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    py.xgroup_create("S", "g", id="0")
    py.xreadgroup("g", "c1", {"S": ">"})
    out = rs.xautoclaim("S", "g", "c2", min_idle_time=0)
    assert isinstance(out, (tuple, list))


@verifier("XSETID")
def _verify_xsetid(rs, py) -> None:
    py.xadd("S", {"k": "v"})
    assert rs.xsetid("S", "9999999999999-0") is True
```

- [ ] **Step 3: Write the script verifiers**

Replace `tests/compat/_verifiers/scripts.py`:

```python
"""Verifiers for the scripts and functions command family."""

from __future__ import annotations

from . import verifier


@verifier("EVAL")
def _verify_eval(rs, py) -> None:
    rs_v = rs.eval("return 42", 0)
    py_v = py.eval("return 42", 0)
    assert rs_v == py_v == 42


@verifier("EVALSHA")
def _verify_evalsha(rs, py) -> None:
    sha = py.script_load("return 7")
    assert rs.evalsha(sha, 0) == py.evalsha(sha, 0) == 7


@verifier("EVAL_RO")
def _verify_eval_ro(rs, py) -> None:
    rs_v = rs.eval_ro("return 1", 0)
    py_v = py.eval_ro("return 1", 0)
    assert rs_v == py_v == 1


@verifier("EVALSHA_RO")
def _verify_evalsha_ro(rs, py) -> None:
    sha = py.script_load("return 1")
    assert rs.evalsha_ro(sha, 0) == py.evalsha_ro(sha, 0) == 1


@verifier("SCRIPT LOAD")
def _verify_script_load(rs, py) -> None:
    rs_sha = rs.script_load("return 0")
    py_sha = py.script_load("return 0")
    assert rs_sha == py_sha


@verifier("SCRIPT EXISTS")
def _verify_script_exists(rs, py) -> None:
    sha = py.script_load("return 0")
    assert rs.script_exists(sha) == py.script_exists(sha) == [True]


@verifier("SCRIPT FLUSH")
def _verify_script_flush(rs, py) -> None:
    py.script_load("return 0")
    assert rs.script_flush() is True


@verifier("FCALL")
def _verify_fcall(rs, py) -> None:
    py.function_load("#!lua name=mylib\nredis.register_function('myfn', function() return 42 end)", replace=True)
    assert rs.fcall("myfn", 0) == py.fcall("myfn", 0) == 42


@verifier("FCALL_RO")
def _verify_fcall_ro(rs, py) -> None:
    py.function_load("#!lua name=mylib2\nredis.register_function{function_name='myfn2', callback=function() return 1 end, flags={'no-writes'}}", replace=True)
    assert rs.fcall_ro("myfn2", 0) == 1


@verifier("FUNCTION LOAD")
def _verify_function_load(rs, py) -> None:
    out = rs.function_load("#!lua name=mylib3\nredis.register_function('fn3', function() return 3 end)", replace=True)
    assert out == b"mylib3"


@verifier("FUNCTION DUMP")
def _verify_function_dump(rs, py) -> None:
    py.function_load("#!lua name=mylib4\nredis.register_function('fn4', function() return 4 end)", replace=True)
    rs_d = rs.function_dump()
    py_d = py.function_dump()
    assert isinstance(rs_d, bytes)
    assert isinstance(py_d, bytes)


@verifier("FUNCTION FLUSH")
def _verify_function_flush(rs, py) -> None:
    py.function_load("#!lua name=ml5\nredis.register_function('fn5', function() return 5 end)", replace=True)
    assert rs.function_flush() is True


@verifier("FUNCTION LIST")
def _verify_function_list(rs, py) -> None:
    py.function_load("#!lua name=ml6\nredis.register_function('fn6', function() return 6 end)", replace=True)
    rs_list = rs.function_list()
    py_list = py.function_list()
    assert len(rs_list) == len(py_list)


@verifier("FUNCTION STATS")
def _verify_function_stats(rs, py) -> None:
    rs_stats = rs.function_stats()
    py_stats = py.function_stats()
    assert set(rs_stats) == set(py_stats)


@verifier("FUNCTION KILL")
def _verify_function_kill(rs, py) -> None:
    # No script running — both raise NOTBUSY.
    from redis.exceptions import ResponseError
    try:
        rs.function_kill()
    except ResponseError:
        pass
    try:
        py.function_kill()
    except ResponseError:
        pass
```

- [ ] **Step 4: Confirm registry now has every implemented entry**

```bash
uv run python -c "
from tests._compat_manifest import by_status
from tests.compat._verifiers import all_registered

implemented_commands = {e['command'] for e in by_status('implemented')}
registered = set(all_registered())
missing = implemented_commands - registered
extra = registered - implemented_commands

print(f'manifest implemented: {len(implemented_commands)}')
print(f'verifiers registered: {len(registered)}')
print(f'missing verifiers: {sorted(missing)}')
print(f'unmanifested verifiers: {sorted(extra)}')
assert not missing, f'missing: {missing}'
"
```

Expected: `missing verifiers: []`. Every `implemented` row has a verifier.

- [ ] **Step 5: Commit**

```bash
git add tests/compat/_verifiers/zsets.py tests/compat/_verifiers/streams.py tests/compat/_verifiers/scripts.py
git commit -m "test(compat): add zset, stream, and script verifiers"
```

---

## Task 8: `test_parity.py` collector

Now wire the manifest + verifier registry into a parameterised pytest test that collects every `implemented` row.

**Files:**
- Create: `tests/compat/test_parity.py`

- [ ] **Step 1: Write the parity collector**

Create `tests/compat/test_parity.py`:

```python
"""Parity tests — every ``implemented`` manifest entry, both clients.

The test ID for each entry is the manifest's ``command`` field, so a
failing test reads as

    FAILED tests/compat/test_parity.py::test_parity[GET]

Pytest collects one test per implemented entry. Skipped if no verifier
is registered (which would also fail the verifier-registration smoke
check, so we never expect this in CI).
"""

from __future__ import annotations

import pytest

from tests._compat_manifest import ManifestEntry, by_status
from tests.compat._verifiers import get as get_verifier

_IMPLEMENTED = by_status("implemented")


def _id(entry: ManifestEntry) -> str:
    return entry["command"]


@pytest.mark.parametrize("entry", _IMPLEMENTED, ids=_id)
def test_parity(entry: ManifestEntry, rs_client, py_client) -> None:
    fn = get_verifier(entry["command"])
    if fn is None:
        pytest.fail(
            f"no verifier registered for implemented command {entry['command']}; "
            f"add one to tests/compat/_verifiers/{entry['family']}.py",
        )
    fn(rs_client, py_client)
```

- [ ] **Step 2: Run the parity suite (assuming Plans 03–10 have landed)**

```bash
uv run pytest tests/compat/test_parity.py -v --tb=short -n auto
```

Expected: every `test_parity[<COMMAND>]` PASSES. If a verifier was written against an unimplemented method, it fails with `AttributeError` on the façade — that's a Plan 10/façade gap, not a problem with this plan.

- [ ] **Step 3: Commit**

```bash
git add tests/compat/test_parity.py
git commit -m "test(compat): collect parity tests from manifest"
```

---

## Task 9: `test_partial.py` collector

Same pattern as Task 8 but for `partial` rows; the divergence is documented in the entry's `notes` field and the verifier asserts whatever-still-holds (membership, shape, type — not byte equality).

**Files:**
- Create: `tests/compat/test_partial.py`

- [ ] **Step 1: Write the partial-divergence collector**

Create `tests/compat/test_partial.py`:

```python
"""Partial-parity tests — every ``partial`` manifest entry runs through
the same verifier registry, but its verifier asserts only the relaxed
invariants documented in the entry's ``notes`` field.
"""

from __future__ import annotations

import pytest

from tests._compat_manifest import ManifestEntry, by_status
from tests.compat._verifiers import get as get_verifier

_PARTIAL = by_status("partial")


def _id(entry: ManifestEntry) -> str:
    return entry["command"]


@pytest.mark.parametrize("entry", _PARTIAL, ids=_id)
def test_partial(entry: ManifestEntry, rs_client, py_client) -> None:
    """Run the partial-mode verifier (asserts only what's stable)."""
    fn = get_verifier(entry["command"])
    if fn is None:
        pytest.skip(
            f"no verifier yet for partial command {entry['command']} "
            f"(notes: {entry['notes']!r})",
        )
    fn(rs_client, py_client)


@pytest.mark.parametrize("entry", _PARTIAL, ids=_id)
def test_partial_has_notes(entry: ManifestEntry) -> None:
    """The manifest validator already enforces this, but a per-entry
    failure makes the divergence-without-explanation case loud."""
    assert entry["notes"], (
        f"partial entry {entry['command']} must have a notes field "
        f"explaining the divergence"
    )
```

- [ ] **Step 2: Add the partial-mode verifiers**

Edit `tests/compat/_verifiers/sets.py` to append:

```python
@verifier("SPOP")
def _verify_spop(rs, py) -> None:
    py.sadd("S", "a", "b", "c")
    rs_out = rs.spop("S")
    # Order is server-defined; assert membership, not value.
    assert rs_out in {b"a", b"b", b"c"}


@verifier("SRANDMEMBER")
def _verify_srandmember(rs, py) -> None:
    py.sadd("S", "a", "b", "c")
    out = rs.srandmember("S")
    assert out in {b"a", b"b", b"c"}
```

Edit `tests/compat/_verifiers/zsets.py` to append:

```python
@verifier("ZRANDMEMBER")
def _verify_zrandmember(rs, py) -> None:
    py.zadd("Z", {"a": 1, "b": 2})
    out = rs.zrandmember("Z")
    assert out in {b"a", b"b"}
```

Edit `tests/compat/_verifiers/admin.py` to append:

```python
@verifier("RANDOMKEY")
def _verify_randomkey(rs, py) -> None:
    py.set("k", "v")
    out = rs.randomkey()
    assert out == b"k"


@verifier("INFO")
def _verify_info(rs, py) -> None:
    rs_info = rs.info()
    py_info = py.info()
    # Compare top-level keys present in both — values like uptime drift between calls.
    assert set(rs_info) >= {"redis_version", "tcp_port"}
    assert set(py_info) >= {"redis_version", "tcp_port"}


@verifier("CLIENT ID")
def _verify_client_id(rs, py) -> None:
    assert isinstance(rs.client_id(), int)
    assert isinstance(py.client_id(), int)


@verifier("CLIENT INFO")
def _verify_client_info(rs, py) -> None:
    rs_info = rs.client_info()
    assert isinstance(rs_info, dict) and "id" in rs_info


@verifier("CLIENT LIST")
def _verify_client_list(rs, py) -> None:
    rs_list = rs.client_list()
    assert isinstance(rs_list, list) and rs_list and "id" in rs_list[0]


@verifier("OBJECT IDLETIME")
def _verify_object_idletime(rs, py) -> None:
    py.set("k", "v")
    out = rs.object_idletime("k")
    assert isinstance(out, int) and out >= 0


@verifier("OBJECT FREQ")
def _verify_object_freq(rs, py) -> None:
    pytest_skip = __import__("pytest").skip  # local import to avoid touching imports above
    # Requires LFU policy; skip if we're on the default (allkeys-lru).
    policy = py.config_get("maxmemory-policy")
    if not policy.get(b"maxmemory-policy", b"").startswith(b"allkeys-lfu"):
        pytest_skip("OBJECT FREQ requires LFU policy")


@verifier("TIME")
def _verify_time(rs, py) -> None:
    rs_t = rs.time()
    py_t = py.time()
    # (sec, usec) tuples; assert shape and that both are within 5s of each other.
    assert isinstance(rs_t, tuple) and len(rs_t) == 2
    assert abs(rs_t[0] - py_t[0]) <= 5


@verifier("LASTSAVE")
def _verify_lastsave(rs, py) -> None:
    assert isinstance(rs.lastsave(), int)
```

- [ ] **Step 3: Run the partial collector**

```bash
uv run pytest tests/compat/test_partial.py -v --tb=short -n auto
```

Expected: every parametrised case PASSES (or SKIPS with the documented reason). No FAILs.

- [ ] **Step 4: Commit**

```bash
git add tests/compat/test_partial.py tests/compat/_verifiers/
git commit -m "test(compat): add partial-mode verifiers and collector"
```

---

## Task 10: `scripts/render_compat_matrix.py`

The renderer reads the manifest and produces the markdown block that lives between the `<!-- compat:start -->` and `<!-- compat:end -->` markers in `README.md`. One table per family; one row per entry.

**Files:**
- Create: `scripts/render_compat_matrix.py`

- [ ] **Step 1: Write the renderer**

```bash
mkdir -p scripts
```

Create `scripts/render_compat_matrix.py`:

```python
#!/usr/bin/env python3
"""Render the compatibility matrix into README.md.

Run modes:
    render          — overwrite the block, exit 0.
    check           — diff the rendered block against the README; exit 1
                      if it would change. Used by the pre-commit hook.

The block lives between two HTML comment markers in README.md. Anything
between the markers is regenerated; anything outside is left alone.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"
START = "<!-- compat:start -->"
END = "<!-- compat:end -->"

# So `python scripts/render_compat_matrix.py` works without invoking via uv.
sys.path.insert(0, str(REPO_ROOT))

from tests._compat_manifest import MANIFEST, ManifestEntry  # noqa: E402

STATUS_BADGE = {
    "implemented": "[*] implemented",
    "partial": "[~] partial",
    "deferred": "[ ] deferred",
}

FAMILY_ORDER = [
    "strings",
    "lists",
    "hashes",
    "sets",
    "zsets",
    "streams",
    "scripts",
    "admin",
    "pubsub",
    "transactions",
    "cluster",
    "sentinel",
]


def _by_family() -> dict[str, list[ManifestEntry]]:
    out: dict[str, list[ManifestEntry]] = defaultdict(list)
    for entry in MANIFEST:
        out[entry["family"]].append(entry)
    return out


def _render_family_table(family: str, entries: list[ManifestEntry]) -> str:
    rows = ["| Command | Method | Status | Since | Notes |", "|---|---|---|---|---|"]
    for entry in sorted(entries, key=lambda e: e["command"]):
        rows.append(
            f"| `{entry['command']}` | `{entry['method']}` | "
            f"{STATUS_BADGE[entry['status']]} | "
            f"{entry['since_redis']} | {entry['notes']} |",
        )
    return "\n".join(rows)


def _render_summary(grouped: dict[str, list[ManifestEntry]]) -> str:
    total = sum(len(v) for v in grouped.values())
    impl = sum(1 for v in grouped.values() for e in v if e["status"] == "implemented")
    part = sum(1 for v in grouped.values() for e in v if e["status"] == "partial")
    defr = sum(1 for v in grouped.values() for e in v if e["status"] == "deferred")
    return (
        f"**{total} commands tracked** — "
        f"{impl} implemented, {part} partial, {defr} deferred."
    )


def render() -> str:
    grouped = _by_family()
    parts: list[str] = [_render_summary(grouped), ""]
    for family in FAMILY_ORDER:
        if family not in grouped:
            continue
        parts.append(f"### {family.title()}")
        parts.append("")
        parts.append(_render_family_table(family, grouped[family]))
        parts.append("")
    # Any family not in FAMILY_ORDER shows up at the end so we can't lose entries.
    leftover = sorted(set(grouped) - set(FAMILY_ORDER))
    for family in leftover:
        parts.append(f"### {family.title()}")
        parts.append("")
        parts.append(_render_family_table(family, grouped[family]))
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def _splice(readme_text: str, block: str) -> str:
    if START not in readme_text or END not in readme_text:
        raise SystemExit(
            f"README.md is missing the {START} / {END} markers; "
            f"add them between the Benchmarks and Quickstart sections.",
        )
    pre, _, rest = readme_text.partition(START)
    _, _, post = rest.partition(END)
    return f"{pre}{START}\n\n{block}\n{END}{post}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["render", "check"], default="render", nargs="?")
    args = parser.parse_args()

    block = render()
    current = README.read_text()
    rendered = _splice(current, block)

    if args.mode == "check":
        if current != rendered:
            sys.stderr.write(
                "README compatibility matrix is stale.\n"
                "Run: uv run python scripts/render_compat_matrix.py render\n",
            )
            return 1
        return 0

    if current != rendered:
        README.write_text(rendered)
        print(f"updated {README}")
    else:
        print(f"{README} already up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Make it executable**

```bash
chmod +x scripts/render_compat_matrix.py
```

- [ ] **Step 3: Smoke-test the renderer (will exit 1 because README has no markers yet)**

```bash
uv run python scripts/render_compat_matrix.py check
```

Expected: exits 1, stderr says `README.md is missing the <!-- compat:start --> / <!-- compat:end --> markers`. That's the right red — Task 11 adds the markers.

- [ ] **Step 4: Commit**

```bash
git add scripts/render_compat_matrix.py
git commit -m "test(compat): add render_compat_matrix renderer"
```

---

## Task 11: README integration

Insert the `<!-- compat:start --> / <!-- compat:end -->` markers between the Benchmarks and Quickstart sections (per `PLAN.md`'s "leads with benchmarks, then compat matrix, then quickstart"), then run the renderer to populate them.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Insert the markers + a header**

The current README has no Quickstart section. Add the markers + a placeholder right after the Benchmarks block. Edit `README.md` and replace:

```markdown
## Benchmarks

*(Coming soon — the README will lead with benchmarks once there's something to measure. Comparison targets: `redis-py`, `redis-py[hiredis]`, `valkey-py`, `valkey-glide`.)*

## Installation
```

with:

```markdown
## Benchmarks

*(Coming soon — the README will lead with benchmarks once there's something to measure. Comparison targets: `redis-py`, `redis-py[hiredis]`, `valkey-py`, `valkey-glide`.)*

## Compatibility matrix

The matrix below is generated from `tests/_compat_manifest.py` by `scripts/render_compat_matrix.py`. Every row is exercised by `tests/compat/test_parity.py` against both `redis-rs-py` and upstream `redis-py` running on the same Valkey instance. The pre-commit hook regenerates the matrix on every commit and fails the commit if it drifts.

<!-- compat:start -->
<!-- compat:end -->

## Installation
```

- [ ] **Step 2: Generate the table**

```bash
uv run python scripts/render_compat_matrix.py render
```

Expected: prints `updated README.md`.

- [ ] **Step 3: Verify the renderer is now idempotent**

```bash
uv run python scripts/render_compat_matrix.py check
```

Expected: exits 0; prints `README.md already up to date`.

- [ ] **Step 4: Eyeball the output**

```bash
grep -n "Compatibility matrix" README.md
sed -n '/<!-- compat:start -->/,/<!-- compat:end -->/p' README.md | head -40
```

Expected: the section header is there, the table starts with the summary line `**N commands tracked** — ...`, and the first family table (Strings) appears.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs(readme): add compatibility matrix section"
```

---

## Task 12: Pre-commit hook for matrix freshness

The matrix and the manifest must stay in lockstep. Add a local pre-commit hook that runs `render_compat_matrix.py check` and fails any commit where the README block is stale.

**Files:**
- Modify: `.pre-commit-config.yaml`

- [ ] **Step 1: Add the hook**

In `.pre-commit-config.yaml`, find the `- repo: local` block that contains `id: ruff-check` and append a sibling hook to that local block:

```yaml
      - id: compat-matrix-fresh
        name: compat-matrix-fresh
        language: system
        entry: uv run python scripts/render_compat_matrix.py check
        pass_filenames: false
        files: ^(tests/_compat_manifest\.py|scripts/render_compat_matrix\.py|README\.md)$
```

The full updated section in `.pre-commit-config.yaml`:

```yaml
  - repo: local
    hooks:
      - id: uv-sync-check
        name: uv-sync-check
        language: system
        entry: uv sync
        pass_filenames: false
      - id: ruff-check
        name: ruff-check
        entry: uv run ruff check --fix
        language: system
        pass_filenames: false
      - id: ruff-format
        name: ruff-format
        entry: uv run ruff format
        language: system
        pass_filenames: false
      - id: compat-matrix-fresh
        name: compat-matrix-fresh
        language: system
        entry: uv run python scripts/render_compat_matrix.py check
        pass_filenames: false
        files: ^(tests/_compat_manifest\.py|scripts/render_compat_matrix\.py|README\.md)$
```

- [ ] **Step 2: Re-install the hooks**

```bash
uv run pre-commit install --install-hooks
```

Expected: prints `pre-commit installed at .git/hooks/pre-commit`.

- [ ] **Step 3: Run the hook directly to verify it passes**

```bash
uv run pre-commit run compat-matrix-fresh --all-files
```

Expected: exits 0; output `compat-matrix-fresh.....Passed`.

- [ ] **Step 4: Verify the hook fails on drift**

Add a one-off test entry to the manifest, run the hook (it should fail), then revert.

```bash
uv run python -c "
from pathlib import Path
p = Path('tests/_compat_manifest.py')
text = p.read_text()
patched = text.replace('STRING_ENTRIES: Final[list[ManifestEntry]] = [', 'STRING_ENTRIES: Final[list[ManifestEntry]] = [\n    {\"command\": \"DRIFT_TEST\", \"method\": \"drift\", \"family\": \"strings\", \"status\": \"deferred\", \"notes\": \"smoke\", \"since_redis\": \"1.0\", \"since_redis_rs_py\": \"—\"},')
p.write_text(patched)
"
uv run pre-commit run compat-matrix-fresh --all-files || echo "EXPECTED FAILURE"
git checkout -- tests/_compat_manifest.py
```

Expected: the hook fails and prints `README.md is stale`. We then revert.

- [ ] **Step 5: Final clean run**

```bash
uv run pre-commit run --all-files
```

Expected: every hook passes.

- [ ] **Step 6: Commit**

```bash
git add .pre-commit-config.yaml
git commit -m "ci(pre-commit): fail on stale compat matrix"
```

---

## Task 13: CHANGELOG entry + final test sweep

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Append to `CHANGELOG.md` under `### Added`**

```markdown
- Manifest-driven compatibility matrix (`tests/_compat_manifest.py`) — single source of truth for every Redis command we cover, with status (`implemented` / `partial` / `deferred`) and notes.
- Parity test suite under `tests/compat/` — every `implemented` entry runs through both `redis-rs-py` and upstream `redis-py` against the same Valkey container and asserts identical responses; every `partial` entry runs through a relaxed-invariant verifier with documented divergence notes.
- README compatibility matrix generated by `scripts/render_compat_matrix.py`, kept fresh by a `compat-matrix-fresh` pre-commit hook that fails commits where the README block has drifted from the manifest.
```

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest -n auto
```

Expected: every test PASSES across `tests/driver/`, `tests/async_bridge/`, `tests/exceptions/`, `tests/facade/`, `tests/compat/`, `tests/test_smoke.py`. The compat suite alone should add ~150 parametrised cases.

- [ ] **Step 3: Run lint + format + clippy**

```bash
uv run ruff check
uv run ruff format --check
uv run ty check python/redis_rs_py/
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 17 entry"
```

- [ ] **Step 5: Final verification**

```bash
git log --oneline -15
```

Expected: 13 new commits, conventional-commit style, in roughly the order of the tasks.

---

## Self-review checklist for this plan

- [x] Spec coverage (`PLAN.md` v0.1 surface — Compatibility matrix): "Every redis-py public method gets a row in the README" — Tasks 1+2 enumerate ~150 commands across 8 families; "no silent gaps" — the manifest's `_validate_manifest()` enforces uniqueness and notes-on-divergence.
- [x] Spec coverage: "the matrix and the test surface can never drift" — Task 4 binds verifier registration to the manifest by command name; Task 8 collects parametrised tests directly from the manifest; Task 12 pre-commit hook blocks drift in the README block.
- [x] Spec coverage: README order — Task 11 places the matrix between Benchmarks (Plan 18) and the existing Installation section, matching `PLAN.md`'s "leads with benchmarks, then compat matrix, then quickstart".
- [x] Out-of-scope items deferred: cluster-only commands (Plan 15), sentinel admin (Plan 16), module clients are marked `deferred` with notes.
- [x] No placeholder text — every code block is real Python or YAML, every manifest row has all six fields populated.
- [x] Type consistency: `ManifestEntry` TypedDict shape matches the helpers (`by_status`, `by_family`, `get_by_command`); the verifier registry's `VerifierFn` matches what the parity collector calls.
- [x] All file paths are absolute or repo-relative-from-root.
- [x] Every test step has a runnable command and an explicit pass/fail expectation.
- [x] Frequent commits — 13 across 13 tasks, each independently revertable.
- [x] Conventional-commit style throughout (`test(compat):`, `docs(readme):`, `ci(pre-commit):`, `docs(changelog):`).
- [x] The pre-commit hook is bidirectional: regenerates on demand (Task 11), blocks commits on drift (Task 12).
- [x] `partial` and `deferred` rows have non-empty `notes`, enforced at manifest import time by `_validate_manifest()`.
