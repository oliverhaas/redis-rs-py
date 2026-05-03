# redis-rs-py — Plan Roadmap

> **For agentic workers:** This is the master index for the v0.1 implementation. Each numbered plan in `docs/superpowers/plans/` is independently executable and produces working, testable software. Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement plans task-by-task.

The full design spec lives at `PLAN.md` in the repo root (mirrored from [`oliverhaas/ideas:packages/python/redis-rs-py.md`](https://github.com/oliverhaas/ideas/blob/main/packages/python/redis-rs-py.md)). This roadmap is the *executable* version of that spec — the spec answers "what and why", the plans answer "in what order, with what tests, with which exact code".

## Source material

- **`PLAN.md`** — the spec.
- **`/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/`** — the prototype this generalises from. ~6.5K LOC of working Rust (`async_bridge.rs`, `connection.rs`, `client.rs`, `adapter.rs`, `test_helpers.rs`). `lib.rs` calls out the upstream from `django-vcache` (MIT, GlitchTip) — the async bridge and the top half of `connection.rs` are verbatim ports of that work.
- **`/home/ohaas/e1+/redis-rs-py/`** — the current scaffolding. `_driver` extension exposes only `__version__`; Python tree is paper-thin per the "Rust by default" principle.

## Architectural North Star

> **Rust by default, Python only when forced.** Both the low-level driver (`RedisRsDriver`) and the high-level redis-py-compatible façade (`Redis`, `asyncio.Redis`, `RedisCluster`, `Sentinel`) are Rust pyclasses. The published wheel is essentially one `.so` plus a one-line `__init__.py`.

Two physical layers per driver: a **regular** `ConnectionManager` (30 s response timeout, multiplexed) and a **lazy blocking** `ConnectionManager` (no response timeout, allocated on first BLPOP/BLMOVE/BLMPOP). Without the split, one blocking command head-of-line-blocks every other multiplexed call.

A **single tokio multi-thread runtime** is owned by a process-global `OnceLock` with PID-checked fork-safe rebuild. The runtime is shared by every `RedisRsDriver` in the process.

Custom **`RedisRsAwaitable`** bridges tokio futures to asyncio without `pyo3-async-runtimes`: 5-poll busy-yield fast path → callback mode (with `_asyncio_future_blocking=True`) on miss → `loop.call_soon_threadsafe(_wake)` from a watcher task. Cancellation drops the oneshot rx and wakes pending callbacks.

## Plan order & dependencies

**Foundation tier** (must come first; everything depends on this):
| # | Plan | Blocks | What it lands |
|---|---|---|---|
| 01 | [foundation-async-bridge.md](01-foundation-async-bridge.md) | everything | Tokio runtime registry, `RedisRsAwaitable`, `RedisRsDriver` skeleton, `ValkeyConn` two-layer connection wrapper, `connect_standard`, end-to-end `aget`/`get`/`aset`/`set`/`adelete`/`delete`/`aping`/`ping`. Test helpers (`_test_*`) for the awaitable contract. |
| 02 | [exceptions.md](02-exceptions.md) | command plans | Full `redis.exceptions` hierarchy via `create_exception!`, `classify_error()` returning the right exception class, `RawResult::Error` carrying class identity, swap `PyConnectionError`/`PyRuntimeError` placeholders to the new types. |

**Command tier** (each plan independent, can run in parallel after the foundation tier):
| # | Plan | What it lands |
|---|---|---|
| 03 | [commands-strings.md](03-commands-strings.md) | Full `SET` matrix (`EX`/`PX`/`NX`/`XX`/`KEEPTTL`/`GET`/`EXAT`/`PXAT`), `GETEX`, `GETDEL`, `COPY`, `INCR`/`INCRBY`/`INCRBYFLOAT`/`DECR`/`DECRBY`, `APPEND`, `STRLEN`, `MGET`, `MSET`, `MSETNX`, `SETRANGE`/`GETRANGE`, `EXISTS`, `DEL`/`UNLINK`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`/`EXPIRETIME`/`PEXPIRETIME`/`TTL`/`PTTL`/`PERSIST`, `RENAME`/`RENAMENX`, `TYPE`, `DUMP`/`RESTORE` (`dump`/`restore`). |
| 04 | [commands-lists.md](04-commands-lists.md) | `LPUSH`, `RPUSH`, `LPOP`, `RPOP` (with count), `LMOVE`, `LPOS`, `LRANGE`, `LLEN`, `LREM`, `LINDEX`, `LSET`, `LINSERT`, `LTRIM`, `LPUSHX`, `RPUSHX`, blocking variants `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `LMPOP`. Lazy blocking-connection wiring. |
| 05 | [commands-hashes.md](05-commands-hashes.md) | `HGET`, `HSET`, `HSETNX`, `HMSET`, `HGETALL`, `HDEL`, `HINCRBY`, `HINCRBYFLOAT`, `HKEYS`, `HVALS`, `HEXISTS`, `HLEN`, `HMGET`, `HSCAN`, `HRANDFIELD`, `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT`/`HEXPIRETIME`/`HPEXPIRETIME`/`HTTL`/`HPTTL`/`HPERSIST` (Redis 7.4 hash-field TTLs). |
| 06 | [commands-sets.md](06-commands-sets.md) | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SINTER`/`SUNION`/`SDIFF` + `STORE` variants, `SINTERCARD`, `SMOVE`, `SPOP` (+ count), `SRANDMEMBER` (+ count), `SSCAN`. |
| 07 | [commands-zsets.md](07-commands-zsets.md) | `ZADD` (full flag matrix `NX`/`XX`/`GT`/`LT`/`CH`/`INCR`), `ZREM`, `ZRANGE` (with `BYSCORE`/`BYLEX`/`REV`/`LIMIT`/`WITHSCORES`), `ZRANGEBYSCORE`/`ZRANGEBYLEX` and `REV` variants, `ZRANGESTORE`, `ZINCRBY`, `ZCARD`, `ZSCORE`, `ZMSCORE`, `ZRANK`/`ZREVRANK` (`WITHSCORE`), `ZREMRANGEBYRANK`/`ZREMRANGEBYSCORE`/`ZREMRANGEBYLEX`, `ZCOUNT`, `ZLEXCOUNT`, `ZPOPMIN`/`ZPOPMAX` (+ count), `BZPOPMIN`/`BZPOPMAX`, `ZMPOP`/`BZMPOP`, `ZRANDMEMBER`, `ZSCAN`, `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE`/`ZUNION`/`ZINTER`/`ZDIFF`. |
| 08 | [commands-streams.md](08-commands-streams.md) | `XADD` (incl. `NOMKSTREAM`/`MAXLEN`/`MINID`/`LIMIT`/`~`), `XLEN`, `XRANGE`/`XREVRANGE`, `XREAD`/`XREADGROUP` (incl. `BLOCK`, `COUNT`, `NOACK`), `XACK`, `XDEL`, `XGROUP CREATE`/`SETID`/`DESTROY`/`DELCONSUMER`/`CREATECONSUMER`, `XINFO STREAM`/`GROUPS`/`CONSUMERS`, `XTRIM`, `XPENDING` (summary + range), `XCLAIM`, `XAUTOCLAIM`, `XSETID`. Decision: pass-through `redis::Value` (per cachex) or flatten to redis-py-shaped tuples — the plan picks flattened-in-Rust to match `redis-py`'s `xread()` output exactly. |
| 09 | [commands-scripts-admin.md](09-commands-scripts-admin.md) | `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`, `SCRIPT LOAD`/`EXISTS`/`FLUSH`, `FCALL`/`FCALL_RO`/`FUNCTION LOAD`/`DUMP`/`FLUSH`/`LIST`/`STATS`/`KILL`. `SCAN` (full + iterator), `KEYS`, `RANDOMKEY`, `DBSIZE`, `FLUSHDB`/`FLUSHALL`. `INFO`, `CONFIG GET`/`SET`/`RESETSTAT`/`REWRITE`, `CLIENT KILL`/`GETNAME`/`SETNAME`/`LIST`/`ID`/`INFO`/`PAUSE`/`UNPAUSE`/`NO-EVICT`/`NO-TOUCH`. `OBJECT ENCODING`/`IDLETIME`/`FREQ`/`REFCOUNT`/`HELP`, `MEMORY USAGE`. `PING`, `ECHO`, `WAIT`, `WAITAOF`, `TIME`, `LASTSAVE`, `BGSAVE`, `BGREWRITEAOF`, `DEBUG SLEEP` (test-only). |

**Façade tier** (needs foundation + most of the command tier):
| # | Plan | What it lands |
|---|---|---|
| 10 | [facade-sync.md](10-facade-sync.md) | `redis_rs_py.Redis` Rust pyclass — redis-py-compatible constructor (`host`, `port`, `db`, `password`, `username`, `ssl`, `socket_timeout`, `socket_connect_timeout`, `max_connections`, `health_check_interval`, `client_name`, etc.), `Redis.from_url(url)`, kwargs accept-and-warn (`kwargs.rs` accepts every redis-py constructor kwarg, warns once per process on unknown), every command method delegated to driver, `__enter__`/`__exit__`/`close`. |
| 11 | [facade-asyncio.md](11-facade-asyncio.md) | `redis_rs_py.asyncio.Redis` Rust pyclass — same constructor surface, methods all return `RedisRsAwaitable`, `aclose`/`__aenter__`/`__aexit__`. Submodule registration via `PyModule::add_submodule`. |
| 12 | [decode-responses.md](12-decode-responses.md) | `decode_responses=True` mode in the façade — bytes → str at the boundary using `encoding`/`encoding_errors`. Lives in Rust (`facade/decode.rs`); applies recursively to lists/dicts/tuples/sets returned by commands. Keys-and-values for HGETALL etc. all flip to str. Native types preserved (sets→`set`, hashes→`dict`, sorted sets→`list[tuple[bytes\|str, float]]`). |

**Advanced tier**:
| # | Plan | What it lands |
|---|---|---|
| 13 | [pipelines-transactions.md](13-pipelines-transactions.md) | `Pipeline` and `Transaction` Rust pyclasses (sync + async), buffered-then-flushed semantics matching redis-py, `pipeline(transaction=True/False)`, `WATCH`/`UNWATCH`/`MULTI`/`EXEC`/`DISCARD`, sticky-connection mode in the driver (`reserve_connection`/`release_connection`), `WatchError` translation, `r.transaction(func, *keys, value_from_callable=False, watch_delay=None, **kwargs)` retry helper. |
| 14 | [pubsub.md](14-pubsub.md) | `PubSub` and `asyncio.PubSub` Rust pyclasses (`subscribe`/`unsubscribe`/`psubscribe`/`punsubscribe`/`ssubscribe`/`sunsubscribe`/`get_message`/`listen`/`run_in_thread`/`close`), one dedicated subscriber connection per `pubsub()` call (separate from the driver pool), bridge from redis-rs `PubSub` stream into a tokio `mpsc::channel` polled by `RedisRsAwaitable`. |
| 15 | [cluster.md](15-cluster.md) | `RedisCluster` Rust pyclass (sync + async), `from_url`, multi-node startup nodes, `read_from_replicas`, `dynamic_startup_nodes`, MOVED/ASK retry delegation to redis-rs `cluster_async`, fan-out for non-routable cross-slot commands, no client-side caching (cluster doesn't support it), `cluster_nodes`/`cluster_slots`/`cluster_shards`/`cluster_info`/`cluster_keyslot`/`cluster_countkeysinslot`/`cluster_getkeysinslot`/`cluster_meet`/`cluster_forget`/`cluster_reset` admin commands. |
| 16 | [sentinel.md](16-sentinel.md) | `Sentinel` Rust pyclass — `master_for(service_name)`/`slave_for(service_name)`, sentinel URL list, `service_name`, automatic failover via the cachex pattern (RwLock'd current master, retry-with-rediscovery on connection-class errors), `sentinel_masters`/`sentinel_master`/`sentinel_slaves`/`sentinel_sentinels`/`sentinel_get_master_addr_by_name`/`sentinel_failover` admin commands. |

**Release tier**:
| # | Plan | What it lands |
|---|---|---|
| 17 | [compat-matrix-and-parity.md](17-compat-matrix-and-parity.md) | `tests/test_compat_redis_py.py` — runs every implemented method against a live testcontainers Valkey, then runs the same call against the upstream `redis-py` client, asserts identical return value/shape/type. README compatibility table generated from a manifest in `tests/_compat_manifest.py` so the matrix and the test surface can never drift apart. |
| 18 | [benchmarks.md](18-benchmarks.md) | `benchmarks/{bench_get_set,bench_pipeline,bench_pubsub,bench_async_throughput}.py` using `pyperf`, run vs `redis-py[hiredis]` and `valkey-glide` against the same testcontainers Valkey, with results posted to `benchmarks/RESULTS.md` and a `benchmarks/run_all.py` orchestrator. CI workflow `bench.yml` runs the smoke benchmark on every PR (compare-to-baseline gate). |
| 19 | [distribution.md](19-distribution.md) | Re-enable `publish.yml`, configure PyPI Trusted Publisher + `pypi` GitHub environment, expand `ci.yml` wheel matrix to cp310–cp314 + cp314t for Linux/macOS/Windows where the spec requires (currently cp314/cp314t only), add musllinux Alpine wheels, add aarch64-macos backfill, add sdist build, add wheel install smoke-test for every (python × platform) cell. |

## How the plans relate

```
                   ┌──────────────┐
                   │ 01 Foundation│  (must land first)
                   └──────┬───────┘
                          │
                   ┌──────▼───────┐
                   │ 02 Exceptions│  (block on 01)
                   └──────┬───────┘
                          │
   ┌──────────────────────┼──────────────────────┐
   │             │        │        │             │
┌──▼──┐ ┌───────▼┐ ┌─────▼─┐ ┌────▼─┐ ┌─────────▼┐ ┌──▼──┐ ┌────────▼┐
│ 03  │ │ 04     │ │ 05    │ │ 06   │ │ 07       │ │ 08  │ │ 09      │
│ Str │ │ Lists  │ │ Hash  │ │ Sets │ │ ZSets    │ │ Strm│ │ Scripts │
└──┬──┘ └────┬───┘ └────┬──┘ └────┬─┘ └────┬─────┘ └──┬──┘ └────┬────┘
   └─────────┴──────────┴─────────┴────────┴──────────┴─────────┘
                                  │
                          ┌───────▼──────┐
                          │ 10 Facade-sy │
                          └───────┬──────┘
                                  │
                          ┌───────▼──────┐
                          │ 11 Facade-as │
                          └───────┬──────┘
                                  │
                          ┌───────▼──────┐
                          │ 12 decode    │
                          └───────┬──────┘
                                  │
       ┌──────────────┬───────────┼───────────┬──────────────┐
       │              │           │           │              │
┌──────▼─────┐ ┌──────▼────┐ ┌────▼─────┐ ┌──▼──────┐ ┌──────▼─────┐
│ 13 Pipe    │ │ 14 PubSub │ │ 15 Cluster│ │ 16 Sent│ │ 17 Compat  │
└──────┬─────┘ └──────┬────┘ └────┬─────┘ └──┬──────┘ └──────┬─────┘
       │              │           │          │               │
       └──────────────┴───────────┼──────────┴───────────────┘
                                  │
                          ┌───────▼──────┐
                          │ 18 Benchmarks│
                          └───────┬──────┘
                                  │
                          ┌───────▼──────┐
                          │ 19 Distrib   │
                          └──────────────┘
```

## File-structure invariants every plan respects

- One Rust file per command family under `crates/redis-rs-py-driver/src/commands/{strings,lists,hashes,sets,zsets,streams,scripts,admin}.rs`. Each file holds the sync + async pair for every command in its family; both call shared private helpers when their bodies would otherwise diverge.
- Façade pyclasses live under `crates/redis-rs-py-driver/src/facade/{sync,asyncio,cluster,sentinel,pubsub,pipeline,pool,decode,kwargs}.rs`, registered into `_driver` (and into the `_driver.asyncio` / `_driver.cluster` / `_driver.sentinel` submodules) by `lib.rs`.
- Exception types live in `crates/redis-rs-py-driver/src/exceptions.rs` and are exposed under both `redis_rs_py.exceptions` (Python module) and as attributes on `redis_rs_py` for `redis-py` compatibility (`from redis_rs_py import RedisError`).
- Python tree stays paper-thin: `python/redis_rs_py/__init__.py` re-exports from `_driver`, `python/redis_rs_py/asyncio/__init__.py` re-exports from `_driver.asyncio`, `python/redis_rs_py/cluster/__init__.py` from `_driver.cluster`, `python/redis_rs_py/sentinel/__init__.py` from `_driver.sentinel`. `python/redis_rs_py/exceptions.py` re-exports from `_driver.exceptions`. `python/redis_rs_py/_driver.pyi` is hand-maintained until `pyo3-stub-gen` becomes viable.
- Tests live under `tests/{driver,facade,pipeline,pubsub,cluster,sentinel,compat,async_bridge}/test_*.py` with one file per command family / topic. `tests/conftest.py` owns the live-Valkey testcontainers fixtures (`valkey_url`, `cluster_urls`, `sentinel_urls`, `valkey_client_sync`, `valkey_client_async`) and is loaded once per process via the `xdist`-friendly group fixture pattern.

## Free-threaded (cp314t) invariants every plan respects

- All Rust globals are `Sync` (the runtime registry is `OnceLock + AtomicU32 + Mutex`; the connection inner enum is `Arc`-cloned).
- Every PyO3 `#[pyclass]` is `Send + Sync` by construction (no `Rc`, no `RefCell`).
- Tests run under `pytest -n auto` against both `python3.14` and `python3.14t` interpreters in CI; the test suite must be free of GIL-implicit-serialization assumptions.

## Done definition for v0.1

When all 19 plans have been executed and merged:

1. `import redis_rs_py as redis` works; every covered redis-py method has the same signature and return shape.
2. `import redis_rs_py.asyncio as redis` works; same surface, async-coloured.
3. The compatibility matrix in the README has a green checkmark in the "Implemented" column for every covered method, with no silent gaps.
4. `pip install redis-rs-py` on Linux/macOS/Windows × cp310/cp311/cp312/cp313/cp314/cp314t works, no compiler required.
5. The benchmark suite reproducibly shows `redis-rs-py` strictly outperforming `redis-py[hiredis]` on every axis and matching-or-beating `valkey-glide` on async throughput, on a fixed reference machine, with results in `benchmarks/RESULTS.md`.
