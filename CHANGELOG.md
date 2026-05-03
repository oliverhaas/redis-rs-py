# Changelog

All notable changes to redis-rs-py will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Full `redis.exceptions`-compatible hierarchy (`RedisError`, `ConnectionError`, `TimeoutError`, `BusyLoadingError`, `AuthenticationError`, `ResponseError`, `NoScriptError`, `ExecAbortError`, `ReadOnlyError`, `OutOfMemoryError`, `NoPermissionError`, `ModuleError`, `LockError`, `LockNotOwnedError`, `WatchError`, `PubSubError`, `MasterDownError`, `SlaveError`, `ClusterError`, `ClusterDownError`, `ClusterCrossSlotError`, `MovedError`, `AskError`, `TryAgainError`, `DataError`, `InvalidResponse`, `AuthenticationWrongNumberOfArgsError`).
- Boundary classifier `errors::classify_error` mapping `redis-rs` errors → exception class via `ErrorKind` + `ServerErrorKind` + message-prefix sniffing.
- Top-level re-exports: `from redis_rs_py import RedisError` matches the redis-py user idiom.
- Tokio runtime singleton with PID-checked fork-safe rebuild (lifted from `django-cachex-redis-rs` / `django-vcache`).
- `RedisRsAwaitable` — custom asyncio bridge with 5-poll busy-yield + callback-mode fallback, full done-callback (with `contextvars.Context` support) + cancellation-wakes-pending-callbacks support.
- `RawResult` typed boundary with 20 variants and a recursive `redis::Value` → Python converter.
- `IntoRawResult` trait + 20 `From<T> for RawResult` blanket impls so command bodies can write `.into_raw_result()`.
- `errors::classify` / `to_py_err` placeholder error classifier (Plan 02 swaps to the full `redis.exceptions` hierarchy).
- `ValkeyConn` two-layer connection wrapper (regular + lazy blocking via `OnceCell`), `connect_standard(url, cache, tls)` factory, RESP3 URL normaliser, and the `dispatch_cmd!` / `conn_method!` dispatch macros.
- `RedisRsDriver.connect_standard(url, *, cache_max_size, cache_ttl_secs, ssl_*)` factory exposing TLS plumb-through and client-side caching opts.
- Canonical commands `get` / `aget` / `set` / `aset` / `delete` / `adelete` / `ping` / `aping` proving the end-to-end pipeline against live Valkey via `testcontainers`.
- `connection_url` getter (returns the resp3-rewritten URL) and `cache_statistics()` (returns `(hit, miss, invalidate)` tuple or `None` when caching isn't enabled).
- Eight `_test_*` awaitable helpers exercising every code path of the bridge in isolation (resolved/none/int, delayed/callback-mode, pending, dropped, error, server-error).
- Hand-maintained `python/redis_rs_py/_driver.pyi` type stubs covering the foundation surface.
- `from redis_rs_py import RedisRsDriver, RedisRsAwaitable, __version__` top-level re-exports.
- Driver list commands: `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`/`RPOP` (with `count=`), `LRANGE`, `LLEN`, `LMOVE`, `LPOS` (with `rank=`/`count=`/`maxlen=`), `LREM`, `LINDEX`, `LSET`, `LINSERT`, `LTRIM`, `LMPOP`. Sync + async pair for every command.
- Blocking list commands: `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, routed through a lazily-allocated second `ConnectionManager` (no response timeout) so a long BLPOP never head-of-line-blocks the multiplexed pipeline.
- Full hash command surface: `HGET`, `HSET` (mapping= kwarg + variadic field/value pairs), `HSETNX`, `HMSET` (deprecated one-shot alias with `DeprecationWarning`), `HMGET`, `HGETALL` → `dict[bytes, bytes]`, `HDEL`, `HINCRBY`, `HINCRBYFLOAT` (direct `f64`, no String round-trip), `HKEYS`, `HVALS`, `HEXISTS`, `HLEN`, `HSCAN` (cursor + match/count/novalues), `HRANDFIELD` (count + withvalues). Sync + async pair for every command.
- Hash field-TTL family (Redis ≥ 7.4 only; skipped on Valkey): `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HEXPIRETIME`, `HPEXPIRETIME`, `HTTL`, `HPTTL`, `HPERSIST`, all supporting the NX/XX/GT/LT modifier matrix.
- Three new `RawResult` variants (`IntList`, `HRandfield`, `HScan`) and a `From<Vec<i64>>` impl to carry hash-scan and hrandfield results through the async bridge.
