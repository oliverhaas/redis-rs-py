# Changelog

All notable changes to redis-rs-py will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
