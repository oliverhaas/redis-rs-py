# redis-rs-py — Implementation Plan

> Source: brainstormed and committed as the idea file at
> [`oliverhaas/ideas:packages/python/redis-rs-py.md`](https://github.com/oliverhaas/ideas/blob/main/packages/python/redis-rs-py.md).
> Mirrored here so this repo is self-contained for new sessions.

A high-performance, drop-in replacement for [`redis-py`](https://github.com/redis/redis-py) and [`valkey-py`](https://github.com/valkey-io/valkey-py), built on PyO3 + tokio + [`redis-rs`](https://github.com/redis-rs/redis-rs). Rust core does all I/O, connection management, and protocol parsing; Python is a thin façade that mirrors the redis-py API.

Prior internal experiment: the Rust I/O driver inside [`django-cachex-redis-rs`](https://github.com/oliverhaas/django-cachex) (~4.7K LOC, tokio + redis-rs `connection-manager` + `cluster-async` + `cache-aio`, custom Python-awaitable bridge) was measured as substantially faster than both `redis-py` (with `hiredis`) and `valkey-glide`. This package generalises that approach into a standalone, redis-py-compatible client.

## Current state of this repo

Scaffolded. Nothing functional yet.

- Maturin pyproject + Cargo workspace + `crates/redis-rs-py-driver/` with a minimal `_driver` pymodule that only exposes `__version__`.
- Python tree: `python/redis_rs_py/{__init__.py, _driver.pyi, py.typed}` — paper-thin, per the "Rust by default" principle.
- Tests: 2 smoke tests in `tests/test_smoke.py` verifying the extension imports.
- Tooling installed: ruff (`ALL` rules with Redis-API ignores), ty, pytest + pytest-asyncio + pytest-xdist + pytest-cov + testcontainers, redis + valkey reference clients, maturin, pre-commit.
- Pre-commit hooks installed (pre-commit + pre-push); cargo fmt + clippy on push.
- CI: `ci.yml` (lint + test on cp314/cp314t + cross-platform wheel build + smoke-test), `tag.yml` (auto-tag on version bump), `dependabot-automerge.yml`, `dependabot.yml` for uv/cargo/actions.
- `publish.yml` is **commented out** for now — re-enable when ready to publish to PyPI (also needs PyPI Trusted Publisher + `pypi` GitHub environment configured first).

## Problem

The current Python Redis/Valkey client landscape is a choice between three unhappy options:

- **`redis-py`** — the de-facto standard, huge API surface, but Python-native. Even with `hiredis` accelerating parsing, connection management and async I/O still pay full Python overhead. Async throughput is mediocre because each command round-trips through Python.
- **`valkey-py`** — a fork of `redis-py` for Valkey. Same Python-native architecture, same performance ceiling.
- **`valkey-glide`** — Rust core, multi-language. Genuinely fast, but the Python binding is a thin shell over `glide-core` with its own (non-redis-py) API, a heavier install, and ergonomics that feel grafted-on. Migrating from `redis-py` is a rewrite, not an import swap.

`hiredis-py` (and `redis-py[hiredis]`) only accelerates parsing. The connection pool, async I/O multiplexing, cluster topology handling, retry logic — all of it stays in Python.

There is no Python client that is simultaneously **(a)** as fast as a Rust-core client, **(b)** drop-in compatible with `redis-py`, and **(c)** lightweight and idiomatic to install and use. That's the gap.

## Scope

### Positioning

Pragmatic 80/20 drop-in for the *current* surface of `redis-py` and `valkey-py`. Targets the methods and APIs that 95% of users actually touch. **No deprecated APIs** — anything either upstream has marked deprecated is skipped on principle, with a clear note in the compatibility matrix. The promise: change one import (or shadow the package name), keep your code, get materially better throughput and latency.

### Architecture

**Guiding principle: Rust by default, Python only when forced.** The aspirational target is a package that's literally one `.so` plus a thin `__init__.py` re-export and a `.pyi` stub — no hand-written Python implementation code. This isn't a hard rule (we won't fight PyO3 to the death over a 5-line Python helper), but every design decision starts from "can this be a Rust pyclass?" and only retreats to Python when the cost is clearly disproportionate.

Both the high-level `redis-py`-compatible API and the low-level driver live in Rust as `#[pyclass]` types:

1. **Low-level driver** (`redis_rs_py._driver.RedisRsDriver`) — the typed, method-per-command surface. Each command exists in two forms: `get(...)` (sync, blocks the thread, releases the GIL across I/O) and `aget(...)` (async, returns a custom awaitable bridged from a tokio future). Power users can drop down to this layer for zero overhead.
2. **High-level façade** (`redis_rs_py.Redis`, `redis_rs_py.asyncio.Redis`, `redis_rs_py.cluster.RedisCluster`, `redis_rs_py.sentinel.Sentinel`) — also Rust pyclasses, exposing redis-py-shaped constructors, kwargs, response shapes, and exception types. Wraps a driver internally. The façade is where the "drop-in" promise lives; doing it in Rust means the kwarg translation, response shaping, exception mapping, and pipeline/pubsub state machines all run at native speed.

Exception classes are defined via PyO3's `create_exception!` to mirror `redis.exceptions`. Pipeline, PubSub, ConnectionPool shim, and Sentinel/Cluster types are all Rust pyclasses.

The single Rust component (built with `maturin`, lives under `crates/redis-rs-py-driver/`) compiles into a single `_driver` extension module and is bundled into the published wheel. Users never see the Rust side; they `pip install redis-rs-py` and `import redis_rs_py`.

A single tokio multi-thread runtime owns:
- Connection pools (single / cluster / sentinel)
- TLS via rustls
- All I/O multiplexing (true async; not "wrap a sync call in a thread")
- Retry / backoff / failover logic (delegated to redis-rs's connection-manager and cluster-async)

Where Python code may still be unavoidable:
- The package's `__init__.py` for re-exports and to expose the `asyncio` submodule namespace cleanly.
- `.pyi` stub generation for type checkers (Rust pyclasses don't auto-publish stubs).
- A handful of pure-data adapters if PyO3 binding turns out to be needlessly verbose for them (e.g. constructing a `warnings.warn` call site). Treat these as exceptions, not the rule.

### v0.1 surface (must ship)

- **Connections.** `Redis(host=, port=, db=, password=, username=, ssl=, socket_timeout=, max_connections=, ...)`, `Redis.from_url(url)`, plus `RedisCluster` and `Sentinel`. Constructor kwargs that don't map to our pooling model are accepted and ignored with a one-shot warning per unknown kwarg — never raise on a kwarg `redis-py` accepts.
- **Async parity.** `redis_rs_py.asyncio.Redis` mirrors `redis.asyncio.Redis`. Same method names — no `a`-prefix at the façade layer; that's a driver-level convention only.
- **Commands.** Everything currently in the `django-cachex-redis-rs` driver (strings, lists incl. blocking `BLPOP`/`BLMOVE`/`BLMPOP`, hashes, sets, sorted sets, streams incl. groups/pending/claim/autoclaim, scan, scripts, info/config/object) **plus** the gaps:
  - Full `SET` option matrix (`EX`/`PX`/`NX`/`XX`/`KEEPTTL`/`GET`/`EXAT`/`PXAT`)
  - `GETEX`, `GETDEL`, `COPY`
  - `CLIENT KILL`/`GETNAME`/`SETNAME`
  - `CONFIG SET` / `CONFIG RESETSTAT`
- **Pipelines & transactions.** `r.pipeline(transaction=True)` context manager; `WATCH` / `UNWATCH` / `MULTI` / `EXEC` semantics matched. Sticky-connection mode in the driver for the duration of a transaction.
- **Pub/Sub.** `r.pubsub()` returning a PubSub object with `subscribe` / `psubscribe` / `get_message` / `listen`. Async equivalent. Each `pubsub()` call gets a dedicated subscriber connection in the Rust core.
- **Response model.** `decode_responses=False` (bytes, default) and `True` (str). Native types where redis-py gives natives (sets → `set`, hashes → `dict`, sorted sets → list of tuples).
- **Exceptions.** Full redis-py exception hierarchy (`RedisError`, `ConnectionError`, `TimeoutError`, `ResponseError`, `DataError`, `BusyLoadingError`, `NoScriptError`, `ReadOnlyError`, `AuthenticationError`, `ClusterDownError`, …). Translated from redis-rs errors at the boundary.
- **Compatibility matrix.** Every redis-py public method gets a row in the README: ✅ implemented / ⚠️ partial (with notes) / ❌ deferred. No silent gaps.

### Deferred (v0.2+ or never)

- Module clients (RedisJSON / RediSearch / RedisTimeSeries / RedisBloom / RedisGears) — these are usually separate clients in the redis-py ecosystem too. Defer indefinitely unless community asks.
- `MONITOR`, `DEBUG OBJECT`, `LATENCY *`, `CLIENT TRACKING` from Python (Rust core already has client-side caching internally via `cache-aio`; exposing the config to Python is v0.2).
- `CLUSTER FAILOVER`, custom RESP3 push handlers.
- OCSP stapling, custom Python `SSLContext` (rustls-only TLS).
- `register_script` helper (you can already do `script_load` + `evalsha`).
- Connection-pool tunables beyond `max_connections` and timeouts — the Rust pool is its own animal; v0.1 surfaces a small subset.

### Compatibility policy

- Tracks the latest stable `redis-py` and `valkey-py` minor versions. Newly-added current commands land via PRs; no release-day SLA. Deprecated additions are skipped on principle.
- No bug-for-bug compat. If `redis-py` has a documented quirk that's clearly wrong, we deviate and document it in the matrix.
- The coverage matrix in the README is the contract, not full feature mirroring.

### Differentiation

- vs. `redis-py` — faster on every axis: no Python-side parser, no per-command Python overhead in the hot path, true async multiplexing.
- vs. `valkey-glide` — idiomatic Python, smaller install footprint, plain `pip install` (no glide-core layer to reason about), redis-py-compatible API surface (glide is its own API, migration cost is high).
- vs. `redis-py[hiredis]` — `hiredis` only accelerates parsing; this moves all of it (connection management, multiplexing, retries, parsing, cluster topology) into Rust.

### Target package layout

The aspirational shape: almost everything in Rust, the Python tree is paper-thin. The crate name is `redis-rs-py-driver` (already scaffolded); the new files to create as the implementation lands are the ones under `src/facade/` and the per-family `commands/` files.

```
redis-rs-py/
  pyproject.toml                # maturin build, abi3 where PyO3 0.28 allows
  Cargo.toml                    # workspace root
  crates/
    redis-rs-py-driver/         # Rust component → _driver extension module
      Cargo.toml
      src/
        lib.rs                  # pymodule registration; submodules for asyncio/cluster/sentinel
        driver.rs               # RedisRsDriver pyclass (low-level)
        connection.rs           # standard / cluster / sentinel pool wiring
        async_bridge.rs         # RedisRsAwaitable (custom, not pyo3-async-runtimes)
        facade/
          sync.rs               # Redis pyclass (high-level, redis-py-compatible)
          asyncio.rs            # Redis pyclass for the asyncio submodule
          cluster.rs            # RedisCluster pyclass
          sentinel.rs           # Sentinel pyclass
          pubsub.rs             # PubSub pyclass
          pipeline.rs           # Pipeline / transaction pyclass
          pool.rs               # ConnectionPool config-carrier shim
          decode.rs             # decode_responses + response shape adapters
          kwargs.rs             # accept-and-warn for redis-py kwargs we ignore
        exceptions.rs           # create_exception! for the redis.exceptions hierarchy
        commands/               # one file per command family if driver.rs grows
  python/
    redis_rs_py/
      __init__.py               # re-exports from _driver; exposes asyncio submodule
      _driver.pyi               # generated stub for Rust pyclasses
  tests/
    conftest.py                 # pytest fixtures (live Redis/Valkey via testcontainers)
    test_driver.py              # low-level driver
    test_facade_sync.py         # high-level façade, sync
    test_facade_async.py        # high-level façade, async (pytest-asyncio)
    test_pipeline.py
    test_pubsub.py
    test_cluster.py
    test_sentinel.py
    test_compat_redis_py.py     # parity assertions vs redis-py on covered surface
  benchmarks/
    bench_get_set.py
    bench_pipeline.py
    bench_pubsub.py
    bench_async_throughput.py   # vs redis-py, vs valkey-glide
  README.md                     # leads with benchmarks, then compat matrix, then quickstart
```

### Distribution

- Prebuilt wheels: Linux x86_64 (cp310–cp314, cp314t), macOS arm64 (cp311–cp314), Windows x86_64 (cp311–cp314). `abi3` where PyO3 0.28 allows.
- musllinux for Alpine. aarch64 for Linux/macOS. sdist as fallback.
- CI: `maturin` + `cibuildwheel` via GitHub Actions. Mirror what `django-cachex` does.

### Tooling

`ruff` (lint + format), `ty` (type check), `pytest` + `pytest-asyncio`, `maturin` (build), `pre-commit`, GitHub Actions for CI/CD. No `mkdocs` for v0.1 — the README *is* the documentation, and benchmarks are its core.

### Testing approach

Live Redis and Valkey servers via `testcontainers`. Tests run against real instances, not mocks. Cluster tests spin up a multi-node fixture; sentinel tests spin up a sentinel set.

Take *inspiration* from the test suites of `redis-py`, `valkey-py`, and `valkey-glide` — they're solving the same problem and have already enumerated the interesting edge cases (RESP2/RESP3 quirks, pub/sub message ordering, cluster slot migration, sentinel failover, pipeline error handling). Don't copy verbatim; lift the test ideas, write our own pytest cases that fit our fixtures and conventions.

## Risks & open questions

- **Maintenance treadmill.** Mirroring two upstream clients means perpetual catch-up. Mitigation: the compatibility matrix is the contract, not full feature mirroring; new commands land via PRs, not on a release-day SLA.
- **`WATCH`/`MULTI`/`EXEC` under a multiplexed pool.** `WATCH` requires a sticky connection. The Rust core multiplexes by default, so transactions need a "reserve a connection from the pool for the duration of this Pipeline" mode. Solvable, but a real complication of the pure submit/await model — needs an explicit design pass before v0.1.
- **Pub/Sub under a multiplexed pool.** Same problem at larger scale: a subscription holds a connection. Plan: a separate "subscriber" object in the Rust core that owns its own dedicated connection per `pubsub()` call, with messages bridged into Python via the awaitable channel.
- **`ConnectionPool` semantics.** redis-py exposes `ConnectionPool` as a first-class object users construct and pass around. We don't have a Python-side pool. Façade exposes a `ConnectionPool` shim that's really a config carrier — accepts the kwargs, hands them to a driver. Document the divergence in the matrix.
- **asyncio cancellation.** Awaitable bridge must honour `task.cancel()` properly — propagate cancellation into the Rust runtime so the in-flight tokio future is dropped. Cachex's `RedisRsAwaitable.cancel()` already does this; verify it survives the broader command surface.
- **Free-threading.** cp314t wheels match the cachex pattern. Driver registry uses a mutex. Document GIL-vs-free-threaded behaviour and what's safe to share across threads.
- **Fork safety.** Same PID-checked registry pattern as cachex; users don't have to think about it under uvicorn / gunicorn workers.
- **Naming.** `redis-rs-py` is the working name. PyPI availability needs to be confirmed; fallbacks: `pyredrs`, `redrs`, `pyredisrs`. Avoid `valkey-py-rs` (collides with the official `valkey-py`).
- **Benchmark fairness.** "Faster than valkey-glide" is the load-bearing claim. Benchmarks must be reproducible, run on equivalent setups, and cover sync, async-single-task, and async-many-concurrent-tasks. Anything less is marketing, not evidence.
- **Future relationship with `django-cachex-redis-rs`.** v0.1 is a parallel implementation (likely starting as a fork of cachex's driver). Long-term the two should consolidate onto one Rust core, with cachex consuming `redis-rs-py-driver` directly. That consolidation is deferred until `redis-rs-py` is stable and the API surface has settled.

## Prior art

- [`redis-py`](https://github.com/redis/redis-py) — the canonical Python client. MIT. The API we're mirroring.
- [`valkey-py`](https://github.com/valkey-io/valkey-py) — Valkey fork of `redis-py`. Tracks Valkey-specific commands.
- [`valkey-glide`](https://github.com/valkey-io/valkey-glide) — Rust-core multi-language client. Apache 2.0. The performance bar to beat on async throughput.
- [`hiredis-py`](https://github.com/redis/hiredis-py) — C-extension RESP parser. Speeds up parsing only; everything else is still Python.
- [`redis-rs`](https://github.com/redis-rs/redis-rs) — the Rust client we wrap. MIT.
- [`PyO3`](https://github.com/PyO3/pyo3) — Rust↔Python bindings framework.
- [`maturin`](https://github.com/PyO3/maturin) — build tool for PyO3 packages.
- [`django-cachex-redis-rs`](https://github.com/oliverhaas/django-cachex) — the in-tree prototype this proposal generalises from.

## Design notes

- **Why not adopt `valkey-glide`'s `glide-core` instead of redis-rs?** `glide-core` is impressive engineering but it's a large dependency, has its own protocol abstractions, and is designed around its own multi-language API rather than a clean Rust-native client interface. `redis-rs` is mature, idiomatic Rust, smaller surface, and easier to bind cleanly to Python. The cachex experiment validated the path.
- **Why not contribute the perf gains upstream to `redis-py`?** The performance ceiling of `redis-py` is fundamentally constrained by being Python-native. You can micro-optimise parsing (which `hiredis` does) but you can't get true async multiplexing without rewriting the I/O core, at which point you're no longer `redis-py`.
- **Why mirror `redis-py` instead of designing a new "better" API?** Adoption cost. A faster client with a new API is a rewrite for every consumer; a faster client with the same API is one line in `requirements.txt`. The 80/20 drop-in framing is the entire value proposition for general-purpose use.
- **Why not also ship the module clients (JSON / Search / TimeSeries)?** Each is a substantial sub-project, the redis-py ecosystem treats them as separate clients (`redis-om`, `redisearch-py`, etc.), and binding them via redis-rs would require redis-rs feature work too. Out of scope for the v0.1 promise.
- **Why custom `RedisRsAwaitable` instead of `pyo3-async-runtimes`?** Already proven in cachex; gives precise control over cancellation, GIL acquisition, and runtime lifecycle. `pyo3-async-runtimes` is fine but adds a dependency and constrains the design.
- **Why push the façade into Rust too?** The redis-py-compatible layer is doing real work — kwarg translation, response shaping, exception mapping, pipeline state, pub/sub message dispatch. Doing it in Python would re-introduce per-call interpreter overhead and undo part of the perf advantage. The principle is "Rust by default, Python only when forced": if a piece of behaviour can be a `#[pyclass]`, it should be. The aspirational state is a package whose runtime is essentially a `.so` file plus a one-line `__init__.py` re-export — not because it's elegant, but because every Python frame on the hot path is a frame the competition pays and we don't.
