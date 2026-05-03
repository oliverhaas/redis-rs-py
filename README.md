# redis-rs-py

[![PyPI version](https://img.shields.io/pypi/v/redis-rs-py.svg?style=flat)](https://pypi.org/project/redis-rs-py/)
[![Python versions](https://img.shields.io/pypi/pyversions/redis-rs-py.svg)](https://pypi.org/project/redis-rs-py/)
[![CI](https://github.com/oliverhaas/redis-rs-py/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/redis-rs-py/actions/workflows/ci.yml)

High-performance, drop-in replacement for [`redis-py`](https://github.com/redis/redis-py) and [`valkey-py`](https://github.com/valkey-io/valkey-py), built on PyO3 + tokio + [`redis-rs`](https://github.com/redis-rs/redis-rs).

> **Status:** pre-alpha scaffold. Not yet usable.

## Why

The current Python Redis/Valkey client landscape is a choice between three unhappy options:

- **`redis-py`** — the de-facto standard, huge API surface, but Python-native. Even with `hiredis` accelerating parsing, connection management and async I/O still pay full Python overhead.
- **`valkey-py`** — a fork of `redis-py` for Valkey. Same Python-native architecture, same performance ceiling.
- **`valkey-glide`** — Rust core, multi-language. Genuinely fast, but the Python binding is a thin shell over `glide-core` with its own (non-redis-py) API. Migrating from `redis-py` is a rewrite.

`redis-rs-py` aims to be all three at once: as fast as a Rust-core client, drop-in compatible with `redis-py`, and as simple to install as `pip install redis-rs-py`.

The architecture: a single Rust extension module (`_driver`) owns a tokio runtime, the redis-rs connection pools (single / cluster / sentinel), TLS via rustls, and the redis-py-compatible API surface. The Python tree is essentially a thin re-export over the compiled `.so`.

## Benchmarks

*(Coming soon — the README will lead with benchmarks once there's something to measure. Comparison targets: `redis-py`, `redis-py[hiredis]`, `valkey-py`, `valkey-glide`.)*

## Installation

```console
pip install redis-rs-py
```

Prebuilt wheels are published for Linux (x86_64, aarch64), macOS (arm64), and Windows (amd64), for both standard and free-threaded CPython 3.14.

## License

MIT
