# Benchmarks

**This report is generated.** Re-run via:

```
uv run --group bench python benchmarks/run_all.py
```

## Reference machine

- Generated: 2026-05-11T07:54:07+00:00
- CPU: x86_64
- Platform: Linux-6.17.0-22-generic-x86_64-with-glibc2.39
- Python: 3.14.2 (main, Dec 17 2025, 21:08:09) [Clang 21.1.4 ]
- Valkey image: `valkey/valkey:8.0`
- Run mode: full (codspeed walltime defaults)

## Results

Higher ops/sec is better. **Bold** is the redis-rs-py baseline; the parenthesised number for each competitor is its multiple of the baseline (1.50x = 50% faster than us, 0.50x = half our throughput).

| Scenario | redis-rs-py | redis-py[hiredis] | valkey-glide |
|---|---|---|---|
| `async-100` | **2,355 ops/s** (424.6 us) | 286 ops/s (0.12x) | 583 ops/s (0.25x) |
| `async-single` | **9,107 ops/s** (109.8 us) | 10,357 ops/s (1.14x) | 6,484 ops/s (0.71x) |
| `connect` | **3,775 ops/s** (264.9 us) | 492 ops/s (0.13x) | 417 ops/s (0.11x) |
| `get` | **18,746 ops/s** (53.3 us) | 15,671 ops/s (0.84x) | 6,479 ops/s (0.35x) |
| `mget` | **10,212 ops/s** (97.9 us) | 10,741 ops/s (1.05x) | 4,313 ops/s (0.42x) |
| `pipeline-1000` | **1,142 ops/s** (875.7 us) | 387 ops/s (0.34x) | 339 ops/s (0.30x) |
| `pubsub-1000` | **14 ops/s** (70162.7 us) | 10 ops/s (0.68x) | 6 ops/s (0.41x) |
| `set` | **18,632 ops/s** (53.7 us) | 14,330 ops/s (0.77x) | 6,386 ops/s (0.34x) |

## Methodology

- One Valkey container per `run_all.py` invocation; FLUSHDB between scenarios where state matters.
- All clients use the same database (db=0), `decode_responses=False`, and the same hot-key payload (100 bytes).
- Bench tests share a single Python process. pytest-codspeed (walltime mode) calibrates the inner-iteration count from a warmup pass and reports the median round time; we convert to ops/sec as `1 / median`. Cross-client cache contamination is acknowledged and discussed in the project README — for absolute publishable numbers, pin the order or run scenarios in isolated subprocesses.
- Codspeed runs a 1-second warmup by default (full run); smoke mode sets `--codspeed-warmup-time=0 --codspeed-max-rounds=1`.
- valkey-glide has no sync API; sync scenarios run via `loop.run_until_complete(coro)` per call. The setup overhead this adds is **disclosed** but constant — direct comparison of valkey-glide on sync scenarios is structurally pessimistic for it.
- Valkey image is pinned (`BENCH_VALKEY_IMAGE` env, defaults to `valkey/valkey:8.0`).
- CI runners are noisy across cloud providers — the **source of truth** is a local run on the reference machine documented above. CI runs are smoke-only and exist to prevent regressions in the bench-suite plumbing, not to publish numbers.

## Reproducing locally

```bash
# 1. run the full sweep (spawns Valkey, runs pytest, renders this file)
uv run --group bench python benchmarks/run_all.py

# 2. re-render this file from the cached JSON without re-running
uv run --group bench python benchmarks/run_all.py --render-only

# 3. run a single scenario by keyword
uv run --group bench python benchmarks/run_all.py --scenario get
```
