# Benchmarks

**This report is generated.** Re-run via:

```
uv run --group bench python benchmarks/run_all.py
```

## Reference machine

- Generated: 2026-05-05T12:55:07+00:00
- CPU: x86_64
- Platform: Linux-6.17.0-22-generic-x86_64-with-glibc2.39
- Python: 3.14.2 (main, Dec 17 2025, 21:08:09) [Clang 21.1.4 ]
- Valkey image: `valkey/valkey:8.0`
- Run mode: smoke (--benchmark-min-rounds=1)

## Results

Higher ops/sec is better. **Bold** is the redis-rs-py baseline; the parenthesised number for each competitor is its multiple of the baseline (1.50x = 50% faster than us, 0.50x = half our throughput).

| Scenario | redis-rs-py | redis-py[hiredis] | valkey-glide |
|---|---|---|---|
| `async-100` | **2,475 ops/s** (404.0 us) | 299 ops/s (0.12x) | 606 ops/s (0.24x) |
| `async-single` | **10,742 ops/s** (93.1 us) | 10,019 ops/s (0.93x) | 6,353 ops/s (0.59x) |
| `connect` | **3,811 ops/s** (262.4 us) | 1,460 ops/s (0.38x) | 1,026 ops/s (0.27x) |
| `get` | **19,309 ops/s** (51.8 us) | 16,883 ops/s (0.87x) | 6,643 ops/s (0.34x) |
| `mget` | **10,405 ops/s** (96.1 us) | 11,510 ops/s (1.11x) | 4,371 ops/s (0.42x) |
| `pipeline-1000` | **1,224 ops/s** (817.1 us) | 411 ops/s (0.34x) | 340 ops/s (0.28x) |
| `pubsub-1000` | **15 ops/s** (65784.4 us) | 10 ops/s (0.69x) | 6 ops/s (0.40x) |
| `set` | **19,625 ops/s** (51.0 us) | 16,228 ops/s (0.83x) | 6,437 ops/s (0.33x) |

## Methodology

- One Valkey container per `run_all.py` invocation; FLUSHDB between scenarios where state matters.
- All clients use the same database (db=0), `decode_responses=False`, and the same hot-key payload (100 bytes).
- Bench tests share a single Python process. pytest-benchmark calibrates inner-iteration count per benchmark and reports the median. Cross-client cache contamination is acknowledged and discussed in the project README — for absolute publishable numbers, pin the order or run scenarios in isolated subprocesses.
- pytest-benchmark warmup is on by default (full run); smoke mode disables it.
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
