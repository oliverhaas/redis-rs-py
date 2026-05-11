# Benchmarks

**This report is generated.** Re-run via:

```
uv run --group bench python benchmarks/run_all.py
```

## Reference machine

- Generated: 2026-05-11T06:19:51+00:00
- CPU: x86_64
- Platform: Linux-6.17.0-22-generic-x86_64-with-glibc2.39
- Python: 3.14.2 (main, Dec 17 2025, 21:08:09) [Clang 21.1.4 ]
- Valkey image: `valkey/valkey:8.0`
- Run mode: smoke (--codspeed-max-rounds=1)

## Results

Higher ops/sec is better. **Bold** is the redis-rs-py baseline; the parenthesised number for each competitor is its multiple of the baseline (1.50x = 50% faster than us, 0.50x = half our throughput).

| Scenario | redis-rs-py | redis-py[hiredis] | valkey-glide |
|---|---|---|---|
| `async-100` | **2,041 ops/s** (489.9 us) | 280 ops/s (0.14x) | 385 ops/s (0.19x) |
| `async-single` | **5,676 ops/s** (176.2 us) | 8,866 ops/s (1.56x) | 4,620 ops/s (0.81x) |
| `connect` | **2,797 ops/s** (357.5 us) | 936 ops/s (0.33x) | 961 ops/s (0.34x) |
| `get` | **13,208 ops/s** (75.7 us) | 13,278 ops/s (1.01x) | 5,940 ops/s (0.45x) |
| `mget` | **7,122 ops/s** (140.4 us) | 10,119 ops/s (1.42x) | 3,813 ops/s (0.54x) |
| `pipeline-1000` | **863 ops/s** (1158.1 us) | 403 ops/s (0.47x) | 296 ops/s (0.34x) |
| `pubsub-1000` | **14 ops/s** (70435.1 us) | 10 ops/s (0.67x) | 6 ops/s (0.41x) |
| `set` | **12,316 ops/s** (81.2 us) | 12,644 ops/s (1.03x) | 6,552 ops/s (0.53x) |

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
