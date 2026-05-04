# Benchmarks

**This report is generated.** Re-run via:

```
uv run --group bench python benchmarks/run_all.py
```

## Reference machine

- Generated: 2026-05-04T03:28:33+00:00
- CPU: x86_64
- Platform: Linux-6.17.0-22-generic-x86_64-with-glibc2.39
- Python: 3.14.2 (main, Dec 17 2025, 21:08:09) [Clang 21.1.4 ]
- Valkey image: `valkey/valkey:8.0`
- Run mode: smoke (--values 1)

## Results

Higher ops/sec is better. **Bold** is the redis-rs-py baseline; the parenthesised number for each competitor is its multiple of the baseline (1.50x = 50% faster than us, 0.50x = half our throughput).

| Scenario | redis-rs-py | redis-py[hiredis] | valkey-glide |
|---|---|---|---|
| `async-100` | **1,376 ops/s** (726.9 us) | 11 ops/s (0.01x) | 331 ops/s (0.24x) |
| `async-single` | **2,618 ops/s** (381.9 us) | 617 ops/s (0.24x) | 3,050 ops/s (1.16x) |
| `connect` | **273 ops/s** (3660.8 us) | 39 ops/s (0.14x) | 17 ops/s (0.06x) |
| `get` | **8,047 ops/s** (124.3 us) | 1,597 ops/s (0.20x) | 2,983 ops/s (0.37x) |
| `mget` | **3,803 ops/s** (263.0 us) | 1,420 ops/s (0.37x) | 2,354 ops/s (0.62x) |
| `pipeline-1000` | **720 ops/s** (1388.9 us) | 316 ops/s (0.44x) | 243 ops/s (0.34x) |
| `pubsub-1000` | **13 ops/s** (75447.4 us) | 10 ops/s (0.72x) | 6 ops/s (0.45x) |
| `set` | **5,867 ops/s** (170.5 us) | 1,639 ops/s (0.28x) | 3,172 ops/s (0.54x) |

## Methodology

- One Valkey container per `run_all.py` invocation; FLUSHDB between scenarios.
- Each scenario script is launched as a fresh subprocess (pyperf default), so no client warms the OS page cache for the next.
- All clients use the same database (db=0), `decode_responses=False`, the same hot-key payload (100 bytes).
- pyperf collects warmup + calibration + median + p99 per scenario (skipped under `--smoke`).
- valkey-glide has no sync API; sync scenarios run via `asyncio.run(...)` per iteration. The setup overhead this adds is **disclosed** but not amortised — direct comparison of valkey-glide on sync scenarios is structurally pessimistic for it.
- Valkey image is pinned (`BENCH_VALKEY_IMAGE` env, defaults to `valkey/valkey:8.0`).
- pyperf-tuned CI runners are unstable across cloud providers — the **source of truth** is a local run on the reference machine documented above. CI runs are smoke-only and exist to prevent regressions in the bench-suite plumbing, not to publish numbers.

## Reproducing locally

```bash
# 1. system tune (optional but recommended; reduces noise to <1%)
uv run --group bench python -m pyperf system tune

# 2. run the full sweep
uv run --group bench python benchmarks/run_all.py

# 3. re-render this file from the cached JSON without re-running
uv run --group bench python benchmarks/run_all.py --render-only

# 4. (optional) restore default CPU governor
uv run --group bench python -m pyperf system reset
```
