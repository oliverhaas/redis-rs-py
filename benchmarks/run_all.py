#!/usr/bin/env python3
"""Bench orchestrator — runs every bench script and renders RESULTS.md.

Usage:
    uv run --group bench python benchmarks/run_all.py [--smoke]

``--smoke`` cuts pyperf to a single value x single process for a fast
gate-on-PR run; the full nightly run uses pyperf's defaults
(``--rigorous --processes 5``).
"""

import argparse
import datetime as dt
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BENCH_DIR = REPO_ROOT / "benchmarks"
RESULTS_DIR = BENCH_DIR / "results"
RESULTS_MD = BENCH_DIR / "RESULTS.md"

# Each entry: (script-name-without-extension, output-json-name)
SCENARIOS: list[tuple[str, str]] = [
    ("bench_get_set", "get_set.json"),
    ("bench_pipeline", "pipeline.json"),
    ("bench_async_throughput", "async_throughput.json"),
    ("bench_pubsub", "pubsub.json"),
    ("bench_connect", "connect.json"),
]

# Display order mirrors the table in RESULTS.md.
CLIENT_ORDER = ["redis-rs-py", "redis-py[hiredis]", "valkey-glide"]
VALKEY_IMAGE = os.environ.get("BENCH_VALKEY_IMAGE", "valkey/valkey:8.0")


def _spawn_valkey() -> tuple[object, str]:
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = DockerContainer(VALKEY_IMAGE).with_exposed_ports(6379)
    container.start()
    wait_for_logs(container, "Ready to accept connections", timeout=30)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)
    return container, f"redis://{host}:{port}/0"


def _run_scenario(script: str, out_json: Path, smoke: bool, env: dict[str, str]) -> None:
    # Remove stale output so pyperf does not refuse to overwrite it.
    out_json.unlink(missing_ok=True)
    cmd: list[str] = [
        sys.executable,
        str(BENCH_DIR / f"{script}.py"),
        "-o",
        str(out_json),
    ]
    if smoke:
        # loops=1: one iteration per sample — sufficient to prove the bench
        # runs without crashing. pubsub already loops over MESSAGES_PER_BATCH
        # internally, so loops>1 multiplies that, making smoke mode very slow.
        cmd += ["--processes", "1", "--values", "1", "--warmups", "0", "--loops", "1"]
    else:
        cmd += ["--rigorous"]
    print(f"\n>>> {script}  ({'smoke' if smoke else 'full'})")
    subprocess.run(cmd, check=True, env=env)


def _load_pyperf_json(path: Path) -> dict[str, dict[str, float]]:
    """Return ``{benchmark_name: {median, mean, stdev, p99, ops_per_sec}}``."""
    raw = json.loads(path.read_text())
    out: dict[str, dict[str, float]] = {}
    for bench in raw.get("benchmarks", []):
        meta = bench.get("metadata", {})
        name = meta.get("name") or bench.get("name", "<unknown>")
        # Each benchmark has runs; flatten all warmups-excluded values.
        values: list[float] = []
        for run in bench.get("runs", []):
            values.extend(run.get("values", []))
        if not values:
            continue
        values.sort()
        n = len(values)
        median = values[n // 2]
        mean = sum(values) / n
        # Sample stdev (n-1).
        if n > 1:
            stdev = (sum((v - mean) ** 2 for v in values) / (n - 1)) ** 0.5
        else:
            stdev = 0.0
        p99 = values[max(0, int(n * 0.99) - 1)]
        # The bench_time_func value is "seconds for `loops` operations". We
        # lose that metadata in this collapse — but we report seconds-per-loop
        # which is the unit pyperf normalises to.
        ops_per_sec = 1.0 / median if median > 0 else 0.0
        out[name] = {
            "median": median,
            "mean": mean,
            "stdev": stdev,
            "p99": p99,
            "ops_per_sec": ops_per_sec,
        }
    return out


def _format_row(scenario: str, by_client: dict[str, dict[str, float]]) -> str:
    cells: list[str] = [f"`{scenario}`"]
    rs_ops = by_client.get("redis-rs-py", {}).get("ops_per_sec", 0.0)
    for client in CLIENT_ORDER:
        m = by_client.get(client)
        if m is None:
            cells.append("—")
            continue
        ops = m["ops_per_sec"]
        speedup = (ops / rs_ops) if (rs_ops > 0 and client != "redis-rs-py") else 1.0
        if client == "redis-rs-py":
            cells.append(f"**{ops:,.0f} ops/s** ({m['median'] * 1e6:.1f} us)")
        else:
            cells.append(f"{ops:,.0f} ops/s ({speedup:.2f}x)")
    return "| " + " | ".join(cells) + " |"


def _render_report(all_results: dict[str, dict[str, dict[str, float]]], full_run: bool) -> str:
    """Compose the RESULTS.md body from collected pyperf JSON."""
    now = dt.datetime.now(tz=dt.UTC).isoformat(timespec="seconds")
    py_ver = sys.version.replace("\n", " ")
    cpu = platform.processor() or platform.machine()
    parts = [
        "# Benchmarks",
        "",
        "**This report is generated.** Re-run via:",
        "",
        "```",
        "uv run --group bench python benchmarks/run_all.py",
        "```",
        "",
        "## Reference machine",
        "",
        f"- Generated: {now}",
        f"- CPU: {cpu}",
        f"- Platform: {platform.platform()}",
        f"- Python: {py_ver}",
        f"- Valkey image: `{VALKEY_IMAGE}`",
        f"- Run mode: {'full (pyperf --rigorous)' if full_run else 'smoke (--values 1)'}",
        "",
        "## Results",
        "",
        "Higher ops/sec is better. **Bold** is the redis-rs-py baseline; the parenthesised number for each competitor is its multiple of the baseline (1.50x = 50% faster than us, 0.50x = half our throughput).",
        "",
    ]

    # Group benchmarks by scenario family (the prefix before the slash).
    by_scenario: dict[str, dict[str, dict[str, float]]] = {}
    for _script, results in all_results.items():
        for full_name, metrics in results.items():
            if "/" not in full_name:
                continue
            scenario, client = full_name.split("/", 1)
            by_scenario.setdefault(scenario, {})[client] = metrics

    if by_scenario:
        parts.append("| Scenario | redis-rs-py | redis-py[hiredis] | valkey-glide |")
        parts.append("|---|---|---|---|")
        for scenario in sorted(by_scenario):
            parts.append(_format_row(scenario, by_scenario[scenario]))
    else:
        parts.append("*No results yet — run `python benchmarks/run_all.py` to populate.*")

    parts.extend(
        [
            "",
            "## Methodology",
            "",
            "- One Valkey container per `run_all.py` invocation; FLUSHDB between scenarios.",
            "- Each scenario script is launched as a fresh subprocess (pyperf default), so no client warms the OS page cache for the next.",
            "- All clients use the same database (db=0), `decode_responses=False`, the same hot-key payload (100 bytes).",
            "- pyperf collects warmup + calibration + median + p99 per scenario (skipped under `--smoke`).",
            "- valkey-glide has no sync API; sync scenarios run via `asyncio.run(...)` per iteration. The setup overhead this adds is **disclosed** but not amortised — direct comparison of valkey-glide on sync scenarios is structurally pessimistic for it.",
            "- Valkey image is pinned (`BENCH_VALKEY_IMAGE` env, defaults to `valkey/valkey:8.0`).",
            "- pyperf-tuned CI runners are unstable across cloud providers — the **source of truth** is a local run on the reference machine documented above. CI runs are smoke-only and exist to prevent regressions in the bench-suite plumbing, not to publish numbers.",
            "",
            "## Reproducing locally",
            "",
            "```bash",
            "# 1. system tune (optional but recommended; reduces noise to <1%)",
            "uv run --group bench python -m pyperf system tune",
            "",
            "# 2. run the full sweep",
            "uv run --group bench python benchmarks/run_all.py",
            "",
            "# 3. re-render this file from the cached JSON without re-running",
            "uv run --group bench python benchmarks/run_all.py --render-only",
            "",
            "# 4. (optional) restore default CPU governor",
            "uv run --group bench python -m pyperf system reset",
            "```",
            "",
        ],
    )
    return "\n".join(parts) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true", help="fast PR-gate run (single sample)")
    parser.add_argument("--render-only", action="store_true", help="re-render RESULTS.md from existing JSON dumps")
    parser.add_argument(
        "--scenario",
        action="append",
        default=None,
        help="run only the named scenario script (repeatable)",
    )
    args = parser.parse_args()

    all_results: dict[str, dict[str, dict[str, float]]] = {}

    if not args.render_only:
        container, url = _spawn_valkey()
        env = os.environ.copy()
        env["BENCH_VALKEY_URL"] = url
        try:
            for script, out_name in SCENARIOS:
                if args.scenario and script not in args.scenario:
                    continue
                out_path = RESULTS_DIR / out_name
                _run_scenario(script, out_path, args.smoke, env)
                all_results[script] = _load_pyperf_json(out_path)
        finally:
            container.stop()
    else:
        for script, out_name in SCENARIOS:
            out_path = RESULTS_DIR / out_name
            if out_path.exists():
                all_results[script] = _load_pyperf_json(out_path)

    RESULTS_MD.write_text(_render_report(all_results, full_run=not args.smoke))
    print(f"\nwrote {RESULTS_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
