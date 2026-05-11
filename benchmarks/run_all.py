#!/usr/bin/env python3
"""Bench orchestrator — spawns Valkey, runs the bench suite, renders RESULTS.md.

Usage:

    uv run --group bench python benchmarks/run_all.py [--smoke]

``--smoke`` runs each benchmark with a single round and no warmup for a
fast gate-on-PR run; the full nightly run uses pytest-codspeed's
walltime defaults (1 s warmup, up to 3 s of timed rounds).

Internally this just spins up a single Valkey container, exports
``BENCH_VALKEY_URL``, and exec's pytest against the ``benchmarks/``
directory with ``--codspeed --codspeed-mode=walltime``. Codspeed writes
a JSON dump per session to ``$CODSPEED_PROFILE_FOLDER/results/<pid>.json``;
we point it at ``benchmarks/results/`` and aggregate from there.
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

CLIENT_ORDER: list[str] = ["redis-rs-py", "redis-py[hiredis]", "valkey-glide"]
VALKEY_IMAGE = os.environ.get("BENCH_VALKEY_IMAGE", "valkey/valkey:8.0")

# Map the client-suffix in test names back to the display name in RESULTS.md.
_CLIENT_SUFFIXES: list[tuple[str, str]] = [
    ("redis_rs_py", "redis-rs-py"),
    ("redis_py_hiredis", "redis-py[hiredis]"),
    ("valkey_glide", "valkey-glide"),
]

# Codspeed's walltime JSON does NOT surface the @pytest.mark.benchmark(group=...)
# value, so we re-derive the scenario from the test-name prefix. Longest prefix
# first so e.g. "test_async_single_" beats a hypothetical "test_async_" match.
_GROUP_PREFIXES: list[tuple[str, str]] = [
    ("test_async_single_", "async-single"),
    ("test_async_100_", "async-100"),
    ("test_pipeline_", "pipeline-1000"),
    ("test_pubsub_", "pubsub-1000"),
    ("test_connect_", "connect"),
    ("test_mget_", "mget"),
    ("test_get_", "get"),
    ("test_set_", "set"),
]


def _spawn_valkey():
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = DockerContainer(VALKEY_IMAGE).with_exposed_ports(6379)
    container.start()
    wait_for_logs(container, "Ready to accept connections", timeout=30)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)
    return container, f"redis://{host}:{port}/0"


def _clear_results_dir() -> None:
    RESULTS_DIR.mkdir(exist_ok=True)
    for stale in RESULTS_DIR.glob("*.json"):
        stale.unlink()


def _run_pytest(smoke: bool, env: dict[str, str], scenarios: list[str] | None) -> None:
    _clear_results_dir()
    env = {**env, "CODSPEED_PROFILE_FOLDER": str(BENCH_DIR)}

    cmd: list[str] = [
        sys.executable,
        "-m",
        "pytest",
        str(BENCH_DIR),
        # The repo's default addopts enable xdist + coverage; both ruin
        # bench timing. Override addopts wholesale to strip them.
        "-o",
        "addopts=",
        "--codspeed",
        "--codspeed-mode=walltime",
        "--no-cov",
        "-p",
        "no:xdist",
        "-q",
    ]
    if smoke:
        cmd += [
            "--codspeed-warmup-time=0",
            "--codspeed-max-rounds=1",
        ]
    # Full mode uses codspeed defaults: 1 s warmup, up to 3 s of timed
    # rounds, auto-sized iter-per-round from warmup mean.
    if scenarios:
        # `-k` filter: matches any test name containing one of the scenario
        # words (e.g. "get or set or pipeline").
        cmd += ["-k", " or ".join(scenarios)]

    print(f"\n>>> pytest benchmarks/  ({'smoke' if smoke else 'full'})")
    # Don't `check=True`: a single failing benchmark shouldn't lose the
    # results from the rest of the run. The renderer below handles partial
    # results gracefully.
    result = subprocess.run(cmd, env=env, check=False)
    if result.returncode != 0:
        print(f"pytest exited with {result.returncode}; rendering whatever ran", file=sys.stderr)


def _latest_results_json() -> Path | None:
    candidates = sorted(RESULTS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _client_from_name(test_name: str) -> str | None:
    for suffix, display in _CLIENT_SUFFIXES:
        if test_name.endswith(suffix):
            return display
    return None


def _group_from_name(test_name: str) -> str | None:
    for prefix, group in _GROUP_PREFIXES:
        if test_name.startswith(prefix):
            return group
    return None


def _load_results(path: Path) -> dict[str, dict[str, dict[str, float]]]:
    """Return ``{group: {client: stats}}`` from a codspeed walltime JSON dump."""
    raw = json.loads(path.read_text())
    out: dict[str, dict[str, dict[str, float]]] = {}
    for bench in raw.get("benchmarks", []):
        name = bench.get("name", "")
        group = _group_from_name(name)
        client = _client_from_name(name)
        if group is None or client is None:
            continue
        stats = bench.get("stats", {})
        median_ns = stats.get("median_ns", 0.0) or 0.0
        median_s = median_ns / 1e9
        out.setdefault(group, {})[client] = {
            "median": median_s,
            "mean": (stats.get("mean_ns", 0.0) or 0.0) / 1e9,
            "stddev": (stats.get("stdev_ns", 0.0) or 0.0) / 1e9,
            "rounds": stats.get("rounds", 0),
            "ops_per_sec": (1e9 / median_ns) if median_ns > 0 else 0.0,
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
        if client == "redis-rs-py":
            cells.append(f"**{ops:,.0f} ops/s** ({m['median'] * 1e6:.1f} us)")
        else:
            speedup = (ops / rs_ops) if rs_ops > 0 else 0.0
            cells.append(f"{ops:,.0f} ops/s ({speedup:.2f}x)")
    return "| " + " | ".join(cells) + " |"


def _render_report(by_scenario: dict[str, dict[str, dict[str, float]]], full_run: bool) -> str:
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
        f"- Run mode: {'full (codspeed walltime defaults)' if full_run else 'smoke (--codspeed-max-rounds=1)'}",
        "",
        "## Results",
        "",
        "Higher ops/sec is better. **Bold** is the redis-rs-py baseline; the parenthesised number for each competitor is its multiple of the baseline (1.50x = 50% faster than us, 0.50x = half our throughput).",
        "",
    ]

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
            "- One Valkey container per `run_all.py` invocation; FLUSHDB between scenarios where state matters.",
            "- All clients use the same database (db=0), `decode_responses=False`, and the same hot-key payload (100 bytes).",
            "- Bench tests share a single Python process. pytest-codspeed (walltime mode) calibrates the inner-iteration count from a warmup pass and reports the median round time; we convert to ops/sec as `1 / median`. Cross-client cache contamination is acknowledged and discussed in the project README — for absolute publishable numbers, pin the order or run scenarios in isolated subprocesses.",
            "- Codspeed runs a 1-second warmup by default (full run); smoke mode sets `--codspeed-warmup-time=0 --codspeed-max-rounds=1`.",
            "- valkey-glide has no sync API; sync scenarios run via `loop.run_until_complete(coro)` per call. The setup overhead this adds is **disclosed** but constant — direct comparison of valkey-glide on sync scenarios is structurally pessimistic for it.",
            "- Valkey image is pinned (`BENCH_VALKEY_IMAGE` env, defaults to `valkey/valkey:8.0`).",
            "- CI runners are noisy across cloud providers — the **source of truth** is a local run on the reference machine documented above. CI runs are smoke-only and exist to prevent regressions in the bench-suite plumbing, not to publish numbers.",
            "",
            "## Reproducing locally",
            "",
            "```bash",
            "# 1. run the full sweep (spawns Valkey, runs pytest, renders this file)",
            "uv run --group bench python benchmarks/run_all.py",
            "",
            "# 2. re-render this file from the cached JSON without re-running",
            "uv run --group bench python benchmarks/run_all.py --render-only",
            "",
            "# 3. run a single scenario by keyword",
            "uv run --group bench python benchmarks/run_all.py --scenario get",
            "```",
            "",
        ],
    )
    return "\n".join(parts) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true", help="fast PR-gate run (single round, no warmup)")
    parser.add_argument("--render-only", action="store_true", help="re-render RESULTS.md from existing JSON")
    parser.add_argument(
        "--scenario",
        action="append",
        default=None,
        help="filter to test names containing this word (repeatable, OR-combined)",
    )
    args = parser.parse_args()

    if not args.render_only:
        container, url = _spawn_valkey()
        env = os.environ.copy()
        env["BENCH_VALKEY_URL"] = url
        try:
            _run_pytest(args.smoke, env, args.scenario)
        finally:
            container.stop()

    results_json = _latest_results_json()
    if results_json is None:
        print(f"no results JSON in {RESULTS_DIR}", file=sys.stderr)
        return 1

    by_scenario = _load_results(results_json)
    RESULTS_MD.write_text(_render_report(by_scenario, full_run=not args.smoke))
    print(f"\nwrote {RESULTS_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
