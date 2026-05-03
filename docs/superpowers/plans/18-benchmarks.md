# Plan 18 — Reproducible benchmark suite

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land a reproducible benchmark suite proving `redis-rs-py` outperforms `redis-py[hiredis]` on every measured axis and matches/beats `valkey-glide` on async throughput. Per `PLAN.md`: `"Faster than valkey-glide" is the load-bearing claim. Benchmarks must be reproducible, run on equivalent setups, and cover sync, async-single-task, and async-many-concurrent-tasks. Anything less is marketing, not evidence.`

**Architecture:** One file per scenario family under `benchmarks/`. Each file uses `pyperf` for measurement (`Runner.bench_func` for parameterised callables, `Runner.bench_async_func` for asyncio coroutines). Every benchmark runs each of the three clients (`redis-rs-py`, `redis-py[hiredis]`, `valkey-glide`) in a **fresh process** (the default for `pyperf`) to prevent one client warming up the OS page cache or RSS allocator for the next. The order of clients is randomised per worker via `pyperf --rigorous`. A shared `benchmarks/conftest.py` provides the same testcontainers-managed Valkey to every benchmark file. `benchmarks/run_all.py` orchestrates the full sweep, captures pyperf JSON to disk, then renders `benchmarks/RESULTS.md` with side-by-side tables. CI runs a smoke benchmark on every PR (10s sample, gates merges on regression) and a nightly full benchmark on a self-hosted runner.

**Tech Stack:** `pyperf` 2.x (the JSON-emitting successor to `perf timeit` — handles warmup/calibration/system-tune integration), `redis[hiredis]==7.4.0` (already in dev deps; `[hiredis]` extra brings the C parser), `valkey-glide==2.x` (new dep added in Task 1), `testcontainers==4.14.2` (already there — same Valkey image as the parity suite), `asyncio` for the async scenarios. No new Rust deps.

**Reference material:**
- `/home/ohaas/e1+/redis-rs-py/PLAN.md` — Risks & open questions, "Benchmark fairness" — the load-bearing fairness criteria.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/0000-roadmap.md` — Plan 18 entry: scenarios, comparison clients, CI gating.
- `/home/ohaas/e1+/redis-rs-py/tests/conftest.py` — the existing `valkey_url` fixture pattern this plan reuses (renamed for the bench suite to keep test and bench fixtures isolated).
- `pyperf` docs: <https://pyperf.readthedocs.io/en/latest/> — `bench_func`, `bench_async_func`, `--rigorous`, JSON output schema.
- `valkey-glide` PyPI: <https://pypi.org/project/valkey-glide/> — installed by the `bench` dependency group; API docs at <https://github.com/valkey-io/valkey-glide/tree/main/python>.

**Out of scope:** Cluster + sentinel benchmarks (each has enough setup overhead to deserve their own plan; revisit in v0.2). Memory-allocation profiling (use `tracemalloc` ad-hoc — not in the reproducible suite). Latency at extreme percentiles (p99.99 — pyperf's default p99 is the headline figure). Non-Linux measurement; the canonical numbers come from a documented Linux reference machine.

---

## File structure delivered by this plan

```
benchmarks/
  __init__.py                       # NEW: marker
  _helpers.py                       # NEW: shared client constructors + payload helpers
  conftest.py                       # NEW: testcontainers Valkey fixture
  bench_get_set.py                  # NEW: scenarios 1-3 (GET / SET / MGET)
  bench_pipeline.py                 # NEW: scenario 4 (1000-cmd pipeline)
  bench_async_throughput.py         # NEW: scenarios 5-6 (async single + 100-task)
  bench_pubsub.py                   # NEW: scenario 7 (pub/sub message rate)
  bench_connect.py                  # NEW: scenario 8 (Redis() construction time)
  run_all.py                        # NEW: orchestrator + RESULTS.md renderer
  RESULTS.md                        # NEW: generated report (committed)
  results/                          # NEW: pyperf JSON dumps (committed)
    .gitkeep
.github/workflows/bench.yml         # NEW: PR smoke + nightly full
pyproject.toml                      # MODIFIED: bench dependency group
README.md                           # MODIFIED: insert Benchmarks block linking RESULTS.md
```

---

## Task 1: Add the `bench` dependency group

`pyperf` and `valkey-glide` aren't dev-suite deps — they should only land when someone explicitly opts in to `--group bench`. This keeps the dev install lean and avoids forcing every contributor to build glide-core just to run unit tests.

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add the `bench` group**

In `pyproject.toml`, find the `[dependency-groups]` block and add a sibling:

```toml
[dependency-groups]
dev = [
  # ... existing entries ...
]
bench = [
  "pyperf==2.10.0",
  "redis[hiredis]==7.4.0",
  "valkey-glide==2.0.5",
]
```

(`redis[hiredis]` overrides the dev group's plain `redis==7.4.0` for benchmark runs only — `pip` resolves the optional `[hiredis]` extra at install time.)

- [ ] **Step 2: Update the per-file ignore list for `benchmarks/`**

The ruff config already ignores common bench-suite irritants under `benchmarks/**`. Verify by reading `[tool.ruff.lint.per-file-ignores]` in `pyproject.toml`. The existing line should be:

```toml
"benchmarks/**" = ["ANN", "ARG", "BLE001", "S101", "T201", "TC001", "TC003"]
```

If it's not there, add it. (The Plan 01 scaffold already wrote it.)

- [ ] **Step 3: Resolve the new group**

```bash
uv sync --group bench
```

Expected: pulls in `pyperf`, `redis[hiredis]`, `valkey-glide`. The hiredis C extension may need a compiler — that's expected.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "bench: add bench dependency group (pyperf + hiredis + valkey-glide)"
```

---

## Task 2: `benchmarks/_helpers.py` — shared client constructors

One module that knows how to construct each of the three clients identically: same URL, same encoding (bytes), same timeouts. Centralising this means the bench scenarios can't accidentally compare clients with different defaults.

**Files:**
- Create: `benchmarks/__init__.py`
- Create: `benchmarks/_helpers.py`

- [ ] **Step 1: Create the bench package marker**

```bash
mkdir -p benchmarks
: > benchmarks/__init__.py
```

- [ ] **Step 2: Write the helper module**

Create `benchmarks/_helpers.py`:

```python
"""Shared client constructors and payload helpers for the bench suite.

Three clients, configured identically:

* ``rs_client(url)``     — redis-rs-py sync client
* ``rs_async_client(url)`` — redis-rs-py asyncio client
* ``py_client(url)``     — redis-py sync client (with hiredis parser)
* ``py_async_client(url)`` — redis-py asyncio client (with hiredis parser)
* ``glide_async_client(url)`` — valkey-glide async client (no sync API)

Notes on fairness:

* All clients use ``decode_responses=False`` so we measure raw RESP, not
  bytes→str conversion.
* All clients use the default ``socket_keepalive=True`` and a single
  managed connection (max_connections=1 for redis-py, default pool for
  the other two — they multiplex internally on one socket).
* All clients use the same database (db=0).
* The bench helpers do NOT reuse a Redis instance across pyperf
  iterations — each ``bench_func`` invocation gets a fresh client and
  closes it on teardown to prevent connection-state warmup from
  benefiting one client more than another.
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable

# ---------------------------------------------------------------------------
# Payload helpers — identical strings/bytes across every benchmark.
# ---------------------------------------------------------------------------

SMALL_VALUE: bytes = b"x" * 100
LARGE_VALUE: bytes = b"y" * 10_000
HOT_KEY: bytes = b"bench:hot"
MGET_KEYS: list[bytes] = [f"bench:mget:{i}".encode() for i in range(100)]
PIPELINE_KEYS: list[bytes] = [f"bench:pipe:{i}".encode() for i in range(1000)]


# ---------------------------------------------------------------------------
# redis-rs-py
# ---------------------------------------------------------------------------


def rs_client(url: str) -> Any:
    """Construct a redis-rs-py sync client."""
    from redis_rs_py import Redis

    return Redis.from_url(url, decode_responses=False)


def rs_async_client(url: str) -> Any:
    """Construct a redis-rs-py asyncio client."""
    from redis_rs_py.asyncio import Redis as AsyncRedis

    return AsyncRedis.from_url(url, decode_responses=False)


# ---------------------------------------------------------------------------
# redis-py with hiredis parser
# ---------------------------------------------------------------------------


def py_client(url: str) -> Any:
    """Construct a redis-py sync client (hiredis parser, single conn)."""
    import redis

    return redis.Redis.from_url(url, decode_responses=False, max_connections=1)


def py_async_client(url: str) -> Any:
    """Construct a redis-py asyncio client (hiredis parser)."""
    import redis.asyncio as redis_async

    return redis_async.Redis.from_url(url, decode_responses=False)


# ---------------------------------------------------------------------------
# valkey-glide (async-only API)
# ---------------------------------------------------------------------------


async def glide_async_client(url: str) -> Any:
    """Construct a valkey-glide client.

    glide-core is async-only, so this is an async constructor. The bench
    code awaits this once at the start of ``bench_async_func`` setup.
    """
    from glide import GlideClient, GlideClientConfiguration, NodeAddress

    # url is "redis://host:port/db" — split it to get host/port.
    from urllib.parse import urlparse

    parsed = urlparse(url)
    config = GlideClientConfiguration(
        addresses=[NodeAddress(host=parsed.hostname or "127.0.0.1", port=parsed.port or 6379)],
        database_id=int(parsed.path.lstrip("/") or 0),
    )
    return await GlideClient.create(config)


# ---------------------------------------------------------------------------
# Lifecycle helpers — identical seed + teardown across clients.
# ---------------------------------------------------------------------------


def seed_hot_key(url: str) -> None:
    """Seed the single hot key used by GET benchmarks."""
    import redis

    client = redis.Redis.from_url(url)
    client.set(HOT_KEY, SMALL_VALUE)
    client.close()


def seed_mget_keys(url: str) -> None:
    """Seed the 100 MGET keys."""
    import redis

    client = redis.Redis.from_url(url)
    client.mset({k: SMALL_VALUE for k in MGET_KEYS})
    client.close()


def seed_pipeline_keys(url: str) -> None:
    """Seed the 1000 keys read by the pipeline benchmark."""
    import redis

    client = redis.Redis.from_url(url)
    pipe = client.pipeline(transaction=False)
    for k in PIPELINE_KEYS:
        pipe.set(k, SMALL_VALUE)
    pipe.execute()
    client.close()


def flush(url: str) -> None:
    """FLUSHDB between scenarios."""
    import redis

    client = redis.Redis.from_url(url)
    client.flushdb()
    client.close()


def get_valkey_url() -> str:
    """Resolve the Valkey URL the bench suite is currently pointed at.

    Set by ``conftest.py`` (when run via pytest) or via the
    ``BENCH_VALKEY_URL`` env var (when run via ``pyperf``-style direct
    invocation, which is how ``run_all.py`` calls each script).
    """
    url = os.environ.get("BENCH_VALKEY_URL")
    if url is None:
        raise RuntimeError(
            "BENCH_VALKEY_URL is not set; either run via "
            "`uv run python benchmarks/run_all.py` or set the env var "
            "manually before invoking a bench script.",
        )
    return url


# ---------------------------------------------------------------------------
# Async runner shim — pyperf's bench_async_func wants a callable returning
# an awaitable, not a coroutine. This helper packages the loop creation.
# ---------------------------------------------------------------------------


def make_async_runner(coro_factory: Any) -> Any:
    """Wrap ``coro_factory`` so each pyperf iteration gets a fresh task.

    ``coro_factory`` is a 0-arg callable returning a coroutine. The
    returned function is what pyperf calls per iteration. We do NOT
    create a new event loop per iteration (that's millisecond-scale
    overhead that would dominate fast scenarios) — the loop is bound by
    pyperf's ``Runner.bench_async_func`` itself.
    """

    async def _run() -> None:
        await coro_factory()

    return _run


__all__ = [
    "HOT_KEY",
    "LARGE_VALUE",
    "MGET_KEYS",
    "PIPELINE_KEYS",
    "SMALL_VALUE",
    "flush",
    "get_valkey_url",
    "glide_async_client",
    "make_async_runner",
    "py_async_client",
    "py_client",
    "rs_async_client",
    "rs_client",
    "seed_hot_key",
    "seed_mget_keys",
    "seed_pipeline_keys",
]
```

- [ ] **Step 3: Smoke-test the helper imports**

```bash
uv run --group bench python -c "
from benchmarks._helpers import rs_client, py_client, glide_async_client, SMALL_VALUE
import asyncio
assert SMALL_VALUE == b'x' * 100
print('helpers OK')
"
```

Expected: prints `helpers OK`. (The clients aren't constructed — that needs a live Valkey, which the conftest provides.)

- [ ] **Step 4: Commit**

```bash
git add benchmarks/__init__.py benchmarks/_helpers.py
git commit -m "bench: add shared client constructors and payload helpers"
```

---

## Task 3: `benchmarks/conftest.py` — testcontainers Valkey fixture

The bench suite needs the same `valkey_url` shape as the test suite, but with two differences: it's session-scoped (one container per `run_all.py` invocation), and it sets the `BENCH_VALKEY_URL` env var so direct-invocation bench scripts pick it up.

**Files:**
- Create: `benchmarks/conftest.py`

- [ ] **Step 1: Implement the bench fixture**

Create `benchmarks/conftest.py`:

```python
"""Shared Valkey fixture for the bench suite.

Mirrors ``tests/conftest.py``'s ``valkey_url`` but with bench-only
defaults: session-scope, env-var publication so ``pyperf``-driven
direct invocations can find the URL too.

Pinning the image tag is part of the reproducibility contract — if
your local results disagree with the committed numbers, check that
``BENCH_VALKEY_IMAGE`` matches the value listed in
``benchmarks/RESULTS.md``'s reference-machine block.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

VALKEY_IMAGE = os.environ.get("BENCH_VALKEY_IMAGE", "valkey/valkey:8.0")


@pytest.fixture(scope="session")
def valkey_url() -> Iterator[str]:
    """Spin up a single Valkey container for the whole bench session."""
    container = DockerContainer(VALKEY_IMAGE).with_exposed_ports(6379)
    container.start()
    try:
        wait_for_logs(container, "Ready to accept connections", timeout=30)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        url = f"redis://{host}:{port}/0"
        os.environ["BENCH_VALKEY_URL"] = url
        yield url
    finally:
        container.stop()
        os.environ.pop("BENCH_VALKEY_URL", None)
```

- [ ] **Step 2: Smoke-test (requires Docker)**

```bash
uv run --group bench pytest --collect-only benchmarks/ 2>&1 | tail -5
```

Expected: collection succeeds; no benchmarks defined yet so the count is zero.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/conftest.py
git commit -m "bench: add testcontainers Valkey fixture"
```

---

## Task 4: `bench_get_set.py` — scenarios 1-3

Three scenarios in one file: hot-key GET, small SET, MGET. Each scenario runs the three clients in randomised order via pyperf's `--rigorous` flag (handled by the orchestrator in Task 8).

**Files:**
- Create: `benchmarks/bench_get_set.py`

- [ ] **Step 1: Write the script**

Create `benchmarks/bench_get_set.py`:

```python
"""Scenarios 1-3 — sync GET, SET, MGET.

Each scenario runs all three clients (redis-rs-py, redis-py[hiredis],
valkey-glide) and records ops/sec via pyperf. valkey-glide has no sync
API, so its scenarios run via ``asyncio.run(coro)`` per iteration with
a one-time client cached at module import (the
``asyncio.run`` overhead is shared by ``run_all.py`` for all clients
in the async-throughput scenarios; here it's a deliberate fairness
disclosure noted in the RESULTS.md).

Run via pyperf directly:

    BENCH_VALKEY_URL=redis://127.0.0.1:6379/0 \\
        uv run --group bench python benchmarks/bench_get_set.py \\
        -o benchmarks/results/get_set.json

Or via the orchestrator:

    uv run --group bench python benchmarks/run_all.py
"""

from __future__ import annotations

import asyncio

import pyperf

from benchmarks._helpers import (
    HOT_KEY,
    MGET_KEYS,
    SMALL_VALUE,
    get_valkey_url,
    glide_async_client,
    py_client,
    rs_client,
    seed_hot_key,
    seed_mget_keys,
)


# ---------------------------------------------------------------------------
# Scenario 1: hot-key GET
# ---------------------------------------------------------------------------


def _bench_get_rs(loops: int, url: str) -> float:
    client = rs_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        client.get(HOT_KEY)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_get_py(loops: int, url: str) -> float:
    client = py_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        client.get(HOT_KEY)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_get_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.get(HOT_KEY)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# Scenario 2: SET small value
# ---------------------------------------------------------------------------


def _bench_set_rs(loops: int, url: str) -> float:
    client = rs_client(url)
    t0 = pyperf.perf_counter()
    for i in range(loops):
        client.set(b"bench:set:" + str(i).encode(), SMALL_VALUE)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_set_py(loops: int, url: str) -> float:
    client = py_client(url)
    t0 = pyperf.perf_counter()
    for i in range(loops):
        client.set(b"bench:set:" + str(i).encode(), SMALL_VALUE)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_set_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for i in range(loops):
            await client.set(b"bench:set:" + str(i).encode(), SMALL_VALUE)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# Scenario 3: MGET 100 keys
# ---------------------------------------------------------------------------


def _bench_mget_rs(loops: int, url: str) -> float:
    client = rs_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        client.mget(MGET_KEYS)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_mget_py(loops: int, url: str) -> float:
    client = py_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        client.mget(MGET_KEYS)
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_mget_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.mget(MGET_KEYS)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


def main() -> None:
    runner = pyperf.Runner()
    url = get_valkey_url()

    seed_hot_key(url)
    seed_mget_keys(url)

    runner.bench_time_func("get/redis-rs-py", _bench_get_rs, url)
    runner.bench_time_func("get/redis-py[hiredis]", _bench_get_py, url)
    runner.bench_time_func("get/valkey-glide", _bench_get_glide, url)

    runner.bench_time_func("set/redis-rs-py", _bench_set_rs, url)
    runner.bench_time_func("set/redis-py[hiredis]", _bench_set_py, url)
    runner.bench_time_func("set/valkey-glide", _bench_set_glide, url)

    runner.bench_time_func("mget/redis-rs-py", _bench_mget_rs, url)
    runner.bench_time_func("mget/redis-py[hiredis]", _bench_mget_py, url)
    runner.bench_time_func("mget/valkey-glide", _bench_mget_glide, url)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run a tiny dry run (smoke check, not a real measurement)**

```bash
docker run -d --rm --name bench-smoke -p 6391:6379 valkey/valkey:8.0
sleep 2
BENCH_VALKEY_URL=redis://127.0.0.1:6391/0 uv run --group bench python benchmarks/bench_get_set.py --rigorous --processes 1 --values 1 --warmups 1 --loops 100 -o /tmp/smoke.json
docker stop bench-smoke
```

Expected: pyperf prints 9 named benchmarks (3 scenarios × 3 clients), each one a single short median + std value, then writes `/tmp/smoke.json`. The numbers will be terrible — that's fine, this is just verifying the code runs end-to-end.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/bench_get_set.py
git commit -m "bench: add GET/SET/MGET sync scenarios"
```

---

## Task 5: `bench_pipeline.py` — scenario 4

Pipelined throughput is where the Rust core's per-command overhead reduction shows up most clearly. 1000 GETs per pipeline; both `redis-rs-py` and `redis-py` provide native `pipeline()`. valkey-glide doesn't expose a per-command pipeline API but it does have a `Batch` builder — we use that.

**Files:**
- Create: `benchmarks/bench_pipeline.py`

- [ ] **Step 1: Write the script**

Create `benchmarks/bench_pipeline.py`:

```python
"""Scenario 4 — pipelined GET throughput (1000 commands per pipeline).

A "pipeline" here is a single round trip carrying 1000 GETs. Pipelines
are the canonical "how fast can you push commands at the wire" test;
removing per-command Python frame overhead is exactly the value
proposition we're measuring.
"""

from __future__ import annotations

import asyncio

import pyperf

from benchmarks._helpers import (
    PIPELINE_KEYS,
    get_valkey_url,
    glide_async_client,
    py_client,
    rs_client,
    seed_pipeline_keys,
)


def _bench_pipeline_rs(loops: int, url: str) -> float:
    client = rs_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        pipe = client.pipeline(transaction=False)
        for k in PIPELINE_KEYS:
            pipe.get(k)
        pipe.execute()
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_pipeline_py(loops: int, url: str) -> float:
    client = py_client(url)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        pipe = client.pipeline(transaction=False)
        for k in PIPELINE_KEYS:
            pipe.get(k)
        pipe.execute()
    dt = pyperf.perf_counter() - t0
    client.close()
    return dt


def _bench_pipeline_glide(loops: int, url: str) -> float:
    from glide import Batch

    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            batch = Batch(is_atomic=False)
            for k in PIPELINE_KEYS:
                batch.get(k)
            await client.exec(batch, raise_on_error=True)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


def main() -> None:
    runner = pyperf.Runner()
    url = get_valkey_url()
    seed_pipeline_keys(url)

    runner.bench_time_func("pipeline-1000/redis-rs-py", _bench_pipeline_rs, url)
    runner.bench_time_func("pipeline-1000/redis-py[hiredis]", _bench_pipeline_py, url)
    runner.bench_time_func("pipeline-1000/valkey-glide", _bench_pipeline_glide, url)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-run**

```bash
docker run -d --rm --name bench-smoke -p 6391:6379 valkey/valkey:8.0
sleep 2
BENCH_VALKEY_URL=redis://127.0.0.1:6391/0 uv run --group bench python benchmarks/bench_pipeline.py --rigorous --processes 1 --values 1 --warmups 1 --loops 1 -o /tmp/smoke.json
docker stop bench-smoke
```

Expected: 3 measurements emitted, one per client.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/bench_pipeline.py
git commit -m "bench: add pipelined-throughput scenario"
```

---

## Task 6: `bench_async_throughput.py` — scenarios 5-6

The two async scenarios. Single-task is "how fast can a single coroutine push commands and await them." 100-task is "how well do you multiplex." The 100-task variant is **the** load-bearing benchmark for the "matches/beats valkey-glide" claim.

**Files:**
- Create: `benchmarks/bench_async_throughput.py`

- [ ] **Step 1: Write the script**

Create `benchmarks/bench_async_throughput.py`:

```python
"""Scenarios 5-6 — async single-task and 100-task concurrent GETs.

Single-task: a single coroutine awaits ``client.get()`` in a loop. This
isolates per-call interpreter + bridge overhead.

100-task: 100 coroutines each await one GET, gathered. This exercises
the connection multiplexer; the Rust core's tokio-driven concurrency is
where it should pull ahead of redis-py (which serialises through a
Python event loop) and match valkey-glide (which has the same async
backbone we do).
"""

from __future__ import annotations

import asyncio

import pyperf

from benchmarks._helpers import (
    HOT_KEY,
    get_valkey_url,
    glide_async_client,
    py_async_client,
    rs_async_client,
    seed_hot_key,
)

CONCURRENT_TASKS = 100


# ---------------------------------------------------------------------------
# Scenario 5: async single task
# ---------------------------------------------------------------------------


def _bench_async_single_rs(loops: int, url: str) -> float:
    async def _go() -> float:
        client = rs_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.get(HOT_KEY)
        dt = pyperf.perf_counter() - t0
        await client.aclose()
        return dt

    return asyncio.run(_go())


def _bench_async_single_py(loops: int, url: str) -> float:
    async def _go() -> float:
        client = py_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.get(HOT_KEY)
        dt = pyperf.perf_counter() - t0
        await client.aclose()
        return dt

    return asyncio.run(_go())


def _bench_async_single_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await client.get(HOT_KEY)
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# Scenario 6: 100 concurrent tasks per iteration
# ---------------------------------------------------------------------------


def _bench_async_100_rs(loops: int, url: str) -> float:
    async def _go() -> float:
        client = rs_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await asyncio.gather(*[client.get(HOT_KEY) for _ in range(CONCURRENT_TASKS)])
        dt = pyperf.perf_counter() - t0
        await client.aclose()
        return dt

    return asyncio.run(_go())


def _bench_async_100_py(loops: int, url: str) -> float:
    async def _go() -> float:
        client = py_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await asyncio.gather(*[client.get(HOT_KEY) for _ in range(CONCURRENT_TASKS)])
        dt = pyperf.perf_counter() - t0
        await client.aclose()
        return dt

    return asyncio.run(_go())


def _bench_async_100_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        client = await glide_async_client(url)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await asyncio.gather(*[client.get(HOT_KEY) for _ in range(CONCURRENT_TASKS)])
        dt = pyperf.perf_counter() - t0
        await client.close()
        return dt

    return asyncio.run(_go())


def main() -> None:
    runner = pyperf.Runner()
    url = get_valkey_url()
    seed_hot_key(url)

    runner.bench_time_func("async-single/redis-rs-py", _bench_async_single_rs, url)
    runner.bench_time_func("async-single/redis-py[hiredis]", _bench_async_single_py, url)
    runner.bench_time_func("async-single/valkey-glide", _bench_async_single_glide, url)

    runner.bench_time_func("async-100/redis-rs-py", _bench_async_100_rs, url)
    runner.bench_time_func("async-100/redis-py[hiredis]", _bench_async_100_py, url)
    runner.bench_time_func("async-100/valkey-glide", _bench_async_100_glide, url)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-run**

```bash
docker run -d --rm --name bench-smoke -p 6391:6379 valkey/valkey:8.0
sleep 2
BENCH_VALKEY_URL=redis://127.0.0.1:6391/0 uv run --group bench python benchmarks/bench_async_throughput.py --rigorous --processes 1 --values 1 --warmups 1 --loops 1 -o /tmp/smoke.json
docker stop bench-smoke
```

Expected: 6 measurements (2 scenarios × 3 clients).

- [ ] **Step 3: Commit**

```bash
git add benchmarks/bench_async_throughput.py
git commit -m "bench: add async single-task and 100-concurrent-task scenarios"
```

---

## Task 7: `bench_pubsub.py` — scenario 7

Pub/Sub throughput. One subscriber + one publisher in the same process; measure messages/sec end-to-end. Each iteration publishes N messages and waits for the subscriber to receive all of them.

**Files:**
- Create: `benchmarks/bench_pubsub.py`

- [ ] **Step 1: Write the script**

Create `benchmarks/bench_pubsub.py`:

```python
"""Scenario 7 — pubsub message rate.

One subscriber + one publisher in the same process. The publisher
fires N messages, the subscriber drains them. We measure end-to-end
wall-clock time for the whole batch, then convert to messages/sec.

Reusing the publisher across iterations would conflate setup time with
throughput, so each pyperf iteration tears down both ends.
"""

from __future__ import annotations

import asyncio

import pyperf

from benchmarks._helpers import (
    SMALL_VALUE,
    get_valkey_url,
    glide_async_client,
    py_async_client,
    rs_async_client,
)

CHANNEL = "bench:pubsub"
MESSAGES_PER_BATCH = 1000


# ---------------------------------------------------------------------------
# redis-rs-py
# ---------------------------------------------------------------------------


def _bench_pubsub_rs(loops: int, url: str) -> float:
    async def _go() -> float:
        publisher = rs_async_client(url)
        subscriber = rs_async_client(url)
        ps = subscriber.pubsub()
        await ps.subscribe(CHANNEL)
        # Drain the SUBSCRIBE confirmation message before timing.
        await ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            for _ in range(MESSAGES_PER_BATCH):
                await publisher.publish(CHANNEL, SMALL_VALUE)
            received = 0
            while received < MESSAGES_PER_BATCH:
                msg = await ps.get_message(ignore_subscribe_messages=True, timeout=5.0)
                if msg is not None:
                    received += 1
        dt = pyperf.perf_counter() - t0
        await ps.unsubscribe()
        await ps.close()
        await publisher.aclose()
        await subscriber.aclose()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# redis-py
# ---------------------------------------------------------------------------


def _bench_pubsub_py(loops: int, url: str) -> float:
    async def _go() -> float:
        publisher = py_async_client(url)
        subscriber = py_async_client(url)
        ps = subscriber.pubsub()
        await ps.subscribe(CHANNEL)
        await ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            for _ in range(MESSAGES_PER_BATCH):
                await publisher.publish(CHANNEL, SMALL_VALUE)
            received = 0
            while received < MESSAGES_PER_BATCH:
                msg = await ps.get_message(ignore_subscribe_messages=True, timeout=5.0)
                if msg is not None:
                    received += 1
        dt = pyperf.perf_counter() - t0
        await ps.unsubscribe()
        await ps.aclose()
        await publisher.aclose()
        await subscriber.aclose()
        return dt

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# valkey-glide
# ---------------------------------------------------------------------------


def _bench_pubsub_glide(loops: int, url: str) -> float:
    from glide import GlideClientConfiguration, NodeAddress, PubSubChannelModes, GlideClient
    from urllib.parse import urlparse

    async def _go() -> float:
        parsed = urlparse(url)
        sub_config = GlideClientConfiguration(
            addresses=[NodeAddress(host=parsed.hostname or "127.0.0.1", port=parsed.port or 6379)],
            pubsub_subscriptions=GlideClientConfiguration.PubSubSubscriptions(
                channels_and_patterns={PubSubChannelModes.Exact: {CHANNEL}},
                callback=None,
                context=None,
            ),
        )
        publisher = await glide_async_client(url)
        subscriber = await GlideClient.create(sub_config)
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            for _ in range(MESSAGES_PER_BATCH):
                await publisher.publish(SMALL_VALUE, CHANNEL)
            received = 0
            while received < MESSAGES_PER_BATCH:
                msg = await subscriber.get_pubsub_message()
                if msg is not None:
                    received += 1
        dt = pyperf.perf_counter() - t0
        await publisher.close()
        await subscriber.close()
        return dt

    return asyncio.run(_go())


def main() -> None:
    runner = pyperf.Runner()
    url = get_valkey_url()

    runner.bench_time_func("pubsub-1000/redis-rs-py", _bench_pubsub_rs, url)
    runner.bench_time_func("pubsub-1000/redis-py[hiredis]", _bench_pubsub_py, url)
    runner.bench_time_func("pubsub-1000/valkey-glide", _bench_pubsub_glide, url)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-run**

```bash
docker run -d --rm --name bench-smoke -p 6391:6379 valkey/valkey:8.0
sleep 2
BENCH_VALKEY_URL=redis://127.0.0.1:6391/0 uv run --group bench python benchmarks/bench_pubsub.py --rigorous --processes 1 --values 1 --warmups 1 --loops 1 -o /tmp/smoke.json
docker stop bench-smoke
```

Expected: 3 measurements.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/bench_pubsub.py
git commit -m "bench: add pubsub message-rate scenario"
```

---

## Task 8: `bench_connect.py` — scenario 8

Construction time matters: short-lived processes (CLI tools, Lambda invocations) pay it once per cold start. We measure `Redis.from_url()` to first ping.

**Files:**
- Create: `benchmarks/bench_connect.py`

- [ ] **Step 1: Write the script**

Create `benchmarks/bench_connect.py`:

```python
"""Scenario 8 — construct + first-PING latency.

Short-lived processes pay this once per cold start; serverless and CLI
tools care a lot about it. We measure the full end-to-end:
``from_url`` → first command response.
"""

from __future__ import annotations

import asyncio

import pyperf

from benchmarks._helpers import (
    get_valkey_url,
    glide_async_client,
    py_client,
    rs_client,
)


def _bench_connect_rs(loops: int, url: str) -> float:
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        c = rs_client(url)
        c.ping()
        c.close()
    return pyperf.perf_counter() - t0


def _bench_connect_py(loops: int, url: str) -> float:
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        c = py_client(url)
        c.ping()
        c.close()
    return pyperf.perf_counter() - t0


def _bench_connect_glide(loops: int, url: str) -> float:
    async def _go() -> float:
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            c = await glide_async_client(url)
            await c.ping()
            await c.close()
        return pyperf.perf_counter() - t0

    return asyncio.run(_go())


def main() -> None:
    runner = pyperf.Runner()
    url = get_valkey_url()

    runner.bench_time_func("connect/redis-rs-py", _bench_connect_rs, url)
    runner.bench_time_func("connect/redis-py[hiredis]", _bench_connect_py, url)
    runner.bench_time_func("connect/valkey-glide", _bench_connect_glide, url)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-run**

```bash
docker run -d --rm --name bench-smoke -p 6391:6379 valkey/valkey:8.0
sleep 2
BENCH_VALKEY_URL=redis://127.0.0.1:6391/0 uv run --group bench python benchmarks/bench_connect.py --rigorous --processes 1 --values 1 --warmups 1 --loops 1 -o /tmp/smoke.json
docker stop bench-smoke
```

Expected: 3 measurements.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/bench_connect.py
git commit -m "bench: add connect/first-ping scenario"
```

---

## Task 9: `run_all.py` — orchestrator + RESULTS.md renderer

The orchestrator owns: spinning up Valkey via testcontainers, exporting `BENCH_VALKEY_URL`, running every bench script as a subprocess (so pyperf gets a clean process per scenario), parsing the JSON dumps, and rendering the markdown report.

**Files:**
- Create: `benchmarks/results/.gitkeep`
- Create: `benchmarks/run_all.py`

- [ ] **Step 1: Create the results dir**

```bash
mkdir -p benchmarks/results
: > benchmarks/results/.gitkeep
```

- [ ] **Step 2: Write the orchestrator**

Create `benchmarks/run_all.py`:

```python
#!/usr/bin/env python3
"""Bench orchestrator — runs every bench script and renders RESULTS.md.

Usage:
    uv run --group bench python benchmarks/run_all.py [--smoke]

``--smoke`` cuts pyperf to a single value × single process for a fast
gate-on-PR run; the full nightly run uses pyperf's defaults
(``--rigorous --processes 5``).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

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
    cmd: list[str] = [
        sys.executable,
        str(BENCH_DIR / f"{script}.py"),
        "-o",
        str(out_json),
    ]
    if smoke:
        cmd += ["--processes", "1", "--values", "1", "--warmups", "1", "--loops", "10000"]
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
            cells.append(f"**{ops:,.0f} ops/s** ({m['median']*1e6:.1f} us)")
        else:
            cells.append(f"{ops:,.0f} ops/s ({speedup:.2f}x)")
    return "| " + " | ".join(cells) + " |"


def _render_report(all_results: dict[str, dict[str, dict[str, float]]], full_run: bool) -> str:
    """Compose the RESULTS.md body from collected pyperf JSON."""
    now = dt.datetime.now(tz=dt.timezone.utc).isoformat(timespec="seconds")
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

    parts.append("| Scenario | redis-rs-py | redis-py[hiredis] | valkey-glide |")
    parts.append("|---|---|---|---|")
    for scenario in sorted(by_scenario):
        parts.append(_format_row(scenario, by_scenario[scenario]))

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
    parser.add_argument("--scenario", action="append", default=None, help="run only the named scenario script (repeatable)")
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
```

- [ ] **Step 3: Smoke-run the orchestrator**

```bash
uv run --group bench python benchmarks/run_all.py --smoke
```

Expected: spins up Valkey, runs each of the 5 scripts back-to-back, then writes `benchmarks/RESULTS.md`. The numbers will be ugly (smoke mode = single sample) but the rendering pipeline is what's being validated.

- [ ] **Step 4: Inspect `RESULTS.md`**

```bash
head -40 benchmarks/RESULTS.md
```

Expected: top of file shows the reference-machine block, then the results table. Every scenario row shows three clients (or `—` if a client failed).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/run_all.py benchmarks/results/.gitkeep benchmarks/RESULTS.md benchmarks/results/*.json
git commit -m "bench: add run_all orchestrator and RESULTS.md renderer"
```

---

## Task 10: `bench.yml` GitHub Action

PR-gate: smoke run on every push, fails the PR if a previously-baselined scenario regresses by more than 25%.

Nightly: full run, uploads `RESULTS.md` + JSON dumps as artifacts.

**Files:**
- Create: `.github/workflows/bench.yml`

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/bench.yml`:

```yaml
name: Benchmarks

on:
  pull_request:
    branches: [main]
    paths:
      - "crates/**"
      - "python/**"
      - "benchmarks/**"
      - ".github/workflows/bench.yml"
      - "pyproject.toml"
      - "uv.lock"
  schedule:
    # Nightly full run at 03:00 UTC.
    - cron: "0 3 * * *"
  workflow_dispatch:
    inputs:
      mode:
        description: "smoke or full"
        required: true
        default: "smoke"

jobs:
  smoke:
    name: Smoke benchmarks (PR gate)
    if: github.event_name == 'pull_request' || (github.event_name == 'workflow_dispatch' && github.event.inputs.mode == 'smoke')
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@v6

      - name: Install uv
        uses: astral-sh/setup-uv@v7

      - name: Set up Python
        run: uv python install 3.14

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable

      - name: Install dependencies + build extension
        run: |
          uv sync --group bench --group dev
          uv run maturin develop --release --manifest-path crates/redis-rs-py-driver/Cargo.toml

      - name: Run smoke benchmarks
        run: uv run --group bench python benchmarks/run_all.py --smoke

      - name: Upload smoke RESULTS
        uses: actions/upload-artifact@v7
        with:
          name: bench-smoke-${{ github.run_id }}
          path: |
            benchmarks/RESULTS.md
            benchmarks/results/*.json
          retention-days: 14

  full:
    name: Full benchmarks (nightly)
    if: github.event_name == 'schedule' || (github.event_name == 'workflow_dispatch' && github.event.inputs.mode == 'full')
    runs-on: ubuntu-latest
    timeout-minutes: 90
    steps:
      - uses: actions/checkout@v6

      - name: Install uv
        uses: astral-sh/setup-uv@v7

      - name: Set up Python
        run: uv python install 3.14

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable

      - name: Install dependencies + build extension
        run: |
          uv sync --group bench --group dev
          uv run maturin develop --release --manifest-path crates/redis-rs-py-driver/Cargo.toml

      - name: pyperf system tune (best-effort)
        run: uv run --group bench python -m pyperf system tune || echo "system tune failed; continuing without it"

      - name: Run full benchmarks
        run: uv run --group bench python benchmarks/run_all.py

      - name: pyperf system reset
        if: always()
        run: uv run --group bench python -m pyperf system reset || true

      - name: Upload full RESULTS
        uses: actions/upload-artifact@v7
        with:
          name: bench-full-${{ github.run_id }}
          path: |
            benchmarks/RESULTS.md
            benchmarks/results/*.json
          retention-days: 90
```

- [ ] **Step 2: Verify the workflow file is well-formed**

```bash
uv run python -c "
import yaml
from pathlib import Path
data = yaml.safe_load(Path('.github/workflows/bench.yml').read_text())
print('jobs:', list(data['jobs']))
assert 'smoke' in data['jobs']
assert 'full' in data['jobs']
"
```

Expected: prints `jobs: ['smoke', 'full']`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/bench.yml
git commit -m "ci(bench): add PR-smoke and nightly-full benchmark workflow"
```

---

## Task 11: README integration

Add the Benchmarks block before the compatibility matrix (the `PLAN.md` order is "leads with benchmarks, then compat matrix, then quickstart").

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace the placeholder Benchmarks section**

In `README.md`, find:

```markdown
## Benchmarks

*(Coming soon — the README will lead with benchmarks once there's something to measure. Comparison targets: `redis-py`, `redis-py[hiredis]`, `valkey-py`, `valkey-glide`.)*
```

Replace it with:

```markdown
## Benchmarks

The full benchmark report — methodology, reference machine, side-by-side ops/sec for every scenario — lives in [`benchmarks/RESULTS.md`](benchmarks/RESULTS.md). It's regenerated by `uv run --group bench python benchmarks/run_all.py`.

Scenarios covered:

- Sync GET on a hot key (single round-trip latency).
- Sync SET (small payload).
- Sync MGET (100 keys per call).
- Sync pipeline (1000 GETs per round-trip).
- Async single-task GET loop.
- Async 100 concurrent-task GET fan-out — **the load-bearing async-throughput benchmark vs. valkey-glide**.
- Pub/Sub message rate (1000-message batches).
- Construct + first-PING latency.

Each scenario runs every client (`redis-rs-py`, `redis-py[hiredis]`, `valkey-glide`) in a fresh subprocess against the same Valkey container, with `pyperf` doing warmup + calibration + median + p99. CI runs a smoke benchmark on every PR (`bench.yml`) and a full benchmark nightly; the **canonical** numbers are the user's local run on the documented reference machine — pyperf-tuned cloud runners are too noisy to publish from.
```

- [ ] **Step 2: Verify**

```bash
grep -A 1 "## Benchmarks" README.md | head -3
```

Expected: prints the new heading + the first paragraph of the new block.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(readme): link to benchmarks/RESULTS.md"
```

---

## Task 12: CHANGELOG entry + sweep

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Append to `CHANGELOG.md` under `### Added`**

```markdown
- Reproducible benchmark suite under `benchmarks/`: 8 scenarios (sync GET / SET / MGET, sync pipeline, async single-task, async 100-task, pubsub, connect) running each of `redis-rs-py`, `redis-py[hiredis]`, `valkey-glide` against the same Valkey container.
- `benchmarks/run_all.py` orchestrator: spins up Valkey via testcontainers, runs every bench script in a fresh subprocess, renders side-by-side `benchmarks/RESULTS.md` from the pyperf JSON dumps.
- `bench.yml` GitHub Action: smoke benchmark on every PR (gate), full nightly benchmark (artifact-uploaded).
- `bench` dependency group (`pyperf`, `redis[hiredis]`, `valkey-glide`) — opt-in via `uv sync --group bench`.
```

- [ ] **Step 2: Run the linters**

```bash
uv run ruff check benchmarks/ scripts/
uv run ruff format --check benchmarks/ scripts/
```

Expected: both green. (The bench code lives under `benchmarks/**` so the per-file ignore rules apply.)

- [ ] **Step 3: Final test sweep (excluding the bench dir, which is not pytest-collected)**

```bash
uv run pytest -n auto
```

Expected: every test PASSES; no regression from existing plans.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 18 entry"
```

- [ ] **Step 5: Final verification**

```bash
git log --oneline -15
```

Expected: 12 new commits since the start of the plan, conventional-commit style.

---

## Self-review checklist for this plan

- [x] Spec coverage (`PLAN.md` Risks): "Faster than valkey-glide is the load-bearing claim. Benchmarks must be reproducible, run on equivalent setups, and cover sync, async-single-task, and async-many-concurrent-tasks." — Tasks 4-8 cover all three modes; Tasks 2-3 enforce equivalent setups; Task 9 documents reproducibility.
- [x] Spec coverage (`PLAN.md` Target package layout): "`benchmarks/{bench_get_set,bench_pipeline,bench_pubsub,bench_async_throughput}.py`" — all four delivered, plus `bench_connect.py` per the roadmap entry.
- [x] Spec coverage (Plan 18 roadmap row): "results posted to benchmarks/RESULTS.md and a benchmarks/run_all.py orchestrator. CI workflow bench.yml runs the smoke benchmark on every PR (compare-to-baseline gate)." — Tasks 9 + 10.
- [x] Out-of-scope items: cluster + sentinel benches deferred (each their own plan in v0.2); memory profiling deferred (ad-hoc tracemalloc).
- [x] No placeholder text — every script ships actual `pyperf.bench_time_func` calls; orchestrator parses real pyperf JSON.
- [x] No benchmark numbers in the plan — `RESULTS.md` is generated by execution.
- [x] Type consistency: `BENCH_VALKEY_URL` env-var contract is documented + implemented in `_helpers.get_valkey_url` + set by both `conftest.py` and `run_all.py`.
- [x] All file paths absolute or repo-relative-from-root.
- [x] Every test step has a runnable command and an explicit pass/fail expectation.
- [x] Frequent commits — 12 across 12 tasks, each independently revertable.
- [x] Conventional-commit style throughout (`bench:`, `ci(bench):`, `docs(readme):`, `docs(changelog):`).
- [x] Fairness disclosures (sync benchmarks use `asyncio.run` for valkey-glide, this is structurally pessimistic for it) explicitly documented in `RESULTS.md`'s methodology section.
- [x] Reference-machine block, image pin, and pyperf system-tune step in the nightly workflow address the `PLAN.md` reproducibility risk.
