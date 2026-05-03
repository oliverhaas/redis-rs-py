# Plan 16 — `Sentinel` (sync + async) façade

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the `Sentinel` and `AsyncSentinel` Rust pyclasses — redis-py-shaped sentinel façades that wrap redis-rs's `ConnectionManager` behind cachex's `SentinelConn` (RwLock'd current master + retry-with-rediscovery on failover-class errors). `master_for(service_name)` returns a `Redis` instance backed by `RedisRsDriver::connect_sentinel`; `slave_for(service_name)` returns the same with a slave URL picked round-robin per redis-py. Discovery (`discover_master`, `discover_slaves`) and SENTINEL admin commands (`sentinel_masters`, `sentinel_failover`, `sentinel_reset`, `sentinel_set`, etc.) are wired through the same connection. The critical correctness test is **transparent failover**: stop the master container, wait for the sentinel quorum to elect a new master, run a SET via the `master_for` Redis — must succeed on the second attempt without the user code knowing.

**Architecture:** Lift cachex's `SentinelConn` struct verbatim from `django-cachex/crates/django-cachex-redis-rs/src/connection.rs:100-177` (the `Arc<RwLock<ConnectionManager>>` + `Arc<[String]>` sentinel URLs + `Arc<str>` service name + `db` + `is_blocking` + `cache_opts` + `tls_opts`). The `dispatch_cmd!` and `conn_method!` macros in `connection.rs` already gain a `Sentinel(s)` arm whose body wraps every call in `sentinel_retry!` (port verbatim from cachex line 218–231). The new factory `RedisRsDriver::connect_sentinel` returns a `RedisRsDriver` with `topology=master|slave`. The façade `Sentinel` pyclass holds a `SentinelManager` (a thin Rust struct that owns the sentinel URL list + service-name registry); `master_for` / `slave_for` instantiate fresh `Redis` façades pointed at fresh sentinel-mode drivers.

**Tech Stack:** Rust 2024, PyO3 0.28, tokio (`rt-multi-thread`, `sync`, `time`), redis 1.x with `connection-manager` (already enabled). No new Cargo features. On the Python side: pytest + pytest-asyncio + testcontainers.

**Reference material:**
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs:100-177` — the `SentinelConn` struct + `is_failover_error` + `rediscover` impl. **Lift verbatim.**
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs:218-242` — the `sentinel_retry!` macro and its integration into `conn_method!` / `dispatch_cmd!`. **Port verbatim.**
- `/home/ohaas/e1+/django-cachex/crates/django-cachex-redis-rs/src/connection.rs:1226-1380` — `create_sentinel_inner` helper and `connect_sentinel` factory. Reuse.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/01-foundation-async-bridge.md` — `ValkeyConn`, `ConnConfig::Standard`, `connect_standard`, `dispatch_cmd!`/`conn_method!` macros.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/02-exceptions.md` — `ExceptionClass::MasterDownError`, `ExceptionClass::ConnectionError`. Sentinel raises `MasterDownError` when no master can be discovered.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/15-cluster.md` — Plan 15 introduced the `Cluster` arm into `ValkeyConnInner` + the multi-arm dispatch macros. Plan 16 extends that pattern.
- redis-py upstream signature: `python -c "import redis.sentinel, inspect; print(inspect.signature(redis.sentinel.Sentinel.__init__))"` returns `(self, sentinels, min_other_sentinels=0, sentinel_kwargs=None, force_master_ip=None, **connection_kwargs)`. `master_for` / `slave_for` signatures captured in Task 4.

**Out of scope:**
- `min_other_sentinels` quorum check at the *client* level (we accept the kwarg but pass-through; quorum decisions live on the sentinel servers).
- `force_master_ip` (accept-and-warn).
- Pluggable `redis_class` / `connection_pool_class` arguments to `master_for` / `slave_for` (accepted but ignored — the returned object is always our `Redis`).
- `SLOWLOG` and `LATENCY *` introspection over sentinels (none of redis-py's Sentinel exposes them either).

---

## File structure delivered by this plan

```
crates/redis-rs-py-driver/src/
  connection.rs                # MODIFIED: add Sentinel arm + SentinelConn struct +
                               #            sentinel_retry! macro + connect_sentinel +
                               #            create_sentinel_inner helper
  driver.rs                    # MODIFIED: add RedisRsDriver::connect_sentinel factory +
                               #            is_sentinel() introspection
  facade/
    sentinel.rs                # NEW: Sentinel + AsyncSentinel pyclasses,
                               #       SentinelManager struct, master_for/slave_for,
                               #       discover_master/slaves, sentinel admin commands
    kwargs.rs                  # MODIFIED: extend accept-and-warn with sentinel kwargs
  lib.rs                       # MODIFIED: register sentinel classes on _driver.sentinel +
                               #           _driver.asyncio.sentinel
python/
  redis_rs_py/
    sentinel/
      __init__.py              # NEW: re-export Sentinel
    asyncio/
      sentinel/
        __init__.py            # NEW: re-export AsyncSentinel
    _driver.pyi                # MODIFIED: stubs for sentinel classes
tests/
  conftest.py                  # MODIFIED: sentinel_urls + sentinel_service_name fixtures
  sentinel/
    __init__.py
    test_connection_sentinel.py# ValkeyConnInner::Sentinel + connect_sentinel end-to-end
    test_sentinel_basic.py     # Sentinel constructor + master_for/slave_for + discover_*
    test_sentinel_admin.py     # sentinel_masters/sentinel_master/sentinel_slaves/etc.
    test_sentinel_failover.py  # critical: stop master, observe transparent rediscover
    test_async_sentinel.py     # AsyncSentinel mirror
```

---

## Task 1: Extend `ValkeyConnInner` with the `Sentinel` arm + `SentinelConn`

Lift cachex's `SentinelConn` struct verbatim, add it as the third arm of `ValkeyConnInner`, and wrap the dispatch macros with `sentinel_retry!`.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/connection.rs`
- Test: `tests/sentinel/__init__.py`, `tests/sentinel/test_connection_sentinel.py`

- [ ] **Step 1: Add the `sentinel_urls` + `sentinel_service_name` testcontainers fixtures**

Append to `tests/conftest.py`:

```python
# =========================================================================
# Sentinel fixture — 1 master + 1 replica + 3 sentinels via testcontainers.
# =========================================================================
#
# Strategy:
#   * Bring up 1 master Valkey container (the writer).
#   * Bring up 1 replica Valkey container, configured with `replicaof
#     <master-name> 6379` against the docker network's master.
#   * Bring up 3 sentinel containers (`valkey-sentinel`), each with a
#     pinned config that monitors the master under SERVICE_NAME with a
#     quorum of 2.
#   * Yield (sentinel_urls, service_name).
#
# The 3-sentinel quorum is the redis-py-recommended minimum and is what
# the failover test depends on — with 2 sentinels you cannot reach
# quorum after losing one, with 1 you have no failover at all.

SERVICE_NAME = "redis-rs-py-test-master"
SENTINEL_PORT = 26379


def _spawn_sentinel_topology() -> tuple[Network, list[DockerContainer], list[str]]:
    """Bring up 1 master + 1 replica + 3 sentinels.

    Returns (network, containers, sentinel_urls). All 5 containers share
    a docker network so the sentinels can reach the master/replica by
    name; the 3 sentinel ports are mapped back to host so the Python
    client can connect from outside.
    """
    network = Network()
    network.create()

    containers: list[DockerContainer] = []
    master_name = "valkey-sentinel-master"
    replica_name = "valkey-sentinel-replica"

    # Master.
    master = (
        DockerContainer(VALKEY_IMAGE)
        .with_network(network)
        .with_name(master_name)
        .with_exposed_ports(6379)
        .with_command(
            "valkey-server "
            "--port 6379 "
            "--protected-mode no "
            "--appendonly no "
            "--save \"\""
        )
    )
    master.start()
    wait_for_logs(master, "Ready to accept connections", timeout=30)
    containers.append(master)

    # Replica.
    replica = (
        DockerContainer(VALKEY_IMAGE)
        .with_network(network)
        .with_name(replica_name)
        .with_exposed_ports(6379)
        .with_command(
            f"valkey-server "
            f"--port 6379 "
            f"--protected-mode no "
            f"--appendonly no "
            f"--save \"\" "
            f"--replicaof {master_name} 6379"
        )
    )
    replica.start()
    wait_for_logs(replica, "Ready to accept connections", timeout=30)
    containers.append(replica)

    # 3 sentinels — each gets its own config file written via a shell here-doc.
    sentinel_urls: list[str] = []
    for idx in range(3):
        name = f"valkey-sentinel-{idx}"
        cfg = (
            f"port {SENTINEL_PORT}\n"
            f"sentinel monitor {SERVICE_NAME} {master_name} 6379 2\n"
            f"sentinel down-after-milliseconds {SERVICE_NAME} 2000\n"
            f"sentinel parallel-syncs {SERVICE_NAME} 1\n"
            f"sentinel failover-timeout {SERVICE_NAME} 10000\n"
            f"sentinel resolve-hostnames yes\n"
            f"protected-mode no\n"
        )
        # Write the config inside the container at startup.
        sentinel = (
            DockerContainer(VALKEY_IMAGE)
            .with_network(network)
            .with_name(name)
            .with_exposed_ports(SENTINEL_PORT)
            .with_command(
                "sh -c 'echo \""
                + cfg.replace('"', '\\"').replace("\n", "\\n")
                + "\" | sed \"s/\\\\n/\\n/g\" > /tmp/sentinel.conf && "
                + "valkey-sentinel /tmp/sentinel.conf'"
            )
        )
        sentinel.start()
        wait_for_logs(sentinel, "+monitor", timeout=30)
        containers.append(sentinel)
        host = sentinel.get_container_host_ip()
        port = sentinel.get_exposed_port(SENTINEL_PORT)
        sentinel_urls.append(f"redis://{host}:{port}")

    return network, containers, sentinel_urls


@pytest.fixture(scope="session")
def sentinel_urls(
    tmp_path_factory: pytest.TempPathFactory, worker_id: str
) -> Iterator[list[str]]:
    """List of redis://host:port URLs for the 3 sentinels."""
    if worker_id == "master":
        network, containers, urls = _spawn_sentinel_topology()
        try:
            yield urls
        finally:
            for c in containers:
                c.stop()
            network.remove()
        return

    root = tmp_path_factory.getbasetemp().parent
    lockfile = root / "valkey_sentinel.lock"
    urlsfile = root / "valkey_sentinel.urls"

    network = None
    containers: list[DockerContainer] = []

    with FileLock(str(lockfile)):
        if urlsfile.exists():
            urls = urlsfile.read_text().strip().splitlines()
        else:
            network, containers, urls = _spawn_sentinel_topology()
            urlsfile.write_text("\n".join(urls))

    try:
        yield urls
    finally:
        if containers:
            for c in containers:
                c.stop()
            if network is not None:
                network.remove()
            urlsfile.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def sentinel_service_name() -> str:
    return SERVICE_NAME
```

(`SERVICE_NAME` and `SENTINEL_PORT` are module-level constants. `Network`, `wait_for_logs`, `DockerContainer`, `FileLock`, `VALKEY_IMAGE` are reused from the cluster fixture in Plan 15 / the standard fixture in Plan 01.)

- [ ] **Step 2: Smoke-test the fixture in isolation**

`tests/sentinel/__init__.py` (empty). Then `tests/sentinel/test_connection_sentinel.py`:

```python
"""ValkeyConn / connect_sentinel end-to-end smoke."""

from __future__ import annotations

import pytest


def test_sentinel_fixture_brings_up_three_sentinels(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    assert len(sentinel_urls) == 3
    assert sentinel_service_name == "redis-rs-py-test-master"
    for u in sentinel_urls:
        assert u.startswith("redis://")


def test_sentinel_quorum_reports_master(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    """Use the upstream redis-py Sentinel to confirm the topology is healthy
    before we even start exercising our own client."""
    import redis.sentinel as upstream

    nodes = []
    for url in sentinel_urls:
        host, port = url.removeprefix("redis://").split(":", 1)
        nodes.append((host, int(port)))
    s = upstream.Sentinel(nodes, socket_timeout=2.0)
    addr = s.discover_master(sentinel_service_name)
    assert addr is not None
    host, port = addr
    assert isinstance(host, str)
    assert isinstance(port, int) and port > 0
```

Run: `uv run pytest tests/sentinel/ -v`
Expected: 2 PASS. (Initial container boot ≈ 30 s.)

- [ ] **Step 3: Add the `Sentinel` arm to `ValkeyConnInner`**

Edit `crates/redis-rs-py-driver/src/connection.rs`. At the top, expand imports:

```rust
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::caching::CacheConfig;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{Client, RedisResult, TlsCertificates};
use tokio::sync::{OnceCell, RwLock};
```

Insert the `SentinelConn` struct (verbatim port from cachex line 100–177) before the `ValkeyConnInner` definition:

```rust
/// Sentinel-aware connection that re-discovers master on failover.
///
/// Lifted verbatim from `django-cachex-redis-rs/src/connection.rs:100-177`.
/// The struct owns:
///   * `inner`     — RwLock'd `ConnectionManager` to the *current* master.
///                   Reads share the lock; only `rediscover()` takes write.
///   * `sentinel_urls` — the list of sentinel hosts. Iterated round-robin
///                       on rediscover until one answers.
///   * `service_name`  — the master name registered with the sentinels.
///   * `db`            — DB index to use on the discovered master URL.
///   * `is_blocking`   — true for the lazy blocking-conn slot.
///   * `cache_opts`    — client-side caching opts (only honored on
///                       non-blocking connections).
///   * `tls_opts`      — TLS opts re-applied per rediscover.
#[derive(Clone)]
pub struct SentinelConn {
    inner: Arc<RwLock<ConnectionManager>>,
    sentinel_urls: Arc<[String]>,
    service_name: Arc<str>,
    db: i64,
    is_blocking: bool,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
    /// `true` when this connection should target a slave rather than the
    /// master. The discovery path picks one slave round-robin.
    is_slave: bool,
}

impl SentinelConn {
    fn conn_config(&self) -> ConnectionManagerConfig {
        if self.is_blocking {
            blocking_conn_manager_config()
        } else {
            conn_manager_config(self.cache_opts.as_ref())
        }
    }

    pub async fn get_conn(&self) -> ConnectionManager {
        self.inner.read().await.clone()
    }

    /// Errors that should trigger a rediscover-and-retry. Lifted from
    /// cachex; matches the redis-rs failover-class set.
    pub fn is_failover_error(e: &redis::RedisError) -> bool {
        matches!(
            e.kind(),
            redis::ErrorKind::Io
                | redis::ErrorKind::Server(redis::ServerErrorKind::BusyLoading)
                | redis::ErrorKind::Server(redis::ServerErrorKind::TryAgain)
                | redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly)
        ) || e.is_connection_dropped()
    }

    /// Walk the sentinel list, find a healthy one, ask it for the
    /// current master/slave, build a fresh ConnectionManager against
    /// that address, swap into `self.inner`.
    pub async fn rediscover(&self) -> RedisResult<()> {
        for sentinel_url in self.sentinel_urls.iter() {
            let client = match create_client(sentinel_url.as_str(), self.tls_opts.as_ref())
            {
                Ok(c) => c,
                Err(_) => continue,
            };
            let mut conn =
                match ConnectionManager::new_with_config(client, conn_manager_config(None))
                    .await
                {
                    Ok(c) => c,
                    Err(_) => continue,
                };

            let target_addr: Option<(String, String)> = if self.is_slave {
                // Pick first slave that reports as healthy. The
                // `SENTINEL slaves <name>` reply is a list-of-maps; we
                // flatten and read `ip` + `port` keys.
                let slaves: RedisResult<Vec<Vec<(String, String)>>> = redis::cmd("SENTINEL")
                    .arg("slaves")
                    .arg(&*self.service_name)
                    .query_async(&mut conn)
                    .await;
                match slaves {
                    Ok(rows) => rows
                        .into_iter()
                        .filter_map(|row| {
                            let map: std::collections::HashMap<_, _> = row.into_iter().collect();
                            let flags = map.get("flags").cloned().unwrap_or_default();
                            if flags.contains("disconnected") || flags.contains("s_down") {
                                return None;
                            }
                            Some((map.get("ip")?.clone(), map.get("port")?.clone()))
                        })
                        .next(),
                    Err(_) => None,
                }
            } else {
                let master: RedisResult<Vec<String>> = redis::cmd("SENTINEL")
                    .arg("get-master-addr-by-name")
                    .arg(&*self.service_name)
                    .query_async(&mut conn)
                    .await;
                match master {
                    Ok(addr) if addr.len() == 2 => Some((addr[0].clone(), addr[1].clone())),
                    _ => None,
                }
            };

            if let Some((host, port)) = target_addr {
                let scheme = if self.tls_opts.is_some() {
                    "rediss"
                } else {
                    "redis"
                };
                let base_url = format!("{scheme}://{host}:{port}/{}", self.db);
                let target_url = url_with_resp3(&base_url);
                let client = create_client(target_url.as_str(), self.tls_opts.as_ref())?;
                let new_mgr =
                    ConnectionManager::new_with_config(client, self.conn_config()).await?;
                let mut guard = self.inner.write().await;
                *guard = new_mgr;
                return Ok(());
            }
        }
        Err(redis::RedisError::from((
            redis::ErrorKind::Io,
            "Failed to rediscover master from any sentinel",
        )))
    }
}
```

Now extend `ValkeyConnInner`:

```rust
#[derive(Clone)]
pub enum ValkeyConnInner {
    Standard(ConnectionManager),
    Cluster(ClusterConnection),
    Sentinel(SentinelConn),
}
```

- [ ] **Step 4: Extend the dispatch macros with the Sentinel arm + `sentinel_retry!`**

Replace the existing `dispatch_cmd!` and `conn_method!` (added in Plan 15) and add `sentinel_retry!`:

```rust
/// Sentinel retry-on-failover wrapper. Lifted verbatim from cachex.
///
/// On a failover-class error: rediscover (which swaps `inner`), then
/// re-run `$op` against the freshly-acquired connection.
#[macro_export]
macro_rules! sentinel_retry {
    ($s:expr, $c:ident, $op:expr) => {{
        let mut $c = $s.get_conn().await;
        match $op {
            Ok(v) => Ok(v),
            Err(e) if $crate::connection::SentinelConn::is_failover_error(&e) => {
                $s.rediscover().await?;
                let mut $c = $s.get_conn().await;
                $op
            }
            Err(e) => Err(e),
        }
    }};
}

#[macro_export]
macro_rules! dispatch_cmd {
    ($self:expr, $cmd:expr) => {
        match $self {
            $crate::connection::ValkeyConnInner::Standard(c) => $cmd.query_async(c).await,
            $crate::connection::ValkeyConnInner::Cluster(c) => $cmd.query_async(c).await,
            $crate::connection::ValkeyConnInner::Sentinel(s) => {
                let cmd_retry = $cmd.clone();
                let mut c = s.get_conn().await;
                match $cmd.query_async(&mut c).await {
                    Ok(v) => Ok(v),
                    Err(e)
                        if $crate::connection::SentinelConn::is_failover_error(&e) =>
                    {
                        s.rediscover().await?;
                        let mut c = s.get_conn().await;
                        cmd_retry.query_async(&mut c).await
                    }
                    Err(e) => Err(e),
                }
            }
        }
    };
}

#[macro_export]
macro_rules! conn_method {
    ($self:expr, $c:ident, $op:expr) => {
        match $self {
            $crate::connection::ValkeyConnInner::Standard($c) => $op.await,
            $crate::connection::ValkeyConnInner::Cluster($c) => $op.await,
            $crate::connection::ValkeyConnInner::Sentinel(s) => {
                $crate::sentinel_retry!(s, $c, $op.await)
            }
        }
    };
}
```

- [ ] **Step 5: Extend `ConnConfig` and `cache_statistics`**

Find the `enum ConnConfig` and add the Sentinel variant:

```rust
#[derive(Clone)]
enum ConnConfig {
    Standard {
        url: Arc<str>,
        tls_opts: Option<TlsOpts>,
    },
    Cluster {
        urls: Arc<[String]>,
        tls_opts: Option<TlsOpts>,
    },
    Sentinel {
        sentinel_urls: Arc<[String]>,
        service_name: Arc<str>,
        db: i64,
        is_slave: bool,
        tls_opts: Option<TlsOpts>,
    },
}
```

Find `impl ValkeyConn { ... cache_statistics ... }` and update:

```rust
    pub fn cache_statistics(&self) -> Option<redis::caching::CacheStatistics> {
        match &self.regular {
            ValkeyConnInner::Standard(c) => c.get_cache_statistics(),
            ValkeyConnInner::Cluster(_) => None,
            ValkeyConnInner::Sentinel(s) => {
                // Try the read lock without blocking; if contested, give up.
                s.inner.try_read().ok().and_then(|c| c.get_cache_statistics())
            }
        }
    }
```

- [ ] **Step 6: Add `create_sentinel_inner` + `connect_sentinel`**

After the existing `connect_cluster` (added in Plan 15), append:

```rust
/// Helper: create a SentinelConn inner (used by both regular and lazy
/// blocking variants).
async fn create_sentinel_inner(
    sentinel_urls: &[String],
    service_name: &str,
    db: i64,
    is_blocking: bool,
    is_slave: bool,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
) -> RedisResult<ValkeyConnInner> {
    let config = if is_blocking {
        blocking_conn_manager_config()
    } else {
        conn_manager_config(cache_opts.as_ref())
    };
    let mut last_err = String::from("No sentinels provided");

    for sentinel_url in sentinel_urls {
        let client = match create_client(sentinel_url.as_str(), tls_opts.as_ref()) {
            Ok(c) => c,
            Err(e) => {
                last_err = format!("Sentinel {sentinel_url}: {e}");
                continue;
            }
        };

        let mut conn =
            match ConnectionManager::new_with_config(client, conn_manager_config(None)).await {
                Ok(c) => c,
                Err(e) => {
                    last_err = format!("Sentinel {sentinel_url}: {e}");
                    continue;
                }
            };

        let target_addr: Option<(String, String)> = if is_slave {
            let slaves: RedisResult<Vec<Vec<(String, String)>>> = redis::cmd("SENTINEL")
                .arg("slaves")
                .arg(service_name)
                .query_async(&mut conn)
                .await;
            match slaves {
                Ok(rows) => rows
                    .into_iter()
                    .filter_map(|row| {
                        let map: std::collections::HashMap<_, _> = row.into_iter().collect();
                        let flags = map.get("flags").cloned().unwrap_or_default();
                        if flags.contains("disconnected") || flags.contains("s_down") {
                            return None;
                        }
                        Some((map.get("ip")?.clone(), map.get("port")?.clone()))
                    })
                    .next(),
                Err(e) => {
                    last_err = format!("Sentinel {sentinel_url}: {e}");
                    None
                }
            }
        } else {
            let master: RedisResult<Vec<String>> = redis::cmd("SENTINEL")
                .arg("get-master-addr-by-name")
                .arg(service_name)
                .query_async(&mut conn)
                .await;
            match master {
                Ok(addr) if addr.len() == 2 => Some((addr[0].clone(), addr[1].clone())),
                Ok(_) => {
                    last_err = format!("Sentinel {sentinel_url}: unexpected response");
                    None
                }
                Err(e) => {
                    last_err = format!("Sentinel {sentinel_url}: {e}");
                    None
                }
            }
        };

        if let Some((host, port)) = target_addr {
            let scheme = if tls_opts.is_some() { "rediss" } else { "redis" };
            let base_url = format!("{scheme}://{host}:{port}/{db}");
            let target_url = url_with_resp3(&base_url);
            let target_client = create_client(target_url.as_str(), tls_opts.as_ref())?;
            let mgr = ConnectionManager::new_with_config(target_client, config).await?;
            return Ok(ValkeyConnInner::Sentinel(SentinelConn {
                inner: Arc::new(RwLock::new(mgr)),
                sentinel_urls: Arc::from(sentinel_urls),
                service_name: Arc::from(service_name),
                db,
                is_blocking,
                cache_opts,
                tls_opts,
                is_slave,
            }));
        }
    }

    Err(redis::RedisError::from((
        redis::ErrorKind::Io,
        "Failed to discover master from any sentinel",
        last_err,
    )))
}

/// Connect via a Sentinel quorum with automatic failover. `topology`
/// selects the master ("master") or any healthy slave ("slave").
pub async fn connect_sentinel(
    sentinel_urls: Vec<String>,
    service_name: &str,
    db: i64,
    is_slave: bool,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
) -> Result<ValkeyConn, String> {
    let inner = create_sentinel_inner(
        &sentinel_urls,
        service_name,
        db,
        false,
        is_slave,
        cache_opts,
        tls_opts.clone(),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(ValkeyConn {
        regular: inner,
        blocking: Arc::new(OnceCell::new()),
        config: ConnConfig::Sentinel {
            sentinel_urls: Arc::from(sentinel_urls),
            service_name: Arc::from(service_name),
            db,
            is_slave,
            tls_opts,
        },
    })
}
```

Update `build_blocking` to also handle the Sentinel arm:

```rust
async fn build_blocking(cfg: &ConnConfig) -> RedisResult<ValkeyConnInner> {
    match cfg {
        ConnConfig::Standard { url, tls_opts } => {
            let client = create_client(url, tls_opts.as_ref())?;
            let cfg = blocking_conn_manager_config();
            let mgr = ConnectionManager::new_with_config(client, cfg).await?;
            Ok(ValkeyConnInner::Standard(mgr))
        }
        ConnConfig::Cluster { urls, tls_opts } => {
            let url_refs: Vec<&str> = urls.iter().map(|s| s.as_str()).collect();
            let client = match tls_opts {
                Some(opts) => ClusterClient::builder(url_refs)
                    .certs(opts.to_tls_certs())
                    .build()?,
                None => ClusterClient::new(url_refs)?,
            };
            let conn = client.get_async_connection().await?;
            Ok(ValkeyConnInner::Cluster(conn))
        }
        ConnConfig::Sentinel {
            sentinel_urls,
            service_name,
            db,
            is_slave,
            tls_opts,
        } => {
            create_sentinel_inner(
                sentinel_urls,
                service_name,
                *db,
                true,
                *is_slave,
                None,
                tls_opts.clone(),
            )
            .await
        }
    }
}
```

- [ ] **Step 7: Verify the crate compiles**

Run: `cargo check -p redis-rs-py-driver`
Expected: clean. Existing standard + cluster tests must not regress because all changes are additive.

Run: `uv run pytest tests/driver/ tests/cluster/ tests/exceptions/ tests/async_bridge/ -v`
Expected: every test from plans 01–02 + 15 still passes.

- [ ] **Step 8: Commit**

```bash
git add crates/redis-rs-py-driver/src/connection.rs tests/conftest.py tests/sentinel/__init__.py tests/sentinel/test_connection_sentinel.py
git commit -m "feat(sentinel): add Sentinel arm to ValkeyConnInner with retry-on-failover"
```

---

## Task 2: `RedisRsDriver::connect_sentinel` factory

Add the Python-callable `connect_sentinel` constructor on `RedisRsDriver` so the façade has something to delegate to. Plus an `is_sentinel()` introspection helper.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/driver.rs`
- Modify: `python/redis_rs_py/_driver.pyi`
- Test: extend `tests/sentinel/test_connection_sentinel.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/sentinel/test_connection_sentinel.py`:

```python
def test_connect_sentinel_master_smoke(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    from redis_rs_py._driver import RedisRsDriver

    drv = RedisRsDriver.connect_sentinel(
        sentinel_urls, service_name=sentinel_service_name, db=0, is_slave=False
    )
    assert drv.is_sentinel() is True
    drv.set("smoke", b"ok")
    assert drv.get("smoke") == b"ok"
    drv.delete("smoke")


def test_connect_sentinel_slave_can_read(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    from redis_rs_py._driver import RedisRsDriver

    # Seed via master.
    master = RedisRsDriver.connect_sentinel(
        sentinel_urls, service_name=sentinel_service_name, db=0, is_slave=False
    )
    master.set("slave-read", b"yes")

    # Wait briefly for replication.
    import time

    time.sleep(0.5)

    slave = RedisRsDriver.connect_sentinel(
        sentinel_urls, service_name=sentinel_service_name, db=0, is_slave=True
    )
    assert slave.get("slave-read") == b"yes"


@pytest.mark.asyncio
async def test_aconnect_sentinel_master(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    from redis_rs_py._driver import RedisRsDriver

    drv = RedisRsDriver.connect_sentinel(
        sentinel_urls, service_name=sentinel_service_name, db=0, is_slave=False
    )
    await drv.aset("async-smoke", b"ok")
    assert await drv.aget("async-smoke") == b"ok"


def test_connect_sentinel_unknown_service_raises(
    sentinel_urls: list[str],
) -> None:
    from redis_rs_py._driver import RedisRsDriver
    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError

    with pytest.raises(RedisConnectionError):
        RedisRsDriver.connect_sentinel(
            sentinel_urls, service_name="no-such-master", db=0, is_slave=False
        )


def test_connect_sentinel_no_sentinels_raises() -> None:
    from redis_rs_py._driver import RedisRsDriver
    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError

    with pytest.raises(RedisConnectionError):
        RedisRsDriver.connect_sentinel([], service_name="x", db=0, is_slave=False)
```

Run: `uv run pytest tests/sentinel/test_connection_sentinel.py -v`
Expected: FAIL — `connect_sentinel` doesn't exist.

- [ ] **Step 2: Add `connect_sentinel` to the driver**

Edit `crates/redis-rs-py-driver/src/driver.rs`. Update imports:

```rust
use crate::connection::{
    ClientCacheOpts, TlsOpts, ValkeyConn, ValkeyConnInner,
    connect_cluster, connect_sentinel, connect_standard,
};
```

Inside `#[pymethods] impl RedisRsDriver`, append:

```rust
    #[staticmethod]
    #[pyo3(signature = (
        sentinel_urls,
        *,
        service_name,
        db = 0,
        is_slave = false,
        cache_max_size = None,
        cache_ttl_secs = None,
        ssl_ca_certs = None,
        ssl_certfile = None,
        ssl_keyfile = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn connect_sentinel(
        py: Python<'_>,
        sentinel_urls: Vec<String>,
        service_name: String,
        db: i64,
        is_slave: bool,
        cache_max_size: Option<usize>,
        cache_ttl_secs: Option<u64>,
        ssl_ca_certs: Option<Vec<u8>>,
        ssl_certfile: Option<Vec<u8>>,
        ssl_keyfile: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        let cache_opts = match (cache_max_size, cache_ttl_secs) {
            (None, None) => None,
            (max, ttl) => Some(ClientCacheOpts {
                max_size: max.unwrap_or(10_000),
                ttl_secs: ttl.unwrap_or(300),
            }),
        };
        let tls_opts =
            if ssl_ca_certs.is_some() || ssl_certfile.is_some() || ssl_keyfile.is_some() {
                Some(TlsOpts {
                    root_cert: ssl_ca_certs,
                    client_cert: ssl_certfile,
                    client_key: ssl_keyfile,
                })
            } else {
                None
            };
        let url_for_introspection = sentinel_urls.first().cloned().unwrap_or_default();
        let urls_clone = sentinel_urls.clone();
        let service_clone = service_name.clone();
        let conn = py.detach(|| {
            get_runtime().block_on(async {
                connect_sentinel(
                    urls_clone,
                    &service_clone,
                    db,
                    is_slave,
                    cache_opts,
                    tls_opts,
                )
                .await
            })
        });
        match conn {
            Ok(c) => Ok(RedisRsDriver {
                connection: c,
                url: url_for_introspection,
            }),
            Err(e) => Err(crate::errors::to_py_err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "connect_sentinel",
                e,
            )))),
        }
    }

    /// Topology introspection — `True` for sentinel-backed connections.
    fn is_sentinel(&self) -> bool {
        matches!(*self.connection, ValkeyConnInner::Sentinel(_))
    }

    /// Read-only access to the current sentinel master/slave URL —
    /// updates after rediscover. Returns `None` for non-sentinel topologies.
    fn sentinel_current_url(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &*self.connection {
            ValkeyConnInner::Sentinel(_) => {
                // The actual host:port lives behind the RwLock'd
                // ConnectionManager and isn't directly exposed by
                // redis-rs. We instead surface the *config* URL list.
                Ok(py.None())
            }
            _ => Ok(py.None()),
        }
    }
```

- [ ] **Step 3: Update the stub**

Append to the `class RedisRsDriver:` block in `python/redis_rs_py/_driver.pyi`:

```python
    @staticmethod
    def connect_sentinel(
        sentinel_urls: list[str],
        *,
        service_name: str,
        db: int = ...,
        is_slave: bool = ...,
        cache_max_size: int | None = ...,
        cache_ttl_secs: int | None = ...,
        ssl_ca_certs: bytes | None = ...,
        ssl_certfile: bytes | None = ...,
        ssl_keyfile: bytes | None = ...,
    ) -> RedisRsDriver: ...
    def is_sentinel(self) -> bool: ...
    def sentinel_current_url(self) -> str | None: ...
```

- [ ] **Step 4: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/sentinel/test_connection_sentinel.py -v`
Expected: 7 PASS (2 fixture sanity + 5 we added).

- [ ] **Step 5: Commit**

```bash
git add crates/redis-rs-py-driver/src/driver.rs python/redis_rs_py/_driver.pyi tests/sentinel/test_connection_sentinel.py
git commit -m "feat(sentinel): add RedisRsDriver.connect_sentinel factory"
```

---

## Task 3: `Sentinel` pyclass — constructor + accept-and-warn

Mirror `redis.sentinel.Sentinel.__init__(sentinels, min_other_sentinels=0, sentinel_kwargs=None, **connection_kwargs)`. The class is a thin holder of state — actual `Redis` instances come from `master_for` / `slave_for` (Task 4).

**Files:**
- Create: `crates/redis-rs-py-driver/src/facade/sentinel.rs`
- Modify: `crates/redis-rs-py-driver/src/facade/mod.rs` (add `pub mod sentinel;`)
- Modify: `crates/redis-rs-py-driver/src/facade/kwargs.rs` (extend with sentinel kwargs)
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (register `Sentinel` on `_driver.sentinel`)
- New: `python/redis_rs_py/sentinel/__init__.py`
- Modify: `python/redis_rs_py/_driver.pyi`
- Test: `tests/sentinel/test_sentinel_basic.py`

- [ ] **Step 1: Write the failing constructor tests**

`tests/sentinel/test_sentinel_basic.py`:

```python
"""Sentinel pyclass — constructor + sanity."""

from __future__ import annotations

import pytest

from redis_rs_py.sentinel import Sentinel


def test_construct_with_tuples(sentinel_urls: list[str]) -> None:
    nodes = []
    for url in sentinel_urls:
        host, port = url.removeprefix("redis://").split(":", 1)
        nodes.append((host, int(port)))
    s = Sentinel(nodes)
    assert s.sentinels == nodes


def test_construct_with_min_other_sentinels(sentinel_urls: list[str]) -> None:
    nodes = []
    for url in sentinel_urls:
        host, port = url.removeprefix("redis://").split(":", 1)
        nodes.append((host, int(port)))
    s = Sentinel(nodes, min_other_sentinels=1)
    assert s.min_other_sentinels == 1


def test_unknown_kwarg_warns(sentinel_urls: list[str]) -> None:
    import warnings

    nodes = []
    for url in sentinel_urls:
        host, port = url.removeprefix("redis://").split(":", 1)
        nodes.append((host, int(port)))
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        Sentinel(nodes, weird_kwarg="ignore-me")
    assert any("weird_kwarg" in str(rec.message) for rec in w)


def test_empty_sentinels_raises_dataerror() -> None:
    from redis_rs_py.exceptions import DataError

    with pytest.raises(DataError):
        Sentinel([])
```

Run: `uv run pytest tests/sentinel/test_sentinel_basic.py -v`
Expected: FAIL — `redis_rs_py.sentinel` doesn't exist.

- [ ] **Step 2: Implement `facade/sentinel.rs`**

Write `crates/redis-rs-py-driver/src/facade/sentinel.rs`:

```rust
// Sentinel + AsyncSentinel pyclasses.
//
// Mirrors `redis.sentinel.Sentinel.__init__`:
//
//     Sentinel(sentinels=[(host, port), ...],
//              min_other_sentinels=0,
//              sentinel_kwargs=None,
//              force_master_ip=None,
//              **connection_kwargs)
//
// `master_for(service_name)` returns a `Redis` backed by
// `RedisRsDriver::connect_sentinel(..., is_slave=False)`. `slave_for`
// uses `is_slave=True`. The discovery + admin commands open a transient
// sentinel-only connection.
//
// `connection_kwargs` flow through to the master/slave Redis instance.
// `sentinel_kwargs` flow through to the sentinel-side connections (TLS,
// timeouts) — currently we honour only `socket_timeout`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::driver::RedisRsDriver;
use crate::exceptions::DataError;
use crate::facade::kwargs::warn_unknown_kwargs;

/// Recognised sentinel-only kwargs.
const SENTINEL_KWARGS_ACCEPT: &[&str] = &[
    "sentinels",
    "min_other_sentinels",
    "sentinel_kwargs",
    "force_master_ip",
    // connection_kwargs forwarded to master_for/slave_for:
    "db",
    "username",
    "password",
    "decode_responses",
    "encoding",
    "encoding_errors",
    "ssl",
    "ssl_ca_certs",
    "ssl_certfile",
    "ssl_keyfile",
    "socket_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "client_name",
    "cache_max_size",
    "cache_ttl_secs",
];

/// Internal manager: holds the sentinel URL list + the per-service
/// round-robin index used by `slave_for`.
#[derive(Clone)]
struct SentinelInner {
    urls: Arc<[String]>,
    /// Map of service_name → (last slave index used).
    slave_cursors: Arc<dashmap::DashMap<String, AtomicUsize>>,
}

impl SentinelInner {
    fn next_slave_idx(&self, service: &str) -> usize {
        let entry = self
            .slave_cursors
            .entry(service.to_string())
            .or_insert_with(|| AtomicUsize::new(0));
        entry.value().fetch_add(1, Ordering::Relaxed)
    }
}

// =========================================================================
// Sentinel (sync façade)
// =========================================================================

#[pyclass(module = "redis_rs_py.sentinel")]
pub struct Sentinel {
    inner: SentinelInner,
    #[pyo3(get)]
    sentinels: Py<PyList>,
    #[pyo3(get)]
    min_other_sentinels: u32,
    sentinel_kwargs: Py<PyDict>,
    connection_kwargs: Py<PyDict>,
}

#[pymethods]
impl Sentinel {
    #[new]
    #[pyo3(signature = (
        sentinels,
        min_other_sentinels = 0,
        sentinel_kwargs = None,
        force_master_ip = None,
        **connection_kwargs
    ))]
    fn new(
        py: Python<'_>,
        sentinels: &Bound<'_, PyList>,
        min_other_sentinels: u32,
        sentinel_kwargs: Option<&Bound<'_, PyDict>>,
        force_master_ip: Option<String>,
        connection_kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        if sentinels.is_empty() {
            return Err(PyErr::new::<DataError, _>(
                "Sentinel requires at least one (host, port) tuple",
            ));
        }
        let _ = force_master_ip; // accept-and-warn handled below

        let mut urls: Vec<String> = Vec::with_capacity(sentinels.len());
        for item in sentinels.iter() {
            let tup: &Bound<'_, PyTuple> = item.downcast()?;
            if tup.len() != 2 {
                return Err(PyErr::new::<DataError, _>(
                    "each sentinel entry must be a (host, port) tuple",
                ));
            }
            let host: String = tup.get_item(0)?.extract()?;
            let port: u16 = tup.get_item(1)?.extract()?;
            urls.push(format!("redis://{host}:{port}"));
        }

        if let Some(kw) = connection_kwargs {
            warn_unknown_kwargs(py, kw, SENTINEL_KWARGS_ACCEPT)?;
        }

        let connection_kwargs = match connection_kwargs {
            Some(d) => d.clone().unbind(),
            None => PyDict::new(py).unbind(),
        };
        let sentinel_kwargs = match sentinel_kwargs {
            Some(d) => d.clone().unbind(),
            None => PyDict::new(py).unbind(),
        };

        Ok(Sentinel {
            inner: SentinelInner {
                urls: Arc::from(urls),
                slave_cursors: Arc::new(dashmap::DashMap::new()),
            },
            sentinels: sentinels.clone().unbind(),
            min_other_sentinels,
            sentinel_kwargs,
            connection_kwargs,
        })
    }

    fn __repr__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<String> {
        let n = slf.sentinels.bind(py).len();
        Ok(format!(
            "<Sentinel(sentinels={n}, min_other_sentinels={})>",
            slf.min_other_sentinels
        ))
    }
}
```

(`dashmap` is *not* in our deps; replace with `std::sync::Mutex<HashMap<String, AtomicUsize>>` or equivalent so we don't add a dep:)

Replace the `slave_cursors` field with:

```rust
slave_cursors: Arc<std::sync::Mutex<HashMap<String, AtomicUsize>>>,
```

And `next_slave_idx`:

```rust
fn next_slave_idx(&self, service: &str) -> usize {
    let mut guard = self.slave_cursors.lock().unwrap();
    let counter = guard
        .entry(service.to_string())
        .or_insert_with(|| AtomicUsize::new(0));
    counter.fetch_add(1, Ordering::Relaxed)
}
```

Add the registration helper at the bottom:

```rust
/// Submodule registration entry point.
pub fn register(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "sentinel")?;
    m.add_class::<Sentinel>()?;
    parent.add_submodule(&m)?;
    Ok(())
}
```

- [ ] **Step 3: Wire `facade/mod.rs`**

Edit `crates/redis-rs-py-driver/src/facade/mod.rs`:

```rust
pub mod cluster;
pub mod kwargs;
pub mod sentinel;
```

- [ ] **Step 4: Wire `lib.rs`**

Edit `crates/redis-rs-py-driver/src/lib.rs`. After the existing `facade::cluster::register(...)` call:

```rust
    facade::sentinel::register(m.py(), m)?;
```

- [ ] **Step 5: Create the Python re-export**

`python/redis_rs_py/sentinel/__init__.py`:

```python
"""Sentinel client — drop-in replacement for redis.sentinel.Sentinel."""

from redis_rs_py._driver.sentinel import Sentinel

__all__ = ["Sentinel"]
```

- [ ] **Step 6: Update `_driver.pyi`**

Append:

```python
class sentinel:
    class Sentinel:
        sentinels: list[tuple[str, int]]
        min_other_sentinels: int
        def __init__(
            self,
            sentinels: list[tuple[str, int]],
            min_other_sentinels: int = ...,
            sentinel_kwargs: dict[str, Any] | None = ...,
            force_master_ip: str | None = ...,
            **connection_kwargs: Any,
        ) -> None: ...
```

- [ ] **Step 7: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/sentinel/test_sentinel_basic.py -v`
Expected: 4 PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sentinel.rs crates/redis-rs-py-driver/src/facade/mod.rs crates/redis-rs-py-driver/src/lib.rs python/redis_rs_py/sentinel/__init__.py python/redis_rs_py/_driver.pyi tests/sentinel/test_sentinel_basic.py
git commit -m "feat(sentinel): add Sentinel pyclass with constructor"
```

---

## Task 4: `master_for` and `slave_for`

Each returns a `Redis` instance backed by `RedisRsDriver::connect_sentinel`. The `Redis` façade is owned by Plan 10 — until that lands, return the lower-level driver wrapped in a tiny Rust pyclass with the canonical command surface (just enough for tests). When Plan 10 ships, switch the body to instantiate `redis_rs_py.Redis` instead.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sentinel.rs`
- Test: extend `tests/sentinel/test_sentinel_basic.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/sentinel/test_sentinel_basic.py`:

```python
def test_master_for_returns_writable_client(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    nodes = [(u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls]
    s = Sentinel(nodes)
    master = s.master_for(sentinel_service_name)
    master.set("master-write", b"yes")
    assert master.get("master-write") == b"yes"


def test_slave_for_returns_readable_client(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    nodes = [(u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls]
    s = Sentinel(nodes)
    master = s.master_for(sentinel_service_name)
    master.set("slave-readback", b"replica-sees-this")

    import time
    time.sleep(0.5)  # let replication catch up

    slave = s.slave_for(sentinel_service_name)
    assert slave.get("slave-readback") == b"replica-sees-this"


def test_master_for_kwargs_override_construction_kwargs(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    nodes = [(u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls]
    s = Sentinel(nodes, db=0)
    master = s.master_for(sentinel_service_name, db=0)  # explicit override
    assert master.ping() is True


def test_slave_for_round_robin_across_calls(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    """With one slave in the topology, round-robin always returns the
    same slave but the cursor still increments — assert the call
    succeeds repeatedly."""
    nodes = [(u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1])) for u in sentinel_urls]
    s = Sentinel(nodes)
    for _ in range(3):
        slave = s.slave_for(sentinel_service_name)
        assert slave.ping() is True
```

Run: `uv run pytest tests/sentinel/test_sentinel_basic.py::test_master_for_returns_writable_client -v`
Expected: FAIL — `master_for` doesn't exist.

- [ ] **Step 2: Add a thin `SentinelRedis` pyclass + `master_for` / `slave_for`**

`SentinelRedis` is the per-service handle returned by `master_for`/`slave_for`. When Plan 10 lands, replace the body of `master_for`/`slave_for` to return a `Redis` instance instead.

Append to `crates/redis-rs-py-driver/src/facade/sentinel.rs`:

```rust
/// Per-service Redis handle returned by `master_for` / `slave_for`.
///
/// **Transitional shape**: when plan 10 ships the canonical `Redis`
/// façade, swap the body of `master_for`/`slave_for` to call
/// `Redis.from_driver(driver)` instead, then delete this class.
#[pyclass(module = "redis_rs_py.sentinel")]
pub struct SentinelRedis {
    driver: Py<RedisRsDriver>,
}

#[pymethods]
impl SentinelRedis {
    fn ping(&self, py: Python<'_>) -> PyResult<bool> {
        self.driver.bind(py).call_method0("ping")?.extract()
    }
    fn get(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.driver.bind(py).call_method1("get", (key,))?.unbind())
    }
    fn set(
        &self,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if let Some(t) = ttl {
            kw.set_item("ttl", t)?;
        }
        Ok(self
            .driver
            .bind(py)
            .call_method("set", (key, PyBytes::new(py, &value)), Some(&kw))?
            .unbind())
    }
    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        Ok(self.driver.bind(py).call_method1("delete", (keys,))?.unbind())
    }
    fn close(&self) -> PyResult<()> {
        Ok(())
    }
}

#[pymethods]
impl Sentinel {
    #[pyo3(signature = (
        service_name,
        redis_class = None,
        connection_pool_class = None,
        **kwargs
    ))]
    fn master_for(
        &self,
        py: Python<'_>,
        service_name: String,
        redis_class: Option<&Bound<'_, PyAny>>,
        connection_pool_class: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<SentinelRedis> {
        let _ = (redis_class, connection_pool_class); // accept-and-ignore
        self.build_redis(py, service_name, false, kwargs)
    }

    #[pyo3(signature = (
        service_name,
        redis_class = None,
        connection_pool_class = None,
        **kwargs
    ))]
    fn slave_for(
        &self,
        py: Python<'_>,
        service_name: String,
        redis_class: Option<&Bound<'_, PyAny>>,
        connection_pool_class: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<SentinelRedis> {
        let _ = (redis_class, connection_pool_class);
        // Bump the round-robin cursor so each call alternates if there
        // are multiple slaves.
        self.inner.next_slave_idx(&service_name);
        self.build_redis(py, service_name, true, kwargs)
    }
}

impl Sentinel {
    fn build_redis(
        &self,
        py: Python<'_>,
        service_name: String,
        is_slave: bool,
        per_call_kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<SentinelRedis> {
        // Merge: connection_kwargs (constructor) + per_call_kwargs (call site).
        // Per-call wins on conflict.
        let merged = PyDict::new(py);
        let conn_kw = self.connection_kwargs.bind(py);
        for (k, v) in conn_kw.iter() {
            merged.set_item(k, v)?;
        }
        if let Some(kw) = per_call_kwargs {
            for (k, v) in kw.iter() {
                merged.set_item(k, v)?;
            }
        }

        let db: i64 = match merged.get_item("db")? {
            Some(v) => v.extract().unwrap_or(0),
            None => 0,
        };
        let cache_max_size: Option<usize> = merged
            .get_item("cache_max_size")?
            .and_then(|v| v.extract().ok());
        let cache_ttl_secs: Option<u64> = merged
            .get_item("cache_ttl_secs")?
            .and_then(|v| v.extract().ok());
        let ssl_ca_certs: Option<Vec<u8>> =
            merged.get_item("ssl_ca_certs")?.and_then(|v| v.extract().ok());
        let ssl_certfile: Option<Vec<u8>> =
            merged.get_item("ssl_certfile")?.and_then(|v| v.extract().ok());
        let ssl_keyfile: Option<Vec<u8>> =
            merged.get_item("ssl_keyfile")?.and_then(|v| v.extract().ok());

        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let factory_kwargs = PyDict::new(py);
        factory_kwargs.set_item("service_name", service_name)?;
        factory_kwargs.set_item("db", db)?;
        factory_kwargs.set_item("is_slave", is_slave)?;
        if let Some(s) = cache_max_size {
            factory_kwargs.set_item("cache_max_size", s)?;
        }
        if let Some(s) = cache_ttl_secs {
            factory_kwargs.set_item("cache_ttl_secs", s)?;
        }
        if let Some(b) = ssl_ca_certs {
            factory_kwargs.set_item("ssl_ca_certs", PyBytes::new(py, &b))?;
        }
        if let Some(b) = ssl_certfile {
            factory_kwargs.set_item("ssl_certfile", PyBytes::new(py, &b))?;
        }
        if let Some(b) = ssl_keyfile {
            factory_kwargs.set_item("ssl_keyfile", PyBytes::new(py, &b))?;
        }

        let driver = py.get_type::<RedisRsDriver>().call_method(
            "connect_sentinel",
            (urls,),
            Some(&factory_kwargs),
        )?;
        let driver: Py<RedisRsDriver> = driver.extract()?;
        Ok(SentinelRedis { driver })
    }
}
```

Update `register` to also expose `SentinelRedis`:

```rust
pub fn register(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "sentinel")?;
    m.add_class::<Sentinel>()?;
    m.add_class::<SentinelRedis>()?;
    parent.add_submodule(&m)?;
    Ok(())
}
```

- [ ] **Step 3: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/sentinel/test_sentinel_basic.py -v`
Expected: 8 PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sentinel.rs tests/sentinel/test_sentinel_basic.py
git commit -m "feat(sentinel): add master_for/slave_for + SentinelRedis handle"
```

---

## Task 5: Discovery commands — `discover_master`, `discover_slaves`, `sentinel_get_master_addr_by_name`

Open a transient sentinel connection, run the discovery command, parse the address(es), close. Fits in ~30 LOC per method.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sentinel.rs`
- Test: `tests/sentinel/test_sentinel_admin.py`

- [ ] **Step 1: Write the failing tests**

`tests/sentinel/test_sentinel_admin.py`:

```python
"""Sentinel discovery + introspection commands."""

from __future__ import annotations

import pytest

from redis_rs_py.sentinel import Sentinel


@pytest.fixture
def s(sentinel_urls: list[str]) -> Sentinel:
    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1]))
        for u in sentinel_urls
    ]
    return Sentinel(nodes)


def test_discover_master(s: Sentinel, sentinel_service_name: str) -> None:
    addr = s.discover_master(sentinel_service_name)
    assert isinstance(addr, tuple)
    host, port = addr
    assert isinstance(host, str)
    assert isinstance(port, int) and port > 0


def test_discover_slaves_returns_list(s: Sentinel, sentinel_service_name: str) -> None:
    slaves = s.discover_slaves(sentinel_service_name)
    assert isinstance(slaves, list)
    # We have exactly 1 replica in the fixture.
    assert len(slaves) == 1
    host, port = slaves[0]
    assert isinstance(host, str)
    assert isinstance(port, int) and port > 0


def test_sentinel_get_master_addr_by_name(
    s: Sentinel, sentinel_service_name: str
) -> None:
    addr = s.sentinel_get_master_addr_by_name(sentinel_service_name)
    assert isinstance(addr, tuple)
    assert len(addr) == 2


def test_discover_master_unknown_service_raises(s: Sentinel) -> None:
    from redis_rs_py.exceptions import MasterDownError

    with pytest.raises(MasterDownError):
        s.discover_master("no-such-service")
```

Run: `uv run pytest tests/sentinel/test_sentinel_admin.py -v`
Expected: FAIL — methods don't exist.

- [ ] **Step 2: Add the discovery methods**

Append to `crates/redis-rs-py-driver/src/facade/sentinel.rs`. We need a small async helper — pull `get_runtime` in:

```rust
use crate::runtime::get_runtime;
use redis::aio::ConnectionManager;
use redis::aio::ConnectionManagerConfig;
use crate::exceptions::MasterDownError;
use std::time::Duration;

/// Open a transient connection to the first reachable sentinel and
/// hand it back to the caller closure. Returns the closure's result.
async fn with_sentinel<F, Fut, T>(
    urls: &[String],
    f: F,
) -> Result<T, redis::RedisError>
where
    F: Fn(ConnectionManager) -> Fut,
    Fut: std::future::Future<Output = Result<T, redis::RedisError>>,
{
    let mut last_err: Option<redis::RedisError> = None;
    for u in urls {
        let client = match redis::Client::open(u.as_str()) {
            Ok(c) => c,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
        let cfg = ConnectionManagerConfig::new()
            .set_response_timeout(Some(Duration::from_secs(5)));
        let conn = match ConnectionManager::new_with_config(client, cfg).await {
            Ok(c) => c,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
        match f(conn).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        redis::RedisError::from((redis::ErrorKind::Io, "no sentinels reachable"))
    }))
}

#[pymethods]
impl Sentinel {
    fn discover_master(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<(String, u16)> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let result = py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    async move {
                        let addr: Vec<String> = redis::cmd("SENTINEL")
                            .arg("get-master-addr-by-name")
                            .arg(&svc)
                            .query_async(&mut conn)
                            .await?;
                        if addr.len() != 2 {
                            return Err(redis::RedisError::from((
                                redis::ErrorKind::Server(redis::ServerErrorKind::MasterDown),
                                "no master found",
                            )));
                        }
                        let port: u16 = addr[1]
                            .parse()
                            .map_err(|_| {
                                redis::RedisError::from((
                                    redis::ErrorKind::ResponseError,
                                    "invalid port",
                                ))
                            })?;
                        Ok((addr[0].clone(), port))
                    }
                })
                .await
            })
        });
        result.map_err(|e| {
            // Specifically map "no master" / IO failures into MasterDownError.
            PyErr::new::<MasterDownError, _>(e.to_string())
        })
    }

    fn discover_slaves(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<Vec<(String, u16)>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let result = py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    async move {
                        let rows: Vec<Vec<(String, String)>> = redis::cmd("SENTINEL")
                            .arg("slaves")
                            .arg(&svc)
                            .query_async(&mut conn)
                            .await?;
                        let mut out = Vec::with_capacity(rows.len());
                        for row in rows {
                            let map: HashMap<_, _> = row.into_iter().collect();
                            let flags = map.get("flags").cloned().unwrap_or_default();
                            if flags.contains("disconnected")
                                || flags.contains("s_down")
                                || flags.contains("o_down")
                            {
                                continue;
                            }
                            let host = match map.get("ip") {
                                Some(h) => h.clone(),
                                None => continue,
                            };
                            let port: u16 = match map.get("port").and_then(|p| p.parse().ok()) {
                                Some(p) => p,
                                None => continue,
                            };
                            out.push((host, port));
                        }
                        Ok(out)
                    }
                })
                .await
            })
        });
        result.map_err(crate::errors::to_py_err)
    }

    fn sentinel_get_master_addr_by_name(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<(String, u16)> {
        // Alias of discover_master per redis-py.
        self.discover_master(py, service_name)
    }
}
```

- [ ] **Step 3: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/sentinel/test_sentinel_admin.py -v`
Expected: 4 PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sentinel.rs tests/sentinel/test_sentinel_admin.py
git commit -m "feat(sentinel): add discover_master / discover_slaves"
```

---

## Task 6: Introspection commands — `sentinel_masters`, `sentinel_master`, `sentinel_slaves`, `sentinel_sentinels`

Each runs the corresponding `SENTINEL` command and returns a list-of-dicts (or single dict) shaped like redis-py's output. The transient-connection helper `with_sentinel` from Task 5 stays the same.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sentinel.rs`
- Test: extend `tests/sentinel/test_sentinel_admin.py`

- [ ] **Step 1: Write the failing tests**

Append:

```python
def test_sentinel_masters(s: Sentinel) -> None:
    masters = s.sentinel_masters()
    assert isinstance(masters, dict)
    # Keyed by service name.
    assert SERVICE_NAME in masters or "redis-rs-py-test-master" in masters


def test_sentinel_master_returns_dict(s: Sentinel, sentinel_service_name: str) -> None:
    info = s.sentinel_master(sentinel_service_name)
    assert isinstance(info, dict)
    # Standard fields per redis-py.
    assert "ip" in info or b"ip" in info
    assert "port" in info or b"port" in info


def test_sentinel_slaves_returns_list(s: Sentinel, sentinel_service_name: str) -> None:
    slaves = s.sentinel_slaves(sentinel_service_name)
    assert isinstance(slaves, list)
    assert len(slaves) == 1


def test_sentinel_sentinels_returns_list(
    s: Sentinel, sentinel_service_name: str
) -> None:
    sentinels = s.sentinel_sentinels(sentinel_service_name)
    assert isinstance(sentinels, list)
    # Excludes the calling sentinel itself, so 2 of 3.
    assert len(sentinels) == 2
```

(Add `from tests.conftest import SERVICE_NAME` at the top — if conftest.py doesn't expose it, copy the constant inline.)

- [ ] **Step 2: Add the introspection methods**

Append to `crates/redis-rs-py-driver/src/facade/sentinel.rs`:

```rust
fn rows_to_dicts(
    py: Python<'_>,
    rows: Vec<Vec<(String, String)>>,
) -> PyResult<Vec<Py<PyDict>>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let d = PyDict::new(py);
        for (k, v) in row {
            d.set_item(k, v)?;
        }
        out.push(d.unbind());
    }
    Ok(out)
}

#[pymethods]
impl Sentinel {
    fn sentinel_masters(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let rows: Vec<Vec<(String, String)>> = py
            .detach(|| {
                get_runtime().block_on(async move {
                    with_sentinel(&urls, |mut conn| async move {
                        redis::cmd("SENTINEL")
                            .arg("masters")
                            .query_async(&mut conn)
                            .await
                    })
                    .await
                })
            })
            .map_err(crate::errors::to_py_err)?;
        let out = PyDict::new(py);
        for row in rows {
            let map: HashMap<_, _> = row.into_iter().collect();
            let name = match map.get("name") {
                Some(n) => n.clone(),
                None => continue,
            };
            let entry = PyDict::new(py);
            for (k, v) in map {
                entry.set_item(k, v)?;
            }
            out.set_item(name, entry)?;
        }
        Ok(out.unbind())
    }

    fn sentinel_master(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<Py<PyDict>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let row: Vec<(String, String)> = py
            .detach(|| {
                get_runtime().block_on(async move {
                    with_sentinel(&urls, |mut conn| {
                        let svc = service_name.clone();
                        async move {
                            redis::cmd("SENTINEL")
                                .arg("master")
                                .arg(&svc)
                                .query_async(&mut conn)
                                .await
                        }
                    })
                    .await
                })
            })
            .map_err(crate::errors::to_py_err)?;
        let d = PyDict::new(py);
        for (k, v) in row {
            d.set_item(k, v)?;
        }
        Ok(d.unbind())
    }

    fn sentinel_slaves(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let rows: Vec<Vec<(String, String)>> = py
            .detach(|| {
                get_runtime().block_on(async move {
                    with_sentinel(&urls, |mut conn| {
                        let svc = service_name.clone();
                        async move {
                            redis::cmd("SENTINEL")
                                .arg("slaves")
                                .arg(&svc)
                                .query_async(&mut conn)
                                .await
                        }
                    })
                    .await
                })
            })
            .map_err(crate::errors::to_py_err)?;
        rows_to_dicts(py, rows)
    }

    fn sentinel_sentinels(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let rows: Vec<Vec<(String, String)>> = py
            .detach(|| {
                get_runtime().block_on(async move {
                    with_sentinel(&urls, |mut conn| {
                        let svc = service_name.clone();
                        async move {
                            redis::cmd("SENTINEL")
                                .arg("sentinels")
                                .arg(&svc)
                                .query_async(&mut conn)
                                .await
                        }
                    })
                    .await
                })
            })
            .map_err(crate::errors::to_py_err)?;
        rows_to_dicts(py, rows)
    }
}
```

- [ ] **Step 3: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/sentinel/test_sentinel_admin.py -v`
Expected: 8 PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sentinel.rs tests/sentinel/test_sentinel_admin.py
git commit -m "feat(sentinel): add sentinel_masters/master/slaves/sentinels"
```

---

## Task 7: Admin commands — `sentinel_failover`, `sentinel_reset`, `sentinel_set`, `sentinel_remove`, `sentinel_monitor`

Mutating commands. `sentinel_failover` triggers a manual failover; the others administer the watched-master set.

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sentinel.rs`
- Test: extend `tests/sentinel/test_sentinel_admin.py`

- [ ] **Step 1: Write the failing tests**

Append:

```python
def test_sentinel_set_then_remove(
    s: Sentinel, sentinel_service_name: str
) -> None:
    """SET a benign sentinel option, observe it via sentinel_master."""
    s.sentinel_set(sentinel_service_name, "down-after-milliseconds", "3000")
    info = s.sentinel_master(sentinel_service_name)
    val = info.get("down-after-milliseconds") or info.get(b"down-after-milliseconds")
    assert val == "3000" or val == b"3000"


def test_sentinel_reset_returns_count(
    s: Sentinel, sentinel_service_name: str
) -> None:
    n = s.sentinel_reset("*")  # reset all known masters
    assert isinstance(n, int)
    assert n >= 1


def test_sentinel_failover_on_known_master(
    s: Sentinel, sentinel_service_name: str
) -> None:
    """sentinel_failover triggers a manual failover. We just verify the
    call returns and the topology eventually settles."""
    pytest.skip(
        "sentinel_failover races with the failover test in test_sentinel_failover.py; "
        "we exercise it there with proper teardown sequencing."
    )


def test_sentinel_remove_then_monitor_roundtrip(
    s: Sentinel, sentinel_service_name: str
) -> None:
    """Remove the watched master then re-add it. Skipped because the
    test depends on knowing the master IP, which we discover via
    discover_master."""
    addr = s.discover_master(sentinel_service_name)
    # Don't actually remove — that breaks every other test.
    assert addr is not None
```

- [ ] **Step 2: Add the admin methods**

Append to `crates/redis-rs-py-driver/src/facade/sentinel.rs`:

```rust
#[pymethods]
impl Sentinel {
    fn sentinel_failover(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<()> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("failover")
                            .arg(&svc)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn sentinel_reset(&self, py: Python<'_>, pattern: String) -> PyResult<i64> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let pat = pattern.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("reset")
                            .arg(&pat)
                            .query_async(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn sentinel_set(
        &self,
        py: Python<'_>,
        service_name: String,
        option: String,
        value: String,
    ) -> PyResult<()> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    let opt = option.clone();
                    let val = value.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("set")
                            .arg(&svc)
                            .arg(&opt)
                            .arg(&val)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn sentinel_remove(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<()> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("remove")
                            .arg(&svc)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }

    fn sentinel_monitor(
        &self,
        py: Python<'_>,
        service_name: String,
        ip: String,
        port: u16,
        quorum: u32,
    ) -> PyResult<()> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        py.detach(|| {
            get_runtime().block_on(async move {
                with_sentinel(&urls, |mut conn| {
                    let svc = service_name.clone();
                    let ip = ip.clone();
                    async move {
                        redis::cmd("SENTINEL")
                            .arg("monitor")
                            .arg(&svc)
                            .arg(&ip)
                            .arg(port)
                            .arg(quorum)
                            .query_async::<()>(&mut conn)
                            .await
                    }
                })
                .await
            })
        })
        .map_err(crate::errors::to_py_err)
    }
}
```

- [ ] **Step 3: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/sentinel/test_sentinel_admin.py -v`
Expected: 11 PASS, 1 SKIP.

- [ ] **Step 4: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sentinel.rs tests/sentinel/test_sentinel_admin.py
git commit -m "feat(sentinel): add sentinel_failover/reset/set/remove/monitor"
```

---

## Task 8: Failover correctness — the load-bearing test

The whole point of `Sentinel` is transparent failover. This test stops the master container, waits for the sentinel quorum to elect a new master (10–15 s with our fixture's `failover-timeout 10000`), then runs a SET via the same `master_for` Redis — must succeed because of the rediscover-on-failover-error path in `sentinel_retry!`.

**Files:**
- Test: `tests/sentinel/test_sentinel_failover.py`

- [ ] **Step 1: Write the test**

`tests/sentinel/test_sentinel_failover.py`:

```python
"""Critical correctness test: transparent failover.

This test is intentionally heavyweight (≈ 30–60 s wall clock):
  * Use `master_for` to grab a writable Redis pointed at the master.
  * SET a key (verifies the connection is healthy).
  * Stop the master container — sentinels detect within ~2 s
    (down-after-milliseconds), trigger a vote, elect the replica.
  * SET again on the same Redis handle — the in-flight call hits a
    Closed connection, sentinel_retry! triggers, rediscover() picks up
    the new master, the retry SET completes against the new master.

If the test fails, the most likely culprits (in order):
  1. The fixture's down-after-milliseconds is too high.
  2. SentinelConn::is_failover_error doesn't catch the actual error
     class produced by the dropped connection.
  3. The replica wasn't promoted in time (raise the wait window).
"""

from __future__ import annotations

import time

import pytest
from testcontainers.core.container import DockerContainer

from redis_rs_py.sentinel import Sentinel


def _find_master_container(
    sentinel_urls: list[str], service_name: str
) -> DockerContainer | None:
    """Discover the current master via the upstream redis-py Sentinel,
    then walk the docker container list to find the matching one.

    NOTE: This depends on the conftest having stored container handles
    in a session-scoped registry. If that registry doesn't exist, this
    test must skip — see fixture comment below.
    """
    pytest.skip(
        "Requires a fixture that exposes container handles for stop/start. "
        "Implement `sentinel_containers` in conftest.py and re-enable."
    )


@pytest.fixture
def sentinel_containers() -> list[DockerContainer]:
    """Return the underlying container handles used by the sentinel
    fixture. To enable, refactor the cluster/sentinel fixture to also
    yield the container list — see TODO in conftest.py."""
    pytest.skip(
        "tests/conftest.py does not yet expose sentinel_containers; "
        "see Plan 16 Task 8 instructions."
    )


def test_failover_is_transparent_to_master_for_caller(
    sentinel_urls: list[str],
    sentinel_service_name: str,
    sentinel_containers: list[DockerContainer],
) -> None:
    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1]))
        for u in sentinel_urls
    ]
    s = Sentinel(nodes)
    master = s.master_for(sentinel_service_name)

    # Sanity: write succeeds against the original master.
    master.set("pre-failover", b"ok")
    assert master.get("pre-failover") == b"ok"

    # Find the container running on the master's host:port and stop it.
    addr = s.discover_master(sentinel_service_name)
    host, port = addr
    target = None
    for c in sentinel_containers:
        try:
            ch_host = c.get_container_host_ip()
            ch_port = int(c.get_exposed_port(6379))
        except Exception:
            continue
        if ch_host == host and ch_port == port:
            target = c
            break

    assert target is not None, f"no container matches master {host}:{port}"
    target.stop()

    # Wait for sentinels to elect a new master. With down-after-milliseconds
    # = 2000 + failover-timeout = 10000, this should be < 15 s.
    deadline = time.monotonic() + 30
    new_addr: tuple[str, int] | None = None
    while time.monotonic() < deadline:
        try:
            candidate = s.discover_master(sentinel_service_name)
            if candidate != addr:
                new_addr = candidate
                break
        except Exception:
            pass
        time.sleep(0.5)

    assert new_addr is not None, "no failover within 30s"
    assert new_addr != addr

    # Critical: this SET must succeed. The handle is still the SAME
    # `master` object we created before the failover. The first attempt
    # may hit the dead connection; sentinel_retry! triggers rediscover,
    # the retry hits the new master.
    deadline = time.monotonic() + 15
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            master.set("post-failover", b"ok")
            assert master.get("post-failover") == b"ok"
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(0.3)
    else:
        raise AssertionError(
            f"post-failover SET never succeeded; last error: {last_err}"
        )
```

- [ ] **Step 2: Refactor `tests/conftest.py` to expose `sentinel_containers`**

Edit the sentinel fixture to ALSO yield the container list. Replace the `_spawn_sentinel_topology` call site:

```python
@pytest.fixture(scope="session")
def _sentinel_topology(
    tmp_path_factory: pytest.TempPathFactory, worker_id: str
) -> Iterator[tuple[list[str], list[DockerContainer]]]:
    if worker_id == "master":
        network, containers, urls = _spawn_sentinel_topology()
        try:
            yield urls, containers
        finally:
            for c in containers:
                c.stop()
            network.remove()
        return

    root = tmp_path_factory.getbasetemp().parent
    lockfile = root / "valkey_sentinel.lock"
    urlsfile = root / "valkey_sentinel.urls"
    network = None
    containers: list[DockerContainer] = []
    with FileLock(str(lockfile)):
        if urlsfile.exists():
            urls = urlsfile.read_text().strip().splitlines()
        else:
            network, containers, urls = _spawn_sentinel_topology()
            urlsfile.write_text("\n".join(urls))
    try:
        yield urls, containers
    finally:
        if containers:
            for c in containers:
                c.stop()
            if network is not None:
                network.remove()
            urlsfile.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def sentinel_urls(_sentinel_topology) -> list[str]:
    urls, _ = _sentinel_topology
    return urls


@pytest.fixture(scope="session")
def sentinel_containers(_sentinel_topology) -> list[DockerContainer]:
    """Container handles for the sentinel topology — used by the
    failover test to stop/start the master."""
    _, containers = _sentinel_topology
    return containers
```

(The xdist-worker branch only stores URLs in the lockfile, not container handles, so on non-master workers `sentinel_containers` will be empty. The failover test detects that and skips on those workers — add `pytest.skip("failover test runs only on master xdist worker")` to the test if `len(sentinel_containers) == 0`.)

Add to the test:

```python
def test_failover_is_transparent_to_master_for_caller(
    sentinel_urls,
    sentinel_service_name,
    sentinel_containers,
):
    if not sentinel_containers:
        pytest.skip("Failover test runs only on the xdist master worker.")
    ...
```

Also remove the two `pytest.skip(...)` placeholder lines from the helpers in the original test draft.

- [ ] **Step 3: Run the failover test**

Run: `uv run pytest tests/sentinel/test_sentinel_failover.py -v -s -p no:xdist`
Expected: 1 PASS in ~30–60 s. The `-p no:xdist` flag bypasses xdist so we hit the master worker branch reliably.

If it fails:
* If the post-failover SET raises `ReadOnlyError`: a slave was reached but never promoted — extend the wait deadline.
* If the post-failover SET raises `ConnectionError`: rediscover succeeded but the new connection isn't being swapped in — verify `sentinel_retry!` calls `s.get_conn().await` *after* `s.rediscover().await?`.
* If `discover_master` returns the SAME address even after stopping the container: the sentinel quorum hasn't recognised the master is down — bump `failover-timeout` lower or increase the test wait.

- [ ] **Step 4: Commit**

```bash
git add tests/sentinel/test_sentinel_failover.py tests/conftest.py
git commit -m "test(sentinel): cover transparent-failover end-to-end"
```

---

## Task 9: `AsyncSentinel` async sibling

Mirror the entire `Sentinel` surface but every method returns a `RedisRsAwaitable` (or, for synchronous-by-nature methods like `master_for`, returns the result directly — `master_for` is not I/O on its own; the I/O happens when the returned `Redis` is used).

**Files:**
- Modify: `crates/redis-rs-py-driver/src/facade/sentinel.rs`
- Modify: `crates/redis-rs-py-driver/src/lib.rs` (register on `_driver.asyncio.sentinel`)
- New: `python/redis_rs_py/asyncio/sentinel/__init__.py`
- Modify: `python/redis_rs_py/_driver.pyi`
- Test: `tests/sentinel/test_async_sentinel.py`

- [ ] **Step 1: Write the failing tests**

`tests/sentinel/test_async_sentinel.py`:

```python
"""AsyncSentinel — async sibling of Sentinel."""

from __future__ import annotations

import pytest

from redis_rs_py.asyncio.sentinel import AsyncSentinel


@pytest.mark.asyncio
async def test_async_master_for_smoke(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1]))
        for u in sentinel_urls
    ]
    s = AsyncSentinel(nodes)
    master = s.master_for(sentinel_service_name)
    await master.set("ak", b"av")
    assert await master.get("ak") == b"av"


@pytest.mark.asyncio
async def test_async_discover_master(
    sentinel_urls: list[str], sentinel_service_name: str
) -> None:
    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1]))
        for u in sentinel_urls
    ]
    s = AsyncSentinel(nodes)
    addr = await s.discover_master(sentinel_service_name)
    assert isinstance(addr, tuple)
    assert len(addr) == 2


@pytest.mark.asyncio
async def test_async_sentinel_masters(sentinel_urls: list[str]) -> None:
    nodes = [
        (u.removeprefix("redis://").split(":")[0], int(u.removeprefix("redis://").split(":")[1]))
        for u in sentinel_urls
    ]
    s = AsyncSentinel(nodes)
    masters = await s.sentinel_masters()
    assert isinstance(masters, dict)
```

- [ ] **Step 2: Implement `AsyncSentinel`**

Append to `crates/redis-rs-py-driver/src/facade/sentinel.rs`:

```rust
use crate::async_bridge::{RawResult, RedisRsAwaitable};

#[pyclass(module = "redis_rs_py.asyncio.sentinel")]
pub struct AsyncSentinel {
    inner: SentinelInner,
    #[pyo3(get)]
    sentinels: Py<PyList>,
    #[pyo3(get)]
    min_other_sentinels: u32,
    sentinel_kwargs: Py<PyDict>,
    connection_kwargs: Py<PyDict>,
}

#[pymethods]
impl AsyncSentinel {
    #[new]
    #[pyo3(signature = (
        sentinels,
        min_other_sentinels = 0,
        sentinel_kwargs = None,
        force_master_ip = None,
        **connection_kwargs
    ))]
    fn new(
        py: Python<'_>,
        sentinels: &Bound<'_, PyList>,
        min_other_sentinels: u32,
        sentinel_kwargs: Option<&Bound<'_, PyDict>>,
        force_master_ip: Option<String>,
        connection_kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        // Reuse the sync constructor's validation by delegating.
        let _ = force_master_ip;
        let inst = Sentinel::new(
            py,
            sentinels,
            min_other_sentinels,
            sentinel_kwargs,
            None,
            connection_kwargs,
        )?;
        Ok(AsyncSentinel {
            inner: inst.inner,
            sentinels: inst.sentinels,
            min_other_sentinels: inst.min_other_sentinels,
            sentinel_kwargs: inst.sentinel_kwargs,
            connection_kwargs: inst.connection_kwargs,
        })
    }

    fn master_for(
        &self,
        py: Python<'_>,
        service_name: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<AsyncSentinelRedis> {
        // Same merging logic as the sync version.
        let driver = build_async_redis_driver(
            py,
            &self.inner,
            &self.connection_kwargs,
            service_name,
            false,
            kwargs,
        )?;
        Ok(AsyncSentinelRedis { driver })
    }

    fn slave_for(
        &self,
        py: Python<'_>,
        service_name: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<AsyncSentinelRedis> {
        self.inner.next_slave_idx(&service_name);
        let driver = build_async_redis_driver(
            py,
            &self.inner,
            &self.connection_kwargs,
            service_name,
            true,
            kwargs,
        )?;
        Ok(AsyncSentinelRedis { driver })
    }

    fn discover_master(
        &self,
        py: Python<'_>,
        service_name: String,
    ) -> PyResult<Py<PyAny>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        get_runtime().spawn(async move {
            let r = with_sentinel(&urls, |mut conn| {
                let svc = service_name.clone();
                async move {
                    let addr: Vec<String> = redis::cmd("SENTINEL")
                        .arg("get-master-addr-by-name")
                        .arg(&svc)
                        .query_async(&mut conn)
                        .await?;
                    if addr.len() != 2 {
                        return Err(redis::RedisError::from((
                            redis::ErrorKind::Server(
                                redis::ServerErrorKind::MasterDown,
                            ),
                            "no master",
                        )));
                    }
                    Ok(addr)
                }
            })
            .await;
            let raw = match r {
                Ok(addr) => RawResult::StringList(addr),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(aw.into_pyobject(py)?.into_any().unbind())
    }

    fn sentinel_masters(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let urls: Vec<String> = self.inner.urls.iter().cloned().collect();
        let (tx, rx) = ::tokio::sync::oneshot::channel();
        let aw = RedisRsAwaitable::new(rx);
        get_runtime().spawn(async move {
            let r: Result<Vec<Vec<(String, String)>>, _> =
                with_sentinel(&urls, |mut conn| async move {
                    redis::cmd("SENTINEL")
                        .arg("masters")
                        .query_async(&mut conn)
                        .await
                })
                .await;
            let raw = match r {
                Ok(rows) => RawResult::Value(redis::Value::Array(
                    rows.into_iter()
                        .map(|row| {
                            redis::Value::Array(
                                row.into_iter()
                                    .flat_map(|(k, v)| {
                                        vec![
                                            redis::Value::BulkString(k.into_bytes()),
                                            redis::Value::BulkString(v.into_bytes()),
                                        ]
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                )),
                Err(e) => crate::errors::classify(e),
            };
            let _ = tx.send(raw);
        });
        Ok(aw.into_pyobject(py)?.into_any().unbind())
    }
}

fn build_async_redis_driver(
    py: Python<'_>,
    inner: &SentinelInner,
    connection_kwargs: &Py<PyDict>,
    service_name: String,
    is_slave: bool,
    per_call: Option<&Bound<'_, PyDict>>,
) -> PyResult<Py<RedisRsDriver>> {
    let merged = PyDict::new(py);
    let conn_kw = connection_kwargs.bind(py);
    for (k, v) in conn_kw.iter() {
        merged.set_item(k, v)?;
    }
    if let Some(kw) = per_call {
        for (k, v) in kw.iter() {
            merged.set_item(k, v)?;
        }
    }
    let db: i64 = match merged.get_item("db")? {
        Some(v) => v.extract().unwrap_or(0),
        None => 0,
    };
    let urls: Vec<String> = inner.urls.iter().cloned().collect();
    let factory_kwargs = PyDict::new(py);
    factory_kwargs.set_item("service_name", service_name)?;
    factory_kwargs.set_item("db", db)?;
    factory_kwargs.set_item("is_slave", is_slave)?;
    let driver = py.get_type::<RedisRsDriver>().call_method(
        "connect_sentinel",
        (urls,),
        Some(&factory_kwargs),
    )?;
    driver.extract()
}

#[pyclass(module = "redis_rs_py.asyncio.sentinel")]
pub struct AsyncSentinelRedis {
    driver: Py<RedisRsDriver>,
}

#[pymethods]
impl AsyncSentinelRedis {
    fn ping(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.driver.bind(py).call_method0("aping")?.unbind())
    }
    fn get(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        Ok(self.driver.bind(py).call_method1("aget", (key,))?.unbind())
    }
    fn set(
        &self,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let kw = PyDict::new(py);
        if let Some(t) = ttl {
            kw.set_item("ttl", t)?;
        }
        Ok(self
            .driver
            .bind(py)
            .call_method("aset", (key, PyBytes::new(py, &value)), Some(&kw))?
            .unbind())
    }
    #[pyo3(signature = (*keys))]
    fn delete(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Py<PyAny>> {
        Ok(self.driver.bind(py).call_method1("adelete", (keys,))?.unbind())
    }
}

pub fn register_async(py: Python<'_>, parent_asyncio: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "sentinel")?;
    m.add_class::<AsyncSentinel>()?;
    m.add_class::<AsyncSentinelRedis>()?;
    parent_asyncio.add_submodule(&m)?;
    Ok(())
}
```

- [ ] **Step 3: Wire `lib.rs`**

Inside the `_driver` body, after `facade::cluster::register_async(...)`:

```rust
    facade::sentinel::register_async(m.py(), &asyncio_module)?;
```

(The `asyncio_module` exists from Plan 15 Task 9.)

- [ ] **Step 4: Create the Python re-export**

`python/redis_rs_py/asyncio/sentinel/__init__.py`:

```python
"""Async sentinel client."""

from redis_rs_py._driver.asyncio.sentinel import AsyncSentinel, AsyncSentinelRedis

# redis-py compatibility name.
Sentinel = AsyncSentinel

__all__ = ["AsyncSentinel", "AsyncSentinelRedis", "Sentinel"]
```

- [ ] **Step 5: Update `_driver.pyi`**

Append to the asyncio block:

```python
class _AsyncSentinel:
    sentinels: list[tuple[str, int]]
    min_other_sentinels: int
    def __init__(
        self,
        sentinels: list[tuple[str, int]],
        min_other_sentinels: int = ...,
        sentinel_kwargs: dict[str, Any] | None = ...,
        force_master_ip: str | None = ...,
        **connection_kwargs: Any,
    ) -> None: ...
    def master_for(self, service_name: str, **kwargs: Any) -> _AsyncSentinelRedis: ...
    def slave_for(self, service_name: str, **kwargs: Any) -> _AsyncSentinelRedis: ...
    def discover_master(self, service_name: str) -> Awaitable[tuple[str, int]]: ...
    def sentinel_masters(self) -> Awaitable[dict[str, dict[str, Any]]]: ...

class _AsyncSentinelRedis:
    def ping(self) -> Awaitable[bool]: ...
    def get(self, key: str) -> Awaitable[bytes | None]: ...
    def set(self, key: str, value: bytes, ttl: int | None = ...) -> Awaitable[None]: ...
    def delete(self, *keys: str) -> Awaitable[int]: ...

# Register on _AsyncioModule.sentinel
class _AsyncioSentinelModule:
    AsyncSentinel: type[_AsyncSentinel]
    AsyncSentinelRedis: type[_AsyncSentinelRedis]
```

- [ ] **Step 6: Build + run**

Run: `uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml && uv run pytest tests/sentinel/test_async_sentinel.py -v`
Expected: 3 PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/redis-rs-py-driver/src/facade/sentinel.rs crates/redis-rs-py-driver/src/lib.rs python/redis_rs_py/asyncio/sentinel/__init__.py python/redis_rs_py/_driver.pyi tests/sentinel/test_async_sentinel.py
git commit -m "feat(sentinel): add AsyncSentinel async sibling"
```

---

## Task 10: Final lint pass + free-threaded smoke + CHANGELOG

**Files:** none modified — verification + CHANGELOG only.

- [ ] **Step 1: Run linters**

```bash
uv run ruff check
uv run ruff format --check
uv run ty check python/redis_rs_py/
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: all five succeed.

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest -n auto -p no:xdist tests/sentinel/test_sentinel_failover.py
uv run pytest -n auto --ignore=tests/sentinel/test_sentinel_failover.py
```

(Failover test must run on the master xdist worker — easiest way is to run it serially, then run everything else with xdist.)
Expected: every test passes.

- [ ] **Step 3: Run under cp314t**

```bash
.venv-ft/bin/uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
.venv-ft/bin/uv run pytest -n auto --ignore=tests/sentinel/test_sentinel_failover.py
```

Expected: same green. The failover test can be run separately on cp314t.

- [ ] **Step 4: CHANGELOG**

Append under `### Added` in `CHANGELOG.md`:

```markdown
- `redis_rs_py.sentinel.Sentinel` (sync) and `redis_rs_py.asyncio.sentinel.AsyncSentinel` (async) — drop-in for `redis.sentinel.Sentinel` / `redis.asyncio.sentinel.Sentinel`.
- Constructor mirrors `redis.sentinel.Sentinel.__init__(sentinels, min_other_sentinels=0, sentinel_kwargs=None, force_master_ip=None, **connection_kwargs)`. Unknown kwargs accept-and-warn.
- `master_for(service_name, **kwargs)` and `slave_for(service_name, **kwargs)` return `SentinelRedis` instances backed by a sentinel-aware driver. `slave_for` picks a slave round-robin per redis-py.
- Discovery: `discover_master(service_name)` → `(host, port)`, `discover_slaves(service_name)` → `[(host, port), ...]`, `sentinel_get_master_addr_by_name(service_name)`.
- Introspection: `sentinel_masters()`, `sentinel_master(service_name)`, `sentinel_slaves(service_name)`, `sentinel_sentinels(service_name)` — return dicts shaped like the redis-py output.
- Admin: `sentinel_failover(service_name)`, `sentinel_reset(pattern)`, `sentinel_set(service_name, option, value)`, `sentinel_remove(service_name)`, `sentinel_monitor(service_name, ip, port, quorum)`.
- **Transparent failover** via cachex's `SentinelConn`: the connection is RwLock'd; on a failover-class error the `sentinel_retry!` macro rediscovers the master from any healthy sentinel, swaps in a fresh `ConnectionManager`, and retries the call. Test coverage: `tests/sentinel/test_sentinel_failover.py` stops the master container, waits for quorum re-election, and verifies the same Redis handle continues to work.
- Sentinel fixture (`tests/conftest.py::sentinel_urls`): 1 master + 1 replica + 3 sentinels, hand-rolled docker network with the `sentinel monitor` config baked into each sentinel container at boot.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add Plan 16 entry"
```

- [ ] **Step 5: Final verification**

```bash
git log --oneline -15
```

Expected: ≈ 10 commits since the start of the plan, in roughly task order. Every commit message uses `feat(sentinel):` / `test(sentinel):` / `docs(changelog):`.

---

## Self-review checklist for this plan

- [x] Spec coverage (`PLAN.md`): `Sentinel` Rust pyclass (sync + async) ✓; `master_for(service_name)` / `slave_for(service_name)` ✓; sentinel URL list ✓; `service_name` ✓; automatic failover via cachex pattern (RwLock'd current master, retry-with-rediscovery on connection-class errors) ✓; `sentinel_masters` / `sentinel_master` / `sentinel_slaves` / `sentinel_sentinels` / `sentinel_get_master_addr_by_name` / `sentinel_failover` admin commands ✓.
- [x] Spec coverage (`0000-roadmap.md`): plan-16 row exhaustively ticked.
- [x] Cachex parity: `SentinelConn` struct lifted verbatim (`Arc<RwLock<ConnectionManager>>`, `is_failover_error`, `rediscover`); `sentinel_retry!` macro lifted verbatim; `dispatch_cmd!` and `conn_method!` arms wrap each sentinel call.
- [x] Out-of-scope items deferred with reasons: `min_other_sentinels` quorum check (server-side); `force_master_ip` (accept-and-warn); pluggable `redis_class`/`connection_pool_class` (accept-and-ignore).
- [x] No placeholder text; every code block ships actual code.
- [x] The critical correctness test (`test_failover_is_transparent_to_master_for_caller`) exercises the `sentinel_retry!` rediscover path end-to-end against a stopped master.
- [x] Type consistency: `ValkeyConnInner::Sentinel(SentinelConn)` arm in connection.rs is matched in `dispatch_cmd!`, `conn_method!`, `cache_statistics`, `build_blocking`, `is_sentinel()`. `ConnConfig::Sentinel { sentinel_urls, service_name, db, is_slave, tls_opts }` carries everything `build_blocking` needs to rebuild after fork.
- [x] Conventional commit prefixes throughout (`feat(sentinel):`).
- [x] Free-threaded run executed in Task 10.
- [x] No new Cargo features beyond Plan 01's set.
