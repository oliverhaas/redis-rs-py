// Connection wrappers and pool wiring.
//
// This is the "standard" half of django-vcache's connection.rs (MIT,
// David Burke / GlitchTip), via django-cachex-redis-rs. Cluster and
// Sentinel variants land in plans 15 and 16; they slot in as new
// `ValkeyConnInner` arms without changing the public API.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::caching::CacheConfig;
use redis::{Client, RedisResult};
use tokio::sync::OnceCell;

#[derive(Clone, Debug)]
pub struct TlsOpts {
    pub root_cert: Option<Vec<u8>>,
    pub client_cert: Option<Vec<u8>>,
    pub client_key: Option<Vec<u8>>,
}

impl TlsOpts {
    fn to_tls_certs(&self) -> redis::TlsCertificates {
        redis::TlsCertificates {
            root_cert: self.root_cert.clone(),
            client_tls: self.client_cert.as_ref().zip(self.client_key.as_ref()).map(
                |(cert, key)| redis::ClientTlsConfig {
                    client_cert: cert.clone(),
                    client_key: key.clone(),
                },
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientCacheOpts {
    pub max_size: usize,
    pub ttl_secs: u64,
}

// `url` and `tls_opts` are read by `build_blocking` (Plan 04 wires this up).
#[allow(dead_code)]
#[derive(Clone)]
enum ConnConfig {
    Standard {
        url: Arc<str>,
        tls_opts: Option<TlsOpts>,
    },
}

#[derive(Clone)]
pub enum ValkeyConnInner {
    Standard(ConnectionManager),
}

// `blocking` and `config` are consumed by `get_blocking` (Plan 04 wires it up
// through the `BLPOP`/`BLMOVE`/`BLMPOP` driver methods).
#[allow(dead_code)]
#[derive(Clone)]
pub struct ValkeyConn {
    regular: ValkeyConnInner,
    blocking: Arc<OnceCell<ValkeyConnInner>>,
    config: ConnConfig,
}

impl std::ops::Deref for ValkeyConn {
    type Target = ValkeyConnInner;
    fn deref(&self) -> &Self::Target {
        &self.regular
    }
}

impl std::ops::DerefMut for ValkeyConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.regular
    }
}

impl ValkeyConn {
    /// Lazily initialize a second connection for blocking commands so they
    /// don't head-of-line-block the multiplexed pipeline. Used by Plan 04.
    #[allow(dead_code)]
    pub async fn get_blocking(&self) -> RedisResult<ValkeyConnInner> {
        let conn = self
            .blocking
            .get_or_try_init(|| async { build_blocking(&self.config).await })
            .await?;
        Ok(conn.clone())
    }

    pub fn cache_statistics(&self) -> Option<redis::caching::CacheStatistics> {
        match &self.regular {
            ValkeyConnInner::Standard(c) => c.get_cache_statistics(),
        }
    }
}

// =========================================================================
// URL helpers
// =========================================================================

/// Force `protocol=resp3` on every URL so client-side caching works and
/// reply types are unified across topologies.
pub fn url_with_resp3(url: &str) -> String {
    if url.contains("protocol=") {
        return url.to_string();
    }
    let (base, fragment) = match url.split_once('#') {
        Some((b, f)) => (b, Some(f)),
        None => (url, None),
    };
    let sep = if base.contains('?') { '&' } else { '?' };
    let mut out = format!("{base}{sep}protocol=resp3");
    if let Some(f) = fragment {
        out.push('#');
        out.push_str(f);
    }
    out
}

// =========================================================================
// Constructors
// =========================================================================

fn create_client(url: &str, tls_opts: Option<&TlsOpts>) -> RedisResult<Client> {
    match tls_opts {
        Some(opts) => Client::build_with_tls(url, opts.to_tls_certs()),
        None => Client::open(url),
    }
}

fn conn_manager_config(cache: Option<&ClientCacheOpts>) -> ConnectionManagerConfig {
    let mut cfg = ConnectionManagerConfig::new()
        .set_pipeline_buffer_size(1000)
        .set_response_timeout(Some(Duration::from_secs(30)));
    if let Some(opts) = cache {
        let cc = CacheConfig::new()
            .set_size(NonZeroUsize::new(opts.max_size).unwrap_or(NonZeroUsize::MIN))
            .set_default_client_ttl(Duration::from_secs(opts.ttl_secs));
        cfg = cfg.set_cache_config(cc);
    }
    cfg
}

// Used by `build_blocking` (Plan 04).
#[allow(dead_code)]
fn blocking_conn_manager_config() -> ConnectionManagerConfig {
    ConnectionManagerConfig::new()
        .set_pipeline_buffer_size(1000)
        .set_response_timeout(None)
}

pub async fn connect_standard(
    url: &str,
    cache_opts: Option<ClientCacheOpts>,
    tls_opts: Option<TlsOpts>,
) -> Result<ValkeyConn, String> {
    let url = url_with_resp3(url);
    let client = create_client(&url, tls_opts.as_ref()).map_err(|e| e.to_string())?;
    let cfg = conn_manager_config(cache_opts.as_ref());
    let mgr = ConnectionManager::new_with_config(client, cfg)
        .await
        .map_err(|e| e.to_string())?;
    Ok(ValkeyConn {
        regular: ValkeyConnInner::Standard(mgr),
        blocking: Arc::new(OnceCell::new()),
        config: ConnConfig::Standard {
            url: Arc::from(url),
            tls_opts,
        },
    })
}

// Called by `ValkeyConn::get_blocking` (Plan 04).
#[allow(dead_code)]
async fn build_blocking(cfg: &ConnConfig) -> RedisResult<ValkeyConnInner> {
    match cfg {
        ConnConfig::Standard { url, tls_opts } => {
            let client = create_client(url, tls_opts.as_ref())?;
            let cfg = blocking_conn_manager_config();
            let mgr = ConnectionManager::new_with_config(client, cfg).await?;
            Ok(ValkeyConnInner::Standard(mgr))
        }
    }
}

// =========================================================================
// Dispatch macros
// =========================================================================

/// For commands that build a `redis::Cmd` by hand and call `.query_async`.
#[macro_export]
macro_rules! dispatch_cmd {
    ($self:expr, $cmd:expr) => {
        match $self {
            $crate::connection::ValkeyConnInner::Standard(c) => $cmd.query_async(c).await,
        }
    };
}

/// For commands that call a method on `redis::AsyncCommands`.
#[macro_export]
macro_rules! conn_method {
    ($self:expr, $c:ident, $op:expr) => {
        match $self {
            $crate::connection::ValkeyConnInner::Standard($c) => $op.await,
        }
    };
}

// =========================================================================
// Per-command async helpers on ValkeyConnInner
// =========================================================================

impl ValkeyConnInner {
    /// Build and dispatch a SET with the full redis-py option matrix.
    #[allow(clippy::too_many_arguments)]
    pub async fn set_full(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        nx: bool,
        xx: bool,
        keepttl: bool,
        get: bool,
    ) -> redis::RedisResult<redis::Value> {
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(value.as_slice());
        if let Some(s) = ex {
            cmd.arg("EX").arg(s);
        }
        if let Some(ms) = px {
            cmd.arg("PX").arg(ms);
        }
        if let Some(ts) = exat {
            cmd.arg("EXAT").arg(ts);
        }
        if let Some(ts) = pxat {
            cmd.arg("PXAT").arg(ts);
        }
        if keepttl {
            cmd.arg("KEEPTTL");
        }
        if nx {
            cmd.arg("NX");
        }
        if xx {
            cmd.arg("XX");
        }
        if get {
            cmd.arg("GET");
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn getex(
        &mut self,
        key: &str,
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<i64>,
        pxat: Option<i64>,
        persist: bool,
    ) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("GETEX");
        cmd.arg(key);
        if let Some(s) = ex {
            cmd.arg("EX").arg(s);
        }
        if let Some(ms) = px {
            cmd.arg("PX").arg(ms);
        }
        if let Some(ts) = exat {
            cmd.arg("EXAT").arg(ts);
        }
        if let Some(ts) = pxat {
            cmd.arg("PXAT").arg(ts);
        }
        if persist {
            cmd.arg("PERSIST");
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn getdel(&mut self, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("GETDEL");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn getrange(
        &mut self,
        key: &str,
        start: i64,
        end: i64,
    ) -> redis::RedisResult<Vec<u8>> {
        let mut cmd = redis::cmd("GETRANGE");
        cmd.arg(key).arg(start).arg(end);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn setrange(
        &mut self,
        key: &str,
        offset: i64,
        value: &[u8],
    ) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("SETRANGE");
        cmd.arg(key).arg(offset).arg(value);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn strlen(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("STRLEN");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn append(&mut self, key: &str, value: &[u8]) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("APPEND");
        cmd.arg(key).arg(value);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn mget(&mut self, keys: &[String]) -> redis::RedisResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut cmd = redis::cmd("MGET");
        for k in keys {
            cmd.arg(k.as_str());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn mset(&mut self, entries: &[(String, Vec<u8>)]) -> redis::RedisResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut cmd = redis::cmd("MSET");
        for (k, v) in entries {
            cmd.arg(k.as_str()).arg(v.as_slice());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn msetnx(&mut self, entries: &[(String, Vec<u8>)]) -> redis::RedisResult<bool> {
        if entries.is_empty() {
            return Ok(true);
        }
        let mut cmd = redis::cmd("MSETNX");
        for (k, v) in entries {
            cmd.arg(k.as_str()).arg(v.as_slice());
        }
        let r: i64 = crate::dispatch_cmd!(self, cmd)?;
        Ok(r == 1)
    }

    pub async fn incr(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("INCR");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn incrby(&mut self, key: &str, delta: i64) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("INCRBY");
        cmd.arg(key).arg(delta);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn incrbyfloat(&mut self, key: &str, delta: f64) -> redis::RedisResult<f64> {
        // Under RESP3, INCRBYFLOAT returns Value::Double, and redis-rs's
        // FromRedisValue<f64> handles it directly. Decoding via String first
        // forces an extra to_string()/parse round-trip with no benefit.
        let mut cmd = redis::cmd("INCRBYFLOAT");
        cmd.arg(key).arg(delta);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn decr(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("DECR");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn decrby(&mut self, key: &str, delta: i64) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("DECRBY");
        cmd.arg(key).arg(delta);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn exists_many(&mut self, keys: &[String]) -> redis::RedisResult<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("EXISTS");
        for k in keys {
            cmd.arg(k.as_str());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn unlink_many(&mut self, keys: &[String]) -> redis::RedisResult<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("UNLINK");
        for k in keys {
            cmd.arg(k.as_str());
        }
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn expire_full(
        &mut self,
        key: &str,
        seconds: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("EXPIRE");
        cmd.arg(key).arg(seconds);
        append_expire_flag(&mut cmd, nx, xx, gt, lt);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn pexpire_full(
        &mut self,
        key: &str,
        milliseconds: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("PEXPIRE");
        cmd.arg(key).arg(milliseconds);
        append_expire_flag(&mut cmd, nx, xx, gt, lt);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn expireat_full(
        &mut self,
        key: &str,
        ts_seconds: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("EXPIREAT");
        cmd.arg(key).arg(ts_seconds);
        append_expire_flag(&mut cmd, nx, xx, gt, lt);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn pexpireat_full(
        &mut self,
        key: &str,
        ts_milliseconds: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("PEXPIREAT");
        cmd.arg(key).arg(ts_milliseconds);
        append_expire_flag(&mut cmd, nx, xx, gt, lt);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn ttl(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("TTL");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn pttl(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("PTTL");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn expiretime(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("EXPIRETIME");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn pexpiretime(&mut self, key: &str) -> redis::RedisResult<i64> {
        let mut cmd = redis::cmd("PEXPIRETIME");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn persist(&mut self, key: &str) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("PERSIST");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn rename(&mut self, src: &str, dst: &str) -> redis::RedisResult<()> {
        let mut cmd = redis::cmd("RENAME");
        cmd.arg(src).arg(dst);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn renamenx(&mut self, src: &str, dst: &str) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("RENAMENX");
        cmd.arg(src).arg(dst);
        let r: i64 = crate::dispatch_cmd!(self, cmd)?;
        Ok(r == 1)
    }

    pub async fn key_type(&mut self, key: &str) -> redis::RedisResult<String> {
        let mut cmd = redis::cmd("TYPE");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    pub async fn copy(
        &mut self,
        src: &str,
        dst: &str,
        db: Option<i64>,
        replace: bool,
    ) -> redis::RedisResult<bool> {
        let mut cmd = redis::cmd("COPY");
        cmd.arg(src).arg(dst);
        if let Some(d) = db {
            cmd.arg("DB").arg(d);
        }
        if replace {
            cmd.arg("REPLACE");
        }
        let r: i64 = crate::dispatch_cmd!(self, cmd)?;
        Ok(r == 1)
    }

    pub async fn dump(&mut self, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
        let mut cmd = redis::cmd("DUMP");
        cmd.arg(key);
        crate::dispatch_cmd!(self, cmd)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn restore(
        &mut self,
        key: &str,
        ttl_ms: i64,
        serialized: &[u8],
        replace: bool,
        absttl: bool,
        idletime: Option<u64>,
        frequency: Option<u64>,
    ) -> redis::RedisResult<()> {
        let mut cmd = redis::cmd("RESTORE");
        cmd.arg(key).arg(ttl_ms).arg(serialized);
        if replace {
            cmd.arg("REPLACE");
        }
        if absttl {
            cmd.arg("ABSTTL");
        }
        if let Some(it) = idletime {
            cmd.arg("IDLETIME").arg(it);
        }
        if let Some(f) = frequency {
            cmd.arg("FREQ").arg(f);
        }
        crate::dispatch_cmd!(self, cmd)
    }
}

fn append_expire_flag(cmd: &mut redis::Cmd, nx: bool, xx: bool, gt: bool, lt: bool) {
    if nx {
        cmd.arg("NX");
    } else if xx {
        cmd.arg("XX");
    } else if gt {
        cmd.arg("GT");
    } else if lt {
        cmd.arg("LT");
    }
}
