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
