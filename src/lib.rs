#![deny(unsafe_code)]

#[cfg(not(unix))]
compile_error!("apt-blitz currently supports Unix only (uses FileExt::write_at/read_at)");

pub mod buffer;
pub mod cache;
pub mod coalescer;
pub mod config;
pub mod downloader;
pub mod ftp;
pub mod proxy;
pub mod rate_limit;

use std::sync::Arc;

use axum::Router;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper_util::rt::tokio::TokioIo;
use reqwest::Client;
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tower_service::Service;
use tracing::{debug, error, info, warn};

use crate::cache::Cache;
use crate::coalescer::Coalescer;
use crate::config::{Config, ProxyType};
use crate::proxy::{handle_connect_tunnel, handle_proxy, AppState};
use crate::rate_limit::{IpRateLimiter, TokenBucket, WorkerLimiter};
use tokio_util::sync::CancellationToken;

pub async fn run_proxy(config: Config) -> anyhow::Result<()> {
    info!(%config, "starting apt-blitz");

    let config = Arc::new(config);

    let mut client_builder = Client::builder()
        .user_agent("apt-blitz/0.1.0");

    if let Some(ref proxy_cfg) = config.upstream_proxy {
        let proxy_url = match proxy_cfg.proxy_type {
            ProxyType::Http | ProxyType::Https => {
                format!("http://{}:{}", proxy_cfg.host, proxy_cfg.port)
            }
            ProxyType::Socks5 => {
                format!("socks5://{}:{}", proxy_cfg.host, proxy_cfg.port)
            }
        };
        let mut proxy = reqwest::Proxy::all(&proxy_url)?;
        if let (Some(user), Some(pass)) = (&proxy_cfg.username, &proxy_cfg.password) {
            proxy = proxy.basic_auth(user, pass);
        }
        for no_host in &config.no_proxy {
            if let Some(no) = reqwest::NoProxy::from_string(no_host) {
                proxy = proxy.no_proxy(Some(no));
            }
        }
        client_builder = client_builder.proxy(proxy);
    }

    let client = client_builder.build()?;

    let cache = Cache::new(config.cache_dir.clone(), config.max_cache_size)?;
    if config.max_cache_size == 0 {
        warn!("PROXY_MAX_CACHE_SIZE=0: cache eviction will happen after every store");
    }

    let temp_dir = config.cache_dir.join("tmp");
    tokio::fs::create_dir_all(&temp_dir).await?;
    let mut dir = tokio::fs::read_dir(&temp_dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "download") {
            tokio::fs::remove_file(&path).await.ok();
        }
    }

    let addr = format!("{}:{}", config.bind, config.port);

    let state = AppState {
        client,
        config: config.clone(),
        cache,
        coalescer: Arc::new(Coalescer::new()),
        temp_dir,
        ip_limiter: {
            let limiter = Arc::new(IpRateLimiter::new(
                config.max_connections_per_ip,
                config.max_total_connections,
                config.per_ip_bandwidth,
            ));
            limiter.start_sweep();
            limiter
        },
        worker_limiter: Arc::new(WorkerLimiter::new(config.max_workers)),
        upstream_bucket: Arc::new(if config.upstream_bandwidth == 0 {
            TokenBucket::unlimited()
        } else {
            TokenBucket::new(config.upstream_bandwidth, config.upstream_bandwidth)
        }),
        cancel: CancellationToken::new(),
    };

    let router = build_app(state);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "listening");

    let mut join_set = JoinSet::new();

    loop {
        tokio::select! {
            biased;

            _ = tokio::signal::ctrl_c() => {
                info!("shutting down, waiting for {} active connections", join_set.len());
                break;
            }

            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        error!("accept error: {e}");
                        continue;
                    }
                };
                let router = router.clone();
                let cfg = config.clone();
                let peer_addr = peer;
                join_set.spawn(async move {
                    handle_connection(stream, router, cfg, peer_addr).await;
                });
            }
        }
    }

    while join_set.join_next().await.is_some() {}
    info!("all connections finished");
    Ok(())
}

pub fn build_app(state: AppState) -> Router {
    use axum::routing::get;
    Router::new()
        .route("/{*url}", get(handle_proxy))
        .with_state(state)
}

async fn handle_connection(
    stream: TcpStream,
    router: Router,
    config: Arc<Config>,
    peer_addr: std::net::SocketAddr,
) {
    let mut peek_buf = [0u8; 7];
    match stream.peek(&mut peek_buf).await {
        Ok(n) if n >= 7 && &peek_buf[..7] == b"CONNECT" => {
            handle_connect_tunnel(
                stream,
                config.upstream_proxy.as_ref(),
                &config.no_proxy,
            ).await;
            return;
        }
        _ => {}
    }

    let svc = hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
        let mut router = router.clone();
        let peer = peer_addr;
        async move {
            let (parts, incoming) = req.into_parts();
            let stream = incoming
                .into_data_stream()
                .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
            let body = axum::body::Body::from_stream(stream);
            let mut axum_req = axum::http::Request::from_parts(parts, body);
            axum_req.extensions_mut().insert(peer);
            router.call(axum_req).await
        }
    });

    let io = TokioIo::new(stream);
    if let Err(err) = hyper::server::conn::http1::Builder::new()
        .preserve_header_case(true)
        .title_case_headers(true)
        .serve_connection(io, svc)
        .await
    {
        debug!("HTTP connection error: {err}");
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::sync::Arc;
    use crate::buffer::SegmentsBuffer;

    pub fn create_temp_buffer(size: u64) -> Arc<SegmentsBuffer> {
        let dir = std::env::temp_dir().join("apt-blitz-test-buffer");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.download", uuid::Uuid::new_v4()));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.set_len(size).unwrap();
        let (buffer, _) = SegmentsBuffer::new(size, file, path);
        buffer
    }
}
