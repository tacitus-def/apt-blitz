#![allow(dead_code)]

use std::sync::Arc;

use apt_blitz::build_app;
use apt_blitz::config::Config;
use apt_blitz::proxy::AppState;
use reqwest::Client;
use sha2::Digest;
use tokio::sync::oneshot;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

pub struct MockUpstream {
    pub server: MockServer,
}

impl MockUpstream {
    pub async fn new() -> Self {
        let server = MockServer::start().await;
        Self { server }
    }

    pub fn uri(&self) -> String {
        self.server.uri()
    }

    pub async fn register_file(&self, name: &str, size: u64) {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let data = Arc::new(data);

        let d = Arc::clone(&data);
        Mock::given(method("HEAD"))
            .and(path(name))
            .respond_with(move |_: &Request| {
                ResponseTemplate::new(200)
                    .insert_header("content-length", d.len().to_string())
                    .insert_header("accept-ranges", "bytes")
                    .set_body_bytes(&[])
            })
            .mount(&self.server)
            .await;

        let d2 = Arc::clone(&data);
        Mock::given(method("GET"))
            .and(path(name))
            .respond_with(move |req: &Request| {
                if let Some(range) = req.headers.get("range") {
                    let range_str = range.to_str().unwrap_or("");
                    if let Some((start, end)) = parse_range(range_str, d2.len() as u64) {
                        let len = (end - start + 1) as usize;
                        let chunk = d2[start as usize..=end as usize].to_vec();
                        return ResponseTemplate::new(206)
                            .insert_header(
                                "content-range",
                                format!("bytes {start}-{end}/{}", d2.len()),
                            )
                            .insert_header("content-length", len.to_string())
                            .set_body_bytes(chunk);
                    }
                }
                ResponseTemplate::new(200)
                    .insert_header("content-length", d2.len().to_string())
                    .insert_header("accept-ranges", "bytes")
                    .set_body_bytes(d2.as_ref().clone())
            })
            .mount(&self.server)
            .await;
    }

    pub async fn register_file_with_delay(
        &self,
        name: &str,
        size: u64,
        delay: std::time::Duration,
    ) {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let data = Arc::new(data);

        let d = Arc::clone(&data);
        Mock::given(method("HEAD"))
            .and(path(name))
            .respond_with(move |_: &Request| {
                ResponseTemplate::new(200)
                    .insert_header("content-length", d.len().to_string())
                    .insert_header("accept-ranges", "bytes")
                    .set_delay(delay)
                    .set_body_bytes(&[])
            })
            .mount(&self.server)
            .await;

        let d2 = Arc::clone(&data);
        Mock::given(method("GET"))
            .and(path(name))
            .respond_with(move |req: &Request| {
                if let Some(range) = req.headers.get("range") {
                    let range_str = range.to_str().unwrap_or("");
                    if let Some((start, end)) = parse_range(range_str, d2.len() as u64) {
                        let len = (end - start + 1) as usize;
                        let chunk = d2[start as usize..=end as usize].to_vec();
                        return ResponseTemplate::new(206)
                            .insert_header(
                                "content-range",
                                format!("bytes {start}-{end}/{}", d2.len()),
                            )
                            .insert_header("content-length", len.to_string())
                            .set_delay(delay)
                            .set_body_bytes(chunk);
                    }
                }
                ResponseTemplate::new(200)
                    .insert_header("content-length", d2.len().to_string())
                    .insert_header("accept-ranges", "bytes")
                    .set_delay(delay)
                    .set_body_bytes(d2.as_ref().clone())
            })
            .mount(&self.server)
            .await;
    }

    pub async fn register_fail_after_bytes(
        &self,
        name: &str,
        size: u64,
        fail_threshold: u64,
    ) {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let data = Arc::new(data);

        let d = Arc::clone(&data);
        Mock::given(method("HEAD"))
            .and(path(name))
            .respond_with(move |_: &Request| {
                ResponseTemplate::new(200)
                    .insert_header("content-length", d.len().to_string())
                    .insert_header("accept-ranges", "bytes")
                    .set_body_bytes(&[])
            })
            .mount(&self.server)
            .await;

        let d2 = Arc::clone(&data);
        Mock::given(method("GET"))
            .and(path(name))
            .respond_with(move |req: &Request| {
                if let Some(range) = req.headers.get("range") {
                    let range_str = range.to_str().unwrap_or("");
                    if let Some((start, _)) = parse_range(range_str, d2.len() as u64) {
                        if start >= fail_threshold {
                            return ResponseTemplate::new(500);
                        }
                    }
                    if let Some((start, end)) = parse_range(range_str, d2.len() as u64) {
                        let len = (end - start + 1) as usize;
                        let chunk = d2[start as usize..=end as usize].to_vec();
                        return ResponseTemplate::new(206)
                            .insert_header(
                                "content-range",
                                format!("bytes {start}-{end}/{}", d2.len()),
                            )
                            .insert_header("content-length", len.to_string())
                            .set_body_bytes(chunk);
                    }
                }
                ResponseTemplate::new(500)
            })
            .mount(&self.server)
            .await;
    }

    pub async fn request_count(&self, name: &str) -> usize {
        self.server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|r| r.url.path() == name)
            .count()
    }
}

pub fn parse_range(range: &str, total: u64) -> Option<(u64, u64)> {
    let range = range.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = if end.is_empty() {
        total - 1
    } else {
        end.parse().ok()?
    };
    if start > end || end >= total {
        return None;
    }
    Some((start, end))
}

pub fn sha256_of(data: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

pub struct TestContext {
    pub upstream: MockUpstream,
    pub proxy_url: String,
    pub client: Client,
    shutdown: Option<oneshot::Sender<()>>,
    _cache_dir: tempfile::TempDir,
}

impl TestContext {
    pub async fn new(max_cache_size: u64) -> Self {
        let upstream = MockUpstream::new().await;

        let cache_dir = tempfile::tempdir().unwrap();
        let config = Config {
            port: 0,
            bind: "127.0.0.1".into(),
            connections: 4,
            cache_dir: cache_dir.path().join("cache"),
            max_cache_size,
            url_maps: vec![],
            upstream_proxy: None,
            no_proxy: vec![],
            max_connections_per_ip: 0,
            max_total_connections: 0,
            max_workers: 0,
            upstream_bandwidth: 0,
            per_ip_bandwidth: 0,
            coalesce_follower_timeout_secs: 50,
            coalesce_max_retries: 3,
        };

        let client = Client::builder()
            .user_agent("apt-blitz-test/0.1.0")
            .build()
            .unwrap();

        let state = AppState::from_config(config, client.clone());

        let app = build_app(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let proxy_url = format!("http://{addr}");

        let (tx, rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            let shutdown_signal = async {
                rx.await.ok();
            };
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal)
                .await
                .ok();
        });

        Self {
            upstream,
            proxy_url,
            client,
            shutdown: Some(tx),
            _cache_dir: cache_dir,
        }
    }

    pub async fn with_config(max_cache_size: u64, config: Config) -> Self {
        let upstream = MockUpstream::new().await;
        let cache_dir = tempfile::tempdir().unwrap();

        let mut config = config;
        config.cache_dir = cache_dir.path().join("cache");
        config.max_cache_size = max_cache_size;

        let client = Client::builder()
            .user_agent("apt-blitz-test/0.1.0")
            .build()
            .unwrap();

        let state = AppState::from_config(config, client.clone());

        let app = build_app(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let proxy_url = format!("http://{addr}");

        let (tx, rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            let shutdown_signal = async {
                rx.await.ok();
            };
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal)
                .await
                .ok();
        });

        Self {
            upstream,
            proxy_url,
            client,
            shutdown: Some(tx),
            _cache_dir: cache_dir,
        }
    }

    pub fn get(&self, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.upstream.uri(), path);
        self.client
            .get(format!("{}/{}", self.proxy_url, url))
    }

    pub async fn get_bytes(&self, path: &str) -> Vec<u8> {
        let resp = self.get(path).send().await.unwrap();
        let status = resp.status();
        let body = resp.bytes().await.unwrap();
        assert!(
            status.is_success(),
            "request to {path} failed: {status}, body: {}",
            String::from_utf8_lossy(&body[..body.len().min(200)])
        );
        body.to_vec()
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}
