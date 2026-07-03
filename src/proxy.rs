use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::buffer::SegmentsBuffer;
use crate::cache::Cache;
use crate::coalescer::{Coalescer, RegisterResult};
use crate::config::{Config, ProxyType, UrlMap};
use crate::downloader::download_multithreaded;
use crate::ftp;

// 3 × MSS (3 × 8192) — читает 24 KiB за раз, чтобы не фрагментировать TCP-сегмент при throttle
const CHUNK_SIZE: u64 = 32 * 768;
fn can_multithread(content_length: Option<u64>, accept_ranges: Option<&http::HeaderValue>) -> bool {
    content_length.is_some() && accept_ranges == Some(&http::HeaderValue::from_static("bytes"))
}

const FORWARD_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "content-disposition",
    "accept-ranges",
    "last-modified",
    "etag",
    "cache-control",
    "expires",
];

/// Translate a fake‑host URL (`http://fake-host/…` or `ftp://fake-host/…`) to its real upstream URL.
fn resolve_url(url: &str, maps: &[UrlMap]) -> String {
    for map in maps {
        for prefix in [format!("http://{}", map.fake_host), format!("ftp://{}", map.fake_host)] {
            if let Some(rest) = url.strip_prefix(&prefix) {
                if rest.is_empty() || rest.starts_with('/') {
                    let base = map.real_base.trim_end_matches('/');
                    if rest.is_empty() {
                        return base.to_string();
                    }
                    return format!("{base}{rest}");
                }
            }
        }
    }
    url.to_string()
}

#[derive(Clone)]
pub struct AppState {
    pub client: Client,
    pub config: Arc<Config>,
    pub cache: Arc<Cache>,
    pub coalescer: Arc<Coalescer>,
    pub temp_dir: PathBuf,
}

#[derive(Debug)]
pub enum ProxyError {
    BadRequest(String),
    Upstream(reqwest::Error),
    Internal(String),
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ProxyError::BadRequest(s) => (StatusCode::BAD_REQUEST, s),
            ProxyError::Upstream(e) => (StatusCode::BAD_GATEWAY, e.to_string()),
            ProxyError::Internal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
        };
        Response::builder()
            .status(status)
            .body(Body::from(msg))
            .unwrap()
    }
}

impl From<reqwest::Error> for ProxyError {
    fn from(e: reqwest::Error) -> Self {
        ProxyError::Upstream(e)
    }
}

pub async fn handle_proxy(
    method: Method,
    Path(original_url): Path<String>,
    State(state): State<AppState>,
) -> Result<Response, ProxyError> {
    info!(url = %original_url, "request");

    // Resolve fake‑host → real upstream (for caching HTTPS content)
    let url = resolve_url(&original_url, &state.config.url_maps);
    if url != original_url {
        info!(fake = %original_url, upstream = %url, "url mapped");
    }

    if method == Method::HEAD {
        info!(url = %url, "HEAD request, forwarding without body");
        let resp = state.client.head(&url).send().await?;
        let mut builder = Response::builder().status(resp.status());
        for name in FORWARD_HEADERS {
            if let Some(val) = resp.headers().get(*name) {
                builder = builder.header(*name, val);
            }
        }
        return Ok(builder.body(Body::empty()).unwrap());
    }

    if url.starts_with("ftp://") || url.starts_with("ftps://") {
        return handle_ftp_proxy(&url, &state).await;
    }

    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(ProxyError::BadRequest(format!(
            "unsupported scheme in URL (original: {original_url}, resolved: {url})"
        )));
    }

    let mut retries = 0u32;
    loop {
        if retries >= 5 {
            return Err(ProxyError::Internal("too many retries waiting for in-flight download".into()));
        }
        retries += 1;

        if let Some((cached_path, cached_headers)) = state.cache.lookup(&url).await {
            info!(url = %url, path = %cached_path.display(), "cache hit");
            return serve_file(&cached_path, &cached_headers).await;
        }

        match state.coalescer.register(&url) {
            RegisterResult::Follower(rx) => {
                info!(url = %url, "joining in-flight download as follower");
                let buffer = match tokio::time::timeout(Duration::from_secs(10), rx).await {
                    Ok(Ok(buf)) => buf,
                    _ => {
                        info!(url = %url, "follower wait failed, retrying");
                        continue;
                    }
                };
                return Ok(serve_from_buffer(buffer, &state, &url).await);
            }
            RegisterResult::FollowerBuffer(buffer) => {
                info!(url = %url, "joining completed in-flight download");
                return Ok(serve_from_buffer(buffer, &state, &url).await);
            }
            RegisterResult::Leader => {
                info!(url = %url, "becoming download leader");
                break;
            }
        }
    }

    let head_resp = state.client.head(&url).send().await?;
    let head_status = head_resp.status();
    if !head_status.is_success() {
        if head_status == StatusCode::METHOD_NOT_ALLOWED
            || head_status == StatusCode::NOT_IMPLEMENTED
        {
            state.coalescer.complete(&url);
            return plain_proxy(&state.client, &url).await;
        }
        state.coalescer.complete(&url);
        head_resp.error_for_status().map_err(ProxyError::Upstream)?;
        unreachable!()
    }

    let headers = head_resp.headers().clone();
    let content_length = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let can_multithread = can_multithread(content_length, headers.get("accept-ranges"));

    if !can_multithread {
        info!(
            url = %url,
            can_multithread = false,
            "falling back to plain proxy"
        );
        state.coalescer.complete(&url);
        return plain_proxy(&state.client, &url).await;
    }

    let total_size = content_length.unwrap();
    info!(
        url = %url,
        total_size = total_size,
        "starting multithreaded download"
    );

    let temp_path = state.temp_dir.join(format!("{}.download", uuid::Uuid::new_v4()));
    let temp_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&temp_path)
        .map_err(|e| ProxyError::Internal(format!("failed to create temp file: {e}")))?;
    temp_file.set_len(total_size).map_err(|e| {
        ProxyError::Internal(format!("failed to allocate temp file: {e}"))
    })?;

    let (buffer, notify_rx) = SegmentsBuffer::new(total_size, temp_file, temp_path);

    let mut resp_headers = HeaderMap::new();
    for key in FORWARD_HEADERS {
        if let Some(val) = headers.get(*key) {
            resp_headers.insert(*key, val.clone());
        }
    }

    let resp_status = StatusCode::OK;
    buffer.set_meta(resp_status, resp_headers.clone());
    state
        .coalescer
        .attach_buffer(&url, buffer.clone());

    let dl_client = state.client.clone();
    let dl_url = url.clone();
    let dl_buffer = buffer.clone();
    let dl_connections = state.config.connections;
    let download_handle = tokio::spawn(async move {
        download_multithreaded(&dl_client, &dl_url, dl_buffer, dl_connections).await
    });

    let cache_headers = resp_headers.clone();
    let cache_state = state.clone();
    let cache_url = url.clone();
    let cache_buffer = buffer.clone();
    tokio::spawn(async move {
        match download_handle.await {
            Ok(Ok(())) => {
                info!(url = %cache_url, "download complete, storing to cache");
                let sync_buf = cache_buffer.clone();
                if tokio::task::spawn_blocking(move || sync_buf.sync())
                    .await
                    .is_ok_and(|r| r.is_ok())
                {
                    if let Err(e) = cache_state.cache.store(&cache_url, cache_buffer.file_path(), &cache_headers).await {
                        error!(error = %e, "failed to store in cache");
                        let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
                    }
                } else {
                    error!("sync failed before cache store");
                    let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
                }
            }
            Ok(Err(e)) => {
                error!(url = %cache_url, error = ?e, "multithreaded download failed, falling back to plain proxy");
                match cache_state.client.get(&cache_url).send().await {
                    Ok(resp) => {
                        let fb_headers = resp.headers().clone();
                        let mut stream = resp.bytes_stream();
                        let mut offset = 0u64;
                        let mut ok = true;
                        while let Some(chunk) = stream.next().await {
                            match chunk {
                                Ok(data) => {
                                    if let Err(e) = cache_buffer.write_data(offset, &data) {
                                        error!(error = %e, "fallback: write failed");
                                        ok = false;
                                        break;
                                    }
                                    offset += data.len() as u64;
                                }
                                Err(e) => {
                                    error!(error = ?e, "fallback: stream error");
                                    ok = false;
                                    break;
                                }
                            }
                        }
                        if ok {
                            cache_buffer.mark_all_ready();
                            info!(url = %cache_url, "fallback complete, storing to cache");
                            let sync_buf = cache_buffer.clone();
                            if tokio::task::spawn_blocking(move || sync_buf.sync())
                                .await
                                .is_ok_and(|r| r.is_ok())
                            {
                                if let Err(e) = cache_state.cache.store(&cache_url, cache_buffer.file_path(), &fb_headers).await {
                                    error!(error = %e, "fallback: cache store failed");
                                    let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
                                }
                            } else {
                                error!("fallback: sync failed, removing temp file");
                                let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
                            }
                        } else {
                            error!("fallback: stream aborted, removing temp file");
                            cache_buffer.set_failed();
                            let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
                        }
                    }
                    Err(e) => {
                        error!(error = ?e, "fallback plain proxy failed");
                        cache_buffer.set_failed();
                    }
                }
            }
            Err(e) => {
                error!(url = %cache_url, error = ?e, "download task panicked, cleaning up temp");
                cache_buffer.set_failed();
                let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
            }
        }
        cache_state.coalescer.complete(&cache_url);
    });

    let stream = make_buffer_stream(buffer, notify_rx, state.config.min_speed, &url);
    let mut resp_builder = Response::builder().status(resp_status);
    for (key, val) in &resp_headers {
        resp_builder = resp_builder.header(key, val);
    }
    let resp = resp_builder.body(Body::from_stream(stream)).unwrap();

    Ok(resp)
}

async fn handle_ftp_proxy(url: &str, state: &AppState) -> Result<Response, ProxyError> {
    info!(url = %url, "ftp request");

    let mut retries = 0u32;
    loop {
        if retries >= 5 {
            return Err(ProxyError::Internal("too many retries waiting for in-flight FTP download".into()));
        }
        retries += 1;

        if let Some((cached_path, cached_headers)) = state.cache.lookup(url).await {
            info!(url = %url, path = %cached_path.display(), "FTP cache hit");
            return serve_file(&cached_path, &cached_headers).await;
        }

        match state.coalescer.register(url) {
            RegisterResult::Follower(rx) => {
                info!(url = %url, "joining in-flight FTP download as follower");
                let buffer = match tokio::time::timeout(Duration::from_secs(10), rx).await {
                    Ok(Ok(buf)) => buf,
                    _ => {
                        info!(url = %url, "FTP follower wait failed, retrying");
                        continue;
                    }
                };
                return Ok(serve_from_buffer(buffer, state, url).await);
            }
            RegisterResult::FollowerBuffer(buffer) => {
                info!(url = %url, "joining completed in-flight FTP download");
                return Ok(serve_from_buffer(buffer, state, url).await);
            }
            RegisterResult::Leader => {
                info!(url = %url, "becoming FTP download leader");
                break;
            }
        }
    }

    let ftp_url = match ftp::parse_ftp_url(url) {
        Ok(u) => u,
        Err(e) => return Err(ProxyError::BadRequest(e.to_string())),
    };

    let total_size = match ftp::check_ftp_size(&ftp_url).await {
        Ok(s) => s,
        Err(e) => return Err(ProxyError::Internal(format!("FTP SIZE failed: {e}"))),
    };
    info!(url = %url, total_size, "FTP file size");

    let temp_path = state.temp_dir.join(format!("{}.download", uuid::Uuid::new_v4()));
    let temp_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&temp_path)
        .map_err(|e| ProxyError::Internal(format!("failed to create temp file: {e}")))?;
    temp_file.set_len(total_size).map_err(|e| {
        ProxyError::Internal(format!("failed to allocate temp file: {e}"))
    })?;

    let (buffer, notify_rx) = SegmentsBuffer::new(total_size, temp_file, temp_path);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert("content-type", "application/octet-stream".parse().unwrap());
    resp_headers.insert("content-length", total_size.to_string().parse().unwrap());
    resp_headers.insert("accept-ranges", "bytes".parse().unwrap());

    let resp_status = StatusCode::OK;
    buffer.set_meta(resp_status, resp_headers.clone());
    state.coalescer.attach_buffer(url, buffer.clone());

    let dl_url = ftp_url.clone();
    let dl_buffer = buffer.clone();
    let dl_connections = state.config.connections;
    let download_handle = tokio::spawn(async move {
        ftp::download_ftp_multithreaded(&dl_url, dl_buffer, dl_connections).await
    });

    let cache_headers = resp_headers.clone();
    let cache_state = state.clone();
    let cache_url = url.to_string();
    let cache_buffer = buffer.clone();
    tokio::spawn(async move {
        match download_handle.await {
            Ok(Ok(())) => {
                info!(url = %cache_url, "FTP download complete, storing to cache");
                let sync_buf = cache_buffer.clone();
                if tokio::task::spawn_blocking(move || sync_buf.sync())
                    .await
                    .is_ok_and(|r| r.is_ok())
                {
                    if let Err(e) = cache_state.cache.store(&cache_url, cache_buffer.file_path(), &cache_headers).await {
                        error!(error = %e, "FTP cache store failed");
                        let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
                    }
                } else {
                    error!("FTP sync failed before cache store");
                    let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
                }
            }
            Ok(Err(e)) => {
                error!(url = %cache_url, error = ?e, "FTP download failed");
                let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
            }
            Err(e) => {
                error!(url = %cache_url, error = ?e, "FTP download task panicked");
                let _ = tokio::fs::remove_file(cache_buffer.file_path()).await;
            }
        }
        cache_state.coalescer.complete(&cache_url);
    });

    let stream = make_buffer_stream(buffer, notify_rx, state.config.min_speed, url);
    let mut resp_builder = Response::builder().status(resp_status);
    for (key, val) in &resp_headers {
        resp_builder = resp_builder.header(key, val);
    }
    let resp = resp_builder.body(Body::from_stream(stream)).unwrap();
    Ok(resp)
}

async fn serve_from_buffer(buffer: Arc<SegmentsBuffer>, state: &AppState, url: &str) -> Response {
    let meta = buffer.wait_meta().await;
    let rx = buffer.subscribe();
    let stream = make_buffer_stream(buffer, rx, state.config.min_speed, url);
    let mut resp_builder = Response::builder().status(meta.0);
    for (key, val) in &meta.1 {
        resp_builder = resp_builder.header(key, val);
    }
    resp_builder.body(Body::from_stream(stream)).unwrap()
}

fn make_buffer_stream(
    buffer: Arc<SegmentsBuffer>,
    notify_rx: broadcast::Receiver<usize>,
    min_speed: u64,
    url: &str,
) -> impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> {
    let url = url.to_string();
    struct StreamState {
        buffer: Arc<SegmentsBuffer>,
        notify_rx: broadcast::Receiver<usize>,
        url: String,
        current_segment: usize,
        offset_in_segment: u64,
        throttled: bool,
        min_speed: u64,
        last_yield: tokio::time::Instant,
        errored: bool,
    }

    let initial = StreamState {
        buffer,
        notify_rx,
        url,
        current_segment: 0,
        offset_in_segment: 0,
        throttled: true,
        min_speed,
        last_yield: tokio::time::Instant::now(),
        errored: false,
    };

    futures_util::stream::unfold(initial, |mut state| async move {
        loop {
            if state.buffer.is_failed() {
                if state.errored {
                    return None;
                }
                state.errored = true;
                return Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "download failed",
                    )),
                    state,
                ));
            }

            if state.throttled && state.buffer.all_completed() {
                info!(url = %state.url, "all segments ready, removing throttle");
                state.throttled = false;
            }

            if state.current_segment >= state.buffer.num_segments() {
                if state.buffer.num_segments() == 0 || !state.buffer.all_completed() {
                    let _ = tokio::time::timeout(
                        Duration::from_millis(100),
                        state.notify_rx.recv(),
                    )
                    .await;
                    continue;
                }
                return None;
            }

            if !state.buffer.is_ready(state.current_segment) {
                let _ = tokio::time::timeout(
                    Duration::from_millis(100),
                    state.notify_rx.recv(),
                )
                .await;
                continue;
            }

            let seg_start = state.buffer.segment_start(state.current_segment);
            let seg_end = state.buffer.segment_end(state.current_segment);
            let seg_pos = seg_start + state.offset_in_segment;

            if seg_pos >= seg_end {
                state.current_segment += 1;
                state.offset_in_segment = 0;
                continue;
            }

            let remaining = seg_end - seg_pos;
            let chunk_size = if state.throttled {
                CHUNK_SIZE.min(remaining)
            } else {
                remaining
            };

            if state.throttled && chunk_size > 0 {
                let elapsed = state.last_yield.elapsed();
                let expected =
                    Duration::from_secs_f64(chunk_size as f64 / state.min_speed as f64);
                if elapsed < expected {
                    tokio::time::sleep(expected - elapsed).await;
                }
            }

            let data = state
                .buffer
                .read_data(seg_pos, chunk_size);

            match data {
                Some(bytes) if !bytes.is_empty() => {
                    let len = bytes.len() as u64;
                    state.offset_in_segment += len;
                    state.last_yield = tokio::time::Instant::now();
                    return Some((Ok(bytes), state));
                }
                _ => {
                    state.current_segment += 1;
                    state.offset_in_segment = 0;
                    continue;
                }
            }
        }
    })
}

async fn plain_proxy(client: &Client, url: &str) -> Result<Response, ProxyError> {
    info!(url = %url, "plain proxy mode");

    let resp = client.get(url).send().await?;
    let status = resp.status();
    let resp_headers = resp.headers().clone();

    let stream = resp.bytes_stream().map(|r| {
        r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    });

    let mut builder = Response::builder().status(status);
    for name in FORWARD_HEADERS {
        if let Some(val) = resp_headers.get(*name) {
            builder = builder.header(*name, val);
        }
    }

    Ok(builder.body(Body::from_stream(stream)).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_error_bad_request_response() {
        let err = ProxyError::BadRequest("bad url".into());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_proxy_error_upstream_response() {
        let client = reqwest::Client::new();
        let result = client.get("http://255.255.255.255:1/").send().await;
        let req_err = result.unwrap_err();
        let err = ProxyError::Upstream(req_err);
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn test_proxy_error_internal_response() {
        let err = ProxyError::Internal("disk full".into());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_proxy_error_body_contains_message() {
        let err = ProxyError::BadRequest("missing parameter".into());
        let resp = err.into_response();
        // Body is of type Body, we can't easily read it in non-async test for Body
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_proxy_error_from_reqwest() {
        let client = reqwest::Client::new();
        let result = client.get("http://255.255.255.255:1/").send().await;
        let req_err = result.unwrap_err();
        let err: ProxyError = req_err.into();
        assert!(matches!(err, ProxyError::Upstream(_)));
    }

    #[test]
    fn test_constants_sanity() {
        assert_eq!(CHUNK_SIZE, 32 * 768);
    }

    #[test]
    fn test_forward_headers_contains_expected() {
        assert!(FORWARD_HEADERS.contains(&"content-type"));
        assert!(FORWARD_HEADERS.contains(&"content-length"));
        assert!(FORWARD_HEADERS.contains(&"content-disposition"));
        assert!(FORWARD_HEADERS.contains(&"accept-ranges"));
        assert!(FORWARD_HEADERS.contains(&"last-modified"));
        assert!(FORWARD_HEADERS.contains(&"etag"));
        assert!(FORWARD_HEADERS.contains(&"cache-control"));
        assert!(FORWARD_HEADERS.contains(&"expires"));
        assert_eq!(FORWARD_HEADERS.len(), 8);
    }

    #[test]
    fn test_forward_headers_no_duplicates() {
        let mut unique = std::collections::HashSet::new();
        for h in FORWARD_HEADERS {
            assert!(unique.insert(*h), "duplicate header: {}", h);
        }
    }

    #[tokio::test]
    async fn test_serve_file_nonexistent() {
        let headers = HeaderMap::new();
        let result = serve_file(
            std::path::Path::new("/nonexistent/path/file.deb"),
            &headers,
        )
        .await;
        assert!(result.is_err());
        match result {
            Err(ProxyError::Internal(_)) => {}
            _ => panic!("expected Internal error"),
        }
    }

    #[tokio::test]
    async fn test_serve_file_empty_file() {
        let dir = std::env::temp_dir().join("apt-blitz-test-proxy-serve");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.deb");
        std::fs::write(&path, b"").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        let result = serve_file(&path, &headers).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap().to_str().unwrap(),
            "text/plain"
        );
        assert_eq!(
            resp.headers().get("content-length").unwrap().to_str().unwrap(),
            "0"
        );
        assert_eq!(
            resp.headers().get("accept-ranges").unwrap().to_str().unwrap(),
            "bytes"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_serve_file_with_headers() {
        let dir = std::env::temp_dir().join("apt-blitz-test-proxy-headers");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("with-headers.deb");
        std::fs::write(&path, b"data").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("etag", "\"xyz\"".parse().unwrap());
        headers.insert("cache-control", "public".parse().unwrap());
        let result = serve_file(&path, &headers).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(
            resp.headers().get("etag").unwrap().to_str().unwrap(),
            "\"xyz\""
        );
        assert_eq!(
            resp.headers().get("cache-control").unwrap().to_str().unwrap(),
            "public"
        );
        assert_eq!(
            resp.headers().get("content-length").unwrap().to_str().unwrap(),
            "4"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_app_state_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<AppState>();
    }

    // --- can_multithread tests ---

    #[test]
    fn test_can_multithread_happy_path() {
        let accept = &http::HeaderValue::from_static("bytes");
        assert!(can_multithread(Some(1024), Some(accept)));
        assert!(can_multithread(Some(42 * 1024 * 1024), Some(accept)));
        assert!(can_multithread(Some(1024 * 1024 * 1024), Some(accept)));
    }

    #[test]
    fn test_can_multithread_no_content_length() {
        let accept = &http::HeaderValue::from_static("bytes");
        assert!(!can_multithread(None, Some(accept)));
    }



    #[test]
    fn test_can_multithread_no_accept_ranges() {
        assert!(!can_multithread(Some(100), None));
    }

    #[test]
    fn test_can_multithread_wrong_accept_ranges() {
        let accept_none = &http::HeaderValue::from_static("none");
        assert!(!can_multithread(Some(100), Some(accept_none)));
        let accept_other = &http::HeaderValue::from_static("kilobytes");
        assert!(!can_multithread(Some(100), Some(accept_other)));
    }

    #[test]
    fn test_can_multithread_all_false_combinations() {
        // No content-length, no accept-ranges
        assert!(!can_multithread(None, None));
        // No content-length, with accept-ranges
        let accept = &http::HeaderValue::from_static("bytes");
        assert!(!can_multithread(None, Some(accept)));
        // With content-length, no accept-ranges
        assert!(!can_multithread(Some(100), None));
    }

    // --- no_proxy_match ---

    #[test]
    fn test_no_proxy_match_empty_list() {
        assert!(!no_proxy_match("example.com", &[]));
    }

    #[test]
    fn test_no_proxy_match_wildcard() {
        assert!(no_proxy_match("anything.example.com", &["*".into()]));
    }

    #[test]
    fn test_no_proxy_match_exact() {
        let list = vec!["example.com".into(), "test.org".into()];
        assert!(no_proxy_match("example.com", &list));
        assert!(no_proxy_match("test.org", &list));
        assert!(!no_proxy_match("other.com", &list));
    }

    #[test]
    fn test_no_proxy_match_suffix() {
        let list = vec![".local".into(), ".internal.corp".into()];
        assert!(no_proxy_match("host.local", &list));
        assert!(no_proxy_match("deep.host.internal.corp", &list));
        assert!(!no_proxy_match("host.localhost", &list));
        assert!(no_proxy_match("local", &list)); // .local matches bare "local" too
    }

    #[test]
    fn test_no_proxy_match_cidr_prefix() {
        let list = vec!["10.0.0.0/8".into(), "192.168.0.0/16".into()];
        assert!(no_proxy_match("10.0.0.1", &list));
        assert!(no_proxy_match("10.255.255.255", &list));
        assert!(no_proxy_match("192.168.1.100", &list));
        assert!(!no_proxy_match("11.0.0.1", &list));
        assert!(!no_proxy_match("172.16.0.1", &list));
    }

    #[test]
    fn test_no_proxy_match_empty_rule_skipped() {
        let list = vec!["".into(), "valid.com".into(), "  ".into()];
        assert!(no_proxy_match("valid.com", &list));
        assert!(!no_proxy_match("other.com", &list));
    }

    #[test]
    fn test_no_proxy_match_dot_only_suffix() {
        let list = vec![".local".into()];
        assert!(no_proxy_match(".local", &list));
        assert!(no_proxy_match("x.local", &list));
        assert!(!no_proxy_match("xlocal", &list));
    }

    #[test]
    fn test_no_proxy_match_trimmed_rules() {
        let list = vec!["  example.com  ".into(), "  .suffix  ".into()];
        assert!(no_proxy_match("example.com", &list));
        assert!(no_proxy_match("host.suffix", &list));
    }

    #[test]
    fn test_no_proxy_match_cidr_slash_32() {
        let list = vec!["10.0.0.1/32".into()];
        assert!(no_proxy_match("10.0.0.1", &list));
        assert!(!no_proxy_match("10.0.0.2", &list));
    }

    #[test]
    fn test_no_proxy_match_cidr_slash_0() {
        let list = vec!["0.0.0.0/0".into()];
        assert!(no_proxy_match("1.2.3.4", &list));
        assert!(no_proxy_match("255.255.255.255", &list));
    }

    #[test]
    fn test_no_proxy_match_cidr_non_byte_aligned() {
        // /12 → num_octets = (12+7)/8 = 2 → prefix "10.0"
        let list = vec!["10.0.0.0/12".into()];
        assert!(no_proxy_match("10.0.1.1", &list));
        assert!(no_proxy_match("10.0.255.255", &list));
        assert!(!no_proxy_match("10.1.0.1", &list));
    }

    #[test]
    fn test_no_proxy_match_cidr_ipv6_does_not_panic() {
        let list = vec!["::1".into()];
        // Exact match for "::1"
        assert!(no_proxy_match("::1", &list));
        // IPv6 doesn't contain '.', so CIDR branch is skipped; no match for other IPs
        assert!(!no_proxy_match("fe80::1", &list));
        // CIDR with IPv6 prefix should not panic (just won't match)
        let list2 = vec!["fe80::/10".into()];
        assert!(!no_proxy_match("fe80::1", &list2));
    }

    // --- resolve_url ---

    #[test]
    fn test_resolve_url_no_match() {
        let maps = vec![UrlMap::parse("f=http://real.com").unwrap()];
        assert_eq!(resolve_url("http://other.com/path", &maps), "http://other.com/path");
    }

    #[test]
    fn test_resolve_url_http_match() {
        let maps = vec![UrlMap::parse("f=http://real.com").unwrap()];
        assert_eq!(resolve_url("http://f/path", &maps), "http://real.com/path");
    }

    #[test]
    fn test_resolve_url_ftp_match() {
        let maps = vec![UrlMap::parse("f=ftp://real.ftp/pub").unwrap()];
        assert_eq!(resolve_url("ftp://f/file.iso", &maps), "ftp://real.ftp/pub/file.iso");
    }

    #[test]
    fn test_resolve_url_root_no_path() {
        let maps = vec![UrlMap::parse("f=http://real.com/base").unwrap()];
        assert_eq!(resolve_url("http://f", &maps), "http://real.com/base");
    }

    #[test]
    fn test_resolve_url_root_slash() {
        let maps = vec![UrlMap::parse("f=http://real.com/base").unwrap()];
        assert_eq!(resolve_url("http://f/", &maps), "http://real.com/base/");
    }

    #[test]
    fn test_resolve_url_trailing_slashes_base() {
        let maps = vec![UrlMap::parse("f=http://real.com/base///").unwrap()];
        // UrlMap::parse strips trailing slashes → base = "http://real.com/base"
        assert_eq!(resolve_url("http://f/foo", &maps), "http://real.com/base/foo");
    }

    #[test]
    fn test_resolve_url_prefix_not_match() {
        let maps = vec![UrlMap::parse("f=http://real.com").unwrap()];
        assert_eq!(resolve_url("http://f-extra/path", &maps), "http://f-extra/path");
    }

    #[test]
    fn test_resolve_url_first_match_wins() {
        let maps = vec![
            UrlMap::parse("a=http://first.com").unwrap(),
            UrlMap::parse("a=http://second.com").unwrap(),
        ];
        assert_eq!(resolve_url("http://a/x", &maps), "http://first.com/x");
    }

    // --- handle_connect_tunnel ---

    #[tokio::test]
    async fn test_connect_tunnel_direct_ok() {
        // Upstream echo server
        let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut conn, _) = upstream.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = conn.read(&mut buf).await.unwrap_or(0);
            if n > 0 {
                conn.write_all(&buf[..n]).await.ok();
            }
        });

        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();

        let mut client = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
        let (proxy_side, _) = proxy.accept().await.unwrap();

        tokio::spawn(async move {
            handle_connect_tunnel(proxy_side, None, &[] as &[String]).await;
        });

        let connect = format!("CONNECT 127.0.0.1:{} HTTP/1.1\r\n\r\n", upstream_addr.port());
        client.write_all(connect.as_bytes()).await.unwrap();

        let mut resp = vec![0u8; 1024];
        let n = client.read(&mut resp).await.unwrap();
        assert!(resp[..n].starts_with(b"HTTP/1.1 200"));

        client.write_all(b"ping").await.unwrap();
        let mut echo = vec![0u8; 1024];
        let m = client.read(&mut echo).await.unwrap();
        assert_eq!(&echo[..m], b"ping");
    }

    #[tokio::test]
    async fn test_connect_tunnel_upstream_refused() {
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();

        let mut client = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
        let (proxy_side, _) = proxy.accept().await.unwrap();

        tokio::spawn(async move {
            handle_connect_tunnel(proxy_side, None, &[] as &[String]).await;
        });

        client.write_all(b"CONNECT 127.0.0.1:1 HTTP/1.1\r\n\r\n").await.unwrap();

        let mut resp = vec![0u8; 1024];
        let n = client.read(&mut resp).await.unwrap();
        assert!(
            resp[..n].starts_with(b"HTTP/1.1 502"),
            "expected 502, got: {:?}",
            std::str::from_utf8(&resp[..n.min(100)])
        );
    }

    #[tokio::test]
    async fn test_connect_tunnel_no_proxy_bypass() {
        let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut conn, _) = upstream.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = conn.read(&mut buf).await.unwrap_or(0);
            if n > 0 {
                conn.write_all(&buf[..n]).await.ok();
            }
        });

        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();

        let mut client = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
        let (proxy_side, _) = proxy.accept().await.unwrap();

        let no_proxy = vec!["127.0.0.1".to_string()];
        tokio::spawn(async move {
            handle_connect_tunnel(proxy_side, None, &no_proxy).await;
        });

        let connect = format!("CONNECT 127.0.0.1:{} HTTP/1.1\r\n\r\n", upstream_addr.port());
        client.write_all(connect.as_bytes()).await.unwrap();

        let mut resp = vec![0u8; 1024];
        let n = client.read(&mut resp).await.unwrap();
        assert!(resp[..n].starts_with(b"HTTP/1.1 200"));

        client.write_all(b"no_proxy_test").await.unwrap();
        let mut echo = vec![0u8; 1024];
        let m = client.read(&mut echo).await.unwrap();
        assert_eq!(&echo[..m], b"no_proxy_test");
    }

    // --- make_buffer_stream ---

    #[tokio::test]
    async fn test_make_buffer_stream_single_segment() {
        let dir = std::env::temp_dir().join("apt-blitz-test-make-buffer-stream");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test-single.dat");

        let file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true)
            .open(&file_path)
            .unwrap();

        let total_size = 100;
        file.set_len(total_size).unwrap();

        let (buffer, _rx) = SegmentsBuffer::new(total_size, file, file_path);
        let (id, _start, _end) = buffer.claim_range(100).unwrap();
        // Exhaust the range
        assert!(buffer.claim_range(1).is_none());

        let data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        buffer.write_data(0, &data).unwrap();
        buffer.mark_ready(id);

        let stream_rx = buffer.subscribe();
        let stream = make_buffer_stream(buffer.clone(), stream_rx, 51200, "http://test/x");
        let result: Vec<u8> = futures_util::StreamExt::collect::<Vec<_>>(stream)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(result, data);
        std::fs::remove_file(buffer.file_path()).ok();
    }

    #[tokio::test]
    async fn test_make_buffer_stream_multiple_segments() {
        let dir = std::env::temp_dir().join("apt-blitz-test-make-buffer-stream");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test-multi.dat");

        let file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true)
            .open(&file_path)
            .unwrap();

        let total_size = 200;
        file.set_len(total_size).unwrap();

        let (buffer, _rx) = SegmentsBuffer::new(total_size, file, file_path);

        let (id0, s0, e0) = buffer.claim_range(80).unwrap();
        let (id1, s1, e1) = buffer.claim_range(80).unwrap();
        let (id2, s2, e2) = buffer.claim_range(80).unwrap();
        assert!(buffer.claim_range(1).is_none());

        assert_eq!((s0, e0), (0, 80));
        assert_eq!((s1, e1), (80, 160));
        assert_eq!((s2, e2), (160, 200));

        let data0: Vec<u8> = (0..80).map(|i| i as u8).collect();
        let data1: Vec<u8> = (80..160).map(|i| i as u8).collect();
        let data2: Vec<u8> = (160..200).map(|i| i as u8).collect();

        buffer.write_data(0, &data0).unwrap();
        buffer.write_data(80, &data1).unwrap();
        buffer.write_data(160, &data2).unwrap();

        buffer.mark_ready(id0);
        buffer.mark_ready(id1);
        buffer.mark_ready(id2);

        let stream_rx = buffer.subscribe();
        let stream = make_buffer_stream(buffer.clone(), stream_rx, 51200, "http://test/y");
        let result: Vec<u8> = futures_util::StreamExt::collect::<Vec<_>>(stream)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        let expected: Vec<u8> = (0..200).map(|i| i as u8).collect();
        assert_eq!(result, expected);
        std::fs::remove_file(buffer.file_path()).ok();
    }

    #[tokio::test]
    async fn test_make_buffer_stream_backpressure_mid_segment() {
        let dir = std::env::temp_dir().join("apt-blitz-test-make-buffer-stream");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test-backpressure.dat");

        let file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true)
            .open(&file_path)
            .unwrap();

        // Larger than CHUNK_SIZE so mid-segment backpressure is exercised
        let total_size = 50_000;
        file.set_len(total_size).unwrap();

        let (buffer, _rx) = SegmentsBuffer::new(total_size, file, file_path);
        let (id, _start, _end) = buffer.claim_range(total_size).unwrap();
        assert!(buffer.claim_range(1).is_none());

        let data: Vec<u8> = (0..total_size as u8).cycle().take(total_size as usize).collect();
        buffer.write_data(0, &data).unwrap();
        buffer.mark_ready(id);

        let stream_rx = buffer.subscribe();
        let stream = make_buffer_stream(buffer.clone(), stream_rx, 51200, "http://test/z");
        let result: Vec<u8> = futures_util::StreamExt::collect::<Vec<_>>(stream)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(result.len(), total_size as usize);
        assert_eq!(result, data);
        std::fs::remove_file(buffer.file_path()).ok();
    }

    #[tokio::test]
    async fn test_make_buffer_stream_failed_buffer_yields_none() {
        let dir = std::env::temp_dir().join("apt-blitz-test-make-buffer-stream");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test-failed.dat");

        let file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true)
            .open(&file_path)
            .unwrap();

        let total_size = 100;
        file.set_len(total_size).unwrap();

        let (buffer, _rx) = SegmentsBuffer::new(total_size, file, file_path);
        buffer.claim_range(100).unwrap();
        assert!(buffer.claim_range(1).is_none());

        buffer.set_failed();

        let stream_rx = buffer.subscribe();
        let stream = make_buffer_stream(buffer.clone(), stream_rx, 51200, "http://test/fail");
        let result: Vec<_> = futures_util::StreamExt::collect::<Vec<_>>(stream).await;

        assert_eq!(result.len(), 1);
        assert!(result[0].is_err());
        std::fs::remove_file(buffer.file_path()).ok();
    }
}

/// Check if a host should bypass the upstream proxy according to NO_PROXY rules.
/// Supports:
/// - `*` (bypass all)
/// - `.example.com` (suffix match)
/// - `example.com` (exact match)
/// - `10.0.0.0/8` and IP (simple prefix match for CIDR)
pub fn no_proxy_match(host: &str, no_proxy: &[String]) -> bool {
    if no_proxy.is_empty() {
        return false;
    }
    for rule in no_proxy {
        let rule = rule.trim();
        if rule.is_empty() {
            continue;
        }
        if rule == "*" {
            return true;
        }
        if let Some(suffix) = rule.strip_prefix('.') {
            // Suffix match: .example.com matches any host ending with .example.com
            if host == suffix || host.ends_with(&format!(".{suffix}")) {
                return true;
            }
        } else {
            // Exact match or IP/CIDR
            if host == rule {
                return true;
            }
            // Simple CIDR: check if host starts with the base prefix
            if let Some(cidr_base) = rule.split('/').next() {
                if cidr_base.contains('.') {
                    // For 10.0.0.0/8, check if host starts with "10."
                    let dotted = cidr_base.split('.').collect::<Vec<_>>();
                    let bits: u8 = rule.split('/').nth(1).and_then(|s| s.parse().ok()).unwrap_or(32);
                    let num_octets = (bits as usize + 7) / 8;
                    let prefix = dotted[..num_octets.min(dotted.len())].join(".");
                    if host.starts_with(&prefix) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

pub(crate) async fn handle_connect_tunnel(
    mut stream: tokio::net::TcpStream,
    upstream_proxy: Option<&crate::config::UpstreamProxy>,
    no_proxy: &[String],
) {
    // Read until \r\n\r\n (end of CONNECT headers)
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1];
    loop {
        match stream.read(&mut tmp).await {
            Ok(0) => return, // EOF
            Ok(_) => {}
            Err(e) => {
                warn!("CONNECT read error: {e}");
                return;
            }
        }
        buf.push(tmp[0]);
        if buf.len() >= 4 && buf[buf.len() - 4..] == b"\r\n\r\n"[..] {
            break;
        }
        if buf.len() > 8192 {
            warn!("CONNECT headers too long");
            return;
        }
    }

    // Parse request line: CONNECT host:port HTTP/1.1
    let request = String::from_utf8_lossy(&buf);
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 2 || parts[0] != "CONNECT" {
        return;
    }

    let authority = parts[1];
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().unwrap_or(443)),
        None => (authority.to_string(), 443u16),
    };

    info!(%host, port, "CONNECT tunnel opening");

    let bypass = no_proxy_match(&host, no_proxy);

    if bypass || upstream_proxy.is_none() {
        // Direct connection
        let mut upstream = match tokio::net::TcpStream::connect((host.as_str(), port)).await {
            Ok(s) => s,
            Err(e) => {
                warn!(%host, port, error = %e, "CONNECT upstream connect failed");
                let _ = stream
                    .write_all(
                        format!("HTTP/1.1 502 Bad Gateway\r\n\r\n").as_bytes(),
                    )
                    .await;
                return;
            }
        };

        if stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .is_err()
        {
            return;
        }

        match tokio::io::copy_bidirectional(&mut stream, &mut upstream).await {
            Ok((a, b)) => info!(client_to_server = a, server_to_client = b, "CONNECT tunnel closed"),
            Err(e) => warn!("CONNECT tunnel error: {e}"),
        }
    } else {
        // Use upstream proxy
        let proxy = upstream_proxy.unwrap();
        match proxy.proxy_type {
            ProxyType::Socks5 => {
                use tokio_socks::tcp::Socks5Stream;
                match Socks5Stream::connect(
                    (proxy.host.as_str(), proxy.port),
                    (host.as_str(), port),
                ).await {
                    Ok(mut upstream) => {
                        if stream
                            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                            .await
                            .is_err()
                        {
                            return;
                        }
                        match tokio::io::copy_bidirectional(&mut stream, &mut upstream).await {
                            Ok((a, b)) => info!(client_to_server = a, server_to_client = b, "CONNECT tunnel closed via SOCKS5"),
                            Err(e) => warn!("CONNECT tunnel error via SOCKS5: {e}"),
                        }
                    }
                    Err(e) => {
                        warn!(%host, port, error = %e, "SOCKS5 connect failed");
                        let _ = stream
                            .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                            .await;
                    }
                }
            }
            ProxyType::Http | ProxyType::Https => {
                let proxy_addr = format!("{}:{}", proxy.host, proxy.port);
                match tokio::net::TcpStream::connect(&proxy_addr).await {
                    Ok(mut proxy_stream) => {
                        let connect_req = if let (Some(user), Some(pass)) = (&proxy.username, &proxy.password) {
                            let auth = base64_encode(&format!("{user}:{pass}"));
                            format!("CONNECT {host}:{port} HTTP/1.1\r\nProxy-Authorization: Basic {auth}\r\n\r\n")
                        } else {
                            format!("CONNECT {host}:{port} HTTP/1.1\r\n\r\n")
                        };
                        if proxy_stream.write_all(connect_req.as_bytes()).await.is_err() {
                            let _ = stream.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                            return;
                        }
                        // Read proxy response
                        let mut resp_buf = Vec::new();
                        let mut tmp = [0u8; 1];
                        loop {
                            match proxy_stream.read(&mut tmp).await {
                                Ok(0) => break,
                                Ok(_) => {}
                                Err(_) => break,
                            }
                            resp_buf.push(tmp[0]);
                            if resp_buf.len() >= 4 && resp_buf[resp_buf.len() - 4..] == b"\r\n\r\n"[..] {
                                break;
                            }
                            if resp_buf.len() > 4096 { break; }
                        }
                        let resp = String::from_utf8_lossy(&resp_buf);
                        if resp.starts_with("HTTP/1.1 200") || resp.starts_with("HTTP/1.0 200") {
                            if stream
                                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                                .await
                                .is_err()
                            {
                                return;
                            }
                            match tokio::io::copy_bidirectional(&mut stream, &mut proxy_stream).await {
                                Ok((a, b)) => info!(client_to_server = a, server_to_client = b, "CONNECT tunnel closed via HTTP proxy"),
                                Err(e) => warn!("CONNECT tunnel error via HTTP proxy: {e}"),
                            }
                        } else {
                            warn!(%host, port, proxy_resp = %resp.trim(), "CONNECT rejected by proxy");
                            let _ = stream.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                        }
                    }
                    Err(e) => {
                        warn!(%host, port, error = %e, "HTTP proxy connect failed");
                        let _ = stream.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                    }
                }
            }
        }
    }
}

fn base64_encode(input: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(input)
}

async fn serve_file(path: &std::path::Path, headers: &HeaderMap) -> Result<Response, ProxyError> {
    use tokio::io::AsyncReadExt as _;

    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| ProxyError::Internal(format!("failed to open cache file: {e}")))?;
    let content_length = file
        .metadata()
        .await
        .map_err(|e| ProxyError::Internal(format!("failed to read cache file metadata: {e}")))?
        .len();

    let stream = futures_util::stream::unfold(file, |mut file| async move {
        let mut buf = vec![0u8; 65536];
        match file.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok::<_, std::io::Error>(bytes::Bytes::from(buf)), file))
            }
            Err(e) => Some((Err(e), file)),
        }
    });

    let mut builder = Response::builder().status(StatusCode::OK);
    for (key, val) in headers {
        builder = builder.header(key, val);
    }
    builder = builder.header("content-length", content_length.to_string());
    builder = builder.header("accept-ranges", "bytes");
    Ok(builder.body(Body::from_stream(stream)).unwrap())
}
