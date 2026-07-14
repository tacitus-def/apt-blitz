use std::sync::Arc;
use std::time::Instant;
use futures_util::StreamExt;
use reqwest::StatusCode;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::buffer::SegmentsBuffer;
use crate::rate_limit::{TokenBucket, WorkerLimiter};

pub(crate) const MIN_SEGMENT_SIZE: u64 = 64 * 1024;
pub(crate) const MAX_SEGMENT_SIZE: u64 = 4 * 1024 * 1024;
const INITIAL_SEGMENT_SIZE: u64 = 1024 * 1024;
pub(crate) const TARGET_SEGMENT_TIME_SECS: f64 = 2.0;

fn next_segment_size(speed: f64) -> u64 {
    if speed <= 0.0 {
        return INITIAL_SEGMENT_SIZE;
    }
    let preferred = (speed * TARGET_SEGMENT_TIME_SECS) as u64;
    preferred.clamp(MIN_SEGMENT_SIZE, MAX_SEGMENT_SIZE)
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum DownloadError {
    Reqwest(reqwest::Error),
    HttpStatus(StatusCode),
    Io(std::io::Error),
    BufferFailed,
    Cancelled,
}

impl From<reqwest::Error> for DownloadError {
    fn from(e: reqwest::Error) -> Self {
        DownloadError::Reqwest(e)
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(e: std::io::Error) -> Self {
        DownloadError::Io(e)
    }
}

pub async fn download_multithreaded(
    client: &reqwest::Client,
    url: &str,
    buffer: Arc<SegmentsBuffer>,
    num_connections: usize,
    worker_limiter: &WorkerLimiter,
    upstream_bucket: &TokenBucket,
    req_id: u64,
) -> Result<(), DownloadError> {
    let cancel = CancellationToken::new();
    let mut handles = Vec::with_capacity(num_connections);
    let worker_limiter = worker_limiter.clone();
    let upstream_bucket = upstream_bucket.clone();

    for i in 0..num_connections {
        let client = client.clone();
        let url = url.to_string();
        let buffer = buffer.clone();
        let child_token = cancel.child_token();
        let bucket = upstream_bucket.clone();
        let limiter = worker_limiter.clone();
        handles.push(tokio::spawn(async move {
            let _permit = limiter.acquire().await;
            download_worker(&client, &url, buffer, child_token, i, &bucket, req_id).await
        }));
    }

    let mut errors = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(DownloadError::Cancelled)) => {}
            Ok(Err(e)) => {
                error!(req_id, error = ?e, "downloader worker failed");
                errors.push(e);
                cancel.cancel();
            }
            Err(e) => {
                error!(req_id, error = ?e, "downloader worker panicked");
                errors.push(DownloadError::BufferFailed);
                cancel.cancel();
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(DownloadError::BufferFailed)
    }
}

async fn download_worker(
    client: &reqwest::Client,
    url: &str,
    buffer: Arc<SegmentsBuffer>,
    cancel: CancellationToken,
    worker_id: usize,
    upstream_bucket: &TokenBucket,
    req_id: u64,
) -> Result<(), DownloadError> {
    let mut preferred_size = INITIAL_SEGMENT_SIZE;

    loop {
        if cancel.is_cancelled() {
            return Err(DownloadError::Cancelled);
        }

        let range = buffer.claim_range(preferred_size);
        let (id, start, end) = match range {
            Some(r) => r,
            None => break,
        };

        let size = end - start;

        let range_header = format!("bytes={}-{}", start, end - 1);
        info!(
            req_id,
            worker = worker_id,
            segment = id,
            range = %range_header,
            "downloading segment"
        );

        let start_time = Instant::now();
        let response = client
            .get(url)
            .header("Range", &range_header)
            .send()
            .await?;

        let status = response.status();
        if status != StatusCode::PARTIAL_CONTENT && status != StatusCode::OK {
            error!(
                req_id,
                worker = worker_id,
                segment = id,
                status = %status,
                "unexpected status for range request"
            );
            return Err(DownloadError::HttpStatus(status));
        }

        let mut stream = response.bytes_stream();
        let mut file_offset = start;
        while let Some(chunk) = stream.next().await {
            if cancel.is_cancelled() {
                return Err(DownloadError::Cancelled);
            }
            let chunk = chunk?;
            let len = chunk.len() as u64;
            if !upstream_bucket.wait_consume(len, &cancel).await {
                return Err(DownloadError::Cancelled);
            }
            buffer.write_data(file_offset, &chunk)?;
            file_offset += len;
        }

        buffer.mark_ready(id);
        info!(
            req_id,
            worker = worker_id,
            segment = id,
            size = size,
            "segment ready"
        );

        let elapsed = start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let speed = size as f64 / elapsed;
            preferred_size = next_segment_size(speed);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants_sanity() {
        assert!(MIN_SEGMENT_SIZE < MAX_SEGMENT_SIZE);
        assert!(INITIAL_SEGMENT_SIZE >= MIN_SEGMENT_SIZE);
        assert!(INITIAL_SEGMENT_SIZE <= MAX_SEGMENT_SIZE);
        assert_eq!(MIN_SEGMENT_SIZE, 64 * 1024);
        assert_eq!(MAX_SEGMENT_SIZE, 4 * 1024 * 1024);
        assert_eq!(INITIAL_SEGMENT_SIZE, 1024 * 1024);
    }

    #[tokio::test]
    async fn test_download_error_from_reqwest() {
        // Create a reqwest::Error by making a request to an invalid URL
        let client = reqwest::Client::new();
        let result = client.get("http://255.255.255.255:1/").send().await;
        let err = result.unwrap_err();
        let de: DownloadError = err.into();
        assert!(matches!(de, DownloadError::Reqwest(_)));
    }

    #[test]
    fn test_download_error_from_io() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let de: DownloadError = err.into();
        assert!(matches!(de, DownloadError::Io(_)));
    }

    #[test]
    fn test_download_error_http_status() {
        let err = DownloadError::HttpStatus(StatusCode::FORBIDDEN);
        match err {
            DownloadError::HttpStatus(s) => assert_eq!(s, StatusCode::FORBIDDEN),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_download_error_debug() {
        let err = DownloadError::BufferFailed;
        let debug = format!("{:?}", err);
        assert!(!debug.is_empty());
    }

    #[test]
    fn test_download_error_cancelled() {
        let err = DownloadError::Cancelled;
        assert!(matches!(err, DownloadError::Cancelled));
    }

    // --- segment adaptation tests ---

    #[test]
    fn test_next_segment_size_zero_speed() {
        assert_eq!(next_segment_size(0.0), INITIAL_SEGMENT_SIZE);
    }

    #[test]
    fn test_next_segment_size_negative_speed() {
        assert_eq!(next_segment_size(-1.0), INITIAL_SEGMENT_SIZE);
    }

    #[test]
    fn test_next_segment_size_very_slow() {
        // speed = 10_000 bytes/sec → target = 20_000 → clamped to MIN = 64K
        let size = next_segment_size(10_000.0);
        assert_eq!(size, MIN_SEGMENT_SIZE);
    }

    #[test]
    fn test_next_segment_size_target_exact_min() {
        // speed = MIN_SEGMENT_SIZE / TARGET = 64K / 2 = 32K → target = 64K
        let size = next_segment_size(MIN_SEGMENT_SIZE as f64 / TARGET_SEGMENT_TIME_SECS);
        assert_eq!(size, MIN_SEGMENT_SIZE);
    }

    #[test]
    fn test_next_segment_size_target_exact_max() {
        // speed = MAX_SEGMENT_SIZE / TARGET = 4M / 2 = 2M → target = 4M
        let size = next_segment_size(MAX_SEGMENT_SIZE as f64 / TARGET_SEGMENT_TIME_SECS);
        assert_eq!(size, MAX_SEGMENT_SIZE);
    }

    #[test]
    fn test_next_segment_size_very_fast() {
        // speed = 100 MB/s → target = 200 MB → clamped to MAX = 4M
        let size = next_segment_size(100_000_000.0);
        assert_eq!(size, MAX_SEGMENT_SIZE);
    }

    #[test]
    fn test_next_segment_size_mid_range() {
        // speed = 512K → target = 1M (which is also INITIAL_SEGMENT_SIZE)
        let size = next_segment_size(524_288.0);
        assert_eq!(size, 1024 * 1024);
    }

    #[test]
    fn test_next_segment_size_clamp_low() {
        // speed = 10 bytes/sec → target = 20 bytes → clamped to MIN = 64K
        let size = next_segment_size(10.0);
        assert_eq!(size, MIN_SEGMENT_SIZE);
    }

    #[test]
    fn test_next_segment_size_clamp_high() {
        // speed = 1 GB/s → target = 2 GB → clamped to MAX = 4M
        let size = next_segment_size(1_000_000_000.0);
        assert_eq!(size, MAX_SEGMENT_SIZE);
    }

    #[test]
    fn test_next_segment_size_monotonic() {
        let speeds = [1000.0, 10_000.0, 100_000.0, 500_000.0, 1_000_000.0, 10_000_000.0];
        let sizes: Vec<u64> = speeds.iter().map(|&s| next_segment_size(s)).collect();
        for w in sizes.windows(2) {
            assert!(w[0] <= w[1], "size should be non-decreasing with speed");
        }
    }

    #[test]
    fn test_next_segment_size_f64_edge_cases() {
        // f64::MAX should not overflow
        let size = next_segment_size(f64::MAX);
        assert_eq!(size, MAX_SEGMENT_SIZE);
        // f64::MIN_POSITIVE
        let size = next_segment_size(f64::MIN_POSITIVE);
        assert_eq!(size, MIN_SEGMENT_SIZE);
    }

    fn create_temp_buffer(size: u64) -> Arc<SegmentsBuffer> {
        crate::test_util::create_temp_buffer(size)
    }

    #[tokio::test]
    async fn test_download_worker_small_file() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let data = b"hello from wiremock worker";

        Mock::given(method("GET"))
            .and(path("/test.dat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", data.len().to_string())
                    .insert_header("accept-ranges", "bytes")
                    .set_body_bytes(data),
            )
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let url = format!("{}/test.dat", server.uri());
        let buffer = create_temp_buffer(data.len() as u64);
        let cancel = CancellationToken::new();
        let bucket = TokenBucket::unlimited();

        let result = download_worker(&client, &url, buffer.clone(), cancel, 0, &bucket, 0).await;
        assert!(result.is_ok());

        assert!(buffer.all_completed());
        let read = buffer.read_data(0, data.len() as u64).unwrap();
        assert_eq!(read.to_vec(), data);
    }

    #[tokio::test]
    async fn test_download_worker_cancelled() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let data_len = 10_000u64;
        let data = vec![0u8; data_len as usize];

        Mock::given(method("GET"))
            .and(path("/slow.dat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", data_len.to_string())
                    .insert_header("accept-ranges", "bytes")
                    .set_body_bytes(data)
                    .set_delay(std::time::Duration::from_secs(60)),
            )
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let url = format!("{}/slow.dat", server.uri());
        let buffer = create_temp_buffer(data_len);
        let cancel = CancellationToken::new();
        let bucket = TokenBucket::unlimited();

        cancel.cancel();

        let result = download_worker(&client, &url, buffer.clone(), cancel, 0, &bucket, 0).await;
        assert!(matches!(result, Err(DownloadError::Cancelled)));
    }
}
