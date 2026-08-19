mod common;

use std::sync::{Arc, Mutex};

use common::TestContext;
use wiremock::matchers::{method, path};
use wiremock::{Mock, Request, ResponseTemplate};

use apt_blitz::config::Config;

fn base_config(max_cache_age: u64) -> Config {
    Config {
        port: 0,
        bind: "127.0.0.1".into(),
        connections: 4,
        cache_dir: Default::default(),
        max_cache_size: 1024 * 1024 * 1024,
        max_cache_age,
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
        coalesce_etag_max_retries: 8,
    }
}

async fn last_request_path(ctx: &TestContext, method: &str, path: &str) -> Option<Request> {
    let reqs = ctx.upstream.server.received_requests().await.unwrap_or_default();
    reqs.into_iter()
        .filter(|r| r.method.as_str() == method && r.url.path() == path)
        .last()
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_revalidates_via_etag_304() {
    let ctx = TestContext::with_config(1024 * 1024 * 1024, base_config(0)).await;
    let name = "/etag-304.deb";
    let size = 512 * 1024;
    ctx.upstream.register_file_with_etag(name, size, "\"etag-v1\"").await;

    let body1 = ctx.get_bytes(name).await;
    assert_eq!(body1.len() as u64, size);

    let head_after_first = ctx.upstream.request_count_by_method(name, "HEAD").await;
    let get_after_first = ctx.upstream.request_count_by_method(name, "GET").await;

    let body2 = ctx.get_bytes(name).await;
    assert_eq!(body2.len() as u64, size);
    assert_eq!(body1, body2);

    // 304 → served from cache: exactly one conditional HEAD, no redownload.
    assert_eq!(
        ctx.upstream.request_count_by_method(name, "HEAD").await,
        head_after_first + 1,
        "exactly one revalidation HEAD expected"
    );
    assert_eq!(
        ctx.upstream.request_count_by_method(name, "GET").await,
        get_after_first,
        "304 must not trigger a redownload"
    );

    // The revalidation request must carry the stored ETag.
    let head = last_request_path(&ctx, "HEAD", name).await.unwrap();
    assert_eq!(
        head.headers.get("if-none-match").unwrap().to_str().unwrap(),
        "\"etag-v1\"",
        "revalidation HEAD must send If-None-Match with the stored ETag"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_file_changed_upstream_redownloads() {
    let ctx = TestContext::with_config(1024 * 1024 * 1024, base_config(0)).await;
    let name = "/changed.deb";
    let size = 128 * 1024;

    let state = Arc::new(Mutex::new(("v1", vec![1u8; size])));
    let state_head = state.clone();
    Mock::given(method("HEAD"))
        .and(path(name))
        .respond_with(move |req: &Request| {
            let guard = state_head.lock().unwrap();
            let (etag, data) = &*guard;
            if req
                .headers
                .get("if-none-match")
                .is_some_and(|v| v.to_str().unwrap_or("") == *etag)
            {
                return ResponseTemplate::new(304).insert_header("etag", *etag);
            }
            ResponseTemplate::new(200)
                .insert_header("content-length", data.len().to_string())
                .insert_header("accept-ranges", "bytes")
                .insert_header("etag", *etag)
                .set_body_bytes([])
        })
        .mount(&ctx.upstream.server)
        .await;

    let state_get = state.clone();
    Mock::given(method("GET"))
        .and(path(name))
        .respond_with(move |req: &Request| {
            let guard = state_get.lock().unwrap();
            let (etag, data) = &*guard;
            if let Some(range) = req.headers.get("range") {
                let range_str = range.to_str().unwrap_or("");
                if let Some((start, end)) = common::parse_range(range_str, data.len() as u64) {
                    let len = (end - start + 1) as usize;
                    let chunk = data[start as usize..=end as usize].to_vec();
                    return ResponseTemplate::new(206)
                        .insert_header(
                            "content-range",
                            format!("bytes {start}-{end}/{}", data.len()),
                        )
                        .insert_header("content-length", len.to_string())
                        .insert_header("etag", *etag)
                        .set_body_bytes(chunk);
                }
            }
            ResponseTemplate::new(200)
                .insert_header("content-length", data.len().to_string())
                .insert_header("accept-ranges", "bytes")
                .insert_header("etag", *etag)
                .set_body_bytes(data.clone())
        })
        .mount(&ctx.upstream.server)
        .await;

    let body1 = ctx.get_bytes(name).await;
    assert_eq!(body1, vec![1u8; size]);

    // Simulate the upstream file being replaced.
    *state.lock().unwrap() = ("v2", vec![2u8; size]);

    let body2 = ctx.get_bytes(name).await;
    assert_eq!(body2, vec![2u8; size], "client must receive the NEW upstream content");

    let get_after_second = ctx.upstream.request_count_by_method(name, "GET").await;

    // Third fetch: cached v2 is still valid → 304, no redownload.
    let body3 = ctx.get_bytes(name).await;
    assert_eq!(body3, vec![2u8; size]);
    assert_eq!(
        ctx.upstream.request_count_by_method(name, "GET").await,
        get_after_second,
        "fresh cached copy must not be redownloaded"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fresh_by_age_skips_upstream() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await; // default max_cache_age = 86400
    let name = "/ttl-fresh.deb";
    ctx.upstream.register_file(name, 256 * 1024).await;

    ctx.get_bytes(name).await;
    let requests_before = ctx.upstream.request_count(name).await;

    let body2 = ctx.get_bytes(name).await;
    assert_eq!(body2.len() as u64, 256 * 1024);
    assert_eq!(
        ctx.upstream.request_count(name).await,
        requests_before,
        "fresh-by-age cache hit must not contact upstream"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn max_cache_age_zero_revalidates_every_hit() {
    let ctx = TestContext::with_config(1024 * 1024 * 1024, base_config(0)).await;
    let name = "/always-revalidate.deb";
    ctx.upstream.register_file_with_etag(name, 256 * 1024, "\"v1\"").await;

    ctx.get_bytes(name).await;
    let head_after_first = ctx.upstream.request_count_by_method(name, "HEAD").await;
    let get_after_first = ctx.upstream.request_count_by_method(name, "GET").await;

    ctx.get_bytes(name).await;
    ctx.get_bytes(name).await;

    let head_after = ctx.upstream.request_count_by_method(name, "HEAD").await;
    let get_after = ctx.upstream.request_count_by_method(name, "GET").await;
    assert_eq!(head_after, head_after_first + 2, "each hit must revalidate");
    assert_eq!(get_after, get_after_first, "no redownload on 304");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_validators_past_window_redownloads() {
    let ctx = TestContext::with_config(1024 * 1024 * 1024, base_config(0)).await;
    let name = "/no-validators.deb";
    // No ETag / Last-Modified from upstream.
    ctx.upstream.register_file(name, 256 * 1024).await;

    ctx.get_bytes(name).await;
    let get_before = ctx.upstream.request_count_by_method(name, "GET").await;

    ctx.get_bytes(name).await;
    let get_after = ctx.upstream.request_count_by_method(name, "GET").await;
    assert!(
        get_after > get_before,
        "file without validators and expired window must be redownloaded"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_revalidates_via_last_modified() {
    let ctx = TestContext::with_config(1024 * 1024 * 1024, base_config(0)).await;
    let name = "/lm-304.deb";
    let lm = "Mon, 01 Jan 2024 00:00:00 GMT";
    ctx.upstream.register_file_with_last_modified(name, 256 * 1024, lm).await;

    ctx.get_bytes(name).await;
    let head_after_first = ctx.upstream.request_count_by_method(name, "HEAD").await;
    let get_after_first = ctx.upstream.request_count_by_method(name, "GET").await;

    ctx.get_bytes(name).await;
    assert_eq!(
        ctx.upstream.request_count_by_method(name, "HEAD").await,
        head_after_first + 1,
        "exactly one conditional HEAD expected"
    );
    assert_eq!(
        ctx.upstream.request_count_by_method(name, "GET").await,
        get_after_first,
        "304 must not trigger a redownload"
    );

    let head = last_request_path(&ctx, "HEAD", name).await.unwrap();
    assert_eq!(
        head.headers.get("if-modified-since").unwrap().to_str().unwrap(),
        lm,
        "revalidation HEAD must send If-Modified-Since with the stored value"
    );
}
