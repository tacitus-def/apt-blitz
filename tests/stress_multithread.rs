mod common;

use std::sync::Arc;

use common::{sha256_of, TestContext};

#[tokio::test(flavor = "multi_thread")]
async fn n_workers_single_file() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;
    let upstream = &ctx.upstream;

    let file_size = 50 * 1024 * 1024; // 50 MB
    upstream.register_file("/large.deb", file_size).await;

    let body = ctx.get_bytes("/large.deb").await;
    assert_eq!(body.len() as u64, file_size);

    let expected: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    assert_eq!(sha256_of(&body), sha256_of(&expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn multithread_workers_issue_range_requests() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;
    let upstream = &ctx.upstream;

    let file_size = 5 * 1024 * 1024; // 5 MB
    upstream.register_file("/range-test.deb", file_size).await;

    let body = ctx.get_bytes("/range-test.deb").await;
    assert_eq!(body.len() as u64, file_size);

    // Upstream should have received at least one Range request (from workers)
    // The HEAD request counts too, but we want to see GET requests with Range
    let requests = upstream
        .server
        .received_requests()
        .await
        .unwrap_or_default();
    let range_gets: Vec<_> = requests
        .iter()
        .filter(|r| {
            r.method == "GET"
                && r.url.path() == "/range-test.deb"
                && r.headers.get("range").is_some()
        })
        .collect();
    assert!(!range_gets.is_empty(), "expected at least one Range GET request");
    assert!(
        range_gets.len() >= 2,
        "expected at least 2 Range GET requests (one per worker), got {}",
        range_gets.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_multithread_downloads() {
    let ctx = Arc::new(TestContext::new(1024 * 1024 * 1024).await);

    let mut handles = Vec::new();
    for i in 0..10 {
        let file_size = 5 * 1024 * 1024; // 5 MB
        let name = format!("/file-{i}.deb");
        ctx.upstream.register_file(&name, file_size).await;

        let ctx = Arc::clone(&ctx);
        handles.push(tokio::spawn(async move {
            let body = ctx.get_bytes(&name).await;
            assert_eq!(body.len() as u64, file_size);
            let expected: Vec<u8> = (0..file_size).map(|j| (j % 256) as u8).collect();
            assert_eq!(sha256_of(&body), sha256_of(&expected));
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn multithread_small_file_falls_back_to_plain() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;
    let upstream = &ctx.upstream;

    let file_size = 100 * 1024; // 100 KB (plain proxy)
    upstream.register_file("/small.deb", file_size).await;

    let body = ctx.get_bytes("/small.deb").await;
    assert_eq!(body.len() as u64, file_size);
}
