mod common;

use std::sync::Arc;
use std::time::Duration;

use common::TestContext;

use apt_blitz::config::Config;

fn base_config() -> Config {
    Config {
        port: 0,
        bind: "127.0.0.1".into(),
        connections: 4,
        cache_dir: Default::default(),
        max_cache_size: 1024 * 1024 * 1024,
        url_maps: vec![],
        upstream_proxy: None,
        no_proxy: vec![],
        max_connections_per_ip: 0,
        max_total_connections: 0,
        max_workers: 0,
        upstream_bandwidth: 0,
        per_ip_bandwidth: 0,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn per_ip_bandwidth_throttles_response() {
    let mut config = base_config();
    config.per_ip_bandwidth = 1024 * 1024;
    let ctx = TestContext::with_config(1024 * 1024 * 1024, config).await;

    let file_size = 8192;
    ctx.upstream.register_file("/throttle.deb", file_size).await;

    let body = ctx.get_bytes("/throttle.deb").await;
    assert_eq!(body.len() as u64, file_size);
}

#[tokio::test(flavor = "multi_thread")]
async fn max_connections_per_ip_rejects() {
    let mut config = base_config();
    config.max_connections_per_ip = 1;
    let ctx = Arc::new(TestContext::with_config(1024 * 1024 * 1024, config).await);

    let file_size = 512 * 1024;
    ctx.upstream
        .register_file_with_delay("/slow.deb", file_size, Duration::from_millis(200))
        .await;

    let ctx1 = Arc::clone(&ctx);
    let h1 = tokio::spawn(async move {
        let resp = ctx1.get("/slow.deb").send().await.unwrap();
        resp.status()
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let ctx2 = Arc::clone(&ctx);
    let h2 = tokio::spawn(async move {
        let resp = ctx2.get("/slow.deb").send().await.unwrap();
        resp.status()
    });

    let s1 = h1.await.unwrap();
    let s2 = h2.await.unwrap();

    let rejected = s1 == reqwest::StatusCode::TOO_MANY_REQUESTS
        || s2 == reqwest::StatusCode::TOO_MANY_REQUESTS;
    assert!(
        rejected,
        "expected one request to be rejected with 429, got s1={s1}, s2={s2}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn max_total_connections_rejects() {
    let mut config = base_config();
    config.max_total_connections = 1;
    let ctx = Arc::new(TestContext::with_config(1024 * 1024 * 1024, config).await);

    let file_size = 512 * 1024;
    ctx.upstream
        .register_file_with_delay("/global-slow.deb", file_size, Duration::from_millis(200))
        .await;

    let ctx1 = Arc::clone(&ctx);
    let h1 = tokio::spawn(async move {
        let resp = ctx1.get("/global-slow.deb").send().await.unwrap();
        resp.status()
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let ctx2 = Arc::clone(&ctx);
    let h2 = tokio::spawn(async move {
        let resp = ctx2.get("/global-slow.deb").send().await.unwrap();
        resp.status()
    });

    let s1 = h1.await.unwrap();
    let s2 = h2.await.unwrap();

    let rejected = s1 == reqwest::StatusCode::TOO_MANY_REQUESTS
        || s2 == reqwest::StatusCode::TOO_MANY_REQUESTS;
    assert!(
        rejected,
        "expected one request rejected with 429, got s1={s1}, s2={s2}"
    );
}
