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
        coalesce_follower_timeout_secs: 50,
        coalesce_max_retries: 3,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn per_ip_bandwidth_throttles_response() {
    // Use a tiny bandwidth (10 KB/s) so the burst is also 10 KB.
    // A 50 KB file exceeds the burst → must take at least ~4 seconds.
    // This proves the token bucket actually throttles, not just accepts the config.
    let mut config = base_config();
    config.per_ip_bandwidth = 10 * 1024; // 10 KB/s
    let ctx = TestContext::with_config(1024 * 1024 * 1024, config).await;

    let file_size = 50 * 1024; // 50 KB — 5x the burst
    ctx.upstream.register_file("/throttle.deb", file_size).await;

    let start = std::time::Instant::now();
    let body = ctx.get_bytes("/throttle.deb").await;
    let elapsed = start.elapsed();

    assert_eq!(body.len() as u64, file_size);
    // 50 KB at 10 KB/s: burst covers 10 KB, remaining 40 KB takes ~4s.
    // Allow generous tolerance for system load.
    assert!(
        elapsed >= Duration::from_secs(2),
        "per-IP throttle did not slow response: 50 KB at 10 KB/s took {:?} (expected >= 2s)",
        elapsed
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn max_connections_per_ip_rejects() {
    let mut config = base_config();
    config.max_connections_per_ip = 1;
    let ctx = Arc::new(TestContext::with_config(1024 * 1024 * 1024, config).await);

    let file_size = 512 * 1024;
    ctx.upstream
        .register_file_with_delay("/slow.deb", file_size, Duration::from_millis(500))
        .await;

    // Retry up to 5 times to avoid flakiness from scheduling races
    for attempt in 0..5 {
        let ctx1 = Arc::clone(&ctx);
        let ctx2 = Arc::clone(&ctx);
        let h1 = tokio::spawn(async move {
            let resp = ctx1.get("/slow.deb").send().await.unwrap();
            resp.status()
        });
        let h2 = tokio::spawn(async move {
            let resp = ctx2.get("/slow.deb").send().await.unwrap();
            resp.status()
        });

        let s1 = h1.await.unwrap();
        let s2 = h2.await.unwrap();

        if s1 == reqwest::StatusCode::TOO_MANY_REQUESTS
            || s2 == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            return; // success
        }
        eprintln!("attempt {attempt}: s1={s1}, s2={s2} — retrying");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no 429 received after 5 attempts");
}

#[tokio::test(flavor = "multi_thread")]
async fn max_total_connections_rejects() {
    let mut config = base_config();
    config.max_total_connections = 1;
    let ctx = Arc::new(TestContext::with_config(1024 * 1024 * 1024, config).await);

    let file_size = 512 * 1024;
    ctx.upstream
        .register_file_with_delay("/global-slow.deb", file_size, Duration::from_millis(500))
        .await;

    // Retry up to 5 times to avoid flakiness from scheduling races
    for attempt in 0..5 {
        let ctx1 = Arc::clone(&ctx);
        let ctx2 = Arc::clone(&ctx);
        let h1 = tokio::spawn(async move {
            let resp = ctx1.get("/global-slow.deb").send().await.unwrap();
            resp.status()
        });
        let h2 = tokio::spawn(async move {
            let resp = ctx2.get("/global-slow.deb").send().await.unwrap();
            resp.status()
        });

        let s1 = h1.await.unwrap();
        let s2 = h2.await.unwrap();

        if s1 == reqwest::StatusCode::TOO_MANY_REQUESTS
            || s2 == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            return; // success
        }
        eprintln!("attempt {attempt}: s1={s1}, s2={s2} — retrying");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no 429 received after 5 attempts");
}
