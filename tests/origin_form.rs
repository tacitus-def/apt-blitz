mod common;

use common::parse_range;

use std::sync::Arc;

use apt_blitz::build_app;
use apt_blitz::config::Config;
use apt_blitz::proxy::AppState;
use reqwest::Client;

#[tokio::test(flavor = "multi_thread")]
async fn origin_form_single_file() {
    let upstream = wiremock::MockServer::start().await;
    let name = "/pool/main/a/apt/apt_1.0.deb";
    let size = 64 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let data = Arc::new(data);

    let d = Arc::clone(&data);
    wiremock::Mock::given(wiremock::matchers::method("HEAD"))
        .and(wiremock::matchers::path(name))
        .respond_with(move |_: &wiremock::Request| {
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-length", d.len().to_string())
                .insert_header("accept-ranges", "bytes")
                .set_body_bytes(&[])
        })
        .mount(&upstream)
        .await;

    let d2 = Arc::clone(&data);
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(name))
        .respond_with(move |req: &wiremock::Request| {
            if let Some(range) = req.headers.get("range") {
                let range_str = range.to_str().unwrap_or("");
                if let Some((start, end)) = parse_range(range_str, d2.len() as u64) {
                    let len = (end - start + 1) as usize;
                    let chunk = d2[start as usize..=end as usize].to_vec();
                    return wiremock::ResponseTemplate::new(206)
                        .insert_header("content-range", format!("bytes {start}-{end}/{}", d2.len()))
                        .insert_header("content-length", len.to_string())
                        .set_body_bytes(chunk);
                }
            }
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-length", d2.len().to_string())
                .insert_header("accept-ranges", "bytes")
                .set_body_bytes(d2.as_ref().clone())
        })
        .mount(&upstream)
        .await;

    let cache_dir = tempfile::tempdir().unwrap();
    let config = Config {
        port: 0,
        bind: "127.0.0.1".into(),
        connections: 4,
        cache_dir: cache_dir.path().join("cache"),
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
    };

    let client = Client::builder()
        .user_agent("apt-blitz-test/0.1.0")
        .build()
        .unwrap();

    let state = AppState::from_config(config, client.clone());

    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // origin-form: path только, Host берётся из заголовка
    let upstream_uri = upstream.uri();
    let upstream_host = upstream_uri.trim_start_matches("http://");
    let resp = client
        .get(format!("http://{addr}{name}"))
        .header("Host", upstream_host)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    assert!(
        status.is_success(),
        "origin-form request failed: {status}, body: {}",
        String::from_utf8_lossy(&body[..body.len().min(200)])
    );
    assert_eq!(body.len() as u64, size);
}

#[tokio::test(flavor = "multi_thread")]
async fn origin_form_with_url_map() {
    let upstream = wiremock::MockServer::start().await;
    let name = "/pool/main.deb";
    let size = 4096;
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let data = Arc::new(data);

    let d = Arc::clone(&data);
    wiremock::Mock::given(wiremock::matchers::method("HEAD"))
        .and(wiremock::matchers::path(name))
        .respond_with(move |_: &wiremock::Request| {
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-length", d.len().to_string())
                .insert_header("accept-ranges", "bytes")
                .set_body_bytes(&[])
        })
        .mount(&upstream)
        .await;

    let d2 = Arc::clone(&data);
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(name))
        .respond_with(move |req: &wiremock::Request| {
            if let Some(range) = req.headers.get("range") {
                let range_str = range.to_str().unwrap_or("");
                if let Some((start, end)) = parse_range(range_str, d2.len() as u64) {
                    let len = (end - start + 1) as usize;
                    let chunk = d2[start as usize..=end as usize].to_vec();
                    return wiremock::ResponseTemplate::new(206)
                        .insert_header("content-range", format!("bytes {start}-{end}/{}", d2.len()))
                        .insert_header("content-length", len.to_string())
                        .set_body_bytes(chunk);
                }
            }
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-length", d2.len().to_string())
                .insert_header("accept-ranges", "bytes")
                .set_body_bytes(d2.as_ref().clone())
        })
        .mount(&upstream)
        .await;

    let cache_dir = tempfile::tempdir().unwrap();
    let config = Config {
        port: 0,
        bind: "127.0.0.1".into(),
        connections: 4,
        cache_dir: cache_dir.path().join("cache"),
        max_cache_size: 1024 * 1024 * 1024,
        // UrlMap: fake-host → реальный upstream
        url_maps: vec![apt_blitz::config::UrlMap::parse(&format!("mirror.example.com={}", upstream.uri())).unwrap()],
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

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // origin-form с Host = fake-host → resolve_url подменяет на реальный upstream
    let resp = client
        .get(format!("http://{addr}{name}"))
        .header("Host", "mirror.example.com")
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    assert!(
        status.is_success(),
        "origin-form + url_map failed: {status}, body: {}",
        String::from_utf8_lossy(&body[..body.len().min(200)])
    );
    assert_eq!(body.len() as u64, size);
}
