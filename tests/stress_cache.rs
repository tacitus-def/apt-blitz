mod common;

use std::sync::Arc;

use common::TestContext;

#[tokio::test(flavor = "multi_thread")]
async fn lru_eviction_under_pressure() {
    let ctx = TestContext::new(5 * 1024 * 1024).await; // 5 MB limit

    let n_files = 20;
    let file_size = 1024 * 1024; // 1 MB each → 20 MB total

    for i in 0..n_files {
        let name = format!("/file-{i}.deb");
        ctx.upstream.register_file(&name, file_size).await;
        let body = ctx.get_bytes(&name).await;
        assert_eq!(body.len() as u64, file_size);
    }

    // Re-fetch last 5 — should be cache HITs
    for i in (n_files - 5)..n_files {
        let name = format!("/file-{i}.deb");
        let body = ctx.get_bytes(&name).await;
        assert_eq!(body.len() as u64, file_size);
    }

    // Re-fetch first — may be evicted, but still accessible
    let body = ctx.get_bytes("/file-0.deb").await;
    assert_eq!(body.len() as u64, file_size);
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_concurrent_lookup_store() {
    let ctx = Arc::new(TestContext::new(1024 * 1024 * 1024).await);

    let n_concurrent = 50;
    let file_size = 64 * 1024;
    let mut handles = Vec::new();

    for i in 0..n_concurrent {
        let name = format!("/concurrent-{i}.deb");
        ctx.upstream.register_file(&name, file_size).await;

        let ctx = Arc::clone(&ctx);
        handles.push(tokio::spawn(async move {
            let body = ctx.get_bytes(&name).await;
            assert_eq!(body.len() as u64, file_size);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cache_hit() {
    let ctx = Arc::new(TestContext::new(1024 * 1024 * 1024).await);

    let file_size = 1024 * 1024; // 1 MB
    ctx.upstream.register_file("/cached.deb", file_size).await;
    let body = ctx.get_bytes("/cached.deb").await;
    assert_eq!(body.len() as u64, file_size);

    let n_clients = 100;
    let mut handles = Vec::new();

    for _ in 0..n_clients {
        let ctx = Arc::clone(&ctx);
        handles.push(tokio::spawn(async move {
            let body = ctx.get_bytes("/cached.deb").await;
            assert_eq!(body.len() as u64, file_size);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let count = ctx.upstream.request_count("/cached.deb").await;
    assert!(
        count <= 3,
        "expected ≤3 HEAD requests to upstream (cache HIT), got {count}"
    );
}
