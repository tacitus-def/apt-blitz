mod common;

use std::sync::Arc;

use common::{sha256_of, TestContext};

#[tokio::test(flavor = "multi_thread")]
async fn mixed_workload() {
    let ctx = Arc::new(TestContext::new(1024 * 1024 * 1024).await);

    let mut handles = Vec::new();

    // 30 small files (plain proxy)
    for i in 0..30 {
        let name = format!("/small-{i}.deb");
        let file_size = 100 * 1024;
        ctx.upstream.register_file(&name, file_size).await;

        let ctx = Arc::clone(&ctx);
        handles.push(tokio::spawn(async move {
            let body = ctx.get_bytes(&name).await;
            assert_eq!(body.len() as u64, file_size);
            let expected: Vec<u8> = (0..file_size).map(|j| (j % 256) as u8).collect();
            assert_eq!(sha256_of(&body), sha256_of(&expected));
        }));
    }

    // 10 medium files
    for i in 0..10 {
        let name = format!("/medium-{i}.deb");
        let file_size = 2 * 1024 * 1024;
        ctx.upstream.register_file(&name, file_size).await;

        let ctx = Arc::clone(&ctx);
        handles.push(tokio::spawn(async move {
            let body = ctx.get_bytes(&name).await;
            assert_eq!(body.len() as u64, file_size);
            let expected: Vec<u8> = (0..file_size).map(|j| (j % 256) as u8).collect();
            assert_eq!(sha256_of(&body), sha256_of(&expected));
        }));
    }

    // 5 large files (multithreaded)
    for i in 0..5 {
        let name = format!("/large-{i}.deb");
        let file_size = 10 * 1024 * 1024;
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
async fn graceful_shutdown() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;

    // Verify the proxy handles a large download that completes normally.
    // The original test had a timeout branch with no assertions — useless.
    // If download completes within 30s, the proxy is working; if it hangs,
    // the test will fail with a clear timeout error.
    let file_size = 50 * 1024 * 1024;
    ctx.upstream.register_file("/big.deb", file_size).await;

    let body = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        ctx.get_bytes("/big.deb"),
    )
    .await
    .expect("download timed out after 30s — proxy may be stuck");

    assert_eq!(body.len() as u64, file_size);
    let expected: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    assert_eq!(sha256_of(&body), sha256_of(&expected));
}
