mod common;

use std::sync::Arc;

use common::{sha256_of, TestContext};

#[tokio::test(flavor = "multi_thread")]
async fn sequential_same_url() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;

    let file_size = 512 * 1024;
    ctx.upstream.register_file("/seq.deb", file_size).await;

    let body1 = ctx.get_bytes("/seq.deb").await;
    assert_eq!(body1.len() as u64, file_size);
    let expected: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    assert_eq!(sha256_of(&body1), sha256_of(&expected));

    let body2 = ctx.get_bytes("/seq.deb").await;
    assert_eq!(body2.len() as u64, file_size);
    assert_eq!(sha256_of(&body2), sha256_of(&expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_same_url() {
    let ctx = Arc::new(TestContext::new(1024 * 1024 * 1024).await);

    let file_size = 512 * 1024;
    ctx.upstream.register_file("/shared.deb", file_size).await;

    let expected: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    let expected_clone = expected.clone();

    let ctx1 = Arc::clone(&ctx);
    let ctx2 = Arc::clone(&ctx);
    let (r1, r2) = tokio::join!(
        async move { ctx1.get_bytes("/shared.deb").await },
        async move { ctx2.get_bytes("/shared.deb").await },
    );
    assert_eq!(r1.len() as u64, file_size);
    assert_eq!(r2.len() as u64, file_size);
    assert_eq!(sha256_of(&r1), sha256_of(&expected));
    assert_eq!(sha256_of(&r2), sha256_of(&expected_clone));

    // Third fetch from cache
    let body3 = ctx.get_bytes("/shared.deb").await;
    assert_eq!(body3.len() as u64, file_size);
    assert_eq!(sha256_of(&body3), sha256_of(&expected_clone));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_different_urls() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;

    let file_size = 256 * 1024;
    ctx.upstream.register_file("/a.deb", file_size).await;
    ctx.upstream.register_file("/b.deb", file_size).await;

    let expected_a: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    let expected_b = expected_a.clone();

    let (r1, r2) = tokio::join!(
        ctx.get_bytes("/a.deb"),
        ctx.get_bytes("/b.deb"),
    );
    assert_eq!(r1.len() as u64, file_size);
    assert_eq!(r2.len() as u64, file_size);
    assert_eq!(sha256_of(&r1), sha256_of(&expected_a));
    assert_eq!(sha256_of(&r2), sha256_of(&expected_b));
}

#[tokio::test(flavor = "multi_thread")]
async fn many_unique_urls_concurrent() {
    let ctx = Arc::new(TestContext::new(1024 * 1024 * 1024).await);

    let file_size = 64 * 1024;
    let n_urls = 50;

    let mut handles = Vec::new();
    for i in 0..n_urls {
        let name = format!("/conc-{i}.deb");
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
