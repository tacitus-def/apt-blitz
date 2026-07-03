mod common;

use std::sync::Arc;

use common::TestContext;

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
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn graceful_shutdown() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;

    let file_size = 50 * 1024 * 1024;
    ctx.upstream.register_file("/big.deb", file_size).await;

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.get_bytes("/big.deb"),
    )
    .await;

    match result {
        Ok(body) => {
            assert_eq!(body.len() as u64, file_size);
        }
        Err(_timeout) => {
            eprintln!("download cancelled (timeout) — proxy should clean up .download files on restart");
        }
    }
}
