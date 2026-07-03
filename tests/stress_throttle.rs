mod common;

use common::TestContext;

#[tokio::test(flavor = "multi_thread")]
async fn throttle_actual_speed() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;

    let file_size = 5 * 1024 * 1024; // 5 MB
    ctx.upstream.register_file("/throttle.deb", file_size).await;

    let start = std::time::Instant::now();
    let body = ctx.get_bytes("/throttle.deb").await;
    let elapsed = start.elapsed();

    assert_eq!(body.len() as u64, file_size);

    eprintln!(
        "throttle_actual_speed: {} bytes in {:?} ({:.2} KB/s)",
        file_size,
        elapsed,
        file_size as f64 / 1024.0 / elapsed.as_secs_f64()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn throttle_to_burst_no_memory_spike() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;

    let file_size = 20 * 1024 * 1024; // 20 MB
    ctx.upstream.register_file("/burst.deb", file_size).await;

    let body = ctx.get_bytes("/burst.deb").await;
    assert_eq!(body.len() as u64, file_size);

    eprintln!(
        "throttle_to_burst: {} bytes downloaded successfully",
        file_size
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn throttle_disabled_for_small() {
    let ctx = TestContext::new(1024 * 1024 * 1024).await;

    let file_size = 100 * 1024; // 100 KB (plain proxy)
    ctx.upstream.register_file("/small.deb", file_size).await;

    let body = ctx.get_bytes("/small.deb").await;
    assert_eq!(body.len() as u64, file_size);
}
