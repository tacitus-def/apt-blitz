mod common;

use common::TestContext;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

async fn mount_error(mockup: &common::MockUpstream, name: &str, status: u16) {
    let status = ResponseTemplate::new(status);
    Mock::given(method("HEAD"))
        .and(path(name))
        .respond_with(status.clone())
        .mount(&mockup.server)
        .await;
    Mock::given(method("GET"))
        .and(path(name))
        .respond_with(status)
        .mount(&mockup.server)
        .await;
}

async fn assert_upstream_status_forwarded(ctx: &TestContext, name: &str, status: u16) {
    let head_requests = ctx.upstream.request_count_by_method(name, "HEAD").await;
    let resp = ctx.get(name).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        status,
        "proxy must forward the upstream status code to the client"
    );
    let resp = ctx.get(name).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        status,
        "second request must also receive the upstream status (404/403 must not be cached)"
    );
    let head_after = ctx.upstream.request_count_by_method(name, "HEAD").await;
    assert_eq!(
        head_after,
        head_requests + 2,
        "each client request must re-probe upstream (error responses must not be cached)"
    );
}

#[tokio::test]
async fn forwards_404_not_found_status() {
    let ctx = TestContext::new(1024 * 1024).await;
    mount_error(&ctx.upstream, "/missing.deb", 404).await;
    assert_upstream_status_forwarded(&ctx, "/missing.deb", 404).await;
}

#[tokio::test]
async fn forwards_403_forbidden_status() {
    let ctx = TestContext::new(1024 * 1024).await;
    mount_error(&ctx.upstream, "/forbidden.deb", 403).await;
    assert_upstream_status_forwarded(&ctx, "/forbidden.deb", 403).await;
}
