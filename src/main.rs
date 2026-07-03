use tracing_subscriber;

use apt_blitz::config::Config;
use apt_blitz::run_proxy;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "apt_blitz=info".into()),
        )
        .init();

    let config = Config::load()?;
    run_proxy(config).await
}
