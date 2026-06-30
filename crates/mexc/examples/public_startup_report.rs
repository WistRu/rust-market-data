use anyhow::{Context, Result};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcPublicRuntimeConfig,
};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let mut runtime = connector
        .public_runtime_builder()
        .connect_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await
        .context("connect stateful with deep snapshot")?;

    println!(
        "manifest spot_connections={} futures_connections={} spot_subscriptions={} futures_subscriptions={}",
        runtime.manifest.spot_connection_count,
        runtime.manifest.futures_connection_count,
        runtime.manifest.spot_subscription_count,
        runtime.manifest.futures_subscription_count
    );
    if let Some(report) = &runtime.deep_report {
        println!(
            "reference_modes index={} fair={} funding={}",
            report.index_price_mode.as_str(),
            report.fair_price_mode.as_str(),
            report.funding_rate_mode.as_str()
        );
        println!(
            "reference_sources index_bulk={} index_endpoint={} fair_bulk={} fair_endpoint={} funding_bulk={} funding_endpoint={}",
            report.index_price_bulk_count,
            report.index_price_endpoint_count,
            report.fair_price_bulk_count,
            report.fair_price_endpoint_count,
            report.funding_rate_bulk_count,
            report.funding_rate_endpoint_count
        );
    }

    let report = runtime
        .await_balanced_startup(Duration::from_secs(30))
        .await
        .context("wait for startup kinds")?;

    println!(
        "startup_ready={} coverage={}/{}",
        report.is_ready(),
        report.seen_count(),
        report.expected_count()
    );
    println!("observed_live_events={}", report.observed_live_events);
    let seen = report
        .seen
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let missing = report
        .missing
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",");
    println!("seen_kinds={seen}");
    println!("missing_kinds={missing}");

    Ok(())
}
