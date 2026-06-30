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
    if let Some(symbol) = runtime.state().futures_symbols.get("BTC_USDT") {
        println!(
            "initial BTC_USDT has_contract={} has_ticker={} has_index={} has_fair={} has_funding_snapshot={}",
            symbol.contract.is_some(),
            symbol.ticker.is_some(),
            symbol.index_price.is_some(),
            symbol.fair_price.is_some(),
            symbol.funding_rate_snapshot.is_some()
        );
    }

    let mut live_events = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while tokio::time::Instant::now() < deadline && live_events < 20 {
        let event = runtime
            .next_live_event()
            .await
            .transpose()?
            .context("deep runtime stream ended unexpectedly")?;
        if live_events < 3 {
            println!(
                "live_kind#{}={}",
                live_events + 1,
                event.kind().expect("live kind").as_str()
            );
        }
        live_events += 1;
    }

    println!("live_events={live_events}");
    if let Some(symbol) = runtime.state().futures_symbols.get("BTC_USDT") {
        println!(
            "post_live BTC_USDT has_contract={} has_ticker={} has_depth={} has_index={} has_fair={} has_funding_snapshot={} has_funding_live={}",
            symbol.contract.is_some(),
            symbol.ticker.is_some(),
            symbol.depth.is_some(),
            symbol.index_price.is_some(),
            symbol.fair_price.is_some(),
            symbol.funding_rate_snapshot.is_some(),
            symbol.funding_rate_live.is_some()
        );
    }

    Ok(())
}
