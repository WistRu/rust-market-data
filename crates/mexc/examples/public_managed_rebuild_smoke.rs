use anyhow::{Context, Result, ensure};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcPublicRuntimeConfig,
};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let mut runtime = connector
        .public_runtime_builder()
        .connect_managed_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await
        .context("connect managed stateful with deep snapshot")?;

    println!(
        "manifest spot_connections={} futures_connections={} spot_subscriptions={} futures_subscriptions={}",
        runtime.manifest().spot_connection_count,
        runtime.manifest().futures_connection_count,
        runtime.manifest().spot_subscription_count,
        runtime.manifest().futures_subscription_count
    );
    if let Some(report) = runtime.deep_report() {
        println!(
            "reference_modes index={} fair={} funding={}",
            report.index_price_mode.as_str(),
            report.fair_price_mode.as_str(),
            report.funding_rate_mode.as_str()
        );
    }

    let first_startup = runtime
        .await_balanced_startup(Duration::from_secs(30))
        .await
        .context("await initial balanced startup")?;
    println!(
        "startup#1 ready={} coverage={}/{} observed_live_events={} rebuilds={}",
        first_startup.is_ready(),
        first_startup.seen_count(),
        first_startup.expected_count(),
        first_startup.observed_live_events,
        runtime.total_rebuilds()
    );
    ensure!(
        first_startup.is_ready(),
        "initial startup did not reach readiness"
    );

    let (rebuilt_startup, rebuilt_gap_warmup) = runtime
        .rebuild_and_await_balanced_startup(Duration::from_secs(30))
        .await
        .context("rebuild runtime and await balanced startup")?;
    println!(
        "startup#2 ready={} coverage={}/{} observed_live_events={} contract_gap_warmup_passes={} contract_gap_initial={} contract_gap_final={} contract_gap_stop_reason={:?} rebuilds={}",
        rebuilt_startup.is_ready(),
        rebuilt_startup.seen_count(),
        rebuilt_startup.expected_count(),
        rebuilt_startup.observed_live_events,
        rebuilt_gap_warmup.passes,
        rebuilt_gap_warmup.initial_gap_count(),
        rebuilt_gap_warmup.final_gap_count(),
        rebuilt_gap_warmup.stop_reason,
        runtime.total_rebuilds()
    );
    ensure!(
        rebuilt_startup.is_ready(),
        "rebuilt runtime did not reach readiness"
    );
    ensure!(
        runtime.total_rebuilds() >= 1,
        "managed runtime did not count rebuild"
    );

    if let Some(symbol) = runtime.state().futures_symbols.get("BTC_USDT") {
        println!(
            "post_rebuild BTC_USDT has_contract={} has_ticker={} has_index={} has_fair={} has_funding_snapshot={}",
            symbol.contract.is_some(),
            symbol.ticker.is_some(),
            symbol.index_price.is_some(),
            symbol.fair_price.is_some(),
            symbol.funding_rate_snapshot.is_some()
        );
    }

    Ok(())
}
