use anyhow::Result;
use mexc::MexcConnector;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let startup_wait_seconds = std::env::var("MEXC_READY_STARTUP_WAIT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30)
        .max(1);

    let ready = MexcConnector::default()
        .connect_managed_balanced_ready(Duration::from_secs(startup_wait_seconds))
        .await?;

    println!(
        "startup_ready={} coverage={}/{} observed_live_events={} contract_gap_warmup_passes={} contract_gap_initial={} contract_gap_final={} contract_gap_stop_reason={:?} spot_connections={} futures_connections={} spot_subscriptions={} futures_subscriptions={}",
        ready.startup.is_ready(),
        ready.startup.seen_count(),
        ready.startup.expected_count(),
        ready.startup.observed_live_events,
        ready.contract_gap_warmup.passes,
        ready.contract_gap_warmup.initial_gap_count(),
        ready.contract_gap_warmup.final_gap_count(),
        ready.contract_gap_warmup.stop_reason,
        ready.runtime.manifest().spot_connection_count,
        ready.runtime.manifest().futures_connection_count,
        ready.runtime.manifest().spot_subscription_count,
        ready.runtime.manifest().futures_subscription_count
    );

    Ok(())
}
