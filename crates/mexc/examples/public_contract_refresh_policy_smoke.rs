use anyhow::{Result, ensure};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcManagedRuntimePolicy,
    MexcPublicRuntimeConfig,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let refresh_after_seconds = std::env::var("MEXC_CONTRACT_REFRESH_AFTER_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5);
    let watch_seconds = std::env::var("MEXC_CONTRACT_REFRESH_WATCH_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20);

    let mut runtime = MexcConnector::default()
        .public_runtime_builder()
        .connect_managed_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await?;

    let startup = runtime
        .await_balanced_startup(Duration::from_secs(20))
        .await?;
    ensure!(startup.is_ready(), "startup did not become ready");

    let mut policy = MexcManagedRuntimePolicy::balanced_defaults();
    policy.contract_refresh_interval = Some(Duration::from_secs(refresh_after_seconds));
    policy.contract_event_refresh = false;
    policy.contract_gap_refresh = false;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(watch_seconds);
    while tokio::time::Instant::now() < deadline {
        let Some(step) = runtime.next_step(&policy).await else {
            break;
        };
        let step = step?;
        if let Some(refresh) = step.contract_refresh {
            println!(
                "startup_ready={} coverage={}/{} refreshed_contracts={} added={} updated={} removed={} unchanged={}",
                startup.is_ready(),
                startup.seen_count(),
                startup.expected_count(),
                refresh.refreshed_contracts,
                refresh.added,
                refresh.updated,
                refresh.removed,
                refresh.unchanged
            );
            println!(
                "contract_refresh_symbols added={:?} updated={:?} removed={:?}",
                refresh.added_symbols, refresh.updated_symbols, refresh.removed_symbols
            );
            println!("contract_refresh_changes {:?}", refresh.changes);
            ensure!(
                refresh.refreshed_contracts > 0,
                "contract refresh returned zero contracts"
            );
            return Ok(());
        }
    }

    anyhow::bail!("contract refresh did not trigger within watch window")
}
