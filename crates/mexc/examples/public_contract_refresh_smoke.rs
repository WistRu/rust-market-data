use anyhow::{Result, ensure};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcPublicRuntimeConfig,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
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

    let before_has_contract = runtime
        .state()
        .futures_symbols
        .get("BTC_USDT")
        .and_then(|symbol| symbol.contract.as_ref())
        .is_some();
    ensure!(
        before_has_contract,
        "BTC_USDT contract missing before refresh"
    );

    let refresh = runtime
        .refresh_futures_contract_snapshot_with_report()
        .await?;

    let after_has_contract = runtime
        .state()
        .futures_symbols
        .get("BTC_USDT")
        .and_then(|symbol| symbol.contract.as_ref())
        .is_some();
    ensure!(
        after_has_contract,
        "BTC_USDT contract missing after refresh"
    );

    println!(
        "startup_ready={} coverage={}/{} refreshed_contracts={} btc_has_contract_before={} btc_has_contract_after={}",
        startup.is_ready(),
        startup.seen_count(),
        startup.expected_count(),
        refresh.refreshed_contracts,
        before_has_contract,
        after_has_contract
    );
    println!(
        "contract_refresh_delta added={} updated={} removed={} unchanged={}",
        refresh.added, refresh.updated, refresh.removed, refresh.unchanged
    );
    println!(
        "contract_refresh_symbols added={:?} updated={:?} removed={:?}",
        refresh.added_symbols, refresh.updated_symbols, refresh.removed_symbols
    );
    println!("contract_refresh_changes {:?}", refresh.changes);

    Ok(())
}
