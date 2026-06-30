use anyhow::{Context, Result, ensure};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector,
    MexcManagedContractRefreshCause, MexcPublicRuntimeConfig,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let symbol =
        std::env::var("MEXC_TARGET_CONTRACT_SYMBOL").unwrap_or_else(|_| "BTC_USDT".to_string());

    let mut runtime = MexcConnector::default()
        .public_runtime_builder()
        .connect_managed_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await
        .context("connect managed stateful with deep snapshot")?;
    let startup = runtime
        .await_balanced_startup(Duration::from_secs(20))
        .await
        .context("await balanced startup")?;
    ensure!(startup.is_ready(), "startup did not become ready");

    let before = runtime
        .state_mut()
        .futures_symbols
        .get_mut(&symbol)
        .context("target symbol missing from futures state before targeted refresh")?;
    before.contract = None;

    let refresh = runtime
        .refresh_futures_contract_symbols_with_report(std::slice::from_ref(&symbol))
        .await
        .context("refresh targeted futures contract")?;

    let after_has_contract = runtime
        .state()
        .futures_symbols
        .get(&symbol)
        .and_then(|entry| entry.contract.as_ref())
        .is_some();

    ensure!(
        matches!(refresh.cause, MexcManagedContractRefreshCause::Targeted),
        "expected targeted refresh cause"
    );
    ensure!(
        refresh.requested_symbols == vec![symbol.clone()],
        "unexpected requested symbols: {:?}",
        refresh.requested_symbols
    );
    ensure!(
        refresh.refreshed_contracts > 0,
        "targeted refresh returned zero contracts"
    );
    ensure!(
        after_has_contract,
        "target contract missing after targeted refresh"
    );

    println!(
        "startup_ready={} coverage={}/{} symbol={} cause=targeted requested_symbols={:?} refreshed_contracts={} added={} updated={} removed={} unchanged={} after_has_contract={}",
        startup.is_ready(),
        startup.seen_count(),
        startup.expected_count(),
        symbol,
        refresh.requested_symbols,
        refresh.refreshed_contracts,
        refresh.added,
        refresh.updated,
        refresh.removed,
        refresh.unchanged,
        after_has_contract
    );
    println!(
        "contract_refresh_symbols added={:?} updated={:?} removed={:?}",
        refresh.added_symbols, refresh.updated_symbols, refresh.removed_symbols
    );
    println!("contract_refresh_changes {:?}", refresh.changes);

    Ok(())
}
