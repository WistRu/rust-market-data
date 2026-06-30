use anyhow::{Context, Result};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcPublicRuntimeConfig,
};

#[tokio::main]
async fn main() -> Result<()> {
    let symbol = std::env::var("MEXC_GAP_SYMBOL").unwrap_or_else(|_| "MX_USDT".to_string());
    let startup_wait_seconds = std::env::var("MEXC_GAP_STARTUP_WAIT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30)
        .max(1);

    let connector = MexcConnector::default();
    let mut runtime = connector
        .public_runtime_builder()
        .connect_managed_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await
        .context("connect managed stateful with deep snapshot")?;

    let startup = runtime
        .await_balanced_startup(std::time::Duration::from_secs(startup_wait_seconds))
        .await
        .context("await balanced startup")?;

    let refresh = runtime
        .refresh_futures_contract_symbols_with_report(std::slice::from_ref(&symbol))
        .await
        .with_context(|| format!("targeted refresh for {symbol}"))?;

    let after_state_present = runtime.state().futures_symbols.contains_key(&symbol);
    let after_has_contract = runtime
        .state()
        .futures_symbols
        .get(&symbol)
        .and_then(|item| item.contract.as_ref())
        .is_some();

    println!(
        "startup_ready={} coverage={}/{} symbol={} requested_symbols={:?} unresolved_requested_symbols={:?} used_full_snapshot_fallback={} refreshed_contracts={} added={} updated={} removed={} unchanged={} after_state_present={} after_has_contract={}",
        startup.is_ready(),
        startup.seen_count(),
        startup.expected_count(),
        symbol,
        refresh.requested_symbols,
        refresh.unresolved_requested_symbols,
        refresh.used_full_snapshot_fallback,
        refresh.refreshed_contracts,
        refresh.added,
        refresh.updated,
        refresh.removed,
        refresh.unchanged,
        after_state_present,
        after_has_contract
    );

    Ok(())
}
