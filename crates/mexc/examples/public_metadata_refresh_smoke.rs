use anyhow::{Result, ensure};
use mexc::{MexcConnector, MexcPublicRuntimeConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let mut runtime = MexcConnector::default()
        .public_runtime_builder()
        .connect_managed_stateful_with_snapshot(MexcPublicRuntimeConfig::balanced())
        .await?;

    let before_spot_symbols = runtime.state().spot_symbols.len();
    let before_futures_symbols = runtime.state().futures_symbols.len();
    let before_exchange_symbols = runtime
        .state()
        .spot_exchange_info
        .as_ref()
        .map(|info| info.symbols.len())
        .unwrap_or(0);

    let refresh = runtime
        .refresh_public_metadata_snapshot_with_report()
        .await?;
    let after_state = runtime.state();

    ensure!(
        after_state.spot_server_time.is_some(),
        "spot server time missing after refresh"
    );
    ensure!(
        after_state.futures_server_time.is_some(),
        "futures server time missing after refresh"
    );
    ensure!(
        after_state.spot_exchange_info.is_some(),
        "spot exchange info missing after refresh"
    );
    ensure!(
        !after_state.spot_default_symbols.is_empty(),
        "spot default symbols missing after refresh"
    );
    ensure!(
        after_state.spot_symbols.contains_key("BTCUSDT"),
        "BTCUSDT missing from spot state after refresh"
    );
    ensure!(
        after_state.futures_symbols.contains_key("BTC_USDT"),
        "BTC_USDT missing from futures state after refresh"
    );

    println!(
        "spot_symbols_before={} spot_symbols_after={} futures_symbols_before={} futures_symbols_after={} spot_exchange_symbols_before={} spot_exchange_symbols_after={}",
        before_spot_symbols,
        after_state.spot_symbols.len(),
        before_futures_symbols,
        after_state.futures_symbols.len(),
        before_exchange_symbols,
        after_state
            .spot_exchange_info
            .as_ref()
            .map(|info| info.symbols.len())
            .unwrap_or(0)
    );
    println!(
        "public_metadata_refresh spot_server_time={}=>{} spot_default_symbols={}=>{} spot_offline_symbols={}=>{} spot_exchange_symbols={}=>{} futures_server_time={}=>{} futures_transferable_currencies={}=>{} futures_insurance_balances={}=>{}",
        refresh
            .spot_server_time_before
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        refresh.spot_server_time_after,
        refresh.spot_default_symbol_count_before,
        refresh.spot_default_symbol_count_after,
        refresh.spot_offline_symbol_count_before,
        refresh.spot_offline_symbol_count_after,
        refresh.spot_exchange_symbol_count_before,
        refresh.spot_exchange_symbol_count_after,
        refresh
            .futures_server_time_before
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        refresh.futures_server_time_after,
        refresh.futures_transferable_currency_count_before,
        refresh.futures_transferable_currency_count_after,
        refresh.futures_insurance_balance_count_before,
        refresh.futures_insurance_balance_count_after
    );

    Ok(())
}
