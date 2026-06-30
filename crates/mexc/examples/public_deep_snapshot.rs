use anyhow::{Context, Result};
use mexc::{DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcPublicState};

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let builder = connector.public_runtime_builder();
    let snapshot = builder
        .bootstrap_deep_snapshot(DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY)
        .await
        .context("bootstrap deep snapshot")?;

    println!(
        "base spot_symbols={} futures_contracts={} futures_tickers={} futures_insurance={}",
        snapshot.base.spot.exchange_info.symbols.len(),
        snapshot.base.futures.contracts.len(),
        snapshot.base.futures.tickers.len(),
        snapshot.base.futures.insurance_balances.len()
    );
    println!(
        "reference index_prices={} fair_prices={} funding_rates={}",
        snapshot.futures_reference.index_prices.len(),
        snapshot.futures_reference.fair_prices.len(),
        snapshot.futures_reference.funding_rates.len()
    );
    println!(
        "reference_modes index={} fair={} funding={}",
        snapshot.report.index_price_mode.as_str(),
        snapshot.report.fair_price_mode.as_str(),
        snapshot.report.funding_rate_mode.as_str()
    );
    println!(
        "reference_sources index_bulk={} index_endpoint={} fair_bulk={} fair_endpoint={} funding_bulk={} funding_endpoint={}",
        snapshot.report.index_price_bulk_count,
        snapshot.report.index_price_endpoint_count,
        snapshot.report.fair_price_bulk_count,
        snapshot.report.fair_price_endpoint_count,
        snapshot.report.funding_rate_bulk_count,
        snapshot.report.funding_rate_endpoint_count
    );

    let state = MexcPublicState::from_deep_snapshot(&snapshot);

    if let Some(symbol) = state.futures_symbols.get("BTC_USDT") {
        println!(
            "futures BTC_USDT has_contract={} has_ticker={} has_index={} has_fair={} has_funding_snapshot={}",
            symbol.contract.is_some(),
            symbol.ticker.is_some(),
            symbol.index_price.is_some(),
            symbol.fair_price.is_some(),
            symbol.funding_rate_snapshot.is_some()
        );
    }

    Ok(())
}
