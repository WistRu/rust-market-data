use anyhow::{Context, Result};
use mexc::MexcConnector;

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let snapshot = connector
        .public_runtime_builder()
        .bootstrap_snapshot()
        .await
        .context("bootstrap public snapshot")?;

    println!(
        "spot default_symbols={} exchange_symbols={} ticker24h={} price_tickers={} book_tickers={}",
        snapshot.spot.default_symbols.data.len(),
        snapshot.spot.exchange_info.symbols.len(),
        snapshot.spot.ticker_24hr.len(),
        snapshot.spot.price_tickers.len(),
        snapshot.spot.book_tickers.len()
    );
    println!(
        "futures contracts={} tickers={} transferable_currencies={} insurance_balances={}",
        snapshot.futures.contracts.len(),
        snapshot.futures.tickers.len(),
        snapshot.futures.transferable_currencies.len(),
        snapshot.futures.insurance_balances.len()
    );

    Ok(())
}
