use anyhow::Result;
use bybit::{BybitCategory, BybitPublicRestClient};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = BybitPublicRestClient::default();
    let spot_symbol = std::env::var("BYBIT_SPOT_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let linear_symbol =
        std::env::var("BYBIT_LINEAR_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    println!("ok time {}", rest.server_time().await?.time_second);
    println!(
        "ok spot_instruments {}",
        rest.instruments_info(BybitCategory::Spot).await?.list.len()
    );
    println!(
        "ok linear_instruments {}",
        rest.instruments_info(BybitCategory::Linear)
            .await?
            .list
            .len()
    );
    println!(
        "ok spot_orderbook bids={}",
        rest.order_book(BybitCategory::Spot, &spot_symbol, Some(50))
            .await?
            .b
            .len()
    );
    println!(
        "ok linear_orderbook bids={}",
        rest.order_book(BybitCategory::Linear, &linear_symbol, Some(50))
            .await?
            .b
            .len()
    );
    rest.tickers(BybitCategory::Spot, Some(&spot_symbol))
        .await?;
    println!("ok spot_tickers");
    rest.tickers(BybitCategory::Linear, Some(&linear_symbol))
        .await?;
    println!("ok linear_tickers");

    Ok(())
}
