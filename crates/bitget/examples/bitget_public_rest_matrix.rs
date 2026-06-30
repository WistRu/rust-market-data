use anyhow::Result;
use bitget::{BitgetInstType, BitgetPublicRestClient, DEFAULT_SYMBOL};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = BitgetPublicRestClient::default();
    let symbol = std::env::var("BITGET_SYMBOL").unwrap_or_else(|_| DEFAULT_SYMBOL.to_string());

    println!("ok spot_symbols {}", rest.spot_symbols(None).await?.len());
    println!(
        "ok usdt_futures_contracts {}",
        rest.futures_contracts(BitgetInstType::UsdtFutures, None)
            .await?
            .len()
    );
    println!("ok identity {} SPOT", symbol);
    println!("ok identity {} USDT-FUTURES", symbol);
    println!(
        "ok spot_orderbook bids={}",
        rest.spot_order_book(&symbol, Some(5)).await?.bids.len()
    );
    println!(
        "ok usdt_futures_orderbook bids={}",
        rest.futures_order_book(BitgetInstType::UsdtFutures, &symbol, Some(5))
            .await?
            .bids
            .len()
    );
    println!(
        "ok spot_ticker rows={}",
        rest.spot_tickers(Some(&symbol)).await?.len()
    );
    println!(
        "ok usdt_futures_ticker rows={}",
        rest.futures_tickers(BitgetInstType::UsdtFutures, Some(&symbol))
            .await?
            .len()
    );
    println!(
        "ok spot_trades {}",
        rest.spot_trades(&symbol, Some(5)).await?.len()
    );
    println!(
        "ok usdt_futures_trades {}",
        rest.futures_trades(BitgetInstType::UsdtFutures, &symbol, Some(5))
            .await?
            .len()
    );

    Ok(())
}
