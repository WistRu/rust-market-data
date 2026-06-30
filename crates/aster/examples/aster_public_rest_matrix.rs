use anyhow::Result;
use aster::{AsterPublicRestClient, OneOrMany};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = AsterPublicRestClient::default();
    let spot_symbol = std::env::var("ASTER_SPOT_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let futures_symbol =
        std::env::var("ASTER_FUTURES_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    rest.spot_ping().await?;
    println!("ok spot_ping");
    println!(
        "ok spot_time {}",
        rest.spot_server_time().await?.server_time
    );
    println!(
        "ok spot_exchange_info symbols={}",
        rest.spot_exchange_info(Some(&spot_symbol))
            .await?
            .symbols
            .len()
    );
    println!(
        "ok spot_depth bids={}",
        rest.spot_order_book(&spot_symbol, Some(5))
            .await?
            .bids
            .len()
    );
    println!(
        "ok spot_trades {}",
        rest.spot_recent_trades(&spot_symbol, Some(2)).await?.len()
    );
    println!(
        "ok spot_agg_trades {}",
        rest.spot_aggregate_trades(&spot_symbol, Some(2))
            .await?
            .len()
    );
    println!(
        "ok spot_klines {}",
        rest.spot_klines(&spot_symbol, "1m", Some(2), None, None)
            .await?
            .len()
    );
    println!(
        "ok spot_ticker_24hr {}",
        rest.spot_ticker_24hr(Some(&spot_symbol)).await?.len()
    );
    println!(
        "ok spot_price_ticker {}",
        rest.spot_price_ticker(Some(&spot_symbol)).await?.len()
    );
    println!(
        "ok spot_book_ticker {}",
        rest.spot_book_ticker(Some(&spot_symbol)).await?.len()
    );

    rest.futures_ping().await?;
    println!("ok futures_ping");
    println!(
        "ok futures_time {}",
        rest.futures_server_time().await?.server_time
    );
    println!(
        "ok futures_exchange_info symbols={}",
        rest.futures_exchange_info(Some(&futures_symbol))
            .await?
            .symbols
            .len()
    );
    println!(
        "ok futures_depth bids={}",
        rest.futures_order_book(&futures_symbol, Some(5))
            .await?
            .bids
            .len()
    );
    println!(
        "ok futures_trades {}",
        rest.futures_recent_trades(&futures_symbol, Some(2))
            .await?
            .len()
    );
    println!(
        "ok futures_agg_trades {}",
        rest.futures_aggregate_trades(&futures_symbol, Some(2))
            .await?
            .len()
    );
    println!(
        "ok futures_klines {}",
        rest.futures_klines(&futures_symbol, "1m", Some(2), None, None)
            .await?
            .len()
    );
    println!(
        "ok futures_premium_index {}",
        rest.futures_premium_index(Some(&futures_symbol))
            .await?
            .len()
    );
    println!(
        "ok futures_funding_rate {}",
        rest.futures_funding_rate(Some(&futures_symbol), Some(2))
            .await?
            .len()
    );
    println!(
        "ok futures_ticker_24hr {}",
        rest.futures_ticker_24hr(Some(&futures_symbol)).await?.len()
    );
    println!(
        "ok futures_price_ticker {}",
        rest.futures_price_ticker(Some(&futures_symbol))
            .await?
            .len()
    );
    match rest.futures_book_ticker(Some(&futures_symbol)).await? {
        OneOrMany::One(_) => println!("ok futures_book_ticker 1"),
        OneOrMany::Many(items) => println!("ok futures_book_ticker {}", items.len()),
    }

    Ok(())
}
