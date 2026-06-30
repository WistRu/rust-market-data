use anyhow::Result;
use binance::{BinancePublicRestClient, OneOrMany};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = BinancePublicRestClient::default();
    let spot_symbol =
        std::env::var("BINANCE_SPOT_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let futures_symbol =
        std::env::var("BINANCE_FUTURES_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    rest.spot_ping().await?;
    println!("ok spot_ping");
    println!(
        "ok spot_time {}",
        rest.spot_server_time().await?.server_time
    );
    println!(
        "ok spot_exchange_info {}",
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
        "ok spot_historical_trades {}",
        rest.spot_historical_trades(&spot_symbol, Some(2), None)
            .await?
            .len()
    );
    println!(
        "ok spot_agg_trades {}",
        rest.spot_aggregate_trades(&spot_symbol, Some(2), None, None, None)
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
        "ok spot_ui_klines {}",
        rest.spot_ui_klines(&spot_symbol, "1m", Some(2), None, None)
            .await?
            .len()
    );
    println!(
        "ok spot_avg_price {}",
        rest.spot_average_price(&spot_symbol).await?
    );
    println!(
        "ok spot_ticker_24hr {}",
        rest.spot_ticker_24hr(Some(&spot_symbol)).await?.len()
    );
    println!(
        "ok spot_trading_day_ticker {}",
        rest.spot_trading_day_ticker(Some(&spot_symbol))
            .await?
            .len()
    );
    println!(
        "ok spot_price_ticker {}",
        rest.spot_price_ticker(Some(&spot_symbol)).await?.len()
    );
    println!(
        "ok spot_book_ticker {}",
        rest.spot_book_ticker(Some(&spot_symbol)).await?.len()
    );
    println!(
        "ok spot_rolling_window_ticker {}",
        rest.spot_rolling_window_ticker(Some(&spot_symbol), Some("1h"))
            .await?
            .len()
    );
    println!(
        "ok spot_reference_price {}",
        rest.spot_reference_price(Some(&spot_symbol)).await?.len()
    );
    println!(
        "ok spot_reference_price_calculation {}",
        rest.spot_reference_price_calculation(&spot_symbol).await?
    );

    rest.futures_ping().await?;
    println!("ok futures_ping");
    println!(
        "ok futures_time {}",
        rest.futures_server_time().await?.server_time
    );
    println!(
        "ok futures_exchange_info {}",
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
    match rest
        .futures_historical_trades(&futures_symbol, Some(2), None)
        .await
    {
        Ok(items) => println!("ok futures_historical_trades {}", items.len()),
        Err(error) => println!("skip futures_historical_trades {error}"),
    }
    println!(
        "ok futures_agg_trades {}",
        rest.futures_aggregate_trades(&futures_symbol, Some(2), None, None, None)
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
        "ok futures_continuous_klines {}",
        rest.futures_continuous_klines(&futures_symbol, "PERPETUAL", "1m", Some(2), None, None)
            .await?
            .len()
    );
    println!(
        "ok futures_index_price_klines {}",
        rest.futures_index_price_klines(&futures_symbol, "1m", Some(2), None, None)
            .await?
            .len()
    );
    println!(
        "ok futures_mark_price_klines {}",
        rest.futures_mark_price_klines(&futures_symbol, "1m", Some(2), None, None)
            .await?
            .len()
    );
    println!(
        "ok futures_premium_index_klines {}",
        rest.futures_premium_index_klines(&futures_symbol, "1m", Some(2), None, None)
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
        rest.futures_funding_rate(Some(&futures_symbol), Some(2), None, None)
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
    println!(
        "ok futures_price_ticker_v2 {}",
        rest.futures_price_ticker_v2(Some(&futures_symbol))
            .await?
            .len()
    );
    match rest.futures_book_ticker(Some(&futures_symbol)).await? {
        OneOrMany::One(_) => println!("ok futures_book_ticker 1"),
        OneOrMany::Many(items) => println!("ok futures_book_ticker {}", items.len()),
    }
    println!(
        "ok futures_open_interest {}",
        rest.futures_open_interest(&futures_symbol).await?
    );
    println!(
        "ok futures_index_info {}",
        rest.futures_index_info(None).await?.len()
    );
    println!(
        "ok futures_asset_index {}",
        rest.futures_asset_index(Some("BTCUSD")).await?.len()
    );
    println!(
        "ok futures_constituents {}",
        rest.futures_constituents(&futures_symbol).await?
    );
    println!(
        "ok futures_symbol_adl_risk {}",
        rest.futures_symbol_adl_risk(Some(&futures_symbol))
            .await?
            .len()
    );
    println!(
        "ok futures_basis {}",
        rest.futures_basis(&futures_symbol, "PERPETUAL", "5m", Some(2), None, None)
            .await?
            .len()
    );

    Ok(())
}
