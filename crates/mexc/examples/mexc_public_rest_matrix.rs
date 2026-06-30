use anyhow::{Context, Result};
use mexc::{MexcPublicRestClient, OneOrMany};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = MexcPublicRestClient::default();
    let spot_symbol = "BTCUSDT";
    let futures_symbol = "BTC_USDT";

    rest.spot_ping().await.context("spot_ping")?;
    println!("ok spot_ping");

    println!("ok futures_ping {}", rest.futures_ping().await?);
    println!(
        "ok spot_server_time {}",
        rest.spot_server_time().await?.server_time
    );
    println!(
        "ok spot_default_symbols {}",
        rest.spot_default_symbols().await?.data.len()
    );
    println!(
        "ok spot_offline_symbols {}",
        rest.spot_offline_symbols().await?.data.len()
    );
    println!(
        "ok spot_exchange_info {}",
        rest.spot_exchange_info().await?.symbols.len()
    );
    println!(
        "ok spot_order_book bids={}",
        rest.spot_order_book(spot_symbol, Some(5)).await?.bids.len()
    );
    println!(
        "ok spot_recent_trades {}",
        rest.spot_recent_trades(spot_symbol, Some(2)).await?.len()
    );
    println!(
        "ok spot_aggregate_trades {}",
        rest.spot_aggregate_trades(spot_symbol, Some(2))
            .await?
            .len()
    );
    println!(
        "ok spot_klines {}",
        rest.spot_klines(spot_symbol, "1m", Some(2), None, None)
            .await?
            .len()
    );
    println!(
        "ok spot_average_price {}",
        rest.spot_average_price(spot_symbol).await?.price
    );
    match rest.spot_ticker_24hr(Some(spot_symbol)).await? {
        OneOrMany::One(item) => println!("ok spot_ticker_24hr {}", item.symbol),
        OneOrMany::Many(items) => println!("ok spot_ticker_24hr {}", items.len()),
    }
    match rest.spot_price_ticker(Some(spot_symbol)).await? {
        OneOrMany::One(item) => println!("ok spot_price_ticker {}", item.symbol),
        OneOrMany::Many(items) => println!("ok spot_price_ticker {}", items.len()),
    }
    match rest.spot_book_ticker(Some(spot_symbol)).await? {
        OneOrMany::One(item) => println!("ok spot_book_ticker {}", item.symbol),
        OneOrMany::Many(items) => println!("ok spot_book_ticker {}", items.len()),
    }

    match rest.futures_contracts(Some(futures_symbol)).await? {
        OneOrMany::One(item) => println!("ok futures_contracts {}", item.symbol),
        OneOrMany::Many(items) => println!("ok futures_contracts {}", items.len()),
    }
    println!(
        "ok futures_server_time {}",
        rest.futures_server_time().await?
    );
    println!(
        "ok futures_support_currencies {}",
        rest.futures_support_currencies().await?.len()
    );
    println!(
        "ok futures_transferable_currencies {}",
        rest.futures_transferable_currencies().await?.len()
    );
    println!(
        "ok futures_depth bids={}",
        rest.futures_depth(futures_symbol).await?.bids.len()
    );
    println!(
        "ok futures_depth_commits {}",
        rest.futures_depth_commits(futures_symbol, 5).await?.len()
    );
    println!(
        "ok futures_index_price {}",
        rest.futures_index_price(futures_symbol).await?.index_price
    );
    println!(
        "ok futures_fair_price {}",
        rest.futures_fair_price(futures_symbol).await?.fair_price
    );
    println!(
        "ok futures_funding_rate {}",
        rest.futures_funding_rate(futures_symbol)
            .await?
            .funding_rate
    );
    println!(
        "ok futures_klines {}",
        rest.futures_klines(futures_symbol, "Min1", Some(2), None, None)
            .await?
            .time
            .len()
    );
    println!(
        "ok futures_index_price_klines {}",
        rest.futures_index_price_klines(futures_symbol, "Min1", Some(2), None, None)
            .await?
            .time
            .len()
    );
    println!(
        "ok futures_fair_price_klines {}",
        rest.futures_fair_price_klines(futures_symbol, "Min1", Some(2), None, None)
            .await?
            .time
            .len()
    );
    println!(
        "ok futures_deals {}",
        rest.futures_deals(futures_symbol, Some(2)).await?.len()
    );
    println!(
        "ok futures_recent_deals {}",
        rest.futures_recent_deals(futures_symbol, Some(2))
            .await?
            .len()
    );
    match rest.futures_ticker(Some(futures_symbol)).await? {
        OneOrMany::One(item) => println!("ok futures_ticker {}", item.symbol),
        OneOrMany::Many(items) => println!("ok futures_ticker {}", items.len()),
    }
    match rest.futures_trend_data(Some(futures_symbol)).await? {
        OneOrMany::One(item) => println!("ok futures_trend_data {}", item.symbol),
        OneOrMany::Many(items) => println!("ok futures_trend_data {}", items.len()),
    }
    println!(
        "ok futures_risk_reverse {}",
        rest.futures_risk_reverse().await?.len()
    );
    println!(
        "ok futures_insurance_fund_balance {}",
        rest.futures_insurance_fund_balance().await?.len()
    );
    println!(
        "ok futures_risk_reverse_history {}",
        rest.futures_risk_reverse_history(futures_symbol, 1, 2)
            .await?
            .result_list
            .len()
    );
    println!(
        "ok futures_insurance_fund_balance_history {}",
        rest.futures_insurance_fund_balance_history(futures_symbol, 1, 2)
            .await?
            .result_list
            .len()
    );
    println!(
        "ok futures_funding_rate_history {}",
        rest.futures_funding_rate_history(futures_symbol, 1, 2)
            .await?
            .result_list
            .len()
    );

    Ok(())
}
