use anyhow::{Result, ensure};
use mexc::MexcPublicRestClient;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = MexcPublicRestClient::default();
    let symbol = "BTC_USDT";
    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_sub(60);
    let start = end.saturating_sub(5 * 60);

    let contract = rest
        .futures_klines(symbol, "Min1", Some(5), Some(start), Some(end))
        .await?;
    let index = rest
        .futures_index_price_klines(symbol, "Min1", Some(5), Some(start), Some(end))
        .await?;
    let fair = rest
        .futures_fair_price_klines(symbol, "Min1", Some(5), Some(start), Some(end))
        .await?;

    ensure!(
        !contract.time.is_empty() && contract.time.len() <= 6,
        "contract kline window did not return recent points within the requested window"
    );
    ensure!(
        !index.time.is_empty() && index.time.len() <= 6,
        "index-price kline window did not return recent points within the requested window"
    );
    ensure!(
        !fair.time.is_empty() && fair.time.len() <= 6,
        "fair-price kline window did not return recent points within the requested window"
    );

    println!(
        "window start={} end={} contract_points={} index_points={} fair_points={}",
        start,
        end,
        contract.time.len(),
        index.time.len(),
        fair.time.len()
    );

    Ok(())
}
