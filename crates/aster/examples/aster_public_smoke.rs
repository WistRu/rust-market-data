use anyhow::Result;
use aster::{AsterPublicRestClient, AsterWsClient, AsterWsSubscription};
use futures::StreamExt;
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    let symbol = std::env::var("ASTER_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let rest = AsterPublicRestClient::default();

    rest.spot_ping().await?;
    rest.futures_ping().await?;
    println!("ok rest_ping spot+futures");
    println!(
        "ok spot_depth bids={}",
        rest.spot_order_book(&symbol, Some(5)).await?.bids.len()
    );
    println!(
        "ok futures_depth bids={}",
        rest.futures_order_book(&symbol, Some(5)).await?.bids.len()
    );

    let mut stream = AsterWsClient::futures()
        .connect_streams(vec![AsterWsSubscription::BookTicker {
            symbol: symbol.clone(),
        }])
        .await?;

    let event = timeout(Duration::from_secs(10), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("websocket stream closed before first event"))??;
    println!("ok futures_ws_sample {}", event["stream"]);

    Ok(())
}
