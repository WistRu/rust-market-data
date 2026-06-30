use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use mexc::{MexcFuturesSubscription, MexcFuturesWsClient, MexcFuturesWsMessage};
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    let symbol =
        std::env::var("MEXC_FUTURES_DEPTH_SYMBOL").unwrap_or_else(|_| "BTC_USDT".to_string());
    let ws = MexcFuturesWsClient::default().with_auto_reconnect(false);

    let mut incremental = ws
        .connect(vec![MexcFuturesSubscription::Depth {
            symbol: symbol.clone(),
            compress: Some(false),
        }])
        .await
        .context("connect futures incremental depth")?;
    let mut full = ws
        .connect(vec![MexcFuturesSubscription::DepthFull {
            symbol: symbol.clone(),
            limit: Some(5),
        }])
        .await
        .context("connect futures full depth")?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut depth_ack = false;
    let mut full_ack = false;
    let mut depth_payload = None;
    let mut full_payload = None;

    while tokio::time::Instant::now() < deadline
        && (!depth_ack || !full_ack || depth_payload.is_none() || full_payload.is_none())
    {
        tokio::select! {
            maybe = timeout(Duration::from_secs(5), incremental.next()) => {
                if let Ok(Some(message)) = maybe {
                    match message? {
                        MexcFuturesWsMessage::Ack(ack) if ack.channel.as_deref() == Some("rs.sub.depth") => {
                            depth_ack = true;
                        }
                        MexcFuturesWsMessage::Depth(event) => {
                            depth_payload = Some((event.data.version, event.data.asks.len(), event.data.bids.len()));
                        }
                        _ => {}
                    }
                }
            }
            maybe = timeout(Duration::from_secs(5), full.next()) => {
                if let Ok(Some(message)) = maybe {
                    match message? {
                        MexcFuturesWsMessage::Ack(ack) if ack.channel.as_deref() == Some("rs.sub.depth.full") => {
                            full_ack = true;
                        }
                        MexcFuturesWsMessage::DepthFull(event) => {
                            full_payload = Some((event.data.version, event.data.asks.len(), event.data.bids.len()));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    ensure!(depth_ack, "incremental depth subscription did not ack");
    ensure!(full_ack, "full depth subscription did not ack");
    let depth_payload = depth_payload.context("incremental depth produced no payload")?;
    let full_payload = full_payload.context("full depth produced no payload")?;

    ensure!(
        full_payload.1 <= 5 && full_payload.2 <= 5,
        "full depth payload exceeded requested limit=5"
    );

    println!(
        "incremental_depth version={} asks={} bids={}",
        depth_payload.0, depth_payload.1, depth_payload.2
    );
    println!(
        "full_depth_limit5 version={} asks={} bids={}",
        full_payload.0, full_payload.1, full_payload.2
    );

    Ok(())
}
