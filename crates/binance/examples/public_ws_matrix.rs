use anyhow::Result;
use binance::{BinanceWsClient, BinanceWsSubscription};
use futures::StreamExt;
use std::collections::BTreeMap;
use tokio::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<()> {
    let spot_symbol =
        std::env::var("BINANCE_SPOT_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let futures_symbol =
        std::env::var("BINANCE_FUTURES_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let watch_seconds = std::env::var("BINANCE_WS_MATRIX_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20)
        .max(1);

    let spot_subscriptions = vec![
        BinanceWsSubscription::AggTrade {
            symbol: spot_symbol.clone(),
        },
        BinanceWsSubscription::BookTicker {
            symbol: spot_symbol.clone(),
        },
        BinanceWsSubscription::DiffDepth {
            symbol: spot_symbol.clone(),
            speed_ms: Some(100),
        },
    ];
    let futures_subscriptions = vec![
        BinanceWsSubscription::AggTrade {
            symbol: futures_symbol.clone(),
        },
        BinanceWsSubscription::BookTicker {
            symbol: futures_symbol.clone(),
        },
        BinanceWsSubscription::MarkPrice {
            symbol: futures_symbol.clone(),
            fast: true,
        },
        BinanceWsSubscription::DiffDepth {
            symbol: futures_symbol.clone(),
            speed_ms: Some(100),
        },
    ];

    let mut spot_stream = BinanceWsClient::spot()
        .connect_streams(spot_subscriptions.clone())
        .await?;
    let mut futures_stream = BinanceWsClient::futures()
        .connect_streams(futures_subscriptions.clone())
        .await?;

    let deadline = Instant::now() + Duration::from_secs(watch_seconds);
    let mut spot_counts = BTreeMap::<String, usize>::new();
    let mut futures_counts = BTreeMap::<String, usize>::new();

    while Instant::now() < deadline {
        tokio::select! {
            item = spot_stream.next() => {
                if let Some(Ok(value)) = item {
                    count_stream(&mut spot_counts, value);
                }
            }
            item = futures_stream.next() => {
                if let Some(Ok(value)) = item {
                    count_stream(&mut futures_counts, value);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }

    println!("spot_expected:");
    for subscription in &spot_subscriptions {
        println!("  {}", subscription.stream_name());
    }
    println!("spot_seen:");
    for (stream, count) in &spot_counts {
        println!("  {stream} {count}");
    }

    println!("futures_expected:");
    for subscription in &futures_subscriptions {
        println!("  {}", subscription.stream_name());
    }
    println!("futures_seen:");
    for (stream, count) in &futures_counts {
        println!("  {stream} {count}");
    }

    Ok(())
}

fn count_stream(counts: &mut BTreeMap<String, usize>, value: serde_json::Value) {
    let stream = value
        .get("stream")
        .and_then(|value| value.as_str())
        .or_else(|| value.get("e").and_then(|value| value.as_str()))
        .unwrap_or("unknown")
        .to_string();
    *counts.entry(stream).or_default() += 1;
}
