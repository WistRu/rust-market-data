use anyhow::Result;
use bybit::{BybitWsClient, BybitWsSubscription};
use futures::StreamExt;
use std::collections::BTreeMap;
use tokio::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<()> {
    let spot_symbol = std::env::var("BYBIT_SPOT_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let linear_symbol =
        std::env::var("BYBIT_LINEAR_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let watch_seconds = std::env::var("BYBIT_WS_MATRIX_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20)
        .max(1);

    let spot_subscriptions = vec![
        BybitWsSubscription::Ticker {
            symbol: spot_symbol.clone(),
        },
        BybitWsSubscription::PublicTrade {
            symbol: spot_symbol.clone(),
        },
        BybitWsSubscription::OrderBook {
            symbol: spot_symbol,
            depth: 50,
        },
    ];
    let linear_subscriptions = vec![
        BybitWsSubscription::Ticker {
            symbol: linear_symbol.clone(),
        },
        BybitWsSubscription::PublicTrade {
            symbol: linear_symbol.clone(),
        },
        BybitWsSubscription::OrderBook {
            symbol: linear_symbol,
            depth: 50,
        },
    ];

    let mut spot_stream = BybitWsClient::spot()
        .connect_topics(spot_subscriptions.clone())
        .await?;
    let mut linear_stream = BybitWsClient::linear()
        .connect_topics(linear_subscriptions.clone())
        .await?;

    let deadline = Instant::now() + Duration::from_secs(watch_seconds);
    let mut spot_counts = BTreeMap::<String, usize>::new();
    let mut linear_counts = BTreeMap::<String, usize>::new();

    while Instant::now() < deadline {
        tokio::select! {
            item = spot_stream.next() => {
                if let Some(Ok(value)) = item {
                    count_topic(&mut spot_counts, value);
                }
            }
            item = linear_stream.next() => {
                if let Some(Ok(value)) = item {
                    count_topic(&mut linear_counts, value);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }

    println!("spot_expected:");
    for subscription in &spot_subscriptions {
        println!("  {}", subscription.topic());
    }
    println!("spot_seen:");
    for (topic, count) in &spot_counts {
        println!("  {topic} {count}");
    }
    println!("linear_expected:");
    for subscription in &linear_subscriptions {
        println!("  {}", subscription.topic());
    }
    println!("linear_seen:");
    for (topic, count) in &linear_counts {
        println!("  {topic} {count}");
    }

    Ok(())
}

fn count_topic(counts: &mut BTreeMap<String, usize>, value: serde_json::Value) {
    let topic = value
        .get("topic")
        .and_then(|value| value.as_str())
        .or_else(|| value.get("op").and_then(|value| value.as_str()))
        .unwrap_or("unknown")
        .to_string();
    *counts.entry(topic).or_default() += 1;
}
