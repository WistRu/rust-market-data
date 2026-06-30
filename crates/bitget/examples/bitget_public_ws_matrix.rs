use anyhow::Result;
use bitget::{BitgetInstrumentId, BitgetWsSubscription, DEFAULT_SYMBOL, sample_ws_counts};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let symbol = std::env::var("BITGET_SYMBOL").unwrap_or_else(|_| DEFAULT_SYMBOL.to_string());
    let watch_seconds = std::env::var("BITGET_WS_MATRIX_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20)
        .max(1);
    let spot = BitgetInstrumentId::spot(symbol.clone());
    let futures = BitgetInstrumentId::usdt_futures(symbol);
    let subscriptions = vec![
        BitgetWsSubscription::Ticker {
            instrument: spot.clone(),
        },
        BitgetWsSubscription::Trades {
            instrument: spot.clone(),
        },
        BitgetWsSubscription::Books5 { instrument: spot },
        BitgetWsSubscription::Ticker {
            instrument: futures.clone(),
        },
        BitgetWsSubscription::Trades {
            instrument: futures.clone(),
        },
        BitgetWsSubscription::Books5 {
            instrument: futures,
        },
    ];

    println!("expected:");
    for subscription in &subscriptions {
        println!("  {}", subscription.label());
    }

    let counts = sample_ws_counts(subscriptions, Duration::from_secs(watch_seconds)).await?;
    println!("seen:");
    for (topic, count) in counts {
        println!("  {topic} {count}");
    }

    Ok(())
}
