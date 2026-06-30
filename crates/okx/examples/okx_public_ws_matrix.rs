use anyhow::Result;
use okx::{
    DEFAULT_SPOT_INST_ID, DEFAULT_SWAP_INST_ID, OkxInstrumentId, OkxWsSubscription,
    sample_ws_counts,
};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let spot_inst_id =
        std::env::var("OKX_SPOT_INST_ID").unwrap_or_else(|_| DEFAULT_SPOT_INST_ID.to_string());
    let swap_inst_id =
        std::env::var("OKX_SWAP_INST_ID").unwrap_or_else(|_| DEFAULT_SWAP_INST_ID.to_string());
    let watch_seconds = std::env::var("OKX_WS_MATRIX_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20)
        .max(1);
    let spot = OkxInstrumentId::spot(spot_inst_id);
    let swap = OkxInstrumentId::swap(swap_inst_id);
    let subscriptions = vec![
        OkxWsSubscription::Ticker {
            instrument: spot.clone(),
        },
        OkxWsSubscription::Trades {
            instrument: spot.clone(),
        },
        OkxWsSubscription::Books5 { instrument: spot },
        OkxWsSubscription::Ticker {
            instrument: swap.clone(),
        },
        OkxWsSubscription::Trades {
            instrument: swap.clone(),
        },
        OkxWsSubscription::Books5 { instrument: swap },
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
