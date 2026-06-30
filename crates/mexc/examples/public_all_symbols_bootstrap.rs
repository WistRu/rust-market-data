use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{
    MexcConnector, MexcFuturesWsMessage, MexcPublicEvent, MexcPublicRuntimeConfig,
    MexcSpotWsMessage,
};
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let builder = connector.public_runtime_builder();
    let runtime = builder
        .connect_with_snapshot(MexcPublicRuntimeConfig::balanced())
        .await
        .context("connect public runtime")?;

    println!(
        "manifest spot_symbols={} futures_symbols={} spot_subscriptions={} futures_subscriptions={} spot_connections={} futures_connections={}",
        runtime.manifest.spot_symbol_count,
        runtime.manifest.futures_symbol_count,
        runtime.manifest.spot_subscription_count,
        runtime.manifest.futures_subscription_count,
        runtime.manifest.spot_connection_count,
        runtime.manifest.futures_connection_count
    );
    println!(
        "snapshot spot_server_time={} spot_default_symbols={} spot_offline_symbols={} spot_exchange_symbols={} spot_24hr={} spot_prices={} spot_books={} futures_server_time={} futures_contracts={} futures_tickers={} futures_currencies={} futures_insurance={}",
        runtime.snapshot.spot.server_time.server_time,
        runtime.snapshot.spot.default_symbols.data.len(),
        runtime.snapshot.spot.offline_symbols.data.len(),
        runtime.snapshot.spot.exchange_info.symbols.len(),
        runtime.snapshot.spot.ticker_24hr.len(),
        runtime.snapshot.spot.price_tickers.len(),
        runtime.snapshot.spot.book_tickers.len(),
        runtime.snapshot.futures.server_time,
        runtime.snapshot.futures.contracts.len(),
        runtime.snapshot.futures.tickers.len(),
        runtime.snapshot.futures.transferable_currencies.len(),
        runtime.snapshot.futures.insurance_balances.len()
    );

    let mut stream = runtime.stream;
    let mut first_spot = None;
    let mut first_futures = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut index = 0usize;
    while tokio::time::Instant::now() < deadline
        && (first_spot.is_none() || first_futures.is_none())
    {
        let event = timeout(Duration::from_secs(20), stream.next())
            .await
            .context("wait for runtime event")?
            .transpose()?
            .context("runtime stream ended unexpectedly")?;
        match &event {
            MexcPublicEvent::Spot(
                MexcSpotWsMessage::SessionStart(_) | MexcSpotWsMessage::Ack(_),
            ) => {}
            MexcPublicEvent::Futures(
                MexcFuturesWsMessage::SessionStart(_) | MexcFuturesWsMessage::Ack(_),
            ) => {}
            MexcPublicEvent::Spot(_) if first_spot.is_none() => {
                first_spot = Some(format!("{event:?}"));
                println!("first spot event at index {index}");
            }
            MexcPublicEvent::Futures(_) if first_futures.is_none() => {
                first_futures = Some(format!("{event:?}"));
                println!("first futures event at index {index}");
            }
            _ => {}
        }
        index += 1;
    }

    println!(
        "spot_seen={} futures_seen={}",
        first_spot.is_some(),
        first_futures.is_some()
    );
    if let Some(event) = first_spot {
        println!("spot_event={event}");
    }
    if let Some(event) = first_futures {
        println!("futures_event={event}");
    }

    Ok(())
}
