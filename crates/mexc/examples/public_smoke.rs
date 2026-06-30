use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{
    MexcFuturesSubscription, MexcFuturesWsClient, MexcFuturesWsMessage, MexcPublicRestClient,
    MexcSpotSubscription, MexcSpotUpdateSpeed, MexcSpotWsClient, MexcSpotWsMessage,
};
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    let rest = MexcPublicRestClient::default();

    let spot_symbols = rest.all_spot_symbols().await.context("load spot symbols")?;
    let futures_symbols = rest
        .all_futures_symbols()
        .await
        .context("load futures symbols")?;

    println!("spot symbols: {}", spot_symbols.len());
    println!("futures symbols: {}", futures_symbols.len());

    let spot_symbol = spot_symbols
        .iter()
        .find(|symbol| symbol.as_str() == "BTCUSDT")
        .cloned()
        .unwrap_or_else(|| spot_symbols[0].clone());

    let futures_symbol = futures_symbols
        .iter()
        .find(|symbol| symbol.as_str() == "BTC_USDT")
        .cloned()
        .unwrap_or_else(|| futures_symbols[0].clone());

    let spot_ws = MexcSpotWsClient::default().with_auto_reconnect(false);
    let mut spot_stream = spot_ws
        .connect(vec![MexcSpotSubscription::AggTrades {
            symbol: spot_symbol.clone(),
            speed: MexcSpotUpdateSpeed::Ms100,
        }])
        .await
        .context("connect spot websocket")?;

    let spot_message = timeout(Duration::from_secs(20), next_spot_payload(&mut spot_stream))
        .await
        .context("wait for spot frame")??;
    println!("spot first frame for {spot_symbol}: {spot_message:?}");

    let futures_ws = MexcFuturesWsClient::default().with_auto_reconnect(false);
    let mut futures_stream = futures_ws
        .connect(vec![MexcFuturesSubscription::Ticker {
            symbol: futures_symbol.clone(),
        }])
        .await
        .context("connect futures websocket")?;

    let futures_message = timeout(
        Duration::from_secs(20),
        next_futures_payload(&mut futures_stream),
    )
    .await
    .context("wait for futures frame")??;
    println!("futures first frame for {futures_symbol}: {futures_message:?}");

    Ok(())
}

async fn next_spot_payload(
    stream: &mut tokio_stream::wrappers::ReceiverStream<Result<MexcSpotWsMessage>>,
) -> Result<MexcSpotWsMessage> {
    while let Some(message) = stream.next().await {
        let message = message?;
        if matches!(
            message,
            MexcSpotWsMessage::SessionStart(_) | MexcSpotWsMessage::Ack(_)
        ) {
            continue;
        }
        return Ok(message);
    }
    anyhow::bail!("spot stream ended before first payload")
}

async fn next_futures_payload(
    stream: &mut tokio_stream::wrappers::ReceiverStream<Result<MexcFuturesWsMessage>>,
) -> Result<MexcFuturesWsMessage> {
    while let Some(message) = stream.next().await {
        let message = message?;
        if matches!(
            message,
            MexcFuturesWsMessage::SessionStart(_) | MexcFuturesWsMessage::Ack(_)
        ) {
            continue;
        }
        return Ok(message);
    }
    anyhow::bail!("futures stream ended before first payload")
}
