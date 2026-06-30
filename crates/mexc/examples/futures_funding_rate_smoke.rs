use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use mexc::{MexcConnector, MexcFuturesSubscription, MexcFuturesWsMessage};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_stream::wrappers::ReceiverStream;

const SUBSCRIPTIONS_PER_CONNECTION: usize = 200;

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let symbols = connector
        .rest
        .all_futures_symbols()
        .await
        .context("fetch all futures symbols")?;
    println!("funding subscriptions={}", symbols.len());

    let subscriptions = symbols
        .iter()
        .map(|symbol| MexcFuturesSubscription::FundingRate {
            symbol: symbol.clone(),
        })
        .collect::<Vec<_>>();

    let (tx, rx) = mpsc::channel(4096);
    for shard in subscriptions.chunks(SUBSCRIPTIONS_PER_CONNECTION) {
        let mut stream = connector
            .futures_ws
            .connect(shard.to_vec())
            .await
            .context("connect futures funding shard")?;
        let tx = tx.clone();
        tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                if tx.send(message).await.is_err() {
                    return;
                }
            }
        });
    }
    drop(tx);

    let mut merged = ReceiverStream::new(rx);
    let mut acked = 0usize;
    let mut payloads = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);

    while tokio::time::Instant::now() < deadline && payloads < 5 {
        let message = timeout(Duration::from_secs(10), merged.next())
            .await
            .context("wait for merged funding stream event")?
            .transpose()?
            .context("merged funding stream ended unexpectedly")?;

        match message {
            MexcFuturesWsMessage::Ack(ack) => {
                acked += 1;
                if acked <= 5 {
                    println!("ack#{} channel={:?} msg={:?}", acked, ack.channel, ack.msg);
                }
            }
            MexcFuturesWsMessage::FundingRate(event) => {
                payloads += 1;
                println!(
                    "payload#{} symbol={} rate={} next_settle={:?}",
                    payloads, event.data.symbol, event.data.rate, event.data.next_settle_time
                );
            }
            _ => {}
        }
    }

    ensure!(
        payloads > 0,
        "did not receive any live funding-rate payloads"
    );
    println!("acked={} payloads={}", acked, payloads);
    Ok(())
}
