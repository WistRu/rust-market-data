use anyhow::{Result, ensure};
use futures::StreamExt;
use mexc::{MexcFuturesSubscription, MexcFuturesWsClient, MexcFuturesWsMessage};
use tokio::time::{Duration, Instant};

#[derive(Debug, Default)]
struct DealStats {
    acked: bool,
    messages: usize,
    trades: usize,
    first_trade_id: Option<String>,
    last_trade_id: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let symbol =
        std::env::var("MEXC_FUTURES_DEALS_SYMBOL").unwrap_or_else(|_| "BTC_USDT".to_string());
    let watch_seconds = std::env::var("MEXC_FUTURES_DEALS_WATCH_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(15);

    let ws = MexcFuturesWsClient::default().with_auto_reconnect(false);
    let mut aggregated = ws
        .connect(vec![MexcFuturesSubscription::Deals {
            symbol: symbol.clone(),
        }])
        .await?;
    let mut raw = ws
        .connect(vec![MexcFuturesSubscription::DealsRaw {
            symbol: symbol.clone(),
        }])
        .await?;

    let deadline = Instant::now() + Duration::from_secs(watch_seconds);
    let mut aggregated_stats = DealStats::default();
    let mut raw_stats = DealStats::default();

    while Instant::now() < deadline {
        tokio::select! {
            maybe = aggregated.next() => {
                if let Some(message) = maybe {
                    record_deal_message(message?, &mut aggregated_stats);
                } else {
                    break;
                }
            }
            maybe = raw.next() => {
                if let Some(message) = maybe {
                    record_deal_message(message?, &mut raw_stats);
                } else {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    ensure!(
        aggregated_stats.acked,
        "aggregated deal subscription did not ack"
    );
    ensure!(raw_stats.acked, "raw deal subscription did not ack");
    ensure!(
        aggregated_stats.trades > 0,
        "aggregated deal stream produced no trades"
    );
    ensure!(raw_stats.trades > 0, "raw deal stream produced no trades");

    println!(
        "aggregated messages={} trades={} first_trade_id={:?} last_trade_id={:?}",
        aggregated_stats.messages,
        aggregated_stats.trades,
        aggregated_stats.first_trade_id,
        aggregated_stats.last_trade_id
    );
    println!(
        "raw messages={} trades={} first_trade_id={:?} last_trade_id={:?}",
        raw_stats.messages, raw_stats.trades, raw_stats.first_trade_id, raw_stats.last_trade_id
    );

    Ok(())
}

fn record_deal_message(message: MexcFuturesWsMessage, stats: &mut DealStats) {
    match message {
        MexcFuturesWsMessage::Ack(_) => {
            stats.acked = true;
        }
        MexcFuturesWsMessage::Deals(event) => {
            stats.messages += 1;
            stats.trades += event.data.len();
            if let Some(first) = event.data.first() {
                if stats.first_trade_id.is_none() {
                    stats.first_trade_id = Some(first.i.clone());
                }
            }
            if let Some(last) = event.data.last() {
                stats.last_trade_id = Some(last.i.clone());
            }
        }
        _ => {}
    }
}
