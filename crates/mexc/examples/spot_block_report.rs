use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{
    MexcSpotKlineInterval, MexcSpotSubscription, MexcSpotUpdateSpeed, MexcSpotWsClient,
    MexcSpotWsMessage, MexcTimezone,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant, timeout};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Serialize)]
struct SpotChannelStatusReport {
    label: String,
    channel: String,
    ack_status: &'static str,
    live_seen: bool,
    first_live_after_ms: Option<u128>,
    first_live_kind: Option<String>,
}

#[derive(Debug, Serialize)]
struct SpotBlockReport {
    unix_started_at_s: u64,
    symbol: String,
    watch_seconds: u64,
    statuses: Vec<SpotChannelStatusReport>,
    failure_reasons: Vec<String>,
    raw_ack_messages: Vec<String>,
}

#[derive(Clone)]
struct SpotExpectation {
    label: &'static str,
    subscription: MexcSpotSubscription,
}

#[derive(Default)]
struct SpotChannelState {
    ack_status: &'static str,
    live_seen: bool,
    first_live_after_ms: Option<u128>,
    first_live_kind: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let symbol = std::env::var("MEXC_SPOT_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let watch_seconds = env_u64("MEXC_SPOT_BLOCK_WATCH_SECONDS", 15).max(1);
    let report_path = std::env::var("MEXC_SPOT_BLOCK_REPORT_PATH").ok();
    let unix_started_at_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs();

    let expectations = vec![
        SpotExpectation {
            label: "agg_trades",
            subscription: MexcSpotSubscription::AggTrades {
                symbol: symbol.clone(),
                speed: MexcSpotUpdateSpeed::Ms100,
            },
        },
        SpotExpectation {
            label: "increase_depth",
            subscription: MexcSpotSubscription::IncreaseDepth {
                symbol: symbol.clone(),
            },
        },
        SpotExpectation {
            label: "increase_depth_batch",
            subscription: MexcSpotSubscription::IncreaseDepthBatch {
                symbol: symbol.clone(),
            },
        },
        SpotExpectation {
            label: "agg_depth",
            subscription: MexcSpotSubscription::AggDepth {
                symbol: symbol.clone(),
                speed: MexcSpotUpdateSpeed::Ms100,
            },
        },
        SpotExpectation {
            label: "limit_depth",
            subscription: MexcSpotSubscription::LimitDepth {
                symbol: symbol.clone(),
                level: 5,
            },
        },
        SpotExpectation {
            label: "book_ticker_agg",
            subscription: MexcSpotSubscription::BookTicker {
                symbol: symbol.clone(),
                speed: MexcSpotUpdateSpeed::Ms100,
            },
        },
        SpotExpectation {
            label: "book_ticker_batch",
            subscription: MexcSpotSubscription::BookTickerBatch {
                symbol: symbol.clone(),
            },
        },
        SpotExpectation {
            label: "book_ticker_raw",
            subscription: MexcSpotSubscription::AggBookTicker {
                symbol: symbol.clone(),
            },
        },
        SpotExpectation {
            label: "kline",
            subscription: MexcSpotSubscription::Kline {
                symbol: symbol.clone(),
                interval: MexcSpotKlineInterval::Min1,
            },
        },
        SpotExpectation {
            label: "mini_ticker",
            subscription: MexcSpotSubscription::MiniTicker {
                symbol: symbol.clone(),
                timezone: MexcTimezone::UTC_PLUS_8,
            },
        },
        SpotExpectation {
            label: "mini_tickers",
            subscription: MexcSpotSubscription::MiniTickers {
                timezone: MexcTimezone::UTC_PLUS_8,
            },
        },
    ];

    let mut states = expectations
        .iter()
        .map(|item| (item.subscription.channel(), SpotChannelState::default()))
        .collect::<BTreeMap<_, _>>();
    let labels = expectations
        .iter()
        .map(|item| (item.subscription.channel(), item.label))
        .collect::<BTreeMap<_, _>>();

    let mut stream = MexcSpotWsClient::default()
        .with_auto_reconnect(false)
        .connect(
            expectations
                .iter()
                .cloned()
                .map(|item| item.subscription)
                .collect(),
        )
        .await
        .context("connect spot block report websocket")?;

    let started_at = Instant::now();
    let deadline = started_at + Duration::from_secs(watch_seconds);
    let mut failure_reasons = Vec::new();
    let mut raw_ack_messages = Vec::new();

    while Instant::now() < deadline {
        let maybe_message = match timeout(Duration::from_secs(5), stream.next()).await {
            Ok(maybe_message) => maybe_message,
            Err(_) => continue,
        };
        let Some(message) = maybe_message else {
            break;
        };
        let message = message?;

        match &message {
            MexcSpotWsMessage::Ack(ack) => {
                if let Some(summary) = ack.parse_spot_subscription_summary() {
                    for channel in summary.successful_channels {
                        if let Some(state) = states.get_mut(&channel) {
                            state.ack_status = "acked";
                        }
                    }
                    for channel in summary.failed_channels {
                        if let Some(state) = states.get_mut(&channel) {
                            state.ack_status = "blocked";
                        }
                    }
                    if let Some(reason) = summary.failure_reason {
                        failure_reasons.push(reason);
                    }
                } else if let Some(message) = &ack.msg {
                    raw_ack_messages.push(message.clone());
                }
            }
            _ => {
                if let Some((channel, kind)) = spot_channel_and_kind(&message) {
                    if let Some(state) = states.get_mut(channel) {
                        state.live_seen = true;
                        if state.first_live_after_ms.is_none() {
                            state.first_live_after_ms = Some(started_at.elapsed().as_millis());
                            state.first_live_kind = Some(kind.to_string());
                        }
                    }
                }
            }
        }
    }

    let statuses = expectations
        .iter()
        .map(|item| {
            let channel = item.subscription.channel();
            let state = states.get(&channel).expect("channel state");
            SpotChannelStatusReport {
                label: labels.get(&channel).expect("channel label").to_string(),
                channel,
                ack_status: state.ack_status,
                live_seen: state.live_seen,
                first_live_after_ms: state.first_live_after_ms,
                first_live_kind: state.first_live_kind.clone(),
            }
        })
        .collect::<Vec<_>>();

    let report = SpotBlockReport {
        unix_started_at_s,
        symbol,
        watch_seconds,
        statuses,
        failure_reasons,
        raw_ack_messages,
    };

    let json = serde_json::to_string_pretty(&report).context("serialize spot block report")?;
    if let Some(path) = report_path {
        std::fs::write(&path, json)
            .with_context(|| format!("write spot block report to {path}"))?;
        println!("wrote spot block report to {path}");
    } else {
        println!("{json}");
    }

    Ok(())
}

fn spot_channel_and_kind(message: &MexcSpotWsMessage) -> Option<(&str, &'static str)> {
    match message {
        MexcSpotWsMessage::SessionStart(_)
        | MexcSpotWsMessage::Ack(_)
        | MexcSpotWsMessage::RawText(_) => None,
        MexcSpotWsMessage::AggTrades(event) => Some((&event.channel, "spot.aggTrades")),
        MexcSpotWsMessage::IncreaseDepth(event) => Some((&event.channel, "spot.increaseDepth")),
        MexcSpotWsMessage::IncreaseDepthBatch(event) => {
            Some((&event.channel, "spot.increaseDepthBatch"))
        }
        MexcSpotWsMessage::AggDepth(event) => Some((&event.channel, "spot.aggDepth")),
        MexcSpotWsMessage::LimitDepth(event) => Some((&event.channel, "spot.limitDepth")),
        MexcSpotWsMessage::BookTicker(event) => Some((&event.channel, "spot.bookTickerRaw")),
        MexcSpotWsMessage::BookTickerBatch(event) => Some((&event.channel, "spot.bookTickerBatch")),
        MexcSpotWsMessage::AggBookTicker(event) => Some((&event.channel, "spot.bookTickerAgg")),
        MexcSpotWsMessage::Kline(event) => Some((&event.channel, "spot.kline")),
        MexcSpotWsMessage::MiniTicker(event) => Some((&event.channel, "spot.miniTicker")),
        MexcSpotWsMessage::MiniTickers(event) => Some((&event.channel, "spot.miniTickers")),
    }
}
