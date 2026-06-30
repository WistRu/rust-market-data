use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{
    MexcFuturesKlineInterval, MexcFuturesSubscription, MexcFuturesWsClient, MexcFuturesWsMessage,
    MexcSpotKlineInterval, MexcSpotSubscription, MexcSpotUpdateSpeed, MexcSpotWsClient,
    MexcSpotWsMessage, MexcTimezone,
};
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::{Duration, Instant};

#[derive(Clone)]
struct SpotExpectation {
    label: &'static str,
    subscription: MexcSpotSubscription,
    live_kind: &'static str,
}

#[derive(Clone)]
struct FuturesExpectation {
    label: &'static str,
    subscription: MexcFuturesSubscription,
    live_kind: Option<&'static str>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let spot_symbol = std::env::var("MEXC_SPOT_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let futures_symbol =
        std::env::var("MEXC_FUTURES_SYMBOL").unwrap_or_else(|_| "BTC_USDT".to_string());
    let watch_seconds = std::env::var("MEXC_WS_MATRIX_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30)
        .max(1);

    let spot_expectations = vec![
        SpotExpectation {
            label: "agg_trades",
            subscription: MexcSpotSubscription::AggTrades {
                symbol: spot_symbol.clone(),
                speed: MexcSpotUpdateSpeed::Ms100,
            },
            live_kind: "spot.aggTrades",
        },
        SpotExpectation {
            label: "increase_depth",
            subscription: MexcSpotSubscription::IncreaseDepth {
                symbol: spot_symbol.clone(),
            },
            live_kind: "spot.increaseDepth",
        },
        SpotExpectation {
            label: "increase_depth_batch",
            subscription: MexcSpotSubscription::IncreaseDepthBatch {
                symbol: spot_symbol.clone(),
            },
            live_kind: "spot.increaseDepthBatch",
        },
        SpotExpectation {
            label: "agg_depth",
            subscription: MexcSpotSubscription::AggDepth {
                symbol: spot_symbol.clone(),
                speed: MexcSpotUpdateSpeed::Ms100,
            },
            live_kind: "spot.aggDepth",
        },
        SpotExpectation {
            label: "limit_depth",
            subscription: MexcSpotSubscription::LimitDepth {
                symbol: spot_symbol.clone(),
                level: 5,
            },
            live_kind: "spot.limitDepth",
        },
        SpotExpectation {
            label: "book_ticker_agg",
            subscription: MexcSpotSubscription::BookTicker {
                symbol: spot_symbol.clone(),
                speed: MexcSpotUpdateSpeed::Ms100,
            },
            live_kind: "spot.bookTickerAgg",
        },
        SpotExpectation {
            label: "book_ticker_batch",
            subscription: MexcSpotSubscription::BookTickerBatch {
                symbol: spot_symbol.clone(),
            },
            live_kind: "spot.bookTickerBatch",
        },
        SpotExpectation {
            label: "book_ticker_raw",
            subscription: MexcSpotSubscription::AggBookTicker {
                symbol: spot_symbol.clone(),
            },
            live_kind: "spot.bookTickerRaw",
        },
        SpotExpectation {
            label: "kline",
            subscription: MexcSpotSubscription::Kline {
                symbol: spot_symbol.clone(),
                interval: MexcSpotKlineInterval::Min1,
            },
            live_kind: "spot.kline",
        },
        SpotExpectation {
            label: "mini_ticker",
            subscription: MexcSpotSubscription::MiniTicker {
                symbol: spot_symbol.clone(),
                timezone: MexcTimezone::UTC_PLUS_8,
            },
            live_kind: "spot.miniTicker",
        },
        SpotExpectation {
            label: "mini_tickers",
            subscription: MexcSpotSubscription::MiniTickers {
                timezone: MexcTimezone::UTC_PLUS_8,
            },
            live_kind: "spot.miniTickers",
        },
    ];

    let futures_expectations = vec![
        FuturesExpectation {
            label: "tickers",
            subscription: MexcFuturesSubscription::Tickers,
            live_kind: Some("futures.tickers"),
        },
        FuturesExpectation {
            label: "ticker",
            subscription: MexcFuturesSubscription::Ticker {
                symbol: futures_symbol.clone(),
            },
            live_kind: Some("futures.ticker"),
        },
        FuturesExpectation {
            label: "deals",
            subscription: MexcFuturesSubscription::Deals {
                symbol: futures_symbol.clone(),
            },
            live_kind: Some("futures.deals"),
        },
        FuturesExpectation {
            label: "depth",
            subscription: MexcFuturesSubscription::Depth {
                symbol: futures_symbol.clone(),
                compress: Some(false),
            },
            live_kind: Some("futures.depth"),
        },
        FuturesExpectation {
            label: "depth_step",
            subscription: MexcFuturesSubscription::DepthStep {
                symbol: futures_symbol.clone(),
                step: "10".to_string(),
            },
            live_kind: Some("futures.depthStep"),
        },
        FuturesExpectation {
            label: "depth_full",
            subscription: MexcFuturesSubscription::DepthFull {
                symbol: futures_symbol.clone(),
                limit: Some(20),
            },
            live_kind: Some("futures.depthFull"),
        },
        FuturesExpectation {
            label: "funding_rate",
            subscription: MexcFuturesSubscription::FundingRate {
                symbol: futures_symbol.clone(),
            },
            live_kind: Some("futures.fundingRate"),
        },
        FuturesExpectation {
            label: "index_price",
            subscription: MexcFuturesSubscription::IndexPrice {
                symbol: futures_symbol.clone(),
            },
            live_kind: Some("futures.indexPrice"),
        },
        FuturesExpectation {
            label: "fair_price",
            subscription: MexcFuturesSubscription::FairPrice {
                symbol: futures_symbol.clone(),
            },
            live_kind: Some("futures.fairPrice"),
        },
        FuturesExpectation {
            label: "kline",
            subscription: MexcFuturesSubscription::Kline {
                symbol: futures_symbol.clone(),
                interval: MexcFuturesKlineInterval::Min1,
            },
            live_kind: Some("futures.kline"),
        },
        FuturesExpectation {
            label: "contract",
            subscription: MexcFuturesSubscription::Contract,
            live_kind: Some("futures.contract"),
        },
        FuturesExpectation {
            label: "event_contract",
            subscription: MexcFuturesSubscription::EventContract,
            live_kind: Some("futures.eventContract"),
        },
    ];

    let spot_ws = MexcSpotWsClient::default().with_auto_reconnect(false);
    let futures_ws = MexcFuturesWsClient::default().with_auto_reconnect(false);

    let mut spot_stream = spot_ws
        .connect(
            spot_expectations
                .iter()
                .cloned()
                .map(|item| item.subscription)
                .collect(),
        )
        .await
        .context("connect spot matrix")?;
    let mut futures_stream = futures_ws
        .connect(
            futures_expectations
                .iter()
                .cloned()
                .map(|item| item.subscription)
                .collect(),
        )
        .await
        .context("connect futures matrix")?;

    let mut seen_kinds = BTreeSet::new();
    let mut first_seen_description = BTreeMap::new();
    let mut acked_spot_channels = BTreeSet::new();
    let mut blocked_spot_channels = BTreeSet::new();
    let mut spot_failure_reasons = Vec::new();
    let mut raw_spot_acks = Vec::new();
    let mut acked_futures_channels = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(watch_seconds);

    while Instant::now() < deadline {
        tokio::select! {
            maybe = spot_stream.next() => {
                let Some(message) = maybe else { break };
                let message = message?;
                if let MexcSpotWsMessage::Ack(ack) = &message {
                    if let Some(summary) = ack.parse_spot_subscription_summary() {
                        acked_spot_channels.extend(summary.successful_channels);
                        blocked_spot_channels.extend(summary.failed_channels);
                        if let Some(reason) = summary.failure_reason {
                            spot_failure_reasons.push(reason);
                        }
                    } else if let Some(msg) = &ack.msg {
                        raw_spot_acks.push(msg.clone());
                    }
                }
                if let Some(kind) = spot_kind(&message) {
                    seen_kinds.insert(kind.to_string());
                    first_seen_description
                        .entry(kind.to_string())
                        .or_insert_with(|| describe_spot(&message));
                }
            }
            maybe = futures_stream.next() => {
                let Some(message) = maybe else { break };
                let message = message?;
                if let MexcFuturesWsMessage::Ack(ack) = &message {
                    if let Some(channel) = &ack.channel {
                        acked_futures_channels.insert(channel.clone());
                    }
                }
                if let Some(kind) = futures_kind(&message) {
                    seen_kinds.insert(kind.to_string());
                    first_seen_description
                        .entry(kind.to_string())
                        .or_insert_with(|| describe_futures(&message));
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }

    println!("spot websocket matrix");
    for expected in &spot_expectations {
        let channel = expected.subscription.channel();
        let ack_status = if blocked_spot_channels.contains(&channel) {
            "blocked"
        } else if acked_spot_channels.contains(&channel) {
            "acked"
        } else {
            "unknown"
        };
        let live_status = if seen_kinds.contains(expected.live_kind) {
            "seen"
        } else {
            "missing"
        };
        println!(
            "spot label={} channel={} ack_status={} live_status={} detail={}",
            expected.label,
            channel,
            ack_status,
            live_status,
            first_seen_description
                .get(expected.live_kind)
                .map(String::as_str)
                .unwrap_or("-")
        );
    }

    println!("futures websocket matrix");
    for expected in &futures_expectations {
        let ack_channel = format!("rs.{}", expected.subscription.method());
        let ack_status = if acked_futures_channels.contains(&ack_channel) {
            "acked"
        } else {
            "unknown"
        };
        let live_status = match expected.live_kind {
            Some(kind) if seen_kinds.contains(kind) => "seen",
            Some(_) => "missing",
            None => "n/a",
        };
        let note = match expected.live_kind {
            Some(kind) => first_seen_description
                .get(kind)
                .map(String::as_str)
                .unwrap_or("-"),
            None => "-",
        };
        println!(
            "futures label={} ack_channel={} ack_status={} live_status={} detail={}",
            expected.label, ack_channel, ack_status, live_status, note
        );
    }

    if !spot_failure_reasons.is_empty() {
        println!("spot_failure_reasons={}", spot_failure_reasons.join(" | "));
    }
    for ack in raw_spot_acks {
        println!("spot_ack_raw={ack}");
    }

    Ok(())
}

fn spot_kind(message: &MexcSpotWsMessage) -> Option<&'static str> {
    match message {
        MexcSpotWsMessage::SessionStart(_)
        | MexcSpotWsMessage::Ack(_)
        | MexcSpotWsMessage::RawText(_) => None,
        MexcSpotWsMessage::AggTrades(_) => Some("spot.aggTrades"),
        MexcSpotWsMessage::IncreaseDepth(_) => Some("spot.increaseDepth"),
        MexcSpotWsMessage::IncreaseDepthBatch(_) => Some("spot.increaseDepthBatch"),
        MexcSpotWsMessage::AggDepth(_) => Some("spot.aggDepth"),
        MexcSpotWsMessage::LimitDepth(_) => Some("spot.limitDepth"),
        MexcSpotWsMessage::BookTicker(_) => Some("spot.bookTickerRaw"),
        MexcSpotWsMessage::BookTickerBatch(_) => Some("spot.bookTickerBatch"),
        MexcSpotWsMessage::AggBookTicker(_) => Some("spot.bookTickerAgg"),
        MexcSpotWsMessage::Kline(_) => Some("spot.kline"),
        MexcSpotWsMessage::MiniTicker(_) => Some("spot.miniTicker"),
        MexcSpotWsMessage::MiniTickers(_) => Some("spot.miniTickers"),
    }
}

fn futures_kind(message: &MexcFuturesWsMessage) -> Option<&'static str> {
    match message {
        MexcFuturesWsMessage::SessionStart(_)
        | MexcFuturesWsMessage::Ack(_)
        | MexcFuturesWsMessage::Raw(_) => None,
        MexcFuturesWsMessage::Tickers(_) => Some("futures.tickers"),
        MexcFuturesWsMessage::Ticker(_) => Some("futures.ticker"),
        MexcFuturesWsMessage::Deals(_) => Some("futures.deals"),
        MexcFuturesWsMessage::Depth(_) => Some("futures.depth"),
        MexcFuturesWsMessage::DepthStep(_) => Some("futures.depthStep"),
        MexcFuturesWsMessage::DepthFull(_) => Some("futures.depthFull"),
        MexcFuturesWsMessage::FundingRate(_) => Some("futures.fundingRate"),
        MexcFuturesWsMessage::IndexPrice(_) => Some("futures.indexPrice"),
        MexcFuturesWsMessage::FairPrice(_) => Some("futures.fairPrice"),
        MexcFuturesWsMessage::Kline(_) => Some("futures.kline"),
        MexcFuturesWsMessage::Contract(_) => Some("futures.contract"),
        MexcFuturesWsMessage::EventContract(_) => Some("futures.eventContract"),
    }
}

fn describe_spot(message: &MexcSpotWsMessage) -> String {
    match message {
        MexcSpotWsMessage::SessionStart(session) => format!(
            "session_start id={} resumed={} subscriptions={}",
            session.session_id, session.resumed, session.subscription_count
        ),
        MexcSpotWsMessage::Ack(ack) => format!("ack {:?}", ack.msg),
        MexcSpotWsMessage::RawText(value) => value.to_string(),
        MexcSpotWsMessage::AggTrades(event) => format!(
            "channel={} symbol={:?} deals={}",
            event.channel,
            event.symbol,
            event.data.deals.len()
        ),
        MexcSpotWsMessage::IncreaseDepth(event) => format!(
            "channel={} symbol={:?} asks={} bids={}",
            event.channel,
            event.symbol,
            event.data.asks.len(),
            event.data.bids.len()
        ),
        MexcSpotWsMessage::IncreaseDepthBatch(event) => format!(
            "channel={} symbol={:?} items={}",
            event.channel,
            event.symbol,
            event.data.items.len()
        ),
        MexcSpotWsMessage::AggDepth(event) => format!(
            "channel={} symbol={:?} asks={} bids={}",
            event.channel,
            event.symbol,
            event.data.asks.len(),
            event.data.bids.len()
        ),
        MexcSpotWsMessage::LimitDepth(event) => format!(
            "channel={} symbol={:?} asks={} bids={}",
            event.channel,
            event.symbol,
            event.data.asks.len(),
            event.data.bids.len()
        ),
        MexcSpotWsMessage::BookTicker(event) => {
            format!("channel={} symbol={:?}", event.channel, event.symbol)
        }
        MexcSpotWsMessage::BookTickerBatch(event) => format!(
            "channel={} symbol={:?} items={}",
            event.channel,
            event.symbol,
            event.data.items.len()
        ),
        MexcSpotWsMessage::AggBookTicker(event) => {
            format!("channel={} symbol={:?}", event.channel, event.symbol)
        }
        MexcSpotWsMessage::Kline(event) => format!(
            "channel={} symbol={:?} interval={}",
            event.channel, event.symbol, event.data.interval
        ),
        MexcSpotWsMessage::MiniTicker(event) => {
            format!("channel={} symbol={:?}", event.channel, event.symbol)
        }
        MexcSpotWsMessage::MiniTickers(event) => {
            format!("channel={} items={}", event.channel, event.data.items.len())
        }
    }
}

fn describe_futures(message: &MexcFuturesWsMessage) -> String {
    match message {
        MexcFuturesWsMessage::SessionStart(session) => format!(
            "session_start id={} resumed={} subscriptions={}",
            session.session_id, session.resumed, session.subscription_count
        ),
        MexcFuturesWsMessage::Ack(ack) => format!("ack {:?}", ack.channel),
        MexcFuturesWsMessage::Raw(value) => value.to_string(),
        MexcFuturesWsMessage::Tickers(event) => {
            format!("channel={} items={}", event.channel, event.data.len())
        }
        MexcFuturesWsMessage::Ticker(event) => format!(
            "channel={} symbol={:?} last={}",
            event.channel, event.symbol, event.data.last_price
        ),
        MexcFuturesWsMessage::Deals(event) => format!(
            "channel={} symbol={:?} deals={}",
            event.channel,
            event.symbol,
            event.data.to_vec().len()
        ),
        MexcFuturesWsMessage::Depth(event) => format!(
            "channel={} symbol={:?} asks={} bids={}",
            event.channel,
            event.symbol,
            event.data.asks.len(),
            event.data.bids.len()
        ),
        MexcFuturesWsMessage::DepthStep(event) => format!(
            "channel={} symbol={:?} asks={} bids={}",
            event.channel,
            event.symbol,
            event.data.asks.len(),
            event.data.bids.len()
        ),
        MexcFuturesWsMessage::DepthFull(event) => format!(
            "channel={} symbol={:?} asks={} bids={}",
            event.channel,
            event.symbol,
            event.data.asks.len(),
            event.data.bids.len()
        ),
        MexcFuturesWsMessage::FundingRate(event) => format!(
            "channel={} symbol={:?} rate={}",
            event.channel, event.symbol, event.data.rate
        ),
        MexcFuturesWsMessage::IndexPrice(event) => format!(
            "channel={} symbol={:?} price={}",
            event.channel, event.symbol, event.data.price
        ),
        MexcFuturesWsMessage::FairPrice(event) => format!(
            "channel={} symbol={:?} price={}",
            event.channel, event.symbol, event.data.price
        ),
        MexcFuturesWsMessage::Kline(event) => format!(
            "channel={} symbol={:?} interval={}",
            event.channel, event.symbol, event.data.interval
        ),
        MexcFuturesWsMessage::Contract(event) => format!(
            "channel={} symbol={:?} state={:?}",
            event.channel, event.symbol, event.data.state
        ),
        MexcFuturesWsMessage::EventContract(event) => format!(
            "channel={} symbol={} contract_id={:?}",
            event.channel, event.data.symbol, event.data.contract_id
        ),
    }
}
