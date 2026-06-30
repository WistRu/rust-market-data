use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{MexcConnector, MexcPublicRuntimeConfig, MexcPublicState};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, timeout};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Serialize)]
struct SymbolReadiness {
    symbol: String,
    has_24hr: bool,
    has_price: bool,
    has_book: bool,
    has_agg_depth: bool,
    kline_intervals: usize,
    mini_channels: usize,
}

#[derive(Debug, Serialize)]
struct FuturesSymbolReadiness {
    symbol: String,
    has_contract: bool,
    has_ticker: bool,
    has_depth: bool,
    has_funding: bool,
    has_index: bool,
    has_fair: bool,
    kline_intervals: usize,
    depth_steps: usize,
}

#[derive(Debug, Serialize)]
struct PublicStateHandoffArtifact {
    unix_generated_at_s: u64,
    watch_seconds: u64,
    applied_events: usize,
    live_applied_events: usize,
    state: mexc::MexcPublicStateHandoffReport,
    btcusdt: Option<SymbolReadiness>,
    btc_usdt: Option<FuturesSymbolReadiness>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let watch_seconds = env_u64("MEXC_STATE_HANDOFF_SECONDS", 20).max(1);
    let report_path = std::env::var("MEXC_STATE_HANDOFF_REPORT_PATH").ok();

    let runtime = MexcConnector::default()
        .public_runtime_builder()
        .connect_with_snapshot(MexcPublicRuntimeConfig::balanced())
        .await
        .context("connect public runtime with snapshot")?;

    let mut state = MexcPublicState::from_snapshot(&runtime.snapshot);
    let mut stream = runtime.stream;
    let mut applied = 0usize;
    let mut live_applied = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(watch_seconds);

    while tokio::time::Instant::now() < deadline {
        let event = timeout(Duration::from_secs(10), stream.next())
            .await
            .context("wait for runtime event")?
            .transpose()?
            .context("runtime stream ended unexpectedly")?;
        state.apply_event(&event);
        applied += 1;
        live_applied += usize::from(event.is_live_payload());

        let spot_ready = state
            .spot_symbols
            .get("BTCUSDT")
            .map(|symbol| {
                symbol.agg_depth.is_some()
                    || symbol.limit_depth.is_some()
                    || !symbol.latest_kline_by_interval.is_empty()
                    || !symbol.mini_ticker_by_channel.is_empty()
                    || symbol.latest_trades.is_some()
            })
            .unwrap_or(false);
        let futures_ready = state
            .futures_symbols
            .get("BTC_USDT")
            .map(|symbol| {
                symbol.depth.is_some()
                    || symbol.funding_rate_live.is_some()
                    || symbol.index_price.is_some()
                    || symbol.fair_price.is_some()
                    || !symbol.latest_kline_by_interval.is_empty()
                    || !symbol.depth_step_by_channel.is_empty()
                    || symbol.latest_deals.is_some()
            })
            .unwrap_or(false);

        if live_applied >= 200 && spot_ready && futures_ready {
            break;
        }
    }

    let artifact = PublicStateHandoffArtifact {
        unix_generated_at_s: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before unix epoch")?
            .as_secs(),
        watch_seconds,
        applied_events: applied,
        live_applied_events: live_applied,
        state: state.handoff_report(),
        btcusdt: state
            .spot_symbols
            .get("BTCUSDT")
            .map(|symbol| SymbolReadiness {
                symbol: "BTCUSDT".to_string(),
                has_24hr: symbol.ticker_24hr.is_some(),
                has_price: symbol.price_ticker.is_some(),
                has_book: symbol.book_ticker_snapshot.is_some() || symbol.agg_book_ticker.is_some(),
                has_agg_depth: symbol.agg_depth.is_some(),
                kline_intervals: symbol.latest_kline_by_interval.len(),
                mini_channels: symbol.mini_ticker_by_channel.len(),
            }),
        btc_usdt: state
            .futures_symbols
            .get("BTC_USDT")
            .map(|symbol| FuturesSymbolReadiness {
                symbol: "BTC_USDT".to_string(),
                has_contract: symbol.contract.is_some(),
                has_ticker: symbol.ticker.is_some(),
                has_depth: symbol.depth.is_some(),
                has_funding: symbol.funding_rate_live.is_some(),
                has_index: symbol.index_price.is_some(),
                has_fair: symbol.fair_price.is_some(),
                kline_intervals: symbol.latest_kline_by_interval.len(),
                depth_steps: symbol.depth_step_by_channel.len(),
            }),
    };

    let json = serde_json::to_string_pretty(&artifact).context("serialize handoff artifact")?;
    if let Some(path) = report_path {
        std::fs::write(&path, json).with_context(|| format!("write handoff artifact to {path}"))?;
        println!("wrote handoff artifact to {path}");
    } else {
        println!("{json}");
    }

    Ok(())
}
