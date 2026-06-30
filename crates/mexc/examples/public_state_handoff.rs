use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{MexcConnector, MexcPublicRuntimeConfig, MexcPublicState};
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    let connector = MexcConnector::default();
    let runtime = connector
        .public_runtime_builder()
        .connect_with_snapshot(MexcPublicRuntimeConfig::balanced())
        .await
        .context("connect public runtime with snapshot")?;

    let mut state = MexcPublicState::from_snapshot(&runtime.snapshot);
    println!(
        "initial spot_symbols={} futures_symbols={} spot_server_time={} futures_server_time={} spot_exchange_symbols={} spot_offline_symbols={}",
        state.spot_symbols.len(),
        state.futures_symbols.len(),
        state.spot_server_time.unwrap_or_default(),
        state.futures_server_time.unwrap_or_default(),
        state
            .spot_exchange_info
            .as_ref()
            .map(|info| info.symbols.len())
            .unwrap_or(0),
        state.spot_offline_symbols.len()
    );

    let mut stream = runtime.stream;
    let mut applied = 0usize;
    let mut live_applied = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while tokio::time::Instant::now() < deadline {
        let event = timeout(Duration::from_secs(10), stream.next())
            .await
            .context("wait for runtime event")?
            .transpose()?
            .context("runtime stream ended unexpectedly")?;
        state.apply_event(&event);
        applied += 1;

        let is_live_payload = event.is_live_payload();
        if is_live_payload {
            live_applied += 1;
        }

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

    println!(
        "applied={} live_applied={} spot_symbols={} futures_symbols={} global_spot_miniticker_channels={} futures_insurance={} spot_default_symbols={} spot_offline_symbols={} spot_exchange_symbols={} spot_server_time={} futures_server_time={}",
        applied,
        live_applied,
        state.spot_symbols.len(),
        state.futures_symbols.len(),
        state.global_spot_mini_tickers_by_channel.len(),
        state.futures_insurance_balances.len(),
        state.spot_default_symbols.len(),
        state.spot_offline_symbols.len(),
        state
            .spot_exchange_info
            .as_ref()
            .map(|info| info.symbols.len())
            .unwrap_or(0),
        state.spot_server_time.unwrap_or_default(),
        state.futures_server_time.unwrap_or_default()
    );

    if let Some(symbol) = state.spot_symbols.get("BTCUSDT") {
        println!(
            "spot BTCUSDT has_24hr={} has_price={} has_book={} has_agg_depth={} kline_intervals={} mini_channels={}",
            symbol.ticker_24hr.is_some(),
            symbol.price_ticker.is_some(),
            symbol.book_ticker_snapshot.is_some() || symbol.agg_book_ticker.is_some(),
            symbol.agg_depth.is_some(),
            symbol.latest_kline_by_interval.len(),
            symbol.mini_ticker_by_channel.len()
        );
    }

    if let Some(symbol) = state.futures_symbols.get("BTC_USDT") {
        println!(
            "futures BTC_USDT has_contract={} has_ticker={} has_depth={} has_funding={} has_index={} has_fair={} kline_intervals={} depth_steps={}",
            symbol.contract.is_some(),
            symbol.ticker.is_some(),
            symbol.depth.is_some(),
            symbol.funding_rate_live.is_some(),
            symbol.index_price.is_some(),
            symbol.fair_price.is_some(),
            symbol.latest_kline_by_interval.len(),
            symbol.depth_step_by_channel.len()
        );
    }

    Ok(())
}
