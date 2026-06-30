use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{
    MexcConnector, MexcFuturesContractChange, MexcFuturesSubscription, MexcFuturesWsMessage,
    MexcPublicState,
};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant, interval, timeout};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Serialize)]
struct ContractRefreshEntry {
    elapsed_ms: u128,
    refreshed_contracts: usize,
    added: usize,
    added_symbols: Vec<String>,
    updated: usize,
    updated_symbols: Vec<String>,
    removed: usize,
    removed_symbols: Vec<String>,
    unchanged: usize,
    changes: Vec<MexcFuturesContractChange>,
}

#[derive(Debug, Serialize)]
struct ContractObserverReport {
    unix_started_at_s: u64,
    watch_seconds: u64,
    refresh_interval_seconds: u64,
    baseline_contracts: usize,
    acked_contract: bool,
    acked_event_contract: bool,
    contract_payloads: usize,
    event_contract_payloads: usize,
    raw_contract_frames: usize,
    raw_event_contract_frames: usize,
    first_contract_payload_after_ms: Option<u128>,
    first_event_contract_payload_after_ms: Option<u128>,
    last_contract_symbol: Option<String>,
    last_event_contract_symbol: Option<String>,
    refresh_count: usize,
    refresh_total_added: usize,
    refresh_total_updated: usize,
    refresh_total_removed: usize,
    refresh_total_unchanged: usize,
    refresh_entries: Vec<ContractRefreshEntry>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let watch_seconds = env_u64("MEXC_CONTRACT_OBSERVER_SECONDS", 60).max(1);
    let refresh_interval_seconds = env_u64("MEXC_CONTRACT_REFRESH_INTERVAL_SECONDS", 15).max(1);
    let report_path = std::env::var("MEXC_CONTRACT_OBSERVER_REPORT_PATH").ok();
    let unix_started_at_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs();

    let connector = MexcConnector::default();
    let mut state = MexcPublicState::default();
    let baseline_contracts = connector
        .rest
        .futures_contracts_all()
        .await
        .context("load baseline futures contracts")?;
    let baseline_contract_count = baseline_contracts.len();
    state.hydrate_futures_contract_snapshot(&baseline_contracts);

    let mut stream = connector
        .futures_ws
        .connect(vec![
            MexcFuturesSubscription::Contract,
            MexcFuturesSubscription::EventContract,
        ])
        .await
        .context("connect contract/event-contract websocket")?;

    let started_at = Instant::now();
    let deadline = started_at + Duration::from_secs(watch_seconds);
    let mut refresh = interval(Duration::from_secs(refresh_interval_seconds));
    refresh.tick().await;

    let mut acked_contract = false;
    let mut acked_event_contract = false;
    let mut contract_payloads = 0usize;
    let mut event_contract_payloads = 0usize;
    let mut raw_contract_frames = 0usize;
    let mut raw_event_contract_frames = 0usize;
    let mut first_contract_payload_after_ms = None;
    let mut first_event_contract_payload_after_ms = None;
    let mut last_contract_symbol = None;
    let mut last_event_contract_symbol = None;
    let mut refresh_count = 0usize;
    let mut refresh_total_added = 0usize;
    let mut refresh_total_updated = 0usize;
    let mut refresh_total_removed = 0usize;
    let mut refresh_total_unchanged = 0usize;
    let mut refresh_entries = Vec::<ContractRefreshEntry>::new();

    while Instant::now() < deadline {
        tokio::select! {
            _ = refresh.tick() => {
                let contracts = connector
                    .rest
                    .futures_contracts_all()
                    .await
                    .context("refresh futures contracts")?;
                let delta = state.hydrate_futures_contract_snapshot(&contracts);
                refresh_count += 1;
                refresh_total_added += delta.added;
                refresh_total_updated += delta.updated;
                refresh_total_removed += delta.removed;
                refresh_total_unchanged += delta.unchanged;
                refresh_entries.push(ContractRefreshEntry {
                    elapsed_ms: started_at.elapsed().as_millis(),
                    refreshed_contracts: delta.refreshed_contracts,
                    added: delta.added,
                    added_symbols: delta.added_symbols,
                    updated: delta.updated,
                    updated_symbols: delta.updated_symbols,
                    removed: delta.removed,
                    removed_symbols: delta.removed_symbols,
                    unchanged: delta.unchanged,
                    changes: delta.changes,
                });
            }
            maybe_message = timeout(Duration::from_secs(10), stream.next()) => {
                let maybe_message = match maybe_message {
                    Ok(message) => message,
                    Err(_) => continue,
                };
                let Some(message) = maybe_message.transpose()? else {
                    continue;
                };
                match message {
                    MexcFuturesWsMessage::SessionStart(_) => {}
                    MexcFuturesWsMessage::Ack(ack) => {
                        if ack.channel.as_deref() == Some("rs.sub.contract") {
                            acked_contract = true;
                        }
                        if ack.channel.as_deref() == Some("rs.sub.event.contract") {
                            acked_event_contract = true;
                        }
                    }
                    MexcFuturesWsMessage::Contract(event) => {
                        contract_payloads += 1;
                        if first_contract_payload_after_ms.is_none() {
                            first_contract_payload_after_ms = Some(started_at.elapsed().as_millis());
                        }
                        last_contract_symbol = Some(event.data.symbol);
                    }
                    MexcFuturesWsMessage::EventContract(event) => {
                        event_contract_payloads += 1;
                        if first_event_contract_payload_after_ms.is_none() {
                            first_event_contract_payload_after_ms = Some(started_at.elapsed().as_millis());
                        }
                        last_event_contract_symbol = Some(event.data.symbol);
                    }
                    MexcFuturesWsMessage::Raw(value) => {
                        match value.get("channel").and_then(|channel| channel.as_str()) {
                            Some("push.contract") => raw_contract_frames += 1,
                            Some("push.event.contract") => raw_event_contract_frames += 1,
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let report = ContractObserverReport {
        unix_started_at_s,
        watch_seconds,
        refresh_interval_seconds,
        baseline_contracts: baseline_contract_count,
        acked_contract,
        acked_event_contract,
        contract_payloads,
        event_contract_payloads,
        raw_contract_frames,
        raw_event_contract_frames,
        first_contract_payload_after_ms,
        first_event_contract_payload_after_ms,
        last_contract_symbol,
        last_event_contract_symbol,
        refresh_count,
        refresh_total_added,
        refresh_total_updated,
        refresh_total_removed,
        refresh_total_unchanged,
        refresh_entries,
    };

    let json =
        serde_json::to_string_pretty(&report).context("serialize contract observer report")?;
    if let Some(path) = report_path {
        std::fs::write(&path, json)
            .with_context(|| format!("write contract observer report to {path}"))?;
        println!("wrote contract observer report to {path}");
    } else {
        println!("{json}");
    }

    Ok(())
}
