use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{MexcConnector, MexcFuturesSubscription, MexcFuturesWsMessage};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant, timeout};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Serialize)]
struct ContractChannelReport {
    unix_started_at_s: u64,
    watch_seconds: u64,
    acked_contract: bool,
    acked_event_contract: bool,
    contract_payloads: usize,
    event_contract_payloads: usize,
    first_contract_payload_after_ms: Option<u128>,
    first_event_contract_payload_after_ms: Option<u128>,
    last_contract_symbol: Option<String>,
    last_event_contract_symbol: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let watch_seconds = env_u64("MEXC_CONTRACT_WATCH_SECONDS", 60).max(1);
    let report_path = std::env::var("MEXC_CONTRACT_REPORT_PATH").ok();
    let unix_started_at_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs();

    let connector = MexcConnector::default();
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
    let mut acked_contract = false;
    let mut acked_event_contract = false;
    let mut contract_payloads = 0usize;
    let mut event_contract_payloads = 0usize;
    let mut first_contract_payload_after_ms = None;
    let mut first_event_contract_payload_after_ms = None;
    let mut last_contract_symbol = None;
    let mut last_event_contract_symbol = None;

    while Instant::now() < deadline {
        let maybe_message = match timeout(Duration::from_secs(10), stream.next()).await {
            Ok(maybe_message) => maybe_message,
            Err(_) => continue,
        };
        let message = maybe_message
            .transpose()?
            .context("contract channel stream ended unexpectedly")?;

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
            _ => {}
        }
    }

    let report = ContractChannelReport {
        unix_started_at_s,
        watch_seconds,
        acked_contract,
        acked_event_contract,
        contract_payloads,
        event_contract_payloads,
        first_contract_payload_after_ms,
        first_event_contract_payload_after_ms,
        last_contract_symbol,
        last_event_contract_symbol,
    };

    let json = serde_json::to_string_pretty(&report).context("serialize contract report")?;
    if let Some(path) = report_path {
        std::fs::write(&path, json).with_context(|| format!("write contract report to {path}"))?;
        println!("wrote contract report to {path}");
    } else {
        println!("{json}");
    }

    Ok(())
}
