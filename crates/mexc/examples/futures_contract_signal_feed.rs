use anyhow::{Context, Result};
use futures::StreamExt;
use mexc::{
    MexcConnector, MexcFuturesContractChange, MexcFuturesSubscription, MexcFuturesWsMessage,
    MexcPublicState, OneOrMany,
};
use serde::Serialize;
use serde_json::Value;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant, interval, timeout};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_optional_path(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[derive(Debug, Serialize)]
struct FeedEvent<'a, T: Serialize> {
    kind: &'a str,
    unix_ms: u128,
    elapsed_ms: u128,
    payload: T,
}

#[derive(Debug, Serialize)]
struct AckPayload {
    channel: Option<String>,
    code: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SignalPayload {
    channel: &'static str,
    symbol: Option<String>,
}

#[derive(Debug, Serialize)]
struct RawSignalPayload {
    channel: String,
    symbol: Option<String>,
    raw: Value,
}

#[derive(Debug, Serialize)]
struct RefreshPayload {
    requested_symbol: String,
    refreshed_contracts: usize,
    added: usize,
    updated: usize,
    removed: usize,
    unchanged: usize,
    changes: Vec<MexcFuturesContractChange>,
}

#[derive(Debug, Serialize)]
struct FeedSummary {
    kind: &'static str,
    elapsed_ms: u128,
    watch_seconds: u64,
    watch_forever: bool,
    checkpoint_interval_seconds: Option<u64>,
    refresh_on_signal: bool,
    baseline_contracts: usize,
    acked_contract: bool,
    acked_event_contract: bool,
    session_starts: usize,
    resumed_sessions: usize,
    typed_contract_payloads: usize,
    typed_event_contract_payloads: usize,
    raw_contract_frames: usize,
    raw_event_contract_frames: usize,
    stream_errors: usize,
    first_contract_payload_after_ms: Option<u128>,
    first_event_contract_payload_after_ms: Option<u128>,
    first_raw_contract_frame_after_ms: Option<u128>,
    first_raw_event_contract_frame_after_ms: Option<u128>,
    last_contract_symbol: Option<String>,
    last_event_contract_symbol: Option<String>,
    last_raw_contract_symbol: Option<String>,
    last_raw_event_contract_symbol: Option<String>,
    last_signal_kind: Option<String>,
    last_signal_symbol: Option<String>,
    last_signal_after_ms: Option<u128>,
    signal_refreshes: usize,
    last_error: Option<String>,
    stop_reason: Option<&'static str>,
    emitted_lines: usize,
}

#[derive(Debug, Default)]
struct FeedStats {
    acked_contract: bool,
    acked_event_contract: bool,
    session_starts: usize,
    resumed_sessions: usize,
    typed_contract_payloads: usize,
    typed_event_contract_payloads: usize,
    raw_contract_frames: usize,
    raw_event_contract_frames: usize,
    stream_errors: usize,
    first_contract_payload_after_ms: Option<u128>,
    first_event_contract_payload_after_ms: Option<u128>,
    first_raw_contract_frame_after_ms: Option<u128>,
    first_raw_event_contract_frame_after_ms: Option<u128>,
    last_contract_symbol: Option<String>,
    last_event_contract_symbol: Option<String>,
    last_raw_contract_symbol: Option<String>,
    last_raw_event_contract_symbol: Option<String>,
    last_signal_kind: Option<String>,
    last_signal_symbol: Option<String>,
    last_signal_after_ms: Option<u128>,
    signal_refreshes: usize,
    last_error: Option<String>,
}

impl FeedStats {
    fn note_signal(&mut self, kind: &str, symbol: Option<&str>, elapsed_ms: u128) {
        self.last_signal_kind = Some(kind.to_string());
        self.last_signal_symbol = symbol.map(ToOwned::to_owned);
        self.last_signal_after_ms = Some(elapsed_ms);
    }

    fn summary(
        &self,
        kind: &'static str,
        elapsed_ms: u128,
        watch_seconds: u64,
        checkpoint_interval_seconds: Option<u64>,
        refresh_on_signal: bool,
        baseline_contracts: usize,
        stop_reason: Option<&'static str>,
        emitted_lines: usize,
    ) -> FeedSummary {
        FeedSummary {
            kind,
            elapsed_ms,
            watch_seconds,
            watch_forever: watch_seconds == 0,
            checkpoint_interval_seconds,
            refresh_on_signal,
            baseline_contracts,
            acked_contract: self.acked_contract,
            acked_event_contract: self.acked_event_contract,
            session_starts: self.session_starts,
            resumed_sessions: self.resumed_sessions,
            typed_contract_payloads: self.typed_contract_payloads,
            typed_event_contract_payloads: self.typed_event_contract_payloads,
            raw_contract_frames: self.raw_contract_frames,
            raw_event_contract_frames: self.raw_event_contract_frames,
            stream_errors: self.stream_errors,
            first_contract_payload_after_ms: self.first_contract_payload_after_ms,
            first_event_contract_payload_after_ms: self.first_event_contract_payload_after_ms,
            first_raw_contract_frame_after_ms: self.first_raw_contract_frame_after_ms,
            first_raw_event_contract_frame_after_ms: self.first_raw_event_contract_frame_after_ms,
            last_contract_symbol: self.last_contract_symbol.clone(),
            last_event_contract_symbol: self.last_event_contract_symbol.clone(),
            last_raw_contract_symbol: self.last_raw_contract_symbol.clone(),
            last_raw_event_contract_symbol: self.last_raw_event_contract_symbol.clone(),
            last_signal_kind: self.last_signal_kind.clone(),
            last_signal_symbol: self.last_signal_symbol.clone(),
            last_signal_after_ms: self.last_signal_after_ms,
            signal_refreshes: self.signal_refreshes,
            last_error: self.last_error.clone(),
            stop_reason,
            emitted_lines,
        }
    }
}

#[derive(Debug, Serialize)]
struct ErrorPayload {
    error: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let watch_seconds = env_u64("MEXC_CONTRACT_SIGNAL_FEED_SECONDS", 60);
    let checkpoint_interval_seconds = env_u64("MEXC_CONTRACT_SIGNAL_CHECKPOINT_SECONDS", 30);
    let checkpoint_interval_seconds =
        (checkpoint_interval_seconds > 0).then_some(checkpoint_interval_seconds);
    let refresh_on_signal = env_u64("MEXC_CONTRACT_SIGNAL_REFRESH", 1) > 0;
    let output_path = env_optional_path("MEXC_CONTRACT_SIGNAL_FEED_PATH");
    let summary_path = env_optional_path("MEXC_CONTRACT_SIGNAL_SUMMARY_PATH");

    let connector = MexcConnector::default();
    let mut state = MexcPublicState::default();
    let baseline_contracts = connector
        .rest
        .futures_contracts_all()
        .await
        .context("load baseline futures contracts")?;
    state.hydrate_futures_contract_snapshot(&baseline_contracts);

    let mut stream = connector
        .futures_ws
        .connect(vec![
            MexcFuturesSubscription::Contract,
            MexcFuturesSubscription::EventContract,
        ])
        .await
        .context("connect contract/event-contract websocket")?;
    let baseline_contract_count = baseline_contracts.len();

    let started_at = Instant::now();
    let deadline = (watch_seconds > 0).then(|| started_at + Duration::from_secs(watch_seconds));
    let mut checkpoint =
        checkpoint_interval_seconds.map(|seconds| interval(Duration::from_secs(seconds)));
    if let Some(checkpoint) = checkpoint.as_mut() {
        checkpoint.tick().await;
    }
    let mut stats = FeedStats::default();
    let mut emitted_lines = 0usize;
    let stop_reason = loop {
        if let Some(deadline) = deadline {
            if Instant::now() >= deadline {
                break "deadline";
            }
        }

        let maybe_message = if let Some(checkpoint) = checkpoint.as_mut() {
            tokio::select! {
                _ = checkpoint.tick() => {
                    emitted_lines += emit_checkpoint(
                        output_path.as_deref(),
                        summary_path.as_deref(),
                        &stats,
                        started_at,
                        watch_seconds,
                        checkpoint_interval_seconds,
                        refresh_on_signal,
                        baseline_contract_count,
                        emitted_lines + 1,
                    )?;
                    continue;
                }
                maybe_message = timeout(Duration::from_secs(10), stream.next()) => maybe_message,
            }
        } else {
            timeout(Duration::from_secs(10), stream.next()).await
        };

        let maybe_message = match maybe_message {
            Ok(maybe_message) => maybe_message,
            Err(_) => continue,
        };
        let Some(message) = maybe_message else {
            break "stream_closed";
        };
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                stats.stream_errors += 1;
                stats.last_error = Some(error.to_string());
                emitted_lines += emit_json_line(
                    output_path.as_deref(),
                    &FeedEvent {
                        kind: "stream_error",
                        unix_ms: unix_ms()?,
                        elapsed_ms: started_at.elapsed().as_millis(),
                        payload: ErrorPayload {
                            error: error.to_string(),
                        },
                    },
                )?;
                if let Some(path) = summary_path.as_deref() {
                    write_json_file(
                        path,
                        &stats.summary(
                            "checkpoint",
                            started_at.elapsed().as_millis(),
                            watch_seconds,
                            checkpoint_interval_seconds,
                            refresh_on_signal,
                            baseline_contract_count,
                            None,
                            emitted_lines,
                        ),
                    )?;
                }
                continue;
            }
        };

        match message {
            MexcFuturesWsMessage::SessionStart(session) => {
                stats.session_starts += 1;
                if session.resumed {
                    stats.resumed_sessions += 1;
                }
                emitted_lines += emit_json_line(
                    output_path.as_deref(),
                    &FeedEvent {
                        kind: "session_start",
                        unix_ms: unix_ms()?,
                        elapsed_ms: started_at.elapsed().as_millis(),
                        payload: session,
                    },
                )?;
            }
            MexcFuturesWsMessage::Ack(ack) => {
                if ack.channel.as_deref() == Some("rs.sub.contract") {
                    stats.acked_contract = true;
                }
                if ack.channel.as_deref() == Some("rs.sub.event.contract") {
                    stats.acked_event_contract = true;
                }
                emitted_lines += emit_json_line(
                    output_path.as_deref(),
                    &FeedEvent {
                        kind: "ack",
                        unix_ms: unix_ms()?,
                        elapsed_ms: started_at.elapsed().as_millis(),
                        payload: AckPayload {
                            channel: ack.channel,
                            code: ack.code,
                        },
                    },
                )?;
            }
            MexcFuturesWsMessage::Contract(event) => {
                stats.typed_contract_payloads += 1;
                if stats.first_contract_payload_after_ms.is_none() {
                    stats.first_contract_payload_after_ms = Some(started_at.elapsed().as_millis());
                }
                let symbol = event
                    .symbol
                    .clone()
                    .or_else(|| Some(event.data.symbol.clone()));
                stats.last_contract_symbol = symbol.clone();
                stats.note_signal(
                    "push.contract",
                    symbol.as_deref(),
                    started_at.elapsed().as_millis(),
                );
                emitted_lines += emit_json_line(
                    output_path.as_deref(),
                    &FeedEvent {
                        kind: "typed_contract_payload",
                        unix_ms: unix_ms()?,
                        elapsed_ms: started_at.elapsed().as_millis(),
                        payload: SignalPayload {
                            channel: "push.contract",
                            symbol: symbol.clone(),
                        },
                    },
                )?;
                if refresh_on_signal {
                    if let Some(symbol) = symbol {
                        let emitted = refresh_symbol(
                            &connector,
                            &mut state,
                            &symbol,
                            started_at,
                            output_path.as_deref(),
                        )
                        .await?;
                        stats.signal_refreshes += emitted;
                        emitted_lines += emitted;
                    }
                }
            }
            MexcFuturesWsMessage::EventContract(event) => {
                stats.typed_event_contract_payloads += 1;
                if stats.first_event_contract_payload_after_ms.is_none() {
                    stats.first_event_contract_payload_after_ms =
                        Some(started_at.elapsed().as_millis());
                }
                let symbol = Some(event.data.symbol.clone());
                stats.last_event_contract_symbol = symbol.clone();
                stats.note_signal(
                    "push.event.contract",
                    symbol.as_deref(),
                    started_at.elapsed().as_millis(),
                );
                emitted_lines += emit_json_line(
                    output_path.as_deref(),
                    &FeedEvent {
                        kind: "typed_event_contract_payload",
                        unix_ms: unix_ms()?,
                        elapsed_ms: started_at.elapsed().as_millis(),
                        payload: SignalPayload {
                            channel: "push.event.contract",
                            symbol: symbol.clone(),
                        },
                    },
                )?;
                if refresh_on_signal {
                    if let Some(symbol) = symbol {
                        let emitted = refresh_symbol(
                            &connector,
                            &mut state,
                            &symbol,
                            started_at,
                            output_path.as_deref(),
                        )
                        .await?;
                        stats.signal_refreshes += emitted;
                        emitted_lines += emitted;
                    }
                }
            }
            MexcFuturesWsMessage::Raw(value) => {
                let Some(channel) = value
                    .get("channel")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                else {
                    continue;
                };
                if channel != "push.contract" && channel != "push.event.contract" {
                    continue;
                }
                let symbol = raw_contract_signal_symbol(&value);
                match channel.as_str() {
                    "push.contract" => {
                        stats.raw_contract_frames += 1;
                        if stats.first_raw_contract_frame_after_ms.is_none() {
                            stats.first_raw_contract_frame_after_ms =
                                Some(started_at.elapsed().as_millis());
                        }
                        stats.last_raw_contract_symbol = symbol.clone();
                    }
                    "push.event.contract" => {
                        stats.raw_event_contract_frames += 1;
                        if stats.first_raw_event_contract_frame_after_ms.is_none() {
                            stats.first_raw_event_contract_frame_after_ms =
                                Some(started_at.elapsed().as_millis());
                        }
                        stats.last_raw_event_contract_symbol = symbol.clone();
                    }
                    _ => {}
                }
                stats.note_signal(
                    &channel,
                    symbol.as_deref(),
                    started_at.elapsed().as_millis(),
                );
                emitted_lines += emit_json_line(
                    output_path.as_deref(),
                    &FeedEvent {
                        kind: "raw_contract_frame",
                        unix_ms: unix_ms()?,
                        elapsed_ms: started_at.elapsed().as_millis(),
                        payload: RawSignalPayload {
                            channel: channel.clone(),
                            symbol: symbol.clone(),
                            raw: value,
                        },
                    },
                )?;
                if refresh_on_signal {
                    if let Some(symbol) = symbol {
                        let emitted = refresh_symbol(
                            &connector,
                            &mut state,
                            &symbol,
                            started_at,
                            output_path.as_deref(),
                        )
                        .await?;
                        stats.signal_refreshes += emitted;
                        emitted_lines += emitted;
                    }
                }
            }
            _ => {}
        }
    };

    let summary = stats.summary(
        "summary",
        started_at.elapsed().as_millis(),
        watch_seconds,
        checkpoint_interval_seconds,
        refresh_on_signal,
        baseline_contract_count,
        Some(stop_reason),
        emitted_lines + 1,
    );
    emit_json_line(output_path.as_deref(), &summary)?;
    if let Some(path) = summary_path.as_deref() {
        write_json_file(path, &summary)?;
        println!("wrote contract signal summary to {path}");
    }
    if let Some(path) = output_path.as_deref() {
        println!("wrote contract signal feed to {path}");
    }
    println!("{}", serde_json::to_string(&summary)?);

    Ok(())
}

async fn refresh_symbol(
    connector: &MexcConnector,
    state: &mut MexcPublicState,
    symbol: &str,
    started_at: Instant,
    output_path: Option<&str>,
) -> Result<usize> {
    let refreshed = match connector
        .rest
        .futures_contracts(Some(symbol))
        .await
        .with_context(|| format!("refresh futures contract for {symbol}"))?
    {
        OneOrMany::One(item) => vec![item],
        OneOrMany::Many(items) => items,
    };
    let delta = state.hydrate_futures_contract_updates(&refreshed);
    emit_json_line(
        output_path,
        &FeedEvent {
            kind: "signal_refresh",
            unix_ms: unix_ms()?,
            elapsed_ms: started_at.elapsed().as_millis(),
            payload: RefreshPayload {
                requested_symbol: symbol.to_string(),
                refreshed_contracts: delta.refreshed_contracts,
                added: delta.added,
                updated: delta.updated,
                removed: delta.removed,
                unchanged: delta.unchanged,
                changes: delta.changes,
            },
        },
    )?;
    Ok(1)
}

fn emit_checkpoint(
    output_path: Option<&str>,
    summary_path: Option<&str>,
    stats: &FeedStats,
    started_at: Instant,
    watch_seconds: u64,
    checkpoint_interval_seconds: Option<u64>,
    refresh_on_signal: bool,
    baseline_contracts: usize,
    emitted_lines: usize,
) -> Result<usize> {
    let summary = stats.summary(
        "checkpoint",
        started_at.elapsed().as_millis(),
        watch_seconds,
        checkpoint_interval_seconds,
        refresh_on_signal,
        baseline_contracts,
        None,
        emitted_lines,
    );
    if let Some(path) = summary_path {
        write_json_file(path, &summary)?;
    }
    emit_json_line(output_path, &summary)
}

fn raw_contract_signal_symbol(raw: &Value) -> Option<String> {
    raw.get("symbol")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            raw.get("data")
                .and_then(Value::as_object)
                .and_then(|data| data.get("symbol"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

fn emit_json_line(path: Option<&str>, value: &impl Serialize) -> Result<usize> {
    let line = serde_json::to_string(value)? + "\n";
    if let Some(path) = path {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open contract signal feed path {path}"))?;
        file.write_all(line.as_bytes())
            .with_context(|| format!("append contract signal feed line to {path}"))?;
    } else {
        print!("{line}");
    }
    Ok(1)
}

fn write_json_file(path: &str, value: &impl Serialize) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    std::fs::write(path, json).with_context(|| format!("write json file to {path}"))?;
    Ok(())
}

fn unix_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_millis())
}
