use anyhow::{Context, Result};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcConnector, MexcManagedRuntimePolicy,
    MexcPublicRuntimeConfig,
};
use serde::Serialize;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Serialize)]
struct StateFeedEvent {
    kind: &'static str,
    unix_ms: u128,
    elapsed_ms: u128,
    total_live_events: usize,
    total_rebuilds: usize,
    total_spot_resets: usize,
    total_futures_resets: usize,
    state: mexc::MexcPublicStateHandoffReport,
}

#[derive(Debug, Serialize)]
struct StateFeedSummary {
    kind: &'static str,
    watch_seconds: u64,
    snapshot_interval_seconds: u64,
    emitted_snapshots: usize,
    total_live_events: usize,
    total_rebuilds: usize,
    total_spot_resets: usize,
    total_futures_resets: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let watch_seconds = env_u64("MEXC_STATE_FEED_SECONDS", 20).max(1);
    let snapshot_interval_seconds = env_u64("MEXC_STATE_FEED_INTERVAL_SECONDS", 2).max(1);
    let output_path = std::env::var("MEXC_STATE_FEED_PATH").ok();

    let mut runtime = MexcConnector::default()
        .public_runtime_builder()
        .connect_managed_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await?;
    runtime
        .await_balanced_startup(Duration::from_secs(20))
        .await
        .context("await balanced startup")?;

    let mut policy = MexcManagedRuntimePolicy::balanced_defaults();
    policy.public_metadata_refresh_interval = None;
    policy.contract_refresh_interval = None;
    policy.contract_event_refresh = false;
    policy.contract_gap_refresh = false;

    let started_at = tokio::time::Instant::now();
    let deadline = started_at + Duration::from_secs(watch_seconds);
    let mut next_snapshot_at = started_at;
    let mut previous = None::<mexc::MexcPublicStateHandoffReport>;
    let mut emitted_snapshots = 0usize;
    let mut total_live_events = 0usize;

    while tokio::time::Instant::now() < deadline {
        let Some(step) = runtime.next_step(&policy).await else {
            break;
        };
        let step = step?;
        total_live_events += usize::from(step.event.is_live_payload());

        let now = tokio::time::Instant::now();
        if now < next_snapshot_at {
            continue;
        }

        let state = runtime.state_handoff_report();
        let changed = previous
            .as_ref()
            .map(|prior| prior != &state)
            .unwrap_or(true);
        if changed {
            let session = runtime.session_status();
            let event = StateFeedEvent {
                kind: "state_snapshot",
                unix_ms: unix_ms()?,
                elapsed_ms: started_at.elapsed().as_millis(),
                total_live_events,
                total_rebuilds: runtime.total_rebuilds(),
                total_spot_resets: session.total_spot_resets,
                total_futures_resets: session.total_futures_resets,
                state: state.clone(),
            };
            emit_json_line(output_path.as_deref(), &event)?;
            emitted_snapshots += 1;
            previous = Some(state);
        }

        while next_snapshot_at <= now {
            next_snapshot_at += Duration::from_secs(snapshot_interval_seconds);
        }
    }

    let session = runtime.session_status();
    let summary = StateFeedSummary {
        kind: "summary",
        watch_seconds,
        snapshot_interval_seconds,
        emitted_snapshots,
        total_live_events,
        total_rebuilds: runtime.total_rebuilds(),
        total_spot_resets: session.total_spot_resets,
        total_futures_resets: session.total_futures_resets,
    };
    if let Some(path) = output_path.as_deref() {
        println!("wrote state change feed to {path}");
    }
    println!("{}", serde_json::to_string(&summary)?);

    Ok(())
}

fn emit_json_line(path: Option<&str>, value: &impl Serialize) -> Result<()> {
    let line = serde_json::to_string(value)? + "\n";
    if let Some(path) = path {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open state change feed path {path}"))?;
        file.write_all(line.as_bytes())
            .with_context(|| format!("append state change feed line to {path}"))?;
    } else {
        print!("{line}");
    }
    Ok(())
}

fn unix_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_millis())
}
