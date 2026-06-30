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
struct ContractChangeFeedEvent<'a> {
    kind: &'static str,
    unix_ms: u128,
    refreshed_contracts: usize,
    added: usize,
    updated: usize,
    removed: usize,
    unchanged: usize,
    change_count: usize,
    report: &'a mexc::MexcManagedContractRefreshReport,
}

#[derive(Debug, Serialize)]
struct ContractChangeFeedSummary {
    kind: &'static str,
    watch_seconds: u64,
    refresh_interval_seconds: u64,
    total_refreshes: usize,
    change_refreshes: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let watch_seconds = env_u64("MEXC_CONTRACT_CHANGE_FEED_SECONDS", 30).max(1);
    let refresh_interval_seconds = env_u64("MEXC_CONTRACT_CHANGE_FEED_INTERVAL_SECONDS", 5).max(1);
    let output_path = std::env::var("MEXC_CONTRACT_CHANGE_FEED_PATH").ok();

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
    policy.contract_refresh_interval = Some(Duration::from_secs(refresh_interval_seconds));
    policy.contract_event_refresh = false;
    policy.contract_gap_refresh = false;

    let mut total_refreshes = 0usize;
    let mut change_refreshes = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(watch_seconds);
    while tokio::time::Instant::now() < deadline {
        let Some(step) = runtime.next_step(&policy).await else {
            break;
        };
        let step = step?;
        let Some(refresh) = step.contract_refresh else {
            continue;
        };
        total_refreshes += 1;
        if refresh.is_noop() {
            continue;
        }

        change_refreshes += 1;
        let event = ContractChangeFeedEvent {
            kind: "contract_refresh_change",
            unix_ms: unix_ms()?,
            refreshed_contracts: refresh.refreshed_contracts,
            added: refresh.added,
            updated: refresh.updated,
            removed: refresh.removed,
            unchanged: refresh.unchanged,
            change_count: refresh.change_count(),
            report: &refresh,
        };
        emit_json_line(output_path.as_deref(), &event)?;
    }

    let summary = ContractChangeFeedSummary {
        kind: "summary",
        watch_seconds,
        refresh_interval_seconds,
        total_refreshes,
        change_refreshes,
    };
    if let Some(path) = output_path.as_deref() {
        println!("wrote contract change feed to {path}");
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
            .with_context(|| format!("open contract change feed path {path}"))?;
        file.write_all(line.as_bytes())
            .with_context(|| format!("append contract change feed line to {path}"))?;
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
