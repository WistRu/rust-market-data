use anyhow::{Context, Result};
use mexc::{
    DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY, MexcBootstrapFetchMode, MexcConnector,
    MexcLiveKindAlertReport, MexcManagedHealOutcome, MexcManagedRuntimePolicy, MexcPublicEventKind,
    MexcPublicRuntimeConfig, MexcPublicStateHandoffReport, MexcRuntimeSessionStatus,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Serialize)]
struct StartupSummary {
    ready: bool,
    seen_count: usize,
    expected_count: usize,
    observed_live_events: usize,
}

#[derive(Debug, Clone, Serialize)]
struct HealthSummary {
    healthy: bool,
    healthy_count: usize,
    expected_count: usize,
    stale_kinds: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DeepBootstrapSummary {
    symbol_count: usize,
    index_price_count: usize,
    fair_price_count: usize,
    funding_rate_count: usize,
    index_price_bulk_count: usize,
    fair_price_bulk_count: usize,
    funding_rate_bulk_count: usize,
    index_price_endpoint_count: usize,
    fair_price_endpoint_count: usize,
    funding_rate_endpoint_count: usize,
    index_price_mode: String,
    fair_price_mode: String,
    funding_rate_mode: String,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSummary {
    total_spot_resets: usize,
    total_futures_resets: usize,
    pending_spot_resets: usize,
    pending_futures_resets: usize,
}

#[derive(Debug, Clone, Serialize)]
struct SoakCheckpoint {
    elapsed_s: u64,
    total_live_events: usize,
    total_rebuilds: usize,
    total_pending_resets: usize,
    heal_trigger_count: usize,
    heal_suppressed_count: usize,
    health: HealthSummary,
    session: SessionSummary,
}

#[derive(Debug, Clone, Serialize)]
struct SoakTimelineEntry {
    elapsed_ms: u128,
    category: String,
    detail: String,
}

#[derive(Debug, Serialize)]
struct SoakReport {
    kind: &'static str,
    elapsed_seconds: u64,
    complete: bool,
    unix_started_at_s: u64,
    watch_seconds: u64,
    checkpoint_every_seconds: u64,
    auto_heal: bool,
    manifest: ManifestSummary,
    deep_bootstrap: Option<DeepBootstrapSummary>,
    total_rebuilds: usize,
    total_live_events: usize,
    total_pending_resets: usize,
    heal_trigger_count: usize,
    heal_suppressed_count: usize,
    public_metadata_refresh_count: usize,
    public_metadata_default_symbols_changed_count: usize,
    public_metadata_offline_symbols_changed_count: usize,
    public_metadata_exchange_info_changed_count: usize,
    public_metadata_transferable_currencies_changed_count: usize,
    public_metadata_insurance_balances_changed_count: usize,
    contract_refresh_count: usize,
    contract_refresh_interval_count: usize,
    contract_refresh_targeted_count: usize,
    contract_refresh_requested_symbol_total: usize,
    contract_refresh_added: usize,
    contract_refresh_updated: usize,
    contract_refresh_removed: usize,
    contract_refresh_unchanged: usize,
    state_handoff: MexcPublicStateHandoffReport,
    kind_counts: BTreeMap<String, usize>,
    startup: StartupSummary,
    health: HealthSummary,
    session: SessionSummary,
    checkpoints: Vec<SoakCheckpoint>,
    timeline: Vec<SoakTimelineEntry>,
}

#[derive(Debug, Serialize)]
struct ManifestSummary {
    spot_symbol_count: usize,
    futures_symbol_count: usize,
    spot_subscription_count: usize,
    futures_subscription_count: usize,
    spot_connection_count: usize,
    futures_connection_count: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let watch_seconds = env_u64("MEXC_SOAK_SECONDS", 60).max(1);
    let checkpoint_every_seconds = env_u64("MEXC_SOAK_CHECKPOINT_EVERY_SECONDS", 30).max(1);
    let auto_heal = env_u64("MEXC_SOAK_AUTO_HEAL", 0) > 0;
    let public_metadata_refresh_interval_seconds =
        env_u64("MEXC_PUBLIC_METADATA_REFRESH_INTERVAL_SECONDS", 0);
    let contract_refresh_interval_seconds = env_u64("MEXC_CONTRACT_REFRESH_INTERVAL_SECONDS", 0);
    let report_path = std::env::var("MEXC_SOAK_REPORT_PATH").ok();

    let unix_started_at_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs();

    let connector = MexcConnector::default();
    let mut runtime = connector
        .public_runtime_builder()
        .connect_managed_stateful_with_deep_snapshot(
            MexcPublicRuntimeConfig::balanced(),
            DEFAULT_FUTURES_REFERENCE_BOOTSTRAP_CONCURRENCY,
        )
        .await
        .context("connect managed stateful with deep snapshot")?;

    let startup = runtime
        .await_balanced_startup(Duration::from_secs(30))
        .await
        .context("await balanced startup")?;

    let mut policy = MexcManagedRuntimePolicy::balanced_defaults();
    policy.auto_heal = auto_heal;
    if public_metadata_refresh_interval_seconds > 0 {
        policy.public_metadata_refresh_interval = Some(Duration::from_secs(
            public_metadata_refresh_interval_seconds,
        ));
    }
    if contract_refresh_interval_seconds > 0 {
        policy.contract_refresh_interval =
            Some(Duration::from_secs(contract_refresh_interval_seconds));
    }

    let started_at = Instant::now();
    let deadline = started_at + Duration::from_secs(watch_seconds);
    let mut total_live_events = 0usize;
    let mut total_pending_resets = 0usize;
    let mut heal_trigger_count = 0usize;
    let mut heal_suppressed_count = 0usize;
    let mut public_metadata_refresh_count = 0usize;
    let mut public_metadata_default_symbols_changed_count = 0usize;
    let mut public_metadata_offline_symbols_changed_count = 0usize;
    let mut public_metadata_exchange_info_changed_count = 0usize;
    let mut public_metadata_transferable_currencies_changed_count = 0usize;
    let mut public_metadata_insurance_balances_changed_count = 0usize;
    let mut contract_refresh_count = 0usize;
    let mut contract_refresh_interval_count = 0usize;
    let mut contract_refresh_targeted_count = 0usize;
    let mut contract_refresh_requested_symbol_total = 0usize;
    let mut contract_refresh_added = 0usize;
    let mut contract_refresh_updated = 0usize;
    let mut contract_refresh_removed = 0usize;
    let mut contract_refresh_unchanged = 0usize;
    let mut kind_counts = BTreeMap::<String, usize>::new();
    let mut checkpoints = Vec::<SoakCheckpoint>::new();
    let mut timeline = Vec::<SoakTimelineEntry>::new();
    let mut next_checkpoint_elapsed_s = checkpoint_every_seconds;
    let mut last_persistent_stale_signature = None::<String>;

    while Instant::now() < deadline {
        let step = tokio::time::timeout(Duration::from_secs(10), runtime.next_step(&policy))
            .await
            .context("wait for managed runtime step")?
            .transpose()?
            .context("managed runtime ended unexpectedly")?;

        if let Some(kind) = step.event.kind() {
            total_live_events += 1;
            *kind_counts.entry(kind.as_str().to_string()).or_insert(0) += 1;
        }
        let elapsed = started_at.elapsed();
        if let Some(reset) = &step.pending_reset {
            total_pending_resets += 1;
            timeline.push(SoakTimelineEntry {
                elapsed_ms: elapsed.as_millis(),
                category: "reset".to_string(),
                detail: format!(
                    "spot_resets_detected={} futures_resets_detected={}",
                    reset.spot_resets_detected, reset.futures_resets_detected
                ),
            });
        }

        push_alert_timeline_entries(
            &mut timeline,
            elapsed,
            &step.health_alerts,
            &mut last_persistent_stale_signature,
        );

        if let Some(outcome) = step.heal_outcome {
            match outcome {
                MexcManagedHealOutcome::Healthy => {}
                MexcManagedHealOutcome::Suppressed {
                    cooldown_remaining_ms,
                    ..
                } => {
                    heal_suppressed_count += 1;
                    timeline.push(SoakTimelineEntry {
                        elapsed_ms: elapsed.as_millis(),
                        category: "heal_suppressed".to_string(),
                        detail: format!("cooldown_remaining_ms={cooldown_remaining_ms}"),
                    });
                }
                MexcManagedHealOutcome::Healed(report) => {
                    heal_trigger_count += 1;
                    timeline.push(SoakTimelineEntry {
                        elapsed_ms: elapsed.as_millis(),
                        category: "heal_triggered".to_string(),
                        detail: format!(
                            "total_rebuilds={} startup_ready={} coverage={}/{}",
                            report.total_rebuilds,
                            report.startup_after_heal.is_ready(),
                            report.startup_after_heal.seen_count(),
                            report.startup_after_heal.expected_count(),
                        ),
                    });
                }
            }
        }

        if let Some(refresh) = step.public_metadata_refresh {
            public_metadata_refresh_count += 1;
            public_metadata_default_symbols_changed_count +=
                usize::from(refresh.spot_default_symbols_changed);
            public_metadata_offline_symbols_changed_count +=
                usize::from(refresh.spot_offline_symbols_changed);
            public_metadata_exchange_info_changed_count +=
                usize::from(refresh.spot_exchange_info_changed);
            public_metadata_transferable_currencies_changed_count +=
                usize::from(refresh.futures_transferable_currencies_changed);
            public_metadata_insurance_balances_changed_count +=
                usize::from(refresh.futures_insurance_balances_changed);
            timeline.push(SoakTimelineEntry {
                elapsed_ms: elapsed.as_millis(),
                category: "public_metadata_refresh".to_string(),
                detail: format!(
                    "spot_server_time={}=>{} default_symbols={}=>{} offline_symbols={}=>{} exchange_symbols={}=>{} futures_server_time={}=>{} transferable_currencies={}=>{} insurance_balances={}=>{}",
                    refresh
                        .spot_server_time_before
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    refresh.spot_server_time_after,
                    refresh.spot_default_symbol_count_before,
                    refresh.spot_default_symbol_count_after,
                    refresh.spot_offline_symbol_count_before,
                    refresh.spot_offline_symbol_count_after,
                    refresh.spot_exchange_symbol_count_before,
                    refresh.spot_exchange_symbol_count_after,
                    refresh
                        .futures_server_time_before
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    refresh.futures_server_time_after,
                    refresh.futures_transferable_currency_count_before,
                    refresh.futures_transferable_currency_count_after,
                    refresh.futures_insurance_balance_count_before,
                    refresh.futures_insurance_balance_count_after
                ),
            });
        }

        if let Some(refresh) = step.contract_refresh {
            contract_refresh_count += 1;
            match refresh.cause {
                mexc::MexcManagedContractRefreshCause::Interval => {
                    contract_refresh_interval_count += 1;
                }
                mexc::MexcManagedContractRefreshCause::Targeted => {
                    contract_refresh_targeted_count += 1;
                }
                mexc::MexcManagedContractRefreshCause::GapBackfill => {
                    contract_refresh_targeted_count += 1;
                }
            }
            contract_refresh_requested_symbol_total += refresh.requested_symbols.len();
            contract_refresh_added += refresh.added;
            contract_refresh_updated += refresh.updated;
            contract_refresh_removed += refresh.removed;
            contract_refresh_unchanged += refresh.unchanged;
            timeline.push(SoakTimelineEntry {
                elapsed_ms: elapsed.as_millis(),
                category: "contract_refresh".to_string(),
                detail: format!(
                    "cause={:?} requested_symbols={:?} unresolved_requested_symbols={:?} used_full_snapshot_fallback={} refreshed_contracts={} added={} updated={} removed={} unchanged={} added_symbols={:?} updated_symbols={:?} removed_symbols={:?} changes={:?}",
                    refresh.cause,
                    refresh.requested_symbols,
                    refresh.unresolved_requested_symbols,
                    refresh.used_full_snapshot_fallback,
                    refresh.refreshed_contracts,
                    refresh.added,
                    refresh.updated,
                    refresh.removed,
                    refresh.unchanged,
                    refresh.added_symbols,
                    refresh.updated_symbols,
                    refresh.removed_symbols,
                    refresh.changes
                ),
            });
        }

        let elapsed_s = elapsed.as_secs();
        if elapsed_s >= next_checkpoint_elapsed_s {
            checkpoints.push(build_checkpoint(
                &runtime.session_status(),
                &runtime.balanced_health_status_with_defaults(),
                elapsed_s,
                total_live_events,
                total_pending_resets,
                heal_trigger_count,
                heal_suppressed_count,
                runtime.total_rebuilds(),
            ));
            if let Some(path) = report_path.as_deref() {
                write_soak_report(
                    path,
                    &build_soak_report(
                        "checkpoint",
                        false,
                        unix_started_at_s,
                        watch_seconds,
                        checkpoint_every_seconds,
                        auto_heal,
                        &runtime,
                        &startup,
                        elapsed_s,
                        total_live_events,
                        total_pending_resets,
                        heal_trigger_count,
                        heal_suppressed_count,
                        public_metadata_refresh_count,
                        public_metadata_default_symbols_changed_count,
                        public_metadata_offline_symbols_changed_count,
                        public_metadata_exchange_info_changed_count,
                        public_metadata_transferable_currencies_changed_count,
                        public_metadata_insurance_balances_changed_count,
                        contract_refresh_count,
                        contract_refresh_interval_count,
                        contract_refresh_targeted_count,
                        contract_refresh_requested_symbol_total,
                        contract_refresh_added,
                        contract_refresh_updated,
                        contract_refresh_removed,
                        contract_refresh_unchanged,
                        &kind_counts,
                        &checkpoints,
                        &timeline,
                    ),
                )?;
            }
            while next_checkpoint_elapsed_s <= elapsed_s {
                next_checkpoint_elapsed_s += checkpoint_every_seconds;
            }
        }
    }

    let report = build_soak_report(
        "summary",
        true,
        unix_started_at_s,
        watch_seconds,
        checkpoint_every_seconds,
        auto_heal,
        &runtime,
        &startup,
        started_at.elapsed().as_secs(),
        total_live_events,
        total_pending_resets,
        heal_trigger_count,
        heal_suppressed_count,
        public_metadata_refresh_count,
        public_metadata_default_symbols_changed_count,
        public_metadata_offline_symbols_changed_count,
        public_metadata_exchange_info_changed_count,
        public_metadata_transferable_currencies_changed_count,
        public_metadata_insurance_balances_changed_count,
        contract_refresh_count,
        contract_refresh_interval_count,
        contract_refresh_targeted_count,
        contract_refresh_requested_symbol_total,
        contract_refresh_added,
        contract_refresh_updated,
        contract_refresh_removed,
        contract_refresh_unchanged,
        &kind_counts,
        &checkpoints,
        &timeline,
    );

    let json = serde_json::to_string_pretty(&report).context("serialize soak report")?;
    if let Some(path) = report_path {
        std::fs::write(&path, json).with_context(|| format!("write soak report to {path}"))?;
        println!("wrote soak report to {path}");
    } else {
        println!("{json}");
    }

    Ok(())
}

fn bootstrap_mode_name(mode: MexcBootstrapFetchMode) -> String {
    mode.as_str().to_string()
}

#[allow(clippy::too_many_arguments)]
fn build_soak_report(
    kind: &'static str,
    complete: bool,
    unix_started_at_s: u64,
    watch_seconds: u64,
    checkpoint_every_seconds: u64,
    auto_heal: bool,
    runtime: &mexc::MexcManagedStatefulPublicRuntime,
    startup: &mexc::MexcLiveKindWaitReport,
    elapsed_seconds: u64,
    total_live_events: usize,
    total_pending_resets: usize,
    heal_trigger_count: usize,
    heal_suppressed_count: usize,
    public_metadata_refresh_count: usize,
    public_metadata_default_symbols_changed_count: usize,
    public_metadata_offline_symbols_changed_count: usize,
    public_metadata_exchange_info_changed_count: usize,
    public_metadata_transferable_currencies_changed_count: usize,
    public_metadata_insurance_balances_changed_count: usize,
    contract_refresh_count: usize,
    contract_refresh_interval_count: usize,
    contract_refresh_targeted_count: usize,
    contract_refresh_requested_symbol_total: usize,
    contract_refresh_added: usize,
    contract_refresh_updated: usize,
    contract_refresh_removed: usize,
    contract_refresh_unchanged: usize,
    kind_counts: &BTreeMap<String, usize>,
    checkpoints: &[SoakCheckpoint],
    timeline: &[SoakTimelineEntry],
) -> SoakReport {
    let final_health = runtime.balanced_health_status_with_defaults();
    let final_session = runtime.session_status();
    let manifest = runtime.manifest().clone();
    let state_handoff = runtime.state_handoff_report();

    SoakReport {
        kind,
        elapsed_seconds,
        complete,
        unix_started_at_s,
        watch_seconds,
        checkpoint_every_seconds,
        auto_heal,
        manifest: ManifestSummary {
            spot_symbol_count: manifest.spot_symbol_count,
            futures_symbol_count: manifest.futures_symbol_count,
            spot_subscription_count: manifest.spot_subscription_count,
            futures_subscription_count: manifest.futures_subscription_count,
            spot_connection_count: manifest.spot_connection_count,
            futures_connection_count: manifest.futures_connection_count,
        },
        deep_bootstrap: runtime.deep_report().map(|report| DeepBootstrapSummary {
            symbol_count: report.symbol_count,
            index_price_count: report.index_price_count,
            fair_price_count: report.fair_price_count,
            funding_rate_count: report.funding_rate_count,
            index_price_bulk_count: report.index_price_bulk_count,
            fair_price_bulk_count: report.fair_price_bulk_count,
            funding_rate_bulk_count: report.funding_rate_bulk_count,
            index_price_endpoint_count: report.index_price_endpoint_count,
            fair_price_endpoint_count: report.fair_price_endpoint_count,
            funding_rate_endpoint_count: report.funding_rate_endpoint_count,
            index_price_mode: bootstrap_mode_name(report.index_price_mode),
            fair_price_mode: bootstrap_mode_name(report.fair_price_mode),
            funding_rate_mode: bootstrap_mode_name(report.funding_rate_mode),
        }),
        total_rebuilds: runtime.total_rebuilds(),
        total_live_events,
        total_pending_resets,
        heal_trigger_count,
        heal_suppressed_count,
        public_metadata_refresh_count,
        public_metadata_default_symbols_changed_count,
        public_metadata_offline_symbols_changed_count,
        public_metadata_exchange_info_changed_count,
        public_metadata_transferable_currencies_changed_count,
        public_metadata_insurance_balances_changed_count,
        contract_refresh_count,
        contract_refresh_interval_count,
        contract_refresh_targeted_count,
        contract_refresh_requested_symbol_total,
        contract_refresh_added,
        contract_refresh_updated,
        contract_refresh_removed,
        contract_refresh_unchanged,
        state_handoff,
        kind_counts: kind_counts.clone(),
        startup: StartupSummary {
            ready: startup.is_ready(),
            seen_count: startup.seen_count(),
            expected_count: startup.expected_count(),
            observed_live_events: startup.observed_live_events,
        },
        health: HealthSummary {
            healthy: final_health.is_healthy(),
            healthy_count: final_health.healthy_count(),
            expected_count: final_health.expected_count(),
            stale_kinds: final_health
                .stale
                .iter()
                .map(|kind| kind.as_str().to_string())
                .collect(),
        },
        session: SessionSummary {
            total_spot_resets: final_session.total_spot_resets,
            total_futures_resets: final_session.total_futures_resets,
            pending_spot_resets: final_session.pending_spot_resets,
            pending_futures_resets: final_session.pending_futures_resets,
        },
        checkpoints: checkpoints.to_vec(),
        timeline: timeline.to_vec(),
    }
}

fn write_soak_report(path: &str, report: &SoakReport) -> Result<()> {
    let json = serde_json::to_string_pretty(report).context("serialize soak report")?;
    std::fs::write(path, json).with_context(|| format!("write soak report to {path}"))?;
    Ok(())
}

fn build_checkpoint(
    session: &MexcRuntimeSessionStatus,
    health: &mexc::MexcLiveKindHealthReport,
    elapsed_s: u64,
    total_live_events: usize,
    total_pending_resets: usize,
    heal_trigger_count: usize,
    heal_suppressed_count: usize,
    total_rebuilds: usize,
) -> SoakCheckpoint {
    SoakCheckpoint {
        elapsed_s,
        total_live_events,
        total_rebuilds,
        total_pending_resets,
        heal_trigger_count,
        heal_suppressed_count,
        health: HealthSummary {
            healthy: health.is_healthy(),
            healthy_count: health.healthy_count(),
            expected_count: health.expected_count(),
            stale_kinds: health
                .stale
                .iter()
                .map(|kind| kind.as_str().to_string())
                .collect(),
        },
        session: SessionSummary {
            total_spot_resets: session.total_spot_resets,
            total_futures_resets: session.total_futures_resets,
            pending_spot_resets: session.pending_spot_resets,
            pending_futures_resets: session.pending_futures_resets,
        },
    }
}

fn push_alert_timeline_entries(
    timeline: &mut Vec<SoakTimelineEntry>,
    elapsed: Duration,
    alerts: &MexcLiveKindAlertReport,
    last_persistent_stale_signature: &mut Option<String>,
) {
    if !alerts.persistent_stale.is_empty() {
        let detail = format_kinds(
            alerts
                .persistent_stale
                .iter()
                .map(|item| item.kind)
                .collect::<Vec<_>>()
                .as_slice(),
        );
        if last_persistent_stale_signature.as_deref() != Some(detail.as_str()) {
            timeline.push(SoakTimelineEntry {
                elapsed_ms: elapsed.as_millis(),
                category: "health_persistent_stale".to_string(),
                detail: detail.clone(),
            });
            *last_persistent_stale_signature = Some(detail);
        }
    } else {
        *last_persistent_stale_signature = None;
    }
}

fn format_kinds(kinds: &[MexcPublicEventKind]) -> String {
    kinds
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",")
}
